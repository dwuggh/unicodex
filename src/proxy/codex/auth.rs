use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use super::transport::{self, HttpClient};
use anyhow::Context;
use axum::http::{HeaderMap, HeaderValue, header::AUTHORIZATION};
use axum::{body::Body, http::Request};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct Credentials {
    authorization: HeaderValue,
    account: Option<HeaderValue>,
}

/// Shared per auth file. Header snapshots are cheap; network refresh has its own lock.
#[derive(Clone)]
pub struct CredentialManager {
    inner: Arc<Source>,
}

// Allocated once behind Arc; keep the request path free of a second indirection.
#[allow(clippy::large_enum_variant)]
enum Source {
    Static(Credentials),
    File(Managed),
}

struct Managed {
    path: PathBuf,
    endpoint: String,
    client: HttpClient,
    snapshot: RwLock<Snapshot>,
    refresh: Mutex<RefreshState>,
}
struct Snapshot {
    credentials: Credentials,
    due_at: Option<DateTime<Utc>>,
    blocked: bool,
}
struct RefreshState {
    document: Value,
    pending: Option<Value>,
    permanent: bool,
    retry_after: Option<Instant>,
}

impl From<Credentials> for CredentialManager {
    fn from(credentials: Credentials) -> Self {
        Self {
            inner: Arc::new(Source::Static(credentials)),
        }
    }
}

impl CredentialManager {
    pub fn from_file(path: &Path, auth_base: &str) -> anyhow::Result<Self> {
        let path = path.canonicalize().context("resolve upstream auth file")?;
        let document: Value =
            serde_json::from_slice(&std::fs::read(&path)?).context("parse upstream auth file")?;
        let snapshot = snapshot(&document)?;
        if document
            .get("OPENAI_API_KEY")
            .and_then(Value::as_str)
            .is_some_and(|s| !s.is_empty())
            || document
                .pointer("/tokens/refresh_token")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
        {
            return Ok(snapshot.credentials.into());
        }
        anyhow::ensure!(
            document.get("auth_mode").and_then(Value::as_str) != Some("chatgptAuthTokens"),
            "upstream refresh credentials must be proxy-owned, not client chatgptAuthTokens"
        );
        Ok(Self {
            inner: Arc::new(Source::File(Managed {
                path,
                endpoint: format!("{}/oauth/token", transport::base_url(auth_base)?),
                client: transport::client()?,
                snapshot: RwLock::new(snapshot),
                refresh: Mutex::new(RefreshState {
                    document,
                    pending: None,
                    permanent: false,
                    retry_after: None,
                }),
            })),
        })
    }

    pub async fn credentials(&self) -> anyhow::Result<Credentials> {
        match self.inner.as_ref() {
            Source::Static(value) => Ok(value.clone()),
            Source::File(managed) => {
                {
                    let current = managed
                        .snapshot
                        .read()
                        .map_err(|_| anyhow::anyhow!("credential lock poisoned"))?;
                    if !current.blocked && current.due_at.is_none_or(|at| Utc::now() < at) {
                        return Ok(current.credentials.clone());
                    }
                }
                managed.refresh(None).await
            }
        }
    }

    /// Called only after a rejected request. Never replays the original request body.
    pub async fn rejected(&self, used: &Credentials) -> anyhow::Result<()> {
        match self.inner.as_ref() {
            Source::Static(_) => {
                tracing::warn!(retryable = false, "static upstream credentials rejected");
                anyhow::bail!("upstream credentials rejected")
            }
            Source::File(managed) => managed.refresh(Some(&used.authorization)).await.map(|_| ()),
        }
    }
}

fn snapshot(document: &Value) -> anyhow::Result<Snapshot> {
    let (token, account) = if let Some(key) = document
        .get("OPENAI_API_KEY")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        (key, None)
    } else {
        (
            document
                .pointer("/tokens/access_token")
                .and_then(Value::as_str)
                .context("missing access token")?,
            document
                .pointer("/tokens/account_id")
                .and_then(Value::as_str),
        )
    };
    let expiry = token
        .split('.')
        .nth(1)
        .and_then(|part| URL_SAFE_NO_PAD.decode(part).ok())
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
        .and_then(|claims| claims.get("exp")?.as_i64())
        .and_then(|seconds| DateTime::from_timestamp(seconds, 0));
    let due_at = expiry
        .map(|at| at - chrono::Duration::minutes(5))
        .or_else(|| {
            document
                .get("last_refresh")
                .and_then(Value::as_str)
                .and_then(|date| DateTime::parse_from_rfc3339(date).ok())
                .map(|at| at.with_timezone(&Utc) + chrono::Duration::days(8))
        });
    Ok(Snapshot {
        credentials: Credentials::bearer(token, account)?,
        due_at,
        blocked: false,
    })
}

impl Managed {
    async fn refresh(&self, rejected: Option<&HeaderValue>) -> anyhow::Result<Credentials> {
        let mut state = self.refresh.lock().await;
        if let Some(document) = state.pending.as_ref() {
            persist(&self.path, document)?;
            let next = snapshot(document)?;
            *self
                .snapshot
                .write()
                .map_err(|_| anyhow::anyhow!("credential lock poisoned"))? = next;
            state.document = state.pending.take().unwrap();
            tracing::info!("pending refreshed credentials persisted");
        }
        if state.permanent {
            tracing::debug!(retryable = false, "credential refresh blocked");
        } else if state.retry_after.is_some_and(|at| Instant::now() < at) {
            tracing::debug!(retryable = true, "credential refresh in cooldown");
        }
        anyhow::ensure!(
            !state.permanent,
            "upstream authentication requires replacement credentials"
        );
        anyhow::ensure!(
            state.retry_after.is_none_or(|at| Instant::now() >= at),
            "upstream credential refresh temporarily unavailable"
        );
        {
            let current = self
                .snapshot
                .read()
                .map_err(|_| anyhow::anyhow!("credential lock poisoned"))?;
            let already_refreshed =
                rejected.is_some_and(|used| *used != current.credentials.authorization);
            if !current.blocked
                && (already_refreshed
                    || (rejected.is_none() && current.due_at.is_none_or(|at| Utc::now() < at)))
            {
                return Ok(current.credentials.clone());
            }
        }
        self.snapshot
            .write()
            .map_err(|_| anyhow::anyhow!("credential lock poisoned"))?
            .blocked = true;
        tracing::debug!(
            reason = if rejected.is_some() {
                "upstream_rejection"
            } else {
                "expiry"
            },
            "credential refresh started"
        );
        let result =
            tokio::time::timeout(Duration::from_secs(15), self.exchange(&state.document)).await;
        let response = match result {
            Ok(Ok((permanent, result))) => {
                if permanent {
                    state.permanent = true;
                }
                result
            }
            Ok(Err(_)) => {
                tracing::warn!(
                    operation = "token_exchange",
                    retryable = true,
                    "credential refresh unavailable"
                );
                Err(anyhow::anyhow!("upstream token refresh unavailable"))
            }
            Err(_) => {
                tracing::warn!(
                    operation = "token_exchange",
                    retryable = true,
                    "credential refresh timed out"
                );
                Err(anyhow::anyhow!("upstream token refresh unavailable"))
            }
        };
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                state.retry_after = Some(Instant::now() + Duration::from_secs(30));
                tracing::warn!(retryable = !state.permanent, "credential refresh failed");
                return Err(error);
            }
        };
        let mut document = state.document.clone();
        for field in ["access_token", "refresh_token", "id_token"] {
            if let Some(value) = response.get(field).filter(|v| !v.is_null()) {
                document["tokens"][field] = value.clone();
            }
        }
        document["last_refresh"] = json!(Utc::now().to_rfc3339());
        let next = snapshot(&document)?;
        state.pending = Some(document);
        persist(&self.path, state.pending.as_ref().unwrap())?;
        state.document = state.pending.take().unwrap();
        state.retry_after = None;
        let credentials = next.credentials.clone();
        *self
            .snapshot
            .write()
            .map_err(|_| anyhow::anyhow!("credential lock poisoned"))? = next;
        tracing::info!("credential refresh succeeded");
        Ok(credentials)
    }

    async fn exchange(&self, document: &Value) -> anyhow::Result<(bool, anyhow::Result<Value>)> {
        let body = json!({"grant_type":"refresh_token", "client_id":"app_EMoamEEZ73f0CkXaXp7hrann",
            "refresh_token": document.pointer("/tokens/refresh_token").and_then(Value::as_str).context("missing refresh token")?});
        let request = Request::post(&self.endpoint)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body)?))?;
        let response = self.client.request(request).await?;
        let status = response.status();
        let bytes = http_body_util::Limited::new(response.into_body(), 1024 * 1024)
            .collect()
            .await
            .map_err(|_| anyhow::anyhow!("invalid token response body"))?
            .to_bytes();
        let response: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        if !status.is_success() {
            let code = response
                .pointer("/error/code")
                .or_else(|| response.get("error"))
                .or_else(|| response.get("code"))
                .and_then(Value::as_str);
            let permanent = status.as_u16() == 401
                || matches!(
                    code,
                    Some(
                        "invalid_grant"
                            | "refresh_token_expired"
                            | "refresh_token_reused"
                            | "refresh_token_invalidated"
                    )
                );
            tracing::warn!(
                upstream_status = status.as_u16(),
                retryable = !permanent,
                "token endpoint rejected refresh"
            );
            return Ok((
                permanent,
                Err(anyhow::anyhow!("upstream token refresh rejected")),
            ));
        }
        let valid_access = response
            .get("access_token")
            .and_then(Value::as_str)
            .is_some_and(|token| Credentials::bearer(token, None).is_ok());
        let valid_optional = ["refresh_token", "id_token"].iter().all(|field| {
            response.get(field).is_none_or(|value| {
                value.is_null() || value.as_str().is_some_and(|token| !token.trim().is_empty())
            })
        });
        if !valid_access || !valid_optional {
            tracing::warn!(retryable = false, "invalid credential refresh response");
            // A successful exchange may have rotated the refresh token already. Do not
            // keep retrying the old token when the returned credentials are unusable.
            return Ok((true, Err(anyhow::anyhow!("invalid token refresh response"))));
        }
        Ok((false, Ok(response)))
    }
}

fn persist(path: &Path, document: &Value) -> anyhow::Result<()> {
    persist_inner(path, document).inspect_err(|_| {
        tracing::error!(
            operation = "persist_credentials",
            "credential persistence failed"
        );
    })
}

fn persist_inner(path: &Path, document: &Value) -> anyhow::Result<()> {
    let parent = path.parent().context("auth file has no parent")?;
    let mut temp = tempfile::NamedTempFile::new_in(parent).context("create private auth file")?;
    serde_json::to_writer_pretty(&mut temp, document)?;
    temp.write_all(b"\n")?;
    temp.as_file().sync_all()?;
    temp.persist(path)
        .map_err(|error| error.error)
        .context("persist rotated upstream credentials")?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

impl Credentials {
    pub fn account_id(&self) -> Option<&str> {
        self.account.as_ref().and_then(|value| value.to_str().ok())
    }

    pub fn bearer(token: &str, account: Option<&str>) -> anyhow::Result<Self> {
        anyhow::ensure!(!token.trim().is_empty(), "empty upstream token");
        let mut authorization = HeaderValue::from_str(&format!("Bearer {token}"))?;
        authorization.set_sensitive(true);
        let account = account.map(HeaderValue::from_str).transpose()?;
        Ok(Self {
            authorization,
            account,
        })
    }

    pub fn apply(&self, headers: &mut HeaderMap) {
        headers.insert(AUTHORIZATION, self.authorization.clone());
        headers.remove("chatgpt-account-id");
        if let Some(account) = &self.account {
            headers.insert("chatgpt-account-id", account.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, http::StatusCode, response::IntoResponse};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Server {
        url: String,
        task: tokio::task::JoinHandle<()>,
    }
    impl Drop for Server {
        fn drop(&mut self) {
            self.task.abort();
        }
    }
    async fn serve(router: Router) -> Server {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Server { url, task }
    }
    fn auth_file(directory: &Path) -> PathBuf {
        let path = directory.join("auth.json");
        let expired = format!("header.{}.sig", URL_SAFE_NO_PAD.encode(br#"{"exp":1}"#));
        std::fs::write(&path, serde_json::to_vec(&json!({"auth_mode":"chatgpt", "extra":{"keep":true},
            "tokens":{"access_token":expired,"refresh_token":"refresh-old","id_token":"id-old","account_id":"real-account","extra":"keep"},
            "last_refresh":"2000-01-01T00:00:00Z"})).unwrap()).unwrap();
        path
    }
    fn token(credentials: &Credentials) -> String {
        let mut headers = HeaderMap::new();
        credentials.apply(&mut headers);
        assert_eq!(headers["chatgpt-account-id"], "real-account");
        headers[AUTHORIZATION].to_str().unwrap().to_owned()
    }

    #[tokio::test]
    async fn concurrent_refresh_rotates_once_preserves_fields_and_survives_restart() {
        use tracing::instrument::WithSubscriber;
        let capture = crate::logging::tests::Capture::default();
        let count = Arc::new(AtomicUsize::new(0));
        let calls = count.clone();
        let server = serve(Router::new().route(
            "/oauth/token",
            axum::routing::post(move |Json(body): Json<Value>| {
                let calls = calls.clone();
                async move {
                    assert_eq!(body["grant_type"], "refresh_token");
                    assert_eq!(body["client_id"], "app_EMoamEEZ73f0CkXaXp7hrann");
                    let n = calls.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        assert_eq!(body["refresh_token"], "refresh-old");
                        Json(json!({"access_token":"access-new", "refresh_token":"refresh-new"}))
                    } else {
                        assert_eq!(body["refresh_token"], "refresh-new");
                        Json(json!({"access_token":"access-next"}))
                    }
                }
            }),
        ))
        .await;
        let directory = tempfile::tempdir().unwrap();
        let path = auth_file(directory.path());
        let manager = CredentialManager::from_file(&path, &server.url).unwrap();
        let results = futures_util::future::join_all((0..12).map(|_| manager.credentials()))
            .with_subscriber(capture.subscriber("warn,unicodex=debug"))
            .await;
        for result in results {
            assert_eq!(token(&result.unwrap()), "Bearer access-new");
        }
        assert_eq!(count.load(Ordering::SeqCst), 1);
        let restarted = CredentialManager::from_file(&path, &server.url).unwrap();
        let used = restarted.credentials().await.unwrap();
        assert_eq!(token(&used), "Bearer access-new");
        restarted.rejected(&used).await.unwrap();
        restarted.rejected(&used).await.unwrap(); // another request rejected the same old snapshot
        assert_eq!(count.load(Ordering::SeqCst), 2);
        assert_eq!(
            token(&restarted.credentials().await.unwrap()),
            "Bearer access-next"
        );
        let saved: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(saved["tokens"]["refresh_token"], "refresh-new");
        assert_eq!(saved["tokens"]["id_token"], "id-old");
        assert_eq!(saved["tokens"]["extra"], "keep");
        assert_eq!(saved["extra"]["keep"], true);
        let logs = capture.text();
        assert_eq!(logs.matches("credential refresh started").count(), 1);
        assert_eq!(logs.matches("credential refresh succeeded").count(), 1);
        for secret in [
            "refresh-old",
            "access-new",
            "refresh-new",
            "id-old",
            "real-account",
        ] {
            assert!(!logs.contains(secret));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[tokio::test]
    async fn rotated_tokens_are_retained_when_persistence_fails() {
        use tracing::instrument::WithSubscriber;
        let capture = crate::logging::tests::Capture::default();
        let count = Arc::new(AtomicUsize::new(0));
        let calls = count.clone();
        let server = serve(Router::new().fallback(move || {
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Json(json!({"access_token":"new", "refresh_token":"rotated"}))
            }
        }))
        .await;
        let directory = tempfile::tempdir().unwrap();
        let path = auth_file(directory.path());
        let manager = CredentialManager::from_file(&path, &server.url).unwrap();
        std::fs::rename(&path, directory.path().join("old.json")).unwrap();
        std::fs::create_dir(&path).unwrap();
        assert!(
            manager
                .credentials()
                .with_subscriber(capture.subscriber("warn,unicodex=debug"))
                .await
                .is_err()
        );
        assert!(
            manager
                .credentials()
                .with_subscriber(capture.subscriber("warn,unicodex=debug"))
                .await
                .is_err()
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
        std::fs::remove_dir(&path).unwrap();
        assert_eq!(
            token(
                &manager
                    .credentials()
                    .with_subscriber(capture.subscriber("warn,unicodex=debug"))
                    .await
                    .unwrap()
            ),
            "Bearer new"
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
        let saved: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(saved["tokens"]["refresh_token"], "rotated");
        let logs = capture.text();
        assert_eq!(logs.matches("credential persistence failed").count(), 2);
        assert!(logs.contains("pending refreshed credentials persisted"));
        assert!(!logs.contains("refresh-old"));
        assert!(!logs.contains("rotated"));
    }

    #[tokio::test]
    async fn refresh_failures_never_publish_bad_credentials_or_spin() {
        use tracing::instrument::WithSubscriber;
        for (status, body, permanent) in [
            (
                StatusCode::SERVICE_UNAVAILABLE,
                json!({"error":"unavailable"}),
                false,
            ),
            (
                StatusCode::BAD_REQUEST,
                json!({"error":"invalid_grant"}),
                true,
            ),
            (StatusCode::OK, json!({"refresh_token":"rotated"}), true),
            (
                StatusCode::OK,
                json!({"access_token":"invalid\ntoken"}),
                true,
            ),
            (
                StatusCode::OK,
                json!({"access_token":"new", "refresh_token":42}),
                true,
            ),
        ] {
            let count = Arc::new(AtomicUsize::new(0));
            let calls = count.clone();
            let server = serve(Router::new().fallback(move || {
                let calls = calls.clone();
                let body = body.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    (status, Json(body)).into_response()
                }
            }))
            .await;
            let directory = tempfile::tempdir().unwrap();
            let path = auth_file(directory.path());
            let original = std::fs::read(&path).unwrap();
            let manager = CredentialManager::from_file(&path, &server.url).unwrap();
            let capture = crate::logging::tests::Capture::default();
            assert!(
                manager
                    .credentials()
                    .with_subscriber(capture.subscriber("warn,unicodex=debug"))
                    .await
                    .is_err()
            );
            let logs = capture.text();
            let failed = logs
                .lines()
                .find(|line| line.contains("credential refresh failed"))
                .unwrap();
            assert!(failed.contains("WARN"));
            assert!(failed.contains(&format!("retryable={}", !permanent)));
            assert!(!logs.contains("refresh-old"));
            assert!(!logs.contains("rotated"));
            assert!(manager.credentials().await.is_err());
            assert_eq!(count.load(Ordering::SeqCst), 1);
            assert_eq!(std::fs::read(path).unwrap(), original);
            let Source::File(managed) = manager.inner.as_ref() else {
                unreachable!()
            };
            managed.refresh.lock().await.retry_after = None;
            assert!(manager.credentials().await.is_err());
            assert_eq!(count.load(Ordering::SeqCst), if permanent { 1 } else { 2 });
        }
    }
}
