use std::{
    future::Future,
    sync::Arc,
    task::{Context, Poll},
};

use anyhow::Context as _;
use axum::{
    body::Body,
    extract::ws::{CloseFrame, Message, WebSocket},
    http::{HeaderMap, header},
    response::Response,
};
use futures_util::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use tokio::sync::Mutex;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{self, client::IntoClientRequest},
};
use tower::Service;
use tracing::{Instrument, instrument::WithSubscriber};

use super::{
    account::{self, CachedWindow},
    auth::{CredentialManager, Credentials},
    routes::{self, Family, Mode, Rewrite},
    stat::{CodexObserver, RequestCapture, ResponseTracker},
    transport::{self, HttpClient},
};
use crate::ledger::{Ledger, WeeklyCredits};
use crate::proxy::{Admission, Observer, Outbound, OutboundInput, ProxyError, Tag, Transport};

type UpstreamSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Clone)]
pub struct CodexOutbound {
    tag: Arc<str>,
    base_url: Arc<str>,
    chatgpt_base_url: Arc<str>,
    api_base_url: Arc<str>,
    auth_base_url: Arc<str>,
    mode: Mode,
    credentials: CredentialManager,
    client: HttpClient,
    observer: Arc<CodexObserver>,
    window: Arc<Mutex<CachedWindow>>,
}

pub struct OutboundOptions {
    pub base_url: String,
    pub chatgpt_base_url: String,
    pub api_base_url: String,
    pub auth_base_url: String,
    pub mode: Mode,
}

impl From<String> for OutboundOptions {
    fn from(base_url: String) -> Self {
        Self {
            base_url,
            chatgpt_base_url: "https://chatgpt.com/backend-api".into(),
            api_base_url: "https://api.openai.com/v1".into(),
            auth_base_url: "https://auth.openai.com".into(),
            mode: Mode::Broad,
        }
    }
}

impl CodexOutbound {
    pub fn new(
        tag: String,
        options: OutboundOptions,
        credentials: CredentialManager,
        observer: Arc<CodexObserver>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            tag: tag.into(),
            base_url: transport::base_url(&options.base_url)?.into(),
            chatgpt_base_url: transport::base_url(&options.chatgpt_base_url)?.into(),
            api_base_url: transport::base_url(&options.api_base_url)?.into(),
            auth_base_url: transport::base_url(&options.auth_base_url)?.into(),
            mode: options.mode,
            credentials,
            client: transport::client()?,
            observer,
            window: Arc::new(Mutex::new(CachedWindow::default())),
        })
    }

    async fn check_admission(&self, user: &str, admission: &Ledger) -> Result<(), ProxyError> {
        match admission.weekly_credits(user)? {
            WeeklyCredits::Unlimited => return Ok(()),
            WeeklyCredits::Limited(amount) if amount.is_zero() => {
                return Err(ProxyError::NoCredits);
            }
            WeeklyCredits::Limited(_) => {}
        }
        let credentials = self
            .credentials
            .credentials()
            .await
            .map_err(ProxyError::UpstreamAuth)?;
        let window = {
            // Only window discovery is serialized, not inference or database reads.
            let mut cache = self.window.lock().await;
            if let Some(window) =
                cache.get(credentials.account_id(), chrono::Utc::now().timestamp())
            {
                window
            } else {
                let fetch = async {
                    let mut request = axum::http::Request::builder()
                        .uri(format!("{}/wham/usage", self.chatgpt_base_url))
                        .header(header::ACCEPT, "application/json")
                        .header(header::ACCEPT_ENCODING, "identity")
                        .body(Body::empty())?;
                    credentials.apply(request.headers_mut());
                    let response = self.client.request(request).await?;
                    if response.status() == axum::http::StatusCode::UNAUTHORIZED {
                        self.credentials.rejected(&credentials).await?;
                    }
                    anyhow::ensure!(
                        response.status().is_success(),
                        "weekly window discovery failed"
                    );
                    anyhow::ensure!(
                        response
                            .headers()
                            .get(header::CONTENT_ENCODING)
                            .is_none_or(|value| value == "identity"),
                        "encoded weekly window response"
                    );
                    let bytes =
                        http_body_util::Limited::new(response.into_body(), 16 * 1024 * 1024)
                            .collect()
                            .await
                            .map_err(|_| {
                                anyhow::anyhow!("weekly window response could not be read")
                            })?
                            .to_bytes();
                    let value: serde_json::Value = serde_json::from_slice(&bytes)?;
                    let (_, window) =
                        account::weekly_window(&value).context("upstream weekly window missing")?;
                    anyhow::ensure!(
                        window.contains(chrono::Utc::now().timestamp()),
                        "upstream weekly window expired"
                    );
                    Ok::<_, anyhow::Error>(window)
                };
                let window = tokio::time::timeout(std::time::Duration::from_secs(30), fetch)
                    .await
                    .map_err(|error| ProxyError::Upstream(error.into()))?
                    .map_err(ProxyError::Upstream)?;
                cache.update(credentials.account_id(), Some(window));
                window
            }
        };
        admission.check(user, Some(window)).await
    }

    async fn rejected(&self, credentials: &Credentials, unlimited: bool) -> Result<(), ProxyError> {
        let result = self.credentials.rejected(credentials).await;
        if unlimited {
            if result.is_err() {
                tracing::warn!(
                    operation = "refresh_credentials",
                    "refresh failed; preserving upstream rejection"
                );
            }
            return Ok(());
        }
        result.map_err(ProxyError::UpstreamAuth)?;
        Err(ProxyError::UpstreamAuth(anyhow::anyhow!(
            "upstream rejected request; credentials refreshed for subsequent requests"
        )))
    }

    async fn forward(self, input: OutboundInput) -> Result<Response, ProxyError> {
        let observer = self.observer.clone();
        let OutboundInput {
            exchange,
            admission,
        } = input;
        let crate::proxy::Exchange {
            mut req,
            transport,
            user,
        } = exchange;
        let unlimited = admission.weekly_credits(&user)? == WeeklyCredits::Unlimited;
        let route = routes::classify(
            if unlimited { Mode::Broad } else { self.mode },
            req.method(),
            req.uri(),
            matches!(&transport, Transport::Ws(_)),
        )
        .inspect_err(|_| {
            tracing::debug!(reason = "unsupported_route", "request rejected");
        })?;
        tracing::debug!(family = ?route.family, path = ?route.path, rewrite = ?route.rewrite, "route classified");
        if route.inference {
            self.check_admission(&user, &admission).await?;
        }
        let resets_window = route.family == Family::Product
            && req.method() == axum::http::Method::POST
            && route.path == "/wham/rate-limit-reset-credits/consume";
        let client_account = req
            .headers()
            .get("chatgpt-account-id")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let credentials = self
            .credentials
            .credentials()
            .await
            .map_err(ProxyError::UpstreamAuth)?;
        let base = match route.family {
            Family::Model => &self.base_url,
            Family::Product => &self.chatgpt_base_url,
            Family::Api => &self.api_base_url,
            Family::Auth => &self.auth_base_url,
        };
        let mut target = format!("{base}{}", route.path);
        if let Some(query) = route.query {
            target.push('?');
            target.push_str(&query);
        }
        let started = std::time::Instant::now();
        match transport {
            Transport::Http => {
                let capture = if route.inference {
                    RequestCapture::inspect(&mut req)
                } else {
                    RequestCapture::default()
                };
                *req.uri_mut() = target
                    .parse()
                    .map_err(|error| ProxyError::BadRequest(anyhow::Error::new(error)))?;
                strip_hop_headers(req.headers_mut());
                req.headers_mut().remove(header::HOST);
                credentials.apply(req.headers_mut());
                if route.inference || route.rewrite.is_some() {
                    req.headers_mut().insert(
                        header::ACCEPT_ENCODING,
                        header::HeaderValue::from_static("identity"),
                    );
                }
                let response = self
                    .client
                    .request(req)
                    .await
                    .map_err(|error| ProxyError::Upstream(error.into()))?;
                let (mut parts, body) = response.into_parts();
                log_upstream_status(parts.status, started.elapsed());
                if parts.status == axum::http::StatusCode::UNAUTHORIZED {
                    self.rejected(&credentials, unlimited).await?;
                }
                // Unlimited status is observed without an adapter; refresh admission's
                // cached bounds on the next limited request instead of rewriting it.
                if parts.status.is_success()
                    && (resets_window || (unlimited && route.rewrite == Some(Rewrite::Usage)))
                {
                    self.window
                        .lock()
                        .await
                        .update(credentials.account_id(), None);
                }
                strip_hop_headers(&mut parts.headers);
                let response = Response::from_parts(parts, Body::new(body));
                if let Some(kind) = route
                    .rewrite
                    .filter(|kind| !unlimited || *kind == Rewrite::Accounts)
                {
                    super::account::rewrite(
                        observer.ledger(),
                        &user,
                        response,
                        kind,
                        client_account.as_deref(),
                        credentials.account_id(),
                        &self.window,
                    )
                    .await
                    .map_err(ProxyError::Internal)
                } else if route.inference || route.rewrite == Some(Rewrite::Usage) {
                    observer
                        .observe_http_with_context(user, response, capture)
                        .await
                        .map_err(ProxyError::Internal)
                } else {
                    Ok(response)
                }
            }
            Transport::Ws(upgrade) => {
                let url = if let Some(rest) = target.strip_prefix("https:") {
                    format!("wss:{rest}")
                } else {
                    target.replacen("http:", "ws:", 1)
                };
                let mut upstream = url
                    .into_client_request()
                    .map_err(|error| ProxyError::BadRequest(error.into()))?;
                strip_hop_headers(req.headers_mut());
                for (name, value) in req.headers() {
                    if !matches!(
                        name.as_str(),
                        "host"
                            | "sec-websocket-key"
                            | "sec-websocket-version"
                            | "sec-websocket-extensions"
                    ) {
                        upstream.headers_mut().append(name, value.clone());
                    }
                }
                credentials.apply(upstream.headers_mut());
                let config = tungstenite::protocol::WebSocketConfig::default()
                    .max_message_size(Some(16 * 1024 * 1024));
                let (socket, handshake) = match tokio_tungstenite::connect_async_with_config(
                    upstream,
                    Some(config),
                    false,
                )
                .await
                {
                    Ok(connected) => connected,
                    Err(tungstenite::Error::Http(response)) => {
                        log_upstream_status(response.status(), started.elapsed());
                        if response.status() == axum::http::StatusCode::UNAUTHORIZED {
                            self.rejected(&credentials, unlimited).await?;
                        }
                        let (mut parts, body) = response.into_parts();
                        strip_hop_headers(&mut parts.headers);
                        let response =
                            Response::from_parts(parts, Body::from(body.unwrap_or_default()));
                        return if route.inference {
                            observer
                                .observe_http(user, response)
                                .await
                                .map_err(ProxyError::Internal)
                        } else {
                            Ok(response)
                        };
                    }
                    Err(error) => return Err(ProxyError::Upstream(error.into())),
                };
                log_upstream_status(handshake.status(), started.elapsed());
                let protocol = handshake
                    .headers()
                    .get(header::SEC_WEBSOCKET_PROTOCOL)
                    .cloned();
                let (mut parts, _) = handshake.into_parts();
                strip_hop_headers(&mut parts.headers);
                parts.headers.remove(header::SEC_WEBSOCKET_ACCEPT);
                parts.headers.remove(header::SEC_WEBSOCKET_PROTOCOL);
                let response = Response::from_parts(parts, Body::empty());
                let observed = if route.inference {
                    observer
                        .clone()
                        .observe_http(user.clone(), response)
                        .await?
                } else {
                    response
                };
                let (parts, body) = observed.into_parts();
                body.collect()
                    .await
                    .map_err(|error| ProxyError::Internal(error.into()))?;
                let upgrade = if let Some(protocol) = protocol {
                    upgrade.protocols([protocol
                        .to_str()
                        .map_err(|error| ProxyError::Upstream(error.into()))?
                        .to_owned()])
                } else {
                    upgrade
                };
                let span = tracing::Span::current();
                let subscriber = tracing::dispatcher::get_default(Clone::clone);
                let failed_span = span.clone();
                let failed_subscriber = subscriber.clone();
                let mut response = upgrade
                    .max_message_size(16 * 1024 * 1024)
                    .on_failed_upgrade(move |_| {
                        tracing::dispatcher::with_default(&failed_subscriber, || {
                            failed_span
                                .in_scope(|| tracing::warn!("client WebSocket upgrade failed"));
                        });
                    })
                    .on_upgrade(move |mut client| {
                        async move {
                            let started = std::time::Instant::now();
                            tracing::debug!("WebSocket relay started");
                            if let Err(error) =
                                relay(&mut client, socket, &user, self, admission, route.inference)
                                    .await
                            {
                                if error.status().is_client_error() {
                                    tracing::warn!(
                                        error_kind = error.kind(),
                                        "WebSocket relay failed"
                                    );
                                } else {
                                    tracing::error!(
                                        error_kind = error.kind(),
                                        "WebSocket relay failed"
                                    );
                                }
                                let _ = client
                                    .send(Message::Close(Some(CloseFrame {
                                        code: 1011,
                                        reason: error.to_string().into(),
                                    })))
                                    .await;
                            } else {
                                tracing::debug!(
                                    session_duration_ms = started.elapsed().as_secs_f64() * 1000.0,
                                    "WebSocket relay finished"
                                );
                            }
                        }
                        .instrument(span)
                        .with_subscriber(subscriber)
                    });
                response.headers_mut().extend(parts.headers);
                Ok(response)
            }
        }
    }
}

fn log_upstream_status(status: axum::http::StatusCode, elapsed: std::time::Duration) {
    let upstream_ready_ms = elapsed.as_secs_f64() * 1000.0;
    if status.is_server_error() {
        tracing::error!(
            upstream_status = status.as_u16(),
            upstream_ready_ms,
            "upstream response received"
        );
    } else if status.is_client_error() {
        tracing::warn!(
            upstream_status = status.as_u16(),
            upstream_ready_ms,
            "upstream response received"
        );
    } else {
        tracing::debug!(
            upstream_status = status.as_u16(),
            upstream_ready_ms,
            "upstream response received"
        );
    }
}

impl Tag for CodexOutbound {
    fn tag(&self) -> &str {
        &self.tag
    }
}

impl Outbound for CodexOutbound {
    type Observer = CodexObserver;

    fn observer(&self) -> &Arc<Self::Observer> {
        &self.observer
    }
}

impl Service<OutboundInput> for CodexOutbound {
    type Response = Response;
    type Error = ProxyError;
    type Future = impl Future<Output = Result<Response, ProxyError>> + Send + 'static;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, input: OutboundInput) -> Self::Future {
        self.clone().forward(input)
    }
}

fn strip_hop_headers(headers: &mut HeaderMap) {
    let named: Vec<_> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(|name| name.trim().to_owned())
        .collect();
    for name in named {
        headers.remove(name);
    }
    for name in [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ] {
        headers.remove(name);
    }
}

async fn relay(
    client: &mut WebSocket,
    mut upstream: UpstreamSocket,
    user: &str,
    outbound: CodexOutbound,
    admission: Arc<Ledger>,
    inference: bool,
) -> Result<(), ProxyError> {
    let observer = &outbound.observer;
    let unlimited = admission.weekly_credits(user)? == WeeklyCredits::Unlimited;
    let mut tracker = ResponseTracker::default();
    loop {
        tokio::select! {
            // Finish persisting an available upstream report before accepting the next client request.
            biased;
            message = upstream.next() => {
                let Some(message) = message else { return Ok(()); };
                let message = message.map_err(|error| ProxyError::Upstream(error.into()))?;
                let message = match message {
                    tungstenite::Message::Text(text) => {
                        if inference { observer.message(user, text.as_bytes(), &mut tracker).await; }
                        Message::Text(text.as_str().to_owned().into())
                    }
                    tungstenite::Message::Binary(bytes) => {
                        if inference { observer.message(user, &bytes, &mut tracker).await; }
                        Message::Binary(bytes)
                    }
                    tungstenite::Message::Close(frame) => {
                        client.send(Message::Close(frame.map(|frame| CloseFrame {
                            code: frame.code.into(), reason: frame.reason.as_str().to_owned().into(),
                        }))).await.map_err(|error| ProxyError::Upstream(error.into()))?;
                        return Ok(());
                    }
                    // Both WebSocket stacks answer control pings automatically.
                    _ => continue,
                };
                client.send(message).await.map_err(|error| ProxyError::Upstream(error.into()))?;
            }
            message = client.recv() => {
                let Some(message) = message else { return Ok(()); };
                let message = message.map_err(|error| ProxyError::BadRequest(error.into()))?;
                let data = match &message {
                    Message::Text(text) => Some(text.as_bytes()),
                    Message::Binary(bytes) => Some(bytes.as_ref()),
                    _ => None,
                };
                if inference && !unlimited && let Some(data) = data
                    && let Ok(value) = serde_json::from_slice::<serde_json::Value>(data)
                    && value.get("type").and_then(serde_json::Value::as_str) == Some("response.create") {
                        match outbound.check_admission(user, &admission).await {
                            Ok(()) => {},
                            Err(ProxyError::NoCredits) => {
                                tracing::warn!(error_kind = "insufficient_credits", "WebSocket request rejected");
                                let mut error = crate::proxy::quota_error();
                                error["type"] = "error".into();
                                error["status"] = 429.into();
                                client.send(Message::Text(error.to_string().into())).await.map_err(|error| ProxyError::Upstream(error.into()))?;
                                continue;
                            }
                            Err(error) => return Err(error),
                        }
                }
                if inference && let Some(data) = data
                    && let Ok(value) = serde_json::from_slice::<serde_json::Value>(data)
                    && value.get("type").and_then(serde_json::Value::as_str) == Some("response.create") {
                        tracker.request(&value);
                }
                let message = match message {
                    Message::Text(text) => tungstenite::Message::Text(text.as_str().to_owned().into()),
                    Message::Binary(bytes) => tungstenite::Message::Binary(bytes),
                    Message::Close(frame) => {
                        upstream.send(tungstenite::Message::Close(frame.map(|frame| tungstenite::protocol::CloseFrame {
                            code: frame.code.into(), reason: frame.reason.as_str().to_owned().into(),
                        }))).await.map_err(|error| ProxyError::Upstream(error.into()))?;
                        return Ok(());
                    }
                    _ => continue,
                };
                upstream.send(message).await.map_err(|error| ProxyError::Upstream(error.into()))?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        app::{App, Rule},
        ledger::Ledger,
        proxy::{
            InboundKind, OutboundKind,
            codex::{inbound::CodexInbound, stat::CodexObserver},
        },
    };
    use axum::{
        Router,
        extract::{Request, ws::WebSocketUpgrade},
        http::StatusCode,
    };
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    use tokio::sync::{Notify, Semaphore};

    struct Server {
        address: std::net::SocketAddr,
        task: tokio::task::JoinHandle<()>,
    }
    impl Drop for Server {
        fn drop(&mut self) {
            self.task.abort();
        }
    }
    async fn serve(router: Router) -> Server {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Server { address, task }
    }
    async fn app(upstream: &Server) -> (App, Ledger) {
        app_with_options(
            OutboundOptions::from(format!("http://{}/backend", upstream.address)),
            Credentials::bearer("upstream-secret", Some("upstream-account"))
                .unwrap()
                .into(),
        )
        .await
    }
    async fn app_with_options(
        options: OutboundOptions,
        credentials: CredentialManager,
    ) -> (App, Ledger) {
        let ledger = Ledger::in_memory(
            [(
                Arc::from("Alice"),
                WeeklyCredits::Limited(rust_decimal::Decimal::ONE),
            )]
            .into_iter()
            .collect(),
        )
        .await
        .unwrap();
        let outbound = CodexOutbound::new(
            "upstream".into(),
            options,
            credentials,
            Arc::new(CodexObserver::new(ledger.clone()).unwrap()),
        )
        .unwrap();
        let credentials = outbound.credentials.credentials().await.unwrap();
        let now = chrono::Utc::now().timestamp();
        outbound.window.lock().await.update(
            credentials.account_id(),
            Some(crate::ledger::WeeklyWindow {
                start: now - 3600,
                end: now + 601200,
            }),
        );
        let app = App {
            inbounds: vec![InboundKind::Codex(
                CodexInbound::new("route-only".into(), "Alice".into(), "local-secret".into())
                    .unwrap(),
            )],
            outbounds: vec![OutboundKind::Codex(outbound)],
            rules: vec![Rule {
                inbounds: vec!["route-only".into()],
                outbound: "upstream".into(),
            }],
            admission: Arc::new(ledger.clone()),
        };
        (app, ledger)
    }
    fn ws_request(proxy: &Server) -> tungstenite::handshake::client::Request {
        let mut req = format!("ws://{}/responses?x=1", proxy.address)
            .into_client_request()
            .unwrap();
        req.headers_mut().insert(
            header::AUTHORIZATION,
            "Bearer local-secret".parse().unwrap(),
        );
        req.headers_mut()
            .insert("chatgpt-account-id", "spoofed".parse().unwrap());
        req.headers_mut()
            .insert("x-custom", "preserved".parse().unwrap());
        req.headers_mut()
            .insert(header::SEC_WEBSOCKET_PROTOCOL, "codex".parse().unwrap());
        req
    }
    async fn next(socket: &mut UpstreamSocket) -> tungstenite::Message {
        tokio::time::timeout(Duration::from_secs(3), socket.next())
            .await
            .expect("relay timed out")
            .unwrap()
            .unwrap()
    }
    async fn create(socket: &mut UpstreamSocket) {
        socket
            .send(tungstenite::Message::Text(
                r#"{"type":"response.create","model":"gpt-6.1-sol"}"#.into(),
            ))
            .await
            .unwrap();
    }

    fn capture_requests(router: Router, capture: &crate::logging::tests::Capture) -> Router {
        let subscriber = capture.subscriber("warn,unicodex=debug");
        router.layer(axum::middleware::from_fn(
            move |req, next: axum::middleware::Next| {
                let subscriber = subscriber.clone();
                async move { next.run(req).await }.with_subscriber(subscriber)
            },
        ))
    }

    #[tokio::test]
    async fn limited_admission_discovers_bounds_and_status_shows_the_numeric_usage_bar() {
        let end = chrono::Utc::now().timestamp() + 601200;
        let upstream = serve(Router::new()
            .route("/product/wham/usage", axum::routing::get(move || async move {
                axum::Json(serde_json::json!({"plan_type":"plus", "rate_limit":{
                    "allowed":true,"limit_reached":false,
                    "secondary_window":{"used_percent":95,"limit_window_seconds":604800,"reset_at":end,"reset_after_seconds":601200}
                }}))
            }))
            .route("/model/responses", axum::routing::post(|| async {
                axum::Json(serde_json::json!({"usage":{"input_tokens":0,"output_tokens":1000}}))
            }))).await;
        let (app, ledger) = app_with_options(
            OutboundOptions {
                base_url: format!("http://{}/model", upstream.address),
                chatgpt_base_url: format!("http://{}/product", upstream.address),
                ..format!("http://{}/model", upstream.address).into()
            },
            Credentials::bearer("upstream-secret", Some("upstream-account"))
                .unwrap()
                .into(),
        )
        .await;
        let OutboundKind::Codex(outbound) = &app.outbounds[0];
        *outbound.window.lock().await = CachedWindow::default();
        for _ in 0..2 {
            let req = axum::http::Request::builder()
                .method("POST")
                .uri("/backend-api/codex/responses")
                .header(header::AUTHORIZATION, "Bearer local-secret")
                .body(Body::from(r#"{"model":"gpt-6.1-sol"}"#))
                .unwrap();
            let response = app.dispatch(req).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            response.into_body().collect().await.unwrap();
        }
        let req = axum::http::Request::builder()
            .uri("/backend-api/wham/usage")
            .header(header::AUTHORIZATION, "Bearer local-secret")
            .body(Body::empty())
            .unwrap();
        let response = app.dispatch(req).await.unwrap();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let status: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status["rate_limit"]["secondary_window"]["used_percent"], 50);
        assert_eq!(status["rate_limit"]["secondary_window"]["reset_at"], end);
        assert_eq!(ledger.entries().await.len(), 2);
    }

    #[tokio::test]
    async fn logs_adapters_rejections_and_upstream_status_without_secrets() {
        let upstream = serve(
            Router::new()
                .route(
                    "/wham/usage",
                    axum::routing::get(|| async {
                        axum::Json(serde_json::json!({"plan_type":"plus"}))
                    }),
                )
                .fallback(|| async {
                    (StatusCode::SERVICE_UNAVAILABLE, "hidden-response-payload")
                }),
        )
        .await;
        let (app, _) = app_with_options(
            OutboundOptions {
                mode: Mode::Strict,
                chatgpt_base_url: format!("http://{}", upstream.address),
                ..format!("http://{}", upstream.address).into()
            },
            Credentials::bearer("hidden-upstream-token", Some("hidden-account-id"))
                .unwrap()
                .into(),
        )
        .await;
        let capture = crate::logging::tests::Capture::default();
        for (path, status) in [
            ("/backend-api/wham/usage", 200),
            ("/greensfunction/backend-api/wham/usage", 404),
            ("/models", 503),
        ] {
            let request = Request::builder()
                .uri(format!("{path}?token=hidden-query"))
                .header("authorization", "Bearer local-secret")
                .header("x-private", "hidden-header")
                .body(Body::from("hidden-request-payload"))
                .unwrap();
            let response = app
                .dispatch(request)
                .with_subscriber(capture.subscriber("warn,unicodex=debug"))
                .await;
            use axum::response::IntoResponse;
            let response = response.into_response();
            assert_eq!(response.status().as_u16(), status);
            response.into_body().collect().await.unwrap();
        }
        let logs = capture.text();
        assert_eq!(logs.matches("response ready").count(), 3);
        assert!(logs.contains("rewrite=Some(Usage)"));
        assert!(logs.contains("unsupported_route"));
        assert!(logs.contains("upstream_status=503"));
        assert!(!logs.contains("hidden"));
        assert!(!logs.contains("local-secret"));
        for (status, level) in [(200, "INFO"), (404, "WARN"), (503, "ERROR")] {
            let line = logs
                .lines()
                .find(|line| {
                    line.contains("response ready") && line.contains(&format!("status={status}"))
                })
                .unwrap();
            assert!(line.contains(level));
            assert!(line.contains("user=\"Alice\""));
            assert!(line.contains("outbound=\"upstream\""));
        }
    }

    #[tokio::test]
    async fn dispatch_forwards_account_operations_and_only_gates_inference() {
        let count = Arc::new(AtomicUsize::new(0));
        let mut servers = Vec::new();
        for family in ["model", "product", "api", "auth"] {
            let calls = count.clone();
            servers.push(serve(Router::new().fallback(move |req: Request| {
                let calls = calls.clone(); async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.headers()[header::AUTHORIZATION], "Bearer upstream-secret");
                    assert_eq!(req.headers()["chatgpt-account-id"], "real-account");
                    axum::Json(match req.uri().path() {
                        "/product/wham/accounts/check" => serde_json::json!({
                            "accounts":[{"id":"real-account", "name":"Upstream workspace", "plan_type":"team", "workspace_backend_origin":"https://us.chatgpt.com/", "account_routing_override":"us"}],
                            "default_account_id":"real-account", "account_ordering":["real-account"]
                        }),
                        "/product/wham/usage" => serde_json::json!({"account_id":"real-account", "user_id":"real-user", "plan_type":"team", "credits":{"balance":"999", "has_credits":true, "unlimited":false},"rate_limit":{"secondary_window":{"used_percent":90,"limit_window_seconds":604800,"reset_at":chrono::Utc::now().timestamp()+601200,"reset_after_seconds":601200}}}),
                        _ => serde_json::json!({"family":family,"path":req.uri().path_and_query().unwrap().as_str()}),
                    })
                }
            })).await);
        }
        let options = OutboundOptions {
            mode: Mode::Strict,
            base_url: format!("http://{}/model", servers[0].address),
            chatgpt_base_url: format!("http://{}/product", servers[1].address),
            api_base_url: format!("http://{}/api", servers[2].address),
            auth_base_url: format!("http://{}/auth", servers[3].address),
        };
        let (app, ledger) = app_with_options(
            options,
            Credentials::bearer("upstream-secret", Some("real-account"))
                .unwrap()
                .into(),
        )
        .await;
        let req = |method: &str, uri: &str| {
            Request::builder()
                .method(method)
                .uri(uri)
                .header("authorization", "Bearer local-secret")
                .header("chatgpt-account-id", "Bob")
                .body(Body::empty())
                .unwrap()
        };
        for (method, path, family, destination) in [
            (
                "POST",
                "/backend-api/codex/responses?x=1",
                "model",
                "/model/responses?x=1",
            ),
            (
                "GET",
                "/backend-api/wham/tasks/list",
                "product",
                "/product/wham/tasks/list",
            ),
            ("POST", "/v1/live", "api", "/api/live"),
        ] {
            let response = app.dispatch(req(method, path)).await.unwrap();
            let value: serde_json::Value =
                serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                    .unwrap();
            assert_eq!(value["family"], family);
            assert_eq!(value["path"], destination);
        }
        assert!(matches!(
            app.dispatch(req("POST", "/custom")).await,
            Err(ProxyError::NoRoute)
        ));
        assert!(matches!(
            app.dispatch(req("DELETE", "/responses")).await,
            Err(ProxyError::NoRoute)
        ));
        ledger
            .record_charge(
                "Alice",
                crate::ledger::CreditAmount::from_decimal(rust_decimal::Decimal::ONE).unwrap(),
            )
            .await
            .unwrap();
        ledger
            .record_charge(
                "Bob",
                crate::ledger::CreditAmount::from_decimal(rust_decimal::Decimal::from(999))
                    .unwrap(),
            )
            .await
            .unwrap();
        // Codex selects its saved workspace ID, but that ID cannot select a ledger user.
        for (requested_id, expected_id) in [
            (Some("Bob"), "Bob"),
            (None, "real-account"),
            (Some(""), "real-account"),
        ] {
            let mut request = req("GET", "/backend-api/wham/accounts/check");
            request.headers_mut().remove("chatgpt-account-id");
            if let Some(id) = requested_id {
                request
                    .headers_mut()
                    .insert("chatgpt-account-id", id.parse().unwrap());
            }
            let response = app.dispatch(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let value: serde_json::Value =
                serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                    .unwrap();
            assert_eq!(value["accounts"][0]["id"], expected_id);
            assert_eq!(value["accounts"][0]["name"], "Upstream workspace");
            assert_eq!(value["accounts"][0]["plan_type"], "team");
            assert_eq!(
                value["accounts"][0]["workspace_backend_origin"],
                "NO_CONSTRAINT"
            );
            assert_eq!(value["accounts"][0]["account_routing_override"], "us");
            assert_eq!(value["default_account_id"], expected_id);
            assert_eq!(value["account_ordering"], serde_json::json!([expected_id]));
        }
        for path in ["/backend-api/wham/usage", "/api/codex/usage"] {
            let response = app.dispatch(req("GET", path)).await.unwrap();
            let value: serde_json::Value =
                serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                    .unwrap();
            assert_eq!(value["account_id"], "real-account");
            assert_eq!(value["plan_type"], "team");
            assert!(value["credits"].is_null());
        }
        for (method, path, family, destination) in [
            (
                "GET",
                "/backend-api/wham/profiles/me",
                "product",
                "/product/wham/profiles/me",
            ),
            (
                "GET",
                "/backend-api/wham/usage/daily-token-usage-breakdown",
                "product",
                "/product/wham/usage/daily-token-usage-breakdown",
            ),
            (
                "GET",
                "/backend-api/wham/rate-limit-reset-credits",
                "product",
                "/product/wham/rate-limit-reset-credits",
            ),
            (
                "POST",
                "/backend-api/wham/rate-limit-reset-credits/consume",
                "product",
                "/product/wham/rate-limit-reset-credits/consume",
            ),
            ("POST", "/auth/oauth/token", "auth", "/auth/oauth/token"),
            (
                "POST",
                "/backend-api/wham/remote/control/server/enroll",
                "product",
                "/product/wham/remote/control/server/enroll",
            ),
            (
                "POST",
                "/v1/analytics/codex/turn-costs",
                "api",
                "/api/analytics/codex/turn-costs",
            ),
        ] {
            let response = app.dispatch(req(method, path)).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let value: serde_json::Value =
                serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                    .unwrap();
            assert_eq!(
                value,
                serde_json::json!({"family":family,"path":destination})
            );
        }
        let before = count.load(Ordering::SeqCst);
        use axum::response::IntoResponse;
        let denied = app
            .dispatch(req("POST", "/responses"))
            .await
            .into_response();
        assert_eq!(denied.status(), StatusCode::TOO_MANY_REQUESTS);
        let value: serde_json::Value =
            serde_json::from_slice(&denied.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(value["error"]["code"], "insufficient_quota");
        // Reset consumption invalidated the cache: only window discovery is forwarded.
        assert_eq!(count.load(Ordering::SeqCst), before + 1);
    }

    #[tokio::test]
    async fn upstream_401_refreshes_without_replaying_the_streamed_request() {
        let count = Arc::new(AtomicUsize::new(0));
        let calls = count.clone();
        let upstream = serve(Router::new().fallback(move |req: Request| {
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                if req.headers()[header::AUTHORIZATION] == "Bearer old" {
                    StatusCode::UNAUTHORIZED
                } else {
                    StatusCode::OK
                }
            }
        }))
        .await;
        let count_refresh = Arc::new(AtomicUsize::new(0));
        let calls = count_refresh.clone();
        let auth = serve(Router::new().fallback(move || {
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                axum::Json(serde_json::json!({"access_token":"new", "refresh_token":"rotated"}))
            }
        }))
        .await;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("auth.json");
        std::fs::write(&path, serde_json::to_vec(&serde_json::json!({"tokens":{"access_token":"old","refresh_token":"real-refresh"},"last_refresh":chrono::Utc::now().to_rfc3339()})).unwrap()).unwrap();
        let manager =
            CredentialManager::from_file(&path, &format!("http://{}", auth.address)).unwrap();
        let (app, _) =
            app_with_options(format!("http://{}", upstream.address).into(), manager).await;
        let req = || {
            Request::builder()
                .method("POST")
                .uri("/responses")
                .header("authorization", "Bearer local-secret")
                .body(Body::from_stream(futures_util::stream::iter([Ok::<
                    _,
                    std::io::Error,
                >(
                    "body"
                )])))
                .unwrap()
        };
        assert!(matches!(
            app.dispatch(req()).await,
            Err(ProxyError::UpstreamAuth(_))
        ));
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(app.dispatch(req()).await.unwrap().status(), StatusCode::OK);
        assert_eq!(count.load(Ordering::SeqCst), 2);
        assert_eq!(count_refresh.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn http_forwards_path_body_and_headers_with_upstream_credentials() {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let upstream = serve(Router::new().fallback(move |req: Request| {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                assert_eq!(
                    req.uri().path_and_query().unwrap().as_str(),
                    "/backend/responses?x=1&x=2"
                );
                assert_eq!(
                    req.headers()[header::AUTHORIZATION],
                    "Bearer upstream-secret"
                );
                assert_eq!(req.headers()["chatgpt-account-id"], "upstream-account");
                assert_eq!(req.headers()["x-custom"], "preserved");
                assert!(!req.headers().contains_key("x-hop"));
                assert_eq!(req.headers()[header::ACCEPT_ENCODING], "identity");
                assert_eq!(
                    req.into_body().collect().await.unwrap().to_bytes().as_ref(),
                    b"request body"
                );
                Response::builder()
                    .status(StatusCode::CREATED)
                    .header("content-type", "application/json")
                    .header("x-custom", "response")
                    .body(Body::from(
                        r#"{"model":"gpt-6.1-sol","usage":{"input_tokens":0,"output_tokens":4000}}"#,
                    ))
                    .unwrap()
            }
        }))
        .await;
        let (app, ledger) = app(&upstream).await;
        let request = || {
            Request::builder()
                .method("POST")
                .uri("/responses?x=1&x=2")
                .header("authorization", "Bearer local-secret")
                .header("chatgpt-account-id", "spoofed")
                .header("x-custom", "preserved")
                .header("connection", "x-hop")
                .header("x-hop", "remove")
                .header("accept-encoding", "gzip")
                .body(Body::from("request body"))
                .unwrap()
        };
        let response = app.dispatch(request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(response.headers()["x-custom"], "response");
        assert_eq!(
            response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .as_ref(),
            br#"{"model":"gpt-6.1-sol","usage":{"input_tokens":0,"output_tokens":4000}}"#
        );
        assert_eq!(ledger.entries().await[0].user, "Alice");
        assert!(matches!(
            app.dispatch(request()).await,
            Err(ProxyError::NoCredits)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn websocket_rechecks_credits_keeps_running_work_and_persists_before_delivery() {
        let capture = crate::logging::tests::Capture::default();
        let calls = Arc::new(AtomicUsize::new(0));
        let handshakes = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let finish = Arc::new(Semaphore::new(0));
        let (count, opened, notify, release) = (
            calls.clone(),
            handshakes.clone(),
            started.clone(),
            finish.clone(),
        );
        let upstream = serve(Router::new().fallback(move |upgrade: WebSocketUpgrade, req: Request| {
            let (count, opened, notify, release) = (count.clone(), opened.clone(), notify.clone(), release.clone());
            async move {
                assert_eq!(req.uri().path_and_query().unwrap().as_str(), "/backend/responses?x=1");
                assert_eq!(req.headers()[header::AUTHORIZATION], "Bearer upstream-secret");
                assert_eq!(req.headers()["chatgpt-account-id"], "upstream-account");
                assert_eq!(req.headers()["x-custom"], "preserved");
                opened.fetch_add(1, Ordering::SeqCst);
                let mut response = upgrade.protocols(["codex"]).on_upgrade(move |mut socket| async move {
                    while let Some(Ok(message)) = socket.recv().await {
                        if !matches!(message, Message::Text(_) | Message::Binary(_)) { continue; }
                        let ordinal = count.fetch_add(1, Ordering::SeqCst);
                        if ordinal == 0 {
                            notify.notify_one();
                            release.acquire().await.unwrap().forget();
                        }
                        if socket.send(Message::Text(r#"{"type":"response.completed","response":{"usage":{"input_tokens":3,"output_tokens":0}}}"#.into())).await.is_err() { break; }
                    }
                });
                response.headers_mut().insert("x-codex-credits-balance", "7.5".parse().unwrap());
                response.headers_mut().insert("x-custom", "handshake".parse().unwrap());
                response
            }
        })).await;
        let (app, ledger) = app(&upstream).await;
        let proxy = serve(capture_requests(app.router(), &capture)).await;
        let (mut client, handshake) = tokio_tungstenite::connect_async(ws_request(&proxy))
            .await
            .unwrap();
        assert_eq!(handshake.headers()["x-custom"], "handshake");
        assert_eq!(handshake.headers()[header::SEC_WEBSOCKET_PROTOCOL], "codex");
        assert!(
            ledger.entries().await.is_empty(),
            "handshakes are not spending"
        );
        create(&mut client).await;
        tokio::time::timeout(Duration::from_secs(3), started.notified())
            .await
            .unwrap();
        ledger
            .record_charge(
                "Alice",
                crate::ledger::CreditAmount::from_decimal(rust_decimal::Decimal::ONE).unwrap(),
            )
            .await
            .unwrap();
        create(&mut client).await;
        let denied = next(&mut client).await;
        let error: serde_json::Value = serde_json::from_str(denied.to_text().unwrap()).unwrap();
        assert_eq!(error["type"], "error");
        assert_eq!(error["status"], 429);
        assert_eq!(error["error"]["code"], "insufficient_quota");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        finish.add_permits(1);
        let completed = next(&mut client).await;
        assert!(completed.to_text().unwrap().contains("response.completed"));
        let entries = ledger.entries().await;
        assert_eq!(
            entries.len(),
            2,
            "charge must be stored before completion delivery"
        );
        assert!(
            entries
                .iter()
                .any(|entry| entry.user == "Alice" && entry.credits == 150_000)
        );
        // Initial admission also prevents opening any upstream WebSocket.
        let denied = tokio_tungstenite::connect_async(ws_request(&proxy))
            .await
            .unwrap_err();
        assert!(
            matches!(denied, tungstenite::Error::Http(response) if response.status() == StatusCode::TOO_MANY_REQUESTS)
        );
        assert_eq!(handshakes.load(Ordering::SeqCst), 1);
        ledger
            .execute("DELETE FROM credit_entries WHERE credits = 1000000000")
            .await;
        client
            .send(tungstenite::Message::Binary(
                axum::body::Bytes::from_static(
                    br#"{"type":"response.create","model":"gpt-6.1-sol"}"#,
                ),
            ))
            .await
            .unwrap();
        assert!(
            next(&mut client)
                .await
                .to_text()
                .unwrap()
                .contains("response.completed")
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        // Accounting failure preserves completion and does not close the session.
        ledger.execute("CREATE TRIGGER reject_charge BEFORE INSERT ON credit_entries BEGIN SELECT RAISE(ABORT, 'database unavailable'); END").await;
        create(&mut client).await;
        assert!(
            next(&mut client)
                .await
                .to_text()
                .unwrap()
                .contains("response.completed")
        );
        assert_eq!(ledger.entries().await.len(), 2);
        let (mut another, _) = tokio_tungstenite::connect_async(ws_request(&proxy))
            .await
            .unwrap();
        another.close(None).await.unwrap();
        let logs = capture.text();
        let started = logs
            .lines()
            .find(|line| line.contains("WebSocket relay started"))
            .unwrap();
        let failed = logs
            .lines()
            .find(|line| line.contains("record_charge_failed"))
            .unwrap();
        assert!(failed.contains("WARN"));
        assert_eq!(
            crate::logging::tests::request_id(started),
            crate::logging::tests::request_id(failed)
        );
        assert!(logs.contains("WebSocket request rejected"));
        assert!(logs.contains("record_charge"));
        for secret in [
            "local-secret",
            "upstream-secret",
            "upstream-account",
            "response.completed",
            "database unavailable",
        ] {
            assert!(!logs.contains(secret), "leaked {secret}");
        }
    }

    #[tokio::test]
    async fn websocket_credit_read_failure_prevents_new_inference() {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let upstream = serve(Router::new().fallback(move |upgrade: WebSocketUpgrade| {
            let count = count.clone();
            async move {
                upgrade
                    .protocols(["codex"])
                    .on_upgrade(move |mut socket| async move {
                        while let Some(Ok(message)) = socket.recv().await {
                            if matches!(message, Message::Text(_) | Message::Binary(_)) {
                                count.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                    })
            }
        }))
        .await;
        let (app, ledger) = app(&upstream).await;
        let proxy = serve(app.router()).await;
        let (mut client, _) = tokio_tungstenite::connect_async(ws_request(&proxy))
            .await
            .unwrap();
        ledger.execute("DROP TABLE credit_entries").await;
        create(&mut client).await;
        assert!(
            matches!(next(&mut client).await, tungstenite::Message::Close(Some(frame)) if u16::from(frame.code) == 1011)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn rejected_websocket_handshake_preserves_status_without_recording_a_charge() {
        let upstream = serve(Router::new().fallback(|| async {
            Response::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .header("x-codex-credits-balance", "0")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"error":"upstream limit"}"#))
                .unwrap()
        }))
        .await;
        let (app, ledger) = app(&upstream).await;
        let proxy = serve(app.router()).await;
        let rejected = tokio_tungstenite::connect_async(ws_request(&proxy))
            .await
            .unwrap_err();
        match rejected {
            tungstenite::Error::Http(response) => {
                assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
                assert_eq!(
                    response.body().as_deref(),
                    Some(br#"{"error":"upstream limit"}"#.as_slice())
                );
            }
            error => panic!("unexpected handshake failure: {error}"),
        }
        assert!(ledger.entries().await.is_empty());
    }
    #[tokio::test]
    async fn consecutive_limited_http_requests_without_response_metadata_are_charged() {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let upstream = serve(Router::new().fallback(move |req: Request| {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                let compressed = req.headers().get("content-encoding").is_some_and(|v| v == "zstd");
                let bytes = req.into_body().collect().await.unwrap().to_bytes();
                let decoded = if compressed { zstd::decode_all(&bytes[..]).unwrap() } else { bytes.to_vec() };
                let request: serde_json::Value = serde_json::from_slice(&decoded).unwrap();
                assert!(request.get("model").is_some());
                let bytes = b"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"same-id-across-http-requests\",\"usage\":{\"input_tokens\":0,\"output_tokens\":1,\"input_tokens_details\":null}}}\n\n";
                let chunks: Vec<_> = bytes.chunks(1).map(|v| Ok::<_, std::io::Error>(axum::body::Bytes::copy_from_slice(v))).collect();
                Response::builder().header("content-type", "text/event-stream").body(Body::from_stream(futures_util::stream::iter(chunks))).unwrap()
            }
        })).await;
        let (app, ledger) = app(&upstream).await;
        for (model, compressed) in [
            ("gpt-6.1-sol", false),
            ("gpt-6.1-sol", true),
            ("unknown", false),
            ("gpt-6.1-sol", false),
        ] {
            let bytes = serde_json::json!({"model":model,"stream":true,"input":[]})
                .to_string()
                .into_bytes();
            let bytes = if compressed {
                zstd::encode_all(&bytes[..], 1).unwrap()
            } else {
                bytes
            };
            let request = Request::builder()
                .method("POST")
                .uri("/responses")
                .header("authorization", "Bearer local-secret")
                .header(
                    "content-encoding",
                    if compressed { "zstd" } else { "identity" },
                )
                .body(Body::from(bytes))
                .unwrap();
            let response = app.dispatch(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            assert!(
                std::str::from_utf8(&bytes)
                    .unwrap()
                    .contains("response.completed")
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        let entries = ledger.entries().await;
        assert_eq!(
            entries.len(),
            3,
            "unpriceable completions are skipped, never poison later requests"
        );
        assert!(
            entries
                .iter()
                .all(|v| v.user == "Alice" && v.credits == 250_000)
        );
    }

    #[tokio::test]
    async fn consecutive_limited_websocket_requests_survive_warmups_and_missing_metadata() {
        let upstream = serve(Router::new().fallback(|upgrade: WebSocketUpgrade| async move {
            upgrade.protocols(["codex"]).on_upgrade(|mut socket| async move {
                let mut ordinal = 0;
                while let Some(Ok(Message::Text(bytes))) = socket.recv().await {
                    let request: serde_json::Value = serde_json::from_str(&bytes).unwrap();
                    ordinal += 1;
                    let id = format!("response-{ordinal}");
                    let created = serde_json::json!({"type":"response.created","response":{"id":id}});
                    socket.send(Message::Text(created.to_string().into())).await.unwrap();
                    let response = if request.get("generate") == Some(&serde_json::Value::Bool(false)) {
                        serde_json::json!({"id":id,"output":[]})
                    } else {
                        serde_json::json!({"id":id,"usage":{"input_tokens":0,"output_tokens":1}})
                    };
                    let completed = serde_json::json!({"type":"response.completed","response":response});
                    if socket.send(Message::Text(completed.to_string().into())).await.is_err() { break; }
                }
            })
        })).await;
        let (app, ledger) = app(&upstream).await;
        let proxy = serve(app.router()).await;
        let (mut client, _) = tokio_tungstenite::connect_async(ws_request(&proxy))
            .await
            .unwrap();
        client
            .send(tungstenite::Message::Text(
                r#"{"type":"response.create","model":"gpt-6.1-sol","generate":false}"#.into(),
            ))
            .await
            .unwrap();
        assert!(
            next(&mut client)
                .await
                .to_text()
                .unwrap()
                .contains("response.created")
        );
        assert!(
            next(&mut client)
                .await
                .to_text()
                .unwrap()
                .contains("response.completed")
        );
        assert!(ledger.entries().await.is_empty());
        for _ in 0..2 {
            create(&mut client).await;
            assert!(
                next(&mut client)
                    .await
                    .to_text()
                    .unwrap()
                    .contains("response.created")
            );
            assert!(
                next(&mut client)
                    .await
                    .to_text()
                    .unwrap()
                    .contains("response.completed")
            );
        }
        let entries = ledger.entries().await;
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|v| v.credits == 250_000));
        client.close(None).await.unwrap();
    }
}
