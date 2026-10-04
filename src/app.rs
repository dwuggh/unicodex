use std::sync::Arc;

use axum::{
    Router,
    extract::{Request, State},
    response::Response,
};
use tower::ServiceExt;
use tracing::Instrument;

use crate::ledger::Ledger;
use crate::proxy::{Inbound, InboundKind, OutboundInput, OutboundKind, ProxyError, Tag};

pub struct Rule {
    pub inbounds: Vec<String>,
    pub outbound: String,
}

pub struct App {
    pub inbounds: Vec<InboundKind>,
    pub outbounds: Vec<OutboundKind>,
    pub rules: Vec<Rule>,
    pub admission: Arc<Ledger>,
}

impl App {
    pub fn router(self) -> Router {
        Router::new().fallback(run).with_state(Arc::new(self))
    }

    pub async fn dispatch(&self, req: Request) -> Result<Response, ProxyError> {
        let span = crate::logging::request_span(&req);
        async {
            let started = std::time::Instant::now();
            tracing::debug!("request received");
            let result = self.dispatch_inner(req).await;
            crate::logging::response_ready(&result, started.elapsed());
            result.map(crate::logging::observe_body)
        }
        .instrument(span)
        .await
    }

    async fn dispatch_inner(&self, req: Request) -> Result<Response, ProxyError> {
        for rule in &self.rules {
            for tag in &rule.inbounds {
                let inbound = self
                    .inbounds
                    .iter()
                    .find(|inbound| inbound.tag() == tag)
                    .ok_or_else(|| anyhow::anyhow!("unknown inbound: {tag}"))?;
                if !inbound.accept(&req) {
                    continue;
                }
                let outbound = self
                    .outbounds
                    .iter()
                    .find(|outbound| outbound.tag() == rule.outbound)
                    .ok_or_else(|| anyhow::anyhow!("unknown outbound: {}", rule.outbound))?;
                // Selection is final: authentication or admission failure never falls through.
                tracing::Span::current().record("inbound", tracing::field::debug(inbound.tag()));
                tracing::Span::current().record("outbound", tracing::field::debug(outbound.tag()));
                let exchange = inbound.clone().oneshot(req).await?;
                tracing::Span::current().record("user", tracing::field::debug(&exchange.user));
                tracing::debug!("inbound selected");
                return outbound
                    .clone()
                    .oneshot(OutboundInput {
                        exchange,
                        admission: self.admission.clone(),
                    })
                    .await;
            }
        }
        tracing::debug!(reason = "no_matching_inbound", "request rejected");
        Err(ProxyError::NoRoute)
    }
}

async fn run(State(app): State<Arc<App>>, req: Request) -> Result<Response, ProxyError> {
    app.dispatch(req).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxy::codex::{
        auth::Credentials,
        inbound::CodexInbound,
        outbound::{CodexOutbound, OutboundOptions},
        stat::CodexObserver,
    };
    use axum::body::Body;
    use sqlx::sqlite::SqlitePoolOptions;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    use tokio::sync::Barrier;
    use tracing::instrument::WithSubscriber;

    struct Server(tokio::task::JoinHandle<()>);
    impl Drop for Server {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    struct Fixture {
        app: App,
        ledger: Ledger,
        calls: Arc<AtomicUsize>,
        server: Server,
    }

    async fn setup(barrier: Option<Arc<Barrier>>) -> Fixture {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let ledger = Ledger::from_pool(
            pool,
            ["Alice", "Bob"]
                .into_iter()
                .map(|user| {
                    (
                        Arc::from(user),
                        crate::ledger::WeeklyCredits::Limited(rust_decimal::Decimal::ONE),
                    )
                })
                .collect(),
        )
        .await
        .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let upstream = Router::new().fallback(move |req: Request| {
            let count = count.clone();
            let barrier = barrier.clone();
            async move {
                if req.uri().path() == "/wham/usage" {
                    return axum::Json(serde_json::json!({"rate_limit":{"secondary_window":{
                        "limit_window_seconds":604800,"reset_at":chrono::Utc::now().timestamp()+601200
                    }}}));
                }
                count.fetch_add(1, Ordering::SeqCst);
                if let Some(barrier) = barrier {
                    barrier.wait().await;
                }
                axum::Json(serde_json::json!({"usage":{"input_tokens":1}}))
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = Server(tokio::spawn(async move {
            axum::serve(listener, upstream).await.unwrap();
        }));
        let app = App {
            inbounds: vec![
                InboundKind::Codex(
                    CodexInbound::new("route-a".into(), "Alice".into(), "alice-key".into())
                        .unwrap(),
                ),
                InboundKind::Codex(
                    CodexInbound::new("route-b".into(), "Bob".into(), "bob-key".into()).unwrap(),
                ),
            ],
            outbounds: vec![OutboundKind::Codex(
                CodexOutbound::new(
                    "upstream".into(),
                    OutboundOptions {
                        chatgpt_base_url: format!("http://{address}"),
                        ..format!("http://{address}").into()
                    },
                    Credentials::bearer("upstream-key", None).unwrap().into(),
                    Arc::new(CodexObserver::new(ledger.clone())),
                )
                .unwrap(),
            )],
            rules: vec![Rule {
                inbounds: vec!["route-a".into(), "route-b".into()],
                outbound: "upstream".into(),
            }],
            admission: Arc::new(ledger.clone()),
        };
        Fixture {
            app,
            ledger,
            calls,
            server,
        }
    }

    fn request(key: &str) -> Request {
        Request::builder()
            .method("POST")
            .uri("/responses")
            .header("authorization", format!("Bearer {key}"))
            .body(Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn concrete_enums_dispatch_concurrently_and_record_the_authenticated_users() {
        use http_body_util::BodyExt;
        let capture = crate::logging::tests::Capture::default();
        let Fixture {
            app,
            ledger,
            calls,
            server: _server,
        } = setup(Some(Arc::new(Barrier::new(2)))).await;
        let dispatch = |req| async {
            app.dispatch(req)
                .await
                .unwrap()
                .into_body()
                .collect()
                .await
                .unwrap()
        };
        let results = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(dispatch(request("alice-key")), dispatch(request("bob-key")))
        })
        .with_subscriber(capture.subscriber("warn,unicodex=debug"))
        .await
        .expect("requests must not serialize behind an application lock");
        assert_eq!(results.0.to_bytes(), results.1.to_bytes());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let users: Vec<String> = sqlx::query_scalar("SELECT user FROM observations ORDER BY user")
            .fetch_all(&ledger.pool)
            .await
            .unwrap();
        assert_eq!(users, ["Alice", "Bob"]);
        let logs = capture.text();
        let summaries: Vec<_> = logs
            .lines()
            .filter(|line| line.contains("response ready"))
            .collect();
        assert_eq!(summaries.len(), 2, "{logs}");
        let mut ids = std::collections::HashSet::new();
        for (user, route) in [("Alice", "route-a"), ("Bob", "route-b")] {
            let line = summaries
                .iter()
                .find(|line| line.contains(&format!("user=\"{user}\"")))
                .unwrap();
            assert!(line.contains(&format!("inbound=\"{route}\"")));
            assert!(line.contains("INFO"));
            assert!(line.contains("status=200"));
            assert!(line.contains("response_ready_ms="));
            let id = crate::logging::tests::request_id(line);
            ids.insert(id);
            assert!(
                logs.lines()
                    .any(|line| line.contains("response body finished")
                        && crate::logging::tests::request_id(line) == id)
            );
        }
        assert_eq!(ids.len(), 2);
        assert!(!logs.contains("alice-key"));
        assert!(!logs.contains("bob-key"));
    }

    #[tokio::test]
    async fn rejection_never_dispatches_and_selected_inbound_failure_does_not_fall_through() {
        let capture = crate::logging::tests::Capture::default();
        let Fixture {
            mut app,
            ledger,
            calls,
            server: _server,
        } = setup(None).await;
        ledger
            .record(
                "Alice",
                Some(serde_json::json!({"usage_metadata":{"amount":"1"}})),
                None,
            )
            .await
            .unwrap();
        assert!(matches!(
            app.dispatch(request("alice-key"))
                .with_subscriber(capture.subscriber("warn,unicodex=debug"))
                .await,
            Err(ProxyError::NoCredits)
        ));
        assert!(matches!(
            app.dispatch(request("unknown-key"))
                .with_subscriber(capture.subscriber("warn,unicodex=debug"))
                .await,
            Err(ProxyError::NoRoute)
        ));
        sqlx::query("DROP TABLE observations")
            .execute(&ledger.pool)
            .await
            .unwrap();
        assert!(matches!(
            app.dispatch(request("alice-key"))
                .with_subscriber(capture.subscriber("warn,unicodex=debug"))
                .await,
            Err(ProxyError::Internal(_))
        ));
        // A malformed upgrade fails in the selected real inbound. Continuing to the
        // next route would instead report its missing inbound as an internal error.
        app.rules[0].inbounds.push("missing-inbound".into());
        let mut invalid = request("alice-key");
        invalid
            .headers_mut()
            .insert("upgrade", "websocket".parse().unwrap());
        assert!(matches!(
            app.dispatch(invalid).await,
            Err(ProxyError::BadRequest(_))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let logs = capture.text();
        for status in [429, 404, 500] {
            let line = logs
                .lines()
                .find(|line| line.contains(&format!("status={status}")))
                .unwrap();
            assert!(line.contains(if status == 500 { "ERROR" } else { "WARN" }));
            assert!(line.contains("request_id="));
        }
        assert!(logs.contains("no_matching_inbound"));
        assert!(logs.contains("read_consumption"));
        assert!(!logs.contains("unknown-key"));
    }
}
