//! Operational logging. Never format request headers, payloads, or error sources.

use std::{
    io::IsTerminal,
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use axum::{body::Body, extract::Request, http::StatusCode, response::Response};
use hyper::body::{Body as HttpBody, Frame, SizeHint};
use tracing::{Dispatch, Span};
use tracing_subscriber::EnvFilter;

use crate::proxy::ProxyError;

const DEFAULT_FILTER: &str = "warn,unicodex=info";
static NEXT_REQUEST: AtomicU64 = AtomicU64::new(1);

fn filter(value: Option<&str>) -> (EnvFilter, bool) {
    match value.filter(|value| !value.trim().is_empty()) {
        Some(value) => match EnvFilter::try_new(value) {
            Ok(filter) => (filter, false),
            Err(_) => (EnvFilter::new(DEFAULT_FILTER), true),
        },
        None => (EnvFilter::new(DEFAULT_FILTER), false),
    }
}

/// Initialize the binary's subscriber; library users can install their own.
pub fn init() {
    let value = std::env::var("RUST_LOG");
    let (filter, invalid) = filter(value.as_deref().ok());
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal())
        .init();
    if invalid || matches!(value, Err(std::env::VarError::NotUnicode(_))) {
        tracing::warn!("invalid RUST_LOG; using warn,unicodex=info");
    }
}

pub(crate) fn request_span(req: &Request) -> Span {
    // This is context, not an ERROR event. Keep it enabled at warning/error-only
    // verbosity too, so failures retain their request identity. Span lifecycle
    // events are disabled by the formatter's default configuration.
    tracing::error_span!(
        "request",
        request_id = NEXT_REQUEST.fetch_add(1, Ordering::Relaxed),
        method = %req.method(),
        http_version = ?req.version(),
        path = ?req.uri().path(),
        user = tracing::field::Empty,
        inbound = tracing::field::Empty,
        outbound = tracing::field::Empty,
    )
}

pub(crate) fn response_ready(result: &Result<Response, ProxyError>, elapsed: Duration) {
    if let Err(ProxyError::BadRequest(error)) = result
        && let Some(rejection) =
            error.downcast_ref::<axum::extract::ws::rejection::WebSocketUpgradeRejection>()
    {
        // Axum's upgrade rejection messages are fixed descriptions of the failed
        // protocol check. They never contain header values or request payloads.
        // Do not apply this formatting to arbitrary anyhow/transport errors.
        tracing::warn!(reason = %rejection, "WebSocket handshake rejected");
    }
    let (status, error_kind) = match result {
        Ok(response) => (response.status(), None),
        Err(error) => (error.status(), Some(error.kind())),
    };
    let response_ready_ms = elapsed.as_secs_f64() * 1000.0;
    if status.is_server_error() {
        tracing::error!(
            status = status.as_u16(),
            response_ready_ms,
            error_kind,
            "response ready"
        );
    } else if status.is_client_error() {
        tracing::warn!(
            status = status.as_u16(),
            response_ready_ms,
            error_kind,
            "response ready"
        );
    } else {
        tracing::info!(
            status = status.as_u16(),
            response_ready_ms,
            "response ready"
        );
    }
}

pub(crate) fn observe_body(response: Response) -> Response {
    // A 101 hands its lifetime to the WebSocket relay, not the HTTP body.
    if response.status() == StatusCode::SWITCHING_PROTOCOLS {
        return response;
    }
    let (parts, inner) = response.into_parts();
    let finished = inner.is_end_stream();
    if finished {
        tracing::debug!(body_duration_ms = 0.0, "response body finished");
    }
    let body = LoggedBody {
        inner,
        span: Span::current(),
        subscriber: tracing::dispatcher::get_default(Clone::clone),
        started: Instant::now(),
        finished,
    };
    Response::from_parts(parts, Body::new(body))
}

// Poll the original body directly: no buffering, no frame/trailer changes, and
// no boxed futures. The dispatcher also preserves isolated test subscribers.
struct LoggedBody {
    inner: Body,
    span: Span,
    subscriber: Dispatch,
    started: Instant,
    finished: bool,
}

impl HttpBody for LoggedBody {
    type Data = axum::body::Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        tracing::dispatcher::with_default(&this.subscriber, || {
            this.span.in_scope(|| {
                let result = Pin::new(&mut this.inner).poll_frame(cx);
                if !this.finished {
                    let body_duration_ms = this.started.elapsed().as_secs_f64() * 1000.0;
                    if matches!(&result, Poll::Ready(Some(Err(_)))) {
                        this.finished = true;
                        tracing::error!(body_duration_ms, "response body failed");
                    } else if matches!(&result, Poll::Ready(None)) || this.inner.is_end_stream() {
                        this.finished = true;
                        tracing::debug!(body_duration_ms, "response body finished");
                    }
                }
                result
            })
        })
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl Drop for LoggedBody {
    fn drop(&mut self) {
        if !self.finished {
            tracing::dispatcher::with_default(&self.subscriber, || {
                self.span.in_scope(|| {
                    tracing::debug!(
                        body_duration_ms = self.started.elapsed().as_secs_f64() * 1000.0,
                        "response body dropped before completion"
                    );
                });
            });
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::{
        io,
        sync::{Arc, Mutex},
    };

    #[derive(Clone, Default)]
    pub(crate) struct Capture(Arc<Mutex<Vec<u8>>>);

    impl io::Write for Capture {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Capture {
        pub(crate) fn subscriber(&self, filter: &str) -> Dispatch {
            let writer = self.clone();
            Dispatch::new(
                tracing_subscriber::fmt()
                    .with_env_filter(EnvFilter::new(filter))
                    .with_writer(move || writer.clone())
                    .with_ansi(false)
                    .without_time()
                    .finish(),
            )
        }

        pub(crate) fn text(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    pub(crate) fn request_id(line: &str) -> &str {
        line.split("request_id=")
            .nth(1)
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
    }

    #[tokio::test]
    async fn late_sse_accounting_failure_retains_context_and_preserves_response() {
        use crate::{
            ledger::Ledger,
            proxy::{Observer, codex::stat::CodexObserver},
        };
        use http_body_util::BodyExt;
        use tracing::{Instrument, instrument::WithSubscriber};

        let ledger = Ledger::in_memory(
            [(
                std::sync::Arc::from("Alice"),
                crate::ledger::WeeklyCredits::Limited(rust_decimal::Decimal::ONE),
            )]
            .into_iter()
            .collect(),
        )
        .await
        .unwrap();
        let observer = Arc::new(CodexObserver::new(ledger.clone()).unwrap());
        let capture = Capture::default();
        let response = async {
            let req = Request::builder()
                .uri("/responses?hidden-query")
                .body(Body::empty())
                .unwrap();
            async {
                let response = Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(Body::from(
                        "data: {\"model\":\"gpt-6.1-sol\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1},\"output\":\"hidden-payload\"}\n\n",
                    ))
                    .unwrap();
                let result = observer
                    .observe_http("Alice".into(), response)
                    .await
                    .map_err(ProxyError::Internal);
                response_ready(&result, Duration::ZERO);
                observe_body(result.unwrap())
            }
            .instrument(request_span(&req))
            .await
        }
        .with_subscriber(capture.subscriber("warn,unicodex=debug"))
        .await;
        assert!(!capture.text().contains("response body failed"));
        ledger.execute("DROP TABLE credit_entries").await;
        // No subscriber is installed here; the body must retain its own context.
        assert!(response.into_body().collect().await.is_ok());
        let logs = capture.text();
        let ready = logs
            .lines()
            .find(|line| line.contains("response ready"))
            .unwrap();
        let failed = logs
            .lines()
            .find(|line| line.contains("record_charge_failed"))
            .unwrap();
        assert!(ready.contains("status=200"));
        assert!(failed.contains("WARN"));
        assert_eq!(request_id(ready), request_id(failed));
        assert!(logs.contains("record_charge"));
        assert!(!logs.contains("hidden"));
    }

    #[tokio::test]
    async fn body_logging_preserves_frames_errors_and_size_hints() {
        use axum::{body::Bytes, http::HeaderMap};
        use http_body_util::{BodyExt, StreamBody};

        let capture = Capture::default();
        let req = Request::builder()
            .uri("/responses")
            .body(Body::empty())
            .unwrap();
        let wrap = |body| {
            tracing::dispatcher::with_default(&capture.subscriber("unicodex=debug"), || {
                request_span(&req).in_scope(|| observe_body(Response::new(body)).into_body())
            })
        };
        let body = wrap(Body::from("hidden-data"));
        assert_eq!(body.size_hint().exact(), Some(11));
        assert_eq!(body.collect().await.unwrap().to_bytes(), "hidden-data");

        let mut trailers = HeaderMap::new();
        trailers.insert("x-trailer", "hidden-trailer".parse().unwrap());
        let frames: Vec<Result<_, io::Error>> = vec![
            Ok(Frame::data(Bytes::from_static(b"hidden-data"))),
            Ok(Frame::trailers(trailers.clone())),
        ];
        let body = wrap(Body::new(StreamBody::new(futures_util::stream::iter(
            frames,
        ))));
        let collected = body.collect().await.unwrap();
        assert_eq!(collected.trailers(), Some(&trailers));
        assert_eq!(collected.to_bytes(), "hidden-data");

        let mut body = wrap(Body::from_stream(futures_util::stream::iter([
            Ok(Bytes::from_static(b"hidden-first-chunk")),
            Err(io::Error::other("hidden-transport-error")),
        ])));
        assert_eq!(
            body.frame().await.unwrap().unwrap().into_data().unwrap(),
            "hidden-first-chunk"
        );
        assert!(!capture.text().contains("response body failed"));
        assert!(body.frame().await.unwrap().is_err());
        drop(body);
        let logs = capture.text();
        assert_eq!(logs.matches("response body failed").count(), 1);
        assert!(!logs.contains("hidden"));
    }

    #[test]
    fn filtering_keeps_error_context_and_uses_safe_defaults() {
        for (input, invalid) in [(None, false), (Some(""), false), (Some("["), true)] {
            let (actual, was_invalid) = filter(input);
            assert_eq!(was_invalid, invalid);
            assert_eq!(
                actual.to_string(),
                EnvFilter::new(DEFAULT_FILTER).to_string()
            );
        }
        let capture = Capture::default();
        tracing::dispatcher::with_default(&capture.subscriber("warn"), || {
            let req = Request::builder()
                .uri("/models?secret=hidden-query")
                .body(Body::empty())
                .unwrap();
            request_span(&req).in_scope(|| {
                tracing::debug!("hidden debug event");
                response_ready(&Ok(Response::new(Body::empty())), Duration::ZERO);
                response_ready(
                    &Err(ProxyError::Upstream(anyhow::anyhow!("hidden-error-secret"))),
                    Duration::ZERO,
                );
            });
        });
        let logs = capture.text();
        assert!(logs.contains("ERROR"));
        assert!(logs.contains("request_id="));
        assert!(logs.contains("status=502"));
        assert!(logs.contains("path=\"/models\""));
        assert!(!logs.contains("hidden"));
        assert!(!logs.contains("status=200"));
    }

    #[tokio::test]
    async fn handshake_rejections_explain_protocol_failures_without_header_values() {
        use axum::{
            extract::{FromRequestParts, ws::WebSocketUpgrade},
            http::{Version, header},
        };
        use tracing::{Instrument, instrument::WithSubscriber};

        for (removed, version, expected) in [
            (
                Some(header::CONNECTION),
                Version::HTTP_11,
                "Connection header did not include 'upgrade'",
            ),
            (
                Some(header::UPGRADE),
                Version::HTTP_11,
                "`Upgrade` header did not include 'websocket'",
            ),
            (
                Some(header::SEC_WEBSOCKET_KEY),
                Version::HTTP_11,
                "`Sec-WebSocket-Key` header missing",
            ),
            (
                Some(header::SEC_WEBSOCKET_VERSION),
                Version::HTTP_11,
                "`Sec-WebSocket-Version` header did not include '13'",
            ),
            (None, Version::HTTP_2, "Request method must be `CONNECT`"),
            (None, Version::HTTP_11, "no upgrade state was present"),
        ] {
            let capture = Capture::default();
            async {
                let mut req = Request::builder()
                    .uri("/backend-api/codex/responses?hidden-query")
                    .version(version)
                    .header(header::AUTHORIZATION, "Bearer hidden-gateway-key")
                    .header(header::CONNECTION, "upgrade")
                    .header(header::UPGRADE, "websocket")
                    .header(header::SEC_WEBSOCKET_VERSION, "13")
                    .header(header::SEC_WEBSOCKET_KEY, "hidden-websocket-key")
                    .body(Body::empty())
                    .unwrap();
                if let Some(header) = removed {
                    req.headers_mut().remove(header);
                }
                let span = request_span(&req);
                async {
                    let (mut parts, _) = req.into_parts();
                    let Err(rejection) =
                        WebSocketUpgrade::from_request_parts(&mut parts, &()).await
                    else {
                        panic!("synthetic handshake must fail");
                    };
                    response_ready(
                        &Err(ProxyError::BadRequest(anyhow::Error::new(rejection))),
                        Duration::ZERO,
                    );
                }
                .instrument(span)
                .await;
            }
            .with_subscriber(capture.subscriber("warn"))
            .await;
            let logs = capture.text();
            let diagnostic = logs
                .lines()
                .find(|line| line.contains("WebSocket handshake rejected"))
                .unwrap();
            let summary = logs
                .lines()
                .find(|line| line.contains("response ready"))
                .unwrap();
            assert!(diagnostic.contains(expected), "{logs}");
            assert!(diagnostic.contains(&format!("http_version={version:?}")));
            assert_eq!(request_id(diagnostic), request_id(summary));
            assert!(summary.contains("status=400"));
            assert!(!logs.contains("hidden"));
        }
    }
}
