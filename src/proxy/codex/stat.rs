use std::sync::Arc;

use axum::{
    body::{Body, Bytes},
    http::HeaderMap,
    response::Response,
};
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Frame;
use serde_json::{Map, Value};

use crate::{
    ledger::{Ledger, WeeklyCredits},
    proxy::Observer,
};

const MAX_REPORT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct CodexObserver {
    ledger: Ledger,
}

impl CodexObserver {
    pub fn new(ledger: Ledger) -> Self {
        Self { ledger }
    }

    pub fn ledger(&self) -> &Ledger {
        &self.ledger
    }

    fn observation_result(&self, user: &str, result: anyhow::Result<()>) -> anyhow::Result<()> {
        if result.is_err() && self.ledger.weekly_credits(user)? == WeeklyCredits::Unlimited {
            tracing::warn!(
                operation = "observe_usage",
                "accounting failed; forwarding unlimited response"
            );
            Ok(())
        } else {
            result
        }
    }

    async fn message(&self, user: &str, bytes: &[u8]) -> anyhow::Result<()> {
        let Ok(value) = serde_json::from_slice::<Value>(bytes) else {
            return Ok(());
        };
        let (usage, credits) = extract(&value);
        self.observation_result(user, self.ledger.record(user, usage, credits).await)
    }

    async fn event(&self, user: &str, event: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        self.message(user, &sse_data(&event)).await?;
        Ok(event)
    }
}

impl Observer for CodexObserver {
    async fn observe_http(
        self: Arc<Self>,
        user: Arc<str>,
        response: Response,
    ) -> anyhow::Result<Response> {
        let unlimited = self.ledger.weekly_credits(&user)? == WeeklyCredits::Unlimited;
        let recorded = self
            .ledger
            .record(&user, None, header_credits(response.headers()))
            .await;
        self.observation_result(&user, recorded)?;
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        let sse = content_type == "text/event-stream";
        let json = content_type == "application/json" || content_type.ends_with("+json");
        if !sse && !json {
            return Ok(response);
        }
        if !response
            .headers()
            .get("content-encoding")
            .is_none_or(|v| v == "identity")
        {
            if unlimited {
                tracing::warn!(
                    operation = "observe_usage",
                    "encoded unlimited response forwarded without body observation"
                );
                return Ok(response);
            }
            anyhow::bail!("encoded accounting response");
        }
        let (parts, mut body) = response.into_parts();
        if json && unlimited {
            let stream = async_stream::try_stream! {
                let mut pending = Vec::new();
                let mut size = 0usize;
                let mut observing = true;
                while let Some(frame) = body.frame().await {
                    let frame = match frame {
                        Ok(frame) => frame,
                        Err(error) => {
                            for frame in pending.drain(..) { yield frame; }
                            Err(anyhow::Error::from(error))?;
                            break;
                        }
                    };
                    if observing {
                        size = size.saturating_add(frame.data_ref().map_or(0, Bytes::len));
                        if size <= MAX_REPORT_BYTES {
                            pending.push(frame);
                            continue;
                        }
                        tracing::warn!(operation = "observe_usage", "JSON observation limit exceeded; forwarding unlimited response");
                        observing = false;
                        for frame in pending.drain(..) { yield frame; }
                    }
                    yield frame;
                }
                if observing {
                    let mut bytes = Vec::with_capacity(size);
                    for frame in &pending {
                        if let Some(data) = frame.data_ref() { bytes.extend_from_slice(data); }
                    }
                    self.message(&user, &bytes).await?;
                    for frame in pending { yield frame; }
                }
            };
            return Ok(Response::from_parts(parts, observed_body(stream)));
        }
        if json {
            let collected = http_body_util::Limited::new(body, MAX_REPORT_BYTES)
                .collect()
                .await
                .map_err(|_| {
                    anyhow::anyhow!("JSON response could not be observed within size limit")
                })?;
            let trailers = collected.trailers().cloned();
            let bytes = collected.to_bytes();
            self.message(&user, &bytes).await?;
            let body = if let Some(trailers) = trailers {
                Body::new(StreamBody::new(futures_util::stream::iter([
                    Ok::<_, std::io::Error>(Frame::data(bytes)),
                    Ok(Frame::trailers(trailers)),
                ])))
            } else {
                Body::from(bytes)
            };
            return Ok(Response::from_parts(parts, body));
        }
        let stream = async_stream::try_stream! {
            let mut parser = Sse::default();
            let mut observing = true;
            while let Some(frame) = body.frame().await {
                let frame = match frame {
                    Ok(frame) => frame,
                    Err(error) => {
                        if unlimited && !parser.bytes.is_empty() {
                            yield Frame::data(Bytes::from(std::mem::take(&mut parser.bytes)));
                        }
                        Err(anyhow::Error::from(error))?;
                        break;
                    }
                };
                if !observing {
                    yield frame;
                    continue;
                }
                match frame.into_data() {
                    Ok(bytes) => {
                        // Limit the pending event rather than the upstream HTTP chunk.
                        for offset in (0..bytes.len()).step_by(4096) {
                            let chunk = &bytes[offset..bytes.len().min(offset + 4096)];
                            if let Err(error) = parser.push(chunk) {
                                if !unlimited { Err(error)?; }
                                tracing::warn!(operation = "observe_usage", "SSE observation limit exceeded; forwarding unlimited response");
                                yield Frame::data(Bytes::from(std::mem::take(&mut parser.bytes)));
                                yield Frame::data(bytes.slice(offset..));
                                observing = false;
                                break;
                            }
                            while let Some(event) = parser.next_event(false) {
                                yield Frame::data(Bytes::from(self.event(&user, event).await?));
                            }
                        }
                    }
                    Err(frame) => {
                        while let Some(event) = parser.next_event(true) {
                            yield Frame::data(Bytes::from(self.event(&user, event).await?));
                        }
                        if let Ok(trailers) = frame.into_trailers() {
                            yield Frame::trailers(trailers);
                        }
                    }
                }
            }
            while let Some(event) = parser.next_event(true) {
                yield Frame::data(Bytes::from(self.event(&user, event).await?));
            }
        };
        Ok(Response::from_parts(parts, observed_body(stream)))
    }

    async fn observe_ws(&self, user: &str, message: &[u8]) -> anyhow::Result<()> {
        if message.len() > MAX_REPORT_BYTES {
            return self.observation_result(
                user,
                Err(anyhow::anyhow!(
                    "WebSocket accounting message exceeds observation limit"
                )),
            );
        }
        self.message(user, message).await
    }
}

// Constrain the stream error type without erasing or boxing the stream itself.
fn observed_body<S>(stream: S) -> Body
where
    S: futures_util::Stream<Item = Result<Frame<Bytes>, anyhow::Error>> + Send + 'static,
{
    Body::new(StreamBody::new(stream))
}

fn header_credits(headers: &HeaderMap) -> Option<Value> {
    let mut fields = Map::new();
    for (header, field) in [
        ("x-codex-credits-has-credits", "has_credits"),
        ("x-codex-credits-unlimited", "unlimited"),
        ("x-codex-credits-balance", "balance"),
    ] {
        if let Some(raw) = headers.get(header).and_then(|value| value.to_str().ok()) {
            let raw = raw.trim();
            let value = if field == "balance" {
                numeric(&Value::String(raw.to_owned())).then(|| Value::String(raw.to_owned()))
            } else if raw.eq_ignore_ascii_case("true") || raw == "1" {
                Some(Value::Bool(true))
            } else if raw.eq_ignore_ascii_case("false") || raw == "0" {
                Some(Value::Bool(false))
            } else {
                None
            };
            if let Some(value) = value {
                fields.insert(field.into(), value);
            }
        }
    }
    object(fields)
}

fn numeric(value: &Value) -> bool {
    value
        .as_f64()
        .or_else(|| value.as_str()?.parse::<f64>().ok())
        .is_some_and(|number| number.is_finite() && number >= 0.0)
}

fn object(fields: Map<String, Value>) -> Option<Value> {
    (!fields.is_empty()).then_some(Value::Object(fields))
}

fn extract(value: &Value) -> (Option<Value>, Option<Value>) {
    let kind = value.get("type").and_then(Value::as_str);
    let terminal = matches!(
        kind,
        Some("response.completed" | "response.done" | "response.failed" | "response.incomplete")
    );
    let response = if terminal {
        value.get("response").unwrap_or(value)
    } else {
        value
    };
    let mut usage = Map::new();
    if terminal || kind.is_none() {
        if let Some(reported) = response.get("usage") {
            for field in ["input_tokens", "output_tokens", "total_tokens"] {
                if let Some(count) = reported.get(field).filter(|value| value.as_u64().is_some()) {
                    usage.insert(field.into(), count.clone());
                }
            }
            for (field, counters) in [
                (
                    "input_tokens_details",
                    &["cached_tokens", "cache_write_tokens"][..],
                ),
                ("output_tokens_details", &["reasoning_tokens"][..]),
            ] {
                let mut details = Map::new();
                for counter in counters {
                    if let Some(count) = reported
                        .get(field)
                        .and_then(|value| value.get(counter))
                        .filter(|value| value.as_u64().is_some())
                    {
                        details.insert((*counter).into(), count.clone());
                    }
                }
                if let Some(details) = object(details) {
                    usage.insert(field.into(), details);
                }
            }
            if let Some(units) = reported
                .get("codex_rollout_budget_units")
                .filter(|value| numeric(value))
            {
                usage.insert("codex_rollout_budget_units".into(), units.clone());
            }
        }
        if let Some(amount) = response
            .get("usage_metadata")
            .and_then(|metadata| metadata.get("amount"))
            .filter(|value| numeric(value))
        {
            usage.insert(
                "usage_metadata".into(),
                serde_json::json!({"amount": amount}),
            );
        }
    }
    let mut credits = Map::new();
    if kind.is_none()
        && let Some(control) = response.get("spend_control")
    {
        credits.insert("spend_control".into(), control.clone());
    }
    if (terminal || kind.is_none() || kind == Some("codex.rate_limits"))
        && let Some(reported) = response.get("credits").or_else(|| {
            value
                .get("rate_limits")
                .and_then(|limits| limits.get("credits"))
        })
    {
        for field in ["has_credits", "unlimited", "balance"] {
            if let Some(value) = reported.get(field).filter(|value| {
                if field == "balance" {
                    numeric(value)
                } else {
                    value.is_boolean()
                }
            }) {
                credits.insert(field.into(), value.clone());
            }
        }
    }
    // An empty terminal report still tells us that consumption was not supplied.
    (
        if terminal {
            Some(Value::Object(usage))
        } else {
            object(usage)
        },
        object(credits),
    )
}

/// Private response-local framing. Raw bytes (including comments, BOM and line endings) survive.
#[derive(Default)]
struct Sse {
    bytes: Vec<u8>,
    scan: usize,
    line_start: usize,
}

impl Sse {
    fn push(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.bytes.len() + bytes.len() <= MAX_REPORT_BYTES,
            "SSE event exceeds observation limit"
        );
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn next_event(&mut self, eof: bool) -> Option<Vec<u8>> {
        while self.scan < self.bytes.len() {
            let byte = self.bytes[self.scan];
            if byte != b'\r' && byte != b'\n' {
                self.scan += 1;
                continue;
            }
            if byte == b'\r' && self.scan + 1 == self.bytes.len() && !eof {
                return None;
            }
            let empty = self.scan == self.line_start;
            self.scan += 1;
            if byte == b'\r' && self.bytes.get(self.scan) == Some(&b'\n') {
                self.scan += 1;
            }
            self.line_start = self.scan;
            if empty {
                let event = self.bytes.drain(..self.scan).collect();
                self.scan = 0;
                self.line_start = 0;
                return Some(event);
            }
        }
        if eof && !self.bytes.is_empty() {
            self.scan = 0;
            self.line_start = 0;
            return Some(std::mem::take(&mut self.bytes));
        }
        None
    }
}

fn sse_data(event: &[u8]) -> Vec<u8> {
    let event = event.strip_prefix(b"\xef\xbb\xbf").unwrap_or(event);
    let mut data = Vec::new();
    for line in event.split(|byte| *byte == b'\n' || *byte == b'\r') {
        if let Some(value) = line.strip_prefix(b"data:") {
            if !data.is_empty() {
                data.push(b'\n');
            }
            data.extend_from_slice(value.strip_prefix(b" ").unwrap_or(value));
        }
    }
    data
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn observer() -> Arc<CodexObserver> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        Arc::new(CodexObserver::new(
            Ledger::from_pool(
                pool,
                ["Alice", "Bob", "Carol"]
                    .into_iter()
                    .map(|user| {
                        (
                            Arc::from(user),
                            WeeklyCredits::Limited(rust_decimal::Decimal::ONE),
                        )
                    })
                    .collect(),
            )
            .await
            .unwrap(),
        ))
    }

    fn response(kind: &str, bytes: &[u8], chunk_size: usize) -> Response {
        let chunks: Vec<_> = bytes
            .chunks(chunk_size)
            .map(|bytes| Ok::<_, std::io::Error>(Bytes::copy_from_slice(bytes)))
            .collect();
        Response::builder()
            .header("content-type", kind)
            .header("x-custom", "preserved")
            .body(Body::from_stream(futures_util::stream::iter(chunks)))
            .unwrap()
    }

    async fn rows(observer: &CodexObserver) -> Vec<(String, Option<String>, Option<String>)> {
        sqlx::query_as("SELECT user, usage, credits FROM observations ORDER BY rowid")
            .fetch_all(&observer.ledger.pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn json_preserves_response_and_records_reported_fields() {
        let observer = observer().await;
        let bytes = br#"{"id":"not-stored","output":[{"text":"private"}],"usage":{"input_tokens":0,"output_tokens":5,"input_tokens_details":{"cached_tokens":0},"arbitrary":"not-stored"},"credits":{"balance":"3.12000000000000000001"}}"#;
        let mut response = response("application/json; charset=utf-8", bytes, 7);
        response
            .headers_mut()
            .insert("x-codex-credits-unlimited", "false".parse().unwrap());
        let observed = observer
            .clone()
            .observe_http("Alice".into(), response)
            .await
            .unwrap();
        assert_eq!(observed.headers()["x-custom"], "preserved");
        assert!(!observed.headers().contains_key("x-codex-credits-balance"));
        assert_eq!(observed.headers()["x-codex-credits-unlimited"], "false");
        assert_eq!(
            observed
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .as_ref(),
            bytes
        );
        let records = rows(&observer).await;
        assert_eq!(records.len(), 2);
        assert_eq!(
            records[0],
            ("Alice".into(), None, Some(r#"{"unlimited":false}"#.into()))
        );
        assert_eq!(records[1].0, "Alice");
        let usage: Value = serde_json::from_str(records[1].1.as_ref().unwrap()).unwrap();
        assert_eq!(
            usage,
            serde_json::json!({"input_tokens":0,"output_tokens":5,"input_tokens_details":{"cached_tokens":0}})
        );
        assert_eq!(
            records[1].2.as_deref(),
            Some(r#"{"balance":"3.12000000000000000001"}"#)
        );
    }

    #[tokio::test]
    async fn fragmented_sse_and_ws_have_isolated_state_and_real_users() {
        let observer = observer().await;
        let bytes = concat!("\u{feff}: keepalive\r\n\r\n",
            "event: response.output_text.delta\r\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"你好\"}\r\n\r\n",
            "data: {\r\ndata: \"type\":\"response.completed\",\r\ndata: \"response\":{\"usage\":{\"output_tokens\":2}}}\r\n\r\n",
            "data: [DONE]\r\n\r\n").as_bytes();
        let first = observer
            .clone()
            .observe_http("Alice".into(), response("text/event-stream", bytes, 1))
            .await
            .unwrap();
        let second_bytes = b"data: {\"type\":\"response.failed\",\"response\":{\"usage\":{\"input_tokens\":9}}}\r\r";
        let second = observer
            .clone()
            .observe_http("Bob".into(), response("text/event-stream", second_bytes, 2))
            .await
            .unwrap();
        let (first, second) =
            tokio::join!(first.into_body().collect(), second.into_body().collect());
        assert_eq!(first.unwrap().to_bytes().as_ref(), bytes);
        assert_eq!(second.unwrap().to_bytes().as_ref(), second_bytes);
        observer
            .observe_ws(
                "Carol",
                br#"{"type":"codex.rate_limits","credits":{"has_credits":false,"balance":"0"}}"#,
            )
            .await
            .unwrap();
        observer
            .observe_ws(
                "Carol",
                br#"{"type":"response.completed","response":{"usage":{"total_tokens":10}}}"#,
            )
            .await
            .unwrap();
        observer.observe_ws("Carol", br#"{"type":"response.output_text.delta","usage":{"input_tokens":999},"delta":"ignored"}"#).await.unwrap();
        observer.observe_ws("Carol", b"not JSON").await.unwrap();
        let records = rows(&observer).await;
        assert_eq!(records.len(), 4);
        assert!(records.iter().any(|(user, usage, _)| user == "Alice"
            && usage.as_deref() == Some(r#"{"output_tokens":2}"#)));
        assert!(
            records.iter().any(|(user, usage, _)| user == "Bob"
                && usage.as_deref() == Some(r#"{"input_tokens":9}"#))
        );
        assert_eq!(
            records
                .iter()
                .filter(|(user, _, _)| user == "Carol")
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn persistence_precedes_completing_bytes_and_failures_surface_on_all_paths() {
        let observer = observer().await;
        let bytes = b"data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"total_tokens\":1}}}\n\n";
        let observed = observer
            .clone()
            .observe_http("Alice".into(), response("text/event-stream", bytes, 1))
            .await
            .unwrap();
        let mut body = observed.into_body();
        assert!(rows(&observer).await.is_empty());
        assert_eq!(
            body.frame()
                .await
                .unwrap()
                .unwrap()
                .into_data()
                .unwrap()
                .as_ref(),
            bytes
        );
        assert_eq!(rows(&observer).await.len(), 1);
        sqlx::query("CREATE TRIGGER reject_observation BEFORE INSERT ON observations BEGIN SELECT RAISE(ABORT, 'database unavailable'); END")
            .execute(&observer.ledger.pool).await.unwrap();
        assert!(
            observer
                .clone()
                .observe_http(
                    "Alice".into(),
                    response("application/json", br#"{"usage":{"total_tokens":1}}"#, 1)
                )
                .await
                .is_err()
        );
        let observed = observer
            .clone()
            .observe_http("Alice".into(), response("text/event-stream", bytes, 1))
            .await
            .unwrap();
        let mut body = observed.into_body();
        let error = body.frame().await.unwrap().unwrap_err();
        assert!(error.to_string().contains("database unavailable"));
        assert!(
            observer
                .observe_ws("Alice", br#"{"usage":{"input_tokens":1}}"#)
                .await
                .is_err()
        );
        let mut handshake = Response::new(Body::empty());
        handshake
            .headers_mut()
            .insert("x-codex-credits-balance", "8".parse().unwrap());
        assert!(
            observer
                .clone()
                .observe_http("Alice".into(), handshake)
                .await
                .is_err()
        );
        assert_eq!(rows(&observer).await.len(), 1);
    }

    #[tokio::test]
    async fn preserves_trailers_and_surfaces_upstream_body_errors() {
        let observer = observer().await;
        let mut trailers = HeaderMap::new();
        trailers.insert("x-trailer", "yes".parse().unwrap());
        let frames: Vec<Result<Frame<Bytes>, std::io::Error>> = vec![
            Ok(Frame::data(Bytes::from_static(b"data: [DONE]\n\n"))),
            Ok(Frame::trailers(trailers.clone())),
        ];
        let response = Response::builder()
            .header("content-type", "text/event-stream")
            .body(Body::new(StreamBody::new(futures_util::stream::iter(
                frames,
            ))))
            .unwrap();
        let body = observer
            .clone()
            .observe_http("Alice".into(), response)
            .await
            .unwrap()
            .into_body()
            .collect()
            .await
            .unwrap();
        assert_eq!(body.trailers(), Some(&trailers));
        assert_eq!(body.to_bytes().as_ref(), b"data: [DONE]\n\n");
        let stream = futures_util::stream::iter([Err::<Bytes, _>(std::io::Error::other(
            "upstream dropped",
        ))]);
        let response = Response::builder()
            .header("content-type", "application/json")
            .body(Body::from_stream(stream))
            .unwrap();
        assert!(
            observer
                .observe_http("Alice".into(), response)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn quota_events_preserve_bytes_headers_and_commit_before_delivery() {
        let observer = observer().await;
        let report = br#"{"type":"codex.rate_limits","credits":{"has_credits":true,"unlimited":true,"balance":"900","extra":"credit metadata"},"rate_limits":{"primary":{"used_percent":99},"extra":"limit metadata"},"extra":"preserved"}"#;
        let event = format!(
            "id: 7\r\nevent: codex.rate_limits\r\n: comment\r\ndata: {}\r\n\r\n",
            std::str::from_utf8(report).unwrap()
        );
        let mut response = response("text/event-stream", event.as_bytes(), 1);
        response
            .headers_mut()
            .insert("x-codex-primary-used-percent", "99".parse().unwrap());
        response
            .headers_mut()
            .insert("etag", "old".parse().unwrap());
        let observed = observer
            .clone()
            .observe_http("Alice".into(), response)
            .await
            .unwrap();
        assert!(!observed.headers().contains_key("x-codex-credits-balance"));
        assert_eq!(observed.headers()["etag"], "old");
        assert_eq!(observed.headers()["x-codex-primary-used-percent"], "99");
        let delivered = observed
            .into_body()
            .frame()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap();
        assert_eq!(delivered.as_ref(), event.as_bytes());
        let records = rows(&observer).await;
        assert_eq!(records.len(), 1);
        assert!(records[0].2.as_ref().unwrap().contains("900"));
        observer.observe_ws("Alice", report).await.unwrap();
        assert_eq!(rows(&observer).await.len(), 2);
    }

    #[tokio::test]
    async fn ignored_websocket_reports_do_not_allocate_a_future() {
        use std::{
            future::Future,
            pin::pin,
            task::{Context, Poll, Waker},
        };
        let observer = observer().await;
        // Verify the counter works before using a zero count as regression evidence.
        let (_, control) = crate::allocation::measure(|| std::hint::black_box(Box::new(42)));
        assert!(
            control > 0,
            "allocation counter must detect a heap allocation"
        );
        // No parser allocations or database work for this valid, unrecognized JSON value.
        // Include future construction, polling and drop in the measured region.
        let (_, allocations) = crate::allocation::measure(|| {
            let future = observer.observe_ws("Alice", b"null");
            fn is_send<T: Send>(_: &T) {}
            is_send(&future);
            assert!(matches!(
                pin!(future).poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Ok(()))
            ));
        });
        assert_eq!(allocations, 0);
        assert!(rows(&observer).await.is_empty());
    }

    #[test]
    fn ignores_unrecognized_and_missing_fields_and_keeps_consumption_separate() {
        assert_eq!(
            extract(
                &serde_json::json!({"output":[{"usage":{"total_tokens":42}}],"usage":{"input_tokens":null}})
            ),
            (None, None)
        );
        let (usage, credits) = extract(&serde_json::json!({
            "usage":{"input_tokens":-1,"total_tokens":1.5,"codex_rollout_budget_units":0.75},
            "usage_metadata":{"amount":"0.1234567890123456789"}, "credits":{"balance":"12"}
        }));
        assert_eq!(
            usage.unwrap(),
            serde_json::json!({"codex_rollout_budget_units":0.75,"usage_metadata":{"amount":"0.1234567890123456789"}})
        );
        assert_eq!(credits.unwrap(), serde_json::json!({"balance":"12"}));
    }
}
