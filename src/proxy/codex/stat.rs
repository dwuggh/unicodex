use std::{
    collections::{HashMap, VecDeque},
    io::Read,
    sync::{Arc, Mutex},
};

use axum::{
    body::{Body, Bytes},
    extract::Request,
    response::Response,
};
use http_body_util::{BodyExt, StreamBody};
use hyper::body::{Body as _, Frame};
use serde_json::Value;

use crate::{
    ledger::Ledger,
    pricing::{BillingContext, CreditPrices},
    proxy::Observer,
};

const MAX_REPORT_BYTES: usize = 16 * 1024 * 1024;
const MAX_TRACKED_RESPONSES: usize = 256;
const MAX_RESPONSE_ID_BYTES: usize = 256;

/// A bounded side copy of an HTTP request; the original frames go to the upstream.
#[derive(Clone, Default)]
pub(crate) struct RequestCapture(Arc<Mutex<BillingContext>>);

impl RequestCapture {
    fn snapshot(&self) -> BillingContext {
        self.0.lock().unwrap().clone()
    }

    pub(crate) fn inspect(request: &mut Request) -> Self {
        let capture = Self::default();
        let result = capture.clone();
        let encoding = request
            .headers()
            .get("content-encoding")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("identity")
            .to_owned();
        let mut body = std::mem::replace(request.body_mut(), Body::empty());
        let stream = async_stream::try_stream! {
            let mut bytes = Vec::new();
            let mut observing = true;
            while let Some(frame) = body.frame().await {
                let frame = frame?;
                if observing && let Some(data) = frame.data_ref() {
                    if bytes.len().saturating_add(data.len()) <= MAX_REPORT_BYTES { bytes.extend_from_slice(data); }
                    else { observing = false; bytes.clear(); warn("inspect_request", "request_limit"); }
                }
                // Store metadata before yielding the last data frame when possible;
                // final EOF below also handles bodies whose end flag is not available.
                if observing && body.is_end_stream() {
                    if let Some(context) = request_context(&bytes, &encoding) { *capture.0.lock().unwrap() = context; }
                    observing = false;
                }
                yield frame;
            }
            if observing && let Some(context) = request_context(&bytes, &encoding) { *capture.0.lock().unwrap() = context; }
        };
        *request.body_mut() = observed_body(stream);
        result
    }
}

fn request_context(bytes: &[u8], encoding: &str) -> Option<BillingContext> {
    let decoded;
    let bytes = match encoding {
        "" | "identity" => bytes,
        "zstd" => {
            let decoder = zstd::stream::read::Decoder::new(bytes).ok()?;
            let mut limited = decoder.take((MAX_REPORT_BYTES + 1) as u64);
            let mut output = Vec::new();
            if limited.read_to_end(&mut output).is_err() || output.len() > MAX_REPORT_BYTES {
                warn("inspect_request", "decode_limit_or_error");
                return None;
            }
            decoded = output;
            &decoded
        }
        _ => {
            warn("inspect_request", "unsupported_encoding");
            return None;
        }
    };
    serde_json::from_slice::<Value>(bytes)
        .ok()
        .map(|value| BillingContext::from_value(&value))
}

/// State belongs to one HTTP response or WebSocket session, never to a user/global observer.
#[derive(Default)]
pub(crate) struct ResponseTracker {
    pending: VecDeque<BillingContext>,
    active: HashMap<String, BillingContext>,
    completed: VecDeque<String>,
    http: Option<RequestCapture>,
    http_finished: bool,
    disabled: bool,
}

impl ResponseTracker {
    fn disable_correlation(&mut self, reason: &'static str) {
        self.pending.clear();
        self.active.clear();
        self.disabled = true;
        warn("track_response", reason);
    }

    pub(crate) fn request(&mut self, value: &Value) {
        if self.disabled {
            return;
        }
        if self.pending.len() + self.active.len() >= MAX_TRACKED_RESPONSES {
            self.disable_correlation("pending_limit");
            return;
        }
        self.pending.push_back(BillingContext::from_value(value));
    }

    fn created(&mut self, id: &str) {
        if self.active.contains_key(id) || self.completed.iter().any(|v| v == id) {
            return;
        }
        let context = if let Some(http) = &self.http {
            http.snapshot()
        } else if self.disabled || self.pending.is_empty() {
            return;
        } else {
            self.pending.pop_front().unwrap_or_default()
        };
        self.active.insert(id.to_owned(), context);
    }

    fn terminal(&mut self, id: Option<&str>) -> Option<BillingContext> {
        if self.http.is_some() {
            if self.http_finished {
                return None;
            }
            self.http_finished = true;
        }
        // Missing IDs are safe only with a single active response and no queued work.
        let inferred_id = (id.is_none() && self.active.len() == 1 && self.pending.is_empty())
            .then(|| self.active.keys().next().unwrap().clone());
        if let Some(id) = id.or(inferred_id.as_deref()) {
            if self.completed.iter().any(|v| v == id) {
                return None;
            }
            self.completed.push_back(id.to_owned());
            if self.completed.len() > MAX_TRACKED_RESPONSES {
                self.completed.pop_front();
            }
            if let Some(context) = self.active.remove(id) {
                return Some(context);
            }
        }
        if let Some(http) = &self.http {
            return Some(http.snapshot());
        }
        // A terminal event without response.created can be associated only when unambiguous.
        if self.active.is_empty() && self.pending.len() == 1 {
            return self.pending.pop_front();
        }
        if !self.pending.is_empty() || !self.active.is_empty() {
            self.disable_correlation("ambiguous_terminal");
        }
        Some(BillingContext::default())
    }
}

#[derive(Clone)]
pub struct CodexObserver {
    ledger: Ledger,
    prices: Arc<CreditPrices>,
}

impl CodexObserver {
    pub fn new(ledger: Ledger) -> anyhow::Result<Self> {
        Ok(Self {
            ledger,
            prices: Arc::new(CreditPrices::bundled()?),
        })
    }

    pub fn ledger(&self) -> &Ledger {
        &self.ledger
    }

    pub(crate) async fn message(&self, user: &str, bytes: &[u8], tracker: &mut ResponseTracker) {
        if bytes.len() > MAX_REPORT_BYTES {
            tracker.disable_correlation("message_limit");
            warn("observe_usage", "message_limit");
            return;
        }
        let Ok(value) = serde_json::from_slice::<Value>(bytes) else {
            return;
        };
        let kind = value.get("type").and_then(Value::as_str);
        let response = value.get("response").unwrap_or(&value);
        let id = response.get("id").and_then(Value::as_str);
        if id.is_some_and(|id| id.len() > MAX_RESPONSE_ID_BYTES) {
            tracker.disable_correlation("response_id_limit");
            return;
        }
        if kind == Some("response.created") {
            if let Some(id) = id {
                tracker.created(id);
            }
            return;
        }
        if kind == Some("error") {
            tracker.pending.clear();
            return;
        }
        let terminal = matches!(
            kind,
            Some(
                "response.completed" | "response.done" | "response.failed" | "response.incomplete"
            )
        );
        if !terminal && !(kind.is_none() && response.get("usage").is_some()) {
            return;
        }
        let Some(context) = tracker.terminal(id) else {
            return;
        };
        if context.warmup
            && response.get("usage").is_none_or(Value::is_null)
            && response
                .get("output")
                .is_none_or(|v| v.as_array().is_some_and(Vec::is_empty))
        {
            return;
        }
        match self.prices.credits(response, &context) {
            Ok(credits) => {
                if self.ledger.record_charge(user, credits).await.is_err() {
                    warn("observe_usage", "record_charge_failed");
                }
            }
            Err(error) => {
                tracing::warn!(operation = "price_usage", user, reason = %error, "charge skipped; forwarding response")
            }
        }
    }

    pub(crate) async fn observe_http_with_context(
        self: Arc<Self>,
        user: Arc<str>,
        response: Response,
        capture: RequestCapture,
    ) -> anyhow::Result<Response> {
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
            warn("observe_usage", "encoded_response");
            return Ok(response);
        }
        let (parts, mut body) = response.into_parts();
        let stream = async_stream::try_stream! {
            let mut tracker = ResponseTracker { http: Some(capture), ..Default::default() };
            let mut observing = true;
            if json {
                let mut pending = Vec::new();
                let mut size = 0usize;
                while let Some(frame) = body.frame().await {
                    let frame = match frame {
                        Ok(frame) => frame,
                        Err(error) => { for frame in pending.drain(..) { yield frame; } Err(anyhow::Error::from(error))?; break; }
                    };
                    if observing {
                        size = size.saturating_add(frame.data_ref().map_or(0, Bytes::len));
                        if size <= MAX_REPORT_BYTES { pending.push(frame); continue; }
                        warn("observe_usage", "json_limit"); observing = false;
                        for frame in pending.drain(..) { yield frame; }
                    }
                    yield frame;
                }
                if observing {
                    let mut bytes = Vec::with_capacity(size);
                    for frame in &pending { if let Some(data) = frame.data_ref() { bytes.extend_from_slice(data); } }
                    self.message(&user, &bytes, &mut tracker).await;
                    for frame in pending { yield frame; }
                }
            } else {
                let mut parser = Sse::default();
                while let Some(frame) = body.frame().await {
                    let frame = match frame {
                        Ok(frame) => frame,
                        Err(error) => {
                            if !parser.bytes.is_empty() { yield Frame::data(Bytes::from(std::mem::take(&mut parser.bytes))); }
                            Err(anyhow::Error::from(error))?; break;
                        }
                    };
                    if !observing { yield frame; continue; }
                    match frame.into_data() {
                        Ok(bytes) => {
                            for offset in (0..bytes.len()).step_by(4096) {
                                let chunk = &bytes[offset..bytes.len().min(offset + 4096)];
                                if parser.push(chunk).is_err() {
                                    warn("observe_usage", "sse_limit");
                                    yield Frame::data(Bytes::from(std::mem::take(&mut parser.bytes)));
                                    yield Frame::data(bytes.slice(offset..)); observing = false; break;
                                }
                                while let Some(event) = parser.next_event(false) {
                                    self.message(&user, &sse_data(&event), &mut tracker).await;
                                    yield Frame::data(Bytes::from(event));
                                }
                            }
                        }
                        Err(frame) => {
                            while let Some(event) = parser.next_event(true) {
                                self.message(&user, &sse_data(&event), &mut tracker).await;
                                yield Frame::data(Bytes::from(event));
                            }
                            yield frame;
                        }
                    }
                }
                while let Some(event) = parser.next_event(true) {
                    self.message(&user, &sse_data(&event), &mut tracker).await;
                    yield Frame::data(Bytes::from(event));
                }
            }
        };
        Ok(Response::from_parts(parts, observed_body(stream)))
    }
}

impl Observer for CodexObserver {
    async fn observe_http(
        self: Arc<Self>,
        user: Arc<str>,
        response: Response,
    ) -> anyhow::Result<Response> {
        self.observe_http_with_context(user, response, RequestCapture::default())
            .await
    }

    async fn observe_ws(&self, user: &str, message: &[u8]) -> anyhow::Result<()> {
        self.message(user, message, &mut ResponseTracker::default())
            .await;
        Ok(())
    }
}

fn warn(operation: &'static str, reason: &'static str) {
    tracing::warn!(
        operation,
        reason,
        "accounting observation skipped; forwarding response"
    );
}

fn observed_body<S>(stream: S) -> Body
where
    S: futures_util::Stream<Item = Result<Frame<Bytes>, anyhow::Error>> + Send + 'static,
{
    Body::new(StreamBody::new(stream))
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
    use crate::ledger::WeeklyCredits;
    use axum::http::HeaderMap;
    use serde_json::json;

    async fn observer() -> Arc<CodexObserver> {
        let ledger = Ledger::in_memory(
            ["Alice", "Bob"]
                .into_iter()
                .map(|v| {
                    (
                        Arc::from(v),
                        WeeklyCredits::Limited(rust_decimal::Decimal::ONE),
                    )
                })
                .collect(),
        )
        .await
        .unwrap();
        Arc::new(CodexObserver::new(ledger).unwrap())
    }

    fn response(kind: &str, bytes: &[u8], chunk_size: usize) -> Response {
        let chunks: Vec<_> = bytes
            .chunks(chunk_size)
            .map(|v| Ok::<_, std::io::Error>(Bytes::copy_from_slice(v)))
            .collect();
        Response::builder()
            .header("content-type", kind)
            .header("x-custom", "preserved")
            .body(Body::from_stream(futures_util::stream::iter(chunks)))
            .unwrap()
    }

    fn context(value: Value) -> RequestCapture {
        RequestCapture(Arc::new(Mutex::new(BillingContext::from_value(&value))))
    }

    #[tokio::test]
    async fn json_is_preserved_and_only_calculated_credits_are_stored() {
        let observer = observer().await;
        let bytes = br#"{"id":"not-stored","model":"gpt-6.1-sol","output":[{"text":"private"}],"usage":{"input_tokens":0,"output_tokens":5,"input_tokens_details":{"cached_tokens":0}},"credits":{"balance":"900"}}"#;
        let mut original = response("application/json; charset=utf-8", bytes, 7);
        original
            .headers_mut()
            .insert("x-codex-credits-unlimited", "false".parse().unwrap());
        let observed = observer
            .clone()
            .observe_http("Alice".into(), original)
            .await
            .unwrap();
        assert_eq!(observed.headers()["x-custom"], "preserved");
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
        let entries = observer.ledger.entries().await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].user, "Alice");
        assert_eq!(entries[0].credits, 1_250_000);
    }

    #[tokio::test]
    async fn fragmented_sse_retains_bytes_and_uses_request_fallback_once() {
        let observer = observer().await;
        let bytes = concat!("\u{feff}: keepalive\r\n\r\n",
            "event: response.output_text.delta\r\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"你好\"}\r\n\r\n",
            "data: {\r\ndata: \"type\":\"response.completed\",\r\ndata: \"response\":{\"id\":\"r1\",\"usage\":{\"input_tokens\":0,\"output_tokens\":2}}}\r\n\r\n",
            "data: {\"type\":\"response.done\",\"response\":{\"id\":\"r1\",\"usage\":{\"input_tokens\":0,\"output_tokens\":2}}}\r\n\r\n",
            "data: [DONE]\r\n\r\n").as_bytes();
        let observed = observer
            .clone()
            .observe_http_with_context(
                "Alice".into(),
                response("text/event-stream", bytes, 1),
                context(json!({"model":"gpt-6.1-sol","service_tier":"fast"})),
            )
            .await
            .unwrap();
        let body = observed.into_body();
        assert!(observer.ledger.entries().await.is_empty());
        let collected = body.collect().await.unwrap().to_bytes();
        assert_eq!(collected.as_ref(), bytes);
        let entries = observer.ledger.entries().await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].credits, 1_000_000);
    }

    #[tokio::test]
    async fn websocket_metadata_is_correlated_with_ids_and_warmups_do_not_poison_it() {
        let observer = observer().await;
        let mut tracker = ResponseTracker::default();
        tracker.request(&json!({"model":"gpt-6-astra","service_tier":"fast","generate":false}));
        observer
            .message(
                "Alice",
                br#"{"type":"response.created","response":{"id":"warmup"}}"#,
                &mut tracker,
            )
            .await;
        tracker.request(&json!({"model":"gpt-6.1-sol","service_tier":"fast"}));
        observer
            .message(
                "Alice",
                br#"{"type":"response.created","response":{"id":"real"}}"#,
                &mut tracker,
            )
            .await;
        let real = br#"{"type":"response.completed","response":{"id":"real","usage":{"input_tokens":0,"output_tokens":1}}}"#;
        observer.message("Alice", real, &mut tracker).await;
        observer
            .message(
                "Alice",
                br#"{"type":"response.completed","response":{"id":"warmup","output":[]}}"#,
                &mut tracker,
            )
            .await;
        observer.message("Alice", real, &mut tracker).await;
        observer
            .message(
                "Alice",
                br#"{"type":"codex.rate_limits","credits":{"balance":"900"}}"#,
                &mut tracker,
            )
            .await;
        tracker.request(&json!({"model":"gpt-6.1-sol"}));
        observer.message("Bob", br#"{"type":"response.failed","response":{"id":"failed","usage":{"input_tokens":0,"output_tokens":1}}}"#, &mut tracker).await;
        let entries = observer.ledger.entries().await;
        assert_eq!(entries.len(), 2);
        assert!(
            entries
                .iter()
                .any(|v| v.user == "Alice" && v.credits == 500_000)
        );
        assert!(
            entries
                .iter()
                .any(|v| v.user == "Bob" && v.credits == 250_000)
        );
    }

    #[tokio::test]
    async fn accounting_failures_preserve_all_response_paths_and_do_not_record_zero() {
        let observer = observer().await;
        let invalid = br#"{"model":"unknown","usage":{"input_tokens":1,"output_tokens":1}}"#;
        let observed = observer
            .clone()
            .observe_http("Alice".into(), response("application/json", invalid, 1))
            .await
            .unwrap();
        assert_eq!(
            observed
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .as_ref(),
            invalid
        );
        assert!(observer.ledger.entries().await.is_empty());
        observer.ledger.execute("CREATE TRIGGER reject_charge BEFORE INSERT ON credit_entries BEGIN SELECT RAISE(ABORT, 'private database error'); END").await;
        let valid = br#"{"model":"gpt-6.1-sol","usage":{"input_tokens":1,"output_tokens":1}}"#;
        let observed = observer
            .clone()
            .observe_http("Alice".into(), response("application/json", valid, 1))
            .await
            .unwrap();
        assert_eq!(
            observed
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .as_ref(),
            valid
        );
        let event = format!("data: {}\n\n", std::str::from_utf8(valid).unwrap());
        let observed = observer
            .clone()
            .observe_http(
                "Alice".into(),
                response("text/event-stream", event.as_bytes(), 1),
            )
            .await
            .unwrap();
        assert_eq!(
            observed
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .as_ref(),
            event.as_bytes()
        );
        observer.observe_ws("Alice", valid).await.unwrap();
        assert!(observer.ledger.entries().await.is_empty());
    }

    #[tokio::test]
    async fn request_inspection_preserves_zstd_bytes_and_trailers() {
        let original =
            br#"{"model":"gpt-6.1-sol","service_tier":"fast","input":[{"text":"private"}]}"#;
        let compressed = zstd::encode_all(&original[..], 1).unwrap();
        let mut trailers = HeaderMap::new();
        trailers.insert("x-trailer", "keep".parse().unwrap());
        let frames = vec![
            Ok::<_, std::io::Error>(Frame::data(Bytes::from(compressed.clone()))),
            Ok(Frame::trailers(trailers.clone())),
        ];
        let mut request = Request::builder()
            .header("content-encoding", "zstd")
            .body(Body::new(StreamBody::new(futures_util::stream::iter(
                frames,
            ))))
            .unwrap();
        let capture = RequestCapture::inspect(&mut request);
        let body = request.into_body().collect().await.unwrap();
        assert_eq!(body.trailers(), Some(&trailers));
        assert_eq!(body.to_bytes().as_ref(), compressed);
        let observer = observer().await;
        let bytes = br#"{"usage":{"input_tokens":0,"output_tokens":1}}"#;
        let observed = observer
            .clone()
            .observe_http_with_context(
                "Alice".into(),
                response("application/json", bytes, 1),
                capture,
            )
            .await
            .unwrap();
        observed.into_body().collect().await.unwrap();
        assert_eq!(observer.ledger.entries().await[0].credits, 500_000);
    }

    #[tokio::test]
    async fn observation_limits_and_encoded_responses_do_not_break_forwarding() {
        let observer = observer().await;
        for kind in ["application/json", "text/event-stream"] {
            let bytes = vec![b'x'; MAX_REPORT_BYTES + 17];
            let observed = observer
                .clone()
                .observe_http("Alice".into(), response(kind, &bytes, 4096))
                .await
                .unwrap();
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
        }
        let mut encoded = response("application/json", b"encoded bytes", 1);
        encoded
            .headers_mut()
            .insert("content-encoding", "gzip".parse().unwrap());
        let observed = observer
            .clone()
            .observe_http("Alice".into(), encoded)
            .await
            .unwrap();
        assert_eq!(
            observed
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .as_ref(),
            b"encoded bytes"
        );
        let mut request = Request::new(Body::from(vec![b'x'; MAX_REPORT_BYTES + 1]));
        RequestCapture::inspect(&mut request);
        assert_eq!(
            request
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .len(),
            MAX_REPORT_BYTES + 1
        );
        let expanded = vec![b' '; MAX_REPORT_BYTES + 1];
        let compressed = zstd::encode_all(&expanded[..], 1).unwrap();
        assert!(request_context(&compressed, "zstd").is_none());
        assert!(observer.ledger.entries().await.is_empty());
    }

    #[tokio::test]
    async fn preserves_trailers_and_actual_upstream_body_errors() {
        let observer = observer().await;
        let mut trailers = HeaderMap::new();
        trailers.insert("x-trailer", "yes".parse().unwrap());
        for kind in ["text/event-stream", "application/json"] {
            let frames = vec![
                Ok::<_, std::io::Error>(Frame::data(Bytes::from_static(b"data: [DONE]\n\n"))),
                Ok(Frame::trailers(trailers.clone())),
            ];
            let original = Response::builder()
                .header("content-type", kind)
                .body(Body::new(StreamBody::new(futures_util::stream::iter(
                    frames,
                ))))
                .unwrap();
            let observed = observer
                .clone()
                .observe_http("Alice".into(), original)
                .await
                .unwrap()
                .into_body()
                .collect()
                .await
                .unwrap();
            assert_eq!(observed.trailers(), Some(&trailers));
            let frames = futures_util::stream::iter([Err::<Bytes, _>(std::io::Error::other(
                "upstream dropped",
            ))]);
            let original = Response::builder()
                .header("content-type", kind)
                .body(Body::from_stream(frames))
                .unwrap();
            assert!(
                observer
                    .clone()
                    .observe_http("Alice".into(), original)
                    .await
                    .unwrap()
                    .into_body()
                    .collect()
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn ambiguous_or_excessive_websocket_metadata_cannot_bill_the_wrong_request() {
        let observer = observer().await;
        let mut tracker = ResponseTracker::default();
        tracker.request(&json!({"model":"gpt-6-astra"}));
        tracker.request(&json!({"model":"gpt-6.1-sol"}));
        observer.message("Alice", br#"{"type":"response.completed","response":{"id":"unmapped","usage":{"input_tokens":0,"output_tokens":1}}}"#, &mut tracker).await;
        assert!(tracker.disabled && tracker.active.is_empty() && tracker.pending.is_empty());
        assert!(observer.ledger.entries().await.is_empty());
        // Explicit response metadata remains usable after request correlation is disabled.
        observer.message("Alice", br#"{"type":"response.completed","response":{"id":"explicit","model":"gpt-6.1-sol","usage":{"input_tokens":0,"output_tokens":1}}}"#, &mut tracker).await;
        assert_eq!(observer.ledger.entries().await[0].credits, 250_000);

        let mut tracker = ResponseTracker::default();
        for i in 0..=MAX_TRACKED_RESPONSES {
            tracker.created(&i.to_string());
        }
        assert!(
            tracker.active.is_empty(),
            "unsolicited IDs must not accumulate"
        );
        for _ in 0..=MAX_TRACKED_RESPONSES {
            tracker.request(&json!({"model":"gpt-6-astra"}));
        }
        assert!(tracker.disabled && tracker.active.is_empty() && tracker.pending.is_empty());
        let oversized_id = json!({"type":"response.completed","response":{"id":"x".repeat(MAX_RESPONSE_ID_BYTES + 1),"model":"gpt-6.1-sol","usage":{"input_tokens":0,"output_tokens":1}}});
        observer
            .message(
                "Alice",
                &serde_json::to_vec(&oversized_id).unwrap(),
                &mut tracker,
            )
            .await;
        assert_eq!(observer.ledger.entries().await.len(), 1);
    }

    #[tokio::test]
    async fn ignored_websocket_reports_do_not_allocate() {
        use std::{
            future::Future,
            pin::pin,
            task::{Context, Poll, Waker},
        };
        let observer = observer().await;
        let (_, control) = crate::allocation::measure(|| std::hint::black_box(Box::new(42)));
        assert!(control > 0);
        let (_, allocations) = crate::allocation::measure(|| {
            let future = observer.observe_ws("Alice", b"null");
            assert!(matches!(
                pin!(future).poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Ok(()))
            ));
        });
        assert_eq!(allocations, 0);
    }
}
