use std::{
    future::Future,
    sync::Arc,
    task::{Context, Poll},
};

use axum::{
    extract::{Request, ws::WebSocketUpgrade},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use tower::Service;

pub mod codex;

use crate::ledger::{Ledger, WeeklyWindow};
use codex::{inbound::CodexInbound, outbound::CodexOutbound};

#[derive(Debug, thiserror::Error)]
pub enum ProxyError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("no matching route")]
    NoRoute,
    #[error("insufficient credits")]
    NoCredits,
    #[error("invalid request")]
    BadRequest(#[source] anyhow::Error),
    #[error("upstream request failed")]
    Upstream(#[source] anyhow::Error),
    #[error("upstream authentication unavailable")]
    UpstreamAuth(#[source] anyhow::Error),
    #[error("internal error")]
    Internal(#[from] anyhow::Error),
}

impl ProxyError {
    pub(crate) fn status(&self) -> StatusCode {
        match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::NoRoute => StatusCode::NOT_FOUND,
            Self::NoCredits => StatusCode::TOO_MANY_REQUESTS,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Upstream(_) | Self::UpstreamAuth(_) => StatusCode::BAD_GATEWAY,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::Unauthorized => "unauthorized",
            Self::NoRoute => "no_route",
            Self::NoCredits => "insufficient_credits",
            Self::BadRequest(_) => "bad_request",
            Self::Upstream(_) => "upstream",
            Self::UpstreamAuth(_) => "upstream_auth",
            Self::Internal(_) => "internal",
        }
    }
}

impl IntoResponse for ProxyError {
    fn into_response(self) -> Response {
        if matches!(self, Self::NoCredits) {
            return (self.status(), axum::Json(quota_error())).into_response();
        }
        (self.status(), self.to_string()).into_response()
    }
}

pub(crate) fn quota_error() -> serde_json::Value {
    serde_json::json!({"error": {
        "type": "insufficient_quota", "code": "insufficient_quota",
        "message": "Insufficient local credits"
    }})
}

pub enum Transport {
    Http,
    Ws(WebSocketUpgrade),
}

pub struct Exchange {
    pub req: Request,
    pub transport: Transport,
    /// Identity established by authentication, independent of routing tags.
    pub user: Arc<str>,
}

pub trait Tag {
    fn tag(&self) -> &str;
}

pub trait Inbound:
    Service<Request, Response = Exchange, Error = ProxyError, Future: Send + 'static>
    + Tag
    + Clone
    + Send
    + Sync
    + 'static
{
    fn accept(&self, req: &Request) -> bool;
}

pub struct OutboundInput {
    pub exchange: Exchange,
    pub admission: Arc<Ledger>,
}

pub trait Outbound:
    Service<OutboundInput, Response = Response, Error = ProxyError, Future: Send + 'static>
    + Tag
    + Clone
    + Send
    + Sync
    + 'static
{
    type Observer: Observer;

    fn observer(&self) -> &Arc<Self::Observer>;
}

pub trait Admission: Send + Sync + 'static {
    fn check<'a>(
        &'a self,
        user: &'a str,
        window: Option<WeeklyWindow>,
    ) -> impl Future<Output = Result<(), ProxyError>> + Send + 'a;
}

pub trait Observer: Send + Sync + 'static {
    fn observe_http(
        self: Arc<Self>,
        user: Arc<str>,
        response: Response,
    ) -> impl Future<Output = anyhow::Result<Response>> + Send + 'static;

    /// Observe accounting without replacing message bytes or their type.
    fn observe_ws<'a>(
        &'a self,
        user: &'a str,
        message: &'a [u8],
    ) -> impl Future<Output = anyhow::Result<()>> + Send + 'a;
}

#[derive(Clone)]
pub enum InboundKind {
    Codex(CodexInbound),
}

impl Tag for InboundKind {
    fn tag(&self) -> &str {
        match self {
            Self::Codex(inbound) => inbound.tag(),
        }
    }
}

impl Inbound for InboundKind {
    fn accept(&self, req: &Request) -> bool {
        match self {
            Self::Codex(inbound) => inbound.accept(req),
        }
    }
}

impl Service<Request> for InboundKind {
    type Response = Exchange;
    type Error = ProxyError;
    type Future = <CodexInbound as Service<Request>>::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self {
            Self::Codex(inbound) => inbound.poll_ready(cx),
        }
    }

    fn call(&mut self, req: Request) -> Self::Future {
        match self {
            Self::Codex(inbound) => inbound.call(req),
        }
    }
}

#[derive(Clone)]
pub enum OutboundKind {
    Codex(CodexOutbound),
}

impl Tag for OutboundKind {
    fn tag(&self) -> &str {
        match self {
            Self::Codex(outbound) => outbound.tag(),
        }
    }
}

impl Outbound for OutboundKind {
    type Observer = <CodexOutbound as Outbound>::Observer;

    fn observer(&self) -> &Arc<Self::Observer> {
        match self {
            Self::Codex(outbound) => outbound.observer(),
        }
    }
}

impl Service<OutboundInput> for OutboundKind {
    type Response = Response;
    type Error = ProxyError;
    type Future = <CodexOutbound as Service<OutboundInput>>::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self {
            Self::Codex(outbound) => outbound.poll_ready(cx),
        }
    }

    fn call(&mut self, input: OutboundInput) -> Self::Future {
        match self {
            Self::Codex(outbound) => outbound.call(input),
        }
    }
}
