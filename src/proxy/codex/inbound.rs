use std::{
    future::Future,
    sync::Arc,
    task::{Context, Poll},
};

use axum::{
    extract::{FromRequestParts, Request, ws::WebSocketUpgrade},
    http::header,
};
use subtle::ConstantTimeEq;
use tower::Service;

use crate::proxy::{Exchange, Inbound, ProxyError, Tag, Transport};

#[derive(Clone)]
pub struct CodexInbound {
    tag: Arc<str>,
    user: Arc<str>,
    key: Arc<str>,
}

impl CodexInbound {
    pub fn new(tag: String, user: String, key: String) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !tag.trim().is_empty() && !user.trim().is_empty(),
            "empty inbound tag or user"
        );
        anyhow::ensure!(!key.trim().is_empty(), "empty inbound key");
        Ok(Self {
            tag: tag.into(),
            user: user.into(),
            key: key.into(),
        })
    }

    fn authenticated(&self, req: &Request) -> bool {
        let mut values = req.headers().get_all(header::AUTHORIZATION).iter();
        let Some(value) = values.next().and_then(|value| value.to_str().ok()) else {
            return false;
        };
        if values.next().is_some() {
            return false;
        }
        let Some((scheme, token)) = value.split_once(' ') else {
            return false;
        };
        scheme.eq_ignore_ascii_case("bearer")
            && bool::from(token.as_bytes().ct_eq(self.key.as_bytes()))
    }

    async fn prepare(self, req: Request) -> Result<Exchange, ProxyError> {
        if !self.authenticated(&req) {
            return Err(ProxyError::Unauthorized);
        }
        let (mut parts, body) = req.into_parts();
        let transport = if parts.headers.contains_key(header::UPGRADE)
            || parts.headers.contains_key(header::SEC_WEBSOCKET_KEY)
        {
            Transport::Ws(
                WebSocketUpgrade::from_request_parts(&mut parts, &())
                    .await
                    .map_err(|error| ProxyError::BadRequest(anyhow::anyhow!(error)))?,
            )
        } else {
            Transport::Http
        };
        Ok(Exchange {
            req: Request::from_parts(parts, body),
            transport,
            user: self.user,
        })
    }
}

impl Tag for CodexInbound {
    fn tag(&self) -> &str {
        &self.tag
    }
}

impl Inbound for CodexInbound {
    fn accept(&self, req: &Request) -> bool {
        self.authenticated(req)
    }
}

impl Service<Request> for CodexInbound {
    type Response = Exchange;
    type Error = ProxyError;
    type Future = impl Future<Output = Result<Exchange, ProxyError>> + Send + 'static;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, req: Request) -> Self::Future {
        self.clone().prepare(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use tower::ServiceExt;

    #[tokio::test]
    async fn authenticates_identity_and_rejects_bad_or_ambiguous_credentials() {
        let inbound =
            CodexInbound::new("routing-tag".into(), "Alice".into(), "secret".into()).unwrap();
        let request = || {
            Request::builder()
                .header("authorization", "Bearer secret")
                .header("chatgpt-account-id", "Mallory")
                .header("x-user", "Mallory")
                .body(Body::empty())
                .unwrap()
        };
        let input = request();
        let (exchange, allocations) = crate::allocation::measure(|| {
            let mut handle = inbound.clone();
            let future = handle.call(input);
            match std::pin::pin!(future).poll(&mut Context::from_waker(std::task::Waker::noop())) {
                Poll::Ready(Ok(exchange)) => exchange,
                _ => panic!("HTTP preparation must complete immediately"),
            }
        });
        assert_eq!(
            allocations, 0,
            "cloning and preparing HTTP must not allocate"
        );
        assert!(Arc::ptr_eq(&exchange.user, &inbound.user));
        assert!(inbound.accept(&request()));
        let exchange = inbound.clone().oneshot(request()).await.unwrap();
        assert_eq!(&*exchange.user, "Alice");
        assert_ne!(&*exchange.user, inbound.tag());
        let mut invalid = request();
        invalid
            .headers_mut()
            .append(header::AUTHORIZATION, "Bearer other".parse().unwrap());
        assert!(!inbound.accept(&invalid));
        assert!(matches!(
            inbound.clone().oneshot(invalid).await,
            Err(ProxyError::Unauthorized)
        ));
        assert!(matches!(
            inbound.clone().oneshot(Request::new(Body::empty())).await,
            Err(ProxyError::Unauthorized)
        ));
        let malformed_upgrade = Request::builder()
            .header("authorization", "Bearer secret")
            .header("upgrade", "websocket")
            .body(Body::empty())
            .unwrap();
        assert!(matches!(
            inbound.clone().oneshot(malformed_upgrade).await,
            Err(ProxyError::BadRequest(_))
        ));
    }
}
