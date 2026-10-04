use axum::{body::Body, http::Uri};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};

pub type HttpClient = Client<HttpsConnector<HttpConnector>, Body>;

pub fn client() -> anyhow::Result<HttpClient> {
    let connector = HttpsConnectorBuilder::new()
        .with_native_roots()?
        .https_or_http()
        .enable_http1()
        .build();
    Ok(Client::builder(TokioExecutor::new()).build(connector))
}

pub fn base_url(base: &str) -> anyhow::Result<String> {
    let uri: Uri = base.parse()?;
    anyhow::ensure!(
        matches!(uri.scheme_str(), Some("http" | "https"))
            && uri.authority().is_some()
            && uri.query().is_none()
            && !base.contains('#')
            && !uri.authority().unwrap().as_str().contains('@'),
        "upstream must be an HTTP(S) base URL without credentials, query or fragment"
    );
    Ok(base.trim_end_matches('/').to_owned())
}
