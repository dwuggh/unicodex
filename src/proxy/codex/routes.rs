use axum::http::{Method, Uri};
use serde::Deserialize;

use crate::proxy::ProxyError;

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    #[default]
    Broad,
    Strict,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Family {
    Model,
    Product,
    Api,
    Auth,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rewrite {
    Accounts,
    Usage,
}

pub struct Route {
    pub family: Family,
    pub path: String,
    pub query: Option<String>,
    pub rewrite: Option<Rewrite>,
    pub inference: bool,
}

/// Concrete routes in codex_web_api_reference.md. A placeholder matches exactly one segment.
/// HTTP MCP also uses GET/DELETE for its stream/session lifecycle.
const MODEL: &[(&str, &str)] = &[
    ("POST", "/responses"),
    ("WS", "/responses"),
    ("GET", "/models"),
    ("POST", "/alpha/search"),
    ("POST", "/memories/trace_summarize"),
    ("POST", "/images/generations"),
    ("POST", "/images/edits"),
    ("POST", "/realtime/calls"),
    ("WS", "/realtime"),
    ("WS", "/live"),
];
const PRODUCT: &[(&str, &str)] = &[
    ("GET", "/wham/accounts/check"),
    ("GET", "/wham/profiles/me"),
    ("POST", "/wham/accounts/send_add_credits_nudge_email"),
    ("GET", "/wham/tasks/list"),
    ("GET", "/wham/tasks/{task}"),
    ("GET", "/wham/tasks/{task}/turns/{turn}/sibling_turns"),
    ("POST", "/wham/tasks"),
    ("GET", "/wham/config/bundle"),
    ("GET", "/wham/settings/user"),
    ("GET", "/wham/workspace-messages"),
    ("GET", "/wham/usage"),
    ("GET", "/wham/rate-limit-reset-credits"),
    ("POST", "/wham/rate-limit-reset-credits/consume"),
    ("GET", "/wham/usage/plan_limit_history"),
    ("POST", "/wham/usage/thread-estimates/query"),
    ("POST", "/wham/usage/thread_usage/query_v2"),
    ("POST", "/wham/usage/thread_usage/query"),
    ("GET", "/wham/usage/daily-token-usage-breakdown"),
    ("GET", "/wham/usage/credit-usage-events"),
    (
        "GET",
        "/wham/usage/daily-workspace-user-token-usage-breakdown",
    ),
    ("GET", "/wham/usage/daily-workspace-user-credit-usage"),
    ("GET", "/wham/analytics/daily-workspace-usage-counts"),
    ("GET", "/wham/analytics/daily-plugin-usage-metrics"),
    ("GET", "/wham/analytics/daily-skill-usage-metrics"),
    ("GET", "/wham/environments"),
    ("GET", "/wham/environments/by-repo/github/{owner}/{repo}"),
    ("GET", "/connectors/directory/list"),
    ("GET", "/connectors/directory/list_workspace"),
    ("POST", "/ps/apps/batch"),
    ("POST", "/ps/mcp"),
    ("GET", "/ps/mcp"),
    ("DELETE", "/ps/mcp"),
    ("GET", "/ps/plugins/suggested/codex"),
    ("GET", "/ps/plugins/search"),
    ("GET", "/ps/plugins/list"),
    ("GET", "/ps/plugins/workspace/shared"),
    ("GET", "/ps/plugins/installed"),
    ("GET", "/ps/plugins/{plugin}"),
    ("GET", "/ps/plugins/{plugin}/skills/{skill}"),
    ("POST", "/ps/plugins/{plugin}/install"),
    ("POST", "/ps/plugins/{plugin}/uninstall"),
    ("GET", "/ps/plugins/workspace/created"),
    ("PUT", "/ps/plugins/{plugin}/shares"),
    ("POST", "/public/plugins/workspace/upload-url"),
    ("POST", "/public/plugins/workspace"),
    ("POST", "/public/plugins/workspace/{id}"),
    ("DELETE", "/public/plugins/workspace/{id}"),
    ("POST", "/files"),
    ("POST", "/files/{id}/uploaded"),
    ("GET", "/wham/agent-identities/jwks"),
    ("POST", "/wham/remote/control/server/enroll"),
    ("POST", "/wham/remote/control/server/refresh"),
    ("WS", "/wham/remote/control/server"),
    ("POST", "/wham/remote/control/server/pair"),
    ("POST", "/wham/remote/control/server/pair/status"),
];
const AUTH: &[(&str, &str)] = &[
    ("GET", "/oauth/authorize"),
    ("POST", "/oauth/token"),
    ("POST", "/oauth/revoke"),
    ("POST", "/api/accounts/deviceauth/usercode"),
    ("POST", "/api/accounts/deviceauth/token"),
    ("GET", "/codex/device"),
    ("GET", "/api/accounts/v1/user-auth-credential/whoami"),
    ("POST", "/api/accounts/v1/agent/register"),
    ("POST", "/api/accounts/v1/agent/{id}/task/register"),
];

fn matches(pattern: &str, path: &str) -> bool {
    let mut expected = pattern.split('/');
    let mut actual = path.split('/');
    loop {
        match (expected.next(), actual.next()) {
            (None, None) => return true,
            (Some(a), Some(b)) if a == b || (a.starts_with('{') && !b.is_empty()) => {}
            _ => return false,
        }
    }
}

pub fn classify(mode: Mode, method: &Method, uri: &Uri, ws: bool) -> Result<Route, ProxyError> {
    let raw = uri.path();
    // Never let encoded separators or dot segments change the destination after matching.
    let lower = raw.to_ascii_lowercase();
    if lower.contains("%2f")
        || lower.contains("%5c")
        || lower.contains("%2e")
        || lower.contains("%25")
        || raw.contains('\\')
        || raw.contains("//")
        || raw.split('/').any(|s| s == "." || s == "..")
    {
        return Err(ProxyError::NoRoute);
    }
    let (family, path) = if let Some(path) = raw.strip_prefix("/backend-api/codex/") {
        (Family::Model, format!("/{path}"))
    } else if let Some(path) = raw.strip_prefix("/backend-api/") {
        (Family::Product, format!("/{path}"))
    } else if let Some(path) = raw.strip_prefix("/api/codex/") {
        (Family::Product, format!("/wham/{path}"))
    } else if let Some(path) = raw.strip_prefix("/v1/") {
        (Family::Api, format!("/{path}"))
    } else if let Some(path) = raw.strip_prefix("/auth/") {
        (Family::Auth, format!("/{path}"))
    } else if raw.starts_with("/oauth/")
        || raw.starts_with("/api/accounts/")
        || raw == "/codex/device"
    {
        (Family::Auth, raw.to_owned())
    } else {
        (Family::Model, raw.to_owned())
    };
    let verb = if ws && method == Method::GET {
        "WS"
    } else if ws {
        "INVALID"
    } else {
        method.as_str()
    };
    let catalog = match family {
        Family::Model | Family::Api => MODEL,
        Family::Product => PRODUCT,
        Family::Auth => AUTH,
    };
    let known = catalog.iter().any(|(m, p)| *m == verb && matches(p, &path))
        || (family == Family::Api
            && verb == "POST"
            && matches!(path.as_str(), "/live" | "/analytics/codex/turn-costs"));
    if mode == Mode::Strict && !known {
        return Err(ProxyError::NoRoute);
    }
    let rewrite = match (family, verb, path.as_str()) {
        (Family::Product, "GET", "/wham/accounts/check") => Some(Rewrite::Accounts),
        (Family::Product, "GET", "/wham/usage") => Some(Rewrite::Usage),
        _ => None,
    };
    let inference = match family {
        Family::Model | Family::Api => {
            (verb == "POST" || verb == "WS") && !path.starts_with("/analytics/")
        }
        Family::Product => verb == "POST" && path == "/wham/tasks",
        Family::Auth => false,
    };
    Ok(Route {
        family,
        path,
        query: uri.query().map(str::to_owned),
        rewrite,
        inference,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn strict_catalog_routes_families_and_rejects_ambiguous_or_unlisted_paths() {
        for (verb, path, family, target) in [
            ("POST", "/responses", Family::Model, "/responses"),
            (
                "WS",
                "/backend-api/codex/responses",
                Family::Model,
                "/responses",
            ),
            (
                "GET",
                "/backend-api/wham/tasks/task1/turns/turn2/sibling_turns",
                Family::Product,
                "/wham/tasks/task1/turns/turn2/sibling_turns",
            ),
            (
                "POST",
                "/backend-api/files/file1/uploaded",
                Family::Product,
                "/files/file1/uploaded",
            ),
            (
                "GET",
                "/backend-api/ps/plugins/plugin1/skills/skill2",
                Family::Product,
                "/ps/plugins/plugin1/skills/skill2",
            ),
            ("DELETE", "/backend-api/ps/mcp", Family::Product, "/ps/mcp"),
            ("POST", "/auth/oauth/token", Family::Auth, "/oauth/token"),
            ("GET", "/oauth/authorize", Family::Auth, "/oauth/authorize"),
            ("POST", "/v1/live", Family::Api, "/live"),
        ] {
            let uri: Uri = format!("{path}?x=1&x=2").parse().unwrap();
            let method: Method = if verb == "WS" { "GET" } else { verb }.parse().unwrap();
            let route = classify(Mode::Strict, &method, &uri, verb == "WS").unwrap();
            assert_eq!(route.family, family);
            assert_eq!(route.path, target);
            assert_eq!(route.query.as_deref(), Some("x=1&x=2"));
        }
        for (method, path, ws) in [
            (Method::DELETE, "/responses", false),
            (Method::GET, "/responses", false),
            (Method::POST, "/responses", true),
            (Method::GET, "/models", true),
            (Method::POST, "/responses/extra", false),
            (Method::POST, "/backend-api/files/x/y/uploaded", false),
        ] {
            assert!(classify(Mode::Strict, &method, &path.parse().unwrap(), ws).is_err());
        }
        for path in [
            "/backend-api/files/%2e%2e/uploaded",
            "/backend-api/files/x%2fy/uploaded",
            "/backend-api/files/%252e%252e/uploaded",
            "/backend-api/files/../uploaded",
            "/backend-api//wham/usage",
        ] {
            for mode in [Mode::Broad, Mode::Strict] {
                assert!(classify(mode, &Method::POST, &path.parse().unwrap(), false).is_err());
            }
        }
        assert!(
            classify(
                Mode::Broad,
                &Method::POST,
                &"/custom".parse().unwrap(),
                false
            )
            .is_ok()
        );
        let route = classify(
            Mode::Strict,
            &Method::GET,
            &"/api/codex/usage".parse().unwrap(),
            false,
        )
        .unwrap();
        assert_eq!(route.rewrite, Some(Rewrite::Usage));
    }
}
