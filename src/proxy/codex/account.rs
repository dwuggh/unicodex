use std::sync::Arc;

use axum::{
    body::{Body, Bytes},
    http::HeaderMap,
    response::Response,
};
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Frame;
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde_json::{Value, json};
use tokio::sync::Mutex;

use super::routes::Rewrite;
use crate::ledger::{Ledger, WeeklyCredits, WeeklyWindow};

/// Shared by handles for one outbound; never used as an accounting identity.
#[derive(Default)]
pub struct CachedWindow {
    account: Option<Arc<str>>,
    window: Option<WeeklyWindow>,
}

impl CachedWindow {
    pub fn get(&self, account: Option<&str>, now: i64) -> Option<WeeklyWindow> {
        self.window
            .filter(|window| self.account.as_deref() == account && window.contains(now))
    }

    pub fn update(&mut self, account: Option<&str>, window: Option<WeeklyWindow>) {
        self.account = account.map(Arc::from);
        self.window = window;
    }
}

pub fn weekly_window(value: &Value) -> Option<(&'static str, WeeklyWindow)> {
    let limit = value.get("rate_limit")?;
    for field in ["secondary_window", "primary_window"] {
        let Some(window) = limit.get(field) else {
            continue;
        };
        if window.get("limit_window_seconds").and_then(Value::as_i64) == Some(604800) {
            let Some(end) = window.get("reset_at").and_then(Value::as_i64) else {
                continue;
            };
            let Some(start) = end.checked_sub(604800) else {
                continue;
            };
            return Some((field, WeeklyWindow { start, end }));
        }
    }
    None
}

/// Only successful account discovery and status reads need a response adapter.
pub async fn rewrite(
    ledger: &Ledger,
    user: &str,
    response: Response,
    kind: Rewrite,
    client_account: Option<&str>,
    upstream_account: Option<&str>,
    window_cache: &Mutex<CachedWindow>,
) -> anyhow::Result<Response> {
    if !response.status().is_success() {
        return Ok(response);
    }
    anyhow::ensure!(
        response
            .headers()
            .get("content-encoding")
            .is_none_or(|v| v == "identity"),
        "encoded account response"
    );
    let (mut parts, body) = response.into_parts();
    let collected = http_body_util::Limited::new(body, 16 * 1024 * 1024)
        .collect()
        .await
        .map_err(|_| anyhow::anyhow!("account response could not be read within size limit"))?;
    let mut trailers = collected.trailers().cloned();
    let bytes = collected.to_bytes();
    let mut value: Value = serde_json::from_slice(&bytes)?;
    let original = value.clone();
    match kind {
        Rewrite::Accounts => accounts(&mut value, client_account, upstream_account)?,
        Rewrite::Usage => {
            let now = chrono::Utc::now().timestamp();
            let reported = weekly_window(&value).filter(|(_, window)| window.contains(now));
            let bounds = {
                let mut cache = window_cache.lock().await;
                if let Some((_, window)) = reported {
                    cache.update(upstream_account, Some(window));
                    Some(window)
                } else {
                    cache.get(upstream_account, now)
                }
            };
            // A sparse status read must not erase valid bounds already discovered for
            // this account. Restore a local weekly bar using those same quota bounds.
            if reported.is_none()
                && let Some(window) = bounds
                && let Some(limit) = value.get_mut("rate_limit").and_then(Value::as_object_mut)
            {
                limit.insert(
                    "secondary_window".into(),
                    json!({
                        "used_percent": 0,
                        "limit_window_seconds": 604800,
                        "reset_at": window.end,
                        "reset_after_seconds": window.end - now,
                    }),
                );
            }
            let WeeklyCredits::Limited(allowance) = ledger.weekly_credits(user)? else {
                anyhow::bail!("unlimited status must use passthrough observation");
            };
            usage(ledger, user, &mut value, allowance, now).await?
        }
    }
    let bytes = if value != original {
        invalidate_body_headers(&mut parts.headers);
        parts.headers.insert("cache-control", "no-store".parse()?);
        if let Some(headers) = &mut trailers {
            invalidate_body_headers(headers);
        }
        Bytes::from(serde_json::to_vec(&value)?)
    } else {
        bytes
    };
    let body = if let Some(trailers) = trailers {
        Body::new(StreamBody::new(futures_util::stream::iter([
            Ok::<_, std::io::Error>(Frame::data(bytes)),
            Ok(Frame::trailers(trailers)),
        ])))
    } else {
        Body::from(bytes)
    };
    Ok(Response::from_parts(parts, body))
}

fn accounts(
    value: &mut Value,
    client_account: Option<&str>,
    upstream_account: Option<&str>,
) -> anyhow::Result<()> {
    // Codex's map deserializer drops routing fields. Normalize maps to its list shape,
    // retaining each entry's metadata, then adapt only the configured upstream account.
    if let Some(map) = value.get("accounts").and_then(Value::as_object) {
        let mut entries = Vec::new();
        let mut references = std::collections::HashMap::new();
        for (key, wrapper) in map {
            let account = wrapper
                .get("account")
                .and_then(Value::as_object)
                .ok_or_else(|| anyhow::anyhow!("invalid account discovery entry"))?;
            let id = account
                .get("account_id")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("missing upstream account id"))?;
            references.insert(key.clone(), id.to_owned());
            let mut entry = wrapper.as_object().cloned().unwrap_or_default();
            entry.extend(account.clone());
            entry.insert("id".into(), json!(id));
            entries.push(Value::Object(entry));
        }
        for field in ["account_ordering", "default_account_id"] {
            if let Some(field) = value.get_mut(field) {
                remap_references(field, &references);
            }
        }
        value["accounts"] = Value::Array(entries);
    }
    let requested = client_account.filter(|id| !id.trim().is_empty());
    let selected = upstream_account
        .map(str::to_owned)
        .or_else(|| {
            value
                .get("default_account_id")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .or_else(|| {
            let entries = value.get("accounts")?.as_array()?;
            (entries.len() == 1)
                .then(|| entries[0].get("id")?.as_str().map(str::to_owned))
                .flatten()
        })
        .or_else(|| requested.map(str::to_owned))
        .ok_or_else(|| anyhow::anyhow!("missing selected upstream account"))?;
    let entries = value
        .get_mut("accounts")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| anyhow::anyhow!("invalid account discovery response"))?;
    let matched: Vec<_> = entries
        .iter()
        .enumerate()
        .filter(|(_, entry)| entry.get("id").and_then(Value::as_str) == Some(&selected))
        .map(|(index, _)| index)
        .collect();
    anyhow::ensure!(
        matched.len() == 1,
        "missing or duplicate selected upstream account"
    );
    let alias = requested.unwrap_or(&selected);
    anyhow::ensure!(
        !entries
            .iter()
            .enumerate()
            .any(|(i, entry)| i != matched[0]
                && entry.get("id").and_then(Value::as_str) == Some(alias)),
        "ambiguous client account alias"
    );
    let entry = &mut entries[matched[0]];
    entry["id"] = json!(alias);
    // Keep discovery inside the configured gateway instead of directing model traffic
    // to a regional upstream origin that would bypass gateway authentication/accounting.
    entry["workspace_backend_origin"] = json!("NO_CONSTRAINT");
    if entry
        .get("account_routing_override")
        .is_none_or(Value::is_null)
    {
        entry["account_routing_override"] = json!("NO_CONSTRAINT");
    }
    let references = [(selected.clone(), alias.to_owned())].into_iter().collect();
    for field in ["account_ordering", "default_account_id"] {
        if let Some(value) = value.get_mut(field) {
            remap_references(value, &references);
        }
    }
    Ok(())
}

fn remap_references(value: &mut Value, mapping: &std::collections::HashMap<String, String>) {
    match value {
        Value::String(id) => {
            if let Some(mapped) = mapping.get(id) {
                *id = mapped.clone();
            }
        }
        Value::Array(ids) => {
            for id in ids {
                remap_references(id, mapping);
            }
        }
        _ => {}
    }
}

async fn usage(
    ledger: &Ledger,
    user: &str,
    value: &mut Value,
    allowance: Decimal,
    now: i64,
) -> anyhow::Result<()> {
    anyhow::ensure!(value.is_object(), "invalid usage response");
    // These are optional display fields in RateLimitStatusPayload. Preserve permission
    // flags and metadata; show only the local user's percentage in the weekly window.
    if value.get("credits").is_some() {
        value["credits"] = Value::Null;
    }
    let weekly = weekly_window(value)
        .filter(|(_, window)| window.contains(now))
        .map(|(field, window)| (field, window, value["rate_limit"][field].clone()));
    if weekly.is_none() {
        tracing::warn!(
            operation = "adapt_usage",
            reason = "current_weekly_window_missing",
            "local usage bar unavailable; upstream weekly quota bounds are required"
        );
    }
    if let Some(limit) = value.get_mut("rate_limit") {
        hide_windows(limit);
        if let Some((field, bounds, mut window)) = weekly
            && let used = ledger
                .consumed_credits(user, bounds.start, bounds.end, now)
                .await?
            && let Some(percent) = used_percent(used, allowance)
        {
            // Keep the upstream window and reset metadata exactly as received.
            window["used_percent"] = json!(percent);
            limit[field] = window;
        }
    }
    if let Some(additional) = value
        .get_mut("additional_rate_limits")
        .and_then(Value::as_array_mut)
    {
        for limit in additional {
            if let Some(limit) = limit.get_mut("rate_limit") {
                hide_windows(limit);
            }
        }
    }
    if let Some(control) = value
        .get_mut("spend_control")
        .and_then(Value::as_object_mut)
        && let Some(limit) = control.get_mut("individual_limit")
    {
        *limit = Value::Null;
    }
    Ok(())
}

fn hide_windows(value: &mut Value) {
    if let Some(object) = value.as_object_mut() {
        for field in ["primary_window", "secondary_window"] {
            if object.contains_key(field) {
                object.insert(field.into(), Value::Null);
            }
        }
    }
}

fn used_percent(used: Decimal, allowance: Decimal) -> Option<i32> {
    if allowance.is_zero() || used >= allowance {
        return Some(100);
    }
    used.checked_div(allowance)?
        .checked_mul(Decimal::from(100))?
        .round()
        .to_i32()
}

pub fn invalidate_body_headers(headers: &mut HeaderMap) {
    for name in [
        "content-length",
        "etag",
        "content-md5",
        "digest",
        "content-digest",
        "repr-digest",
    ] {
        headers.remove(name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovery_preserves_metadata_and_matches_saved_workspace_for_both_formats() {
        let selected = json!({"id":"upstream", "plan_type":"team", "name":"Engineering",
            "structure":"workspace", "profile_picture_url":"https://example.invalid/icon",
            "workspace_backend_origin":"https://us.chatgpt.com", "account_routing_override":"us",
            "features":{"future_field":true}});
        let other = json!({"id":"other", "name":"Personal", "plan_type":"plus"});
        let original = json!({"accounts":[selected,other], "default_account_id":"upstream",
            "account_ordering":["other","upstream"], "extra":"untouched"});
        let mut value = original.clone();
        accounts(&mut value, Some("saved-in-auth-json"), Some("upstream")).unwrap();
        let mut expected = original.clone();
        expected["accounts"][0]["id"] = json!("saved-in-auth-json");
        expected["accounts"][0]["workspace_backend_origin"] = json!("NO_CONSTRAINT");
        expected["default_account_id"] = json!("saved-in-auth-json");
        expected["account_ordering"][1] = json!("saved-in-auth-json");
        assert_eq!(value, expected);
        assert!(accounts(&mut original.clone(), Some("other"), Some("upstream")).is_err());
        assert!(accounts(&mut original.clone(), Some("saved"), Some("missing")).is_err());

        let mut value = json!({"accounts":{
            "workspace-key":{"account":{"account_id":"upstream", "plan_type":"team", "name":"Engineering", "structure":"workspace"}, "entitlement":{"enabled":true}},
            "personal-key":{"account":{"account_id":"personal", "plan_type":"plus"}}
        }, "account_ordering":["workspace-key","personal-key"], "default_account_id":"workspace-key"});
        accounts(&mut value, Some("saved"), Some("upstream")).unwrap();
        let entries = value["accounts"].as_array().unwrap();
        let selected = entries.iter().find(|entry| entry["id"] == "saved").unwrap();
        assert_eq!(selected["plan_type"], "team");
        assert_eq!(selected["entitlement"], json!({"enabled":true}));
        assert_eq!(selected["workspace_backend_origin"], "NO_CONSTRAINT");
        assert_eq!(selected["account_routing_override"], "NO_CONSTRAINT");
        assert_eq!(value["account_ordering"], json!(["saved", "personal"]));
        assert_eq!(value["default_account_id"], "saved");
        let mut no_header = original;
        accounts(&mut no_header, None, Some("upstream")).unwrap();
        assert_eq!(no_header["accounts"][0]["id"], "upstream");
    }

    async fn ledger() -> Ledger {
        Ledger::in_memory(
            [(Arc::from("Alice"), WeeklyCredits::Limited(Decimal::ONE))]
                .into_iter()
                .collect(),
        )
        .await
        .unwrap()
    }

    #[test]
    fn malformed_secondary_window_does_not_hide_a_valid_weekly_primary() {
        let payload = json!({"rate_limit":{
            "secondary_window":{"limit_window_seconds":604800,"reset_at":null},
            "primary_window":{"limit_window_seconds":604800,"reset_at":1800000000}
        }});
        let (field, window) = weekly_window(&payload).unwrap();
        assert_eq!(field, "primary_window");
        assert_eq!(window.end, 1800000000);
    }

    #[tokio::test]
    async fn sparse_status_preserves_the_local_weekly_bar_only_for_valid_account_bounds() {
        let ledger = ledger().await;
        ledger
            .record_charge(
                "Alice",
                crate::ledger::CreditAmount::from_decimal(Decimal::new(25, 2)).unwrap(),
            )
            .await
            .unwrap();
        let now = chrono::Utc::now().timestamp();
        let window = WeeklyWindow {
            start: now - 3600,
            end: now + 601200,
        };
        let cache = Mutex::new(CachedWindow::default());
        cache.lock().await.update(Some("upstream"), Some(window));
        let sparse = json!({"plan_type":"plus", "rate_limit":{
            "allowed":true,"limit_reached":false,
            "primary_window":{"used_percent":95,"limit_window_seconds":18000}
        }});
        let response = rewrite(
            &ledger,
            "Alice",
            Response::new(Body::from(sparse.to_string())),
            Rewrite::Usage,
            None,
            Some("upstream"),
            &cache,
        )
        .await
        .unwrap();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let local: Value = serde_json::from_slice(&body).unwrap();
        let bar = &local["rate_limit"]["secondary_window"];
        assert_eq!(bar["used_percent"], 25);
        assert_eq!(bar["limit_window_seconds"], 604800);
        assert_eq!(bar["reset_at"], window.end);
        assert!(bar["reset_after_seconds"].as_i64().unwrap() > 0);
        assert_eq!(local["rate_limit"]["allowed"], true);
        assert!(local["rate_limit"]["primary_window"].is_null());
        assert!(cache.lock().await.get(Some("upstream"), now).is_some());

        for (account, bounds) in [
            (Some("different-account"), window),
            (
                Some("upstream"),
                WeeklyWindow {
                    start: now - 604800,
                    end: now - 1,
                },
            ),
        ] {
            cache.lock().await.update(account, Some(bounds));
            let response = rewrite(
                &ledger,
                "Alice",
                Response::new(Body::from(sparse.to_string())),
                Rewrite::Usage,
                None,
                Some("upstream"),
                &cache,
            )
            .await
            .unwrap();
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let local: Value = serde_json::from_slice(&body).unwrap();
            assert!(local["rate_limit"]["secondary_window"].is_null());
        }
        assert_eq!(ledger.entries().await.len(), 1);
    }

    fn status(reset: i64) -> Value {
        json!({"account_id":"upstream", "user_id":"upstream-user", "plan_type":"team",
            "rate_limit":{"allowed":false,"limit_reached":true,"primary_window":{"used_percent":99},
                "secondary_window":{"used_percent":90,"limit_window_seconds":604800,"reset_at":reset,"reset_after_seconds":601200}},
            "credits":{"balance":"900","has_credits":true,"unlimited":false},
            "spend_control":{"reached":true,"individual_limit":{
                "source":"workspace", "limit":"100", "used":"90", "remaining":"10",
                "used_percent":90,"remaining_percent":10,"reset_at":reset,"reset_after_seconds":2505600}},
            "additional_rate_limits":[{"limit_name":"Extra", "metered_feature":"extra", "normal_model_slug":"gpt-extra",
                "rate_limit":{"allowed":true,"limit_reached":false,"primary_window":{"used_percent":80}}}],
            "rate_limit_reset_credits":{"available_count":1}, "rate_limit_upsell":{"action":"preserved"},
            "rate_limit_reached_type":{"type":"workspace_member_usage_limit_reached"},"unknown":"preserved"})
    }

    #[tokio::test]
    async fn status_uses_only_user_consumption_and_preserves_upstream_policy_and_reset() {
        let ledger = ledger().await;
        let now = chrono::Utc::now().timestamp();
        let start = now - 3600;
        let end = start + 7 * 86400;
        for (user, amount) in [("Alice", "0.1"), ("Alice", "0.2"), ("Bob", "999")] {
            ledger
                .record_charge(
                    user,
                    crate::ledger::CreditAmount::from_decimal(
                        Decimal::from_str_exact(amount).unwrap(),
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
        }
        let original = status(end);
        let mut value = original.clone();
        usage(&ledger, "Alice", &mut value, Decimal::ONE, now)
            .await
            .unwrap();
        let mut expected = original.clone();
        expected["credits"] = Value::Null;
        expected["rate_limit"]["primary_window"] = Value::Null;
        expected["rate_limit"]["secondary_window"]["used_percent"] = json!(30);
        expected["additional_rate_limits"][0]["rate_limit"]["primary_window"] = Value::Null;
        expected["spend_control"]["individual_limit"] = Value::Null;
        assert_eq!(value, expected);
        assert_eq!(
            ledger.entries().await.len(),
            3,
            "status snapshots must not create charges"
        );
        let mut absent_limit = json!({"plan_type":"plus", "unknown":true});
        let original = absent_limit.clone();
        usage(&ledger, "Alice", &mut absent_limit, Decimal::ONE, now)
            .await
            .unwrap();
        assert_eq!(absent_limit, original);
    }

    #[tokio::test]
    async fn adapters_preserve_errors_and_trailers_and_surface_database_failure() {
        let ledger = ledger().await;
        let error = Response::builder()
            .status(429)
            .header("etag", "original")
            .body(Body::from("upstream error"))
            .unwrap();
        let error = rewrite(
            &ledger,
            "Alice",
            error,
            Rewrite::Usage,
            None,
            None,
            &Mutex::default(),
        )
        .await
        .unwrap();
        assert_eq!(error.status(), 429);
        assert_eq!(error.headers()["etag"], "original");
        assert_eq!(
            error.into_body().collect().await.unwrap().to_bytes(),
            "upstream error"
        );
        let mut trailers = HeaderMap::new();
        trailers.insert("digest", "stale".parse().unwrap());
        trailers.insert("x-trailer", "keep".parse().unwrap());
        let response = Response::builder()
            .header("etag", "stale")
            .header("x-extra", "keep")
            .body(Body::new(StreamBody::new(futures_util::stream::iter([
                Ok::<_, std::io::Error>(Frame::data(Bytes::from_static(
                    br#"{"plan_type":"plus","credits":{"balance":"9"}}"#,
                ))),
                Ok(Frame::trailers(trailers)),
            ]))))
            .unwrap();
        let response = rewrite(
            &ledger,
            "Alice",
            response,
            Rewrite::Usage,
            None,
            None,
            &Mutex::default(),
        )
        .await
        .unwrap();
        assert!(!response.headers().contains_key("etag"));
        assert_eq!(response.headers()["x-extra"], "keep");
        let body = response.into_body().collect().await.unwrap();
        assert_eq!(body.trailers().unwrap()["x-trailer"], "keep");
        assert!(!body.trailers().unwrap().contains_key("digest"));
        assert_eq!(
            serde_json::from_slice::<Value>(&body.to_bytes()).unwrap(),
            json!({"plan_type":"plus","credits":null})
        );
        ledger.execute("DROP TABLE credit_entries").await;
        assert!(
            rewrite(
                &ledger,
                "Alice",
                Response::new(Body::from(
                    status(chrono::Utc::now().timestamp() + 601200).to_string()
                )),
                Rewrite::Usage,
                None,
                None,
                &Mutex::default()
            )
            .await
            .is_err()
        );
    }
}
