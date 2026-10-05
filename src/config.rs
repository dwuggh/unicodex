use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::proxy::{InboundKind, OutboundKind};
use anyhow::Context;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    app::{App, Rule},
    ledger::{Ledger, WeeklyCredits},
    proxy::codex::{
        auth::{CredentialManager, Credentials},
        inbound::CodexInbound,
        outbound::{CodexOutbound, OutboundOptions},
        routes::Mode,
        stat::CodexObserver,
    },
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub server: Server,
    #[serde(default = "default_database")]
    pub database: PathBuf,
    pub inbounds: Vec<InboundConfig>,
    pub outbounds: Vec<OutboundConfig>,
    pub routing: Routing,
}

fn default_database() -> PathBuf {
    "unicodex.turso.db".into()
}
fn default_upstream() -> String {
    "https://chatgpt.com/backend-api/codex".into()
}
fn default_chatgpt() -> String {
    "https://chatgpt.com/backend-api".into()
}
fn default_api() -> String {
    "https://api.openai.com/v1".into()
}
fn default_auth() -> String {
    "https://auth.openai.com".into()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Server {
    pub listen: SocketAddr,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InboundConfig {
    pub id: String,
    /// Authenticated user identity. `name` is accepted for the original YAML example.
    #[serde(alias = "name")]
    pub user: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub key: String,
    #[serde(deserialize_with = "weekly_credits")]
    pub weekly_credits: WeeklyCredits,
}

fn weekly_credits<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<WeeklyCredits, D::Error> {
    let value = String::deserialize(deserializer)?;
    if value == "unlimited" {
        return Ok(WeeklyCredits::Unlimited);
    }
    let amount = Decimal::from_str_exact(&value).map_err(serde::de::Error::custom)?;
    if amount.is_sign_negative() {
        return Err(serde::de::Error::custom(
            "weekly credits must be nonnegative",
        ));
    }
    Ok(WeeklyCredits::Limited(amount))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboundConfig {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default = "default_upstream")]
    pub base_url: String,
    #[serde(default)]
    pub mode: Mode,
    #[serde(default = "default_chatgpt")]
    pub chatgpt_base_url: String,
    #[serde(default = "default_api")]
    pub api_base_url: String,
    #[serde(default = "default_auth")]
    pub auth_base_url: String,
    pub auth_file: Option<PathBuf>,
    pub token_env: Option<String>,
    pub account_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Routing {
    pub rules: Vec<RuleConfig>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleConfig {
    #[serde(alias = "inbounds")]
    pub inbound_ids: Vec<String>,
    #[serde(alias = "outbound")]
    pub outbound_id: String,
}

impl Config {
    pub fn read(path: &Path) -> anyhow::Result<Self> {
        let mut config: Self = serde_yaml::from_str(
            &std::fs::read_to_string(path)
                .with_context(|| format!("read configuration {}", path.display()))?,
        )
        .context("parse YAML configuration")?;
        let parent = path.parent().unwrap_or(Path::new("."));
        if config.database.is_relative() {
            config.database = parent.join(&config.database);
        }
        for outbound in &mut config.outbounds {
            if let Some(path) = &mut outbound.auth_file
                && path.is_relative()
            {
                *path = parent.join(&*path);
            }
        }
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> anyhow::Result<()> {
        let mut inbounds = HashSet::new();
        let mut outbounds = HashSet::new();
        let mut users = HashSet::new();
        let mut keys = HashSet::new();
        for inbound in &self.inbounds {
            anyhow::ensure!(inbound.kind == "codex", "unsupported inbound type");
            anyhow::ensure!(
                !inbound.id.trim().is_empty() && inbounds.insert(&inbound.id),
                "empty or duplicate inbound id"
            );
            anyhow::ensure!(
                !inbound.user.trim().is_empty() && users.insert(&inbound.user),
                "empty or duplicate user"
            );
            anyhow::ensure!(!inbound.key.trim().is_empty(), "empty inbound key");
            anyhow::ensure!(keys.insert(&inbound.key), "inbound keys must be unique");
            anyhow::ensure!(
                !matches!(inbound.weekly_credits, WeeklyCredits::Limited(value) if value.is_sign_negative()),
                "weekly credits must be nonnegative"
            );
        }
        for outbound in &self.outbounds {
            anyhow::ensure!(outbound.kind == "codex", "unsupported outbound type");
            anyhow::ensure!(
                !outbound.id.trim().is_empty() && outbounds.insert(&outbound.id),
                "empty or duplicate outbound id"
            );
            anyhow::ensure!(
                outbound.auth_file.is_some() ^ outbound.token_env.is_some(),
                "outbound requires exactly one of auth_file or token_env"
            );
        }
        anyhow::ensure!(!self.routing.rules.is_empty(), "no routing rules");
        for rule in &self.routing.rules {
            anyhow::ensure!(
                outbounds.contains(&rule.outbound_id),
                "unknown outbound {}",
                rule.outbound_id
            );
            anyhow::ensure!(!rule.inbound_ids.is_empty(), "empty routing rule");
            for tag in &rule.inbound_ids {
                anyhow::ensure!(inbounds.contains(tag), "unknown inbound {tag}");
            }
        }
        Ok(())
    }

    pub async fn build(self) -> anyhow::Result<App> {
        self.validate()?;
        let mut inbounds = Vec::new();
        let mut outbounds = Vec::new();
        for inbound in &self.inbounds {
            inbounds.push(InboundKind::Codex(CodexInbound::new(
                inbound.id.clone(),
                inbound.user.clone(),
                inbound.key.clone(),
            )?));
        }
        let weekly_credits = self
            .inbounds
            .iter()
            .map(|inbound| (Arc::from(inbound.user.as_str()), inbound.weekly_credits))
            .collect();
        let ledger = Ledger::open(&self.database, weekly_credits).await?;
        let observer = Arc::new(CodexObserver::new(ledger.clone())?);
        let mut managers: HashMap<PathBuf, (String, CredentialManager)> = HashMap::new();
        for outbound in self.outbounds {
            let credentials = if let Some(path) = &outbound.auth_file {
                let path = path.canonicalize().context("resolve upstream auth file")?;
                if let Some((base, manager)) = managers.get(&path) {
                    anyhow::ensure!(
                        base == &outbound.auth_base_url,
                        "one auth file cannot use different refresh authorities"
                    );
                    manager.clone()
                } else {
                    let manager = CredentialManager::from_file(&path, &outbound.auth_base_url)?;
                    managers.insert(path, (outbound.auth_base_url.clone(), manager.clone()));
                    manager
                }
            } else {
                let name = outbound
                    .token_env
                    .as_deref()
                    .context("missing upstream token environment variable")?;
                Credentials::bearer(
                    &std::env::var(name).with_context(|| format!("read {name}"))?,
                    outbound.account_id.as_deref(),
                )?
                .into()
            };
            outbounds.push(OutboundKind::Codex(CodexOutbound::new(
                outbound.id,
                OutboundOptions {
                    base_url: outbound.base_url,
                    chatgpt_base_url: outbound.chatgpt_base_url,
                    api_base_url: outbound.api_base_url,
                    auth_base_url: outbound.auth_base_url,
                    mode: outbound.mode,
                },
                credentials,
                observer.clone(),
            )?));
        }
        Ok(App {
            inbounds,
            outbounds,
            rules: self
                .routing
                .rules
                .into_iter()
                .map(|rule| Rule {
                    inbounds: rule.inbound_ids,
                    outbound: rule.outbound_id,
                })
                .collect(),
            admission: Arc::new(ledger.clone()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxy::Inbound;

    #[tokio::test]
    async fn yaml_keys_authenticate_users_and_invalid_config_fails_early() {
        let mut config: Config =
            serde_yaml::from_str(include_str!("../config.example.yaml")).unwrap();
        config.validate().unwrap();
        assert_eq!(config.inbounds[0].user, "Alice");
        assert_ne!(config.inbounds[0].user, config.inbounds[0].id);
        let bob_key = config.inbounds[1].key.clone();
        for invalid in [String::new(), "   ".into(), config.inbounds[0].key.clone()] {
            config.inbounds[1].key = invalid;
            assert!(config.validate().is_err());
        }
        config.inbounds[1].key = bob_key;
        config.validate().unwrap();
        config.routing.rules[0].outbound_id = "missing".into();
        assert!(config.validate().is_err());
        let original = include_str!("../config.example.yaml").replace("user: Alice", "name: Alice");
        let legacy = original.replace("    mode: broad\n", "");
        let legacy: Config = serde_yaml::from_str(&legacy).unwrap();
        assert_eq!(legacy.outbounds[0].mode, Mode::Broad);
        let strict: Config =
            serde_yaml::from_str(&original.replace("mode: broad", "mode: strict")).unwrap();
        assert_eq!(strict.outbounds[0].mode, Mode::Strict);
        assert!(
            serde_yaml::from_str::<Config>(&original.replace("mode: broad", "mode: typo")).is_err()
        );
        let mut config: Config = serde_yaml::from_str(&original).unwrap();
        config.validate().unwrap();
        config.outbounds[0].token_env = Some("EXTRA".into());
        assert!(config.validate().is_err());
        assert!(
            serde_yaml::from_str::<Config>(&format!("{}\nunknown_setting: true", original))
                .is_err()
        );

        // Exercise YAML-to-service wiring without contacting an upstream account.
        config.outbounds[0].token_env = None;
        let directory = tempfile::tempdir().unwrap();
        config.database = directory.path().join("credits.sqlite3");
        let upstream_auth = directory.path().join("upstream-auth.json");
        std::fs::write(&upstream_auth, br#"{"OPENAI_API_KEY":"upstream-only"}"#).unwrap();
        config.outbounds[0].auth_file = Some(upstream_auth);
        let keys: Vec<_> = config
            .inbounds
            .iter()
            .map(|inbound| (inbound.key.clone(), inbound.user.clone()))
            .collect();
        let app = config.build().await.unwrap();
        for (key, user) in keys {
            for account_id in [None, Some("untrusted-client-account")] {
                let mut request = axum::extract::Request::builder()
                    .uri("/backend-api/wham/accounts/check")
                    .header("authorization", format!("Bearer {key}"));
                if let Some(account_id) = account_id {
                    request = request.header("chatgpt-account-id", account_id);
                }
                use tower::ServiceExt;
                let request = request.body(axum::body::Body::empty()).unwrap();
                let inbound = app
                    .inbounds
                    .iter()
                    .find(|inbound| inbound.accept(&request))
                    .unwrap();
                let exchange = inbound.clone().oneshot(request).await.unwrap();
                assert_eq!(&*exchange.user, user);
            }
        }
    }
}
