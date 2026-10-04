use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};

use anyhow::Context;
use rust_decimal::Decimal;
use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};

use crate::proxy::{Admission, ProxyError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WeeklyCredits {
    Limited(Decimal),
    Unlimited,
}

#[derive(Clone, Copy, Debug)]
pub struct WeeklyWindow {
    pub start: i64,
    pub end: i64,
}

impl WeeklyWindow {
    pub fn contains(self, now: i64) -> bool {
        self.start <= now && now < self.end
    }
}

/// Shared database pool; observations contain only identity, time and reported accounting fields.
#[derive(Clone)]
pub struct Ledger {
    pub(crate) pool: SqlitePool,
    weekly_credits: Arc<HashMap<Arc<str>, WeeklyCredits>>,
}

impl Ledger {
    pub fn weekly_credits(&self, user: &str) -> anyhow::Result<WeeklyCredits> {
        self.weekly_credits
            .get(user)
            .copied()
            .context("missing user credit policy")
    }

    /// Sum the user's reported consumption in [start, end), excluding future reports.
    pub(crate) async fn consumed_credits(
        &self,
        user: &str,
        start: i64,
        end: i64,
        now: i64,
    ) -> anyhow::Result<Option<rust_decimal::Decimal>> {
        let reports: Vec<String> = sqlx::query_scalar(
            "SELECT usage FROM observations WHERE user = ? AND usage IS NOT NULL
             AND unixepoch(timestamp) >= ? AND unixepoch(timestamp) < ?
             AND unixepoch(timestamp) <= ?",
        )
        .bind(user)
        .bind(start)
        .bind(end)
        .bind(now)
        .fetch_all(&self.pool)
        .await
        .inspect_err(|_| {
            tracing::error!(operation = "read_consumption", "database operation failed")
        })?;
        let mut total = rust_decimal::Decimal::ZERO;
        for report in reports {
            let report: serde_json::Value = serde_json::from_str(&report)?;
            let Some(amount) = report.pointer("/usage_metadata/amount").and_then(decimal) else {
                return Ok(None);
            };
            let Some(next) = total.checked_add(amount) else {
                return Ok(None);
            };
            if next.checked_sub(total) != Some(amount) || next.checked_sub(amount) != Some(total) {
                return Ok(None);
            }
            total = next;
        }
        Ok(Some(total))
    }

    pub async fn open(
        path: &Path,
        weekly_credits: HashMap<Arc<str>, WeeklyCredits>,
    ) -> anyhow::Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await
            .inspect_err(|_| {
                tracing::error!(operation = "open_database", "database operation failed")
            })?;
        Self::from_pool(pool, weekly_credits).await
    }

    pub async fn from_pool(
        pool: SqlitePool,
        weekly_credits: HashMap<Arc<str>, WeeklyCredits>,
    ) -> anyhow::Result<Self> {
        sqlx::raw_sql(
            "CREATE TABLE IF NOT EXISTS observations (
                timestamp TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                user TEXT NOT NULL,
                usage TEXT,
                credits TEXT,
                CHECK (usage IS NOT NULL OR credits IS NOT NULL)
            );
            CREATE INDEX IF NOT EXISTS observations_user_time ON observations(user, timestamp);",
        )
        .execute(&pool)
        .await
        .inspect_err(|_| {
            tracing::error!(operation = "initialize_schema", "database operation failed")
        })?;
        Ok(Self {
            pool,
            weekly_credits: Arc::new(weekly_credits),
        })
    }

    pub(crate) async fn record(
        &self,
        user: &str,
        usage: Option<serde_json::Value>,
        credits: Option<serde_json::Value>,
    ) -> anyhow::Result<()> {
        if usage.is_none() && credits.is_none() {
            return Ok(());
        }
        sqlx::query("INSERT INTO observations(user, usage, credits) VALUES (?, ?, ?)")
            .bind(user)
            .bind(usage.map(|value| value.to_string()))
            .bind(credits.map(|value| value.to_string()))
            .execute(&self.pool)
            .await
            .inspect_err(|_| {
                tracing::error!(
                    operation = "record_observation",
                    "database operation failed"
                )
            })?;
        Ok(())
    }
}

/// Decimal strings stay decimal; malformed, negative, or unrepresentable amounts are unknown.
pub(crate) fn decimal(value: &serde_json::Value) -> Option<rust_decimal::Decimal> {
    let text = match value {
        serde_json::Value::String(value) => value.clone(),
        serde_json::Value::Number(value) => value.to_string(),
        _ => return None,
    };
    rust_decimal::Decimal::from_str_exact(&text)
        .or_else(|error| {
            let Some((mantissa, _)) = text.split_once(['e', 'E']) else {
                return Err(error);
            };
            rust_decimal::Decimal::from_str_exact(mantissa)?;
            rust_decimal::Decimal::from_scientific(&text)
        })
        .ok()
        .filter(|value| !value.is_sign_negative())
}

impl Admission for Ledger {
    async fn check(&self, user: &str, window: Option<WeeklyWindow>) -> Result<(), ProxyError> {
        let WeeklyCredits::Limited(allowance) = self.weekly_credits(user)? else {
            return Ok(());
        };
        if allowance <= Decimal::ZERO {
            return Err(ProxyError::NoCredits);
        }
        let now = chrono::Utc::now().timestamp();
        let window = window
            .filter(|window| window.contains(now))
            .context("current weekly window unavailable")?;
        let used = self
            .consumed_credits(user, window.start, window.end, now)
            .await?
            .context("weekly consumption unknown")?;
        if used < allowance {
            Ok(())
        } else {
            Err(ProxyError::NoCredits)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn consumption_sums_exact_reports_for_the_user_in_the_current_period() {
        use rust_decimal::Decimal;
        use serde_json::json;
        let ledger = Ledger::from_pool(
            SqlitePoolOptions::new()
                .max_connections(1)
                .connect("sqlite::memory:")
                .await
                .unwrap(),
            Default::default(),
        )
        .await
        .unwrap();
        let start = 1_800_000_000i64;
        let end = start + 7 * 86400;
        for (time, user, amount) in [
            (start - 1, "Alice", "999"),
            (start, "Alice", "0.1"),
            (start + 1, "Alice", "0.2"),
            (start + 1, "Bob", "999"),
            (end, "Alice", "999"),
        ] {
            sqlx::query("INSERT INTO observations(timestamp,user,usage) VALUES (strftime('%Y-%m-%dT%H:%M:%SZ', ?, 'unixepoch'),?,?)")
                .bind(time).bind(user).bind(json!({"usage_metadata":{"amount":amount}}).to_string())
                .execute(&ledger.pool).await.unwrap();
        }
        assert_eq!(
            ledger
                .consumed_credits("Alice", start, end, start + 2)
                .await
                .unwrap(),
            Some(Decimal::new(3, 1))
        );
        assert_eq!(
            ledger
                .consumed_credits("Bob", start, end, start + 2)
                .await
                .unwrap(),
            Some(Decimal::from(999))
        );
        assert_eq!(
            ledger
                .consumed_credits("Alice", start, end, end)
                .await
                .unwrap(),
            Some(Decimal::new(3, 1))
        );
        for invalid in [
            json!("bad"),
            json!("-1"),
            json!("0.00000000000000000000000000001"),
            json!("1.00000000000000000000000000001e0"),
            serde_json::Value::Null,
        ] {
            assert!(decimal(&invalid).is_none(), "{invalid}");
        }
        assert_eq!(decimal(&json!("1.25e-2")), Some(Decimal::new(125, 4)));
    }

    #[tokio::test]
    async fn weekly_credits_are_local_and_fail_closed() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let ledger = Ledger::from_pool(
            pool,
            [
                (Arc::from("Alice"), WeeklyCredits::Limited(Decimal::ONE)),
                (Arc::from("Bob"), WeeklyCredits::Limited(Decimal::ZERO)),
            ]
            .into_iter()
            .collect(),
        )
        .await
        .unwrap();
        let now = chrono::Utc::now().timestamp();
        let window = Some(WeeklyWindow {
            start: now - 3600,
            end: now + 601200,
        });
        assert!(matches!(
            ledger.check("Alice", None).await,
            Err(ProxyError::Internal(_))
        ));
        assert!(ledger.check("Alice", window).await.is_ok());
        assert!(matches!(
            ledger.check("Bob", window).await,
            Err(ProxyError::NoCredits)
        ));
        // Upstream account snapshots are evidence, not local user allowances.
        ledger
            .record(
                "Bob",
                None,
                Some(serde_json::json!({"balance":"999","unlimited":true})),
            )
            .await
            .unwrap();
        assert!(matches!(
            ledger.check("Bob", window).await,
            Err(ProxyError::NoCredits)
        ));
        ledger
            .record(
                "Alice",
                Some(serde_json::json!({"usage_metadata":{"amount":"1"}})),
                None,
            )
            .await
            .unwrap();
        assert!(matches!(
            ledger.check("Alice", window).await,
            Err(ProxyError::NoCredits)
        ));
        sqlx::query("DROP TABLE observations")
            .execute(&ledger.pool)
            .await
            .unwrap();
        assert!(matches!(
            ledger.check("Alice", window).await,
            Err(ProxyError::Internal(_))
        ));
    }
}
