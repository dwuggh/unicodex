use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};

use anyhow::{Context, ensure};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use toasty::{
    Db,
    stmt::{Type, Value},
};
use toasty_driver_turso::Turso;

use crate::proxy::{Admission, ProxyError};

const CREDIT_SCALE: i64 = 1_000_000_000;

/// Exact nonnegative credits in billionths. Construction validates precision and range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CreditAmount(i64);

impl CreditAmount {
    pub fn from_decimal(value: Decimal) -> anyhow::Result<Self> {
        ensure!(!value.is_sign_negative(), "negative credit amount");
        let scaled = value
            .checked_mul(Decimal::from(CREDIT_SCALE))
            .context("credit amount overflow")?;
        ensure!(
            scaled.fract().is_zero(),
            "credit amount exceeds storage precision"
        );
        let units = scaled.to_i64().context("credit amount overflow")?;
        ensure!(
            Decimal::from(units) / Decimal::from(CREDIT_SCALE) == value,
            "credit amount loses precision"
        );
        Ok(Self(units))
    }

    pub fn decimal(self) -> Decimal {
        Decimal::new(self.0, 9)
    }
}

#[derive(Debug, toasty::Model)]
#[table = "credit_entries"]
#[index(name = "credit_entries_user_timestamp", user, timestamp)]
pub(crate) struct CreditEntry {
    #[key]
    #[auto]
    pub id: i64,
    pub timestamp: i64,
    pub user: String,
    pub credits: i64,
}

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

#[derive(Clone)]
pub struct Ledger {
    db: Db,
    weekly_credits: Arc<HashMap<Arc<str>, WeeklyCredits>>,
}

impl Ledger {
    pub fn weekly_credits(&self, user: &str) -> anyhow::Result<WeeklyCredits> {
        self.weekly_credits
            .get(user)
            .copied()
            .ok_or_else(|| ledger_error("missing_user_credit_policy"))
    }

    pub async fn open(
        path: &Path,
        weekly_credits: HashMap<Arc<str>, WeeklyCredits>,
    ) -> anyhow::Result<Self> {
        Self::from_driver(Turso::file(path), weekly_credits).await
    }

    async fn from_driver(
        driver: Turso,
        weekly_credits: HashMap<Arc<str>, WeeklyCredits>,
    ) -> anyhow::Result<Self> {
        let mut db = Db::builder()
            .models(toasty::models!(CreditEntry))
            // Serialize short DB operations, never network requests or inference.
            .max_pool_size(1)
            .pool_wait_timeout(Some(Duration::from_secs(5)))
            .log_statement_params(false)
            .build(driver)
            .await?;
        let tables = toasty::sql::query(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'credit_entries'",
        )
        .exec(&mut db)
        .await?;
        if tables.is_empty() {
            db.push_schema().await?;
        }
        // Opening an existing ledger never rewrites its schema or stored charges.
        toasty::sql::query("SELECT id, timestamp, user, credits FROM credit_entries LIMIT 0")
            .exec(&mut db)
            .await?;
        Ok(Self {
            db,
            weekly_credits: Arc::new(weekly_credits),
        })
    }

    /// Sum the user's stored charges in [start, end), excluding future timestamps.
    /// Bounds and now use Unix seconds; stored timestamps retain milliseconds.
    pub(crate) async fn consumed_credits(
        &self,
        user: &str,
        start: i64,
        end: i64,
        now: i64,
    ) -> anyhow::Result<Decimal> {
        let start = start
            .checked_mul(1000)
            .ok_or_else(|| ledger_error("window_start_overflow"))?;
        let end = end
            .checked_mul(1000)
            .ok_or_else(|| ledger_error("window_end_overflow"))?;
        let until = now
            .checked_add(1)
            .and_then(|v| v.checked_mul(1000))
            .ok_or_else(|| ledger_error("window_time_overflow"))?;
        let rows = toasty::sql::query("SELECT COALESCE(SUM(credits), 0) FROM credit_entries WHERE user = ?1 AND timestamp >= ?2 AND timestamp < ?3 AND timestamp < ?4")
            .bind(user).bind(start).bind(end).bind(until)
            .column_types([Type::I64]).exec(&mut self.db.clone()).await
            .inspect_err(|_| tracing::error!(operation = "read_consumption", reason = "query_failed", "database operation failed"))?;
        let Some(Value::Record(fields)) = rows.first() else {
            return Err(ledger_error("invalid_credit_total_row"));
        };
        let Some(Value::I64(units)) = fields.first() else {
            return Err(ledger_error("noninteger_credit_total"));
        };
        if *units < 0 {
            return Err(ledger_error("negative_credit_total"));
        }
        Ok(CreditAmount(*units).decimal())
    }

    pub(crate) async fn record_charge(
        &self,
        user: &str,
        credits: CreditAmount,
    ) -> anyhow::Result<()> {
        self.record_at(user, credits, chrono::Utc::now().timestamp_millis())
            .await
    }

    async fn record_at(
        &self,
        user: &str,
        credits: CreditAmount,
        timestamp: i64,
    ) -> anyhow::Result<()> {
        CreditEntry::create()
            .timestamp(timestamp)
            .user(user)
            .credits(credits.0)
            .exec(&mut self.db.clone())
            .await
            .inspect_err(|_| {
                tracing::error!(operation = "record_charge", "database operation failed")
            })?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn in_memory(
        weekly_credits: HashMap<Arc<str>, WeeklyCredits>,
    ) -> anyhow::Result<Self> {
        Self::from_driver(Turso::in_memory(), weekly_credits).await
    }

    #[cfg(test)]
    pub(crate) async fn entries(&self) -> Vec<CreditEntry> {
        CreditEntry::all().exec(&mut self.db.clone()).await.unwrap()
    }

    #[cfg(test)]
    pub(crate) async fn execute(&self, sql: &str) {
        toasty::sql::statement(sql)
            .exec(&mut self.db.clone())
            .await
            .unwrap();
    }
}

fn ledger_error(reason: &'static str) -> anyhow::Error {
    tracing::error!(operation = "check_credits", reason, "ledger check failed");
    anyhow::anyhow!(reason)
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
            .ok_or_else(|| ledger_error("current_weekly_window_unavailable"))?;
        let used = self
            .consumed_credits(user, window.start, window.end, now)
            .await?;
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

    fn amount(value: &str) -> CreditAmount {
        CreditAmount::from_decimal(Decimal::from_str_exact(value).unwrap()).unwrap()
    }

    #[test]
    fn amounts_preserve_precision_and_reject_overflow() {
        for value in [
            "0",
            "0.000000001",
            "0.00000025",
            "0.1",
            "9223372036.854775807",
        ] {
            assert_eq!(
                amount(value).decimal(),
                Decimal::from_str_exact(value).unwrap()
            );
        }
        for value in ["-1", "0.0000000001", "9223372036.854775808"] {
            assert!(CreditAmount::from_decimal(Decimal::from_str_exact(value).unwrap()).is_err());
        }
    }

    #[tokio::test]
    async fn sums_exact_numeric_charges_with_user_and_window_isolation() {
        let ledger = Ledger::in_memory(Default::default()).await.unwrap();
        let start = 1_800_000_000;
        let end = start + 604800;
        for (timestamp, user, value) in [
            (start - 1, "Alice", "999"),
            (start, "Alice", "0.1"),
            (start + 1, "Alice", "0.2"),
            (start + 1, "Bob", "999"),
            (end, "Alice", "999"),
            (start + 3, "Alice", "7"),
        ] {
            ledger
                .record_at(user, amount(value), timestamp * 1000)
                .await
                .unwrap();
        }
        assert_eq!(
            ledger
                .consumed_credits("Alice", start, end, start + 2)
                .await
                .unwrap(),
            Decimal::new(3, 1)
        );
        assert_eq!(
            ledger
                .consumed_credits("Bob", start, end, start + 2)
                .await
                .unwrap(),
            Decimal::from(999)
        );
        assert_eq!(
            ledger
                .consumed_credits("Nobody", start, end, start + 2)
                .await
                .unwrap(),
            Decimal::ZERO
        );
    }

    #[tokio::test]
    async fn restart_preserves_charges_without_schema_tracking() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("ledger.db");
        let ledger = Ledger::open(&path, Default::default()).await.unwrap();
        let now = chrono::Utc::now().timestamp();
        assert_eq!(
            ledger
                .consumed_credits("Alice", now - 3600, now + 3600, now)
                .await
                .unwrap(),
            Decimal::ZERO
        );
        ledger
            .record_charge("Alice", amount("0.125"))
            .await
            .unwrap();
        drop(ledger);
        let ledger = Ledger::open(&path, Default::default()).await.unwrap();
        assert_eq!(ledger.entries().await[0].credits, 125_000_000);
        assert_eq!(
            ledger
                .consumed_credits("Alice", now - 3600, now + 3600, now)
                .await
                .unwrap(),
            Decimal::new(125, 3)
        );
        ledger
            .record_charge("Alice", amount("0.125"))
            .await
            .unwrap();
        let entries = ledger.entries().await;
        assert_eq!(entries.len(), 2);
        assert_ne!(entries[0].id, entries[1].id);
        // Turso may keep an internal sequence table for the automatic primary key.
        let tables = toasty::sql::query(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT GLOB 'sqlite_*' AND name NOT GLOB '__turso_internal_*'",
        )
        .exec(&mut ledger.db.clone())
        .await
        .unwrap();
        assert_eq!(tables.len(), 1, "{tables:?}");
        assert!(
            matches!(&tables[0], Value::Record(fields) if fields[0] == Value::String("credit_entries".into()))
        );
    }

    #[tokio::test]
    async fn concurrent_charges_do_not_collide_or_lose_updates() {
        let ledger = Ledger::in_memory(Default::default()).await.unwrap();
        let charges = (0..20).map(|_| ledger.record_at("Alice", amount("0.1"), 1_800_000_000_000));
        for result in futures_util::future::join_all(charges).await {
            result.unwrap();
        }
        let entries = ledger.entries().await;
        assert_eq!(entries.len(), 20);
        let ids: std::collections::HashSet<_> = entries.iter().map(|v| v.id).collect();
        assert_eq!(ids.len(), 20);
        assert_eq!(
            ledger
                .consumed_credits("Alice", 1_800_000_000, 1_800_000_001, 1_800_000_000)
                .await
                .unwrap(),
            Decimal::from(2)
        );
    }

    #[tokio::test]
    async fn summed_credits_reject_integer_overflow() {
        let ledger = Ledger::in_memory(Default::default()).await.unwrap();
        ledger
            .record_at("Alice", CreditAmount(i64::MAX), 1000)
            .await
            .unwrap();
        ledger
            .record_at("Alice", CreditAmount(1), 1000)
            .await
            .unwrap();
        assert!(ledger.consumed_credits("Alice", 0, 2, 1).await.is_err());
    }

    #[tokio::test]
    async fn quota_exhaustion_and_database_errors_remain_distinct() {
        let ledger = Ledger::in_memory(
            [
                (Arc::from("Alice"), WeeklyCredits::Limited(Decimal::ONE)),
                (Arc::from("Bob"), WeeklyCredits::Unlimited),
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
        ledger.record_charge("Alice", amount("1")).await.unwrap();
        assert!(matches!(
            ledger.check("Alice", window).await,
            Err(ProxyError::NoCredits)
        ));
        ledger.execute("DROP TABLE credit_entries").await;
        assert!(matches!(
            ledger.check("Alice", window).await,
            Err(ProxyError::Internal(_))
        ));
        assert!(ledger.check("Bob", None).await.is_ok());
    }
}
