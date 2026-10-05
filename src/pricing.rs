use std::collections::HashMap;

use anyhow::{Context, ensure};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;

use crate::ledger::CreditAmount;

#[derive(Clone, Debug, Default)]
enum BillingField {
    #[default]
    Missing,
    Text(String),
    Invalid,
}

impl BillingField {
    fn parse(value: Option<&Value>) -> Self {
        match value {
            None | Some(Value::Null) => Self::Missing,
            Some(Value::String(value)) if !value.is_empty() && value.len() <= 256 => {
                Self::Text(value.clone())
            }
            _ => Self::Invalid,
        }
    }

    fn resolved<'a>(&'a self, fallback: &'a Self) -> Result<Option<&'a str>, PricingError> {
        match self {
            Self::Text(value) => Ok(Some(value)),
            Self::Invalid => Err(PricingError::InvalidMetadata),
            Self::Missing => match fallback {
                Self::Text(value) => Ok(Some(value)),
                Self::Missing => Ok(None),
                Self::Invalid => Err(PricingError::InvalidMetadata),
            },
        }
    }
}

/// Only the small routing/billing fields survive request inspection.
#[derive(Clone, Debug, Default)]
pub struct BillingContext {
    model: BillingField,
    tier: BillingField,
    program: BillingField,
    pub(crate) warmup: bool,
}

impl BillingContext {
    pub(crate) fn from_value(value: &Value) -> Self {
        let program = match value.get("access_programs") {
            None | Some(Value::Null) => BillingField::Missing,
            Some(Value::Object(programs)) => BillingField::parse(programs.get("cyber")),
            _ => BillingField::Invalid,
        };
        Self {
            model: BillingField::parse(value.get("model")),
            tier: BillingField::parse(value.get("service_tier")),
            program,
            warmup: value.get("generate") == Some(&Value::Bool(false)),
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum PricingError {
    #[error("invalid_billing_metadata")]
    InvalidMetadata,
    #[error("unsupported_model")]
    UnsupportedModel,
    #[error("unsupported_access_program")]
    UnsupportedProgram,
    #[error("unsupported_service_tier")]
    UnsupportedTier,
    #[error("missing_or_invalid_token_counts")]
    InvalidUsage,
    #[error("unsupported_token_modality")]
    UnsupportedModality,
    #[error("credit_arithmetic_overflow_or_precision")]
    Arithmetic,
}

#[derive(Debug, PartialEq, Eq)]
struct TokenUsage {
    input: u64,
    cached: u64,
    output: u64,
}

impl TokenUsage {
    fn parse(response: &Value) -> Result<Self, PricingError> {
        let usage = response
            .get("usage")
            .and_then(Value::as_object)
            .ok_or(PricingError::InvalidUsage)?;
        let input = usage
            .get("input_tokens")
            .and_then(Value::as_u64)
            .ok_or(PricingError::InvalidUsage)?;
        let output = usage
            .get("output_tokens")
            .and_then(Value::as_u64)
            .ok_or(PricingError::InvalidUsage)?;
        let cached = match usage.get("input_tokens_details") {
            None | Some(Value::Null) => 0,
            Some(Value::Object(details)) => match details.get("cached_tokens") {
                None | Some(Value::Null) => 0,
                Some(value) => value.as_u64().ok_or(PricingError::InvalidUsage)?,
            },
            _ => return Err(PricingError::InvalidUsage),
        };
        if cached > input {
            return Err(PricingError::InvalidUsage);
        }
        for field in ["input_tokens_details", "output_tokens_details"] {
            if let Some(details) = usage.get(field) {
                for modality in ["image_tokens", "audio_tokens"] {
                    if let Some(count) = details.get(modality)
                        && count.as_u64() != Some(0)
                    {
                        return Err(PricingError::UnsupportedModality);
                    }
                }
            }
        }
        Ok(Self {
            input,
            cached,
            output,
        })
    }
}

#[derive(Deserialize)]
pub(crate) struct CreditPrices {
    version: u32,
    unit: String,
    tokens_per_unit: u32,
    base_speed: String,
    speed_multipliers: SpeedMultipliers,
    models: HashMap<String, TokenPrices>,
    access_programs: HashMap<String, TokenPrices>,
}

#[derive(Deserialize)]
struct SpeedMultipliers {
    #[serde(deserialize_with = "price")]
    standard: Decimal,
    #[serde(deserialize_with = "price")]
    fast: Decimal,
    ultrafast: HashMap<String, String>,
}

#[derive(Deserialize)]
struct TokenPrices {
    #[serde(deserialize_with = "price")]
    input: Decimal,
    #[serde(deserialize_with = "price")]
    cached_input: Decimal,
    #[serde(deserialize_with = "price")]
    output: Decimal,
}

fn price<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Decimal, D::Error> {
    let value = String::deserialize(deserializer)?;
    Decimal::from_str_exact(&value)
        .ok()
        .filter(|v| !v.is_sign_negative())
        .ok_or_else(|| serde::de::Error::custom("price must be a nonnegative decimal string"))
}

impl CreditPrices {
    pub(crate) fn bundled() -> anyhow::Result<Self> {
        let prices: Self = serde_yaml::from_str(include_str!("../codex_credit_prices.yaml"))
            .context("parse Codex credit prices")?;
        ensure!(
            prices.version == 1
                && prices.unit == "credits"
                && prices.tokens_per_unit == 1_000_000
                && prices.base_speed == "standard",
            "unsupported credit price table"
        );
        ensure!(!prices.models.is_empty(), "empty credit price table");
        for (model, multiplier) in &prices.speed_multipliers.ultrafast {
            ensure!(
                prices.models.contains_key(model)
                    && Decimal::from_str_exact(multiplier).is_ok_and(|v| !v.is_sign_negative()),
                "invalid speed multiplier"
            );
        }
        // Every published per-token rate must fit the storage unit exactly.
        for rates in prices
            .models
            .values()
            .chain(prices.access_programs.values())
        {
            for rate in [rates.input, rates.cached_input, rates.output] {
                for multiplier in [
                    prices.speed_multipliers.standard,
                    prices.speed_multipliers.fast,
                ] {
                    let per_token = rate
                        .checked_mul(multiplier)
                        .and_then(|v| v.checked_div(Decimal::from(prices.tokens_per_unit)))
                        .context("invalid per-token price")?;
                    CreditAmount::from_decimal(per_token)?;
                }
            }
        }
        for (model, multiplier) in &prices.speed_multipliers.ultrafast {
            let multiplier = Decimal::from_str_exact(multiplier)?;
            let rates = &prices.models[model];
            for rate in [rates.input, rates.cached_input, rates.output] {
                CreditAmount::from_decimal(
                    rate.checked_mul(multiplier)
                        .and_then(|v| v.checked_div(Decimal::from(prices.tokens_per_unit)))
                        .context("invalid ultrafast per-token price")?,
                )?;
            }
        }
        Ok(prices)
    }

    pub(crate) fn credits(
        &self,
        response: &Value,
        request: &BillingContext,
    ) -> Result<CreditAmount, PricingError> {
        let reported = BillingContext::from_value(response);
        let model = reported
            .model
            .resolved(&request.model)?
            .ok_or(PricingError::UnsupportedModel)?;
        let program = reported
            .program
            .resolved(&request.program)?
            .unwrap_or("standard");
        let rates = if program == "standard" {
            self.models
                .get(model)
                .ok_or(PricingError::UnsupportedModel)?
        } else {
            self.access_programs
                .get(program)
                .ok_or(PricingError::UnsupportedProgram)?
        };
        let multiplier = match reported.tier.resolved(&request.tier)?.unwrap_or("standard") {
            "default" | "standard" => self.speed_multipliers.standard,
            "priority" | "fast" => self.speed_multipliers.fast,
            "ultrafast" if program == "standard" => self
                .speed_multipliers
                .ultrafast
                .get(model)
                .and_then(|v| Decimal::from_str_exact(v).ok())
                .ok_or(PricingError::UnsupportedTier)?,
            _ => return Err(PricingError::UnsupportedTier),
        };
        let usage = TokenUsage::parse(response)?;
        let amount = Decimal::from(usage.input - usage.cached)
            .checked_mul(rates.input)
            .and_then(|v| {
                Decimal::from(usage.cached)
                    .checked_mul(rates.cached_input)
                    .and_then(|cached| v.checked_add(cached))
            })
            .and_then(|v| {
                Decimal::from(usage.output)
                    .checked_mul(rates.output)
                    .and_then(|output| v.checked_add(output))
            })
            .and_then(|v| v.checked_mul(multiplier))
            .and_then(|v| v.checked_div(Decimal::from(self.tokens_per_unit)))
            .ok_or(PricingError::Arithmetic)?;
        CreditAmount::from_decimal(amount).map_err(|_| PricingError::Arithmetic)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn calculates_exact_tokens_cache_and_speed_without_double_charging() {
        let prices = CreditPrices::bundled().unwrap();
        let response = json!({"model":"gpt-6.1-sol", "usage":{"input_tokens":1000,"output_tokens":2000,"input_tokens_details":{"cached_tokens":100,"cache_write_tokens":900},"output_tokens_details":{"reasoning_tokens":1000}}});
        for (tier, expected) in [
            ("standard", "0.54525"),
            ("fast", "1.0905"),
            ("priority", "1.0905"),
        ] {
            let request = BillingContext::from_value(&json!({"service_tier":tier}));
            assert_eq!(
                prices.credits(&response, &request).unwrap().decimal(),
                Decimal::from_str_exact(expected).unwrap()
            );
        }
        let tiny = json!({"model":"gpt-6-luna","usage":{"input_tokens":1,"output_tokens":0,"input_tokens_details":{"cached_tokens":1}}});
        assert_eq!(
            prices
                .credits(&tiny, &BillingContext::default())
                .unwrap()
                .decimal(),
            Decimal::new(25, 8)
        );
    }

    #[test]
    fn request_fallback_and_response_overrides_follow_protocol_defaults() {
        let prices = CreditPrices::bundled().unwrap();
        let request =
            BillingContext::from_value(&json!({"model":"gpt-6-astra","service_tier":"fast"}));
        let mut response =
            json!({"usage":{"input_tokens":0,"output_tokens":1,"input_tokens_details":null}});
        assert_eq!(
            prices.credits(&response, &request).unwrap().decimal(),
            Decimal::new(25, 4)
        );
        response["model"] = json!("gpt-6.1-sol");
        response["service_tier"] = json!("default");
        assert_eq!(
            prices.credits(&response, &request).unwrap().decimal(),
            Decimal::new(25, 5)
        );
        response["service_tier"] = Value::Null;
        assert_eq!(
            prices.credits(&response, &request).unwrap().decimal(),
            Decimal::new(5, 4)
        );
        response["model"] = json!("gpt-6-astra");
        response["service_tier"] = json!("ultrafast");
        assert_eq!(
            prices.credits(&response, &request).unwrap().decimal(),
            Decimal::new(75, 4)
        );
        let standard = BillingContext::from_value(&json!({"model":"gpt-6.1-sol"}));
        response = json!({"usage":{"input_tokens":1,"output_tokens":0}});
        assert_eq!(
            prices.credits(&response, &standard).unwrap().decimal(),
            Decimal::new(5, 5)
        );
    }

    #[test]
    fn invalid_or_unsupported_data_is_never_given_a_fallback_price() {
        let prices = CreditPrices::bundled().unwrap();
        let request = BillingContext::from_value(&json!({"model":"gpt-6.1-sol"}));
        let base = json!({"usage":{"input_tokens":1,"output_tokens":1}});
        for patch in [
            json!({"model":"unknown"}),
            json!({"model":123}),
            json!({"service_tier":"auto"}),
            json!({"service_tier":false}),
            json!({"access_programs":{"cyber":"unknown"}}),
            json!({"usage":{"input_tokens":-1,"output_tokens":1}}),
            json!({"usage":{"input_tokens":1}}),
            json!({"usage":{"input_tokens":1,"output_tokens":1,"input_tokens_details":{"cached_tokens":2}}}),
            json!({"usage":{"input_tokens":1,"output_tokens":1,"input_tokens_details":false}}),
            json!({"usage":{"input_tokens":1,"output_tokens":1,"input_tokens_details":{"audio_tokens":1}}}),
        ] {
            let mut response = base.clone();
            for (k, v) in patch.as_object().unwrap() {
                response[k] = v.clone();
            }
            assert!(prices.credits(&response, &request).is_err(), "{response}");
        }
        let overflow = json!({"usage":{"input_tokens":u64::MAX,"output_tokens":u64::MAX}});
        assert_eq!(
            prices.credits(&overflow, &request),
            Err(PricingError::Arithmetic)
        );
    }

    #[test]
    fn daybreak_program_rates_are_separate_from_model_rates() {
        let prices = CreditPrices::bundled().unwrap();
        let request = BillingContext::from_value(
            &json!({"model":"gpt-6-astra","access_programs":{"cyber":"daybreak_red"}}),
        );
        let response = json!({"usage":{"input_tokens":1000000,"output_tokens":0}});
        assert_eq!(
            prices.credits(&response, &request).unwrap().decimal(),
            Decimal::new(3125, 1)
        );
    }
}
