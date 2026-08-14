//! One canonical semantic authority for PostgreSQL-compatible JSONB values.
//!
//! SQL admission and the durable JSONB cell codec both call this module. The
//! codec defines physical bytes only; it does not carry a parallel notion of
//! numeric, object-key, or NUL equivalence.

use serde_json::{Number, Value};

pub(crate) fn parse_jsonb(raw: &str) -> Result<Value, String> {
    let mut value = serde_json::from_str::<Value>(raw).map_err(|error| error.to_string())?;
    normalize_jsonb(&mut value)?;
    Ok(value)
}

pub(crate) fn canonical_jsonb_text(raw: &str) -> Result<String, String> {
    serde_json::to_string(&parse_jsonb(raw)?).map_err(|error| error.to_string())
}

pub(crate) fn normalize_jsonb(value: &mut Value) -> Result<(), String> {
    match value {
        Value::String(value) => reject_jsonb_nul(value)?,
        Value::Array(values) => {
            for value in values {
                normalize_jsonb(value)?;
            }
        }
        Value::Object(values) => {
            let old = std::mem::take(values);
            let mut entries = old.into_iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.as_bytes().cmp(right.as_bytes()));
            for (key, mut value) in entries {
                reject_jsonb_nul(&key)?;
                normalize_jsonb(&mut value)?;
                values.insert(key, value);
            }
        }
        Value::Number(number) => {
            let canonical = canonical_decimal(&number.to_string())?;
            *number = serde_json::from_str::<Number>(&canonical)
                .map_err(|error| format!("canonical JSONB number is invalid: {error}"))?;
        }
        Value::Null | Value::Bool(_) => {}
    }
    Ok(())
}

/// Canonical signed decimal coefficient plus base-10 exponent.
pub(crate) fn canonical_decimal(raw: &str) -> Result<String, String> {
    let (negative, raw) = raw
        .strip_prefix('-')
        .map_or((false, raw), |raw| (true, raw));
    let exponent_at = raw.find(['e', 'E']);
    let (mantissa, exponent) = exponent_at.map_or((raw, "0"), |at| (&raw[..at], &raw[at + 1..]));
    let exponent = exponent
        .parse::<i64>()
        .map_err(|_| "JSONB exponent exceeds i64")?;
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if integer.is_empty()
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err("JSONB number has invalid decimal digits".into());
    }
    let mut digits = format!("{integer}{fraction}");
    let Some(first_nonzero) = digits.find(|byte: char| byte != '0') else {
        return Ok("0".into());
    };
    digits.drain(..first_nonzero);
    let mut scale = i64::try_from(fraction.len())
        .map_err(|_| "JSONB fractional scale exceeds i64")?
        .checked_sub(exponent)
        .ok_or("JSONB decimal scale overflow")?;
    while digits.len() > 1 && digits.ends_with('0') {
        digits.pop();
        scale = scale.checked_sub(1).ok_or("JSONB decimal scale overflow")?;
    }
    let sign = if negative { "-" } else { "" };
    if scale == 0 {
        Ok(format!("{sign}{digits}"))
    } else {
        let exponent = scale.checked_neg().ok_or("JSONB exponent overflow")?;
        Ok(format!("{sign}{digits}e{exponent}"))
    }
}

pub(crate) fn reject_jsonb_nul(value: &str) -> Result<(), String> {
    if value.contains('\0') {
        Err("PostgreSQL JSONB does not support the Unicode NUL escape (\\u0000)".to_owned())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_semantics_are_lossless_and_shared() {
        for equivalent in ["42", "42.0", "4.2e1"] {
            assert_eq!(canonical_jsonb_text(equivalent).unwrap(), "42");
        }
        assert_eq!(canonical_jsonb_text("-0.000e999").unwrap(), "0");
        assert_eq!(
            canonical_jsonb_text("1.2345678901234567890").unwrap(),
            canonical_jsonb_text("1234567890123456789e-18").unwrap()
        );
        assert_eq!(
            canonical_jsonb_text("9007199254740993.0").unwrap(),
            "9007199254740993"
        );
        assert!(parse_jsonb(r#"{"a":"\u0000"}"#).is_err());
        assert!(parse_jsonb(r#"{"a\u0000":1}"#).is_err());
    }
}
