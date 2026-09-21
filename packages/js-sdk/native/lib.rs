#![recursion_limit = "256"]

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(target_family = "wasm")]
mod browser_storage;
#[cfg(target_family = "wasm")]
mod js_storage;
#[cfg(not(target_family = "wasm"))]
mod napi;
mod session;
mod telemetry;
#[cfg(target_family = "wasm")]
mod wasm;

mod component_runtime;

#[cfg(not(target_family = "wasm"))]
pub(crate) mod component_runtime_napi;
#[cfg(target_family = "wasm")]
pub(crate) mod component_runtime_wasm;

fn parse_durability(value: Option<&str>) -> Result<lix::Durability, lix::LixError> {
    match value {
        None | Some("durable") => Ok(lix::Durability::Durable),
        Some("buffered") => Ok(lix::Durability::Buffered),
        Some(_) => Err(lix::LixError::new(
            lix::LixError::CODE_INVALID_PARAM,
            "durability must be durable or buffered",
        )),
    }
}

fn parse_timestamptz(value: &str) -> Result<i64, String> {
    if !is_canonical_timestamptz(value) {
        return Err("timestamp must use canonical RFC 3339 syntax".to_owned());
    }
    value
        .parse::<chrono::DateTime<chrono::FixedOffset>>()
        .map(|value| value.timestamp_micros())
        .map_err(|error| error.to_string())
}

fn is_canonical_timestamptz(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut index = 0;
    let signed_year = matches!(bytes.first(), Some(b'+' | b'-'));
    if signed_year {
        index += 1;
    }

    let year_start = index;
    while matches!(bytes.get(index), Some(byte) if byte.is_ascii_digit()) {
        index += 1;
    }
    let year_len = index - year_start;
    if (signed_year && !(4..=6).contains(&year_len)) || (!signed_year && year_len != 4) {
        return false;
    }
    if !matches!(bytes.get(index), Some(b'-')) {
        return false;
    }
    index += 1;

    let Some(month) = parse_two_digits(bytes, index) else {
        return false;
    };
    index += 2;
    if !matches!(bytes.get(index), Some(b'-')) {
        return false;
    }
    index += 1;

    let Some(day) = parse_two_digits(bytes, index) else {
        return false;
    };
    index += 2;
    if !matches!(bytes.get(index), Some(b'T')) {
        return false;
    }
    index += 1;

    let Some(hour) = parse_two_digits(bytes, index) else {
        return false;
    };
    index += 2;
    if !matches!(bytes.get(index), Some(b':')) {
        return false;
    }
    index += 1;

    let Some(minute) = parse_two_digits(bytes, index) else {
        return false;
    };
    index += 2;
    if !matches!(bytes.get(index), Some(b':')) {
        return false;
    }
    index += 1;

    let Some(second) = parse_two_digits(bytes, index) else {
        return false;
    };
    index += 2;
    if second > 59 {
        return false;
    }

    if matches!(bytes.get(index), Some(b'.')) {
        index += 1;
        let fraction_start = index;
        while matches!(bytes.get(index), Some(byte) if byte.is_ascii_digit()) {
            index += 1;
        }
        if !(1..=6).contains(&(index - fraction_start)) {
            return false;
        }
    }

    if matches!(bytes.get(index), Some(b'Z')) {
        return index + 1 == bytes.len();
    }
    if !matches!(bytes.get(index), Some(b'+' | b'-')) {
        return false;
    }
    index += 1;
    let Some(offset_hour) = parse_two_digits(bytes, index) else {
        return false;
    };
    index += 2;
    if !matches!(bytes.get(index), Some(b':')) {
        return false;
    }
    index += 1;
    let Some(offset_minute) = parse_two_digits(bytes, index) else {
        return false;
    };

    month > 0
        && month <= 12
        && day > 0
        && day <= 31
        && hour <= 23
        && minute <= 59
        && offset_hour <= 23
        && offset_minute <= 59
        && index + 2 == bytes.len()
}

fn parse_two_digits(bytes: &[u8], index: usize) -> Option<u8> {
    let first = *bytes.get(index)?;
    let second = *bytes.get(index + 1)?;
    if !first.is_ascii_digit() || !second.is_ascii_digit() {
        return None;
    }
    Some((first - b'0') * 10 + second - b'0')
}

#[cfg(test)]
mod timestamptz_tests {
    use super::parse_timestamptz;

    #[test]
    fn accepts_canonical_and_extended_chrono_timestamps() {
        for value in [
            "2015-02-18T23:59:59.234567+05:00",
            "+010000-01-01T01:02:03.000000Z",
            "-262143-01-01T01:02:03.000000Z",
            "-262143-01-01T23:59:00.000000+23:59",
            "+262142-12-31T00:00:59.000000-23:59",
        ] {
            assert!(parse_timestamptz(value).is_ok(), "{value}");
        }
    }

    #[test]
    fn rejects_invalid_or_incomplete_timestamps() {
        for value in [
            "2026-02-29T01:21:47Z",
            "2026-09-21",
            "2026-09-21T01:21:47+2400",
            "2015-02-18T23:59:60.234567+05:00",
            "2015-02-18T23:59:59.1234567+05:00",
            "2015-02-18T23:16:09 UTC",
            "-262143-01-01T01:02:03.000000+23:59",
            "+262142-12-31T23:00:00.000000-23:59",
        ] {
            assert!(parse_timestamptz(value).is_err(), "{value}");
        }
    }
}

const JS_MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;
const JS_MAX_SAFE_INTEGER_I64: i64 = 9_007_199_254_740_991;

/// Decode the number carried by an explicitly tagged `integer` value.
///
/// The native N-API serde bridge represents JavaScript numbers outside the
/// signed/unsigned 32-bit ranges as floating-point JSON numbers. The tag
/// carries the caller's numeric intent, so the JSON number's internal variant
/// cannot be used as the integer check. Validate the JavaScript safe-integer
/// range and integrality before converting the floating representation.
#[expect(
    clippy::cast_possible_truncation,
    reason = "the value is bounded by the JavaScript safe-integer range"
)]
fn parse_json_integer(value: Option<serde_json::Value>) -> Result<i64, lix::LixError> {
    let invalid = || {
        lix::LixError::new(
            lix::LixError::CODE_INVALID_PARAM,
            "integer value must be an integer",
        )
    };

    let Some(serde_json::Value::Number(number)) = value else {
        return Err(invalid());
    };

    if let Some(value) = number.as_i64() {
        return (-JS_MAX_SAFE_INTEGER_I64..=JS_MAX_SAFE_INTEGER_I64)
            .contains(&value)
            .then_some(value)
            .ok_or_else(invalid);
    }

    if let Some(value) = number.as_u64() {
        return i64::try_from(value)
            .ok()
            .filter(|value| (-JS_MAX_SAFE_INTEGER_I64..=JS_MAX_SAFE_INTEGER_I64).contains(value))
            .ok_or_else(invalid);
    }

    let Some(value) = number.as_f64() else {
        return Err(invalid());
    };
    if !value.is_finite()
        || value.fract() != 0.0
        || !(-JS_MAX_SAFE_INTEGER..=JS_MAX_SAFE_INTEGER).contains(&value)
    {
        return Err(invalid());
    }

    Ok(value as i64)
}

#[cfg(test)]
mod integer_tests {
    use super::*;

    fn json_float(value: f64) -> serde_json::Value {
        serde_json::Value::Number(
            serde_json::Number::from_f64(value).expect("test values must be finite"),
        )
    }

    #[test]
    fn accepts_exact_safe_integer_boundaries_from_json_floats() {
        for (input, expected) in [
            (-2_147_483_648.0, -2_147_483_648),
            (-2_147_483_649.0, -2_147_483_649),
            (4_294_967_295.0, 4_294_967_295),
            (4_294_967_296.0, 4_294_967_296),
            (-9_007_199_254_740_991.0, -9_007_199_254_740_991),
            (9_007_199_254_740_991.0, 9_007_199_254_740_991),
        ] {
            assert_eq!(parse_json_integer(Some(json_float(input))), Ok(expected));
        }
    }

    #[test]
    fn rejects_non_integral_non_safe_and_non_numeric_values() {
        for input in [
            json_float(1.5),
            json_float(-9_007_199_254_740_992.0),
            json_float(9_007_199_254_740_992.0),
            serde_json::Value::Null,
            serde_json::Value::String("1".to_owned()),
        ] {
            assert_eq!(
                parse_json_integer(Some(input)).map_err(|error| error.message),
                Err("integer value must be an integer".to_owned())
            );
        }
        assert!(parse_json_integer(None).is_err());
    }

    #[test]
    fn accepts_integer_json_numbers_without_widening_them() {
        for (input, expected) in [(serde_json::json!(-41), -41), (serde_json::json!(41), 41)] {
            assert_eq!(parse_json_integer(Some(input)), Ok(expected));
        }
    }
}
