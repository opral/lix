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
