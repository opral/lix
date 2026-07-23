use serde_json::Value as JsonValue;

use crate::LixError;

const I64_LOWER_INCLUSIVE_AS_F64: f64 = -9_223_372_036_854_775_808.0;
const I64_UPPER_EXCLUSIVE_AS_F64: f64 = 9_223_372_036_854_775_808.0;

/// Project a JSON-Schema integer through the public SQL `BIGINT` contract.
///
/// JSON has one numeric kind, so mathematically integral real spellings such
/// as `1.0` are valid JSON-Schema integers. SQL still needs an exact, bounded
/// `i64`: normalize integral real values and reject every value that cannot be
/// represented instead of silently projecting SQL `NULL`.
#[expect(
    clippy::cast_possible_truncation,
    reason = "the explicit integral and BIGINT range checks make the f64-to-i64 cast exact"
)]
pub(crate) fn json_bigint_value(
    value: Option<&JsonValue>,
    surface_name: &str,
    column_name: &str,
) -> Result<Option<i64>, LixError> {
    match value {
        None | Some(JsonValue::Null) => Ok(None),
        Some(number_value @ JsonValue::Number(number)) => {
            if let Some(value) = number.as_i64() {
                return Ok(Some(value));
            }
            if number.as_u64().is_some() {
                return Err(bigint_projection_error(
                    surface_name,
                    column_name,
                    number_value,
                ));
            }
            let Some(value) = number.as_f64() else {
                return Err(bigint_projection_error(
                    surface_name,
                    column_name,
                    number_value,
                ));
            };
            if value.fract() != 0.0
                || !(I64_LOWER_INCLUSIVE_AS_F64..I64_UPPER_EXCLUSIVE_AS_F64).contains(&value)
            {
                return Err(bigint_projection_error(
                    surface_name,
                    column_name,
                    number_value,
                ));
            }
            Ok(Some(value as i64))
        }
        Some(other) => Err(bigint_projection_error(surface_name, column_name, other)),
    }
}

/// Project a JSON-Schema number through the public SQL `DOUBLE PRECISION`
/// contract.
pub(crate) fn json_double_value(
    value: Option<&JsonValue>,
    surface_name: &str,
    column_name: &str,
) -> Result<Option<f64>, LixError> {
    match value {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Number(value)) => value
            .as_f64()
            .map(Some)
            .ok_or_else(|| double_projection_error(surface_name, column_name, value.to_string())),
        Some(other) => Err(double_projection_error(
            surface_name,
            column_name,
            other.to_string(),
        )),
    }
}

fn bigint_projection_error(surface_name: &str, column_name: &str, value: &JsonValue) -> LixError {
    LixError::new(
        LixError::CODE_TYPE_MISMATCH,
        format!(
            "typed SQL surface '{surface_name}' column '{column_name}' cannot represent JSON value {value} as BIGINT"
        ),
    )
    .with_hint(
        "Store an integral JSON number between -9223372036854775808 and 9223372036854775807.",
    )
}

fn double_projection_error(surface_name: &str, column_name: &str, value: String) -> LixError {
    LixError::new(
        LixError::CODE_TYPE_MISMATCH,
        format!(
            "typed SQL surface '{surface_name}' column '{column_name}' cannot represent JSON value {value} as DOUBLE PRECISION"
        ),
    )
}
