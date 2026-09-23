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

/// SQL evaluation values. JSON is a SQL type, never the transport for other
/// scalar types. Conversion to a JSON document belongs only at JSON boundaries.
#[derive(Clone, Debug)]
pub(crate) enum SqlValue {
    SqlNull,
    SqlText(String),
    Boolean(bool),
    Integer(i64),
    Unsigned(u64),
    Real(f64),
    Uuid(uuid::Uuid),
    Timestamptz(i64),
    Json(JsonValue),
    RowRef(crate::RowRef),
    Blob(crate::Blob),
}

impl SqlValue {
    pub(crate) fn from_public(value: &crate::Value) -> Result<Self, LixError> {
        Ok(match value {
            crate::Value::Null => Self::SqlNull,
            crate::Value::Text(v) => Self::SqlText(v.clone()),
            crate::Value::Boolean(v) => Self::Boolean(*v),
            crate::Value::Integer(v) => Self::Integer(*v),
            crate::Value::Real(v) => Self::Real(*v),
            crate::Value::Timestamptz(v) => Self::Timestamptz(*v),
            crate::Value::Jsonb(v) => Self::Json(
                super::udfs::common::parse_jsonb(v.as_str())
                    .map_err(|error| LixError::new(LixError::CODE_TYPE_MISMATCH, error))?,
            ),
            crate::Value::RowRef(v) => Self::RowRef(v.clone()),
            crate::Value::Blob(v) => Self::Blob(v.clone()),
        })
    }

    pub(crate) fn from_schema(value: &lix_schema::Value) -> Self {
        match value {
            lix_schema::Value::Null => Self::SqlNull,
            lix_schema::Value::Text(v) => Self::SqlText(v.clone()),
            lix_schema::Value::Uuid(v) => Self::Uuid(*v),
            lix_schema::Value::Int8(v) => Self::Integer(*v),
            lix_schema::Value::Float8(v) => Self::Real(*v),
            lix_schema::Value::Boolean(v) => Self::Boolean(*v),
            lix_schema::Value::Jsonb(v) => Self::Json(v.as_value().clone()),
            lix_schema::Value::Timestamptz(v) => Self::Timestamptz(*v),
        }
    }

    pub(crate) fn into_public(self) -> Result<crate::Value, LixError> {
        Ok(match self {
            Self::SqlNull => crate::Value::Null,
            Self::SqlText(v) => crate::Value::Text(v),
            Self::Uuid(v) => crate::Value::Text(v.to_string()),
            Self::Boolean(v) => crate::Value::Boolean(v),
            Self::Integer(v) => crate::Value::Integer(v),
            Self::Unsigned(v) => unsigned_integer_result(v)?,
            Self::Real(v) if v.is_finite() => crate::Value::Real(v),
            Self::Real(_) => {
                return Err(LixError::new(
                    LixError::CODE_TYPE_MISMATCH,
                    "SQL real result must be finite",
                ));
            }
            Self::Timestamptz(v) => crate::Value::Timestamptz(v),
            Self::Json(v) => crate::Value::Jsonb(v.into()),
            Self::RowRef(v) => crate::Value::RowRef(v),
            Self::Blob(v) => crate::Value::Blob(v),
        })
    }

    /// Equality of like native types needs neither a JSON DOM nor Arrow arrays.
    pub(crate) fn same_type_equal(&self, other: &Self) -> Option<bool> {
        Some(match (self, other) {
            (Self::SqlText(a), Self::SqlText(b)) => a == b,
            (Self::Uuid(a), Self::Uuid(b)) => a == b,
            (Self::Boolean(a), Self::Boolean(b)) => a == b,
            (Self::Integer(a), Self::Integer(b)) => a == b,
            (Self::Unsigned(a), Self::Unsigned(b)) => a == b,
            // Match DataFusion's float equality contract: signed zero compares
            // equal, and NaNs with the same payload compare equal.
            (Self::Real(a), Self::Real(b)) => {
                a == b || (a.is_nan() && b.is_nan() && a.to_bits() == b.to_bits())
            }
            (Self::Timestamptz(a), Self::Timestamptz(b)) => a == b,
            (Self::RowRef(a), Self::RowRef(b)) => a == b,
            (Self::Blob(a), Self::Blob(b)) => a == b,
            _ => return None,
        })
    }

    /// Physical scalar representation shared with DataFusion. Semantic JSONB
    /// and RowRef metadata is attached by the caller at Arrow field boundaries.
    pub(crate) fn scalar(self) -> datafusion::common::ScalarValue {
        use datafusion::common::ScalarValue as S;
        match self {
            Self::SqlNull => S::Null,
            Self::SqlText(v) => S::Utf8(Some(v)),
            Self::Uuid(v) => S::Utf8(Some(v.to_string())),
            Self::Boolean(v) => S::Boolean(Some(v)),
            Self::Integer(v) => S::Int64(Some(v)),
            Self::Unsigned(v) => S::UInt64(Some(v)),
            Self::Real(v) => S::Float64(Some(v)),
            Self::Timestamptz(v) => S::TimestampMicrosecond(Some(v), Some("UTC".into())),
            Self::Json(v) => S::Utf8(Some(v.to_string())),
            Self::RowRef(v) => S::Utf8(Some(v.as_str().to_owned())),
            Self::Blob(v) => S::LargeBinary(Some(v.to_vec())),
        }
    }

    pub(crate) fn from_scalar(value: datafusion::common::ScalarValue) -> Result<Self, LixError> {
        use datafusion::common::ScalarValue as S;
        if value.is_null() {
            return Ok(Self::SqlNull);
        }
        match value {
            S::Utf8(Some(v)) | S::Utf8View(Some(v)) | S::LargeUtf8(Some(v)) => Ok(Self::SqlText(v)),
            S::Boolean(Some(v)) => Ok(Self::Boolean(v)),
            S::Int64(Some(v)) => Ok(Self::Integer(v)),
            S::UInt64(Some(v)) => Ok(Self::Unsigned(v)),
            S::Float64(Some(v)) if v.is_finite() => Ok(Self::Real(v)),
            S::TimestampMicrosecond(Some(v), _) => Ok(Self::Timestamptz(v)),
            S::Binary(Some(v)) | S::LargeBinary(Some(v)) => Ok(Self::Blob(v.into())),
            v => Err(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                format!("unsupported SQL scalar {v:?}"),
            )),
        }
    }

    /// Explicit document serialization, not SQL coercion.
    pub(crate) fn into_json(self) -> Result<JsonValue, LixError> {
        Ok(match self {
            Self::SqlNull => JsonValue::Null,
            Self::SqlText(v) => v.into(),
            Self::Uuid(v) => v.to_string().into(),
            Self::Boolean(v) => v.into(),
            Self::Integer(v) => v.into(),
            Self::Unsigned(v) => v.into(),
            Self::Real(v) => serde_json::Number::from_f64(v)
                .map(JsonValue::Number)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_TYPE_MISMATCH,
                        "non-finite SQL real cannot be serialized as JSON",
                    )
                })?,
            Self::Timestamptz(v) => chrono::DateTime::from_timestamp_micros(v)
                .map(|v| JsonValue::String(v.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)))
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_TYPE_MISMATCH,
                        "timestamp is outside the JSON timestamp range",
                    )
                })?,
            Self::Json(v) => v,
            Self::RowRef(v) => v.as_str().to_owned().into(),
            Self::Blob(v) => JsonValue::Array(v.iter().copied().map(JsonValue::from).collect()),
        })
    }

    /// Use the same Arrow cast rules as planned assignments, then decode the
    /// destination's semantic type. JSON null is the text `null`, not SQL NULL.
    pub(crate) fn assign(
        self,
        target: lix_schema::DataType,
    ) -> Result<lix_schema::Value, LixError> {
        use datafusion::arrow::datatypes::{DataType as A, TimeUnit};
        use lix_schema::{DataType as T, Value as V};
        if matches!(self, Self::SqlNull) {
            return Ok(V::Null);
        }
        if matches!(self, Self::Json(_)) && target != T::Jsonb {
            return Err(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                "JSONB assignment to a scalar column requires an explicit CAST",
            ));
        }
        // Fast identity conversions avoid Arrow's one-element array allocation.
        let value = match (self, target) {
            (Self::SqlText(v), T::Text) => return Ok(V::Text(v)),
            (Self::RowRef(v), T::Text) => return Ok(V::Text(v.as_str().to_owned())),
            (Self::Uuid(v), T::Uuid) => return Ok(V::Uuid(v)),
            (Self::Integer(v), T::Int8) => return Ok(V::Int8(v)),
            (Self::Real(v), T::Float8) if v.is_finite() => return Ok(V::Float8(v)),
            (Self::Boolean(v), T::Boolean) => return Ok(V::Boolean(v)),
            (Self::Json(v), T::Jsonb) => return Ok(V::Jsonb(v.into())),
            (Self::Timestamptz(v), T::Timestamptz) => return Ok(V::Timestamptz(v)),
            (value, _) => value,
        };
        let arrow_type = match target {
            T::Text | T::Uuid | T::Jsonb => A::Utf8,
            T::Int8 => A::Int64,
            T::Float8 => A::Float64,
            T::Boolean => A::Boolean,
            T::Timestamptz => A::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        };
        let source = value.scalar();
        validate_assignment_types(&source.data_type(), &arrow_type)?;
        let scalar = source
            .cast_to(&arrow_type)
            .map_err(super::error::datafusion_error_to_lix_error)?;
        let value = Self::from_scalar(scalar)?;
        match (target, value) {
            (T::Text, Self::SqlText(v)) => Ok(V::Text(v)),
            (T::Uuid, Self::SqlText(v)) => uuid::Uuid::parse_str(&v).map(V::Uuid).map_err(|e| {
                LixError::new(LixError::CODE_TYPE_MISMATCH, format!("invalid UUID: {e}"))
            }),
            (T::Jsonb, Self::SqlText(v)) => Ok(V::Jsonb(
                serde_json::from_str::<JsonValue>(&v)
                    .unwrap_or(JsonValue::String(v))
                    .into(),
            )),
            (T::Int8, Self::Integer(v)) => Ok(V::Int8(v)),
            (T::Float8, Self::Real(v)) => Ok(V::Float8(v)),
            (T::Boolean, Self::Boolean(v)) => Ok(V::Boolean(v)),
            (T::Timestamptz, Self::Timestamptz(v)) => Ok(V::Timestamptz(v)),
            (_, Self::SqlNull) => Ok(V::Null),
            _ => Err(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                "invalid SQL assignment",
            )),
        }
    }
}

/// Epoch integers are not timestamps. Apply this guard before either scalar
/// evaluation or generic Arrow assignment planning can erase the source type.
pub(crate) fn validate_assignment_types(
    source: &datafusion::arrow::datatypes::DataType,
    target: &datafusion::arrow::datatypes::DataType,
) -> Result<(), LixError> {
    use datafusion::arrow::datatypes::DataType as A;
    if matches!(target, A::Timestamp(..))
        && !matches!(
            source,
            A::Null | A::Utf8 | A::Utf8View | A::LargeUtf8 | A::Timestamp(..)
        )
    {
        return Err(LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            "TIMESTAMPTZ assignment requires a timestamp or timestamp text",
        ));
    }
    Ok(())
}

/// Physical parameter conversion uses the same JSONB normalization as SQL casts.
pub(crate) fn public_scalar(
    value: &crate::Value,
) -> Result<datafusion::common::ScalarValue, LixError> {
    Ok(SqlValue::from_public(value)?.scalar())
}

pub(crate) fn unsigned_integer_result(value: u64) -> Result<crate::Value, LixError> {
    i64::try_from(value)
        .map(crate::Value::Integer)
        .map_err(|_| {
            LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                "SQL integer exceeds BIGINT result range; CAST AS TEXT to return decimal text",
            )
        })
}
