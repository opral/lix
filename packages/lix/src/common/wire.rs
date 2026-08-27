use crate::{Json, LixError, LixNotice, ResultColumnType, SqlQueryResult, Value};
use base64::Engine as _;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum WireValue {
    Null { value: () },
    Bool { value: bool },
    Int { value: i64 },
    Float { value: f64 },
    Text { value: String },
    Jsonb { value: Json },
    #[serde(rename = "row_ref")]
    RowRef { value: String },
    Timestamptz { value: String },
    Blob { base64: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WireQueryResult {
    pub rows: Vec<Vec<WireValue>>,
    #[serde(default)]
    pub columns: Vec<String>,
    #[serde(default)]
    pub column_types: Vec<ResultColumnType>,
    #[serde(default)]
    pub notices: Vec<LixNotice>,
}

impl WireValue {
    pub fn try_from_engine(value: &Value) -> Result<Self, LixError> {
        match value {
            Value::Null => Ok(Self::Null { value: () }),
            Value::Boolean(value) => Ok(Self::Bool { value: *value }),
            Value::Integer(value) => Ok(Self::Int { value: *value }),
            Value::Real(value) => {
                if !value.is_finite() {
                    return Err(LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        message: "cannot encode non-finite float value to wire format".to_string(),
                        hint: None,
                        details: None,
                    });
                }
                Ok(Self::Float { value: *value })
            }
            Value::Text(value) => Ok(Self::Text {
                value: value.clone(),
            }),
            Value::Jsonb(value) => Ok(Self::Jsonb {
                value: value.clone(),
            }),
            Value::RowRef(value) => Ok(Self::RowRef {
                value: value.as_str().to_owned(),
            }),
            Value::Timestamptz(value) => Ok(Self::Timestamptz {
                value: format_timestamptz(*value)?,
            }),
            Value::Blob(value) => Ok(Self::Blob {
                base64: base64::engine::general_purpose::STANDARD.encode(value),
            }),
        }
    }

    pub fn try_into_engine(self) -> Result<Value, LixError> {
        match self {
            Self::Null { .. } => Ok(Value::Null),
            Self::Bool { value } => Ok(Value::Boolean(value)),
            Self::Int { value } => Ok(Value::Integer(value)),
            Self::Float { value } => {
                if !value.is_finite() {
                    return Err(LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        message: "cannot decode non-finite float value from wire format"
                            .to_string(),
                        hint: None,
                        details: None,
                    });
                }
                Ok(Value::Real(value))
            }
            Self::Text { value } => Ok(Value::Text(value)),
            Self::Jsonb { value } => Ok(Value::Jsonb(value)),
            Self::RowRef { value } => crate::row_ref::decode_str(&value)
                .map(|_| Value::RowRef(crate::RowRef(value))),
            Self::Timestamptz { value } => parse_timestamptz(&value).map(Value::Timestamptz),
            Self::Blob { base64 } => {
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(base64.as_bytes())
                    .map_err(|error| LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        message: format!("failed to decode wire blob base64: {error}"),
                        hint: None,
                        details: None,
                    })?;
                Ok(Value::Blob(decoded.into()))
            }
        }
    }
}

fn format_timestamptz(value: i64) -> Result<String, LixError> {
    chrono::DateTime::from_timestamp_micros(value)
        .map(|value| value.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
        .ok_or_else(|| LixError::new(LixError::CODE_INVALID_PARAM, "timestamptz is out of range"))
}

fn parse_timestamptz(value: &str) -> Result<i64, LixError> {
    chrono::DateTime::parse_from_rfc3339(value)
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!("timestamptz wire value is invalid: {error}"),
            )
        })?
        .timestamp_micros()
        .try_into()
        .map_err(|_| LixError::new(LixError::CODE_INVALID_PARAM, "timestamptz is out of range"))
}

impl WireQueryResult {
    pub fn try_from_engine(result: &SqlQueryResult) -> Result<Self, LixError> {
        let mut rows = Vec::with_capacity(result.rows.len());
        for row in &result.rows {
            let mut wire_row = Vec::with_capacity(row.len());
            for value in row {
                wire_row.push(WireValue::try_from_engine(value)?);
            }
            rows.push(wire_row);
        }
        Ok(Self {
            rows,
            columns: result.columns.clone(),
            column_types: result.column_types.clone(),
            notices: result.notices.clone(),
        })
    }

    pub fn try_into_engine(self) -> Result<SqlQueryResult, LixError> {
        let mut rows = Vec::with_capacity(self.rows.len());
        for row in self.rows {
            let mut engine_row = Vec::with_capacity(row.len());
            for value in row {
                engine_row.push(value.try_into_engine()?);
            }
            rows.push(engine_row);
        }
        Ok(SqlQueryResult {
            rows,
            columns: self.columns,
            column_types: self.column_types,
            notices: self.notices,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{WireQueryResult, WireValue};
    use crate::{LixNotice, ResultColumnType, SqlQueryResult, Value};
    use serde_json::json;

    #[test]
    fn value_roundtrip_preserves_all_variants() {
        let original = vec![
            Value::Null,
            Value::Boolean(true),
            Value::Integer(42),
            Value::Real(1.5),
            Value::Text("hello".to_string()),
            Value::Jsonb(json!({"hello": "world"}).into()),
            Value::RowRef(
                crate::row_ref::encode("lix_key_value", &crate::row_pk::RowPk::single("hello"))
                    .expect("test row reference should encode"),
            ),
            Value::Timestamptz(1_700_000_000_000_000),
            Value::Blob(vec![1, 2, 3].into()),
        ];

        for value in original {
            let wire = WireValue::try_from_engine(&value).expect("to wire should succeed");
            let roundtrip = wire
                .try_into_engine()
                .expect("from wire to engine should succeed");
            assert_eq!(roundtrip, value);
        }
    }

    #[test]
    fn query_result_roundtrip_preserves_rows_and_columns() {
        let original = SqlQueryResult {
            rows: vec![
                vec![
                    Value::Integer(1),
                    Value::Text("a".to_string()),
                    Value::Blob(vec![0x41, 0x42].into()),
                ],
                vec![Value::Null, Value::Boolean(false), Value::Real(2.5)],
            ],
            columns: vec!["i".to_string(), "t".to_string(), "b".to_string()],
            column_types: vec![
                ResultColumnType::Integer,
                ResultColumnType::Text,
                ResultColumnType::Blob,
            ],
            notices: vec![LixNotice {
                code: "LIX_TEST_NOTICE".to_string(),
                message: "test notice".to_string(),
                hint: Some("test hint".to_string()),
            }],
        };

        let wire = WireQueryResult::try_from_engine(&original).expect("to wire should succeed");
        let roundtrip = wire
            .try_into_engine()
            .expect("from wire to engine should succeed");
        assert_eq!(roundtrip, original);
    }

    #[test]
    fn canonical_json_uses_lowercase_kinds_only() {
        let wire = WireQueryResult {
            rows: vec![vec![
                WireValue::Null { value: () },
                WireValue::Bool { value: true },
                WireValue::Int { value: 1 },
                WireValue::Float { value: 1.5 },
                WireValue::Text {
                    value: "hello".to_string(),
                },
                WireValue::Jsonb {
                    value: json!({"hello": "world"}).into(),
                },
                WireValue::RowRef {
                    value: "lix_row_ref:v1:AAAADWxpeF9rZXlfdmFsdWUAAQMAAAAFaGVsbG8"
                        .to_string(),
                },
                WireValue::Timestamptz {
                    value: "2023-11-14T22:13:20.000000Z".to_string(),
                },
                WireValue::Blob {
                    base64: "AQI=".to_string(),
                },
            ]],
            columns: vec!["a".to_string()],
            column_types: vec![ResultColumnType::Null],
            notices: Vec::new(),
        };

        let serialized =
            serde_json::to_string(&wire).expect("wire query result should serialize to json");
        assert!(serialized.contains("\"kind\":\"null\""));
        assert!(serialized.contains("\"kind\":\"bool\""));
        assert!(serialized.contains("\"kind\":\"int\""));
        assert!(serialized.contains("\"kind\":\"float\""));
        assert!(serialized.contains("\"kind\":\"text\""));
        assert!(serialized.contains("\"kind\":\"jsonb\""));
        assert!(serialized.contains("\"kind\":\"row_ref\""));
        assert!(serialized.contains("\"kind\":\"timestamptz\""));
        assert!(serialized.contains("\"kind\":\"blob\""));
        assert!(!serialized.contains("\"kind\":\"Null\""));
        assert!(!serialized.contains("\"kind\":\"Bool\""));
        assert!(!serialized.contains("\"kind\":\"Integer\""));
        assert!(!serialized.contains("\"kind\":\"Real\""));
        assert!(!serialized.contains("\"kind\":\"Text\""));
        assert!(!serialized.contains("\"kind\":\"Json\""));
        assert!(!serialized.contains("\"kind\":\"timestamp\""));
        assert!(!serialized.contains("\"kind\":\"Blob\""));
    }

    #[test]
    fn legacy_json_and_timestamp_wire_kinds_are_rejected() {
        for legacy in [
            json!({ "kind": "json", "value": { "ok": true } }),
            json!({ "kind": "timestamp", "value": "2023-11-14T22:13:20Z" }),
        ] {
            let error = serde_json::from_value::<WireValue>(legacy)
                .expect_err("legacy wire value kind must not decode");
            assert!(
                error.to_string().contains("unknown variant"),
                "unexpected serde rejection: {error}"
            );
        }
    }

    #[test]
    fn null_shape_is_explicitly_canonical() {
        let value = WireValue::Null { value: () };
        let json = serde_json::to_value(value).expect("wire value should serialize");
        assert_eq!(json, json!({ "kind": "null", "value": null }));
    }
}
