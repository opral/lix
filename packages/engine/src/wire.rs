use crate::{LixError, QueryResult, Value};
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
    Blob { base64: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WireQueryResult {
    pub rows: Vec<Vec<WireValue>>,
    #[serde(default)]
    pub columns: Vec<String>,
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
                        description: "cannot encode non-finite float value to wire format"
                            .to_string(),
                    });
                }
                Ok(Self::Float { value: *value })
            }
            Value::Text(value) => Ok(Self::Text {
                value: value.clone(),
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
                        description: "cannot decode non-finite float value from wire format"
                            .to_string(),
                    });
                }
                Ok(Value::Real(value))
            }
            Self::Text { value } => Ok(Value::Text(value)),
            Self::Blob { base64 } => {
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(base64.as_bytes())
                    .map_err(|error| LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        description: format!("failed to decode wire blob base64: {error}"),
                    })?;
                Ok(Value::Blob(decoded))
            }
        }
    }
}

impl WireQueryResult {
    pub fn try_from_engine(result: &QueryResult) -> Result<Self, LixError> {
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
        })
    }

    pub fn try_into_engine(self) -> Result<QueryResult, LixError> {
        let mut rows = Vec::with_capacity(self.rows.len());
        for row in self.rows {
            let mut engine_row = Vec::with_capacity(row.len());
            for value in row {
                engine_row.push(value.try_into_engine()?);
            }
            rows.push(engine_row);
        }
        Ok(QueryResult {
            rows,
            columns: self.columns,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{WireQueryResult, WireValue};
    use crate::{QueryResult, Value};
    use serde_json::json;

    #[test]
    fn value_roundtrip_preserves_all_variants() {
        let original = vec![
            Value::Null,
            Value::Boolean(true),
            Value::Integer(42),
            Value::Real(1.5),
            Value::Text("hello".to_string()),
            Value::Blob(vec![1, 2, 3]),
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
        let original = QueryResult {
            rows: vec![
                vec![
                    Value::Integer(1),
                    Value::Text("a".to_string()),
                    Value::Blob(vec![0x41, 0x42]),
                ],
                vec![Value::Null, Value::Boolean(false), Value::Real(2.5)],
            ],
            columns: vec!["i".to_string(), "t".to_string(), "b".to_string()],
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
                WireValue::Blob {
                    base64: "AQI=".to_string(),
                },
            ]],
            columns: vec!["a".to_string()],
        };

        let serialized =
            serde_json::to_string(&wire).expect("wire query result should serialize to json");
        assert!(serialized.contains("\"kind\":\"null\""));
        assert!(serialized.contains("\"kind\":\"bool\""));
        assert!(serialized.contains("\"kind\":\"int\""));
        assert!(serialized.contains("\"kind\":\"float\""));
        assert!(serialized.contains("\"kind\":\"text\""));
        assert!(serialized.contains("\"kind\":\"blob\""));
        assert!(!serialized.contains("\"kind\":\"Null\""));
        assert!(!serialized.contains("\"kind\":\"Bool\""));
        assert!(!serialized.contains("\"kind\":\"Integer\""));
        assert!(!serialized.contains("\"kind\":\"Real\""));
        assert!(!serialized.contains("\"kind\":\"Text\""));
        assert!(!serialized.contains("\"kind\":\"Blob\""));
    }

    #[test]
    fn null_shape_is_explicitly_canonical() {
        let value = WireValue::Null { value: () };
        let json = serde_json::to_value(value).expect("wire value should serialize");
        assert_eq!(json, json!({ "kind": "null", "value": null }));
    }
}
