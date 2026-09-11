//! Dispatch request parameters by their wire tag once. Nesting an untagged
//! enum around WireValue made serde buffer and clone large text parameters.
use crate::{Json, WireValue};
use serde::{Deserialize, Deserializer};

#[derive(Debug)]
pub(super) enum RequestWireValue {
    BlobSplice(RequestBlobSplice),
    Value(WireValue),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct RequestBlobSplice {
    pub(super) base_sha256: String,
    pub(super) result_sha256: String,
    pub(super) prefix_bytes: u64,
    pub(super) suffix_bytes: u64,
    pub(super) insert_base64: String,
}

impl<'de> Deserialize<'de> for RequestWireValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(tag = "kind", rename_all = "lowercase")]
        enum Tagged {
            Null {
                value: (),
            },
            Bool {
                value: bool,
            },
            Int {
                value: i64,
            },
            Float {
                value: f64,
            },
            Text {
                value: String,
            },
            Jsonb {
                value: Json,
            },
            #[serde(rename = "row_ref")]
            RowRef {
                value: String,
            },
            Timestamptz {
                value: String,
            },
            Blob {
                base64: String,
            },
            #[serde(rename = "blob-splice")]
            BlobSplice(RequestBlobSplice),
        }
        Ok(match Tagged::deserialize(deserializer)? {
            Tagged::Null { value } => Self::Value(WireValue::Null { value }),
            Tagged::Bool { value } => Self::Value(WireValue::Bool { value }),
            Tagged::Int { value } => Self::Value(WireValue::Int { value }),
            Tagged::Float { value } => Self::Value(WireValue::Float { value }),
            Tagged::Text { value } => Self::Value(WireValue::Text { value }),
            Tagged::Jsonb { value } => Self::Value(WireValue::Jsonb { value }),
            Tagged::RowRef { value } => Self::Value(WireValue::RowRef { value }),
            Tagged::Timestamptz { value } => Self::Value(WireValue::Timestamptz { value }),
            Tagged::Blob { base64 } => Self::Value(WireValue::Blob { base64 }),
            Tagged::BlobSplice(splice) => Self::BlobSplice(splice),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sql_request_value_preserves_all_wire_types() {
        let values = [
            json!({"kind":"null","value":null}),
            json!({"kind":"bool","value":true}),
            json!({"kind":"int","value":i64::MIN}),
            json!({"kind":"float","value":-0.0}),
            json!({"kind":"text","value":"\u{feff}\r\n\"λ😀\\\0".repeat(16_384)}),
            json!({"kind":"jsonb","value":{"b":[null,42],"a":"text"}}),
            json!({"kind":"row_ref","value":"validated by the engine"}),
            json!({"kind":"timestamptz","value":"2026-09-10T00:00:00Z"}),
            json!({"kind":"blob","base64":"AAH/"}),
        ];
        for value in values {
            let encoded = serde_json::to_vec(&value).unwrap();
            let expected: WireValue = serde_json::from_slice(&encoded).unwrap();
            let RequestWireValue::Value(actual) = serde_json::from_slice(&encoded).unwrap() else {
                panic!("ordinary value became a splice")
            };
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn sql_request_value_accepts_late_kind_and_blob_splices() {
        let RequestWireValue::Value(WireValue::Text { value }) =
            serde_json::from_str(r#"{"value":"before tag","kind":"text"}"#).unwrap()
        else {
            panic!("late tag rejected")
        };
        assert_eq!(value, "before tag");
        let value = json!({"kind":"blob-splice","baseSha256":"a".repeat(64),"resultSha256":"b".repeat(64),"prefixBytes":3,"suffixBytes":2,"insertBase64":"AAH/"});
        let RequestWireValue::BlobSplice(splice) = serde_json::from_value(value).unwrap() else {
            panic!("splice rejected")
        };
        assert_eq!(splice.prefix_bytes, 3);
        assert_eq!(splice.suffix_bytes, 2);
        assert_eq!(splice.base_sha256, "a".repeat(64));
        assert_eq!(splice.result_sha256, "b".repeat(64));
        assert_eq!(splice.insert_base64, "AAH/");
    }

    #[test]
    fn sql_request_value_rejects_malformed_parameters() {
        for encoded in [
            r#"{"kind":"unknown","value":0}"#,
            r#"{"value":"no tag"}"#,
            r#"{"kind":"text","value":123}"#,
            r#"{"kind":"int","value":1.5}"#,
            r#"{"kind":"text","kind":"blob","value":"ambiguous"}"#,
            r#"{"kind":"blob-splice","baseSha256":"missing fields"}"#,
        ] {
            assert!(
                serde_json::from_str::<RequestWireValue>(encoded).is_err(),
                "accepted {encoded}"
            );
        }
    }
}
