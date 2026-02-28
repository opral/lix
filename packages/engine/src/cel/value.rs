use cel::Value as CelValue;
use serde_json::Value as JsonValue;

use crate::LixError;

pub fn json_to_cel(value: &JsonValue) -> Result<CelValue, LixError> {
    cel::to_value(value).map_err(|err| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!("failed to convert JSON value to CEL value: {err}"),
    })
}

pub fn cel_to_json(value: &CelValue) -> Result<JsonValue, LixError> {
    value.json().map_err(|err| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!("failed to convert CEL value to JSON value: {err}"),
    })
}

#[cfg(test)]
mod tests {
    use super::{cel_to_json, json_to_cel};
    use serde_json::json;

    #[test]
    fn converts_json_scalars() {
        let value = json!("hello");
        let cel = json_to_cel(&value).expect("convert to CEL");
        let roundtrip = cel_to_json(&cel).expect("convert to JSON");
        assert_eq!(roundtrip, value);
    }

    #[test]
    fn converts_json_objects_and_arrays() {
        let value = json!({
            "name": "Ada",
            "flags": [true, false],
            "meta": {
                "count": 1
            }
        });
        let cel = json_to_cel(&value).expect("convert to CEL");
        let roundtrip = cel_to_json(&cel).expect("convert to JSON");
        assert_eq!(roundtrip, value);
    }
}
