use serde_json::Value as JsonValue;

pub(crate) fn loosely_true(value: &JsonValue) -> bool {
    match value {
        JsonValue::Bool(boolean) => *boolean,
        JsonValue::Number(number) => {
            number.as_i64() == Some(1) || number.as_u64() == Some(1) || number.as_f64() == Some(1.0)
        }
        JsonValue::String(text) => text == "1",
        _ => false,
    }
}

pub(crate) fn loosely_false(value: &JsonValue) -> bool {
    match value {
        JsonValue::Bool(boolean) => !boolean,
        JsonValue::Number(number) => {
            number.as_i64() == Some(0) || number.as_u64() == Some(0) || number.as_f64() == Some(0.0)
        }
        JsonValue::String(text) => text.is_empty() || text == "0",
        JsonValue::Array(values) => values.is_empty(),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{loosely_false, loosely_true};

    #[test]
    fn loosely_true_handles_supported_truthy_shapes() {
        assert!(loosely_true(&json!(true)));
        assert!(loosely_true(&json!(1)));
        assert!(loosely_true(&json!(1.0)));
        assert!(loosely_true(&json!("1")));
    }

    #[test]
    fn loosely_false_handles_supported_falsy_shapes() {
        assert!(loosely_false(&json!(false)));
        assert!(loosely_false(&json!(0)));
        assert!(loosely_false(&json!(0.0)));
        assert!(loosely_false(&json!("0")));
        assert!(loosely_false(&json!("")));
        assert!(loosely_false(&json!([])));
    }
}
