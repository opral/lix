use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use serde_json::Value as JsonValue;

use super::common::{extract_json_path, json_text_value, scalar_inputs};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct LixJsonGetText {
    signature: Signature,
}

impl LixJsonGetText {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for LixJsonGetText {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "lix_json_get_text"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() < 2 {
            return plan_err!("lix_json_get_text requires at least 2 arguments");
        }

        let scalar_inputs = scalar_inputs(&args.args);
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let len = arrays.first().map(|array| array.len()).unwrap_or(1);

        let mut values = Vec::with_capacity(len);
        for row in 0..len {
            values.push(match extract_json_path(self.name(), &arrays, row)? {
                None | Some(JsonValue::Null) => None,
                Some(JsonValue::Bool(value)) => Some(if value {
                    "true".to_string()
                } else {
                    "false".to_string()
                }),
                Some(JsonValue::String(value)) => Some(value),
                Some(other) => Some(json_text_value(&other)?),
            });
        }
        if scalar_inputs {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                values.into_iter().next().flatten(),
            )))
        } else {
            Ok(ColumnarValue::Array(Arc::new(StringArray::from(values))))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::single_text;

    #[tokio::test]
    async fn returns_unwrapped_text() {
        assert_eq!(
            single_text("SELECT lix_json_get_text('{\"name\":\"Ada\"}', 'name')").await,
            Some("Ada".to_string())
        );
        assert_eq!(
            single_text("SELECT lix_json_get_text('{\"active\":true}', 'active')").await,
            Some("true".to_string())
        );
    }

    #[tokio::test]
    async fn missing_path_returns_null() {
        assert_eq!(
            single_text("SELECT lix_json_get_text('{\"name\":\"Ada\"}', 'missing')").await,
            None
        );
    }
}
