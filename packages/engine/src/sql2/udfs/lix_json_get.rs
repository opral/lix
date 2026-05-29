use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use serde_json::Value as JsonValue;

use crate::sql2::result_metadata::json_field;

use super::common::{extract_json_path, json_json_value, scalar_inputs};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct LixJsonGet {
    signature: Signature,
}

impl LixJsonGet {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for LixJsonGet {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "lix_json_get"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(json_field(self.name(), true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() < 2 {
            return plan_err!("lix_json_get requires at least 2 arguments");
        }

        let scalar_inputs = scalar_inputs(&args.args);
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let len = arrays.first().map(|array| array.len()).unwrap_or(1);

        let mut values = Vec::with_capacity(len);
        for row in 0..len {
            values.push(match extract_json_path(self.name(), &arrays, row)? {
                None | Some(JsonValue::Null) => None,
                Some(other) => Some(json_json_value(&other)?),
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
    async fn returns_json_representation() {
        assert_eq!(
            single_text("SELECT lix_json_get('{\"name\":\"Ada\"}', 'name')").await,
            Some("\"Ada\"".to_string())
        );
        assert_eq!(
            single_text("SELECT lix_json_get('{\"tags\":[\"db\"]}', 'tags')").await,
            Some("[\"db\"]".to_string())
        );
    }

    #[tokio::test]
    async fn missing_path_returns_null() {
        assert_eq!(
            single_text("SELECT lix_json_get('{\"name\":\"Ada\"}', 'missing')").await,
            None
        );
    }
}
