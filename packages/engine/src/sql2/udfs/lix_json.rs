use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{DataFusionError, Result, ScalarValue, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use serde_json::Value as JsonValue;

use crate::sql2::result_metadata::json_field;

use super::common::{scalar_inputs, text_like_value};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct LixJson;

impl ScalarUDFImpl for LixJson {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "lix_json"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(json_field(self.name(), true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return plan_err!("lix_json requires exactly 1 argument");
        }
        let scalar_inputs = scalar_inputs(&args.args);
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let input = &arrays[0];
        let len = input.len();
        let mut values = Vec::with_capacity(len);
        for row in 0..len {
            values.push(json_value(input.as_ref(), row)?);
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

fn json_value(array: &dyn Array, row: usize) -> Result<Option<String>> {
    if matches!(array.data_type(), DataType::Null) {
        return Ok(Some("null".to_string()));
    }
    let Some(raw) = text_like_value(array, row)? else {
        return Ok(Some("null".to_string()));
    };
    let parsed = serde_json::from_str::<JsonValue>(&raw).map_err(|error| {
        DataFusionError::Execution(format!(
            "lix_json() expected valid JSON text, got error: {error}"
        ))
    })?;
    Ok(Some(serde_json::to_string(&parsed).map_err(|error| {
        DataFusionError::Execution(format!("lix_json() could not render JSON: {error}"))
    })?))
}

#[cfg(test)]
mod tests {
    use super::super::test_support::single_text;

    #[tokio::test]
    async fn canonicalizes_json_text() {
        assert_eq!(
            single_text("SELECT lix_json('{ \"name\" : \"Ada\" }')").await,
            Some("{\"name\":\"Ada\"}".to_string())
        );
    }

    #[tokio::test]
    async fn null_input_returns_json_null() {
        assert_eq!(
            single_text("SELECT lix_json(NULL)").await,
            Some("null".to_string())
        );
    }
}
