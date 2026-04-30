use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{plan_err, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use serde_json::Value as JsonValue;

use super::common::{scalar_inputs, text_like_value};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct LixJson;

impl ScalarUDFImpl for LixJson {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
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
}
