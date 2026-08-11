use std::{any::Any, sync::Arc};

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{DataFusionError, Result, ScalarValue, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::sql2::result_metadata::json_field;

use super::common::{canonical_jsonb_text, scalar_inputs, text_like_value};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct LixJsonb(Signature);

impl LixJsonb {
    pub(super) fn new() -> Self {
        Self(Signature::any(1, Volatility::Immutable))
    }
}

impl ScalarUDFImpl for LixJsonb {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &'static str {
        "__lix_jsonb"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(json_field(self.name(), true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return plan_err!("JSONB cast requires 1 argument");
        }
        let scalar = scalar_inputs(&args.args);
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let mut output = Vec::with_capacity(arrays[0].len());
        for row in 0..arrays[0].len() {
            output.push(match text_like_value(arrays[0].as_ref(), row)? {
                None => None,
                Some(raw) => Some(canonical_jsonb_text(&raw).map_err(|error| {
                    DataFusionError::Execution(format!("invalid JSONB value: {error}"))
                })?),
            });
        }
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                output.into_iter().next().flatten(),
            )))
        } else {
            Ok(ColumnarValue::Array(Arc::new(StringArray::from(output))))
        }
    }
}
