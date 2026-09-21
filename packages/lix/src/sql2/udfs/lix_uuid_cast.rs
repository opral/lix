use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use super::common::{parse_uuid, scalar_inputs, text_like_value};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct LixUuidCast(Signature);

impl LixUuidCast {
    pub(super) fn new() -> Self {
        Self(Signature::any(1, Volatility::Immutable))
    }
}

impl ScalarUDFImpl for LixUuidCast {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "__lix_uuid_cast"
    }

    fn signature(&self) -> &Signature {
        &self.0
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // Lix's public result boundary represents UUID values as canonical
        // strings while preserving UUID as the logical schema type.
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return plan_err!("UUID cast requires 1 argument");
        }
        let scalar = scalar_inputs(&args.args);
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let mut output = Vec::with_capacity(arrays[0].len());
        for row in 0..arrays[0].len() {
            output.push(match text_like_value(arrays[0].as_ref(), row)? {
                None => None,
                Some(raw) => Some(parse_uuid(&raw)?.to_string()),
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
