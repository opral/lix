//! A text cast has its own SQL result type even when its physical input is UTF-8.
//! DataFusion's ordinary Cast retains input metadata and can remove a no-op
//! UTF-8 cast before parameters are bound, leaking JSONB/RowRef semantics.
use std::{any::Any, sync::Arc};

use datafusion::arrow::{
    compute::cast_with_options,
    datatypes::{DataType, Field, FieldRef},
};
use datafusion::common::{Result, format::DEFAULT_CAST_OPTIONS, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct LixTextCast(Signature);

impl LixTextCast {
    pub(super) fn new() -> Self {
        Self(Signature::any(1, Volatility::Immutable))
    }
}

impl ScalarUDFImpl for LixTextCast {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &'static str {
        "__lix_text_cast"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        Ok(format!("CAST({} AS TEXT)", args[0].schema_name()))
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn return_field_from_args(&self, _: ReturnFieldArgs) -> Result<FieldRef> {
        // Construct a fresh field: the source's JSONB, RowRef, or schema type
        // metadata does not describe a TEXT result.
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, true)))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return plan_err!("TEXT cast requires 1 argument");
        };
        match value {
            ColumnarValue::Scalar(value) => {
                Ok(ColumnarValue::Scalar(value.cast_to(&DataType::Utf8)?))
            }
            ColumnarValue::Array(value) => Ok(ColumnarValue::Array(cast_with_options(
                value.as_ref(),
                &DataType::Utf8,
                &DEFAULT_CAST_OPTIONS,
            )?)),
        }
    }
}
