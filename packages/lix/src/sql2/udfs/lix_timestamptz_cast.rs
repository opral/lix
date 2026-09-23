//! Keep TIMESTAMPTZ casts in Lix's public microsecond/UTC result contract.
//!
//! DataFusion's SQL timestamp cast defaults to nanoseconds.  That physical
//! type is not a public Lix result type, so TIMESTAMPTZ casts go through this
//! private scalar UDF, whose schema is always the same type used by native
//! timestamptz parameters and registered columns.
use std::sync::Arc;

use datafusion::arrow::{
    compute::cast_with_options,
    datatypes::{DataType, Field, FieldRef, TimeUnit},
};
use datafusion::common::{Result, format::DEFAULT_CAST_OPTIONS, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct LixTimestamptzCast(Signature);

impl LixTimestamptzCast {
    pub(super) fn new() -> Self {
        Self(Signature::any(1, Volatility::Immutable))
    }
}

impl ScalarUDFImpl for LixTimestamptzCast {

    fn name(&self) -> &'static str {
        "__lix_timestamptz_cast"
    }

    fn signature(&self) -> &Signature {
        &self.0
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        Ok(format!("CAST({} AS TIMESTAMPTZ)", args[0].schema_name()))
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(Self::data_type())
    }

    fn return_field_from_args(&self, _: ReturnFieldArgs) -> Result<FieldRef> {
        // A fresh field prevents source metadata (for example JSONB text
        // metadata) from leaking into the timestamp result.
        Ok(Arc::new(Field::new(self.name(), Self::data_type(), true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return plan_err!("TIMESTAMPTZ cast requires 1 argument");
        };
        match value {
            ColumnarValue::Scalar(value) => {
                Ok(ColumnarValue::Scalar(value.cast_to(&Self::data_type())?))
            }
            ColumnarValue::Array(value) => Ok(ColumnarValue::Array(cast_with_options(
                value.as_ref(),
                &Self::data_type(),
                &DEFAULT_CAST_OPTIONS,
            )?)),
        }
    }
}

impl LixTimestamptzCast {
    fn data_type() -> DataType {
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    }
}
