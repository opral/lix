//! Assignment coercion is stricter than an explicit numeric CAST: fractions
//! cannot silently disappear while storing a BIGINT schema column.
use datafusion::arrow::array::{Array, Float64Array, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::{any::Any, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AssignBigint(Signature);

impl ScalarUDFImpl for AssignBigint {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &'static str {
        "__lix_assign_bigint"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let convert = |value| {
            crate::sql2::value_contract::exact_real_bigint(value)
                .map_err(crate::sql2::error::lix_error_to_datafusion_error)
        };
        match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Float64(value)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Int64(value.map(convert).transpose()?),
            )),
            ColumnarValue::Array(array) => {
                let array = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        datafusion::common::DataFusionError::Internal(
                            "BIGINT assignment expected Float64".into(),
                        )
                    })?;
                let values = (0..array.len())
                    .map(|i| {
                        if array.is_null(i) {
                            Ok(None)
                        } else {
                            convert(array.value(i)).map(Some)
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(ColumnarValue::Array(Arc::new(Int64Array::from(values))))
            }
            _ => datafusion::common::internal_err!("BIGINT assignment expected Float64"),
        }
    }
}

pub(crate) fn expression(expr: Expr) -> Expr {
    ScalarUDF::from(AssignBigint(Signature::exact(
        vec![DataType::Float64],
        Volatility::Immutable,
    )))
    .call(vec![expr])
}
