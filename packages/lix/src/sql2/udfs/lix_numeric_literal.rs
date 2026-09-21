//! Preserves the source spelling of numeric literals until a comparison's
//! column type is known.  The logical-plan rewrite consumes this marker for
//! integer columns; other numeric contexts use its ordinary f64 value.
use std::{any::Any, sync::Arc};

use datafusion::arrow::array::{Array, Float64Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, ScalarValue, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct LixNumericLiteral(Signature);

impl LixNumericLiteral {
    pub(super) fn new() -> Self {
        Self(Signature::exact(
            vec![DataType::Utf8],
            Volatility::Immutable,
        ))
    }
}

impl ScalarUDFImpl for LixNumericLiteral {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "__lix_numeric_literal"
    }

    fn signature(&self) -> &Signature {
        &self.0
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = args.args.as_slice() else {
            return plan_err!("numeric literal marker requires one argument");
        };
        let scalar = matches!(arg, ColumnarValue::Scalar(_));
        let arrays = ColumnarValue::values_to_arrays(std::slice::from_ref(arg))?;
        let input = arrays
            .first()
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                DataFusionError::Execution("numeric literal marker expects text".into())
            })?;
        let values = (0..input.len())
            .map(|row| {
                if input.is_null(row) {
                    Ok(None)
                } else {
                    input.value(row).parse::<f64>().map(Some).map_err(|error| {
                        DataFusionError::Execution(format!(
                            "invalid numeric literal '{}': {error}",
                            input.value(row)
                        ))
                    })
                }
            })
            .collect::<Result<Vec<_>>>()?;
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::Float64(
                values.into_iter().next().flatten(),
            )))
        } else {
            Ok(ColumnarValue::Array(Arc::new(Float64Array::from(values))))
        }
    }
}
