use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, FixedSizeBinaryArray, Int64Array,
    LargeBinaryArray, LargeStringArray, StringArray, StringViewArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, ScalarValue, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct LixOctetLength {
    signature: Signature,
}

impl LixOctetLength {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for LixOctetLength {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "octet_length"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return plan_err!("octet_length requires exactly one argument");
        }
        if matches!(
            arg_types[0],
            DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
                | DataType::FixedSizeBinary(_)
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Null
        ) {
            Ok(arg_types.to_vec())
        } else {
            plan_err!(
                "octet_length only accepts character or binary string values, got {}",
                arg_types[0]
            )
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = args.args.as_slice() else {
            return plan_err!("octet_length requires exactly one argument");
        };
        let scalar_input = matches!(arg, ColumnarValue::Scalar(_));
        let arrays = ColumnarValue::values_to_arrays(std::slice::from_ref(arg))?;
        let array = arrays
            .first()
            .ok_or_else(|| DataFusionError::Internal("octet_length received no argument".into()))?;
        let values = (0..array.len())
            .map(|row| byte_length(array, row))
            .collect::<Result<Vec<_>>>()?;

        if scalar_input {
            Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                values.into_iter().next().flatten(),
            )))
        } else {
            Ok(ColumnarValue::Array(Arc::new(Int64Array::from(values))))
        }
    }
}

fn byte_length(array: &ArrayRef, row: usize) -> Result<Option<i64>> {
    if array.is_null(row) {
        return Ok(None);
    }

    let length = match array.data_type() {
        DataType::Binary => array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("Binary data type should have a BinaryArray")
            .value(row)
            .len(),
        DataType::LargeBinary => array
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .expect("LargeBinary data type should have a LargeBinaryArray")
            .value(row)
            .len(),
        DataType::BinaryView => array
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .expect("BinaryView data type should have a BinaryViewArray")
            .value(row)
            .len(),
        DataType::FixedSizeBinary(_) => array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("FixedSizeBinary data type should have a FixedSizeBinaryArray")
            .value(row)
            .len(),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 data type should have a StringArray")
            .value(row)
            .len(),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .expect("LargeUtf8 data type should have a LargeStringArray")
            .value(row)
            .len(),
        DataType::Utf8View => array
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("Utf8View data type should have a StringViewArray")
            .value(row)
            .len(),
        data_type => {
            return Err(DataFusionError::Execution(format!(
                "octet_length does not support {data_type}"
            )));
        }
    };

    i64::try_from(length)
        .map(Some)
        .map_err(|_| DataFusionError::Execution("octet_length result exceeds BIGINT".to_string()))
}
