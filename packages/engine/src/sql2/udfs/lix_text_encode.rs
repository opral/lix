use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use super::common::{
    array_ref, binary_array_from_owned, encode_utf8_value, scalar_inputs,
    validate_utf8_encoding_arg,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct LixTextEncode {
    signature: Signature,
}

impl LixTextEncode {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![Signature::any(1, Volatility::Immutable).type_signature],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for LixTextEncode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "lix_text_encode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !(1..=2).contains(&args.args.len()) {
            return plan_err!("lix_text_encode requires 1 or 2 arguments");
        }
        validate_utf8_encoding_arg(self.name(), args.args.get(1))?;

        let scalar_inputs = scalar_inputs(&args.args);
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let input = &arrays[0];
        let len = input.len();

        let mut values = Vec::with_capacity(len);
        for row in 0..len {
            values.push(encode_utf8_value(input.as_ref(), row)?);
        }
        if scalar_inputs {
            Ok(ColumnarValue::Scalar(ScalarValue::Binary(
                values.into_iter().next().flatten(),
            )))
        } else {
            Ok(ColumnarValue::Array(array_ref(binary_array_from_owned(
                &values,
            ))))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::single_binary;

    #[tokio::test]
    async fn encodes_utf8_text_to_binary() {
        assert_eq!(
            single_binary("SELECT lix_text_encode('Ada')").await,
            Some(b"Ada".to_vec())
        );
    }
}
