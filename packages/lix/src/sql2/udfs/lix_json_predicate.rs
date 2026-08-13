use std::{any::Any, sync::Arc};

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use serde_json::Value;

use super::common::{json_value_to_serde, scalar_inputs, text_like_value};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct LixJsonPredicate {
    signature: Signature,
    kind: PredicateKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum PredicateKind {
    Contains,
    Exists,
}

impl LixJsonPredicate {
    pub(super) fn contains() -> Self {
        Self::new(PredicateKind::Contains)
    }

    pub(super) fn exists() -> Self {
        Self::new(PredicateKind::Exists)
    }

    fn new(kind: PredicateKind) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            kind,
        }
    }
}

impl ScalarUDFImpl for LixJsonPredicate {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        match self.kind {
            PredicateKind::Contains => "__lix_json_contains",
            PredicateKind::Exists => "__lix_json_exists",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return plan_err!("{} requires 2 arguments", self.name());
        }
        let scalar = scalar_inputs(&args.args);
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let mut output = Vec::with_capacity(arrays[0].len());
        for row in 0..arrays[0].len() {
            let Some(left) = json_value_to_serde(arrays[0].as_ref(), row)? else {
                output.push(None);
                continue;
            };
            let value = match self.kind {
                PredicateKind::Contains => json_value_to_serde(arrays[1].as_ref(), row)?
                    .map(|right| contains(&left, &right)),
                PredicateKind::Exists => {
                    text_like_value(arrays[1].as_ref(), row)?.map(|key| exists(&left, &key))
                }
            };
            output.push(value);
        }
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(
                output.into_iter().next().flatten(),
            )))
        } else {
            Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(output))))
        }
    }
}

fn contains(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Object(left), Value::Object(right)) => right
            .iter()
            .all(|(key, value)| left.get(key).is_some_and(|left| contains(left, value))),
        (Value::Array(left), Value::Array(right)) => right
            .iter()
            .all(|value| left.iter().any(|left| contains(left, value))),
        (Value::Array(left), right) => left.iter().any(|left| contains(left, right)),
        _ => left == right,
    }
}

fn exists(value: &Value, key: &str) -> bool {
    match value {
        Value::Object(value) => value.contains_key(key),
        Value::Array(value) => value.iter().any(|value| value.as_str() == Some(key)),
        _ => false,
    }
}
