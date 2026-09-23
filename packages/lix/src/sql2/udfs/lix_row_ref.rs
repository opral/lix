use std::hash::{Hash, Hasher};
use std::sync::Arc;

use base64::Engine as _;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{DataFusionError, Result, ScalarValue, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::row_pk::{RowPk, RowPkComponentType};
use crate::sql2::catalog::PublicCatalog;
use crate::sql2::result_metadata::row_ref_field;

use super::common::scalar_inputs;

#[derive(Debug)]
pub(super) struct LixRowRef {
    signature: Signature,
    catalog: Arc<PublicCatalog>,
}

impl PartialEq for LixRowRef {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature && Arc::ptr_eq(&self.catalog, &other.catalog)
    }
}

impl Eq for LixRowRef {}

impl Hash for LixRowRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
        Arc::as_ptr(&self.catalog).hash(state);
    }
}

impl LixRowRef {
    pub(super) fn new(catalog: Arc<PublicCatalog>) -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Stable),
            catalog,
        }
    }
}

impl ScalarUDFImpl for LixRowRef {
        fn name(&self) -> &'static str {
        "lix_row_ref"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(row_ref_field(self.name(), true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() < 3 {
            return plan_err!(
                "lix_row_ref requires a relation, a nullable file id, and at least one primary-key value"
            );
        }
        let scalar = scalar_inputs(&args.args);
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let len = arrays[0].len();
        let mut output = Vec::with_capacity(len);
        for row in 0..len {
            let relation_value = ScalarValue::try_from_array(arrays[0].as_ref(), row)?;
            let file_id_value = ScalarValue::try_from_array(arrays[1].as_ref(), row)?;
            output.push(
                construct_row_ref(
                    &self.catalog,
                    &relation_value,
                    &file_id_value,
                    arrays[2..]
                        .iter()
                        .map(|array| ScalarValue::try_from_array(array.as_ref(), row)),
                )?
                .map(|row_ref| row_ref.as_str().to_owned()),
            );
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

/// Shared constructor for SELECT UDF evaluation and native INSERT/UPDATE expressions.
pub(crate) fn construct_row_ref(
    catalog: &PublicCatalog,
    relation_value: &ScalarValue,
    file_id_value: &ScalarValue,
    components: impl ExactSizeIterator<Item = Result<ScalarValue>>,
) -> Result<Option<crate::RowRef>> {
    let relation = scalar_text(relation_value).ok_or_else(|| {
        DataFusionError::Execution("lix_row_ref relation must be non-null text".to_string())
    })?;
    let file_id = scalar_optional_text(file_id_value).ok_or_else(|| {
        DataFusionError::Execution("lix_row_ref file id must be null or text".to_string())
    })?;
    let component_types = crate::row_ref::primary_key_component_types(catalog, relation)
        .map_err(crate::sql2::error::lix_error_to_datafusion_error)?;
    if component_types.len() != components.len() {
        return Err(DataFusionError::Execution(format!(
            "lix_row_ref relation '{relation}' requires {} primary-key values, got {}",
            component_types.len(),
            components.len(),
        )));
    }
    let mut parts = Vec::with_capacity(component_types.len());
    for (index, (value, expected)) in components.zip(&component_types).enumerate() {
        let value = value?;
        if value.is_null() {
            return Ok(None);
        }
        parts.push(external_component(&value, *expected, index)?);
    }
    let row_pk = RowPk::from_external_parts(parts, &component_types).map_err(|error| {
        DataFusionError::Execution(format!(
            "lix_row_ref relation '{relation}' has an invalid primary key: {error}"
        ))
    })?;
    crate::row_ref::encode(relation, file_id, &row_pk)
        .map(Some)
        .map_err(crate::sql2::error::lix_error_to_datafusion_error)
}

fn external_component(
    value: &ScalarValue,
    expected: RowPkComponentType,
    index: usize,
) -> Result<String> {
    let value = match expected {
        RowPkComponentType::Uuid | RowPkComponentType::String => {
            scalar_text(value).map(str::to_owned)
        }
        RowPkComponentType::Bytes => match value {
            ScalarValue::Binary(Some(value))
            | ScalarValue::LargeBinary(Some(value))
            | ScalarValue::BinaryView(Some(value)) => {
                Some(base64::engine::general_purpose::STANDARD.encode(value))
            }
            _ => scalar_text(value).map(str::to_owned),
        },
        RowPkComponentType::Integer => scalar_integer(value).map(|value| value.to_string()),
    };
    value.ok_or_else(|| {
        DataFusionError::Execution(format!(
            "lix_row_ref primary-key value {} must be a non-null {}",
            index + 1,
            match expected {
                RowPkComponentType::Uuid => "UUID string",
                RowPkComponentType::Integer => "integer",
                RowPkComponentType::String => "text value",
                RowPkComponentType::Bytes => "base64 string",
            }
        ))
    })
}

fn scalar_text(value: &ScalarValue) -> Option<&str> {
    match value {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => Some(value),
        _ => None,
    }
}

fn scalar_optional_text(value: &ScalarValue) -> Option<Option<&str>> {
    match value {
        ScalarValue::Null => Some(None),
        ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) | ScalarValue::Utf8View(None) => {
            Some(None)
        }
        _ => scalar_text(value).map(Some),
    }
}

fn scalar_integer(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int8(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int16(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int32(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int64(Some(value)) => Some(*value),
        ScalarValue::UInt8(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt16(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt32(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt64(Some(value)) => i64::try_from(*value).ok(),
        _ => None,
    }
}
