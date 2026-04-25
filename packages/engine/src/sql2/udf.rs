use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, StringArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{plan_err, DataFusionError, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    lit, ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use serde_json::Value as JsonValue;

use crate::functions::{
    DynFunctionProvider, LixFunctionProvider, SharedFunctionProvider, SystemFunctionProvider,
};

pub(crate) fn system_sql2_function_provider() -> DynFunctionProvider {
    SharedFunctionProvider::new(
        Box::new(SystemFunctionProvider) as Box<dyn LixFunctionProvider + Send>
    )
}

pub(crate) fn register_sql2_udfs(ctx: &SessionContext, functions: DynFunctionProvider) {
    ctx.register_udf(ScalarUDF::from(LixJsonExtract::new(
        "lix_json_extract",
        JsonExtractMode::Text,
    )));
    ctx.register_udf(ScalarUDF::from(LixJsonExtract::new(
        "lix_json_extract_json",
        JsonExtractMode::Json,
    )));
    ctx.register_udf(ScalarUDF::from(LixJsonExtract::new(
        "lix_json_extract_variant",
        JsonExtractMode::Variant,
    )));
    ctx.register_udf(ScalarUDF::from(LixJsonExtract::new(
        "lix_json_extract_boolean",
        JsonExtractMode::Boolean,
    )));
    ctx.register_udf(ScalarUDF::from(LixTextCodec::new(
        "lix_text_decode",
        TextCodecMode::Decode,
    )));
    ctx.register_udf(ScalarUDF::from(LixTextCodec::new(
        "lix_text_encode",
        TextCodecMode::Encode,
    )));
    ctx.register_udf(ScalarUDF::from(LixJson));
    ctx.register_udf(ScalarUDF::from(LixEmptyBlob));
    ctx.register_udf(ScalarUDF::from(LixUuidV7 { functions }));
}

pub(crate) fn lix_json_extract_text_expr(json_expr: Expr, property_name: &str) -> Expr {
    ScalarUDF::from(LixJsonExtract::new(
        "lix_json_extract",
        JsonExtractMode::Text,
    ))
    .call(vec![json_expr, lit(property_name.to_string())])
}

pub(crate) fn lix_json_extract_json_expr(json_expr: Expr, property_name: &str) -> Expr {
    ScalarUDF::from(LixJsonExtract::new(
        "lix_json_extract_json",
        JsonExtractMode::Json,
    ))
    .call(vec![json_expr, lit(property_name.to_string())])
}

pub(crate) fn lix_json_extract_variant_expr(json_expr: Expr, property_name: &str) -> Expr {
    ScalarUDF::from(LixJsonExtract::new(
        "lix_json_extract_variant",
        JsonExtractMode::Variant,
    ))
    .call(vec![json_expr, lit(property_name.to_string())])
}

pub(crate) fn lix_json_extract_boolean_expr(json_expr: Expr, property_name: &str) -> Expr {
    ScalarUDF::from(LixJsonExtract::new(
        "lix_json_extract_boolean",
        JsonExtractMode::Boolean,
    ))
    .call(vec![json_expr, lit(property_name.to_string())])
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum JsonExtractMode {
    Text,
    Json,
    Variant,
    Boolean,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LixJsonExtract {
    name: &'static str,
    mode: JsonExtractMode,
    signature: Signature,
}

impl LixJsonExtract {
    fn new(name: &'static str, mode: JsonExtractMode) -> Self {
        Self {
            name,
            mode,
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for LixJsonExtract {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(match self.mode {
            JsonExtractMode::Boolean => DataType::Boolean,
            JsonExtractMode::Text | JsonExtractMode::Json => DataType::Utf8,
            JsonExtractMode::Variant => DataType::Binary,
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() < 2 {
            return plan_err!("{} requires at least 2 arguments", self.name);
        }

        let scalar_inputs = args
            .args
            .iter()
            .all(|value| matches!(value, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let len = arrays.first().map(ArrayRef::len).unwrap_or(1);

        match self.mode {
            JsonExtractMode::Boolean => {
                let mut values = Vec::with_capacity(len);
                for row in 0..len {
                    let extracted = extract_json_path(&arrays, row)?;
                    values.push(match extracted {
                        Some(JsonValue::Bool(value)) => Some(value),
                        _ => None,
                    });
                }
                if scalar_inputs {
                    Ok(ColumnarValue::Scalar(ScalarValue::Boolean(
                        values.into_iter().next().flatten(),
                    )))
                } else {
                    Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(values))))
                }
            }
            JsonExtractMode::Text | JsonExtractMode::Json => {
                let mut values = Vec::with_capacity(len);
                for row in 0..len {
                    let extracted = extract_json_path(&arrays, row)?;
                    values.push(match (self.mode, extracted) {
                        (_, None) => None,
                        (_, Some(JsonValue::Null)) => None,
                        (JsonExtractMode::Text, Some(JsonValue::Bool(value))) => Some(if value {
                            "true".to_string()
                        } else {
                            "false".to_string()
                        }),
                        (JsonExtractMode::Text, Some(JsonValue::String(value))) => Some(value),
                        (JsonExtractMode::Text, Some(other)) => Some(json_text_value(&other)?),
                        (JsonExtractMode::Json, Some(other)) => Some(json_json_value(&other)?),
                        (JsonExtractMode::Variant, _) | (JsonExtractMode::Boolean, _) => {
                            unreachable!()
                        }
                    });
                }
                if scalar_inputs {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                        values.into_iter().next().flatten(),
                    )))
                } else {
                    Ok(ColumnarValue::Array(Arc::new(StringArray::from(values))))
                }
            }
            JsonExtractMode::Variant => {
                let mut values = Vec::with_capacity(len);
                for row in 0..len {
                    let extracted = extract_json_path(&arrays, row)?;
                    values.push(match extracted {
                        None => None,
                        Some(other) => Some(json_variant_value(&other)?),
                    });
                }
                if scalar_inputs {
                    Ok(ColumnarValue::Scalar(ScalarValue::Binary(
                        values.into_iter().next().flatten(),
                    )))
                } else {
                    let refs = values
                        .iter()
                        .map(|value| value.as_deref())
                        .collect::<Vec<_>>();
                    Ok(ColumnarValue::Array(Arc::new(BinaryArray::from(refs))))
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TextCodecMode {
    Decode,
    Encode,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LixTextCodec {
    name: &'static str,
    mode: TextCodecMode,
    signature: Signature,
}

impl LixTextCodec {
    fn new(name: &'static str, mode: TextCodecMode) -> Self {
        Self {
            name,
            mode,
            signature: Signature::one_of(
                vec![Signature::any(1, Volatility::Immutable).type_signature],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for LixTextCodec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(match self.mode {
            TextCodecMode::Decode => DataType::Utf8,
            TextCodecMode::Encode => DataType::Binary,
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !(1..=2).contains(&args.args.len()) {
            return plan_err!("{} requires 1 or 2 arguments", self.name);
        }
        validate_utf8_encoding_arg(self.name, args.args.get(1))?;

        let scalar_inputs = args
            .args
            .iter()
            .all(|value| matches!(value, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let input = &arrays[0];
        let len = input.len();

        match self.mode {
            TextCodecMode::Decode => {
                let mut values = Vec::with_capacity(len);
                for row in 0..len {
                    let decoded = decode_utf8_value(input.as_ref(), row)?;
                    values.push(decoded);
                }
                if scalar_inputs {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                        values.into_iter().next().flatten(),
                    )))
                } else {
                    Ok(ColumnarValue::Array(Arc::new(StringArray::from(values))))
                }
            }
            TextCodecMode::Encode => {
                let mut values = Vec::with_capacity(len);
                for row in 0..len {
                    values.push(encode_utf8_value(input.as_ref(), row)?);
                }
                if scalar_inputs {
                    Ok(ColumnarValue::Scalar(ScalarValue::Binary(
                        values.into_iter().next().flatten(),
                    )))
                } else {
                    let refs = values
                        .iter()
                        .map(|value| value.as_deref())
                        .collect::<Vec<_>>();
                    Ok(ColumnarValue::Array(Arc::new(BinaryArray::from(refs))))
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LixJson;

impl ScalarUDFImpl for LixJson {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "lix_json"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return plan_err!("lix_json requires exactly 1 argument");
        }
        let scalar_inputs = matches!(args.args[0], ColumnarValue::Scalar(_));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let input = &arrays[0];
        let len = input.len();
        let mut values = Vec::with_capacity(len);
        for row in 0..len {
            values.push(json_value(input.as_ref(), row)?);
        }
        if scalar_inputs {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                values.into_iter().next().flatten(),
            )))
        } else {
            Ok(ColumnarValue::Array(Arc::new(StringArray::from(values))))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LixEmptyBlob;

impl ScalarUDFImpl for LixEmptyBlob {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "lix_empty_blob"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::nullary(Volatility::Immutable));
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(Vec::new()))))
    }
}

#[derive(Clone)]
struct LixUuidV7 {
    functions: DynFunctionProvider,
}

impl PartialEq for LixUuidV7 {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for LixUuidV7 {}

impl std::hash::Hash for LixUuidV7 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl std::fmt::Debug for LixUuidV7 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixUuidV7").finish()
    }
}

impl ScalarUDFImpl for LixUuidV7 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "lix_uuid_v7"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::nullary(Volatility::Volatile));
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !args.args.is_empty() {
            return plan_err!("lix_uuid_v7 requires no arguments");
        }
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            self.functions.call_uuid_v7(),
        ))))
    }
}

fn extract_json_path(arrays: &[ArrayRef], row: usize) -> Result<Option<JsonValue>> {
    let Some(mut current) = json_value_to_serde(arrays[0].as_ref(), row)? else {
        return Ok(None);
    };

    for path in &arrays[1..] {
        let Some(segment) = json_path_segment(path.as_ref(), row)? else {
            return Ok(None);
        };
        let next = match segment {
            JsonPathSegment::Key(key) => current.get(&key).cloned(),
            JsonPathSegment::Index(index) => current
                .as_array()
                .and_then(|values| values.get(index))
                .cloned(),
        };
        let Some(value) = next else {
            return Ok(None);
        };
        current = value;
    }

    Ok(Some(current))
}

fn json_value(array: &dyn Array, row: usize) -> Result<Option<String>> {
    let Some(raw) = text_like_value(array, row)? else {
        return Ok(Some("null".to_string()));
    };
    let parsed = serde_json::from_str::<JsonValue>(&raw).map_err(|error| {
        DataFusionError::Execution(format!(
            "lix_json() expected valid JSON text, got error: {error}"
        ))
    })?;
    Ok(Some(serde_json::to_string(&parsed).map_err(|error| {
        DataFusionError::Execution(format!("lix_json() could not render JSON: {error}"))
    })?))
}

fn json_value_to_serde(array: &dyn Array, row: usize) -> Result<Option<JsonValue>> {
    let Some(raw) = text_like_value(array, row)? else {
        return Ok(None);
    };
    serde_json::from_str::<JsonValue>(&raw)
        .map(Some)
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "lix_json_extract() expected valid JSON text in its first argument, got error: {error}"
            ))
        })
}

fn text_like_value(array: &dyn Array, row: usize) -> Result<Option<String>> {
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return Ok((!array.is_null(row)).then(|| array.value(row).to_string()));
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok((!array.is_null(row)).then(|| array.value(row).to_string()));
    }
    if let Some(value) = numeric_value(array, row)? {
        return Ok(Some(value));
    }
    if let Some(array) = array.as_any().downcast_ref::<BooleanArray>() {
        return Ok((!array.is_null(row)).then(|| {
            if array.value(row) {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }));
    }
    if let Some(array) = array.as_any().downcast_ref::<BinaryArray>() {
        return Ok(
            (!array.is_null(row)).then(|| String::from_utf8_lossy(array.value(row)).to_string())
        );
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok(
            (!array.is_null(row)).then(|| String::from_utf8_lossy(array.value(row)).to_string())
        );
    }
    Err(DataFusionError::Execution(format!(
        "unsupported argument type for JSON/text function: {:?}",
        array.data_type()
    )))
}

fn numeric_value(array: &dyn Array, row: usize) -> Result<Option<String>> {
    macro_rules! numeric_array {
        ($ty:ty) => {
            if let Some(array) = array.as_any().downcast_ref::<$ty>() {
                return Ok((!array.is_null(row)).then(|| array.value(row).to_string()));
            }
        };
    }

    numeric_array!(Int8Array);
    numeric_array!(Int16Array);
    numeric_array!(Int32Array);
    numeric_array!(Int64Array);
    numeric_array!(UInt8Array);
    numeric_array!(UInt16Array);
    numeric_array!(UInt32Array);
    numeric_array!(UInt64Array);
    numeric_array!(Float32Array);
    numeric_array!(Float64Array);
    Ok(None)
}

fn decode_utf8_value(array: &dyn Array, row: usize) -> Result<Option<String>> {
    if let Some(array) = array.as_any().downcast_ref::<BinaryArray>() {
        return (!array.is_null(row))
            .then(|| String::from_utf8(array.value(row).to_vec()))
            .transpose()
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "lix_text_decode() expected valid UTF8 bytes: {error}"
                ))
            });
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return (!array.is_null(row))
            .then(|| String::from_utf8(array.value(row).to_vec()))
            .transpose()
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "lix_text_decode() expected valid UTF8 bytes: {error}"
                ))
            });
    }
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return Ok((!array.is_null(row)).then(|| array.value(row).to_string()));
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok((!array.is_null(row)).then(|| array.value(row).to_string()));
    }
    Err(DataFusionError::Execution(format!(
        "lix_text_decode() expected Binary or Utf8, got {:?}",
        array.data_type()
    )))
}

fn encode_utf8_value(array: &dyn Array, row: usize) -> Result<Option<Vec<u8>>> {
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return Ok((!array.is_null(row)).then(|| array.value(row).as_bytes().to_vec()));
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok((!array.is_null(row)).then(|| array.value(row).as_bytes().to_vec()));
    }
    if let Some(array) = array.as_any().downcast_ref::<BinaryArray>() {
        return Ok((!array.is_null(row)).then(|| array.value(row).to_vec()));
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok((!array.is_null(row)).then(|| array.value(row).to_vec()));
    }
    Err(DataFusionError::Execution(format!(
        "lix_text_encode() expected Utf8 or Binary, got {:?}",
        array.data_type()
    )))
}

fn validate_utf8_encoding_arg(fn_name: &str, encoding: Option<&ColumnarValue>) -> Result<()> {
    let Some(encoding) = encoding else {
        return Ok(());
    };
    let arrays = ColumnarValue::values_to_arrays(std::slice::from_ref(encoding))?;
    let array = &arrays[0];
    if array.len() == 0 {
        return Ok(());
    }
    let Some(value) = text_like_value(array.as_ref(), 0)? else {
        return Ok(());
    };
    let normalized = value.trim().to_ascii_uppercase().replace('-', "");
    if normalized == "UTF8" {
        Ok(())
    } else {
        plan_err!("{fn_name}() only supports UTF8 encoding, got '{value}'")
    }
}

fn json_text_value(value: &JsonValue) -> Result<String> {
    match value {
        JsonValue::String(text) => Ok(text.clone()),
        JsonValue::Number(number) => Ok(number.to_string()),
        JsonValue::Bool(boolean) => Ok(if *boolean {
            "true".to_string()
        } else {
            "false".to_string()
        }),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            serde_json::to_string(value).map_err(|error| {
                DataFusionError::Execution(format!(
                    "lix_json_extract() could not render JSON value: {error}"
                ))
            })
        }
        JsonValue::Null => Ok("null".to_string()),
    }
}

fn json_json_value(value: &JsonValue) -> Result<String> {
    serde_json::to_string(value).map_err(|error| {
        DataFusionError::Execution(format!(
            "lix_json_extract_json() could not render JSON value: {error}"
        ))
    })
}

fn json_variant_value(value: &JsonValue) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|error| {
        DataFusionError::Execution(format!(
            "lix_json_extract_variant() could not render JSON value: {error}"
        ))
    })
}

enum JsonPathSegment {
    Key(String),
    Index(usize),
}

fn json_path_segment(array: &dyn Array, row: usize) -> Result<Option<JsonPathSegment>> {
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(
            (!array.is_null(row)).then(|| JsonPathSegment::Key(array.value(row).to_string()))
        );
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(
            (!array.is_null(row)).then(|| JsonPathSegment::Key(array.value(row).to_string()))
        );
    }
    macro_rules! index_array {
        ($ty:ty) => {
            if let Some(array) = array.as_any().downcast_ref::<$ty>() {
                if array.is_null(row) {
                    return Ok(None);
                }
                let value = array.value(row);
                let index = usize::try_from(value).map_err(|_| {
                    DataFusionError::Execution(
                        "lix_json_extract() path indexes must be non-negative integers".to_string(),
                    )
                })?;
                return Ok(Some(JsonPathSegment::Index(index)));
            }
        };
    }
    index_array!(UInt8Array);
    index_array!(UInt16Array);
    index_array!(UInt32Array);
    index_array!(UInt64Array);
    index_array!(Int8Array);
    index_array!(Int16Array);
    index_array!(Int32Array);
    index_array!(Int64Array);
    Err(DataFusionError::Execution(format!(
        "lix_json_extract() path arguments must be strings or non-negative integers, got {:?}",
        array.data_type()
    )))
}
