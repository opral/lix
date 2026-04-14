use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::functions::DynFunctionProvider;
use crate::LixError;

pub(crate) mod defaults;
pub(crate) mod overrides;

pub(crate) trait SchemaAnnotationEvaluator {
    fn evaluate_schema_annotation_expression(
        &self,
        expression: &str,
        variables: &JsonMap<String, JsonValue>,
        functions: &DynFunctionProvider,
    ) -> Result<JsonValue, LixError>;
}
