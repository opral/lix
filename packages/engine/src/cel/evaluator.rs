use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use cel::Program;
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::functions::{LixFunctionProvider, SharedFunctionProvider, SystemFunctionProvider};
use crate::LixError;

use super::context::build_context_with_functions;
use super::value::cel_to_json;

#[derive(Debug)]
struct CompiledProgram {
    program: Program,
}

#[derive(Default)]
pub struct CelEvaluator {
    programs: RwLock<HashMap<String, Arc<CompiledProgram>>>,
}

impl CelEvaluator {
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(dead_code)]
    pub fn evaluate(
        &self,
        expression: &str,
        variables: &JsonMap<String, JsonValue>,
    ) -> Result<JsonValue, LixError> {
        let functions = SharedFunctionProvider::new(SystemFunctionProvider);
        self.evaluate_with_functions(expression, variables, functions)
    }

    pub fn evaluate_with_functions<P>(
        &self,
        expression: &str,
        variables: &JsonMap<String, JsonValue>,
        functions: SharedFunctionProvider<P>,
    ) -> Result<JsonValue, LixError>
    where
        P: LixFunctionProvider + Send + 'static,
    {
        let compiled = self.compile(expression)?;
        let context = build_context_with_functions(variables, functions)?;
        let value = compiled.program.execute(&context).map_err(|err| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!("failed to evaluate CEL expression '{expression}': {err}"),
        })?;
        cel_to_json(&value)
    }

    fn compile(&self, expression: &str) -> Result<Arc<CompiledProgram>, LixError> {
        if let Some(existing) = self.programs.read().unwrap().get(expression).cloned() {
            return Ok(existing);
        }

        let program = Program::compile(expression).map_err(|err| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!("failed to parse CEL expression '{expression}': {err}"),
        })?;
        let compiled = Arc::new(CompiledProgram { program });

        self.programs
            .write()
            .unwrap()
            .insert(expression.to_string(), compiled.clone());

        Ok(compiled)
    }
}

#[cfg(test)]
mod tests {
    use super::CelEvaluator;
    use serde_json::{json, Map as JsonMap, Value as JsonValue};

    #[test]
    fn evaluates_basic_expressions() {
        let evaluator = CelEvaluator::new();
        let value = evaluator
            .evaluate("'open'", &JsonMap::new())
            .expect("evaluate CEL");
        assert_eq!(value, JsonValue::String("open".to_string()));
    }

    #[test]
    fn evaluates_with_variables() {
        let evaluator = CelEvaluator::new();
        let mut context = JsonMap::new();
        context.insert("name".to_string(), JsonValue::String("sample".to_string()));
        let value = evaluator
            .evaluate("name + '-slug'", &context)
            .expect("evaluate CEL");
        assert_eq!(value, JsonValue::String("sample-slug".to_string()));
    }

    #[test]
    fn reports_parse_errors() {
        let evaluator = CelEvaluator::new();
        let err = evaluator
            .evaluate("lix_uuid_v7(", &JsonMap::new())
            .expect_err("expected parse error");
        assert!(err.to_string().contains("failed to parse CEL expression"));
    }

    #[test]
    fn reports_runtime_errors() {
        let evaluator = CelEvaluator::new();
        let err = evaluator
            .evaluate("1 / 0", &JsonMap::new())
            .expect_err("expected runtime error");
        assert!(err
            .to_string()
            .contains("failed to evaluate CEL expression"));
    }

    #[test]
    fn supports_function_calls() {
        let evaluator = CelEvaluator::new();
        let value = evaluator
            .evaluate("lix_timestamp()", &JsonMap::new())
            .expect("evaluate CEL");
        let as_text = value.as_str().expect("timestamp as string");
        assert!(as_text.contains('T'));
    }

    #[test]
    fn caches_compiled_programs() {
        let evaluator = CelEvaluator::new();
        let mut context = JsonMap::new();
        context.insert("name".to_string(), json!("x"));

        let _ = evaluator
            .evaluate("name + '-slug'", &context)
            .expect("first evaluation");
        let _ = evaluator
            .evaluate("name + '-slug'", &context)
            .expect("second evaluation");

        let size = evaluator.programs.read().unwrap().len();
        assert_eq!(size, 1);
    }

    #[test]
    fn errors_on_unknown_variable() {
        let evaluator = CelEvaluator::new();
        let err = evaluator
            .evaluate("missing_var + '-slug'", &JsonMap::new())
            .expect_err("expected unknown variable error");
        assert!(err.to_string().contains("Undeclared reference"));
    }
}
