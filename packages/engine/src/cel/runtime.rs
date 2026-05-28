use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use cel::Program;
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::LixError;

use super::context::build_context_with_functions;
use super::error::{cel_parse_error, cel_runtime_error};
use super::provider::CelFunctionProvider;
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

    pub fn evaluate_with_functions<P>(
        &self,
        expression: &str,
        variables: &JsonMap<String, JsonValue>,
        functions: P,
    ) -> Result<JsonValue, LixError>
    where
        P: CelFunctionProvider,
    {
        let compiled = self.compile(expression)?;
        let context = build_context_with_functions(variables, functions)?;
        let value = compiled
            .program
            .execute(&context)
            .map_err(|error| cel_runtime_error(expression, error))?;
        cel_to_json(&value)
    }

    fn compile(&self, expression: &str) -> Result<Arc<CompiledProgram>, LixError> {
        if let Some(existing) = self.programs.read().unwrap().get(expression).cloned() {
            return Ok(existing);
        }

        let program =
            Program::compile(expression).map_err(|error| cel_parse_error(expression, error))?;
        let compiled = Arc::new(CompiledProgram { program });

        self.programs
            .write()
            .unwrap()
            .insert(expression.to_string(), compiled.clone());

        Ok(compiled)
    }
}

pub(crate) fn shared_runtime() -> &'static CelEvaluator {
    static SHARED_RUNTIME: OnceLock<CelEvaluator> = OnceLock::new();
    SHARED_RUNTIME.get_or_init(CelEvaluator::new)
}

#[cfg(test)]
mod tests {
    use super::CelEvaluator;
    use crate::cel::CelFunctionProvider;
    use serde_json::{json, Map as JsonMap, Value as JsonValue};

    #[derive(Clone)]
    struct FixedFunctions;

    impl CelFunctionProvider for FixedFunctions {
        fn call_uuid_v7(&self) -> uuid::Uuid {
            uuid::Uuid::nil()
        }

        fn call_timestamp(&self) -> String {
            "1970-01-01T00:00:00.000Z".to_string()
        }
    }

    fn fixed_functions() -> FixedFunctions {
        FixedFunctions
    }

    #[test]
    fn evaluates_basic_expressions() {
        let evaluator = CelEvaluator::new();
        let value = evaluator
            .evaluate_with_functions("'open'", &JsonMap::new(), fixed_functions())
            .expect("evaluate CEL");
        assert_eq!(value, JsonValue::String("open".to_string()));
    }

    #[test]
    fn evaluates_with_variables() {
        let evaluator = CelEvaluator::new();
        let mut context = JsonMap::new();
        context.insert("name".to_string(), JsonValue::String("sample".to_string()));
        let value = evaluator
            .evaluate_with_functions("name + '-slug'", &context, fixed_functions())
            .expect("evaluate CEL");
        assert_eq!(value, JsonValue::String("sample-slug".to_string()));
    }

    #[test]
    fn reports_parse_errors() {
        let evaluator = CelEvaluator::new();
        let err = evaluator
            .evaluate_with_functions("lix_uuid_v7(", &JsonMap::new(), fixed_functions())
            .expect_err("expected parse error");
        assert!(err.to_string().contains("failed to parse CEL expression"));
    }

    #[test]
    fn reports_runtime_errors() {
        let evaluator = CelEvaluator::new();
        let err = evaluator
            .evaluate_with_functions("1 / 0", &JsonMap::new(), fixed_functions())
            .expect_err("expected runtime error");
        assert!(err
            .to_string()
            .contains("failed to evaluate CEL expression"));
    }

    #[test]
    fn supports_function_calls() {
        let evaluator = CelEvaluator::new();
        let value = evaluator
            .evaluate_with_functions("lix_timestamp()", &JsonMap::new(), fixed_functions())
            .expect("evaluate CEL");
        assert_eq!(value.as_str(), Some("1970-01-01T00:00:00.000Z"));
    }

    #[test]
    fn caches_compiled_programs() {
        let evaluator = CelEvaluator::new();
        let mut context = JsonMap::new();
        context.insert("name".to_string(), json!("x"));

        let _ = evaluator
            .evaluate_with_functions("name + '-slug'", &context, fixed_functions())
            .expect("first evaluation");
        let _ = evaluator
            .evaluate_with_functions("name + '-slug'", &context, fixed_functions())
            .expect("second evaluation");

        let size = evaluator.programs.read().unwrap().len();
        assert_eq!(size, 1);
    }

    #[test]
    fn errors_on_unknown_variable() {
        let evaluator = CelEvaluator::new();
        let err = evaluator
            .evaluate_with_functions("missing_var + '-slug'", &JsonMap::new(), fixed_functions())
            .expect_err("expected unknown variable error");
        assert!(err.to_string().contains("Undeclared reference"));
    }
}
