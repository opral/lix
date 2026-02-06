use cel::Context;
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::functions::{LixFunctionProvider, SharedFunctionProvider, SystemFunctionProvider};
use crate::LixError;

use super::value::json_to_cel;

pub fn build_context_with_functions<P>(
    variables: &JsonMap<String, JsonValue>,
    functions: SharedFunctionProvider<P>,
) -> Result<Context<'static>, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let mut context = Context::default();

    let uuid_functions = functions.clone();
    context.add_function("lix_uuid_v7", move || uuid_functions.call_uuid_v7());
    let timestamp_functions = functions.clone();
    context.add_function("lix_timestamp", move || {
        timestamp_functions.call_timestamp()
    });

    for (name, value) in variables {
        let cel_value = json_to_cel(value)?;
        context.add_variable_from_value(name.clone(), cel_value);
    }

    Ok(context)
}

#[allow(dead_code)]
pub fn build_context(variables: &JsonMap<String, JsonValue>) -> Result<Context<'static>, LixError> {
    let functions = SharedFunctionProvider::new(SystemFunctionProvider);
    build_context_with_functions(variables, functions)
}

#[cfg(test)]
mod tests {
    use super::{build_context, build_context_with_functions};
    use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
    use cel::Program;
    use serde_json::Map as JsonMap;

    #[test]
    fn registers_lix_uuid_v7_function() {
        let context = build_context(&JsonMap::new()).expect("build context");
        let program = Program::compile("lix_uuid_v7()").expect("compile CEL");
        let value = program.execute(&context).expect("execute CEL");
        let as_json = value.json().expect("to json");
        assert!(as_json.as_str().is_some());
    }

    #[test]
    fn errors_on_unknown_variables() {
        let context = build_context(&JsonMap::new()).expect("build context");
        let program = Program::compile("missing_var == null").expect("compile CEL");
        let err = program
            .execute(&context)
            .expect_err("execute CEL should fail");
        assert!(err.to_string().contains("Undeclared reference"));
    }

    struct FixedFunctions;

    impl LixFunctionProvider for FixedFunctions {
        fn uuid_v7(&mut self) -> String {
            "uuid-fixed".to_string()
        }

        fn timestamp(&mut self) -> String {
            "1970-01-01T00:00:00.000Z".to_string()
        }
    }

    #[test]
    fn uses_supplied_function_provider() {
        let functions = SharedFunctionProvider::new(FixedFunctions);
        let context =
            build_context_with_functions(&JsonMap::new(), functions).expect("build context");
        let program = Program::compile("lix_uuid_v7()").expect("compile CEL");
        let value = program.execute(&context).expect("execute CEL");
        assert_eq!(value.json().expect("to json").as_str(), Some("uuid-fixed"));
    }
}
