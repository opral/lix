use cel::Context;
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::LixError;

use super::provider::CelFunctionProvider;
use super::value::json_to_cel;

pub(crate) fn build_context_with_functions<P>(
    variables: &JsonMap<String, JsonValue>,
    functions: P,
) -> Result<Context<'static>, LixError>
where
    P: CelFunctionProvider,
{
    let mut context = Context::default();

    let uuid_functions = functions.clone();
    context.add_function("lix_uuid_v7", move || {
        uuid_functions.call_uuid_v7().to_string()
    });
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

#[cfg(test)]
mod tests {
    use super::build_context_with_functions;
    use crate::cel::CelFunctionProvider;
    use cel::Program;
    use serde_json::Map as JsonMap;

    #[test]
    fn registers_lix_uuid_v7_function() {
        let context = build_context_with_functions(&JsonMap::new(), fixed_functions())
            .expect("build context");
        let program = Program::compile("lix_uuid_v7()").expect("compile CEL");
        let value = program.execute(&context).expect("execute CEL");
        let as_json = value.json().expect("to json");
        assert!(as_json.as_str().is_some());
    }

    #[test]
    fn errors_on_unknown_variables() {
        let context = build_context_with_functions(&JsonMap::new(), fixed_functions())
            .expect("build context");
        let program = Program::compile("missing_var == null").expect("compile CEL");
        let err = program
            .execute(&context)
            .expect_err("execute CEL should fail");
        assert!(err.to_string().contains("Undeclared reference"));
    }

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
    fn uses_supplied_function_provider() {
        let context = build_context_with_functions(&JsonMap::new(), fixed_functions())
            .expect("build context");
        let program = Program::compile("lix_uuid_v7()").expect("compile CEL");
        let value = program.execute(&context).expect("execute CEL");
        assert_eq!(
            value.json().expect("to json").as_str(),
            Some("00000000-0000-0000-0000-000000000000")
        );
    }
}
