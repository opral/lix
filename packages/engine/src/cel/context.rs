use cel::Context;
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::functions::timestamp::timestamp;
use crate::functions::uuid_v7::uuid_v7;
use crate::LixError;

use super::value::json_to_cel;

pub fn build_context(variables: &JsonMap<String, JsonValue>) -> Result<Context<'static>, LixError> {
    let mut context = Context::default();

    context.add_function("lix_uuid_v7", || uuid_v7());
    context.add_function("lix_timestamp", || timestamp());

    for (name, value) in variables {
        let cel_value = json_to_cel(value)?;
        context.add_variable_from_value(name.clone(), cel_value);
    }

    Ok(context)
}

#[cfg(test)]
mod tests {
    use super::build_context;
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
}
