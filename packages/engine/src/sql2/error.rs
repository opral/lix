use datafusion::error::DataFusionError;

use crate::LixError;

pub(crate) fn datafusion_error_to_lix_error(error: DataFusionError) -> LixError {
    if let Some(error) = lix_error_from_datafusion_error(&error) {
        return error;
    }

    classify_datafusion_error(&error)
}

pub(crate) fn lix_error_to_datafusion_error(error: LixError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

fn lix_error_from_datafusion_error(error: &DataFusionError) -> Option<LixError> {
    match error {
        DataFusionError::External(error) => error
            .downcast_ref::<LixError>()
            .cloned()
            .map(normalize_external_sql_error),
        DataFusionError::Context(_, error) | DataFusionError::Diagnostic(_, error) => {
            lix_error_from_datafusion_error(error)
        }
        DataFusionError::Shared(error) => lix_error_from_datafusion_error(error),
        DataFusionError::Collection(errors) => {
            errors.iter().find_map(lix_error_from_datafusion_error)
        }
        _ => None,
    }
}

fn normalize_external_sql_error(error: LixError) -> LixError {
    let lower = error.message.to_ascii_lowercase();
    if error.code.starts_with("LIX_ERROR_PATH_")
        || error.code == LixError::CODE_INVALID_JSON_PATH
        || (error.code == LixError::CODE_TYPE_MISMATCH
            && lower.contains("cannot store blob values directly"))
        || (error.code == LixError::CODE_SCHEMA_DEFINITION && lower.contains("system schema"))
    {
        return LixError {
            code: LixError::CODE_INVALID_PARAM.to_string(),
            ..error
        };
    }
    error
}

fn classify_datafusion_error(error: &DataFusionError) -> LixError {
    let message = format!("sql2 DataFusion error: {error}");
    let lower = message.to_ascii_lowercase();

    if looks_like_json_udf_miss(&lower) {
        return LixError::new(LixError::CODE_UDF_NOT_FOUND, message)
            .with_hint("Use lix_json_get(json, key_or_index, ...) for JSON values or lix_json_get_text(json, key_or_index, ...) for text.");
    }

    if looks_like_unsupported_dialect(&lower) {
        return LixError::new(LixError::CODE_DIALECT_UNSUPPORTED, message)
            .with_hint("Lix SQL uses DataFusion syntax. Use lix_json_get(...) or lix_json_get_text(...) for JSON access, and placeholders like ?, ? or $1, $2, ...");
    }

    if looks_like_unsupported_runtime_plan(&lower) {
        return LixError::new(LixError::CODE_UNSUPPORTED_SQL_RUNTIME_PLAN, message)
            .with_hint("This SQL feature currently plans to a physical operator that is not supported by this engine runtime. Rewrite the query to avoid the unsupported operator, or run it on a runtime that supports the full physical plan.");
    }

    if lower.contains("uses variadic path segments") {
        return LixError::new(LixError::CODE_INVALID_JSON_PATH, message)
            .with_hint("Pass path segments as separate arguments, for example lix_json_get_text(document, 'user', 'name'), not '$.user.name' or '/user/name'.");
    }

    if lower.contains("failed to parse placeholder id")
        || lower.contains("placeholder")
        || lower.contains("bind")
    {
        return LixError::new(LixError::CODE_PARSE_ERROR, message)
            .with_hint("Use placeholders like ?, ? or numbered placeholders like $1, $2, ...");
    }

    if lower.contains("table not found")
        || (lower.contains("table") && lower.contains("not found"))
        || lower.contains("no table named")
        || lower.contains("failed to resolve table")
        || lower.contains("could not find table")
        || (lower.contains("relation") && lower.contains("not found"))
    {
        return LixError::new(LixError::CODE_TABLE_NOT_FOUND, message)
            .with_hint("Use information_schema.tables to inspect available Lix SQL tables.");
    }

    if (lower.contains("column") || lower.contains("field"))
        && (lower.contains("not found")
            || lower.contains("does not exist")
            || lower.contains("no field named"))
    {
        return LixError::new(LixError::CODE_COLUMN_NOT_FOUND, message);
    }

    if lower.contains("schema validation") {
        return LixError::new(LixError::CODE_SCHEMA_VALIDATION, message);
    }

    if lower.contains("schema definition") {
        return LixError::new(LixError::CODE_SCHEMA_DEFINITION, message);
    }

    if lower.contains("unsupported sql type json") {
        return LixError::new(LixError::CODE_DIALECT_UNSUPPORTED, message)
            .with_hint("Declare JSON/object columns through lix.registerSchema(...) or lix_schema_definition; SQL type JSON is not supported.");
    }

    if looks_like_type_mismatch(&lower) {
        if lower.contains("encountered non utf-8 data") {
            return LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                "Lix SQL string functions require valid UTF-8 text; blob data could not be decoded as UTF-8",
            )
            .with_hint(
                "Pass text to string functions. Raw blob parameters stay binary and are not implicitly decoded as UTF-8.",
            );
        }
        return LixError::new(LixError::CODE_TYPE_MISMATCH, message)
            .with_hint("Check the SQL function argument types. JSON text can be converted with lix_json(...); JSON fields can be read with lix_json_get(...) or lix_json_get_text(...).");
    }

    if matches!(
        error,
        DataFusionError::Plan(_) | DataFusionError::SchemaError(_, _)
    ) {
        return LixError::new(LixError::CODE_PARSE_ERROR, message);
    }

    if lower.contains("constraint")
        || lower.contains("not null")
        || lower.contains("non-nullable")
        || lower.contains("unique")
        || lower.contains("duplicate")
        || lower.contains("primary key")
        || lower.contains("foreign key")
    {
        return LixError::new(LixError::CODE_CONSTRAINT_VIOLATION, message);
    }

    match error {
        DataFusionError::SQL(_, _) => LixError::new(LixError::CODE_PARSE_ERROR, message),
        DataFusionError::NotImplemented(_) => {
            LixError::new(LixError::CODE_DIALECT_UNSUPPORTED, message)
        }
        DataFusionError::Plan(_) | DataFusionError::SchemaError(_, _) => {
            LixError::new(LixError::CODE_PARSE_ERROR, message)
        }
        DataFusionError::IoError(_) | DataFusionError::ObjectStore(_) => {
            LixError::new(LixError::CODE_STORAGE_ERROR, message)
        }
        DataFusionError::Internal(_) => LixError::new(LixError::CODE_INTERNAL_ERROR, message),
        _ => LixError::new(LixError::CODE_UNKNOWN, message),
    }
}

fn looks_like_json_udf_miss(lower: &str) -> bool {
    let json_function_guess = [
        "json_extract",
        "json_get",
        "json_get_string",
        "json_get_text",
        "json_extract_string",
        "json_extract_text",
    ]
    .iter()
    .any(|name| lower.contains(name));

    json_function_guess
        && (lower.contains("function")
            || lower.contains("udf")
            || lower.contains("not found")
            || lower.contains("does not exist")
            || lower.contains("did you mean"))
}

fn looks_like_unsupported_dialect(lower: &str) -> bool {
    lower.contains("->>")
        || lower.contains("operator does not exist")
        || lower.contains("unsupported sql type json")
        || lower.contains("sqlite_master")
        || lower.contains("returning")
}

fn looks_like_unsupported_runtime_plan(lower: &str) -> bool {
    lower.contains("sql physical operator")
        && lower.contains("is not supported by the webassembly runtime yet")
}

fn looks_like_type_mismatch(lower: &str) -> bool {
    (lower.contains("type")
        || lower.contains("signature")
        || lower.contains("coerc")
        || lower.contains("argument")
        || lower.contains("convert"))
        && (lower.contains("mismatch")
            || lower.contains("incompatible")
            || lower.contains("expected")
            || lower.contains("cannot")
            || lower.contains("invalid"))
}
