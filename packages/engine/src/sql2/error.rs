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
        DataFusionError::External(error) => error.downcast_ref::<LixError>().cloned(),
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

fn classify_datafusion_error(error: &DataFusionError) -> LixError {
    let description = format!("sql2 DataFusion error: {error}");
    let lower = description.to_ascii_lowercase();

    if lower.contains("failed to parse placeholder id")
        || lower.contains("placeholder")
        || lower.contains("bind")
    {
        return LixError::new(LixError::CODE_BINDING_ERROR, description).with_hint(
            "Use numbered placeholders like $1, $2, ...; '?' placeholders are not supported.",
        );
    }

    if lower.contains("requires start_commit_id")
        || lower.contains("history filter")
        || lower.contains("history table")
    {
        return LixError::new(LixError::CODE_HISTORY_FILTER_REQUIRED, description)
            .with_hint("Add a commit/version range predicate before querying history tables.");
    }

    if lower.contains("table not found")
        || (lower.contains("table") && lower.contains("not found"))
        || lower.contains("no table named")
        || lower.contains("failed to resolve table")
        || lower.contains("could not find table")
        || (lower.contains("relation") && lower.contains("not found"))
    {
        return LixError::new(LixError::CODE_TABLE_NOT_FOUND, description)
            .with_hint("Use information_schema.tables to inspect available Lix SQL tables.");
    }

    if (lower.contains("column") || lower.contains("field"))
        && (lower.contains("not found")
            || lower.contains("does not exist")
            || lower.contains("no field named"))
    {
        return LixError::new(LixError::CODE_COLUMN_NOT_FOUND, description);
    }

    if lower.contains("schema validation") {
        return LixError::new(LixError::CODE_SCHEMA_VALIDATION, description);
    }

    if lower.contains("schema definition") {
        return LixError::new(LixError::CODE_SCHEMA_DEFINITION, description);
    }

    if lower.contains("constraint")
        || lower.contains("not null")
        || lower.contains("non-nullable")
        || lower.contains("unique")
        || lower.contains("duplicate")
        || lower.contains("primary key")
        || lower.contains("foreign key")
    {
        return LixError::new(LixError::CODE_CONSTRAINT_VIOLATION, description);
    }

    if lower.contains("unsupported sql type json") {
        return LixError::new(LixError::CODE_UNSUPPORTED_SQL, description)
            .with_hint("Declare JSON/object columns through lix.registerSchema(...) or lix_registered_schema; SQL type JSON is not supported.");
    }

    match error {
        DataFusionError::SQL(_, _) => LixError::new(LixError::CODE_PARSE_ERROR, description),
        DataFusionError::NotImplemented(_) => {
            LixError::new(LixError::CODE_UNSUPPORTED_SQL, description)
        }
        DataFusionError::Plan(_) | DataFusionError::SchemaError(_, _) => {
            LixError::new(LixError::CODE_PARSE_ERROR, description)
        }
        DataFusionError::IoError(_) | DataFusionError::ObjectStore(_) => {
            LixError::new(LixError::CODE_STORAGE_ERROR, description)
        }
        DataFusionError::Internal(_) => LixError::new(LixError::CODE_INTERNAL_ERROR, description),
        _ => LixError::new(LixError::CODE_UNKNOWN, description),
    }
}
