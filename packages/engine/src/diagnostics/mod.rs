use std::ops::ControlFlow;

use crate::catalog::SurfaceRegistry;
use crate::catalog::{builtin_public_surface_columns, builtin_public_surface_names};
use crate::contracts::{ReadDiagnosticCatalogSnapshot, ReadDiagnosticContext};
use crate::LixBackend;
use crate::LixError;
use sqlparser::ast::{visit_relations, ObjectNamePart, Statement};

fn build_error(code: &str, description: impl Into<String>) -> LixError {
    LixError::new(code, description.into())
}

pub(crate) fn table_not_found_read_error() -> LixError {
    let available_tables = crate::sql::protected_builtin_public_surface_names().join(", ");
    build_error(
        "LIX_ERROR_TABLE_NOT_FOUND",
        format!(
            "Table not found. Known Lix tables: {available_tables}. If you are querying custom backend tables, ensure they exist in the connected database."
        ),
    )
}

pub(crate) fn schema_not_registered_error(
    schema_key: &str,
    available_schema_keys: &[&str],
) -> LixError {
    let available = if available_schema_keys.is_empty() {
        "Available schemas: (none).".to_string()
    } else {
        format!("Available schemas: {}.", available_schema_keys.join(", "))
    };
    build_error(
        "LIX_ERROR_SCHEMA_NOT_REGISTERED",
        format!(
            "Schema `{schema_key}` is not registered. Register or install the schema before querying it. {available} Inspect registered schemas via `SELECT * FROM lix_registered_schema`."
        ),
    )
}

pub(crate) fn sql_unknown_table_error(
    table_name: &str,
    available_tables: &[&str],
    offset: Option<usize>,
) -> LixError {
    let available_tables = if available_tables.is_empty() {
        "Available tables: (none).".to_string()
    } else {
        format!("Available tables: {}.", available_tables.join(", "))
    };
    let location = offset
        .map(|value| format!(" Location: SQL offset {value}."))
        .unwrap_or_default();
    build_error(
        "LIX_ERROR_SQL_UNKNOWN_TABLE",
        format!("Table `{table_name}` does not exist. {available_tables}{location}"),
    )
}

pub(crate) fn sql_unknown_column_error(
    column_name: &str,
    table_name: Option<&str>,
    available_columns: &[&str],
    offset: Option<usize>,
) -> LixError {
    let table_segment = table_name
        .map(|table| format!(" on `{table}`"))
        .unwrap_or_else(|| " in this query".to_string());
    let available_columns = if available_columns.is_empty() {
        "Available columns: (unknown).".to_string()
    } else {
        format!("Available columns: {}.", available_columns.join(", "))
    };
    let location = offset
        .map(|value| format!(" Location: SQL offset {value}."))
        .unwrap_or_default();
    build_error(
        "LIX_ERROR_SQL_UNKNOWN_COLUMN",
        format!(
            "Column `{column_name}` does not exist{table_segment}. {available_columns}{location}"
        ),
    )
}

pub(crate) fn internal_table_access_denied_error() -> LixError {
    let inventory = crate::sql::builtin_relation_inventory();
    let available_tables = inventory.protected_builtin_public_surfaces.join(", ");
    let internal_storage_namespaces = inventory
        .internal_relation_families
        .iter()
        .map(|family| format!("`{}*`", family.prefix))
        .collect::<Vec<_>>()
        .join(", ");
    build_error(
        "LIX_ERROR_INTERNAL_TABLE_ACCESS_DENIED",
        format!(
            "Direct writes against internal storage relations can lead to data corruption. {policy_choice} Protected internal storage includes exact built-in tables plus managed relation families such as {internal_storage_namespaces}. DDL against internal storage and protected Lix system relations is also denied. Public SQL tables remain writable, including `lix_state` and `lix_state_by_version`. Public SQL tables: {available_tables}.",
            policy_choice = crate::sql::relation_policy_choice_summary(),
        ),
    )
}

pub(crate) fn public_create_table_denied_error() -> LixError {
    build_error(
        "LIX_ERROR_PUBLIC_CREATE_TABLE_DENIED",
        "CREATE TABLE is not supported in public Lix SQL. Instead, store a schema definition in `lix_registered_schema`; registered schemas become queryable entity views.",
    )
}

pub(crate) fn mixed_public_internal_query_error(internal_tables: &[String]) -> LixError {
    let available_tables = crate::sql::protected_builtin_public_surface_names().join(", ");
    let internal_tables = if internal_tables.is_empty() {
        "`lix_internal_*`".to_string()
    } else {
        internal_tables
            .iter()
            .map(|table| format!("`{table}`"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    build_error(
        "LIX_ERROR_INTERNAL_TABLE_ACCESS_DENIED",
        format!(
            "Queries that reference public Lix tables must not also access internal engine tables in the same statement. Internal tables referenced: {internal_tables}. Public SQL tables: {available_tables}."
        ),
    )
}

pub(crate) fn read_only_view_write_error(view_name: &str, operation: &str) -> LixError {
    let guidance = if let Some(base_view) = view_name.strip_suffix("_history") {
        format!("Use `{base_view}` or `{base_view}_by_version` for writes.")
    } else {
        "Use the corresponding writable view for writes.".to_string()
    };
    build_error(
        "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED",
        format!("`{view_name}` is read-only. `{operation}` is not supported. {guidance}"),
    )
}

pub(crate) fn transaction_control_statement_denied_error() -> LixError {
    build_error(
        "LIX_ERROR_TRANSACTION_CONTROL_STATEMENT_DENIED",
        "Standalone transaction control is not supported via execute(). Use a single BEGIN ... COMMIT script or the typed transaction API.",
    )
}

pub(crate) const FILE_DATA_EXPECTS_BYTES_MESSAGE: &str = "data expects bytes. To insert text use lix_text_encode(): INSERT INTO lix_file (path, data) VALUES ('file.txt', lix_text_encode('your text'))";

pub(crate) fn file_data_expects_bytes_error() -> LixError {
    build_error(
        "LIX_ERROR_FILE_DATA_EXPECTS_BYTES",
        FILE_DATA_EXPECTS_BYTES_MESSAGE,
    )
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn normalize_sql_error(error: LixError, statements: &[Statement]) -> LixError {
    if let Some(missing_column) = parse_unknown_column_name(&error.description) {
        let relation_names = relation_names_from_statements(statements);
        let table_name = choose_table_for_unknown_column(&missing_column, &relation_names);
        let available_columns = table_name
            .as_deref()
            .and_then(builtin_public_surface_columns)
            .unwrap_or_default();
        let available_column_refs = available_columns
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        return sql_unknown_column_error(
            &missing_column,
            table_name.as_deref(),
            available_column_refs.as_slice(),
            parse_sql_offset(&error.description),
        );
    }

    if is_missing_relation_error(&error) {
        if let Some(table_name) = parse_unknown_table_name(&error.description).or_else(|| {
            relation_names_from_statements(statements)
                .into_iter()
                .next()
        }) {
            let available_tables = builtin_public_surface_names();
            let available_table_refs = available_tables
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            return sql_unknown_table_error(
                &table_name,
                available_table_refs.as_slice(),
                parse_sql_offset(&error.description),
            );
        }
        return table_not_found_read_error();
    }

    let public_surfaces = builtin_public_surfaces_in_statements(statements);
    if !public_surfaces.is_empty() {
        let sanitized =
            sanitize_lowered_public_sql_error_description(&error.description, &public_surfaces);
        if sanitized != error.description {
            return LixError::new(&error.code, sanitized);
        }
    }

    error
}

pub(crate) async fn normalize_sql_error_with_backend_and_relation_names(
    backend: &dyn LixBackend,
    error: LixError,
    relation_names: &[String],
) -> LixError {
    if let Some(missing_column) = parse_unknown_column_name(&error.description) {
        let table_name = choose_table_for_unknown_column(&missing_column, relation_names);
        let available_columns = if let Some(table_name) = table_name.as_deref() {
            resolve_available_columns(table_name, Some(backend)).await
        } else {
            Vec::new()
        };
        let available_column_refs = available_columns
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        return sql_unknown_column_error(
            &missing_column,
            table_name.as_deref(),
            available_column_refs.as_slice(),
            parse_sql_offset(&error.description),
        );
    }

    if is_missing_relation_error(&error) {
        if let Some(table_name) =
            parse_unknown_table_name(&error.description).or_else(|| relation_names.first().cloned())
        {
            let available_tables = resolve_available_tables(backend).await;
            let available_table_refs = available_tables
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            return sql_unknown_table_error(
                &table_name,
                available_table_refs.as_slice(),
                parse_sql_offset(&error.description),
            );
        }
        return table_not_found_read_error();
    }

    let public_surfaces =
        public_surfaces_in_relation_names_with_backend(backend, relation_names, None).await;
    if !public_surfaces.is_empty() {
        let sanitized =
            sanitize_lowered_public_sql_error_description(&error.description, &public_surfaces);
        if sanitized != error.description {
            return LixError::new(&error.code, sanitized);
        }
    }

    error
}

pub(crate) fn normalize_sql_error_with_read_diagnostic_context(
    error: LixError,
    diagnostic_context: &ReadDiagnosticContext,
) -> LixError {
    normalize_sql_error_with_relation_names_and_catalog_snapshot(
        error,
        &diagnostic_context.relation_names,
        &diagnostic_context.catalog_snapshot,
    )
}

pub(crate) fn build_read_diagnostic_catalog_snapshot(
    surface_registry: &SurfaceRegistry,
    relation_names: &[String],
) -> ReadDiagnosticCatalogSnapshot {
    let mut public_surfaces: Vec<String> = Vec::new();
    let mut available_columns_by_relation = std::collections::BTreeMap::new();

    for relation_name in relation_names {
        let columns = builtin_public_surface_columns(relation_name)
            .or_else(|| surface_registry.public_surface_columns(relation_name));
        let Some(columns) = columns else {
            continue;
        };

        if !public_surfaces
            .iter()
            .any(|surface| surface.eq_ignore_ascii_case(relation_name))
        {
            public_surfaces.push(relation_name.clone());
        }
        available_columns_by_relation
            .entry(relation_name.clone())
            .or_insert(columns);
    }

    let mut available_tables = builtin_public_surface_names();
    available_tables.extend(surface_registry.public_surface_names());
    available_tables.sort();
    available_tables.dedup();

    ReadDiagnosticCatalogSnapshot {
        public_surfaces,
        available_tables,
        available_columns_by_relation,
    }
}

pub(crate) fn sanitize_lowered_public_sql_error_description(
    description: &str,
    public_surfaces: &[String],
) -> String {
    if let Some(ambiguous) = extract_backend_error_summary(description, "ambiguous column name:") {
        return ambiguous;
    }
    if let Some(missing) = extract_backend_error_summary(description, "no such column:") {
        return missing;
    }
    if let Some(missing_relation) = extract_backend_error_summary(description, "no such table:") {
        return missing_relation;
    }
    if lowered_public_sql_artifact_leaked(description) {
        let surface_summary = if public_surfaces.is_empty() {
            "public read".to_string()
        } else {
            format!("public read on {}", public_surfaces.join(", "))
        };
        return format!("{surface_summary} execution failed after lowering");
    }
    description.to_string()
}

async fn resolve_available_columns(
    table_name: &str,
    backend: Option<&dyn LixBackend>,
) -> Vec<String> {
    if let Some(columns) = builtin_public_surface_columns(table_name) {
        return columns;
    }

    let Some(backend) = backend else {
        return Vec::new();
    };

    let registry = match crate::runtime::load_public_surface_registry_with_backend(backend).await {
        Ok(registry) => registry,
        Err(_) => return Vec::new(),
    };
    registry
        .public_surface_columns(table_name)
        .unwrap_or_default()
}

async fn resolve_available_tables(backend: &dyn LixBackend) -> Vec<String> {
    match crate::runtime::load_public_surface_registry_with_backend(backend).await {
        Ok(registry) => registry.public_surface_names(),
        Err(_) => builtin_public_surface_names(),
    }
}

async fn public_surfaces_in_relation_names_with_backend(
    backend: &dyn LixBackend,
    relation_names: &[String],
    fallback_statements: Option<&[Statement]>,
) -> Vec<String> {
    let registry = match crate::runtime::load_public_surface_registry_with_backend(backend).await {
        Ok(registry) => registry,
        Err(_) => {
            return fallback_statements
                .map(builtin_public_surfaces_in_statements)
                .unwrap_or_default()
        }
    };
    relation_names
        .iter()
        .filter(|name| registry.bind_relation_name(name).is_some())
        .cloned()
        .collect()
}

fn builtin_public_surfaces_in_statements(statements: &[Statement]) -> Vec<String> {
    let builtin_surfaces = builtin_public_surface_names();
    relation_names_from_statements(statements)
        .into_iter()
        .filter(|name| {
            builtin_surfaces
                .iter()
                .any(|surface| surface.eq_ignore_ascii_case(name))
        })
        .collect()
}

pub(crate) fn is_missing_relation_error(err: &LixError) -> bool {
    if err.code == "LIX_ERROR_SQL_UNKNOWN_TABLE" || err.code == "LIX_ERROR_TABLE_NOT_FOUND" {
        return true;
    }
    let lower = err.description.to_lowercase();
    lower.contains("no such table")
        || lower.contains("relation")
            && (lower.contains("does not exist")
                || lower.contains("undefined table")
                || lower.contains("unknown"))
}

fn relation_names_from_statements(statements: &[Statement]) -> Vec<String> {
    let mut result = Vec::<String>::new();
    for statement in statements {
        let _ = visit_relations(statement, |relation| {
            if let Some(name) = relation
                .0
                .last()
                .and_then(ObjectNamePart::as_ident)
                .map(|ident| ident.value.clone())
            {
                let exists = result
                    .iter()
                    .any(|existing| existing.eq_ignore_ascii_case(&name));
                if !exists {
                    result.push(name);
                }
            }
            ControlFlow::<()>::Continue(())
        });
    }
    result
}

fn choose_table_for_unknown_column(
    missing_column: &str,
    relation_names: &[String],
) -> Option<String> {
    if let Some((qualifier, _)) = missing_column.split_once('.') {
        for relation in relation_names {
            if relation.eq_ignore_ascii_case(qualifier) {
                return Some(relation.clone());
            }
        }
    }

    if relation_names.len() == 1 {
        return Some(relation_names[0].clone());
    }

    for relation in relation_names {
        if builtin_public_surface_columns(relation).is_some() {
            return Some(relation.clone());
        }
    }

    None
}

fn normalize_sql_error_with_relation_names_and_catalog_snapshot(
    error: LixError,
    relation_names: &[String],
    catalog_snapshot: &ReadDiagnosticCatalogSnapshot,
) -> LixError {
    if let Some(missing_column) = parse_unknown_column_name(&error.description) {
        let table_name = choose_table_for_unknown_column(&missing_column, relation_names);
        let available_columns = table_name
            .as_deref()
            .map(|table_name| {
                lookup_available_columns(catalog_snapshot, table_name).unwrap_or_default()
            })
            .unwrap_or_default();
        let available_column_refs = available_columns
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        return sql_unknown_column_error(
            &missing_column,
            table_name.as_deref(),
            available_column_refs.as_slice(),
            parse_sql_offset(&error.description),
        );
    }

    if is_missing_relation_error(&error) {
        if let Some(table_name) =
            parse_unknown_table_name(&error.description).or_else(|| relation_names.first().cloned())
        {
            let available_table_refs = catalog_snapshot
                .available_tables
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            return sql_unknown_table_error(
                &table_name,
                available_table_refs.as_slice(),
                parse_sql_offset(&error.description),
            );
        }
        return table_not_found_read_error();
    }

    if !catalog_snapshot.public_surfaces.is_empty() {
        let sanitized = sanitize_lowered_public_sql_error_description(
            &error.description,
            &catalog_snapshot.public_surfaces,
        );
        if sanitized != error.description {
            return LixError::new(&error.code, sanitized);
        }
    }

    error
}

fn lookup_available_columns(
    catalog_snapshot: &ReadDiagnosticCatalogSnapshot,
    relation_name: &str,
) -> Option<Vec<String>> {
    catalog_snapshot
        .available_columns_by_relation
        .iter()
        .find_map(|(candidate, columns)| {
            candidate
                .eq_ignore_ascii_case(relation_name)
                .then(|| columns.clone())
        })
}

fn parse_unknown_column_name(description: &str) -> Option<String> {
    extract_name_after_prefix(description, "no such column:")
        .or_else(|| extract_name_between(description, "column \"", "\" does not exist"))
        .or_else(|| extract_name_between(description, "column '", "' does not exist"))
}

fn parse_unknown_table_name(description: &str) -> Option<String> {
    extract_name_after_prefix(description, "no such table:")
        .or_else(|| extract_name_between(description, "relation \"", "\" does not exist"))
        .or_else(|| extract_name_between(description, "relation '", "' does not exist"))
}

fn parse_sql_offset(description: &str) -> Option<usize> {
    let lower = description.to_ascii_lowercase();
    let marker = "at offset ";
    let index = lower.rfind(marker)?;
    let start = index + marker.len();
    let digits: String = lower[start..]
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect();
    if digits.is_empty() {
        return None;
    }
    digits.parse::<usize>().ok()
}

fn lowered_public_sql_artifact_leaked(description: &str) -> bool {
    let lower = description.to_ascii_lowercase();
    lower.contains("lix_internal_")
        || lower.contains("with target_versions")
        || lower.contains("lix_internal_live_v1_")
        || lower.contains("lix_internal_live_untracked_v1_")
}

pub(crate) fn extract_backend_error_summary(description: &str, marker: &str) -> Option<String> {
    let start = description.find(marker)?;
    let tail = &description[start + marker.len()..];
    let end = tail
        .find(" in ")
        .or_else(|| tail.find(" at offset"))
        .unwrap_or(tail.len());
    let value = tail[..end].trim();
    if value.is_empty() {
        return None;
    }
    Some(format!("{marker} {value}"))
}

fn extract_name_between(description: &str, start_marker: &str, end_marker: &str) -> Option<String> {
    let lower = description.to_ascii_lowercase();
    let start_marker_lower = start_marker.to_ascii_lowercase();
    let end_marker_lower = end_marker.to_ascii_lowercase();
    let start = lower.find(&start_marker_lower)? + start_marker_lower.len();
    let end = lower[start..].find(&end_marker_lower)? + start;
    sanitize_name(&description[start..end])
}

fn extract_name_after_prefix(description: &str, prefix: &str) -> Option<String> {
    let lower = description.to_ascii_lowercase();
    let marker = prefix.to_ascii_lowercase();
    let mut start = lower.find(&marker)? + marker.len();
    while description[start..]
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_whitespace())
    {
        start += 1;
    }
    let mut end = description.len();
    for stop in [' ', '\n', '\r', '\t', ',', ')', ';'] {
        if let Some(index) = description[start..].find(stop) {
            end = end.min(start + index);
        }
    }
    sanitize_name(&description[start..end])
}

fn sanitize_name(raw: &str) -> Option<String> {
    let trimmed = raw
        .trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('\'');
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.to_string())
}
