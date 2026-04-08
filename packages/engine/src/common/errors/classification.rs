use crate::common::errors;
use crate::contracts::artifacts::{ReadDiagnosticCatalogSnapshot, ReadDiagnosticContext};
use crate::contracts::surface::SurfaceRegistry;
use crate::surfaces::{builtin_public_surface_columns, builtin_public_surface_names};
use crate::LixBackend;
use crate::LixError;
use sqlparser::ast::{visit_relations, ObjectNamePart, Statement};
use std::ops::ControlFlow;

#[cfg(test)]
pub(crate) fn normalize_sql_error(error: LixError, statements: &[Statement]) -> LixError {
    if let Some(missing_column) = parse_unknown_column_name(&error.description) {
        let relation_names = relation_names_from_statements(statements);
        let table_name = choose_table_for_unknown_column(&missing_column, &relation_names);
        let available_columns = table_name
            .as_deref()
            .and_then(|table_name| builtin_public_surface_columns(table_name))
            .unwrap_or_default();
        let available_column_refs = available_columns
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        return errors::sql_unknown_column_error(
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
            return errors::sql_unknown_table_error(
                &table_name,
                available_table_refs.as_slice(),
                parse_sql_offset(&error.description),
            );
        }
        return errors::table_not_found_read_error();
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
        let table_name = choose_table_for_unknown_column(&missing_column, &relation_names);
        let available_columns = if let Some(table_name) = table_name.as_deref() {
            resolve_available_columns(table_name, Some(backend)).await
        } else {
            Vec::new()
        };
        let available_column_refs = available_columns
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        return errors::sql_unknown_column_error(
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
            return errors::sql_unknown_table_error(
                &table_name,
                available_table_refs.as_slice(),
                parse_sql_offset(&error.description),
            );
        }
        return errors::table_not_found_read_error();
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

    let registry = match crate::surfaces::load_public_surface_registry_with_backend(backend).await {
        Ok(registry) => registry,
        Err(_) => return Vec::new(),
    };
    registry
        .public_surface_columns(table_name)
        .unwrap_or_default()
}

async fn resolve_available_tables(backend: &dyn LixBackend) -> Vec<String> {
    match crate::surfaces::load_public_surface_registry_with_backend(backend).await {
        Ok(registry) => registry.public_surface_names(),
        Err(_) => builtin_public_surface_names(),
    }
}

async fn public_surfaces_in_relation_names_with_backend(
    backend: &dyn LixBackend,
    relation_names: &[String],
    fallback_statements: Option<&[Statement]>,
) -> Vec<String> {
    let registry = match crate::surfaces::load_public_surface_registry_with_backend(backend).await {
        Ok(registry) => registry,
        Err(_) => {
            return fallback_statements
                .map(builtin_public_surfaces_in_statements)
                .unwrap_or_default();
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
        return errors::sql_unknown_column_error(
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
            return errors::sql_unknown_table_error(
                &table_name,
                available_table_refs.as_slice(),
                parse_sql_offset(&error.description),
            );
        }
        return errors::table_not_found_read_error();
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

#[cfg(test)]
mod tests {
    use super::{
        build_read_diagnostic_catalog_snapshot, is_missing_relation_error, normalize_sql_error,
        normalize_sql_error_with_read_diagnostic_context,
        sanitize_lowered_public_sql_error_description,
    };
    use crate::contracts::artifacts::{ReadDiagnosticCatalogSnapshot, ReadDiagnosticContext};
    use crate::contracts::surface::{builtin_surface_descriptors, SurfaceRegistry};
    use crate::LixError;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use std::collections::BTreeMap;

    #[test]
    fn classifies_missing_relation_messages() {
        assert!(is_missing_relation_error(&LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "no such table: foo".to_string(),
        }));
        assert!(is_missing_relation_error(&LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "ERROR: relation \"foo\" does not exist".to_string(),
        }));
        assert!(is_missing_relation_error(&LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "undefined table: relation foo".to_string(),
        }));
        assert!(!is_missing_relation_error(&LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "CHECK constraint failed".to_string(),
        }));
    }

    #[test]
    fn normalizes_unknown_column_with_table_hints() {
        let statements = Parser::parse_sql(
            &GenericDialect {},
            "SELECT file_id, schema_key, entity_id, status, plugin_key FROM lix_working_changes",
        )
        .expect("parse SQL");
        let error = normalize_sql_error(
            LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "no such column: plugin_key in SELECT ... at offset 47".to_string(),
            },
            &statements,
        );
        assert_eq!(error.code, "LIX_ERROR_SQL_UNKNOWN_COLUMN");
        assert!(error
            .description
            .contains("Column `plugin_key` does not exist on `lix_working_changes`."));
        assert!(error.description.contains("Available columns:"));
        assert!(error.description.contains("schema_key"));
    }

    #[test]
    fn normalizes_unknown_table_with_table_hints() {
        let statements =
            Parser::parse_sql(&GenericDialect {}, "SELECT * FROM lix_sate").expect("parse SQL");
        let error = normalize_sql_error(
            LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "no such table: lix_sate".to_string(),
            },
            &statements,
        );
        assert_eq!(error.code, "LIX_ERROR_SQL_UNKNOWN_TABLE");
        assert!(error
            .description
            .contains("Table `lix_sate` does not exist."));
        assert!(error.description.contains("lix_state"));
    }

    #[test]
    fn normalizes_unknown_column_for_lix_change_with_columns() {
        let statements = Parser::parse_sql(
            &GenericDialect {},
            "SELECT snapshot_json FROM lix_change WHERE id = 'c1'",
        )
        .expect("parse SQL");
        let error = normalize_sql_error(
            LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "no such column: snapshot_json".to_string(),
            },
            &statements,
        );
        assert_eq!(error.code, "LIX_ERROR_SQL_UNKNOWN_COLUMN");
        assert!(error
            .description
            .contains("Column `snapshot_json` does not exist on `lix_change`."));
        assert!(error
            .description
            .contains("Available columns: id, entity_id"));
    }

    #[test]
    fn normalizes_unknown_column_for_lix_registered_schema_with_column_catalog() {
        let statements =
            Parser::parse_sql(&GenericDialect {}, "SELECT id FROM lix_registered_schema")
                .expect("parse SQL");
        let error = normalize_sql_error(
            LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "no such column: id at offset 7".to_string(),
            },
            &statements,
        );

        assert_eq!(error.code, "LIX_ERROR_SQL_UNKNOWN_COLUMN");
        assert!(error
            .description
            .contains("Column `id` does not exist on `lix_registered_schema`."));
        assert!(error.description.contains("Available columns: value"));
        assert!(error.description.contains("lixcol_entity_id"));
        assert!(!error.description.contains("Available columns: (unknown)."));
    }

    #[test]
    fn sanitizes_lowered_public_ambiguous_column_errors() {
        let sanitized = sanitize_lowered_public_sql_error_description(
            "ambiguous column name: *.lix_key_value.key in SELECT * FROM (WITH target_versions AS (...) SELECT * FROM lix_internal_live_v1_lix_key_value)",
            &[String::from("lix_key_value")],
        );

        assert_eq!(sanitized, "ambiguous column name: *.lix_key_value.key");
    }

    #[test]
    fn normalizes_lowered_public_sql_leaks_to_generic_message() {
        let statements = Parser::parse_sql(
            &GenericDialect {},
            "SELECT * FROM lix_key_value, lix_key_value",
        )
        .expect("parse SQL");
        let error = normalize_sql_error(
            LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description:
                    "backend failed in SELECT * FROM (WITH target_versions AS (...) SELECT * FROM lix_internal_live_v1_lix_key_value)"
                        .to_string(),
            },
            &statements,
        );

        assert_eq!(error.code, "LIX_ERROR_UNKNOWN");
        assert_eq!(
            error.description,
            "public read on lix_key_value execution failed after lowering"
        );
    }

    #[test]
    fn read_diagnostic_context_preserves_lowered_public_sanitization_without_backend_registry() {
        let context = ReadDiagnosticContext {
            source_sql: vec!["SELECT * FROM lix_key_value".into()],
            relation_names: vec!["lix_key_value".into()],
            catalog_snapshot: ReadDiagnosticCatalogSnapshot {
                public_surfaces: vec!["lix_key_value".into()],
                available_tables: vec!["lix_key_value".into()],
                available_columns_by_relation: BTreeMap::from([(
                    "lix_key_value".into(),
                    vec!["key".into(), "value".into()],
                )]),
            },
            explain_mode: None,
            plain_explain_template: None,
            analyzed_explain_template: None,
        };

        let error = normalize_sql_error_with_read_diagnostic_context(
            LixError {
                code: "LIX_ERROR_UNKNOWN".into(),
                description:
                    "backend failed in SELECT * FROM (WITH target_versions AS (...) SELECT * FROM lix_internal_live_v1_lix_key_value)"
                        .into(),
            },
            &context,
        );

        assert_eq!(
            error.description,
            "public read on lix_key_value execution failed after lowering"
        );
    }

    #[test]
    fn read_diagnostic_catalog_snapshot_captures_public_surface_columns_from_registry() {
        let mut registry = SurfaceRegistry::default();
        registry.insert_descriptors(builtin_surface_descriptors());

        let snapshot =
            build_read_diagnostic_catalog_snapshot(&registry, &["lix_registered_schema".into()]);

        assert!(snapshot
            .public_surfaces
            .iter()
            .any(|name| name == "lix_registered_schema"));
        assert!(snapshot
            .available_tables
            .iter()
            .any(|name| name == "lix_registered_schema"));
        assert!(snapshot
            .available_columns_by_relation
            .get("lix_registered_schema")
            .is_some_and(|columns| columns.iter().any(|column| column == "value")));
    }
}
