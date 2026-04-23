use crate::history::{
    load_directory_history_rows, load_file_history_rows, DirectoryHistoryRow, FileHistoryRow,
};
use crate::sql::{
    PreparedDirectoryHistoryAggregate, PreparedDirectoryHistoryField,
    PreparedDirectoryHistoryPredicate, PreparedDirectoryHistoryReadPlan,
    PreparedDirectoryHistorySortKey, PreparedFileHistoryAggregate, PreparedFileHistoryField,
    PreparedFileHistoryPredicate, PreparedFileHistoryReadPlan, PreparedFileHistorySortKey,
    PreparedHistoryReadPlan,
};
use crate::{LixBackend, LixError, QueryResult, Value};

pub(crate) async fn execute_history_read_plan_with_backend(
    backend: &dyn LixBackend,
    plan: &PreparedHistoryReadPlan,
) -> Result<QueryResult, LixError> {
    match plan {
        PreparedHistoryReadPlan::FileHistory(plan) => {
            execute_file_history_read(backend, plan).await
        }
        PreparedHistoryReadPlan::DirectoryHistory(plan) => {
            execute_directory_history_read(backend, plan).await
        }
    }
}

async fn execute_file_history_read(
    backend: &dyn LixBackend,
    plan: &PreparedFileHistoryReadPlan,
) -> Result<QueryResult, LixError> {
    let mut rows = load_file_history_rows(backend, &plan.request).await?;
    rows.retain(|row| file_history_row_matches_predicates(row, &plan.predicates));

    if matches!(plan.aggregate, Some(PreparedFileHistoryAggregate::Count)) {
        let columns = vec![plan
            .aggregate_output_name
            .clone()
            .unwrap_or_else(|| "COUNT(*)".to_string())];
        return Ok(QueryResult {
            rows: vec![vec![Value::Integer(rows.len() as i64)]],
            columns,
        });
    }

    rows.sort_by(|left, right| compare_file_history_rows(left, right, &plan.sort_keys));

    let offset = usize::try_from(plan.offset).unwrap_or(usize::MAX);
    let limit = plan.limit.and_then(|value| usize::try_from(value).ok());
    let rows = rows.into_iter().skip(offset);
    let rows = if let Some(limit) = limit {
        rows.take(limit).collect::<Vec<_>>()
    } else {
        rows.collect::<Vec<_>>()
    };

    let columns = if plan.wildcard_projection {
        plan.wildcard_columns.clone()
    } else {
        plan.projections
            .iter()
            .map(|projection| projection.output_name.clone())
            .collect()
    };
    let rows = rows
        .into_iter()
        .map(|row| project_file_history_row(&row, plan))
        .collect();

    Ok(QueryResult { rows, columns })
}

fn file_history_row_matches_predicates(
    row: &FileHistoryRow,
    predicates: &[PreparedFileHistoryPredicate],
) -> bool {
    predicates
        .iter()
        .all(|predicate| file_history_row_matches_predicate(row, predicate))
}

fn file_history_row_matches_predicate(
    row: &FileHistoryRow,
    predicate: &PreparedFileHistoryPredicate,
) -> bool {
    match predicate {
        PreparedFileHistoryPredicate::Eq(field, value) => {
            file_history_field_value(row, field) == *value
        }
        PreparedFileHistoryPredicate::NotEq(field, value) => {
            file_history_field_value(row, field) != *value
        }
        PreparedFileHistoryPredicate::Gt(field, value) => {
            compare_public_values(&file_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_gt())
        }
        PreparedFileHistoryPredicate::GtEq(field, value) => {
            compare_public_values(&file_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_gt() || ordering.is_eq())
        }
        PreparedFileHistoryPredicate::Lt(field, value) => {
            compare_public_values(&file_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_lt())
        }
        PreparedFileHistoryPredicate::LtEq(field, value) => {
            compare_public_values(&file_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_lt() || ordering.is_eq())
        }
        PreparedFileHistoryPredicate::In(field, values) => values
            .iter()
            .any(|value| file_history_field_value(row, field) == *value),
        PreparedFileHistoryPredicate::IsNull(field) => {
            matches!(file_history_field_value(row, field), Value::Null)
        }
        PreparedFileHistoryPredicate::IsNotNull(field) => {
            !matches!(file_history_field_value(row, field), Value::Null)
        }
    }
}

fn compare_file_history_rows(
    left: &FileHistoryRow,
    right: &FileHistoryRow,
    sort_keys: &[PreparedFileHistorySortKey],
) -> std::cmp::Ordering {
    if sort_keys.is_empty() {
        return std::cmp::Ordering::Equal;
    }
    for key in sort_keys {
        let Some(field) = &key.field else {
            continue;
        };
        let ordering = compare_public_values(
            &file_history_field_value(left, field),
            &file_history_field_value(right, field),
        )
        .unwrap_or(std::cmp::Ordering::Equal);
        if ordering != std::cmp::Ordering::Equal {
            return if key.descending {
                ordering.reverse()
            } else {
                ordering
            };
        }
    }
    std::cmp::Ordering::Equal
}

fn project_file_history_row(
    row: &FileHistoryRow,
    plan: &PreparedFileHistoryReadPlan,
) -> Vec<Value> {
    if plan.wildcard_projection {
        return plan
            .wildcard_columns
            .iter()
            .map(|column| {
                file_history_field_from_column_name_for_projection(column)
                    .map(|field| file_history_field_value(row, &field))
                    .unwrap_or(Value::Null)
            })
            .collect();
    }

    plan.projections
        .iter()
        .map(|projection| file_history_field_value(row, &projection.field))
        .collect()
}

fn file_history_field_from_column_name_for_projection(
    column: &str,
) -> Option<PreparedFileHistoryField> {
    match column.to_ascii_lowercase().as_str() {
        "id" => Some(PreparedFileHistoryField::Id),
        "path" => Some(PreparedFileHistoryField::Path),
        "data" => Some(PreparedFileHistoryField::Data),
        "hidden" => Some(PreparedFileHistoryField::Hidden),
        "lixcol_entity_id" => Some(PreparedFileHistoryField::EntityId),
        "lixcol_schema_key" => Some(PreparedFileHistoryField::SchemaKey),
        "lixcol_file_id" => Some(PreparedFileHistoryField::FileId),
        "lixcol_version_id" => Some(PreparedFileHistoryField::VersionId),
        "lixcol_plugin_key" => Some(PreparedFileHistoryField::PluginKey),
        "lixcol_schema_version" => Some(PreparedFileHistoryField::SchemaVersion),
        "lixcol_change_id" => Some(PreparedFileHistoryField::ChangeId),
        "lixcol_metadata" => Some(PreparedFileHistoryField::LixcolMetadata),
        "lixcol_commit_id" => Some(PreparedFileHistoryField::CommitId),
        "lixcol_commit_created_at" => Some(PreparedFileHistoryField::CommitCreatedAt),
        "lixcol_root_commit_id" => Some(PreparedFileHistoryField::RootCommitId),
        "lixcol_depth" => Some(PreparedFileHistoryField::Depth),
        _ => None,
    }
}

fn file_history_field_value(row: &FileHistoryRow, field: &PreparedFileHistoryField) -> Value {
    match field {
        PreparedFileHistoryField::Id => Value::Text(row.id.clone()),
        PreparedFileHistoryField::Path => row
            .path
            .as_ref()
            .map(|value| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
        PreparedFileHistoryField::Data => row
            .data
            .as_ref()
            .map(|value| Value::Blob(value.clone()))
            .unwrap_or(Value::Null),
        PreparedFileHistoryField::Hidden => row.hidden.map(Value::Boolean).unwrap_or(Value::Null),
        PreparedFileHistoryField::EntityId => Value::Text(row.lixcol_entity_id.clone()),
        PreparedFileHistoryField::SchemaKey => Value::Text(row.lixcol_schema_key.clone()),
        PreparedFileHistoryField::FileId => row
            .lixcol_file_id
            .as_ref()
            .map(|value| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
        PreparedFileHistoryField::VersionId => Value::Text(row.lixcol_version_id.clone()),
        PreparedFileHistoryField::PluginKey => row
            .lixcol_plugin_key
            .as_ref()
            .map(|value| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
        PreparedFileHistoryField::SchemaVersion => Value::Text(row.lixcol_schema_version.clone()),
        PreparedFileHistoryField::ChangeId => Value::Text(row.lixcol_change_id.clone()),
        PreparedFileHistoryField::LixcolMetadata => row
            .lixcol_metadata
            .as_ref()
            .map(|value| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
        PreparedFileHistoryField::CommitId => Value::Text(row.lixcol_commit_id.clone()),
        PreparedFileHistoryField::CommitCreatedAt => {
            Value::Text(row.lixcol_commit_created_at.clone())
        }
        PreparedFileHistoryField::RootCommitId => Value::Text(row.lixcol_root_commit_id.clone()),
        PreparedFileHistoryField::Depth => Value::Integer(row.lixcol_depth),
    }
}

async fn execute_directory_history_read(
    backend: &dyn LixBackend,
    plan: &PreparedDirectoryHistoryReadPlan,
) -> Result<QueryResult, LixError> {
    let mut rows = load_directory_history_rows(backend, &plan.request).await?;
    rows.retain(|row| directory_history_row_matches_predicates(row, &plan.predicates));

    if matches!(
        plan.aggregate,
        Some(PreparedDirectoryHistoryAggregate::Count)
    ) {
        let columns = vec![plan
            .aggregate_output_name
            .clone()
            .unwrap_or_else(|| "COUNT(*)".to_string())];
        return Ok(QueryResult {
            rows: vec![vec![Value::Integer(rows.len() as i64)]],
            columns,
        });
    }

    rows.sort_by(|left, right| compare_directory_history_rows(left, right, &plan.sort_keys));

    let offset = usize::try_from(plan.offset).unwrap_or(usize::MAX);
    let limit = plan.limit.and_then(|value| usize::try_from(value).ok());
    let rows = rows.into_iter().skip(offset);
    let rows = if let Some(limit) = limit {
        rows.take(limit).collect::<Vec<_>>()
    } else {
        rows.collect::<Vec<_>>()
    };

    let columns = if plan.wildcard_projection {
        plan.wildcard_columns.clone()
    } else {
        plan.projections
            .iter()
            .map(|projection| projection.output_name.clone())
            .collect()
    };
    let rows = rows
        .into_iter()
        .map(|row| project_directory_history_row(&row, plan))
        .collect();

    Ok(QueryResult { rows, columns })
}

fn directory_history_row_matches_predicates(
    row: &DirectoryHistoryRow,
    predicates: &[PreparedDirectoryHistoryPredicate],
) -> bool {
    predicates
        .iter()
        .all(|predicate| directory_history_row_matches_predicate(row, predicate))
}

fn directory_history_row_matches_predicate(
    row: &DirectoryHistoryRow,
    predicate: &PreparedDirectoryHistoryPredicate,
) -> bool {
    match predicate {
        PreparedDirectoryHistoryPredicate::Eq(field, value) => {
            directory_history_field_value(row, field) == *value
        }
        PreparedDirectoryHistoryPredicate::NotEq(field, value) => {
            directory_history_field_value(row, field) != *value
        }
        PreparedDirectoryHistoryPredicate::Gt(field, value) => {
            compare_public_values(&directory_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_gt())
        }
        PreparedDirectoryHistoryPredicate::GtEq(field, value) => {
            compare_public_values(&directory_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_gt() || ordering.is_eq())
        }
        PreparedDirectoryHistoryPredicate::Lt(field, value) => {
            compare_public_values(&directory_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_lt())
        }
        PreparedDirectoryHistoryPredicate::LtEq(field, value) => {
            compare_public_values(&directory_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_lt() || ordering.is_eq())
        }
        PreparedDirectoryHistoryPredicate::In(field, values) => values
            .iter()
            .any(|value| directory_history_field_value(row, field) == *value),
        PreparedDirectoryHistoryPredicate::IsNull(field) => {
            matches!(directory_history_field_value(row, field), Value::Null)
        }
        PreparedDirectoryHistoryPredicate::IsNotNull(field) => {
            !matches!(directory_history_field_value(row, field), Value::Null)
        }
    }
}

fn compare_directory_history_rows(
    left: &DirectoryHistoryRow,
    right: &DirectoryHistoryRow,
    sort_keys: &[PreparedDirectoryHistorySortKey],
) -> std::cmp::Ordering {
    if sort_keys.is_empty() {
        return std::cmp::Ordering::Equal;
    }
    for key in sort_keys {
        let Some(field) = &key.field else {
            continue;
        };
        let ordering = compare_public_values(
            &directory_history_field_value(left, field),
            &directory_history_field_value(right, field),
        )
        .unwrap_or(std::cmp::Ordering::Equal);
        if ordering != std::cmp::Ordering::Equal {
            return if key.descending {
                ordering.reverse()
            } else {
                ordering
            };
        }
    }
    std::cmp::Ordering::Equal
}

fn project_directory_history_row(
    row: &DirectoryHistoryRow,
    plan: &PreparedDirectoryHistoryReadPlan,
) -> Vec<Value> {
    if plan.wildcard_projection {
        return plan
            .wildcard_columns
            .iter()
            .map(|column| {
                directory_history_field_from_column_name_for_projection(column)
                    .map(|field| directory_history_field_value(row, &field))
                    .unwrap_or(Value::Null)
            })
            .collect();
    }

    plan.projections
        .iter()
        .map(|projection| directory_history_field_value(row, &projection.field))
        .collect()
}

fn directory_history_field_from_column_name_for_projection(
    column: &str,
) -> Option<PreparedDirectoryHistoryField> {
    match column.to_ascii_lowercase().as_str() {
        "id" => Some(PreparedDirectoryHistoryField::Id),
        "parent_id" => Some(PreparedDirectoryHistoryField::ParentId),
        "name" => Some(PreparedDirectoryHistoryField::Name),
        "path" => Some(PreparedDirectoryHistoryField::Path),
        "hidden" => Some(PreparedDirectoryHistoryField::Hidden),
        "lixcol_entity_id" => Some(PreparedDirectoryHistoryField::EntityId),
        "lixcol_schema_key" => Some(PreparedDirectoryHistoryField::SchemaKey),
        "lixcol_file_id" => Some(PreparedDirectoryHistoryField::FileId),
        "lixcol_version_id" => Some(PreparedDirectoryHistoryField::VersionId),
        "lixcol_plugin_key" => Some(PreparedDirectoryHistoryField::PluginKey),
        "lixcol_schema_version" => Some(PreparedDirectoryHistoryField::SchemaVersion),
        "lixcol_change_id" => Some(PreparedDirectoryHistoryField::ChangeId),
        "lixcol_metadata" => Some(PreparedDirectoryHistoryField::LixcolMetadata),
        "lixcol_commit_id" => Some(PreparedDirectoryHistoryField::CommitId),
        "lixcol_commit_created_at" => Some(PreparedDirectoryHistoryField::CommitCreatedAt),
        "lixcol_root_commit_id" => Some(PreparedDirectoryHistoryField::RootCommitId),
        "lixcol_depth" => Some(PreparedDirectoryHistoryField::Depth),
        _ => None,
    }
}

fn directory_history_field_value(
    row: &DirectoryHistoryRow,
    field: &PreparedDirectoryHistoryField,
) -> Value {
    match field {
        PreparedDirectoryHistoryField::Id => Value::Text(row.id.clone()),
        PreparedDirectoryHistoryField::ParentId => row
            .parent_id
            .as_ref()
            .map(|value| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
        PreparedDirectoryHistoryField::Name => Value::Text(row.name.clone()),
        PreparedDirectoryHistoryField::Path => row
            .path
            .as_ref()
            .map(|value| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
        PreparedDirectoryHistoryField::Hidden => {
            row.hidden.map(Value::Boolean).unwrap_or(Value::Null)
        }
        PreparedDirectoryHistoryField::EntityId => Value::Text(row.lixcol_entity_id.clone()),
        PreparedDirectoryHistoryField::SchemaKey => Value::Text(row.lixcol_schema_key.clone()),
        PreparedDirectoryHistoryField::FileId => row
            .lixcol_file_id
            .as_ref()
            .map(|value| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
        PreparedDirectoryHistoryField::VersionId => Value::Text(row.lixcol_version_id.clone()),
        PreparedDirectoryHistoryField::PluginKey => row
            .lixcol_plugin_key
            .as_ref()
            .map(|value| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
        PreparedDirectoryHistoryField::SchemaVersion => {
            Value::Text(row.lixcol_schema_version.clone())
        }
        PreparedDirectoryHistoryField::ChangeId => Value::Text(row.lixcol_change_id.clone()),
        PreparedDirectoryHistoryField::LixcolMetadata => row
            .lixcol_metadata
            .as_ref()
            .map(|value| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
        PreparedDirectoryHistoryField::CommitId => Value::Text(row.lixcol_commit_id.clone()),
        PreparedDirectoryHistoryField::CommitCreatedAt => {
            Value::Text(row.lixcol_commit_created_at.clone())
        }
        PreparedDirectoryHistoryField::RootCommitId => {
            Value::Text(row.lixcol_root_commit_id.clone())
        }
        PreparedDirectoryHistoryField::Depth => Value::Integer(row.lixcol_depth),
    }
}

fn compare_public_values(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Integer(left), Value::Integer(right)) => Some(left.cmp(right)),
        (Value::Real(left), Value::Real(right)) => left.partial_cmp(right),
        (Value::Integer(left), Value::Real(right)) => (*left as f64).partial_cmp(right),
        (Value::Real(left), Value::Integer(right)) => left.partial_cmp(&(*right as f64)),
        (Value::Text(left), Value::Text(right)) => Some(left.cmp(right)),
        (Value::Boolean(left), Value::Boolean(right)) => Some(left.cmp(right)),
        (Value::Null, Value::Null) => Some(std::cmp::Ordering::Equal),
        _ => None,
    }
}
