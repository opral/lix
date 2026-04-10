use crate::contracts::artifacts::{
    DirectoryHistoryRow, FileHistoryRow, PreparedDirectDirectoryHistoryField,
    PreparedDirectEntityHistoryField, PreparedDirectFileHistoryField, PreparedDirectPublicRead,
    PreparedDirectStateHistoryField, PreparedDirectoryHistoryAggregate,
    PreparedDirectoryHistoryDirectReadPlan, PreparedDirectoryHistoryPredicate,
    PreparedDirectoryHistorySortKey, PreparedEntityHistoryDirectReadPlan,
    PreparedEntityHistoryPredicate, PreparedEntityHistorySortKey, PreparedFileHistoryAggregate,
    PreparedFileHistoryDirectReadPlan, PreparedFileHistoryPredicate, PreparedFileHistorySortKey,
    PreparedStateHistoryAggregate, PreparedStateHistoryAggregatePredicate,
    PreparedStateHistoryDirectReadPlan, PreparedStateHistoryPredicate,
    PreparedStateHistoryProjectionValue, PreparedStateHistorySortKey,
    PreparedStateHistorySortValue, StateHistoryRow,
};
use crate::contracts::traits::CommittedStateHistoryReader;
use crate::execution::read::filesystem::history::{
    load_directory_history_rows, load_file_history_rows,
};
use crate::{LixBackend, LixError, QueryResult, Value};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

pub(crate) async fn execute_direct_public_read_with_backend(
    backend: &dyn LixBackend,
    plan: &PreparedDirectPublicRead,
) -> Result<QueryResult, LixError> {
    match plan {
        PreparedDirectPublicRead::StateHistory(plan) => {
            execute_direct_state_history_read(backend, plan).await
        }
        PreparedDirectPublicRead::EntityHistory(plan) => {
            execute_direct_entity_history_read(backend, plan).await
        }
        PreparedDirectPublicRead::FileHistory(plan) => {
            execute_direct_file_history_read(backend, plan).await
        }
        PreparedDirectPublicRead::DirectoryHistory(plan) => {
            execute_direct_directory_history_read(backend, plan).await
        }
    }
}

async fn execute_direct_state_history_read(
    backend: &dyn LixBackend,
    plan: &PreparedStateHistoryDirectReadPlan,
) -> Result<QueryResult, LixError> {
    let mut rows = backend
        .load_committed_state_history_rows(&plan.request)
        .await?;
    rows.retain(|row| state_history_row_matches_predicates(row, &plan.predicates));

    if state_history_plan_uses_grouping(plan) {
        return execute_grouped_direct_state_history_read(rows, plan);
    }

    rows.sort_by(|left, right| compare_state_history_rows(left, right, &plan.sort_keys));

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
        .map(|row| project_state_history_row(&row, plan))
        .collect();

    Ok(QueryResult { rows, columns })
}

async fn execute_direct_entity_history_read(
    backend: &dyn LixBackend,
    plan: &PreparedEntityHistoryDirectReadPlan,
) -> Result<QueryResult, LixError> {
    let rows = backend
        .load_committed_state_history_rows(&plan.request)
        .await?;
    let mut rows = rows
        .into_iter()
        .map(EntityHistoryRowView::try_from_state_row)
        .collect::<Result<Vec<_>, _>>()?;
    rows.retain(|row| entity_history_row_matches_predicates(row, &plan.predicates));
    rows.sort_by(|left, right| compare_entity_history_rows(left, right, &plan.sort_keys));

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
        .map(|row| project_entity_history_row(&row, plan))
        .collect();

    Ok(QueryResult { rows, columns })
}

struct EntityHistoryRowView {
    row: StateHistoryRow,
    snapshot: Option<JsonValue>,
}

impl EntityHistoryRowView {
    fn try_from_state_row(row: StateHistoryRow) -> Result<Self, LixError> {
        let snapshot = match row.snapshot_content.as_deref() {
            Some(snapshot) => Some(serde_json::from_str(snapshot).map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "direct entity-history execution could not parse snapshot_content: {error}"
                    ),
                )
            })?),
            None => None,
        };
        Ok(Self { row, snapshot })
    }
}

fn state_history_plan_uses_grouping(plan: &PreparedStateHistoryDirectReadPlan) -> bool {
    !plan.group_by_fields.is_empty()
        || plan.having.is_some()
        || plan.projections.iter().any(|projection| {
            matches!(
                projection.value,
                PreparedStateHistoryProjectionValue::Aggregate(_)
            )
        })
}

fn execute_grouped_direct_state_history_read(
    rows: Vec<StateHistoryRow>,
    plan: &PreparedStateHistoryDirectReadPlan,
) -> Result<QueryResult, LixError> {
    let mut groups = BTreeMap::<String, StateHistoryGroupAccumulator>::new();
    for row in rows {
        let group_values = plan
            .group_by_fields
            .iter()
            .map(|field| state_history_field_value(&row, field))
            .collect::<Vec<_>>();
        let key = state_history_group_key(&group_values)?;
        let entry = groups
            .entry(key)
            .or_insert_with(|| StateHistoryGroupAccumulator {
                group_values,
                count: 0,
            });
        entry.count += 1;
    }

    if groups.is_empty() && plan.group_by_fields.is_empty() {
        groups.insert(
            "__all__".to_string(),
            StateHistoryGroupAccumulator {
                group_values: Vec::new(),
                count: 0,
            },
        );
    }

    let mut grouped = groups
        .into_values()
        .filter(|group| state_history_group_matches_having(group, plan.having.as_ref()))
        .collect::<Vec<_>>();
    grouped.sort_by(|left, right| compare_state_history_groups(left, right, plan));

    let offset = usize::try_from(plan.offset).unwrap_or(usize::MAX);
    let limit = plan.limit.and_then(|value| usize::try_from(value).ok());
    let grouped = grouped.into_iter().skip(offset);
    let grouped = if let Some(limit) = limit {
        grouped.take(limit).collect::<Vec<_>>()
    } else {
        grouped.collect::<Vec<_>>()
    };

    let columns = plan
        .projections
        .iter()
        .map(|projection| projection.output_name.clone())
        .collect();
    let rows = grouped
        .into_iter()
        .map(|group| project_state_history_group(&group, plan))
        .collect();

    Ok(QueryResult { rows, columns })
}

struct StateHistoryGroupAccumulator {
    group_values: Vec<Value>,
    count: i64,
}

fn state_history_group_key(values: &[Value]) -> Result<String, LixError> {
    serde_json::to_string(values).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("could not serialize state-history group key: {error}"),
        )
    })
}

fn state_history_group_matches_having(
    group: &StateHistoryGroupAccumulator,
    predicate: Option<&PreparedStateHistoryAggregatePredicate>,
) -> bool {
    let Some(predicate) = predicate else {
        return true;
    };
    match predicate {
        PreparedStateHistoryAggregatePredicate::Eq(PreparedStateHistoryAggregate::Count, value) => {
            group.count == *value
        }
        PreparedStateHistoryAggregatePredicate::NotEq(
            PreparedStateHistoryAggregate::Count,
            value,
        ) => group.count != *value,
        PreparedStateHistoryAggregatePredicate::Gt(PreparedStateHistoryAggregate::Count, value) => {
            group.count > *value
        }
        PreparedStateHistoryAggregatePredicate::GtEq(
            PreparedStateHistoryAggregate::Count,
            value,
        ) => group.count >= *value,
        PreparedStateHistoryAggregatePredicate::Lt(PreparedStateHistoryAggregate::Count, value) => {
            group.count < *value
        }
        PreparedStateHistoryAggregatePredicate::LtEq(
            PreparedStateHistoryAggregate::Count,
            value,
        ) => group.count <= *value,
    }
}

fn state_history_row_matches_predicates(
    row: &StateHistoryRow,
    predicates: &[PreparedStateHistoryPredicate],
) -> bool {
    predicates
        .iter()
        .all(|predicate| state_history_row_matches_predicate(row, predicate))
}

fn entity_history_row_matches_predicates(
    row: &EntityHistoryRowView,
    predicates: &[PreparedEntityHistoryPredicate],
) -> bool {
    predicates
        .iter()
        .all(|predicate| entity_history_row_matches_predicate(row, predicate))
}

fn state_history_row_matches_predicate(
    row: &StateHistoryRow,
    predicate: &PreparedStateHistoryPredicate,
) -> bool {
    match predicate {
        PreparedStateHistoryPredicate::Eq(field, value) => {
            state_history_field_value(row, field) == *value
        }
        PreparedStateHistoryPredicate::NotEq(field, value) => {
            state_history_field_value(row, field) != *value
        }
        PreparedStateHistoryPredicate::Gt(field, value) => {
            compare_public_values(&state_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_gt())
        }
        PreparedStateHistoryPredicate::GtEq(field, value) => {
            compare_public_values(&state_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_gt() || ordering.is_eq())
        }
        PreparedStateHistoryPredicate::Lt(field, value) => {
            compare_public_values(&state_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_lt())
        }
        PreparedStateHistoryPredicate::LtEq(field, value) => {
            compare_public_values(&state_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_lt() || ordering.is_eq())
        }
        PreparedStateHistoryPredicate::In(field, values) => values
            .iter()
            .any(|value| state_history_field_value(row, field) == *value),
        PreparedStateHistoryPredicate::IsNull(field) => {
            matches!(state_history_field_value(row, field), Value::Null)
        }
        PreparedStateHistoryPredicate::IsNotNull(field) => {
            !matches!(state_history_field_value(row, field), Value::Null)
        }
    }
}

fn entity_history_row_matches_predicate(
    row: &EntityHistoryRowView,
    predicate: &PreparedEntityHistoryPredicate,
) -> bool {
    match predicate {
        PreparedEntityHistoryPredicate::Eq(field, value) => {
            entity_history_field_value(row, field) == *value
        }
        PreparedEntityHistoryPredicate::NotEq(field, value) => {
            entity_history_field_value(row, field) != *value
        }
        PreparedEntityHistoryPredicate::Gt(field, value) => {
            compare_public_values(&entity_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_gt())
        }
        PreparedEntityHistoryPredicate::GtEq(field, value) => {
            compare_public_values(&entity_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_gt() || ordering.is_eq())
        }
        PreparedEntityHistoryPredicate::Lt(field, value) => {
            compare_public_values(&entity_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_lt())
        }
        PreparedEntityHistoryPredicate::LtEq(field, value) => {
            compare_public_values(&entity_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_lt() || ordering.is_eq())
        }
        PreparedEntityHistoryPredicate::In(field, values) => values
            .iter()
            .any(|value| entity_history_field_value(row, field) == *value),
        PreparedEntityHistoryPredicate::IsNull(field) => {
            matches!(entity_history_field_value(row, field), Value::Null)
        }
        PreparedEntityHistoryPredicate::IsNotNull(field) => {
            !matches!(entity_history_field_value(row, field), Value::Null)
        }
    }
}

fn compare_state_history_rows(
    left: &StateHistoryRow,
    right: &StateHistoryRow,
    sort_keys: &[PreparedStateHistorySortKey],
) -> std::cmp::Ordering {
    if sort_keys.is_empty() {
        return std::cmp::Ordering::Equal;
    }

    for key in sort_keys {
        let Some(PreparedStateHistorySortValue::Field(field)) = &key.value else {
            continue;
        };
        let ordering = compare_public_values(
            &state_history_field_value(left, field),
            &state_history_field_value(right, field),
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

fn compare_entity_history_rows(
    left: &EntityHistoryRowView,
    right: &EntityHistoryRowView,
    sort_keys: &[PreparedEntityHistorySortKey],
) -> std::cmp::Ordering {
    if sort_keys.is_empty() {
        return std::cmp::Ordering::Equal;
    }

    for key in sort_keys {
        let Some(field) = &key.field else {
            continue;
        };
        let ordering = compare_public_values(
            &entity_history_field_value(left, field),
            &entity_history_field_value(right, field),
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

fn project_state_history_row(
    row: &StateHistoryRow,
    plan: &PreparedStateHistoryDirectReadPlan,
) -> Vec<Value> {
    if plan.wildcard_projection {
        return plan
            .wildcard_columns
            .iter()
            .map(|column| {
                direct_state_history_field_from_column_name_for_projection(column)
                    .map(|field| state_history_field_value(row, &field))
                    .unwrap_or(Value::Null)
            })
            .collect();
    }

    plan.projections
        .iter()
        .map(|projection| match &projection.value {
            PreparedStateHistoryProjectionValue::Field(field) => {
                state_history_field_value(row, field)
            }
            PreparedStateHistoryProjectionValue::Aggregate(
                PreparedStateHistoryAggregate::Count,
            ) => Value::Integer(1),
        })
        .collect()
}

fn project_entity_history_row(
    row: &EntityHistoryRowView,
    plan: &PreparedEntityHistoryDirectReadPlan,
) -> Vec<Value> {
    if plan.wildcard_projection {
        return plan
            .wildcard_columns
            .iter()
            .map(|column| {
                direct_entity_history_field_from_column_name(&plan.surface_binding, column)
                    .map(|field| entity_history_field_value(row, &field))
                    .unwrap_or(Value::Null)
            })
            .collect();
    }

    plan.projections
        .iter()
        .map(|projection| entity_history_field_value(row, &projection.field))
        .collect()
}

fn entity_history_field_value(
    row: &EntityHistoryRowView,
    field: &PreparedDirectEntityHistoryField,
) -> Value {
    match field {
        PreparedDirectEntityHistoryField::State(field) => {
            state_history_field_value(&row.row, field)
        }
        PreparedDirectEntityHistoryField::Property(property) => row
            .snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.get(property))
            .map(json_value_to_public_value)
            .unwrap_or(Value::Null),
    }
}

fn json_value_to_public_value(value: &JsonValue) -> Value {
    match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(value) => Value::Boolean(*value),
        JsonValue::Number(number) => {
            if let Some(value) = number.as_i64() {
                Value::Integer(value)
            } else if let Some(value) = number.as_f64() {
                Value::Real(value)
            } else {
                Value::Text(number.to_string())
            }
        }
        JsonValue::String(value) => Value::Text(value.clone()),
        JsonValue::Array(_) | JsonValue::Object(_) => Value::Json(value.clone()),
    }
}

fn compare_state_history_groups(
    left: &StateHistoryGroupAccumulator,
    right: &StateHistoryGroupAccumulator,
    plan: &PreparedStateHistoryDirectReadPlan,
) -> std::cmp::Ordering {
    if plan.sort_keys.is_empty() {
        return std::cmp::Ordering::Equal;
    }

    for key in &plan.sort_keys {
        let Some(value) = &key.value else {
            continue;
        };
        let ordering = match value {
            PreparedStateHistorySortValue::Field(field) => compare_public_values(
                &state_history_group_field_value(left, &plan.group_by_fields, field),
                &state_history_group_field_value(right, &plan.group_by_fields, field),
            ),
            PreparedStateHistorySortValue::Aggregate(PreparedStateHistoryAggregate::Count) => {
                compare_public_values(&Value::Integer(left.count), &Value::Integer(right.count))
            }
        }
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

fn state_history_group_field_value(
    group: &StateHistoryGroupAccumulator,
    group_by_fields: &[PreparedDirectStateHistoryField],
    field: &PreparedDirectStateHistoryField,
) -> Value {
    group_by_fields
        .iter()
        .position(|candidate| candidate == field)
        .and_then(|index| group.group_values.get(index).cloned())
        .unwrap_or(Value::Null)
}

fn project_state_history_group(
    group: &StateHistoryGroupAccumulator,
    plan: &PreparedStateHistoryDirectReadPlan,
) -> Vec<Value> {
    plan.projections
        .iter()
        .map(|projection| match &projection.value {
            PreparedStateHistoryProjectionValue::Field(field) => {
                state_history_group_field_value(group, &plan.group_by_fields, field)
            }
            PreparedStateHistoryProjectionValue::Aggregate(
                PreparedStateHistoryAggregate::Count,
            ) => Value::Integer(group.count),
        })
        .collect()
}

fn direct_state_history_field_from_column_name_for_projection(
    column: &str,
) -> Option<PreparedDirectStateHistoryField> {
    match column.to_ascii_lowercase().as_str() {
        "entity_id" => Some(PreparedDirectStateHistoryField::EntityId),
        "schema_key" => Some(PreparedDirectStateHistoryField::SchemaKey),
        "file_id" => Some(PreparedDirectStateHistoryField::FileId),
        "plugin_key" => Some(PreparedDirectStateHistoryField::PluginKey),
        "snapshot_content" => Some(PreparedDirectStateHistoryField::SnapshotContent),
        "metadata" => Some(PreparedDirectStateHistoryField::Metadata),
        "schema_version" => Some(PreparedDirectStateHistoryField::SchemaVersion),
        "change_id" => Some(PreparedDirectStateHistoryField::ChangeId),
        "commit_id" => Some(PreparedDirectStateHistoryField::CommitId),
        "commit_created_at" => Some(PreparedDirectStateHistoryField::CommitCreatedAt),
        "root_commit_id" => Some(PreparedDirectStateHistoryField::RootCommitId),
        "depth" => Some(PreparedDirectStateHistoryField::Depth),
        "version_id" => Some(PreparedDirectStateHistoryField::VersionId),
        _ => None,
    }
}

fn state_history_field_value(
    row: &StateHistoryRow,
    field: &PreparedDirectStateHistoryField,
) -> Value {
    match field {
        PreparedDirectStateHistoryField::EntityId => Value::Text(row.entity_id.clone()),
        PreparedDirectStateHistoryField::SchemaKey => Value::Text(row.schema_key.clone()),
        PreparedDirectStateHistoryField::FileId => Value::Text(row.file_id.clone()),
        PreparedDirectStateHistoryField::PluginKey => Value::Text(row.plugin_key.clone()),
        PreparedDirectStateHistoryField::SnapshotContent => row
            .snapshot_content
            .as_ref()
            .map(|value: &String| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
        PreparedDirectStateHistoryField::Metadata => row
            .metadata
            .as_ref()
            .map(|value: &String| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
        PreparedDirectStateHistoryField::SchemaVersion => Value::Text(row.schema_version.clone()),
        PreparedDirectStateHistoryField::ChangeId => Value::Text(row.change_id.clone()),
        PreparedDirectStateHistoryField::CommitId => Value::Text(row.commit_id.clone()),
        PreparedDirectStateHistoryField::CommitCreatedAt => {
            Value::Text(row.commit_created_at.clone())
        }
        PreparedDirectStateHistoryField::RootCommitId => Value::Text(row.root_commit_id.clone()),
        PreparedDirectStateHistoryField::Depth => Value::Integer(row.depth),
        PreparedDirectStateHistoryField::VersionId => Value::Text(row.version_id.clone()),
    }
}

async fn execute_direct_file_history_read(
    backend: &dyn LixBackend,
    plan: &PreparedFileHistoryDirectReadPlan,
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
    plan: &PreparedFileHistoryDirectReadPlan,
) -> Vec<Value> {
    if plan.wildcard_projection {
        return plan
            .wildcard_columns
            .iter()
            .map(|column| {
                direct_file_history_field_from_column_name_for_projection(column)
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

fn direct_file_history_field_from_column_name_for_projection(
    column: &str,
) -> Option<PreparedDirectFileHistoryField> {
    match column.to_ascii_lowercase().as_str() {
        "id" => Some(PreparedDirectFileHistoryField::Id),
        "path" => Some(PreparedDirectFileHistoryField::Path),
        "data" => Some(PreparedDirectFileHistoryField::Data),
        "metadata" => Some(PreparedDirectFileHistoryField::Metadata),
        "hidden" => Some(PreparedDirectFileHistoryField::Hidden),
        "lixcol_entity_id" => Some(PreparedDirectFileHistoryField::EntityId),
        "lixcol_schema_key" => Some(PreparedDirectFileHistoryField::SchemaKey),
        "lixcol_file_id" => Some(PreparedDirectFileHistoryField::FileId),
        "lixcol_version_id" => Some(PreparedDirectFileHistoryField::VersionId),
        "lixcol_plugin_key" => Some(PreparedDirectFileHistoryField::PluginKey),
        "lixcol_schema_version" => Some(PreparedDirectFileHistoryField::SchemaVersion),
        "lixcol_change_id" => Some(PreparedDirectFileHistoryField::ChangeId),
        "lixcol_metadata" => Some(PreparedDirectFileHistoryField::LixcolMetadata),
        "lixcol_commit_id" => Some(PreparedDirectFileHistoryField::CommitId),
        "lixcol_commit_created_at" => Some(PreparedDirectFileHistoryField::CommitCreatedAt),
        "lixcol_root_commit_id" => Some(PreparedDirectFileHistoryField::RootCommitId),
        "lixcol_depth" => Some(PreparedDirectFileHistoryField::Depth),
        _ => None,
    }
}

fn file_history_field_value(row: &FileHistoryRow, field: &PreparedDirectFileHistoryField) -> Value {
    match field {
        PreparedDirectFileHistoryField::Id => Value::Text(row.id.clone()),
        PreparedDirectFileHistoryField::Path => row
            .path
            .as_ref()
            .map(|value| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
        PreparedDirectFileHistoryField::Data => row
            .data
            .as_ref()
            .map(|value| Value::Blob(value.clone()))
            .unwrap_or(Value::Null),
        PreparedDirectFileHistoryField::Metadata => row
            .metadata
            .as_ref()
            .map(|value| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
        PreparedDirectFileHistoryField::Hidden => {
            row.hidden.map(Value::Boolean).unwrap_or(Value::Null)
        }
        PreparedDirectFileHistoryField::EntityId => Value::Text(row.lixcol_entity_id.clone()),
        PreparedDirectFileHistoryField::SchemaKey => Value::Text(row.lixcol_schema_key.clone()),
        PreparedDirectFileHistoryField::FileId => Value::Text(row.lixcol_file_id.clone()),
        PreparedDirectFileHistoryField::VersionId => Value::Text(row.lixcol_version_id.clone()),
        PreparedDirectFileHistoryField::PluginKey => Value::Text(row.lixcol_plugin_key.clone()),
        PreparedDirectFileHistoryField::SchemaVersion => {
            Value::Text(row.lixcol_schema_version.clone())
        }
        PreparedDirectFileHistoryField::ChangeId => Value::Text(row.lixcol_change_id.clone()),
        PreparedDirectFileHistoryField::LixcolMetadata => row
            .lixcol_metadata
            .as_ref()
            .map(|value| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
        PreparedDirectFileHistoryField::CommitId => Value::Text(row.lixcol_commit_id.clone()),
        PreparedDirectFileHistoryField::CommitCreatedAt => {
            Value::Text(row.lixcol_commit_created_at.clone())
        }
        PreparedDirectFileHistoryField::RootCommitId => {
            Value::Text(row.lixcol_root_commit_id.clone())
        }
        PreparedDirectFileHistoryField::Depth => Value::Integer(row.lixcol_depth),
    }
}

async fn execute_direct_directory_history_read(
    backend: &dyn LixBackend,
    plan: &PreparedDirectoryHistoryDirectReadPlan,
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
    plan: &PreparedDirectoryHistoryDirectReadPlan,
) -> Vec<Value> {
    if plan.wildcard_projection {
        return plan
            .wildcard_columns
            .iter()
            .map(|column| {
                direct_directory_history_field_from_column_name_for_projection(column)
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

fn direct_directory_history_field_from_column_name_for_projection(
    column: &str,
) -> Option<PreparedDirectDirectoryHistoryField> {
    match column.to_ascii_lowercase().as_str() {
        "id" => Some(PreparedDirectDirectoryHistoryField::Id),
        "parent_id" => Some(PreparedDirectDirectoryHistoryField::ParentId),
        "name" => Some(PreparedDirectDirectoryHistoryField::Name),
        "path" => Some(PreparedDirectDirectoryHistoryField::Path),
        "hidden" => Some(PreparedDirectDirectoryHistoryField::Hidden),
        "lixcol_entity_id" => Some(PreparedDirectDirectoryHistoryField::EntityId),
        "lixcol_schema_key" => Some(PreparedDirectDirectoryHistoryField::SchemaKey),
        "lixcol_file_id" => Some(PreparedDirectDirectoryHistoryField::FileId),
        "lixcol_version_id" => Some(PreparedDirectDirectoryHistoryField::VersionId),
        "lixcol_plugin_key" => Some(PreparedDirectDirectoryHistoryField::PluginKey),
        "lixcol_schema_version" => Some(PreparedDirectDirectoryHistoryField::SchemaVersion),
        "lixcol_change_id" => Some(PreparedDirectDirectoryHistoryField::ChangeId),
        "lixcol_metadata" => Some(PreparedDirectDirectoryHistoryField::LixcolMetadata),
        "lixcol_commit_id" => Some(PreparedDirectDirectoryHistoryField::CommitId),
        "lixcol_commit_created_at" => Some(PreparedDirectDirectoryHistoryField::CommitCreatedAt),
        "lixcol_root_commit_id" => Some(PreparedDirectDirectoryHistoryField::RootCommitId),
        "lixcol_depth" => Some(PreparedDirectDirectoryHistoryField::Depth),
        _ => None,
    }
}

fn directory_history_field_value(
    row: &DirectoryHistoryRow,
    field: &PreparedDirectDirectoryHistoryField,
) -> Value {
    match field {
        PreparedDirectDirectoryHistoryField::Id => Value::Text(row.id.clone()),
        PreparedDirectDirectoryHistoryField::ParentId => row
            .parent_id
            .as_ref()
            .map(|value| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
        PreparedDirectDirectoryHistoryField::Name => Value::Text(row.name.clone()),
        PreparedDirectDirectoryHistoryField::Path => row
            .path
            .as_ref()
            .map(|value| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
        PreparedDirectDirectoryHistoryField::Hidden => {
            row.hidden.map(Value::Boolean).unwrap_or(Value::Null)
        }
        PreparedDirectDirectoryHistoryField::EntityId => Value::Text(row.lixcol_entity_id.clone()),
        PreparedDirectDirectoryHistoryField::SchemaKey => {
            Value::Text(row.lixcol_schema_key.clone())
        }
        PreparedDirectDirectoryHistoryField::FileId => Value::Text(row.lixcol_file_id.clone()),
        PreparedDirectDirectoryHistoryField::VersionId => {
            Value::Text(row.lixcol_version_id.clone())
        }
        PreparedDirectDirectoryHistoryField::PluginKey => {
            Value::Text(row.lixcol_plugin_key.clone())
        }
        PreparedDirectDirectoryHistoryField::SchemaVersion => {
            Value::Text(row.lixcol_schema_version.clone())
        }
        PreparedDirectDirectoryHistoryField::ChangeId => Value::Text(row.lixcol_change_id.clone()),
        PreparedDirectDirectoryHistoryField::LixcolMetadata => row
            .lixcol_metadata
            .as_ref()
            .map(|value| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
        PreparedDirectDirectoryHistoryField::CommitId => Value::Text(row.lixcol_commit_id.clone()),
        PreparedDirectDirectoryHistoryField::CommitCreatedAt => {
            Value::Text(row.lixcol_commit_created_at.clone())
        }
        PreparedDirectDirectoryHistoryField::RootCommitId => {
            Value::Text(row.lixcol_root_commit_id.clone())
        }
        PreparedDirectDirectoryHistoryField::Depth => Value::Integer(row.lixcol_depth),
    }
}

fn direct_state_history_field_from_column_name(
    column: &str,
) -> Option<PreparedDirectStateHistoryField> {
    match column.to_ascii_lowercase().as_str() {
        "entity_id" | "lixcol_entity_id" => Some(PreparedDirectStateHistoryField::EntityId),
        "schema_key" | "lixcol_schema_key" => Some(PreparedDirectStateHistoryField::SchemaKey),
        "file_id" | "lixcol_file_id" => Some(PreparedDirectStateHistoryField::FileId),
        "plugin_key" | "lixcol_plugin_key" => Some(PreparedDirectStateHistoryField::PluginKey),
        "snapshot_content" => Some(PreparedDirectStateHistoryField::SnapshotContent),
        "metadata" | "lixcol_metadata" => Some(PreparedDirectStateHistoryField::Metadata),
        "schema_version" | "lixcol_schema_version" => {
            Some(PreparedDirectStateHistoryField::SchemaVersion)
        }
        "change_id" | "lixcol_change_id" => Some(PreparedDirectStateHistoryField::ChangeId),
        "commit_id" | "lixcol_commit_id" => Some(PreparedDirectStateHistoryField::CommitId),
        "commit_created_at" => Some(PreparedDirectStateHistoryField::CommitCreatedAt),
        "root_commit_id" | "lixcol_root_commit_id" => {
            Some(PreparedDirectStateHistoryField::RootCommitId)
        }
        "depth" | "lixcol_depth" => Some(PreparedDirectStateHistoryField::Depth),
        "version_id" | "lixcol_version_id" => Some(PreparedDirectStateHistoryField::VersionId),
        _ => None,
    }
}

fn direct_entity_history_field_from_column_name(
    surface_binding: &crate::catalog::SurfaceBinding,
    column: &str,
) -> Option<PreparedDirectEntityHistoryField> {
    let lowercase = column.to_ascii_lowercase();
    if let Some(field) = direct_state_history_field_from_column_name(column) {
        return Some(PreparedDirectEntityHistoryField::State(field));
    }
    if surface_binding
        .descriptor
        .visible_columns
        .iter()
        .any(|candidate| candidate.eq_ignore_ascii_case(column))
    {
        return Some(PreparedDirectEntityHistoryField::Property(lowercase));
    }
    None
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
