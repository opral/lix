use crate::commit::{
    load_exact_committed_state_row, ExactCommittedStateRow, ExactCommittedStateRowRequest,
};
use crate::sql2::planner::ir::{
    MutationPayload, PlannedStateRow, PlannedWrite, ResolvedRowRef, ResolvedWritePlan, RowLineage,
    SchemaProof, ScopeProof, TargetSetProof, WriteLane, WriteMode, WriteOperationKind,
};
use crate::{LixBackend, Value};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WriteResolveError {
    pub(crate) message: String,
}

pub(crate) async fn resolve_write_plan(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let target_write_lane = match planned_write.command.mode {
        WriteMode::Tracked => Some(write_lane_from_scope(&planned_write.scope_proof)?),
        WriteMode::Untracked => None,
    };

    match planned_write.command.operation_kind {
        WriteOperationKind::Insert => resolve_insert_write_plan(planned_write, target_write_lane),
        WriteOperationKind::Update | WriteOperationKind::Delete => {
            resolve_existing_state_write(backend, planned_write, target_write_lane).await
        }
    }
}

fn resolve_insert_write_plan(
    planned_write: &PlannedWrite,
    target_write_lane: Option<WriteLane>,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let entity_id = resolved_entity_id(planned_write)?;
    let schema_key = resolved_schema_key(planned_write)?;
    let version_id = resolved_version_id(planned_write)?;

    Ok(ResolvedWritePlan {
        authoritative_pre_state: Vec::new(),
        intended_post_state: vec![PlannedStateRow {
            entity_id: entity_id.clone(),
            schema_key,
            version_id,
            values: payload_map(planned_write)?,
            tombstone: false,
        }],
        tombstones: Vec::new(),
        lineage: vec![RowLineage {
            entity_id,
            source_change_id: None,
            source_commit_id: None,
        }],
        target_write_lane,
    })
}

async fn resolve_existing_state_write(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    target_write_lane: Option<WriteLane>,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    if !matches!(target_write_lane, Some(WriteLane::SingleVersion(_))) {
        return Err(WriteResolveError {
            message:
                "sql2 day-1 update/delete resolver only supports explicit single-version lanes"
                    .to_string(),
        });
    }
    if !planned_write.command.selector.exact_only {
        return Err(WriteResolveError {
            message: "sql2 day-1 update/delete resolver only supports exact conjunctive selectors"
                .to_string(),
        });
    }

    let entity_id = resolved_entity_id(planned_write)?;
    let schema_key = resolved_schema_key(planned_write)?;
    let version_id = resolved_version_id(planned_write)?.ok_or_else(|| WriteResolveError {
        message: "sql2 existing-row write resolver requires a concrete version_id".to_string(),
    })?;
    let mut executor = backend;
    let Some(current_row) = load_exact_committed_state_row(
        &mut executor,
        &ExactCommittedStateRowRequest {
            entity_id: entity_id.clone(),
            schema_key: schema_key.clone(),
            version_id: version_id.clone(),
            exact_filters: planned_write.command.selector.exact_filters.clone(),
        },
    )
    .await
    .map_err(write_resolve_backend_error)?
    else {
        return Ok(ResolvedWritePlan {
            authoritative_pre_state: Vec::new(),
            intended_post_state: Vec::new(),
            tombstones: Vec::new(),
            lineage: Vec::new(),
            target_write_lane,
        });
    };

    let row_ref = ResolvedRowRef {
        entity_id: current_row.entity_id.clone(),
        schema_key: current_row.schema_key.clone(),
        version_id: Some(current_row.version_id.clone()),
        source_change_id: current_row.source_change_id.clone(),
        source_commit_id: None,
    };
    let lineage = vec![RowLineage {
        entity_id: current_row.entity_id.clone(),
        source_change_id: current_row.source_change_id.clone(),
        source_commit_id: None,
    }];

    match planned_write.command.operation_kind {
        WriteOperationKind::Update => {
            let values = merged_update_values(&current_row, planned_write)?;
            ensure_identity_columns_preserved(&current_row, &values)?;

            Ok(ResolvedWritePlan {
                authoritative_pre_state: vec![row_ref],
                intended_post_state: vec![PlannedStateRow {
                    entity_id: current_row.entity_id,
                    schema_key: current_row.schema_key,
                    version_id: Some(current_row.version_id),
                    values,
                    tombstone: false,
                }],
                tombstones: Vec::new(),
                lineage,
                target_write_lane,
            })
        }
        WriteOperationKind::Delete => Ok(ResolvedWritePlan {
            authoritative_pre_state: vec![row_ref.clone()],
            intended_post_state: vec![PlannedStateRow {
                entity_id: current_row.entity_id,
                schema_key: current_row.schema_key,
                version_id: Some(current_row.version_id),
                values: current_row.values,
                tombstone: true,
            }],
            tombstones: vec![row_ref],
            lineage,
            target_write_lane,
        }),
        WriteOperationKind::Insert => Err(WriteResolveError {
            message: "sql2 existing-row resolver does not handle inserts".to_string(),
        }),
    }
}

fn merged_update_values(
    current_row: &ExactCommittedStateRow,
    planned_write: &PlannedWrite,
) -> Result<BTreeMap<String, Value>, WriteResolveError> {
    let MutationPayload::Patch(payload) = &planned_write.command.payload else {
        return Err(WriteResolveError {
            message: "sql2 update resolver requires a patch payload".to_string(),
        });
    };

    let mut values = current_row.values.clone();
    for (key, value) in payload {
        values.insert(key.clone(), value.clone());
    }
    Ok(values)
}

fn ensure_identity_columns_preserved(
    current_row: &ExactCommittedStateRow,
    values: &BTreeMap<String, Value>,
) -> Result<(), WriteResolveError> {
    for (column, expected) in [
        ("entity_id", current_row.entity_id.as_str()),
        ("schema_key", current_row.schema_key.as_str()),
        ("file_id", current_row.file_id.as_str()),
        ("version_id", current_row.version_id.as_str()),
    ] {
        let Some(actual) = values.get(column).and_then(text_from_value) else {
            return Err(WriteResolveError {
                message: format!("sql2 update resolver requires '{column}' in authoritative row"),
            });
        };
        if actual != expected {
            return Err(WriteResolveError {
                message: format!("sql2 day-1 update resolver does not support changing '{column}'"),
            });
        }
    }

    Ok(())
}

fn resolved_entity_id(planned_write: &PlannedWrite) -> Result<String, WriteResolveError> {
    if let Some(TargetSetProof::Exact(entity_ids)) = &planned_write.target_set_proof {
        if entity_ids.len() == 1 {
            return Ok(entity_ids
                .iter()
                .next()
                .expect("singleton exact target-set proof")
                .clone());
        }
    }

    payload_text_value(planned_write, "entity_id").ok_or_else(|| WriteResolveError {
        message: "sql2 day-1 write resolver requires an exact entity target".to_string(),
    })
}

fn resolved_schema_key(planned_write: &PlannedWrite) -> Result<String, WriteResolveError> {
    match &planned_write.schema_proof {
        SchemaProof::Exact(schema_keys) if schema_keys.len() == 1 => Ok(schema_keys
            .iter()
            .next()
            .expect("singleton exact schema proof")
            .clone()),
        _ => payload_text_value(planned_write, "schema_key").ok_or_else(|| WriteResolveError {
            message: "sql2 write resolver requires an exact schema proof or schema_key literal"
                .to_string(),
        }),
    }
}

fn resolved_version_id(planned_write: &PlannedWrite) -> Result<Option<String>, WriteResolveError> {
    match &planned_write.scope_proof {
        ScopeProof::ActiveVersion => planned_write
            .command
            .execution_context
            .requested_version_id
            .clone()
            .map(Some)
            .ok_or_else(|| WriteResolveError {
                message:
                    "sql2 write resolver requires requested_version_id for ActiveVersion writes"
                        .to_string(),
            }),
        ScopeProof::SingleVersion(version_id) => Ok(Some(version_id.clone())),
        ScopeProof::FiniteVersionSet(version_ids) if version_ids.len() == 1 => {
            Ok(version_ids.iter().next().cloned())
        }
        ScopeProof::FiniteVersionSet(_) => Err(WriteResolveError {
            message: "sql2 day-1 write resolver cannot resolve multi-version writes".to_string(),
        }),
        ScopeProof::Unknown | ScopeProof::Unbounded => Err(WriteResolveError {
            message: "sql2 day-1 write resolver requires a bounded scope proof".to_string(),
        }),
    }
}

fn write_lane_from_scope(scope_proof: &ScopeProof) -> Result<WriteLane, WriteResolveError> {
    match scope_proof {
        ScopeProof::ActiveVersion => Ok(WriteLane::ActiveVersion),
        ScopeProof::SingleVersion(version_id) => Ok(WriteLane::SingleVersion(version_id.clone())),
        ScopeProof::FiniteVersionSet(version_ids) if version_ids.len() == 1 => {
            Ok(WriteLane::SingleVersion(
                version_ids
                    .iter()
                    .next()
                    .expect("singleton version set")
                    .clone(),
            ))
        }
        ScopeProof::FiniteVersionSet(_) => Err(WriteResolveError {
            message: "sql2 day-1 tracked writes require exactly one write lane".to_string(),
        }),
        ScopeProof::Unknown | ScopeProof::Unbounded => Err(WriteResolveError {
            message: "sql2 day-1 tracked writes require a bounded write lane".to_string(),
        }),
    }
}

fn payload_map(planned_write: &PlannedWrite) -> Result<BTreeMap<String, Value>, WriteResolveError> {
    match &planned_write.command.payload {
        MutationPayload::FullSnapshot(payload) | MutationPayload::Patch(payload) => {
            Ok(payload.clone())
        }
        MutationPayload::Tombstone => Ok(Default::default()),
    }
}

fn payload_text_value(planned_write: &PlannedWrite, key: &str) -> Option<String> {
    let (MutationPayload::FullSnapshot(payload) | MutationPayload::Patch(payload)) =
        &planned_write.command.payload
    else {
        return None;
    };

    match payload.get(key) {
        Some(Value::Text(value)) => Some(value.clone()),
        _ => None,
    }
}

fn text_from_value(value: &Value) -> Option<String> {
    match value {
        Value::Text(value) => Some(value.clone()),
        Value::Integer(value) => Some(value.to_string()),
        Value::Boolean(value) => Some(value.to_string()),
        Value::Real(value) => Some(value.to_string()),
        _ => None,
    }
}

fn write_resolve_backend_error(error: crate::LixError) -> WriteResolveError {
    WriteResolveError {
        message: error.description,
    }
}

#[cfg(test)]
mod tests {
    use super::resolve_write_plan;
    use crate::sql2::catalog::SurfaceRegistry;
    use crate::sql2::core::contracts::{BoundStatement, ExecutionContext};
    use crate::sql2::core::parser::parse_sql_script;
    use crate::sql2::planner::canonicalize::canonicalize_write;
    use crate::sql2::planner::ir::WriteLane;
    use crate::sql2::planner::semantics::proof_engine::prove_write;
    use crate::{LixBackend, LixError, QueryResult, SqlDialect, Value};
    use async_trait::async_trait;

    #[derive(Default)]
    struct FakeBackend {
        state_rows: Vec<Vec<Value>>,
    }

    #[async_trait(?Send)]
    impl LixBackend for FakeBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("FROM \"lix_internal_state_materialized_v1_lix_key_value\"") {
                let entity_filter = extract_sql_string_filter(sql, "entity_id");
                let version_filter = extract_sql_string_filter(sql, "version_id");
                let file_filter = extract_sql_string_filter(sql, "file_id");
                let plugin_filter = extract_sql_string_filter(sql, "plugin_key");
                return Ok(QueryResult {
                        rows: self
                            .state_rows
                        .iter()
                        .filter(|row| {
                            let entity_matches = match entity_filter.as_ref() {
                                Some(entity_id) => {
                                    matches!(row.first(), Some(Value::Text(value)) if value == entity_id)
                                }
                                None => true,
                            };
                            let version_matches = match version_filter.as_ref() {
                                Some(version_id) => {
                                    matches!(row.get(4), Some(Value::Text(value)) if value == version_id)
                                }
                                None => true,
                            };
                            let file_matches = match file_filter.as_ref() {
                                Some(file_id) => {
                                    matches!(row.get(3), Some(Value::Text(value)) if value == file_id)
                                }
                                None => true,
                            };
                            let plugin_matches = match plugin_filter.as_ref() {
                                Some(plugin_key) => {
                                    matches!(row.get(5), Some(Value::Text(value)) if value == plugin_key)
                                }
                                None => true,
                            };
                            entity_matches && version_matches && file_matches && plugin_matches
                        })
                        .cloned()
                        .collect(),
                        columns: vec![
                            "entity_id".to_string(),
                            "schema_key".to_string(),
                            "schema_version".to_string(),
                            "file_id".to_string(),
                            "version_id".to_string(),
                            "plugin_key".to_string(),
                            "snapshot_content".to_string(),
                            "metadata".to_string(),
                            "change_id".to_string(),
                        ],
                    });
            }

            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn begin_transaction(&self) -> Result<Box<dyn crate::LixTransaction + '_>, LixError> {
            Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "transactions are not needed in this test backend".to_string(),
            })
        }
    }

    fn extract_sql_string_filter(sql: &str, column: &str) -> Option<String> {
        let marker = format!("{column} = '");
        let start = sql.find(&marker)? + marker.len();
        let rest = &sql[start..];
        let end = rest.find('\'')?;
        Some(rest[..end].to_string())
    }

    fn planned_write(
        sql: &str,
        requested_version_id: &str,
    ) -> crate::sql2::planner::ir::PlannedWrite {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let mut statements = parse_sql_script(sql).expect("SQL should parse");
        let statement = statements.pop().expect("single statement");
        let bound = BoundStatement::from_statement(
            statement,
            Vec::new(),
            ExecutionContext {
                requested_version_id: Some(requested_version_id.to_string()),
                ..ExecutionContext::default()
            },
        );
        let canonicalized =
            canonicalize_write(bound, &registry).expect("write should canonicalize");
        prove_write(&canonicalized).expect("proofs should succeed")
    }

    #[tokio::test]
    async fn resolves_active_version_insert_with_active_lane() {
        let backend = FakeBackend::default();
        let resolved = resolve_write_plan(
            &backend,
            &planned_write(
                "INSERT INTO lix_state (entity_id, schema_key, file_id, plugin_key, snapshot_content, schema_version) \
                 VALUES ('entity-1', 'lix_key_value', 'lix', 'lix', '{\"key\":\"hello\"}', '1')",
                "main",
            ),
        )
        .await
        .expect("write should resolve");

        assert_eq!(
            resolved.intended_post_state[0].version_id.as_deref(),
            Some("main")
        );
        assert_eq!(resolved.target_write_lane, Some(WriteLane::ActiveVersion));
    }

    #[tokio::test]
    async fn resolves_explicit_version_insert_with_single_version_lane() {
        let backend = FakeBackend::default();
        let resolved = resolve_write_plan(
            &backend,
            &planned_write(
                "INSERT INTO lix_state_by_version (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version) \
                 VALUES ('entity-1', 'lix_key_value', 'lix', 'version-a', 'lix', '{\"key\":\"hello\"}', '1')",
                "main",
            ),
        )
        .await
        .expect("write should resolve");

        assert_eq!(
            resolved.target_write_lane,
            Some(WriteLane::SingleVersion("version-a".to_string()))
        );
        assert_eq!(
            resolved.intended_post_state[0].version_id.as_deref(),
            Some("version-a")
        );
    }

    #[tokio::test]
    async fn resolves_update_from_authoritative_pre_state() {
        let backend = FakeBackend {
            state_rows: vec![vec![
                Value::Text("entity-1".to_string()),
                Value::Text("lix_key_value".to_string()),
                Value::Text("1".to_string()),
                Value::Text("lix".to_string()),
                Value::Text("version-a".to_string()),
                Value::Text("lix".to_string()),
                Value::Text("{\"value\":\"before\"}".to_string()),
                Value::Text("{\"m\":1}".to_string()),
                Value::Text("change-1".to_string()),
            ]],
        };
        let resolved = resolve_write_plan(
            &backend,
            &planned_write(
                "UPDATE lix_state_by_version \
                 SET snapshot_content = '{\"value\":\"after\"}' \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'entity-1' \
                   AND version_id = 'version-a'",
                "main",
            ),
        )
        .await
        .expect("write should resolve");

        assert_eq!(resolved.authoritative_pre_state.len(), 1);
        assert_eq!(
            resolved.intended_post_state[0]
                .values
                .get("file_id")
                .and_then(super::text_from_value)
                .as_deref(),
            Some("lix")
        );
        assert_eq!(
            resolved.intended_post_state[0]
                .values
                .get("snapshot_content")
                .and_then(super::text_from_value)
                .as_deref(),
            Some("{\"value\":\"after\"}")
        );
    }

    #[tokio::test]
    async fn resolves_delete_from_authoritative_pre_state() {
        let backend = FakeBackend {
            state_rows: vec![vec![
                Value::Text("entity-1".to_string()),
                Value::Text("lix_key_value".to_string()),
                Value::Text("1".to_string()),
                Value::Text("lix".to_string()),
                Value::Text("version-a".to_string()),
                Value::Text("lix".to_string()),
                Value::Text("{\"value\":\"before\"}".to_string()),
                Value::Null,
                Value::Text("change-1".to_string()),
            ]],
        };
        let resolved = resolve_write_plan(
            &backend,
            &planned_write(
                "DELETE FROM lix_state_by_version \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'entity-1' \
                   AND version_id = 'version-a'",
                "main",
            ),
        )
        .await
        .expect("write should resolve");

        assert_eq!(resolved.authoritative_pre_state.len(), 1);
        assert_eq!(resolved.tombstones.len(), 1);
        assert!(resolved.intended_post_state[0].tombstone);
    }

    #[tokio::test]
    async fn leaves_noop_update_with_no_rows_to_append() {
        let backend = FakeBackend::default();
        let resolved = resolve_write_plan(
            &backend,
            &planned_write(
                "UPDATE lix_state_by_version \
                 SET snapshot_content = '{\"value\":\"after\"}' \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'entity-missing' \
                   AND version_id = 'version-a'",
                "main",
            ),
        )
        .await
        .expect("missing rows should resolve as a no-op");

        assert_eq!(
            resolved.target_write_lane,
            Some(WriteLane::SingleVersion("version-a".into()))
        );
        assert!(resolved.authoritative_pre_state.is_empty());
        assert!(resolved.intended_post_state.is_empty());
    }

    #[tokio::test]
    async fn rejects_update_that_changes_identity_columns() {
        let backend = FakeBackend {
            state_rows: vec![vec![
                Value::Text("entity-1".to_string()),
                Value::Text("lix_key_value".to_string()),
                Value::Text("1".to_string()),
                Value::Text("lix".to_string()),
                Value::Text("version-a".to_string()),
                Value::Text("lix".to_string()),
                Value::Text("{\"value\":\"before\"}".to_string()),
                Value::Null,
                Value::Text("change-1".to_string()),
            ]],
        };
        let error = resolve_write_plan(
            &backend,
            &planned_write(
                "UPDATE lix_state_by_version \
                 SET file_id = 'other-file' \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'entity-1' \
                   AND version_id = 'version-a'",
                "main",
            ),
        )
        .await
        .expect_err("identity-changing update should stay off the sql2 live slice");

        assert!(error
            .message
            .contains("does not support changing 'file_id'"));
    }

    #[tokio::test]
    async fn exact_file_filter_prevents_mismatched_updates() {
        let backend = FakeBackend {
            state_rows: vec![vec![
                Value::Text("entity-1".to_string()),
                Value::Text("lix_key_value".to_string()),
                Value::Text("1".to_string()),
                Value::Text("lix".to_string()),
                Value::Text("version-a".to_string()),
                Value::Text("lix".to_string()),
                Value::Text("{\"value\":\"before\"}".to_string()),
                Value::Null,
                Value::Text("change-1".to_string()),
            ]],
        };
        let resolved = resolve_write_plan(
            &backend,
            &planned_write(
                "UPDATE lix_state_by_version \
                 SET snapshot_content = '{\"value\":\"after\"}' \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'entity-1' \
                   AND file_id = 'other-file' \
                   AND version_id = 'version-a'",
                "main",
            ),
        )
        .await
        .expect("mismatched exact filters should resolve as a no-op");

        assert!(resolved.intended_post_state.is_empty());
    }

    #[tokio::test]
    async fn rejects_non_exact_or_selectors() {
        let backend = FakeBackend {
            state_rows: vec![vec![
                Value::Text("entity-1".to_string()),
                Value::Text("lix_key_value".to_string()),
                Value::Text("1".to_string()),
                Value::Text("lix".to_string()),
                Value::Text("version-a".to_string()),
                Value::Text("lix".to_string()),
                Value::Text("{\"value\":\"before\"}".to_string()),
                Value::Null,
                Value::Text("change-1".to_string()),
            ]],
        };
        let error = resolve_write_plan(
            &backend,
            &planned_write(
                "UPDATE lix_state_by_version \
                 SET snapshot_content = '{\"value\":\"after\"}' \
                 WHERE schema_key = 'lix_key_value' \
                   AND (entity_id = 'entity-1' OR entity_id = 'entity-2') \
                   AND version_id = 'version-a'",
                "main",
            ),
        )
        .await
        .expect_err("unsupported selectors should stay off the live sql2 slice");

        assert!(error.message.contains("exact conjunctive selectors"));
    }
}
