use crate::builtin_schema::types::LixVersionPointer;
use crate::sql2::planner::ir::{
    CommitPreconditions, ExpectedTip, IdempotencyKey, PlannedWrite, WriteLane, WriteMode,
};
use crate::{LixBackend, LixError};
use serde_json::json;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SemanticEffect {
    pub(crate) effect_key: String,
    pub(crate) target: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DomainChangeBatch {
    pub(crate) change_ids: Vec<String>,
    pub(crate) write_lane: WriteLane,
    pub(crate) writer_key: Option<String>,
    pub(crate) semantic_effects: Vec<SemanticEffect>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DomainChangeError {
    pub(crate) message: String,
}

pub(crate) fn build_domain_change_batch(
    planned_write: &PlannedWrite,
) -> Result<Option<DomainChangeBatch>, DomainChangeError> {
    if planned_write.command.mode != WriteMode::Tracked {
        return Ok(None);
    }

    let resolved = planned_write
        .resolved_write_plan
        .as_ref()
        .ok_or_else(|| DomainChangeError {
            message: "sql2 domain-change derivation requires a resolved write plan".to_string(),
        })?;
    let write_lane = resolved
        .target_write_lane
        .clone()
        .ok_or_else(|| DomainChangeError {
            message: "sql2 domain-change derivation requires exactly one tracked write lane"
                .to_string(),
        })?;

    let mut change_ids = Vec::new();
    let mut semantic_effects = Vec::new();
    for row in &resolved.intended_post_state {
        let version_descriptor = row
            .version_id
            .clone()
            .unwrap_or_else(|| "active".to_string());
        let operation_key = if row.tombstone {
            "state.delete"
        } else {
            "state.upsert"
        };
        change_ids.push(format!(
            "sql2:{}:{}:{}:{}",
            operation_key, row.schema_key, row.entity_id, version_descriptor
        ));
        semantic_effects.push(SemanticEffect {
            effect_key: operation_key.to_string(),
            target: format!(
                "{}:{}@{}",
                row.schema_key, row.entity_id, version_descriptor
            ),
        });
    }

    Ok(Some(DomainChangeBatch {
        change_ids,
        write_lane,
        writer_key: planned_write.command.execution_context.writer_key.clone(),
        semantic_effects,
    }))
}

pub(crate) async fn derive_commit_preconditions(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<Option<CommitPreconditions>, DomainChangeError> {
    if planned_write.command.mode != WriteMode::Tracked {
        return Ok(None);
    }

    let resolved = planned_write
        .resolved_write_plan
        .as_ref()
        .ok_or_else(|| DomainChangeError {
            message: "sql2 commit precondition derivation requires a resolved write plan"
                .to_string(),
        })?;
    let write_lane = resolved
        .target_write_lane
        .clone()
        .ok_or_else(|| DomainChangeError {
            message: "sql2 commit precondition derivation requires exactly one tracked write lane"
                .to_string(),
        })?;
    let version_id = version_id_for_write_lane(&write_lane, planned_write)?;
    let current_tip = load_current_tip_commit_id(backend, &version_id).await?;
    let idempotency_key = build_idempotency_key(planned_write, &write_lane, &current_tip)?;

    Ok(Some(CommitPreconditions {
        write_lane,
        expected_tip: ExpectedTip::CommitId(current_tip),
        idempotency_key,
    }))
}

fn version_id_for_write_lane(
    write_lane: &WriteLane,
    planned_write: &PlannedWrite,
) -> Result<String, DomainChangeError> {
    match write_lane {
        WriteLane::ActiveVersion => planned_write
            .command
            .execution_context
            .requested_version_id
            .clone()
            .ok_or_else(|| DomainChangeError {
                message: "sql2 commit precondition derivation requires requested_version_id for ActiveVersion writes".to_string(),
            }),
        WriteLane::SingleVersion(version_id) => Ok(version_id.clone()),
        WriteLane::GlobalAdmin => Err(DomainChangeError {
            message: "sql2 day-1 commit preconditions do not yet support GlobalAdmin writes"
                .to_string(),
        }),
    }
}

async fn load_current_tip_commit_id(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<String, DomainChangeError> {
    let sql = format!(
        "SELECT snapshot_content \
         FROM lix_internal_state_materialized_v1_lix_version_pointer \
         WHERE schema_key = 'lix_version_pointer' \
           AND entity_id = '{version_id}' \
           AND file_id = 'lix' \
           AND version_id = 'global' \
           AND is_tombstone = 0 \
           AND snapshot_content IS NOT NULL \
         LIMIT 1",
        version_id = escape_sql_string(version_id),
    );
    let result = backend
        .execute(&sql, &[])
        .await
        .map_err(domain_change_backend_error)?;
    let Some(row) = result.rows.first() else {
        return Err(DomainChangeError {
            message: format!(
                "sql2 commit precondition derivation could not find a version tip for '{}'",
                version_id
            ),
        });
    };
    let Some(crate::Value::Text(snapshot_content)) = row.first() else {
        return Err(DomainChangeError {
            message: format!(
                "sql2 commit precondition derivation expected text snapshot_content for '{}'",
                version_id
            ),
        });
    };
    let pointer: LixVersionPointer =
        serde_json::from_str(snapshot_content).map_err(|error| DomainChangeError {
            message: format!(
                "sql2 commit precondition derivation could not parse version tip snapshot for '{}': {error}",
                version_id
            ),
        })?;
    if pointer.commit_id.is_empty() {
        return Err(DomainChangeError {
            message: format!(
                "sql2 commit precondition derivation found an empty commit_id for '{}'",
                version_id
            ),
        });
    }
    Ok(pointer.commit_id)
}

fn build_idempotency_key(
    planned_write: &PlannedWrite,
    write_lane: &WriteLane,
    current_tip: &str,
) -> Result<IdempotencyKey, DomainChangeError> {
    let resolved = planned_write
        .resolved_write_plan
        .as_ref()
        .ok_or_else(|| DomainChangeError {
            message: "sql2 idempotency-key derivation requires a resolved write plan".to_string(),
        })?;
    Ok(IdempotencyKey(
        json!({
            "surface": planned_write.command.target.descriptor.public_name,
            "operation": format!("{:?}", planned_write.command.operation_kind),
            "lane": format!("{:?}", write_lane),
            "tip": current_tip,
            "writer_key": planned_write.command.execution_context.writer_key,
            "payload": format!("{:?}", planned_write.command.payload),
            "resolved_rows": format!("{:?}", resolved.intended_post_state),
        })
        .to_string(),
    ))
}

fn domain_change_backend_error(error: LixError) -> DomainChangeError {
    DomainChangeError {
        message: error.description,
    }
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::{build_domain_change_batch, derive_commit_preconditions};
    use crate::sql2::catalog::SurfaceRegistry;
    use crate::sql2::core::contracts::{BoundStatement, ExecutionContext};
    use crate::sql2::core::parser::parse_sql_script;
    use crate::sql2::planner::canonicalize::canonicalize_write;
    use crate::sql2::planner::ir::{ExpectedTip, WriteLane};
    use crate::sql2::planner::semantics::proof_engine::prove_write;
    use crate::sql2::planner::semantics::write_resolver::resolve_write_plan;
    use crate::{LixBackend, LixError, QueryResult, SqlDialect, Value};
    use async_trait::async_trait;
    use serde_json::to_string;

    #[derive(Default)]
    struct FakeBackend {
        version_tip_commit_id: Option<String>,
        expected_version_id: Option<String>,
    }

    #[async_trait(?Send)]
    impl LixBackend for FakeBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("FROM lix_internal_state_materialized_v1_lix_version_pointer") {
                if let Some(expected_version_id) = &self.expected_version_id {
                    assert!(
                        sql.contains(&format!("entity_id = '{}'", expected_version_id)),
                        "unexpected version pointer query: {sql}"
                    );
                }
                let rows = self
                    .version_tip_commit_id
                    .as_ref()
                    .map(|commit_id| {
                        vec![vec![Value::Text(
                            to_string(&crate::builtin_schema::types::LixVersionPointer {
                                id: self
                                    .expected_version_id
                                    .clone()
                                    .unwrap_or_else(|| "main".to_string()),
                                commit_id: commit_id.clone(),
                            })
                            .expect("version pointer JSON"),
                        )]]
                    })
                    .unwrap_or_default();
                return Ok(QueryResult {
                    rows,
                    columns: vec!["snapshot_content".to_string()],
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
                writer_key: Some("writer-a".to_string()),
                ..ExecutionContext::default()
            },
        );
        let canonicalized =
            canonicalize_write(bound, &registry).expect("write should canonicalize");
        let mut planned_write = prove_write(&canonicalized).expect("proofs should succeed");
        let resolved_write_plan = resolve_write_plan(&planned_write).expect("write should resolve");
        planned_write.resolved_write_plan = Some(resolved_write_plan);
        planned_write
    }

    #[test]
    fn builds_tracked_domain_change_batch_for_state_insert() {
        let planned_write = planned_write(
            "INSERT INTO lix_state_by_version (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version) \
             VALUES ('entity-1', 'lix_key_value', 'lix', 'version-a', 'lix', '{\"key\":\"hello\"}', '1')",
            "main",
        );

        let batch = build_domain_change_batch(&planned_write)
            .expect("domain changes should derive")
            .expect("tracked writes should produce a batch");

        assert_eq!(
            batch.write_lane,
            WriteLane::SingleVersion("version-a".to_string())
        );
        assert_eq!(batch.change_ids.len(), 1);
        assert_eq!(batch.semantic_effects.len(), 1);
        assert_eq!(batch.writer_key.as_deref(), Some("writer-a"));
    }

    #[tokio::test]
    async fn derives_commit_preconditions_from_version_pointer_tip() {
        let planned_write = planned_write(
            "INSERT INTO lix_state_by_version (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version) \
             VALUES ('entity-1', 'lix_key_value', 'lix', 'version-a', 'lix', '{\"key\":\"hello\"}', '1')",
            "main",
        );
        let backend = FakeBackend {
            version_tip_commit_id: Some("commit-123".to_string()),
            expected_version_id: Some("version-a".to_string()),
        };

        let preconditions = derive_commit_preconditions(&backend, &planned_write)
            .await
            .expect("preconditions should derive")
            .expect("tracked writes should require commit preconditions");

        assert_eq!(
            preconditions.write_lane,
            WriteLane::SingleVersion("version-a".to_string())
        );
        assert_eq!(
            preconditions.expected_tip,
            ExpectedTip::CommitId("commit-123".to_string())
        );
        assert!(
            preconditions.idempotency_key.0.contains("commit-123"),
            "idempotency key should reflect the expected tip"
        );
    }
}
