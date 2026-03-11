use crate::sql::public::planner::ir::{
    CommitPreconditions, ExpectedTip, IdempotencyKey, MutationPayload, PlannedStateRow,
    PlannedWrite, WriteLane, WriteMode,
};
use crate::state::commit::{
    load_committed_global_tip_commit_id, load_committed_version_tip_commit_id, ProposedDomainChange,
};
use crate::{LixBackend, LixError};
use serde_json::{json, Map, Value as JsonValue};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SemanticEffect {
    pub(crate) effect_key: String,
    pub(crate) target: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DomainChangeBatch {
    pub(crate) changes: Vec<ProposedDomainChange>,
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
    let resolved = planned_write
        .resolved_write_plan
        .as_ref()
        .ok_or_else(|| DomainChangeError {
            message: "sql2 domain-change derivation requires a resolved write plan".to_string(),
        })?;
    if resolved.execution_mode != WriteMode::Tracked {
        return Ok(None);
    }
    if resolved.intended_post_state.is_empty() {
        return Ok(None);
    }
    let write_lane = resolved
        .target_write_lane
        .clone()
        .ok_or_else(|| DomainChangeError {
            message: "sql2 domain-change derivation requires exactly one tracked write lane"
                .to_string(),
        })?;

    let mut changes = Vec::new();
    let mut semantic_effects = Vec::new();
    for row in &resolved.intended_post_state {
        let version_descriptor = row
            .version_id
            .clone()
            .unwrap_or_else(|| "active".to_string());
        let writer_key = command_writer_key(planned_write);
        let operation_key = if row.tombstone {
            "state.delete"
        } else {
            "state.upsert"
        };
        changes.push(ProposedDomainChange {
            entity_id: row.entity_id.clone(),
            schema_key: row.schema_key.clone(),
            schema_version: text_value(&row.values, "schema_version"),
            file_id: text_value(&row.values, "file_id"),
            plugin_key: text_value(&row.values, "plugin_key"),
            snapshot_content: if row.tombstone {
                None
            } else {
                serialized_value(&row.values, "snapshot_content")
            },
            metadata: serialized_value(&row.values, "metadata"),
            version_id: row.version_id.clone().ok_or_else(|| DomainChangeError {
                message: "sql2 domain-change derivation requires a concrete version_id".to_string(),
            })?,
            writer_key,
        });
        semantic_effects.push(SemanticEffect {
            effect_key: operation_key.to_string(),
            target: format!(
                "{}:{}@{}",
                row.schema_key, row.entity_id, version_descriptor
            ),
        });
    }

    Ok(Some(DomainChangeBatch {
        changes,
        write_lane,
        writer_key: planned_write.command.execution_context.writer_key.clone(),
        semantic_effects,
    }))
}

pub(crate) async fn derive_commit_preconditions(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<Option<CommitPreconditions>, DomainChangeError> {
    let resolved = planned_write
        .resolved_write_plan
        .as_ref()
        .ok_or_else(|| DomainChangeError {
            message: "sql2 commit precondition derivation requires a resolved write plan"
                .to_string(),
        })?;
    if resolved.execution_mode != WriteMode::Tracked {
        return Ok(None);
    }
    if resolved.intended_post_state.is_empty() {
        return Ok(None);
    }
    let write_lane = resolved
        .target_write_lane
        .clone()
        .ok_or_else(|| DomainChangeError {
            message: "sql2 commit precondition derivation requires exactly one tracked write lane"
                .to_string(),
        })?;
    let mut executor = backend;
    let current_tip = current_tip_for_write_lane(&mut executor, &write_lane, planned_write).await?;
    let idempotency_key = build_idempotency_key(planned_write, &write_lane, &current_tip)?;

    Ok(Some(CommitPreconditions {
        write_lane,
        expected_tip: ExpectedTip::CommitId(current_tip),
        idempotency_key,
    }))
}

async fn current_tip_for_write_lane(
    executor: &mut dyn crate::state::commit::CommitQueryExecutor,
    write_lane: &WriteLane,
    planned_write: &PlannedWrite,
) -> Result<String, DomainChangeError> {
    match write_lane {
        WriteLane::ActiveVersion => {
            let version_id = planned_write
            .command
            .execution_context
            .requested_version_id
            .clone()
            .ok_or_else(|| DomainChangeError {
                message: "sql2 commit precondition derivation requires requested_version_id for ActiveVersion writes".to_string(),
            })?;
            load_committed_version_tip_commit_id(executor, &version_id)
                .await
                .map_err(domain_change_backend_error)?
                .ok_or_else(|| DomainChangeError {
                    message: format!(
                        "sql2 commit precondition derivation could not find a version tip for '{}'",
                        version_id
                    ),
                })
        }
        WriteLane::SingleVersion(version_id) => {
            load_committed_version_tip_commit_id(executor, version_id)
                .await
                .map_err(domain_change_backend_error)?
                .ok_or_else(|| DomainChangeError {
                    message: format!(
                        "sql2 commit precondition derivation could not find a version tip for '{}'",
                        version_id
                    ),
                })
        }
        WriteLane::GlobalAdmin => load_committed_global_tip_commit_id(executor)
            .await
            .map_err(domain_change_backend_error)?
            .ok_or_else(|| DomainChangeError {
                message: "sql2 commit precondition derivation could not find the global admin tip"
                    .to_string(),
            }),
    }
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
    let summarized = json!({
        "surface": planned_write.command.target.descriptor.public_name,
        "operation": format!("{:?}", planned_write.command.operation_kind),
        "lane": format!("{:?}", write_lane),
        "tip": current_tip,
        "writer_key": planned_write.command.execution_context.writer_key,
        "payload": summarize_mutation_payload(&planned_write.command.payload),
        "resolved_rows": summarize_planned_rows(&resolved.intended_post_state),
    });
    let summarized_bytes = serde_json::to_vec(&summarized).map_err(|error| DomainChangeError {
        message: format!("sql2 idempotency-key serialization failed: {error}"),
    })?;
    let fingerprint = crate::plugin::runtime::binary_blob_hash_hex(&summarized_bytes);

    Ok(IdempotencyKey(
        json!({
            "surface": planned_write.command.target.descriptor.public_name,
            "operation": format!("{:?}", planned_write.command.operation_kind),
            "lane": format!("{:?}", write_lane),
            "tip": current_tip,
            "fingerprint": fingerprint,
        })
        .to_string(),
    ))
}

fn summarize_mutation_payload(payload: &MutationPayload) -> JsonValue {
    match payload {
        MutationPayload::FullSnapshot(values) => json!({
            "kind": "full_snapshot",
            "values": summarize_value_map(values),
        }),
        MutationPayload::BulkFullSnapshot(rows) => json!({
            "kind": "bulk_full_snapshot",
            "rows": rows.iter().map(summarize_value_map).collect::<Vec<_>>(),
        }),
        MutationPayload::Patch(values) => json!({
            "kind": "patch",
            "values": summarize_value_map(values),
        }),
        MutationPayload::Tombstone => json!({
            "kind": "tombstone",
        }),
    }
}

fn summarize_planned_rows(rows: &[PlannedStateRow]) -> JsonValue {
    JsonValue::Array(
        rows.iter()
            .map(|row| {
                json!({
                    "entity_id": row.entity_id,
                    "schema_key": row.schema_key,
                    "version_id": row.version_id,
                    "tombstone": row.tombstone,
                    "values": summarize_value_map(&row.values),
                })
            })
            .collect(),
    )
}

fn summarize_value_map(values: &std::collections::BTreeMap<String, crate::Value>) -> JsonValue {
    let mut map = Map::new();
    for (key, value) in values {
        map.insert(key.clone(), summarize_engine_value(value));
    }
    JsonValue::Object(map)
}

fn summarize_engine_value(value: &crate::Value) -> JsonValue {
    match value {
        crate::Value::Null => json!({
            "kind": "null",
        }),
        crate::Value::Text(text) => json!({
            "kind": "text",
            "sha256": crate::plugin::runtime::binary_blob_hash_hex(text.as_bytes()),
            "len": text.len(),
        }),
        crate::Value::Json(value) => {
            let encoded = value.to_string();
            json!({
                "kind": "json",
                "sha256": crate::plugin::runtime::binary_blob_hash_hex(encoded.as_bytes()),
                "len": encoded.len(),
            })
        }
        crate::Value::Blob(bytes) => json!({
            "kind": "blob",
            "sha256": crate::plugin::runtime::binary_blob_hash_hex(bytes),
            "len": bytes.len(),
        }),
        crate::Value::Integer(value) => json!({
            "kind": "integer",
            "value": value,
        }),
        crate::Value::Real(value) => json!({
            "kind": "real",
            "value": value,
        }),
        crate::Value::Boolean(value) => json!({
            "kind": "boolean",
            "value": value,
        }),
    }
}

fn domain_change_backend_error(error: LixError) -> DomainChangeError {
    DomainChangeError {
        message: error.description,
    }
}

fn text_value(
    values: &std::collections::BTreeMap<String, crate::Value>,
    key: &str,
) -> Option<String> {
    match values.get(key) {
        Some(crate::Value::Text(value)) => Some(value.clone()),
        Some(crate::Value::Integer(value)) => Some(value.to_string()),
        Some(crate::Value::Boolean(value)) => Some(value.to_string()),
        Some(crate::Value::Real(value)) => Some(value.to_string()),
        _ => None,
    }
}

fn serialized_value(
    values: &std::collections::BTreeMap<String, crate::Value>,
    key: &str,
) -> Option<String> {
    match values.get(key) {
        Some(crate::Value::Json(value)) => Some(value.to_string()),
        _ => text_value(values, key),
    }
}

fn command_writer_key(planned_write: &PlannedWrite) -> Option<String> {
    match &planned_write.command.payload {
        crate::sql::public::planner::ir::MutationPayload::FullSnapshot(payload)
        | crate::sql::public::planner::ir::MutationPayload::Patch(payload) => {
            if !payload.contains_key("writer_key") {
                return planned_write.command.execution_context.writer_key.clone();
            }

            match payload.get("writer_key") {
                Some(crate::Value::Text(value)) => Some(value.clone()),
                Some(crate::Value::Null) | None => None,
                _ => None,
            }
        }
        crate::sql::public::planner::ir::MutationPayload::BulkFullSnapshot(payloads) => {
            payloads.first().and_then(|payload| {
                if !payload.contains_key("writer_key") {
                    return planned_write.command.execution_context.writer_key.clone();
                }
                match payload.get("writer_key") {
                    Some(crate::Value::Text(value)) => Some(value.clone()),
                    Some(crate::Value::Null) | None => None,
                    _ => None,
                }
            })
        }
        crate::sql::public::planner::ir::MutationPayload::Tombstone => {
            planned_write.command.execution_context.writer_key.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{build_domain_change_batch, derive_commit_preconditions};
    use crate::sql::public::catalog::SurfaceRegistry;
    use crate::sql::public::core::contracts::{BoundStatement, ExecutionContext};
    use crate::sql::public::core::parser::parse_sql_script;
    use crate::sql::public::planner::canonicalize::canonicalize_write;
    use crate::sql::public::planner::ir::{ExpectedTip, WriteLane};
    use crate::sql::public::planner::semantics::proof_engine::prove_write;
    use crate::sql::public::planner::semantics::write_resolver::resolve_write_plan;
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
            if sql.contains("FROM lix_internal_change c")
                && sql.contains("c.schema_key = 'lix_version_pointer'")
            {
                if let Some(expected_version_id) = &self.expected_version_id {
                    assert!(
                        sql.contains(&format!("c.entity_id = '{}'", expected_version_id)),
                        "unexpected version pointer query: {sql}"
                    );
                }
                let rows = self
                    .version_tip_commit_id
                    .as_ref()
                    .map(|commit_id| {
                        vec![vec![Value::Text(
                            to_string(&crate::schema::builtin::types::LixVersionPointer {
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

    async fn planned_write_with_params(
        sql: &str,
        params: Vec<Value>,
        requested_version_id: &str,
    ) -> crate::sql::public::planner::ir::PlannedWrite {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let mut statements = parse_sql_script(sql).expect("SQL should parse");
        let statement = statements.pop().expect("single statement");
        let bound = BoundStatement::from_statement(
            statement,
            params,
            ExecutionContext {
                requested_version_id: Some(requested_version_id.to_string()),
                writer_key: Some("writer-a".to_string()),
                ..ExecutionContext::default()
            },
        );
        let canonicalized =
            canonicalize_write(bound, &registry).expect("write should canonicalize");
        let mut planned_write = prove_write(&canonicalized).expect("proofs should succeed");
        let resolved_write_plan = resolve_write_plan(&FakeBackend::default(), &planned_write)
            .await
            .expect("write should resolve");
        planned_write.resolved_write_plan = Some(resolved_write_plan);
        planned_write
    }

    async fn planned_write(
        sql: &str,
        requested_version_id: &str,
    ) -> crate::sql::public::planner::ir::PlannedWrite {
        planned_write_with_params(sql, Vec::new(), requested_version_id).await
    }

    #[tokio::test]
    async fn builds_tracked_domain_change_batch_for_state_insert() {
        let planned_write = planned_write(
            "INSERT INTO lix_state_by_version (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version) \
             VALUES ('entity-1', 'lix_key_value', 'lix', 'version-a', 'lix', '{\"key\":\"hello\"}', '1')",
            "main",
        )
        .await;

        let batch = build_domain_change_batch(&planned_write)
            .expect("domain changes should derive")
            .expect("tracked writes should produce a batch");

        assert_eq!(
            batch.write_lane,
            WriteLane::SingleVersion("version-a".to_string())
        );
        assert_eq!(batch.changes.len(), 1);
        assert_eq!(batch.semantic_effects.len(), 1);
        assert_eq!(batch.writer_key.as_deref(), Some("writer-a"));
        assert_eq!(batch.changes[0].schema_version.as_deref(), Some("1"));
        assert_eq!(batch.changes[0].file_id.as_deref(), Some("lix"));
        assert_eq!(batch.changes[0].plugin_key.as_deref(), Some("lix"));
    }

    #[tokio::test]
    async fn derives_commit_preconditions_from_version_pointer_tip() {
        let planned_write = planned_write(
            "INSERT INTO lix_state_by_version (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version) \
             VALUES ('entity-1', 'lix_key_value', 'lix', 'version-a', 'lix', '{\"key\":\"hello\"}', '1')",
            "main",
        )
        .await;
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

    #[tokio::test]
    async fn derives_compact_commit_preconditions_for_large_blob_payloads() {
        let planned_write = planned_write_with_params(
            "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
             VALUES ($1, $2, $3, 'version-a')",
            vec![
                Value::Text("plugin-archive".to_string()),
                Value::Text("/plugins/json.zip".to_string()),
                Value::Blob(vec![7; 1024 * 1024]),
            ],
            "main",
        )
        .await;
        let backend = FakeBackend {
            version_tip_commit_id: Some("commit-123".to_string()),
            expected_version_id: Some("version-a".to_string()),
        };

        let preconditions = derive_commit_preconditions(&backend, &planned_write)
            .await
            .expect("preconditions should derive")
            .expect("tracked writes should require commit preconditions");

        assert!(
            preconditions.idempotency_key.0.len() < 512,
            "idempotency key should stay compact for large blob payloads"
        );
        assert!(
            preconditions.idempotency_key.0.contains("commit-123"),
            "idempotency key should still reflect the expected tip"
        );
    }
}
