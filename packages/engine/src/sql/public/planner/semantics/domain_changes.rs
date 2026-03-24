use crate::sql::public::planner::ir::{
    CommitPreconditions, ExpectedHead, IdempotencyKey, MutationPayload, PlannedStateRow,
    PlannedWrite, ResolvedWritePartition, WriteLane, WriteMode,
};
use crate::state::commit::ProposedDomainChange;
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
) -> Result<Vec<DomainChangeBatch>, DomainChangeError> {
    let resolved = planned_write
        .resolved_write_plan
        .as_ref()
        .ok_or_else(|| DomainChangeError {
            message: "public domain-change derivation requires a resolved write plan".to_string(),
        })?;
    resolved
        .partitions
        .iter()
        .filter(|partition| partition.execution_mode == WriteMode::Tracked)
        .map(|partition| build_domain_change_batch_for_partition(planned_write, partition))
        .collect()
}

pub(crate) async fn derive_commit_preconditions(
    _backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<Vec<CommitPreconditions>, DomainChangeError> {
    let resolved = planned_write
        .resolved_write_plan
        .as_ref()
        .ok_or_else(|| DomainChangeError {
            message: "public commit precondition derivation requires a resolved write plan"
                .to_string(),
        })?;
    let mut preconditions = Vec::new();
    for (partition_index, partition) in resolved
        .partitions
        .iter()
        .enumerate()
        .filter(|(_, partition)| partition.execution_mode == WriteMode::Tracked)
    {
        let write_lane = partition
            .target_write_lane
            .clone()
            .ok_or_else(|| DomainChangeError {
                message:
                    "public commit precondition derivation requires exactly one tracked write lane"
                        .to_string(),
            })?;
        let idempotency_key =
            build_idempotency_key(planned_write, partition, partition_index, &write_lane)?;
        preconditions.push(CommitPreconditions {
            write_lane,
            expected_head: ExpectedHead::CurrentHead,
            idempotency_key,
        });
    }
    Ok(preconditions)
}

fn build_domain_change_batch_for_partition(
    planned_write: &PlannedWrite,
    partition: &ResolvedWritePartition,
) -> Result<DomainChangeBatch, DomainChangeError> {
    if partition.intended_post_state.is_empty() {
        return Err(DomainChangeError {
            message: "public domain-change derivation requires tracked rows".to_string(),
        });
    }
    let write_lane = partition
        .target_write_lane
        .clone()
        .ok_or_else(|| DomainChangeError {
            message: "public domain-change derivation requires exactly one tracked write lane"
                .to_string(),
        })?;

    let mut changes = Vec::new();
    let mut semantic_effects = Vec::new();
    for row in &partition.intended_post_state {
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
            entity_id: require_identity(row.entity_id.clone(), "public domain-change entity_id")?,
            schema_key: require_identity(
                row.schema_key.clone(),
                "public domain-change schema_key",
            )?,
            schema_version: text_value(&row.values, "schema_version")
                .map(|value| require_identity(value, "public domain-change schema_version"))
                .transpose()?,
            file_id: text_value(&row.values, "file_id")
                .map(|value| require_identity(value, "public domain-change file_id"))
                .transpose()?,
            plugin_key: text_value(&row.values, "plugin_key")
                .map(|value| require_identity(value, "public domain-change plugin_key"))
                .transpose()?,
            snapshot_content: if row.tombstone {
                None
            } else {
                serialized_value(&row.values, "snapshot_content")
            },
            metadata: serialized_value(&row.values, "metadata"),
            version_id: require_identity(
                row.version_id.clone().ok_or_else(|| DomainChangeError {
                    message: "public domain-change derivation requires a concrete version_id"
                        .to_string(),
                })?,
                "public domain-change version_id",
            )?,
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

    Ok(DomainChangeBatch {
        changes,
        write_lane,
        writer_key: planned_write.command.execution_context.writer_key.clone(),
        semantic_effects,
    })
}

fn build_idempotency_key(
    planned_write: &PlannedWrite,
    partition: &ResolvedWritePartition,
    partition_index: usize,
    write_lane: &WriteLane,
) -> Result<IdempotencyKey, DomainChangeError> {
    let summarized = json!({
        "surface": planned_write.command.target.descriptor.public_name,
        "operation": format!("{:?}", planned_write.command.operation_kind),
        "partition_index": partition_index,
        "lane": format!("{:?}", write_lane),
        "writer_key": planned_write.command.execution_context.writer_key,
        "payload": summarize_mutation_payload(&planned_write.command.payload),
        "resolved_rows": summarize_partition_rows(partition),
    });
    let summarized_bytes = serde_json::to_vec(&summarized).map_err(|error| DomainChangeError {
        message: format!("public idempotency-key serialization failed: {error}"),
    })?;
    let fingerprint = crate::plugin::runtime::binary_blob_hash_hex(&summarized_bytes);

    Ok(IdempotencyKey(
        json!({
            "surface": planned_write.command.target.descriptor.public_name,
            "operation": format!("{:?}", planned_write.command.operation_kind),
            "partition_index": partition_index,
            "lane": format!("{:?}", write_lane),
            "fingerprint": fingerprint,
        })
        .to_string(),
    ))
}

fn summarize_partition_rows(partition: &ResolvedWritePartition) -> JsonValue {
    summarize_planned_rows(&partition.intended_post_state)
}

fn summarize_mutation_payload(payload: &MutationPayload) -> JsonValue {
    match payload {
        MutationPayload::InsertRows(rows) => json!({
            "kind": "insert_rows",
            "rows": rows.iter().map(summarize_value_map).collect::<Vec<_>>(),
        }),
        MutationPayload::UpdatePatch(values) => json!({
            "kind": "update_patch",
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

fn require_identity<T>(value: impl Into<String>, context: &str) -> Result<T, DomainChangeError>
where
    T: TryFrom<String, Error = LixError>,
{
    let value = value.into();
    T::try_from(value.clone()).map_err(|_| DomainChangeError {
        message: format!(
            "{context} must be a non-empty canonical identity, got '{}'",
            value
        ),
    })
}

fn command_writer_key(planned_write: &PlannedWrite) -> Option<String> {
    match &planned_write.command.payload {
        crate::sql::public::planner::ir::MutationPayload::UpdatePatch(payload) => {
            if !payload.contains_key("writer_key") {
                return planned_write.command.execution_context.writer_key.clone();
            }

            match payload.get("writer_key") {
                Some(crate::Value::Text(value)) => Some(value.clone()),
                Some(crate::Value::Null) | None => None,
                _ => None,
            }
        }
        crate::sql::public::planner::ir::MutationPayload::InsertRows(payloads) => {
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
    use crate::sql::public::planner::ir::{ExpectedHead, WriteLane};
    use crate::sql::public::planner::semantics::write_analysis::analyze_write;
    use crate::sql::public::planner::semantics::write_resolver::resolve_write_plan;
    use crate::{LixBackend, LixError, QueryResult, SqlDialect, Value};
    use async_trait::async_trait;
    use serde_json::to_string;

    #[derive(Default)]
    struct FakeBackend {
        version_head_commit_id: Option<String>,
        expected_version_id: Option<String>,
    }

    #[async_trait(?Send)]
    impl LixBackend for FakeBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("FROM lix_internal_change c")
                && sql.contains("c.schema_key = 'lix_version_ref'")
            {
                if let Some(expected_version_id) = &self.expected_version_id {
                    assert!(
                        sql.contains(&format!("c.entity_id = '{}'", expected_version_id)),
                        "unexpected version ref query: {sql}"
                    );
                }
                let rows = self
                    .version_head_commit_id
                    .as_ref()
                    .map(|commit_id| {
                        vec![vec![Value::Text(
                            to_string(&crate::schema::builtin::types::LixVersionRef {
                                id: self
                                    .expected_version_id
                                    .clone()
                                    .unwrap_or_else(|| "main".to_string()),
                                commit_id: commit_id.clone(),
                            })
                            .expect("version ref JSON"),
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

        async fn begin_transaction(&self) -> Result<Box<dyn crate::LixBackendTransaction + '_>, LixError> {
            Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "transactions are not needed in this test backend".to_string(),
            })
        }

        async fn begin_savepoint(
            &self,
            _name: &str,
        ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, LixError> {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "begin_savepoint not supported in test backend",
            ))
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
        let mut planned_write =
            analyze_write(&canonicalized).expect("write analysis should succeed");
        let resolved_write_plan = resolve_write_plan(&FakeBackend::default(), &planned_write, None)
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

        let batches = build_domain_change_batch(&planned_write)
            .expect("domain changes should derive")
            .into_iter()
            .next()
            .expect("tracked writes should produce a batch");

        assert_eq!(
            batches.write_lane,
            WriteLane::SingleVersion("version-a".to_string())
        );
        assert_eq!(batches.changes.len(), 1);
        assert_eq!(batches.semantic_effects.len(), 1);
        assert_eq!(batches.writer_key.as_deref(), Some("writer-a"));
        assert_eq!(batches.changes[0].schema_version.as_deref(), Some("1"));
        assert_eq!(batches.changes[0].file_id.as_deref(), Some("lix"));
        assert_eq!(batches.changes[0].plugin_key.as_deref(), Some("lix"));
    }

    #[tokio::test]
    async fn derives_commit_preconditions_against_current_head() {
        let planned_write = planned_write(
            "INSERT INTO lix_state_by_version (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version) \
             VALUES ('entity-1', 'lix_key_value', 'lix', 'version-a', 'lix', '{\"key\":\"hello\"}', '1')",
            "main",
        )
        .await;
        let backend = FakeBackend {
            version_head_commit_id: Some("commit-123".to_string()),
            expected_version_id: Some("version-a".to_string()),
        };

        let preconditions = derive_commit_preconditions(&backend, &planned_write)
            .await
            .expect("preconditions should derive")
            .into_iter()
            .next()
            .expect("tracked writes should require commit preconditions");

        assert_eq!(
            preconditions.write_lane,
            WriteLane::SingleVersion("version-a".to_string())
        );
        assert_eq!(preconditions.expected_head, ExpectedHead::CurrentHead);
        assert!(
            preconditions.idempotency_key.0.contains("\"fingerprint\""),
            "idempotency key should carry a stable payload fingerprint"
        );
        assert!(
            !preconditions.idempotency_key.0.contains("commit-123"),
            "idempotency key should no longer force a pre-read of the current head"
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
            version_head_commit_id: Some("commit-123".to_string()),
            expected_version_id: Some("version-a".to_string()),
        };

        let preconditions = derive_commit_preconditions(&backend, &planned_write)
            .await
            .expect("preconditions should derive")
            .into_iter()
            .next()
            .expect("tracked writes should require commit preconditions");

        assert!(
            preconditions.idempotency_key.0.len() < 512,
            "idempotency key should stay compact for large blob payloads"
        );
        assert!(
            !preconditions.idempotency_key.0.contains("commit-123"),
            "idempotency key should stay tip-independent for large payloads"
        );
    }
}
