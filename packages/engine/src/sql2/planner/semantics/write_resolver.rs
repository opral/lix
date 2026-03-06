use crate::sql2::planner::ir::{
    MutationPayload, PlannedStateRow, PlannedWrite, ResolvedWritePlan, RowLineage, SchemaProof,
    ScopeProof, WriteLane, WriteMode,
};
use crate::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WriteResolveError {
    pub(crate) message: String,
}

pub(crate) fn resolve_write_plan(
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let entity_id =
        payload_text_value(planned_write, "entity_id").ok_or_else(|| WriteResolveError {
            message: "sql2 write resolver requires entity_id for day-1 state inserts".to_string(),
        })?;
    let schema_key = resolved_schema_key(planned_write)?;
    let version_id = resolved_version_id(planned_write)?;
    let target_write_lane = match planned_write.command.mode {
        WriteMode::Tracked => Some(write_lane_from_scope(&planned_write.scope_proof)?),
        WriteMode::Untracked => None,
    };

    Ok(ResolvedWritePlan {
        authoritative_pre_state: Vec::new(),
        intended_post_state: vec![PlannedStateRow {
            entity_id: entity_id.clone(),
            schema_key,
            version_id,
            values: payload_map(planned_write)?,
            tombstone: matches!(planned_write.command.payload, MutationPayload::Tombstone),
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

fn payload_map(
    planned_write: &PlannedWrite,
) -> Result<std::collections::BTreeMap<String, Value>, WriteResolveError> {
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

#[cfg(test)]
mod tests {
    use super::resolve_write_plan;
    use crate::sql2::catalog::SurfaceRegistry;
    use crate::sql2::core::contracts::{BoundStatement, ExecutionContext};
    use crate::sql2::core::parser::parse_sql_script;
    use crate::sql2::planner::canonicalize::canonicalize_write;
    use crate::sql2::planner::ir::WriteLane;
    use crate::sql2::planner::semantics::proof_engine::prove_write;

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

    #[test]
    fn resolves_active_version_insert_with_active_lane() {
        let resolved = resolve_write_plan(&planned_write(
            "INSERT INTO lix_state (entity_id, schema_key, file_id, plugin_key, snapshot_content, schema_version) \
             VALUES ('entity-1', 'lix_key_value', 'lix', 'lix', '{\"key\":\"hello\"}', '1')",
            "main",
        ))
        .expect("write should resolve");

        assert_eq!(
            resolved.intended_post_state[0].version_id.as_deref(),
            Some("main")
        );
        assert_eq!(resolved.target_write_lane, Some(WriteLane::ActiveVersion));
    }

    #[test]
    fn resolves_explicit_version_insert_with_single_version_lane() {
        let resolved = resolve_write_plan(&planned_write(
            "INSERT INTO lix_state_by_version (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version) \
             VALUES ('entity-1', 'lix_key_value', 'lix', 'version-a', 'lix', '{\"key\":\"hello\"}', '1')",
            "main",
        ))
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
}
