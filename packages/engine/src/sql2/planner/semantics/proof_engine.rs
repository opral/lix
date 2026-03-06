use crate::sql2::planner::canonicalize::CanonicalizedWrite;
use crate::sql2::planner::ir::{
    MutationPayload, PlannedWrite, SchemaProof, ScopeProof, StateSourceKind, TargetSetProof,
    WriteMode,
};
use crate::Value;
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProofError {
    pub(crate) message: String,
}

pub(crate) fn prove_write(canonicalized: &CanonicalizedWrite) -> Result<PlannedWrite, ProofError> {
    let scope_proof = prove_scope(canonicalized)?;
    if canonicalized.write_command.mode == WriteMode::Tracked
        && matches!(scope_proof, ScopeProof::Unknown | ScopeProof::Unbounded)
    {
        return Err(ProofError {
            message: "tracked sql2 writes require a bounded scope proof".to_string(),
        });
    }

    let schema_proof = prove_schema(canonicalized);
    let target_set_proof = prove_target_set(canonicalized);

    Ok(PlannedWrite {
        command: canonicalized.write_command.clone(),
        scope_proof,
        schema_proof,
        target_set_proof,
        state_source: match canonicalized.write_command.mode {
            WriteMode::Tracked => StateSourceKind::AuthoritativeCommitted,
            WriteMode::Untracked => StateSourceKind::UntrackedOverlay,
        },
        resolved_write_plan: None,
        commit_preconditions: None,
        residual_execution_predicates: canonicalized
            .write_command
            .selector
            .residual_predicates
            .clone(),
        backend_rejections: Vec::new(),
    })
}

fn prove_scope(canonicalized: &CanonicalizedWrite) -> Result<ScopeProof, ProofError> {
    match canonicalized.surface_binding.default_scope {
        crate::sql2::catalog::DefaultScopeSemantics::ActiveVersion => {
            if canonicalized
                .write_command
                .execution_context
                .requested_version_id
                .is_some()
            {
                Ok(ScopeProof::ActiveVersion)
            } else {
                Ok(ScopeProof::Unknown)
            }
        }
        crate::sql2::catalog::DefaultScopeSemantics::ExplicitVersion => {
            let Some(version_id) = payload_text_value(canonicalized, "version_id") else {
                return Err(ProofError {
                    message: format!(
                        "sql2 write proof requires explicit version_id for '{}'",
                        canonicalized.surface_binding.descriptor.public_name
                    ),
                });
            };
            Ok(ScopeProof::SingleVersion(version_id))
        }
        crate::sql2::catalog::DefaultScopeSemantics::History => Ok(ScopeProof::Unbounded),
        crate::sql2::catalog::DefaultScopeSemantics::GlobalAdmin
        | crate::sql2::catalog::DefaultScopeSemantics::WorkingChanges => Ok(ScopeProof::Unknown),
    }
}

fn prove_schema(canonicalized: &CanonicalizedWrite) -> SchemaProof {
    if let Some(schema_key) = canonicalized
        .surface_binding
        .implicit_overrides
        .fixed_schema_key
        .clone()
    {
        return SchemaProof::Exact(BTreeSet::from([schema_key]));
    }

    match payload_text_value(canonicalized, "schema_key") {
        Some(schema_key) => SchemaProof::Exact(BTreeSet::from([schema_key])),
        None => SchemaProof::Unknown,
    }
}

fn prove_target_set(canonicalized: &CanonicalizedWrite) -> Option<TargetSetProof> {
    payload_text_value(canonicalized, "entity_id")
        .map(|entity_id| TargetSetProof::Exact(BTreeSet::from([entity_id])))
        .or(Some(TargetSetProof::Unknown))
}

fn payload_text_value(canonicalized: &CanonicalizedWrite, key: &str) -> Option<String> {
    let (MutationPayload::FullSnapshot(payload) | MutationPayload::Patch(payload)) =
        &canonicalized.write_command.payload
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
    use super::prove_write;
    use crate::sql2::catalog::SurfaceRegistry;
    use crate::sql2::core::contracts::{BoundStatement, ExecutionContext};
    use crate::sql2::core::parser::parse_sql_script;
    use crate::sql2::planner::canonicalize::canonicalize_write;
    use crate::sql2::planner::ir::{SchemaProof, ScopeProof, TargetSetProof};
    use std::collections::BTreeSet;

    fn canonicalized_write(
        sql: &str,
        requested_version_id: &str,
    ) -> crate::sql2::planner::canonicalize::CanonicalizedWrite {
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
        canonicalize_write(bound, &registry).expect("write should canonicalize")
    }

    #[test]
    fn proves_active_scope_for_lix_state_insert() {
        let planned = prove_write(&canonicalized_write(
            "INSERT INTO lix_state (entity_id, schema_key, file_id, plugin_key, snapshot_content, schema_version) \
             VALUES ('entity-1', 'lix_key_value', 'lix', 'lix', '{\"key\":\"hello\"}', '1')",
            "main",
        ))
        .expect("proofs should succeed");

        assert_eq!(planned.scope_proof, ScopeProof::ActiveVersion);
        assert_eq!(
            planned.schema_proof,
            SchemaProof::Exact(BTreeSet::from(["lix_key_value".to_string()]))
        );
        assert_eq!(
            planned.target_set_proof,
            Some(TargetSetProof::Exact(BTreeSet::from([
                "entity-1".to_string()
            ])))
        );
    }

    #[test]
    fn proves_single_version_scope_for_lix_state_by_version_insert() {
        let planned = prove_write(&canonicalized_write(
            "INSERT INTO lix_state_by_version (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version) \
             VALUES ('entity-1', 'lix_key_value', 'lix', 'version-a', 'lix', '{\"key\":\"hello\"}', '1')",
            "main",
        ))
        .expect("proofs should succeed");

        assert_eq!(
            planned.scope_proof,
            ScopeProof::SingleVersion("version-a".to_string())
        );
    }
}
