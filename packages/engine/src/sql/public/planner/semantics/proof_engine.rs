use crate::sql::common::placeholders::{resolve_placeholder_index, PlaceholderState};
use crate::sql::public::catalog::SurfaceOverrideValue;
use crate::sql::public::planner::canonicalize::CanonicalizedWrite;
use crate::sql::public::planner::ir::{
    MutationPayload, PlannedWrite, SchemaProof, ScopeProof, StateSourceKind, TargetSetProof,
    WriteModeRequest,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::Value;
use sqlparser::ast::{BinaryOperator, Expr, Statement, Value as SqlValue};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProofError {
    pub(crate) message: String,
}

pub(crate) fn prove_write(canonicalized: &CanonicalizedWrite) -> Result<PlannedWrite, ProofError> {
    let scope_proof = prove_scope(canonicalized)?;
    if !matches!(
        canonicalized.write_command.requested_mode,
        WriteModeRequest::ForceUntracked
    ) && matches!(scope_proof, ScopeProof::Unknown | ScopeProof::Unbounded)
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
        state_source: match canonicalized.write_command.requested_mode {
            WriteModeRequest::ForceUntracked => StateSourceKind::UntrackedOverlay,
            WriteModeRequest::Auto | WriteModeRequest::ForceTracked => {
                StateSourceKind::AuthoritativeCommitted
            }
        },
        resolved_write_plan: None,
        commit_preconditions: None,
        residual_execution_predicates: canonicalized
            .write_command
            .selector
            .residual_predicates
            .iter()
            .map(ToString::to_string)
            .collect(),
        backend_rejections: Vec::new(),
    })
}

fn prove_scope(canonicalized: &CanonicalizedWrite) -> Result<ScopeProof, ProofError> {
    if let Some(version_id) = forced_write_version_id(canonicalized) {
        return Ok(ScopeProof::SingleVersion(version_id));
    }

    match canonicalized.surface_binding.default_scope {
        crate::sql::public::catalog::DefaultScopeSemantics::ActiveVersion => {
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
        crate::sql::public::catalog::DefaultScopeSemantics::ExplicitVersion => {
            let Some(version_id) = write_text_value(canonicalized, "version_id") else {
                return Err(ProofError {
                    message: format!(
                        "sql2 write proof requires version_id for '{}'",
                        canonicalized.surface_binding.descriptor.public_name
                    ),
                });
            };
            Ok(ScopeProof::SingleVersion(version_id))
        }
        crate::sql::public::catalog::DefaultScopeSemantics::History => Ok(ScopeProof::Unbounded),
        crate::sql::public::catalog::DefaultScopeSemantics::GlobalAdmin => {
            Ok(ScopeProof::GlobalAdmin)
        }
        crate::sql::public::catalog::DefaultScopeSemantics::WorkingChanges => {
            Ok(ScopeProof::Unknown)
        }
    }
}

fn forced_write_version_id(canonicalized: &CanonicalizedWrite) -> Option<String> {
    canonicalized
        .surface_binding
        .implicit_overrides
        .fixed_version_id
        .clone()
        .or_else(|| {
            if surface_forces_global_scope(canonicalized)
                || write_bool_value(canonicalized, "global") == Some(true)
            {
                Some(GLOBAL_VERSION_ID.to_string())
            } else {
                None
            }
        })
}

fn surface_forces_global_scope(canonicalized: &CanonicalizedWrite) -> bool {
    canonicalized
        .surface_binding
        .implicit_overrides
        .predicate_overrides
        .iter()
        .any(|predicate| {
            predicate.column == "global" && predicate.value == SurfaceOverrideValue::Boolean(true)
        })
}

fn prove_schema(canonicalized: &CanonicalizedWrite) -> SchemaProof {
    if let Some(schema_key) = filesystem_write_schema_key(canonicalized) {
        return SchemaProof::Exact(BTreeSet::from([schema_key.to_string()]));
    }

    if let Some(schema_key) = canonicalized
        .surface_binding
        .implicit_overrides
        .fixed_schema_key
        .clone()
    {
        return SchemaProof::Exact(BTreeSet::from([schema_key]));
    }

    match write_text_value(canonicalized, "schema_key") {
        Some(schema_key) => SchemaProof::Exact(BTreeSet::from([schema_key])),
        None => SchemaProof::Unknown,
    }
}

fn prove_target_set(canonicalized: &CanonicalizedWrite) -> Option<TargetSetProof> {
    let target_key = match canonicalized
        .surface_binding
        .descriptor
        .public_name
        .as_str()
    {
        "lix_version"
        | "lix_file"
        | "lix_file_by_version"
        | "lix_directory"
        | "lix_directory_by_version" => "id",
        _ => "entity_id",
    };
    write_text_value(canonicalized, target_key)
        .map(|entity_id| TargetSetProof::Exact(BTreeSet::from([entity_id])))
        .or(Some(TargetSetProof::Unknown))
}

fn filesystem_write_schema_key(canonicalized: &CanonicalizedWrite) -> Option<&'static str> {
    match canonicalized
        .surface_binding
        .descriptor
        .public_name
        .as_str()
    {
        "lix_file" | "lix_file_by_version" => Some("lix_file_descriptor"),
        "lix_directory" | "lix_directory_by_version" => Some("lix_directory_descriptor"),
        _ => None,
    }
}

fn write_text_value(canonicalized: &CanonicalizedWrite, key: &str) -> Option<String> {
    payload_text_value(canonicalized, key).or_else(|| selection_text_value(canonicalized, key))
}

fn write_bool_value(canonicalized: &CanonicalizedWrite, key: &str) -> Option<bool> {
    payload_bool_value(canonicalized, key).or_else(|| selection_bool_value(canonicalized, key))
}

fn payload_text_value(canonicalized: &CanonicalizedWrite, key: &str) -> Option<String> {
    match &canonicalized.write_command.payload {
        MutationPayload::FullSnapshot(payload) | MutationPayload::Patch(payload) => {
            match payload.get(key) {
                Some(Value::Text(value)) => Some(value.clone()),
                _ => None,
            }
        }
        MutationPayload::BulkFullSnapshot(payloads) => {
            let mut values = payloads
                .iter()
                .filter_map(|payload| match payload.get(key) {
                    Some(Value::Text(value)) => Some(value.clone()),
                    _ => None,
                });
            let first = values.next()?;
            values.all(|candidate| candidate == first).then_some(first)
        }
        MutationPayload::Tombstone => None,
    }
}

fn payload_bool_value(canonicalized: &CanonicalizedWrite, key: &str) -> Option<bool> {
    match &canonicalized.write_command.payload {
        MutationPayload::FullSnapshot(payload) | MutationPayload::Patch(payload) => {
            bool_value_from_payload(payload, key)
        }
        MutationPayload::BulkFullSnapshot(payloads) => {
            let mut values = payloads
                .iter()
                .map(|payload| bool_value_from_payload(payload, key));
            let first = values.next()??;
            values
                .all(|candidate| candidate == Some(first))
                .then_some(first)
        }
        MutationPayload::Tombstone => None,
    }
}

fn bool_value_from_payload(
    payload: &std::collections::BTreeMap<String, Value>,
    key: &str,
) -> Option<bool> {
    match payload.get(key) {
        Some(Value::Boolean(value)) => Some(*value),
        Some(Value::Integer(value)) => Some(*value != 0),
        Some(Value::Text(value)) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" => Some(true),
            "0" | "false" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn selection_text_value(canonicalized: &CanonicalizedWrite, key: &str) -> Option<String> {
    if let Some(Value::Text(value)) = canonicalized.write_command.selector.exact_filters.get(key) {
        return Some(value.clone());
    }

    let selection = match &canonicalized.bound_statement.statement {
        Statement::Update(update) => update.selection.as_ref(),
        Statement::Delete(delete) => delete.selection.as_ref(),
        _ => None,
    }?;
    let mut placeholder_state = PlaceholderState::new();
    extract_string_equality(
        selection,
        key,
        &canonicalized.bound_statement.bound_parameters,
        &mut placeholder_state,
    )
}

fn selection_bool_value(canonicalized: &CanonicalizedWrite, key: &str) -> Option<bool> {
    match canonicalized.write_command.selector.exact_filters.get(key) {
        Some(Value::Boolean(value)) => return Some(*value),
        Some(Value::Integer(value)) => return Some(*value != 0),
        Some(Value::Text(value)) => {
            return match value.trim().to_ascii_lowercase().as_str() {
                "1" | "true" => Some(true),
                "0" | "false" => Some(false),
                _ => None,
            }
        }
        _ => {}
    }

    None
}

fn extract_string_equality(
    expr: &Expr,
    key: &str,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Option<String> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => extract_string_equality(left, key, params, placeholder_state)
            .or_else(|| extract_string_equality(right, key, params, placeholder_state)),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if expr_references_column(left, key) {
                expr_to_string_value(right, params, placeholder_state)
            } else if expr_references_column(right, key) {
                expr_to_string_value(left, params, placeholder_state)
            } else {
                None
            }
        }
        Expr::Nested(inner) => extract_string_equality(inner, key, params, placeholder_state),
        _ => None,
    }
}

fn expr_references_column(expr: &Expr, key: &str) -> bool {
    match expr {
        Expr::Identifier(identifier) => identifier.value.eq_ignore_ascii_case(key),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .is_some_and(|identifier| identifier.value.eq_ignore_ascii_case(key)),
        Expr::Nested(inner) => expr_references_column(inner, key),
        _ => false,
    }
}

fn expr_to_string_value(
    expr: &Expr,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Option<String> {
    match expr {
        Expr::Value(value) => match &value.value {
            SqlValue::SingleQuotedString(value)
            | SqlValue::DoubleQuotedString(value)
            | SqlValue::TripleSingleQuotedString(value)
            | SqlValue::TripleDoubleQuotedString(value) => Some(value.clone()),
            SqlValue::Placeholder(token) => {
                let index =
                    resolve_placeholder_index(token, params.len(), placeholder_state).ok()?;
                match params.get(index) {
                    Some(Value::Text(value)) => Some(value.clone()),
                    Some(Value::Integer(value)) => Some(value.to_string()),
                    _ => None,
                }
            }
            _ => None,
        },
        Expr::Nested(inner) => expr_to_string_value(inner, params, placeholder_state),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::prove_write;
    use crate::sql::public::catalog::SurfaceRegistry;
    use crate::sql::public::core::contracts::{BoundStatement, ExecutionContext};
    use crate::sql::public::core::parser::parse_sql_script;
    use crate::sql::public::planner::canonicalize::canonicalize_write;
    use crate::sql::public::planner::ir::{SchemaProof, ScopeProof, TargetSetProof};
    use std::collections::BTreeSet;

    fn canonicalized_write(
        sql: &str,
        requested_version_id: &str,
    ) -> crate::sql::public::planner::canonicalize::CanonicalizedWrite {
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

    #[test]
    fn proves_scope_and_target_from_lix_state_by_version_update_predicate() {
        let planned = prove_write(&canonicalized_write(
            "UPDATE lix_state_by_version \
             SET snapshot_content = '{\"value\":\"after\"}' \
             WHERE schema_key = 'lix_key_value' \
               AND entity_id = 'entity-1' \
               AND version_id = 'version-a'",
            "main",
        ))
        .expect("proofs should succeed");

        assert_eq!(
            planned.scope_proof,
            ScopeProof::SingleVersion("version-a".to_string())
        );
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
}
