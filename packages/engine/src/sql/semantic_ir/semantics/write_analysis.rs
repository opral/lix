use crate::contracts::surface::SurfaceOverrideValue;
use crate::sql::logical_plan::public_ir::{
    MutationPayload, PlannedWrite, SchemaProof, ScopeProof, StateSourceKind, TargetSetProof,
    WriteModeRequest,
};
use crate::sql::semantic_ir::canonicalize::{
    evaluate_public_write_expr_to_value, CanonicalizedWrite,
};
use crate::sql::semantic_ir::semantics::surface_semantics::canonical_filter_column_name;
use crate::version::GLOBAL_VERSION_ID;
use crate::Value;
use sqlparser::ast::{BinaryOperator, Expr};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WriteAnalysisError {
    pub(crate) message: String,
}

pub(crate) fn analyze_write(
    canonicalized: &CanonicalizedWrite,
) -> Result<PlannedWrite, WriteAnalysisError> {
    let scope_proof = analyze_write_scope(canonicalized)?;
    if !matches!(
        canonicalized.write_command.requested_mode,
        WriteModeRequest::ForceUntracked
    ) && matches!(scope_proof, ScopeProof::Unknown | ScopeProof::Unbounded)
    {
        return Err(WriteAnalysisError {
            message: "tracked public writes require a bounded scope analysis".to_string(),
        });
    }

    let schema_proof = derive_write_schema_facts(canonicalized);
    let target_set_proof = derive_write_target_facts(canonicalized);

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
        commit_preconditions: Vec::new(),
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

fn analyze_write_scope(
    canonicalized: &CanonicalizedWrite,
) -> Result<ScopeProof, WriteAnalysisError> {
    if let Some(scope_proof) = insert_scope_proof(canonicalized) {
        return Ok(scope_proof);
    }

    if let Some(version_id) = forced_write_version_id(canonicalized) {
        return Ok(ScopeProof::SingleVersion(version_id));
    }

    match canonicalized.surface_binding.default_scope {
        crate::contracts::surface::DefaultScopeSemantics::ActiveVersion => {
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
        crate::contracts::surface::DefaultScopeSemantics::ExplicitVersion => {
            Ok(write_scope_for_explicit_version_surface(canonicalized))
        }
        crate::contracts::surface::DefaultScopeSemantics::History => Ok(ScopeProof::Unbounded),
        crate::contracts::surface::DefaultScopeSemantics::GlobalAdmin => {
            Ok(ScopeProof::GlobalAdmin)
        }
        crate::contracts::surface::DefaultScopeSemantics::WorkingChanges => Ok(ScopeProof::Unknown),
    }
}

fn insert_scope_proof(canonicalized: &CanonicalizedWrite) -> Option<ScopeProof> {
    let MutationPayload::InsertRows(rows) = &canonicalized.write_command.payload else {
        return None;
    };

    Some(match canonicalized.surface_binding.default_scope {
        crate::contracts::surface::DefaultScopeSemantics::ActiveVersion => {
            insert_scope_for_active_version_surface(canonicalized, rows)
        }
        crate::contracts::surface::DefaultScopeSemantics::ExplicitVersion => {
            insert_scope_for_explicit_version_surface(rows)
        }
        crate::contracts::surface::DefaultScopeSemantics::History => ScopeProof::Unbounded,
        crate::contracts::surface::DefaultScopeSemantics::GlobalAdmin => ScopeProof::GlobalAdmin,
        crate::contracts::surface::DefaultScopeSemantics::WorkingChanges => ScopeProof::Unknown,
    })
}

fn insert_scope_for_active_version_surface(
    canonicalized: &CanonicalizedWrite,
    rows: &[std::collections::BTreeMap<String, Value>],
) -> ScopeProof {
    if surface_forces_global_scope(canonicalized) {
        return ScopeProof::SingleVersion(GLOBAL_VERSION_ID.to_string());
    }

    let Some(active_version_id) = canonicalized
        .write_command
        .execution_context
        .requested_version_id
        .as_ref()
    else {
        return ScopeProof::Unknown;
    };

    let mut version_ids = BTreeSet::new();
    let mut uses_local_active_version = false;

    for payload in rows {
        if bool_value_from_payload(payload, "global") == Some(true) {
            version_ids.insert(GLOBAL_VERSION_ID.to_string());
        } else {
            version_ids.insert(active_version_id.clone());
            uses_local_active_version = true;
        }
    }

    match version_ids.len() {
        0 => ScopeProof::Unknown,
        1 if uses_local_active_version && !version_ids.contains(GLOBAL_VERSION_ID) => {
            ScopeProof::ActiveVersion
        }
        1 => ScopeProof::SingleVersion(
            version_ids
                .into_iter()
                .next()
                .expect("singleton version scope"),
        ),
        _ => ScopeProof::FiniteVersionSet(version_ids),
    }
}

fn insert_scope_for_explicit_version_surface(
    rows: &[std::collections::BTreeMap<String, Value>],
) -> ScopeProof {
    let mut version_ids = BTreeSet::new();
    for payload in rows {
        if bool_value_from_payload(payload, "global") == Some(true) {
            version_ids.insert(GLOBAL_VERSION_ID.to_string());
            continue;
        }

        let Some(version_id) = payload.get("version_id").and_then(|value| match value {
            Value::Text(version_id) => Some(version_id.clone()),
            _ => None,
        }) else {
            return ScopeProof::FiniteVersionSet(BTreeSet::new());
        };
        version_ids.insert(version_id);
    }

    match version_ids.len() {
        0 => ScopeProof::FiniteVersionSet(version_ids),
        1 => ScopeProof::SingleVersion(
            version_ids
                .into_iter()
                .next()
                .expect("singleton version scope"),
        ),
        _ => ScopeProof::FiniteVersionSet(version_ids),
    }
}

fn write_scope_for_explicit_version_surface(canonicalized: &CanonicalizedWrite) -> ScopeProof {
    write_text_value(canonicalized, "version_id")
        .map(ScopeProof::SingleVersion)
        .or_else(|| {
            finite_selector_version_ids(canonicalized).map(|version_ids| match version_ids.len() {
                1 => ScopeProof::SingleVersion(
                    version_ids
                        .into_iter()
                        .next()
                        .expect("singleton version scope"),
                ),
                _ => ScopeProof::FiniteVersionSet(version_ids),
            })
        })
        .unwrap_or_else(|| ScopeProof::FiniteVersionSet(BTreeSet::new()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum VersionSelectorConstraint {
    Unconstrained,
    Finite(BTreeSet<String>),
    Unknown,
}

fn finite_selector_version_ids(canonicalized: &CanonicalizedWrite) -> Option<BTreeSet<String>> {
    let constraint = canonicalized
        .write_command
        .selector
        .residual_predicates
        .iter()
        .fold(
            VersionSelectorConstraint::Unconstrained,
            |acc, predicate| {
                combine_version_constraints_with_and(
                    acc,
                    analyze_version_selector_constraint(predicate, canonicalized),
                )
            },
        );

    match constraint {
        VersionSelectorConstraint::Finite(version_ids) => Some(version_ids),
        VersionSelectorConstraint::Unconstrained | VersionSelectorConstraint::Unknown => None,
    }
}

fn analyze_version_selector_constraint(
    expr: &Expr,
    canonicalized: &CanonicalizedWrite,
) -> VersionSelectorConstraint {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => combine_version_constraints_with_and(
            analyze_version_selector_constraint(left, canonicalized),
            analyze_version_selector_constraint(right, canonicalized),
        ),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => combine_version_constraints_with_or(
            analyze_version_selector_constraint(left, canonicalized),
            analyze_version_selector_constraint(right, canonicalized),
        ),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => version_constraint_for_binary_equality(left, right, canonicalized)
            .or_else(|| version_constraint_for_binary_equality(right, left, canonicalized))
            .unwrap_or_else(|| version_constraint_for_non_version_expr(expr)),
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            if canonical_filter_column_name(expr) != Some("version_id") {
                return version_constraint_for_non_version_expr(expr);
            }

            let mut version_ids = BTreeSet::new();
            for value_expr in list {
                let Some(version_id) = selector_expr_text_value(value_expr, canonicalized) else {
                    return VersionSelectorConstraint::Unknown;
                };
                version_ids.insert(version_id);
            }

            VersionSelectorConstraint::Finite(version_ids)
        }
        Expr::Nested(inner) => analyze_version_selector_constraint(inner, canonicalized),
        _ => version_constraint_for_non_version_expr(expr),
    }
}

fn version_constraint_for_binary_equality(
    column_expr: &Expr,
    value_expr: &Expr,
    canonicalized: &CanonicalizedWrite,
) -> Option<VersionSelectorConstraint> {
    if canonical_filter_column_name(column_expr) != Some("version_id") {
        return None;
    }

    Some(
        selector_expr_text_value(value_expr, canonicalized)
            .map(|version_id| VersionSelectorConstraint::Finite(BTreeSet::from([version_id])))
            .unwrap_or(VersionSelectorConstraint::Unknown),
    )
}

fn selector_expr_text_value(expr: &Expr, canonicalized: &CanonicalizedWrite) -> Option<String> {
    match evaluate_public_write_expr_to_value(
        expr,
        &canonicalized.write_command.bound_parameters,
        &canonicalized.write_command.execution_context,
    )
    .ok()?
    {
        Value::Text(value) => Some(value),
        Value::Integer(value) => Some(value.to_string()),
        _ => None,
    }
}

fn version_constraint_for_non_version_expr(expr: &Expr) -> VersionSelectorConstraint {
    if expr_references_version_id(expr) {
        VersionSelectorConstraint::Unknown
    } else {
        VersionSelectorConstraint::Unconstrained
    }
}

fn expr_references_version_id(expr: &Expr) -> bool {
    if canonical_filter_column_name(expr) == Some("version_id") {
        return true;
    }

    match expr {
        Expr::BinaryOp { left, right, .. } => {
            expr_references_version_id(left) || expr_references_version_id(right)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => expr_references_version_id(expr),
        Expr::InList { expr, list, .. } => {
            expr_references_version_id(expr) || list.iter().any(expr_references_version_id)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_references_version_id(expr)
                || expr_references_version_id(low)
                || expr_references_version_id(high)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            expr_references_version_id(expr) || expr_references_version_id(pattern)
        }
        _ => false,
    }
}

fn combine_version_constraints_with_and(
    left: VersionSelectorConstraint,
    right: VersionSelectorConstraint,
) -> VersionSelectorConstraint {
    match (left, right) {
        (VersionSelectorConstraint::Unknown, _) | (_, VersionSelectorConstraint::Unknown) => {
            VersionSelectorConstraint::Unknown
        }
        (VersionSelectorConstraint::Unconstrained, other)
        | (other, VersionSelectorConstraint::Unconstrained) => other,
        (
            VersionSelectorConstraint::Finite(left_ids),
            VersionSelectorConstraint::Finite(right_ids),
        ) => VersionSelectorConstraint::Finite(
            left_ids
                .intersection(&right_ids)
                .cloned()
                .collect::<BTreeSet<_>>(),
        ),
    }
}

fn combine_version_constraints_with_or(
    left: VersionSelectorConstraint,
    right: VersionSelectorConstraint,
) -> VersionSelectorConstraint {
    match (left, right) {
        (VersionSelectorConstraint::Unknown, _) | (_, VersionSelectorConstraint::Unknown) => {
            VersionSelectorConstraint::Unknown
        }
        (VersionSelectorConstraint::Unconstrained, VersionSelectorConstraint::Unconstrained) => {
            VersionSelectorConstraint::Unconstrained
        }
        (VersionSelectorConstraint::Unconstrained, _)
        | (_, VersionSelectorConstraint::Unconstrained) => VersionSelectorConstraint::Unknown,
        (
            VersionSelectorConstraint::Finite(mut left_ids),
            VersionSelectorConstraint::Finite(right_ids),
        ) => {
            left_ids.extend(right_ids);
            VersionSelectorConstraint::Finite(left_ids)
        }
    }
}

fn forced_write_version_id(canonicalized: &CanonicalizedWrite) -> Option<String> {
    if surface_forces_global_scope(canonicalized)
        || write_bool_value(canonicalized, "global") == Some(true)
    {
        Some(GLOBAL_VERSION_ID.to_string())
    } else {
        None
    }
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

fn derive_write_schema_facts(canonicalized: &CanonicalizedWrite) -> SchemaProof {
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

fn derive_write_target_facts(canonicalized: &CanonicalizedWrite) -> Option<TargetSetProof> {
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
    payload_text_value(canonicalized, key).or_else(|| selector_text_value(canonicalized, key))
}

fn write_bool_value(canonicalized: &CanonicalizedWrite, key: &str) -> Option<bool> {
    payload_bool_value(canonicalized, key).or_else(|| selector_bool_value(canonicalized, key))
}

fn payload_text_value(canonicalized: &CanonicalizedWrite, key: &str) -> Option<String> {
    match &canonicalized.write_command.payload {
        MutationPayload::InsertRows(rows) => {
            let mut values = rows.iter().filter_map(|payload| match payload.get(key) {
                Some(Value::Text(value)) => Some(value.clone()),
                _ => None,
            });
            let first = values.next()?;
            values.all(|candidate| candidate == first).then_some(first)
        }
        MutationPayload::UpdatePatch(payload) => match payload.get(key) {
            Some(Value::Text(value)) => Some(value.clone()),
            _ => None,
        },
        MutationPayload::Tombstone => None,
    }
}

fn payload_bool_value(canonicalized: &CanonicalizedWrite, key: &str) -> Option<bool> {
    match &canonicalized.write_command.payload {
        MutationPayload::InsertRows(rows) => {
            let mut values = rows
                .iter()
                .map(|payload| bool_value_from_payload(payload, key));
            let first = values.next()??;
            values
                .all(|candidate| candidate == Some(first))
                .then_some(first)
        }
        MutationPayload::UpdatePatch(payload) => bool_value_from_payload(payload, key),
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

fn selector_text_value(canonicalized: &CanonicalizedWrite, key: &str) -> Option<String> {
    match canonicalized.write_command.selector.exact_filters.get(key) {
        Some(Value::Text(value)) => Some(value.clone()),
        Some(Value::Integer(value)) => Some(value.to_string()),
        _ => None,
    }
}

fn selector_bool_value(canonicalized: &CanonicalizedWrite, key: &str) -> Option<bool> {
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

#[cfg(test)]
mod tests {
    use super::analyze_write;
    use crate::contracts::surface::SurfaceRegistry;
    use crate::sql::binder::bind_statement;
    use crate::sql::logical_plan::public_ir::{SchemaProof, ScopeProof, TargetSetProof};
    use crate::sql::semantic_ir::canonicalize::canonicalize_write;
    use crate::sql::semantic_ir::ExecutionContext;
    use std::collections::BTreeSet;

    fn canonicalized_write(
        sql: &str,
        requested_version_id: &str,
    ) -> crate::sql::semantic_ir::canonicalize::CanonicalizedWrite {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let mut statements = crate::sql::parser::parse_sql_script(sql).expect("SQL should parse");
        let statement = statements.pop().expect("single statement");
        let bound = bind_statement(
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
    fn analyzes_active_scope_for_lix_state_insert() {
        let planned = analyze_write(&canonicalized_write(
            "INSERT INTO lix_state (entity_id, schema_key, file_id, plugin_key, snapshot_content, schema_version) \
             VALUES ('entity-1', 'lix_key_value', 'lix', 'lix', '{\"key\":\"hello\"}', '1')",
            "main",
        ))
        .expect("write analysis should succeed");

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
    fn analyzes_single_version_scope_for_lix_state_by_version_insert() {
        let planned = analyze_write(&canonicalized_write(
            "INSERT INTO lix_state_by_version (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version) \
             VALUES ('entity-1', 'lix_key_value', 'lix', 'version-a', 'lix', '{\"key\":\"hello\"}', '1')",
            "main",
        ))
        .expect("write analysis should succeed");

        assert_eq!(
            planned.scope_proof,
            ScopeProof::SingleVersion("version-a".to_string())
        );
    }

    #[test]
    fn analyzes_finite_scope_for_multi_version_insert_rows() {
        let planned = analyze_write(&canonicalized_write(
            "INSERT INTO lix_state_by_version (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked) \
             VALUES \
             ('entity-tracked', 'lix_key_value', 'lix', 'version-a', 'lix', '{\"key\":\"tracked\"}', '1', false), \
             ('entity-untracked', 'lix_key_value', 'lix', 'version-b', 'lix', '{\"key\":\"untracked\"}', '1', true)",
            "main",
        ))
        .expect("write analysis should succeed");

        assert_eq!(
            planned.scope_proof,
            ScopeProof::FiniteVersionSet(BTreeSet::from([
                "version-a".to_string(),
                "version-b".to_string(),
            ]))
        );
    }

    #[test]
    fn analyzes_finite_scope_for_mixed_active_and_global_insert_rows() {
        let planned = analyze_write(&canonicalized_write(
            "INSERT INTO lix_state (entity_id, schema_key, file_id, plugin_key, snapshot_content, schema_version, global) \
             VALUES \
             ('entity-local', 'lix_key_value', 'lix', 'lix', '{\"key\":\"local\"}', '1', false), \
             ('entity-global', 'lix_key_value', 'lix', 'lix', '{\"key\":\"global\"}', '1', true)",
            "version-active",
        ))
        .expect("write analysis should succeed");

        assert_eq!(
            planned.scope_proof,
            ScopeProof::FiniteVersionSet(BTreeSet::from([
                "global".to_string(),
                "version-active".to_string(),
            ]))
        );
    }

    #[test]
    fn analyzes_scope_and_target_from_lix_state_by_version_update_predicate() {
        let planned = analyze_write(&canonicalized_write(
            "UPDATE lix_state_by_version \
             SET snapshot_content = '{\"value\":\"after\"}' \
             WHERE schema_key = 'lix_key_value' \
               AND entity_id = 'entity-1' \
               AND version_id = 'version-a'",
            "main",
        ))
        .expect("write analysis should succeed");

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

    #[test]
    fn explicit_version_surfaces_keep_bounded_scope_without_exact_version_literal() {
        let planned = analyze_write(&canonicalized_write(
            "UPDATE lix_state_by_version \
             SET snapshot_content = '{\"value\":\"after\"}' \
             WHERE schema_key = 'lix_key_value' \
               AND entity_id = 'entity-1' \
               AND version_id IN ('version-a', 'version-b')",
            "main",
        ))
        .expect("write analysis should succeed");

        assert_eq!(
            planned.scope_proof,
            ScopeProof::FiniteVersionSet(BTreeSet::from([
                "version-a".to_string(),
                "version-b".to_string(),
            ]))
        );
    }

    #[test]
    fn explicit_version_surfaces_union_or_version_predicates() {
        let planned = analyze_write(&canonicalized_write(
            "DELETE FROM lix_state_by_version \
             WHERE schema_key = 'lix_key_value' \
               AND ((version_id = 'version-a' AND entity_id = 'entity-a') \
                 OR (version_id = 'version-b' AND entity_id = 'entity-b'))",
            "main",
        ))
        .expect("write analysis should succeed");

        assert_eq!(
            planned.scope_proof,
            ScopeProof::FiniteVersionSet(BTreeSet::from([
                "version-a".to_string(),
                "version-b".to_string(),
            ]))
        );
    }
}
