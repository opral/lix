use super::canonicalize::{
    canonicalize_read, canonicalize_write, CanonicalizeError, CanonicalizedWrite,
};
use super::internal::NormalizedInternalStatements;
use super::statement::BoundStatement;
use crate::errors::schema_not_registered_error;
use crate::sql::backend::PushdownDecision;
use crate::sql::catalog::{
    SurfaceBinding, SurfaceCapability, SurfaceFamily, SurfaceRegistry, SurfaceVariant,
};
use crate::sql::logical_plan::{DependencySpec, DirectPublicReadPlan, PublicReadLogicalPlan, PublicWriteLogicalPlan};
use crate::sql::logical_plan::public_ir::{
    CanonicalStateScan, CommitPreconditions, PlannedWrite, ReadCommand, ReadContract, ReadPlan,
    ResolvedWritePlan, SchemaProof, ScopeProof, StructuredPublicRead, TargetSetProof,
    WriteCommand,
};
use crate::sql::semantic_ir::semantics::dependency_spec::derive_dependency_spec_from_structured_public_read;
use crate::sql::semantic_ir::semantics::domain_changes::DomainChangeBatch;
use crate::sql::semantic_ir::semantics::effective_state_resolver::{
    build_effective_state, EffectiveStatePlan, EffectiveStateRequest,
};
use crate::sql::semantic_ir::semantics::write_analysis::{analyze_write, WriteAnalysisError};
use crate::sql::services::state_reader::load_committed_version_head_commit_id;
use crate::{LixBackend, LixError};
use sqlparser::ast::{
    AnalyzeFormatKind, BinaryOperator, DescribeAlias, Expr, Ident, SetExpr, Statement,
    TableFactor, UtilityOption, Value as SqlValue,
};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExplainOptions {
    pub(crate) describe_alias: DescribeAlias,
    pub(crate) analyze: bool,
    pub(crate) verbose: bool,
    pub(crate) query_plan: bool,
    pub(crate) estimate: bool,
    pub(crate) format: Option<AnalyzeFormatKind>,
    pub(crate) options: Option<Vec<UtilityOption>>,
}

impl ExplainOptions {
    pub(crate) fn wrap(self, statement: SemanticStatement) -> SemanticStatement {
        SemanticStatement::Explain(ExplainedStatement {
            options: self,
            statement: Box::new(statement),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExplainedStatement {
    pub(crate) options: ExplainOptions,
    pub(crate) statement: Box<SemanticStatement>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PublicReadSemantics {
    pub(crate) bound_statement: BoundStatement,
    pub(crate) surface_bindings: Vec<SurfaceBinding>,
    pub(crate) structured_read: Option<StructuredPublicRead>,
    pub(crate) effective_state_request: Option<EffectiveStateRequest>,
    pub(crate) effective_state_plan: Option<EffectiveStatePlan>,
}

impl PublicReadSemantics {
    pub(crate) fn semantic_statement(&self, explain: Option<ExplainOptions>) -> SemanticStatement {
        let statement = SemanticStatement::PublicRead(self.clone());
        match explain {
            Some(explain) => explain.wrap(statement),
            None => statement,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StructuredPublicReadAnalysis {
    pub(crate) semantics: PublicReadSemantics,
    pub(crate) dependency_spec: Option<DependencySpec>,
}

impl StructuredPublicReadAnalysis {
    pub(crate) fn structured_read(&self) -> &StructuredPublicRead {
        self.semantics
            .structured_read
            .as_ref()
            .expect("structured public read analysis always has a structured read")
    }

    pub(crate) fn logical_plan(&self) -> PublicReadLogicalPlan {
        PublicReadLogicalPlan::Structured {
            read: self.structured_read().clone(),
            dependency_spec: self.dependency_spec.clone(),
            effective_state_request: self.semantics.effective_state_request.clone(),
            effective_state_plan: self.semantics.effective_state_plan.clone(),
        }
    }

    pub(crate) fn logical_plan_with_direct_execution(
        &self,
        direct_plan: DirectPublicReadPlan,
    ) -> PublicReadLogicalPlan {
        PublicReadLogicalPlan::DirectHistory {
            read: self.structured_read().clone(),
            direct_plan,
            dependency_spec: self.dependency_spec.clone(),
            effective_state_request: self.semantics.effective_state_request.clone(),
            effective_state_plan: self.semantics.effective_state_plan.clone(),
        }
    }
}

pub(crate) async fn prepare_structured_public_read_analysis(
    backend: &dyn LixBackend,
    bound_statement: BoundStatement,
    active_version_id: &str,
    registry: &SurfaceRegistry,
) -> Result<Option<StructuredPublicReadAnalysis>, LixError> {
    let structured_read = match canonicalize_read(bound_statement.clone(), registry) {
        Ok(canonicalized) => canonicalized.structured_read(),
        Err(_error) => match try_build_direct_state_history_structured_read(bound_statement, registry)?
        {
            Some(structured_read) => structured_read,
            None => return Ok(None),
        },
    };

    let structured_read =
        maybe_bind_active_history_root(backend, structured_read, active_version_id)
            .await
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "public read preparation could not bind active history root",
                )
            })?;
    let dependency_spec = augment_dependency_spec_for_public_read(
        registry,
        &structured_read,
        derive_dependency_spec_from_structured_public_read(&structured_read),
    );
    if structured_read.surface_binding.descriptor.surface_family == SurfaceFamily::State {
        if let Some(error) = unknown_public_state_schema_error(registry, dependency_spec.as_ref()) {
            return Err(error);
        }
    }
    let effective_state = build_effective_state(&structured_read, dependency_spec.as_ref());

    Ok(Some(StructuredPublicReadAnalysis {
        semantics: PublicReadSemantics {
            bound_statement: structured_read.bound_statement.clone(),
            surface_bindings: vec![structured_read.surface_binding.clone()],
            structured_read: Some(structured_read),
            effective_state_request: effective_state.as_ref().map(|(request, _)| request.clone()),
            effective_state_plan: effective_state.as_ref().map(|(_, plan)| plan.clone()),
        },
        dependency_spec,
    }))
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PublicWriteSemantics {
    pub(crate) canonicalized: CanonicalizedWrite,
}

impl PublicWriteSemantics {
    pub(crate) fn prepare(
        bound_statement: BoundStatement,
        registry: &SurfaceRegistry,
    ) -> Result<Self, CanonicalizeError> {
        canonicalize_write(bound_statement, registry).map(|canonicalized| Self { canonicalized })
    }

    pub(crate) fn semantic_statement(&self) -> SemanticStatement {
        SemanticStatement::PublicWrite(self.clone())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PublicWriteAnalysis {
    pub(crate) semantics: PublicWriteSemantics,
    pub(crate) planned_write: PlannedWrite,
}

impl PublicWriteAnalysis {
    pub(crate) fn logical_plan(&self) -> PublicWriteLogicalPlan {
        PublicWriteLogicalPlan {
            planned_write: self.planned_write.clone(),
        }
    }
}

pub(crate) fn analyze_public_write_semantics(
    semantics: &PublicWriteSemantics,
) -> Result<PublicWriteAnalysis, WriteAnalysisError> {
    analyze_write(&semantics.canonicalized).map(|planned_write| PublicWriteAnalysis {
        semantics: semantics.clone(),
        planned_write,
    })
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SemanticStatement {
    PublicRead(PublicReadSemantics),
    PublicWrite(PublicWriteSemantics),
    Internal(NormalizedInternalStatements),
    Explain(ExplainedStatement),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BoundPublicLeaf {
    pub(crate) public_name: String,
    pub(crate) surface_family: SurfaceFamily,
    pub(crate) surface_variant: SurfaceVariant,
    pub(crate) capability: SurfaceCapability,
    pub(crate) requires_effective_state: bool,
}

impl BoundPublicLeaf {
    pub(crate) fn from_surface_binding(binding: &SurfaceBinding) -> Self {
        Self {
            public_name: binding.descriptor.public_name.clone(),
            surface_family: binding.descriptor.surface_family,
            surface_variant: binding.descriptor.surface_variant,
            capability: binding.capability,
            requires_effective_state: matches!(
                binding.descriptor.surface_family,
                SurfaceFamily::State | SurfaceFamily::Entity
            ),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PublicWriteInvariantTrace {
    pub(crate) batch_local_checks: Vec<String>,
    pub(crate) commit_time_checks: Vec<String>,
    pub(crate) physical_checks: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct PublicExecutionDebugTrace {
    pub(crate) semantic_statement: Option<SemanticStatement>,
    pub(crate) bound_statements: Vec<BoundStatement>,
    pub(crate) surface_bindings: Vec<String>,
    pub(crate) bound_public_leaves: Vec<BoundPublicLeaf>,
    pub(crate) dependency_spec: Option<DependencySpec>,
    pub(crate) effective_state_request: Option<EffectiveStateRequest>,
    pub(crate) effective_state_plan: Option<EffectiveStatePlan>,
    pub(crate) pushdown_decision: Option<PushdownDecision>,
    pub(crate) write_command: Option<WriteCommand>,
    pub(crate) scope_proof: Option<ScopeProof>,
    pub(crate) schema_proof: Option<SchemaProof>,
    pub(crate) target_set_proof: Option<TargetSetProof>,
    pub(crate) resolved_write_plan: Option<ResolvedWritePlan>,
    pub(crate) domain_change_batches: Vec<DomainChangeBatch>,
    pub(crate) commit_preconditions: Vec<CommitPreconditions>,
    pub(crate) invariant_trace: Option<PublicWriteInvariantTrace>,
    pub(crate) write_phase_trace: Vec<String>,
    pub(crate) lowered_sql: Vec<String>,
}

fn try_build_direct_state_history_structured_read(
    bound_statement: BoundStatement,
    registry: &SurfaceRegistry,
) -> Result<Option<StructuredPublicRead>, LixError> {
    let Statement::Query(query) = &bound_statement.statement else {
        return Ok(None);
    };
    if query.with.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
    {
        return Ok(None);
    }

    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    if select.distinct.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || select.connect_by.is_some()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.from.len() != 1
        || !select.from[0].joins.is_empty()
    {
        return Ok(None);
    }

    let TableFactor::Table { name, alias, .. } = &select.from[0].relation else {
        return Ok(None);
    };
    let alias = alias.clone();
    let projection = select.projection.clone();
    let selection = select.selection.clone();
    let selection_predicates = select
        .selection
        .as_ref()
        .map(split_read_conjunctive_predicates)
        .unwrap_or_default();
    let group_by = select.group_by.clone();
    let having = select.having.clone();
    let order_by = query.order_by.clone();
    let limit_clause = query.limit_clause.clone();
    let Some(surface_binding) = registry.bind_object_name(name) else {
        return Ok(None);
    };
    if surface_binding.descriptor.public_name != "lix_state_history" {
        return Ok(None);
    }

    let scan = CanonicalStateScan::from_surface_binding(surface_binding.clone()).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "state-history direct preparation could not derive a canonical state scan",
        )
    })?;

    Ok(Some(StructuredPublicRead {
        bound_statement,
        surface_binding,
        read_command: ReadCommand {
            root: ReadPlan::scan(scan),
            contract: ReadContract::CommittedAtStart,
            requested_commit_mapping: None,
        },
        query: crate::sql::logical_plan::public_ir::NormalizedPublicReadQuery {
            source_alias: alias,
            projection,
            selection,
            selection_predicates,
            group_by,
            having,
            order_by,
            limit_clause,
        },
    }))
}

fn split_read_conjunctive_predicates(expr: &Expr) -> Vec<Expr> {
    let mut predicates = Vec::new();
    collect_read_conjunctive_predicates(expr, &mut predicates);
    predicates
}

fn collect_read_conjunctive_predicates(expr: &Expr, predicates: &mut Vec<Expr>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_read_conjunctive_predicates(left, predicates);
            collect_read_conjunctive_predicates(right, predicates);
        }
        Expr::Nested(inner) => collect_read_conjunctive_predicates(inner, predicates),
        _ => predicates.push(expr.clone()),
    }
}

async fn maybe_bind_active_history_root(
    backend: &dyn LixBackend,
    mut structured_read: StructuredPublicRead,
    active_version_id: &str,
) -> Option<StructuredPublicRead> {
    let descriptor = &structured_read.surface_binding.descriptor;
    let public_name = descriptor.public_name.as_str();
    let uses_active_history_root = descriptor.surface_variant == SurfaceVariant::History
        && matches!(
            descriptor.surface_family,
            SurfaceFamily::State | SurfaceFamily::Entity | SurfaceFamily::Filesystem
        )
        && !public_name.ends_with("_history_by_version");
    if !uses_active_history_root || structured_read_has_root_commit_predicate(&structured_read) {
        return Some(structured_read);
    }

    let root_commit_id = load_committed_version_head_commit_id(backend, active_version_id)
        .await
        .ok()??;
    let root_predicate = Expr::BinaryOp {
        left: Box::new(Expr::Identifier(Ident::new("lixcol_root_commit_id"))),
        op: BinaryOperator::Eq,
        right: Box::new(Expr::Value(
            SqlValue::SingleQuotedString(root_commit_id).into(),
        )),
    };

    structured_read.query.selection = Some(match structured_read.query.selection.take() {
        Some(existing) => Expr::BinaryOp {
            left: Box::new(existing),
            op: BinaryOperator::And,
            right: Box::new(root_predicate.clone()),
        },
        None => root_predicate.clone(),
    });
    structured_read
        .query
        .selection_predicates
        .push(root_predicate);
    Some(structured_read)
}

fn structured_read_has_root_commit_predicate(structured_read: &StructuredPublicRead) -> bool {
    structured_read
        .query
        .selection_predicates
        .iter()
        .any(expr_has_root_commit_predicate)
}

fn expr_has_root_commit_predicate(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            expr_references_root_commit(left) || expr_references_root_commit(right)
        }
        Expr::Nested(inner) => expr_has_root_commit_predicate(inner),
        _ => false,
    }
}

fn expr_references_root_commit(expr: &Expr) -> bool {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => matches!(
            parts[1].value.to_ascii_lowercase().as_str(),
            "lixcol_root_commit_id" | "root_commit_id"
        ),
        Expr::Identifier(identifier) => matches!(
            identifier.value.to_ascii_lowercase().as_str(),
            "lixcol_root_commit_id" | "root_commit_id"
        ),
        Expr::Nested(inner) => expr_references_root_commit(inner),
        _ => false,
    }
}

pub(crate) fn dependency_spec_has_unknown_schema_keys(
    registry: &SurfaceRegistry,
    dependency_spec: Option<&DependencySpec>,
) -> bool {
    let Some(dependency_spec) = dependency_spec else {
        return false;
    };
    if dependency_spec.schema_keys.is_empty() {
        return false;
    }
    let registered = registry
        .registered_schema_keys()
        .into_iter()
        .collect::<BTreeSet<_>>();
    dependency_spec
        .schema_keys
        .iter()
        .any(|schema_key| !registered.contains(schema_key))
}

pub(crate) fn unknown_public_state_schema_error(
    registry: &SurfaceRegistry,
    dependency_spec: Option<&DependencySpec>,
) -> Option<LixError> {
    if !dependency_spec_has_unknown_schema_keys(registry, dependency_spec) {
        return None;
    }
    let dependency_spec = dependency_spec?;
    let registered = registry.registered_state_surface_schema_keys();
    let available_refs = registered.iter().map(String::as_str).collect::<Vec<_>>();
    let unknown = dependency_spec.schema_keys.iter().find(|schema_key| {
        !registered
            .iter()
            .any(|registered| registered == *schema_key)
    })?;
    Some(schema_not_registered_error(unknown, &available_refs))
}

pub(crate) fn augment_dependency_spec_for_public_read(
    registry: &SurfaceRegistry,
    structured_read: &StructuredPublicRead,
    dependency_spec: Option<DependencySpec>,
) -> Option<DependencySpec> {
    let dependency_spec = dependency_spec?;
    augment_dependency_spec_for_broad_public_read(registry, Some(dependency_spec)).map(
        |mut dependency_spec| {
            let has_state_schema_keys = !dependency_spec.schema_keys.is_empty();
            if structured_read.surface_binding.descriptor.surface_family == SurfaceFamily::State
                && !has_state_schema_keys
            {
                dependency_spec.schema_keys = registry
                    .registered_state_surface_schema_keys()
                    .into_iter()
                    .collect();
            }
            dependency_spec
        },
    )
}

pub(crate) fn augment_dependency_spec_for_broad_public_read(
    registry: &SurfaceRegistry,
    dependency_spec: Option<DependencySpec>,
) -> Option<DependencySpec> {
    let mut dependency_spec = dependency_spec?;
    let references_state_like_surface = dependency_spec.relations.iter().any(|relation| {
        registry.bind_relation_name(relation).is_some_and(|binding| {
            matches!(
                binding.descriptor.surface_family,
                SurfaceFamily::State | SurfaceFamily::Entity | SurfaceFamily::Filesystem
            )
        })
    });
    let has_state_schema_keys = !dependency_spec.schema_keys.is_empty();
    if references_state_like_surface && !has_state_schema_keys {
        dependency_spec
            .schema_keys
            .extend(registry.registered_state_surface_schema_keys());
    }
    Some(dependency_spec)
}
