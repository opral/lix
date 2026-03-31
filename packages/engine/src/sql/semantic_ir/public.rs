use super::canonicalize::{
    canonicalize_read_parts, canonicalize_write, CanonicalizeError, CanonicalizedWrite,
};
use super::hydration::hydrate_structured_public_read;
use super::internal::NormalizedInternalStatements;
use super::statement::BoundStatement;
use crate::contracts::surface::{
    SurfaceBinding, SurfaceCapability, SurfaceFamily, SurfaceRegistry, SurfaceVariant,
};
use crate::errors::schema_not_registered_error;
use crate::sql::logical_plan::public_ir::{
    BroadPublicReadStatement, CanonicalStateScan, PlannedWrite, ReadCommand, ReadContract,
    ReadPlan, StructuredPublicRead,
};
use crate::sql::logical_plan::{
    DependencySpec, DirectPublicReadPlan, PublicReadLogicalPlan, PublicWriteLogicalPlan,
};
use crate::sql::semantic_ir::semantics::dependency_spec::derive_dependency_spec_from_structured_public_read;
use crate::sql::semantic_ir::semantics::effective_state_resolver::{
    build_effective_state, EffectiveStatePlan, EffectiveStateRequest,
};
use crate::sql::semantic_ir::semantics::write_analysis::{analyze_write, WriteAnalysisError};
use crate::{LixBackend, LixError};
use sqlparser::ast::{BinaryOperator, Expr, SetExpr, Statement, TableFactor};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PublicReadSemantics {
    pub(crate) surface_bindings: Vec<SurfaceBinding>,
    pub(crate) broad_statement: Option<Box<BroadPublicReadStatement>>,
    pub(crate) structured_read: Option<StructuredPublicRead>,
    pub(crate) effective_state_request: Option<EffectiveStateRequest>,
    pub(crate) effective_state_plan: Option<EffectiveStatePlan>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StructuredPublicReadAnalysis {
    pub(crate) bound_statement: BoundStatement,
    pub(crate) semantics: PublicReadSemantics,
    pub(crate) dependency_spec: Option<DependencySpec>,
}

pub(crate) enum StructuredPublicReadPreparation {
    Prepared(StructuredPublicReadAnalysis),
    Declined(BoundStatement),
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
) -> Result<StructuredPublicReadPreparation, LixError> {
    let structured_read = match canonicalize_read_parts(&bound_statement, registry) {
        Ok(parts) => StructuredPublicRead {
            bound_parameters: bound_statement.bound_parameters.clone(),
            requested_version_id: bound_statement
                .execution_context
                .requested_version_id
                .clone(),
            surface_binding: parts.surface_binding,
            read_command: parts.read_command,
            query: parts.query,
        },
        Err(_error) => {
            match try_build_direct_state_history_structured_read(&bound_statement, registry)? {
                Some(structured_read) => structured_read,
                None => return Ok(StructuredPublicReadPreparation::Declined(bound_statement)),
            }
        }
    };

    let structured_read =
        hydrate_structured_public_read(backend, structured_read, active_version_id)
            .await?
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

    Ok(StructuredPublicReadPreparation::Prepared(
        StructuredPublicReadAnalysis {
            bound_statement,
            semantics: PublicReadSemantics {
                surface_bindings: vec![structured_read.surface_binding.clone()],
                broad_statement: None,
                structured_read: Some(structured_read),
                effective_state_request: effective_state
                    .as_ref()
                    .map(|(request, _)| request.clone()),
                effective_state_plan: effective_state.as_ref().map(|(_, plan)| plan.clone()),
            },
            dependency_spec,
        },
    ))
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

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub(crate) struct PublicWriteInvariantTrace {
    pub(crate) batch_local_checks: Vec<String>,
    pub(crate) commit_time_checks: Vec<String>,
    pub(crate) physical_checks: Vec<String>,
}

fn try_build_direct_state_history_structured_read(
    bound_statement: &BoundStatement,
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

    let scan =
        CanonicalStateScan::from_surface_binding(surface_binding.clone()).ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "state-history direct preparation could not derive a canonical state scan",
            )
        })?;

    Ok(Some(StructuredPublicRead {
        bound_parameters: bound_statement.bound_parameters.clone(),
        requested_version_id: bound_statement
            .execution_context
            .requested_version_id
            .clone(),
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
        registry
            .bind_relation_name(relation)
            .is_some_and(|binding| {
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
