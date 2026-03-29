use crate::filesystem::live_projection::FilesystemProjectionScope;
use crate::sql::ast::lowering::lower_statement;
use crate::sql::backend::PushdownDecision;
use crate::sql::binder::runtime::{RuntimeBindingKind, StatementBindingSource};
use crate::sql::binder::{compile_statement_binding_template_with_state, RuntimeBindingValues};
use crate::sql::catalog::SurfaceBinding;
use crate::sql::executor::contracts::effects::PlanEffects;
use crate::sql::executor::contracts::planned_statement::SchemaLiveTableRequirement;
use crate::sql::logical_plan::direct_reads::DirectPublicReadPlan;
use crate::sql::logical_plan::public_ir::{
    CommitPreconditions, FilesystemKind, PlannedStateRow, VersionScope,
};
use crate::sql::parser::placeholders::PlaceholderState;
use crate::sql::semantic_ir::semantics::domain_changes::DomainChangeBatch;
use crate::{LixError, SqlDialect, Value};
use sqlparser::ast::{Statement, TableAlias, TableFactor};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PhysicalPlan {
    PublicRead(PreparedPublicReadExecution),
    PublicWrite(PreparedPublicWriteExecution),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PreparedPublicReadExecution {
    LoweredSql(LoweredReadProgram),
    Direct(DirectPublicReadPlan),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LoweredReadProgram {
    pub(crate) statements: Vec<LoweredReadStatement>,
    pub(crate) pushdown_decision: PushdownDecision,
    pub(crate) result_columns: LoweredResultColumns,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LoweredReadStatement {
    pub(crate) shell_statement: Statement,
    pub(crate) bindings: LoweredStatementBindings,
    pub(crate) relation_render_nodes: Vec<TerminalRelationRenderNode>,
}

impl LoweredReadStatement {
    #[cfg(test)]
    pub(crate) fn render_sql(&self, dialect: SqlDialect) -> Result<String, LixError> {
        render_statement_sql(
            self.shell_statement.clone(),
            &self.relation_render_nodes,
            dialect,
        )
    }

    pub(crate) fn bind_and_render_sql(
        &self,
        params: &[Value],
        runtime_bindings: &RuntimeBindingValues,
        dialect: SqlDialect,
    ) -> Result<(String, Vec<Value>), LixError> {
        let bound_params = self.bindings.bind_params(params, runtime_bindings)?;
        let sql = render_statement_sql(
            self.shell_statement.clone(),
            &self.relation_render_nodes,
            dialect,
        )?;
        Ok((sql, bound_params))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LoweredStatementBindings {
    pub(crate) used_bindings: Vec<StatementBindingSource>,
    pub(crate) minimum_param_count: usize,
}

impl LoweredStatementBindings {
    fn bind_params(
        &self,
        params: &[Value],
        runtime_bindings: &RuntimeBindingValues,
    ) -> Result<Vec<Value>, LixError> {
        if params.len() < self.minimum_param_count {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "statement binding expected at least {} params, got {}",
                    self.minimum_param_count,
                    params.len()
                ),
            ));
        }

        Ok(self
            .used_bindings
            .iter()
            .map(|binding| match binding {
                StatementBindingSource::UserParam(source_index) => params[*source_index].clone(),
                StatementBindingSource::Runtime(RuntimeBindingKind::ActiveVersionId) => {
                    Value::Text(runtime_bindings.active_version_id.clone())
                }
                StatementBindingSource::Runtime(RuntimeBindingKind::ActiveAccountIdsJson) => {
                    Value::Text(runtime_bindings.active_account_ids_json.clone())
                }
            })
            .collect())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TerminalRelationRenderNode {
    pub(crate) placeholder_relation_name: String,
    pub(crate) alias: TableAlias,
    pub(crate) rendered_factor_sql: String,
}

pub(crate) fn compile_lowered_read_statement(
    dialect: SqlDialect,
    params_len: usize,
    statement: Statement,
    relation_render_nodes: Vec<TerminalRelationRenderNode>,
) -> Result<LoweredReadStatement, LixError> {
    let template = compile_statement_binding_template_with_state(
        &statement,
        params_len,
        dialect,
        PlaceholderState::new(),
    )?;

    Ok(LoweredReadStatement {
        shell_statement: template.statement,
        bindings: LoweredStatementBindings {
            used_bindings: template.used_bindings,
            minimum_param_count: template.minimum_param_count,
        },
        relation_render_nodes,
    })
}

fn render_statement_sql(
    statement: Statement,
    relation_render_nodes: &[TerminalRelationRenderNode],
    dialect: SqlDialect,
) -> Result<String, LixError> {
    let lowered = lower_statement(statement, dialect)?;
    let mut sql = lowered.to_string();
    for render_node in relation_render_nodes {
        sql = sql.replace(
            &placeholder_table_factor_sql(render_node),
            &render_node.rendered_factor_sql,
        );
    }
    Ok(sql)
}

pub(crate) fn placeholder_table_factor_sql(render_node: &TerminalRelationRenderNode) -> String {
    TableFactor::Table {
        name: sqlparser::ast::ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            sqlparser::ast::Ident::new(&render_node.placeholder_relation_name),
        )]),
        alias: Some(render_node.alias.clone()),
        args: None,
        with_hints: vec![],
        version: None,
        with_ordinality: false,
        partitions: vec![],
        json_path: None,
        sample: None,
        index_hints: vec![],
    }
    .to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoweredResultColumn {
    Untyped,
    Boolean,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LoweredResultColumns {
    Static(Vec<LoweredResultColumn>),
    ByColumnName(BTreeMap<String, LoweredResultColumn>),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PreparedPublicWriteExecution {
    Noop,
    Materialize(PublicWriteMaterialization),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PublicWriteMaterialization {
    pub(crate) partitions: Vec<PublicWriteExecutionPartition>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PublicWriteExecutionPartition {
    Tracked(TrackedWriteExecution),
    Untracked(UntrackedWriteExecution),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TrackedWriteExecution {
    pub(crate) schema_live_table_requirements: Vec<SchemaLiveTableRequirement>,
    pub(crate) domain_change_batch: Option<DomainChangeBatch>,
    pub(crate) create_preconditions: CommitPreconditions,
    pub(crate) semantic_effects: PlanEffects,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct UntrackedWriteExecution {
    pub(crate) intended_post_state: Vec<PlannedStateRow>,
    pub(crate) semantic_effects: PlanEffects,
    pub(crate) persist_filesystem_payloads_before_write: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FilesystemPublicSurface {
    File,
    FileByVersion,
    Directory,
    DirectoryByVersion,
}

impl FilesystemPublicSurface {
    pub(crate) fn from_public_name(public_name: &str) -> Option<Self> {
        match public_name.to_ascii_lowercase().as_str() {
            "lix_file" => Some(Self::File),
            "lix_file_by_version" => Some(Self::FileByVersion),
            "lix_directory" => Some(Self::Directory),
            "lix_directory_by_version" => Some(Self::DirectoryByVersion),
            _ => None,
        }
    }

    pub(crate) fn from_surface_binding(binding: &SurfaceBinding) -> Option<Self> {
        Self::from_public_name(&binding.descriptor.public_name)
    }

    pub(crate) fn from_filesystem_read(
        binding: &SurfaceBinding,
        kind: FilesystemKind,
        version_scope: VersionScope,
    ) -> Option<Self> {
        let surface = Self::from_surface_binding(binding)?;
        match (surface, kind, version_scope) {
            (Self::File, FilesystemKind::File, VersionScope::ActiveVersion)
            | (Self::FileByVersion, FilesystemKind::File, VersionScope::ExplicitVersion)
            | (Self::Directory, FilesystemKind::Directory, VersionScope::ActiveVersion)
            | (
                Self::DirectoryByVersion,
                FilesystemKind::Directory,
                VersionScope::ExplicitVersion,
            ) => Some(surface),
            _ => None,
        }
    }

    pub(crate) fn projection_scope(self) -> FilesystemProjectionScope {
        match self {
            Self::File | Self::Directory => FilesystemProjectionScope::ActiveVersion,
            Self::FileByVersion | Self::DirectoryByVersion => {
                FilesystemProjectionScope::ExplicitVersion
            }
        }
    }

    pub(crate) fn kind(self) -> FilesystemKind {
        match self {
            Self::File | Self::FileByVersion => FilesystemKind::File,
            Self::Directory | Self::DirectoryByVersion => FilesystemKind::Directory,
        }
    }

    pub(crate) fn needs_active_version_id(self) -> bool {
        matches!(self, Self::File | Self::Directory)
    }
}

#[cfg(test)]
mod tests {
    use super::FilesystemPublicSurface;

    #[test]
    fn filesystem_surface_names_map_to_typed_variants() {
        assert_eq!(
            FilesystemPublicSurface::from_public_name("lix_file"),
            Some(FilesystemPublicSurface::File)
        );
        assert_eq!(
            FilesystemPublicSurface::from_public_name("lix_directory_by_version"),
            Some(FilesystemPublicSurface::DirectoryByVersion)
        );
        assert_eq!(FilesystemPublicSurface::from_public_name("message"), None);
    }
}
