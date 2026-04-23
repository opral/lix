use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::datasource::ViewTable;
use datafusion::logical_expr::expr_fn::{col, try_cast};
use datafusion::logical_expr::{lit, Expr, LogicalPlan};
use datafusion::prelude::SessionContext;
use futures_util::future::BoxFuture;

#[cfg(test)]
use self::sql_fragments::filesystem_file_data_sql;
use self::sql_fragments::filesystem_ranked_winner_sql;
use self::sql_fragments::{
    filesystem_directory_path_sql, filesystem_directory_view_sql,
    filesystem_file_history_blob_resolution_sql, filesystem_file_history_descriptor_resolution_sql,
    filesystem_file_history_event_candidates_sql, filesystem_file_history_events_sql,
    filesystem_file_history_path_sql, filesystem_file_history_view_sql, filesystem_file_path_sql,
    filesystem_file_view_sql, filesystem_winner_relation_name,
};
use super::udf::{lix_json_extract_boolean_expr, lix_json_extract_text_expr, register_sql2_udfs};
use crate::catalog::{SurfaceFamily, SurfaceRegistry, SurfaceVariant};
use crate::common::escape_sql_string;
use crate::LixError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Sql2FilesystemViewBaseRelation {
    FileDescriptorRows,
    DirectoryDescriptorRows,
    BinaryBlobRefRows,
    FileDescriptorHistoryRows,
    DirectoryDescriptorHistoryRows,
    BinaryBlobRefHistoryRows,
}

impl Sql2FilesystemViewBaseRelation {
    pub(crate) fn relation_name(self) -> &'static str {
        match self {
            Self::FileDescriptorRows => "lix_file_descriptor_rows",
            Self::DirectoryDescriptorRows => "lix_directory_descriptor_rows",
            Self::BinaryBlobRefRows => "lix_binary_blob_ref_rows",
            Self::FileDescriptorHistoryRows => "lix_file_descriptor_history_rows",
            Self::DirectoryDescriptorHistoryRows => "lix_directory_descriptor_history_rows",
            Self::BinaryBlobRefHistoryRows => "lix_binary_blob_ref_history_rows",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Sql2FilesystemStateBaseRelation {
    LixState,
    LixStateByVersion,
    LixStateHistory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Sql2FilesystemProjectionType {
    Text,
    Boolean,
    Integer,
    Blob,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedSql2FilesystemBaseRelationColumn {
    pub(crate) public_name: String,
    pub(crate) source_column_name: String,
    pub(crate) projection_type: Sql2FilesystemProjectionType,
    pub(crate) expression: PreparedSql2FilesystemBaseRelationExpr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PreparedSql2FilesystemBaseRelationExpr {
    StateColumn { column_name: String },
    JsonPayloadProperty { property_name: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedSql2FilesystemBaseRelationPlan {
    pub(crate) public_name: Sql2FilesystemViewBaseRelation,
    pub(crate) schema_key: String,
    pub(crate) state_base_relation: Sql2FilesystemStateBaseRelation,
    pub(crate) column_order: Vec<String>,
    pub(crate) column_plans: BTreeMap<String, PreparedSql2FilesystemBaseRelationColumn>,
}

impl PreparedSql2FilesystemBaseRelationPlan {
    pub(crate) fn projection_exprs(&self) -> Vec<Expr> {
        self.column_order
            .iter()
            .filter_map(|column_name| self.column_plans.get(column_name))
            .map(|column| {
                filesystem_base_relation_projection_expr(column).alias(column.public_name.clone())
            })
            .collect()
    }

    pub(crate) fn compiled_logical_plan(
        &self,
        ctx: &SessionContext,
        provider: Arc<dyn TableProvider>,
    ) -> Result<LogicalPlan, LixError> {
        let dataframe = ctx
            .read_table(provider)
            .map_err(datafusion_error_to_lix_error)?
            .filter(col("schema_key").eq(lit(self.schema_key.clone())))
            .map_err(datafusion_error_to_lix_error)?
            .select(self.projection_exprs())
            .map_err(datafusion_error_to_lix_error)?;
        Ok(dataframe.into_unoptimized_plan())
    }

    #[allow(dead_code)]
    pub(crate) fn compiled_view_provider(
        &self,
        ctx: &SessionContext,
        provider: Arc<dyn TableProvider>,
    ) -> Result<Arc<dyn TableProvider>, LixError> {
        Ok(view_provider_from_logical_plan(
            self.compiled_logical_plan(ctx, provider)?,
        ))
    }

    pub(crate) async fn compiled_ranked_winner_logical_plan(
        &self,
        ctx: &SessionContext,
        provider: Arc<dyn TableProvider>,
    ) -> Result<LogicalPlan, LixError> {
        let relation_name = self.public_name.relation_name();
        ctx.register_table(relation_name, provider)
            .map_err(datafusion_error_to_lix_error)?;
        let sql = filesystem_ranked_winner_sql(self.public_name);
        let dataframe = ctx.sql(&sql).await.map_err(datafusion_error_to_lix_error)?;
        Ok(dataframe.into_unoptimized_plan())
    }

    pub(crate) async fn compiled_ranked_winner_view_provider(
        &self,
        ctx: &SessionContext,
        provider: Arc<dyn TableProvider>,
    ) -> Result<Arc<dyn TableProvider>, LixError> {
        Ok(view_provider_from_logical_plan(
            self.compiled_ranked_winner_logical_plan(ctx, provider)
                .await?,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PreparedSql2FilesystemViewExpr {
    FileDescriptorColumn { column_name: String },
    DirectoryDescriptorColumn { column_name: String },
    BinaryBlobRefColumn { column_name: String },
    DerivedDirectoryPath,
    DerivedFilePath,
    DerivedFileData,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedSql2FilesystemViewColumn {
    pub(crate) public_name: String,
    pub(crate) projection_type: Sql2FilesystemProjectionType,
    pub(crate) expression: PreparedSql2FilesystemViewExpr,
    pub(crate) required_source_columns: BTreeMap<Sql2FilesystemViewBaseRelation, Vec<String>>,
    pub(crate) winner_source_relation: Option<Sql2FilesystemViewBaseRelation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedSql2FilesystemViewPlan {
    pub(crate) public_name: String,
    pub(crate) surface_variant: SurfaceVariant,
    pub(crate) base_relations: Vec<Sql2FilesystemViewBaseRelation>,
    pub(crate) base_relation_plans:
        BTreeMap<Sql2FilesystemViewBaseRelation, PreparedSql2FilesystemBaseRelationPlan>,
    pub(crate) column_order: Vec<String>,
    pub(crate) column_plans: BTreeMap<String, PreparedSql2FilesystemViewColumn>,
}

impl PreparedSql2FilesystemViewPlan {
    #[cfg(test)]
    pub(crate) fn column_plan(
        &self,
        column_name: &str,
    ) -> Option<&PreparedSql2FilesystemViewColumn> {
        self.column_plans.get(column_name)
    }

    pub(crate) async fn compiled_directory_path_logical_plan(
        &self,
        ctx: &SessionContext,
        directory_winner_provider: Arc<dyn TableProvider>,
    ) -> Result<LogicalPlan, LixError> {
        let Some(directory_relation) = self.base_relations.iter().copied().find(|relation| {
            matches!(
                relation,
                Sql2FilesystemViewBaseRelation::DirectoryDescriptorRows
                    | Sql2FilesystemViewBaseRelation::DirectoryDescriptorHistoryRows
            )
        }) else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "filesystem view '{}' is missing a directory descriptor winner relation",
                    self.public_name
                ),
            ));
        };

        let winner_relation_name = filesystem_winner_relation_name(directory_relation);
        register_named_table(ctx, winner_relation_name, directory_winner_provider)?;
        let dataframe = ctx
            .sql(&filesystem_directory_path_sql(
                directory_relation,
                winner_relation_name,
            ))
            .await
            .map_err(datafusion_error_to_lix_error)?;
        Ok(dataframe.into_unoptimized_plan())
    }

    pub(crate) async fn compiled_file_path_logical_plan(
        &self,
        ctx: &SessionContext,
        file_winner_provider: Arc<dyn TableProvider>,
        directory_winner_provider: Arc<dyn TableProvider>,
    ) -> Result<LogicalPlan, LixError> {
        let Some(file_relation) = self.base_relations.iter().copied().find(|relation| {
            matches!(
                relation,
                Sql2FilesystemViewBaseRelation::FileDescriptorRows
                    | Sql2FilesystemViewBaseRelation::FileDescriptorHistoryRows
            )
        }) else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "filesystem view '{}' is missing a file descriptor winner relation",
                    self.public_name
                ),
            ));
        };
        let Some(directory_relation) = self.base_relations.iter().copied().find(|relation| {
            matches!(
                relation,
                Sql2FilesystemViewBaseRelation::DirectoryDescriptorRows
                    | Sql2FilesystemViewBaseRelation::DirectoryDescriptorHistoryRows
            )
        }) else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "filesystem view '{}' is missing a directory descriptor winner relation",
                    self.public_name
                ),
            ));
        };

        let file_winner_relation_name = filesystem_winner_relation_name(file_relation);
        let directory_winner_relation_name = filesystem_winner_relation_name(directory_relation);
        register_named_tables(
            ctx,
            [
                (file_winner_relation_name, file_winner_provider),
                (directory_winner_relation_name, directory_winner_provider),
            ],
        )?;
        let dataframe = ctx
            .sql(&filesystem_file_path_sql(
                file_relation,
                directory_relation,
                file_winner_relation_name,
                directory_winner_relation_name,
            ))
            .await
            .map_err(datafusion_error_to_lix_error)?;
        Ok(dataframe.into_unoptimized_plan())
    }

    #[cfg(test)]
    pub(crate) async fn compiled_file_data_logical_plan(
        &self,
        ctx: &SessionContext,
        file_winner_provider: Arc<dyn TableProvider>,
        blob_winner_provider: Arc<dyn TableProvider>,
    ) -> Result<LogicalPlan, LixError> {
        let Some(file_relation) = self.base_relations.iter().copied().find(|relation| {
            matches!(
                relation,
                Sql2FilesystemViewBaseRelation::FileDescriptorRows
                    | Sql2FilesystemViewBaseRelation::FileDescriptorHistoryRows
            )
        }) else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "filesystem view '{}' is missing a file descriptor winner relation",
                    self.public_name
                ),
            ));
        };
        let Some(blob_relation) = self.base_relations.iter().copied().find(|relation| {
            matches!(
                relation,
                Sql2FilesystemViewBaseRelation::BinaryBlobRefRows
                    | Sql2FilesystemViewBaseRelation::BinaryBlobRefHistoryRows
            )
        }) else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "filesystem view '{}' is missing a blob winner relation",
                    self.public_name
                ),
            ));
        };

        let file_winner_relation_name = filesystem_winner_relation_name(file_relation);
        let blob_winner_relation_name = filesystem_winner_relation_name(blob_relation);
        register_named_tables(
            ctx,
            [
                (file_winner_relation_name, file_winner_provider),
                (blob_winner_relation_name, blob_winner_provider),
            ],
        )?;
        let dataframe = ctx
            .sql(&filesystem_file_data_sql(
                file_relation,
                blob_relation,
                file_winner_relation_name,
                blob_winner_relation_name,
            ))
            .await
            .map_err(datafusion_error_to_lix_error)?;
        Ok(dataframe.into_unoptimized_plan())
    }

    pub(crate) async fn compiled_lix_file_logical_plan(
        &self,
        ctx: &SessionContext,
        active_version_id: &str,
        file_winner_provider: Arc<dyn TableProvider>,
        directory_winner_provider: Arc<dyn TableProvider>,
        file_data_provider: Arc<dyn TableProvider>,
    ) -> Result<LogicalPlan, LixError> {
        if !self.public_name.starts_with("lix_file") {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "filesystem view '{}' is not a file surface and cannot compile a lix_file view",
                    self.public_name
                ),
            ));
        }

        let file_paths_relation_name = "__lix_file_paths";
        let file_data_relation_name = "__lix_file_data";
        register_named_view(
            ctx,
            file_paths_relation_name,
            self.compiled_file_path_logical_plan(
                ctx,
                file_winner_provider,
                directory_winner_provider,
            )
            .await?,
        )?;
        register_named_table(ctx, file_data_relation_name, file_data_provider)?;

        let file_relation = primary_descriptor_relation(&self.public_name, self.surface_variant);
        let dataframe = ctx
            .sql(&filesystem_file_view_sql(
                self,
                filesystem_winner_relation_name(file_relation),
                file_paths_relation_name,
                file_data_relation_name,
                active_version_id,
            ))
            .await
            .map_err(datafusion_error_to_lix_error)?;
        Ok(dataframe.into_unoptimized_plan())
    }

    pub(crate) async fn compiled_lix_file_view_provider(
        &self,
        ctx: &SessionContext,
        active_version_id: &str,
        file_winner_provider: Arc<dyn TableProvider>,
        directory_winner_provider: Arc<dyn TableProvider>,
        file_data_provider: Arc<dyn TableProvider>,
    ) -> Result<Arc<dyn TableProvider>, LixError> {
        Ok(view_provider_from_logical_plan(
            self.compiled_lix_file_logical_plan(
                ctx,
                active_version_id,
                file_winner_provider,
                directory_winner_provider,
                file_data_provider,
            )
            .await?,
        ))
    }

    pub(crate) async fn compiled_file_history_event_candidates_logical_plan(
        &self,
        ctx: &SessionContext,
        file_history_rows_provider: Arc<dyn TableProvider>,
        directory_history_rows_provider: Arc<dyn TableProvider>,
        blob_history_rows_provider: Arc<dyn TableProvider>,
    ) -> Result<LogicalPlan, LixError> {
        let file_history_rows_relation_name = "__lix_file_history_rows";
        let directory_history_rows_relation_name = "__lix_directory_history_rows";
        let blob_history_rows_relation_name = "__lix_blob_history_rows";
        register_named_tables(
            ctx,
            [
                (file_history_rows_relation_name, file_history_rows_provider),
                (
                    directory_history_rows_relation_name,
                    directory_history_rows_provider,
                ),
                (blob_history_rows_relation_name, blob_history_rows_provider),
            ],
        )?;
        let dataframe = ctx
            .sql(&filesystem_file_history_event_candidates_sql(
                file_history_rows_relation_name,
                directory_history_rows_relation_name,
                blob_history_rows_relation_name,
            ))
            .await
            .map_err(datafusion_error_to_lix_error)?;
        Ok(dataframe.into_unoptimized_plan())
    }

    pub(crate) async fn compiled_file_history_events_logical_plan(
        &self,
        ctx: &SessionContext,
        event_candidates_provider: Arc<dyn TableProvider>,
    ) -> Result<LogicalPlan, LixError> {
        let relation_name = "__lix_file_history_event_candidates";
        register_named_table(ctx, relation_name, event_candidates_provider)?;
        let dataframe = ctx
            .sql(&filesystem_file_history_events_sql(relation_name))
            .await
            .map_err(datafusion_error_to_lix_error)?;
        Ok(dataframe.into_unoptimized_plan())
    }

    pub(crate) async fn compiled_file_history_descriptor_resolution_logical_plan(
        &self,
        ctx: &SessionContext,
        events_provider: Arc<dyn TableProvider>,
        file_history_rows_provider: Arc<dyn TableProvider>,
    ) -> Result<LogicalPlan, LixError> {
        let events_relation_name = "__lix_file_history_events";
        let file_history_rows_relation_name = "__lix_file_history_rows";
        register_named_tables(
            ctx,
            [
                (events_relation_name, events_provider),
                (file_history_rows_relation_name, file_history_rows_provider),
            ],
        )?;
        let dataframe = ctx
            .sql(&filesystem_file_history_descriptor_resolution_sql(
                events_relation_name,
                file_history_rows_relation_name,
            ))
            .await
            .map_err(datafusion_error_to_lix_error)?;
        Ok(dataframe.into_unoptimized_plan())
    }

    pub(crate) async fn compiled_file_history_path_logical_plan(
        &self,
        ctx: &SessionContext,
        descriptor_resolution_provider: Arc<dyn TableProvider>,
        directory_history_rows_provider: Arc<dyn TableProvider>,
    ) -> Result<LogicalPlan, LixError> {
        let descriptor_resolution_relation_name = "__lix_file_history_descriptor_resolution";
        let directory_history_rows_relation_name = "__lix_directory_history_rows";
        register_named_tables(
            ctx,
            [
                (
                    descriptor_resolution_relation_name,
                    descriptor_resolution_provider,
                ),
                (
                    directory_history_rows_relation_name,
                    directory_history_rows_provider,
                ),
            ],
        )?;
        let dataframe = ctx
            .sql(&filesystem_file_history_path_sql(
                descriptor_resolution_relation_name,
                directory_history_rows_relation_name,
            ))
            .await
            .map_err(datafusion_error_to_lix_error)?;
        Ok(dataframe.into_unoptimized_plan())
    }

    pub(crate) async fn compiled_file_history_blob_resolution_logical_plan(
        &self,
        ctx: &SessionContext,
        events_provider: Arc<dyn TableProvider>,
        blob_history_rows_provider: Arc<dyn TableProvider>,
    ) -> Result<LogicalPlan, LixError> {
        let events_relation_name = "__lix_file_history_events";
        let blob_history_rows_relation_name = "__lix_blob_history_rows";
        register_named_tables(
            ctx,
            [
                (events_relation_name, events_provider),
                (blob_history_rows_relation_name, blob_history_rows_provider),
            ],
        )?;
        let dataframe = ctx
            .sql(&filesystem_file_history_blob_resolution_sql(
                events_relation_name,
                blob_history_rows_relation_name,
            ))
            .await
            .map_err(datafusion_error_to_lix_error)?;
        Ok(dataframe.into_unoptimized_plan())
    }

    pub(crate) async fn compiled_lix_file_history_view_provider(
        &self,
        ctx: &SessionContext,
        file_history_rows_provider: Arc<dyn TableProvider>,
        directory_history_rows_provider: Arc<dyn TableProvider>,
        blob_history_rows_provider: Arc<dyn TableProvider>,
    ) -> Result<Arc<dyn TableProvider>, LixError> {
        if !matches!(self.surface_variant, SurfaceVariant::History) {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "filesystem view '{}' is not a history file surface",
                    self.public_name
                ),
            ));
        }

        let event_candidates_plan = self.clone();
        let file_history_rows_for_event_candidates = Arc::clone(&file_history_rows_provider);
        let directory_history_rows_for_event_candidates =
            Arc::clone(&directory_history_rows_provider);
        let blob_history_rows_for_event_candidates = Arc::clone(&blob_history_rows_provider);
        let event_candidates_provider = compile_view_provider_in_fresh_ctx(|ctx| {
            Box::pin(async move {
                event_candidates_plan
                    .compiled_file_history_event_candidates_logical_plan(
                        ctx,
                        file_history_rows_for_event_candidates,
                        directory_history_rows_for_event_candidates,
                        blob_history_rows_for_event_candidates,
                    )
                    .await
            })
        })
        .await?;
        let events_plan = self.clone();
        let events_provider = compile_view_provider_in_fresh_ctx(|ctx| {
            Box::pin(async move {
                events_plan
                    .compiled_file_history_events_logical_plan(ctx, event_candidates_provider)
                    .await
            })
        })
        .await?;
        let descriptor_resolution_plan = self.clone();
        let events_for_descriptor_resolution = Arc::clone(&events_provider);
        let file_history_rows_for_descriptor_resolution = Arc::clone(&file_history_rows_provider);
        let descriptor_resolution_provider = compile_view_provider_in_fresh_ctx(|ctx| {
            Box::pin(async move {
                descriptor_resolution_plan
                    .compiled_file_history_descriptor_resolution_logical_plan(
                        ctx,
                        events_for_descriptor_resolution,
                        file_history_rows_for_descriptor_resolution,
                    )
                    .await
            })
        })
        .await?;
        let file_paths_relation_name = "__lix_file_history_paths";
        let file_blob_relation_name = "__lix_file_history_blob_resolution";
        let file_paths_plan = self.clone();
        let descriptor_resolution_for_paths = Arc::clone(&descriptor_resolution_provider);
        let file_paths_provider = compile_view_provider_in_fresh_ctx(|ctx| {
            Box::pin(async move {
                file_paths_plan
                    .compiled_file_history_path_logical_plan(
                        ctx,
                        descriptor_resolution_for_paths,
                        directory_history_rows_provider,
                    )
                    .await
            })
        })
        .await?;
        let blob_resolution_plan = self.clone();
        let events_for_blob_resolution = Arc::clone(&events_provider);
        let blob_resolution_provider = compile_view_provider_in_fresh_ctx(|ctx| {
            Box::pin(async move {
                blob_resolution_plan
                    .compiled_file_history_blob_resolution_logical_plan(
                        ctx,
                        events_for_blob_resolution,
                        blob_history_rows_provider,
                    )
                    .await
            })
        })
        .await?;
        register_named_tables(
            ctx,
            [
                (file_paths_relation_name, file_paths_provider),
                (file_blob_relation_name, blob_resolution_provider),
            ],
        )?;

        let descriptor_resolution_relation_name = "__lix_file_history_descriptor_resolution";
        let events_relation_name = "__lix_file_history_events";
        register_named_tables(
            ctx,
            [
                (
                    descriptor_resolution_relation_name,
                    descriptor_resolution_provider,
                ),
                (events_relation_name, events_provider),
            ],
        )?;

        let dataframe = ctx
            .sql(&filesystem_file_history_view_sql(
                self,
                events_relation_name,
                descriptor_resolution_relation_name,
                file_paths_relation_name,
                file_blob_relation_name,
            ))
            .await
            .map_err(datafusion_error_to_lix_error)?;

        Ok(view_provider_from_logical_plan(
            dataframe.into_unoptimized_plan(),
        ))
    }

    pub(crate) async fn compiled_lix_directory_logical_plan(
        &self,
        ctx: &SessionContext,
        active_version_id: &str,
        directory_winner_provider: Arc<dyn TableProvider>,
    ) -> Result<LogicalPlan, LixError> {
        if !self.public_name.starts_with("lix_directory") {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "filesystem view '{}' is not a directory surface and cannot compile a lix_directory view",
                    self.public_name
                ),
            ));
        }

        let directory_paths_relation_name = "__lix_directory_paths";
        register_named_view(
            ctx,
            directory_paths_relation_name,
            self.compiled_directory_path_logical_plan(ctx, directory_winner_provider)
                .await?,
        )?;

        let directory_relation =
            primary_descriptor_relation(&self.public_name, self.surface_variant);
        let dataframe = ctx
            .sql(&filesystem_directory_view_sql(
                self,
                filesystem_winner_relation_name(directory_relation),
                directory_paths_relation_name,
                active_version_id,
            ))
            .await
            .map_err(datafusion_error_to_lix_error)?;
        Ok(dataframe.into_unoptimized_plan())
    }

    pub(crate) async fn compiled_lix_directory_view_provider(
        &self,
        ctx: &SessionContext,
        active_version_id: &str,
        directory_winner_provider: Arc<dyn TableProvider>,
    ) -> Result<Arc<dyn TableProvider>, LixError> {
        Ok(view_provider_from_logical_plan(
            self.compiled_lix_directory_logical_plan(
                ctx,
                active_version_id,
                directory_winner_provider,
            )
            .await?,
        ))
    }

    pub(crate) async fn compiled_lix_directory_history_view_provider(
        &self,
        ctx: &SessionContext,
        active_version_id: &str,
        directory_winner_provider: Arc<dyn TableProvider>,
    ) -> Result<Arc<dyn TableProvider>, LixError> {
        if !matches!(self.surface_variant, SurfaceVariant::History) {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "filesystem view '{}' is not a history directory surface",
                    self.public_name
                ),
            ));
        }
        self.compiled_lix_directory_view_provider(ctx, active_version_id, directory_winner_provider)
            .await
    }
}

pub(crate) fn prepared_filesystem_view_plans_for_registry(
    registry: &SurfaceRegistry,
    surface_names: &[String],
) -> BTreeMap<String, PreparedSql2FilesystemViewPlan> {
    surface_names
        .iter()
        .filter_map(|surface_name| {
            let resolved = registry.bind_relation_name(surface_name)?;
            (resolved.descriptor.surface_family == SurfaceFamily::Filesystem).then(|| {
                let column_order = resolved.descriptor.visible_columns.clone();
                let base_relations = filesystem_view_base_relations(
                    resolved.descriptor.public_name.as_str(),
                    resolved.descriptor.surface_variant,
                );
                let base_relation_plans = prepared_filesystem_base_relation_plans(
                    &base_relations,
                    resolved.descriptor.surface_variant,
                );
                let column_plans = column_order
                    .iter()
                    .map(|column_name| {
                        (
                            column_name.clone(),
                            PreparedSql2FilesystemViewColumn {
                                public_name: column_name.clone(),
                                projection_type: filesystem_projection_type_for_public_column(
                                    resolved.descriptor.public_name.as_str(),
                                    resolved.descriptor.surface_variant,
                                    column_name,
                                ),
                                expression: prepared_filesystem_view_expr_for_column(
                                    resolved.descriptor.public_name.as_str(),
                                    resolved.descriptor.surface_variant,
                                    column_name,
                                ),
                                required_source_columns:
                                    required_source_columns_for_filesystem_column(
                                        resolved.descriptor.public_name.as_str(),
                                        resolved.descriptor.surface_variant,
                                        column_name,
                                    ),
                                winner_source_relation:
                                    winning_source_relation_for_filesystem_column(
                                        resolved.descriptor.public_name.as_str(),
                                        resolved.descriptor.surface_variant,
                                        column_name,
                                    ),
                            },
                        )
                    })
                    .collect::<BTreeMap<_, _>>();

                PreparedSql2FilesystemViewPlan {
                    public_name: resolved.descriptor.public_name.clone(),
                    surface_variant: resolved.descriptor.surface_variant,
                    base_relations,
                    base_relation_plans,
                    column_order,
                    column_plans,
                }
            })
        })
        .map(|plan| (plan.public_name.clone(), plan))
        .collect()
}

fn prepared_filesystem_base_relation_plans(
    base_relations: &[Sql2FilesystemViewBaseRelation],
    surface_variant: SurfaceVariant,
) -> BTreeMap<Sql2FilesystemViewBaseRelation, PreparedSql2FilesystemBaseRelationPlan> {
    base_relations
        .iter()
        .copied()
        .map(|base_relation| {
            (
                base_relation,
                PreparedSql2FilesystemBaseRelationPlan {
                    public_name: base_relation,
                    schema_key: filesystem_base_relation_schema_key(base_relation).to_string(),
                    state_base_relation: filesystem_state_base_relation_for_variant(
                        surface_variant,
                    ),
                    column_order: filesystem_base_relation_columns(base_relation)
                        .iter()
                        .map(|(public_name, _)| public_name.to_string())
                        .collect(),
                    column_plans: filesystem_base_relation_columns(base_relation)
                        .iter()
                        .map(|(public_name, source_column_name)| {
                            (
                                public_name.to_string(),
                                PreparedSql2FilesystemBaseRelationColumn {
                                    public_name: public_name.to_string(),
                                    source_column_name: source_column_name.to_string(),
                                    projection_type: filesystem_projection_type_for_base_column(
                                        base_relation,
                                        public_name,
                                    ),
                                    expression: filesystem_base_relation_expr_for_column(
                                        base_relation,
                                        public_name,
                                    ),
                                },
                            )
                        })
                        .collect(),
                },
            )
        })
        .collect()
}

fn filesystem_view_base_relations(
    public_name: &str,
    surface_variant: SurfaceVariant,
) -> Vec<Sql2FilesystemViewBaseRelation> {
    let is_file_surface = public_name.starts_with("lix_file");
    let history = matches!(surface_variant, SurfaceVariant::History);

    match (is_file_surface, history) {
        (true, false) => vec![
            Sql2FilesystemViewBaseRelation::FileDescriptorRows,
            Sql2FilesystemViewBaseRelation::DirectoryDescriptorRows,
            Sql2FilesystemViewBaseRelation::BinaryBlobRefRows,
        ],
        (true, true) => vec![
            Sql2FilesystemViewBaseRelation::FileDescriptorHistoryRows,
            Sql2FilesystemViewBaseRelation::DirectoryDescriptorHistoryRows,
            Sql2FilesystemViewBaseRelation::BinaryBlobRefHistoryRows,
        ],
        (false, false) => vec![Sql2FilesystemViewBaseRelation::DirectoryDescriptorRows],
        (false, true) => vec![Sql2FilesystemViewBaseRelation::DirectoryDescriptorHistoryRows],
    }
}

fn filesystem_state_base_relation_for_variant(
    surface_variant: SurfaceVariant,
) -> Sql2FilesystemStateBaseRelation {
    match surface_variant {
        SurfaceVariant::Default | SurfaceVariant::ByVersion => {
            Sql2FilesystemStateBaseRelation::LixStateByVersion
        }
        SurfaceVariant::History => Sql2FilesystemStateBaseRelation::LixStateHistory,
        SurfaceVariant::WorkingChanges => Sql2FilesystemStateBaseRelation::LixState,
    }
}

pub(crate) fn filesystem_base_relation_schema_key(
    base_relation: Sql2FilesystemViewBaseRelation,
) -> &'static str {
    match base_relation {
        Sql2FilesystemViewBaseRelation::FileDescriptorRows
        | Sql2FilesystemViewBaseRelation::FileDescriptorHistoryRows => "lix_file_descriptor",
        Sql2FilesystemViewBaseRelation::DirectoryDescriptorRows
        | Sql2FilesystemViewBaseRelation::DirectoryDescriptorHistoryRows => {
            "lix_directory_descriptor"
        }
        Sql2FilesystemViewBaseRelation::BinaryBlobRefRows
        | Sql2FilesystemViewBaseRelation::BinaryBlobRefHistoryRows => "lix_binary_blob_ref",
    }
}

fn filesystem_base_relation_columns(
    base_relation: Sql2FilesystemViewBaseRelation,
) -> &'static [(&'static str, &'static str)] {
    match base_relation {
        Sql2FilesystemViewBaseRelation::FileDescriptorRows => &[
            ("entity_id", "entity_id"),
            ("schema_key", "schema_key"),
            ("file_id", "file_id"),
            ("version_id", "version_id"),
            ("plugin_key", "plugin_key"),
            ("schema_version", "schema_version"),
            ("global", "global"),
            ("change_id", "change_id"),
            ("created_at", "created_at"),
            ("updated_at", "updated_at"),
            ("commit_id", "commit_id"),
            ("untracked", "untracked"),
            ("metadata", "metadata"),
            ("id", "id"),
            ("directory_id", "directory_id"),
            ("name", "name"),
            ("extension", "extension"),
            ("hidden", "hidden"),
        ],
        Sql2FilesystemViewBaseRelation::FileDescriptorHistoryRows => &[
            ("entity_id", "entity_id"),
            ("schema_key", "schema_key"),
            ("file_id", "file_id"),
            ("version_id", "version_id"),
            ("plugin_key", "plugin_key"),
            ("schema_version", "schema_version"),
            ("change_id", "change_id"),
            ("commit_id", "commit_id"),
            ("commit_created_at", "commit_created_at"),
            ("root_commit_id", "root_commit_id"),
            ("depth", "depth"),
            ("metadata", "metadata"),
            ("id", "id"),
            ("directory_id", "directory_id"),
            ("name", "name"),
            ("extension", "extension"),
            ("hidden", "hidden"),
        ],
        Sql2FilesystemViewBaseRelation::DirectoryDescriptorRows => &[
            ("entity_id", "entity_id"),
            ("schema_key", "schema_key"),
            ("file_id", "file_id"),
            ("version_id", "version_id"),
            ("plugin_key", "plugin_key"),
            ("schema_version", "schema_version"),
            ("global", "global"),
            ("change_id", "change_id"),
            ("created_at", "created_at"),
            ("updated_at", "updated_at"),
            ("commit_id", "commit_id"),
            ("untracked", "untracked"),
            ("metadata", "metadata"),
            ("id", "id"),
            ("parent_id", "parent_id"),
            ("name", "name"),
            ("hidden", "hidden"),
        ],
        Sql2FilesystemViewBaseRelation::DirectoryDescriptorHistoryRows => &[
            ("entity_id", "entity_id"),
            ("schema_key", "schema_key"),
            ("file_id", "file_id"),
            ("version_id", "version_id"),
            ("plugin_key", "plugin_key"),
            ("schema_version", "schema_version"),
            ("change_id", "change_id"),
            ("commit_id", "commit_id"),
            ("commit_created_at", "commit_created_at"),
            ("root_commit_id", "root_commit_id"),
            ("depth", "depth"),
            ("metadata", "metadata"),
            ("id", "id"),
            ("parent_id", "parent_id"),
            ("name", "name"),
            ("hidden", "hidden"),
        ],
        Sql2FilesystemViewBaseRelation::BinaryBlobRefRows => &[
            ("entity_id", "entity_id"),
            ("schema_key", "schema_key"),
            ("file_id", "file_id"),
            ("version_id", "version_id"),
            ("plugin_key", "plugin_key"),
            ("schema_version", "schema_version"),
            ("global", "global"),
            ("change_id", "change_id"),
            ("created_at", "created_at"),
            ("updated_at", "updated_at"),
            ("commit_id", "commit_id"),
            ("untracked", "untracked"),
            ("metadata", "metadata"),
            ("id", "id"),
            ("blob_hash", "blob_hash"),
            ("size_bytes", "size_bytes"),
        ],
        Sql2FilesystemViewBaseRelation::BinaryBlobRefHistoryRows => &[
            ("entity_id", "entity_id"),
            ("schema_key", "schema_key"),
            ("file_id", "file_id"),
            ("version_id", "version_id"),
            ("plugin_key", "plugin_key"),
            ("schema_version", "schema_version"),
            ("change_id", "change_id"),
            ("commit_id", "commit_id"),
            ("commit_created_at", "commit_created_at"),
            ("root_commit_id", "root_commit_id"),
            ("depth", "depth"),
            ("metadata", "metadata"),
            ("id", "id"),
            ("blob_hash", "blob_hash"),
            ("size_bytes", "size_bytes"),
        ],
    }
}

fn primary_descriptor_relation(
    public_name: &str,
    surface_variant: SurfaceVariant,
) -> Sql2FilesystemViewBaseRelation {
    let history = matches!(surface_variant, SurfaceVariant::History);
    if public_name.starts_with("lix_file") {
        if history {
            Sql2FilesystemViewBaseRelation::FileDescriptorHistoryRows
        } else {
            Sql2FilesystemViewBaseRelation::FileDescriptorRows
        }
    } else if history {
        Sql2FilesystemViewBaseRelation::DirectoryDescriptorHistoryRows
    } else {
        Sql2FilesystemViewBaseRelation::DirectoryDescriptorRows
    }
}

fn descriptor_expr_for_relation(
    relation: Sql2FilesystemViewBaseRelation,
    column_name: String,
) -> PreparedSql2FilesystemViewExpr {
    match relation {
        Sql2FilesystemViewBaseRelation::FileDescriptorRows
        | Sql2FilesystemViewBaseRelation::FileDescriptorHistoryRows => {
            PreparedSql2FilesystemViewExpr::FileDescriptorColumn { column_name }
        }
        Sql2FilesystemViewBaseRelation::DirectoryDescriptorRows
        | Sql2FilesystemViewBaseRelation::DirectoryDescriptorHistoryRows => {
            PreparedSql2FilesystemViewExpr::DirectoryDescriptorColumn { column_name }
        }
        Sql2FilesystemViewBaseRelation::BinaryBlobRefRows
        | Sql2FilesystemViewBaseRelation::BinaryBlobRefHistoryRows => {
            PreparedSql2FilesystemViewExpr::BinaryBlobRefColumn { column_name }
        }
    }
}

fn base_relation_source_column_for_public_column(
    base_relation: Sql2FilesystemViewBaseRelation,
    public_column_name: &str,
) -> Option<&str> {
    let source_column_name = public_column_name
        .strip_prefix("lixcol_")
        .unwrap_or(public_column_name);
    filesystem_base_relation_columns(base_relation)
        .iter()
        .find_map(|(public_name, source_name)| {
            (*public_name == source_column_name).then_some(*source_name)
        })
}

fn filesystem_projection_type_for_base_column(
    _base_relation: Sql2FilesystemViewBaseRelation,
    column_name: &str,
) -> Sql2FilesystemProjectionType {
    match column_name {
        "hidden" | "global" | "untracked" => Sql2FilesystemProjectionType::Boolean,
        "depth" | "size_bytes" => Sql2FilesystemProjectionType::Integer,
        _ => Sql2FilesystemProjectionType::Text,
    }
}

fn filesystem_projection_type_for_public_column(
    _public_name: &str,
    _surface_variant: SurfaceVariant,
    column_name: &str,
) -> Sql2FilesystemProjectionType {
    match column_name {
        "data" => Sql2FilesystemProjectionType::Blob,
        "hidden" | "lixcol_global" | "lixcol_untracked" => Sql2FilesystemProjectionType::Boolean,
        "lixcol_depth" => Sql2FilesystemProjectionType::Integer,
        _ => Sql2FilesystemProjectionType::Text,
    }
}

fn filesystem_base_relation_expr_for_column(
    base_relation: Sql2FilesystemViewBaseRelation,
    column_name: &str,
) -> PreparedSql2FilesystemBaseRelationExpr {
    match base_relation {
        Sql2FilesystemViewBaseRelation::FileDescriptorRows
        | Sql2FilesystemViewBaseRelation::FileDescriptorHistoryRows => match column_name {
            "id" | "directory_id" | "name" | "extension" | "hidden" => {
                PreparedSql2FilesystemBaseRelationExpr::JsonPayloadProperty {
                    property_name: column_name.to_string(),
                }
            }
            _ => PreparedSql2FilesystemBaseRelationExpr::StateColumn {
                column_name: column_name.to_string(),
            },
        },
        Sql2FilesystemViewBaseRelation::DirectoryDescriptorRows
        | Sql2FilesystemViewBaseRelation::DirectoryDescriptorHistoryRows => match column_name {
            "id" | "parent_id" | "name" | "hidden" => {
                PreparedSql2FilesystemBaseRelationExpr::JsonPayloadProperty {
                    property_name: column_name.to_string(),
                }
            }
            _ => PreparedSql2FilesystemBaseRelationExpr::StateColumn {
                column_name: column_name.to_string(),
            },
        },
        Sql2FilesystemViewBaseRelation::BinaryBlobRefRows
        | Sql2FilesystemViewBaseRelation::BinaryBlobRefHistoryRows => match column_name {
            "id" => PreparedSql2FilesystemBaseRelationExpr::StateColumn {
                column_name: "file_id".to_string(),
            },
            "blob_hash" | "size_bytes" => {
                PreparedSql2FilesystemBaseRelationExpr::JsonPayloadProperty {
                    property_name: column_name.to_string(),
                }
            }
            _ => PreparedSql2FilesystemBaseRelationExpr::StateColumn {
                column_name: column_name.to_string(),
            },
        },
    }
}

fn filesystem_base_relation_projection_expr(
    column: &PreparedSql2FilesystemBaseRelationColumn,
) -> Expr {
    match &column.expression {
        PreparedSql2FilesystemBaseRelationExpr::StateColumn { column_name } => {
            col(column_name.clone())
        }
        PreparedSql2FilesystemBaseRelationExpr::JsonPayloadProperty { property_name } => {
            let snapshot_content = col("snapshot_content");
            match column.projection_type {
                Sql2FilesystemProjectionType::Text => {
                    lix_json_extract_text_expr(snapshot_content, property_name)
                }
                Sql2FilesystemProjectionType::Boolean => {
                    lix_json_extract_boolean_expr(snapshot_content, property_name)
                }
                Sql2FilesystemProjectionType::Integer => try_cast(
                    lix_json_extract_text_expr(snapshot_content, property_name),
                    datafusion::arrow::datatypes::DataType::Int64,
                ),
                Sql2FilesystemProjectionType::Blob => col(property_name.clone()),
            }
        }
    }
}

fn datafusion_error_to_lix_error(error: datafusion::common::DataFusionError) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("sql2 DataFusion error: {error}"),
    )
}

fn new_filesystem_compile_ctx() -> SessionContext {
    let ctx = SessionContext::new();
    register_sql2_udfs(&ctx);
    ctx
}

pub(crate) fn view_provider_from_logical_plan(plan: LogicalPlan) -> Arc<dyn TableProvider> {
    Arc::new(ViewTable::new(plan, None))
}

async fn compile_view_provider_in_fresh_ctx<F>(build: F) -> Result<Arc<dyn TableProvider>, LixError>
where
    F: for<'a> FnOnce(&'a SessionContext) -> BoxFuture<'a, Result<LogicalPlan, LixError>>,
{
    let ctx = new_filesystem_compile_ctx();
    Ok(view_provider_from_logical_plan(build(&ctx).await?))
}

fn register_named_table(
    ctx: &SessionContext,
    relation_name: &str,
    provider: Arc<dyn TableProvider>,
) -> Result<(), LixError> {
    ctx.register_table(relation_name, provider)
        .map_err(datafusion_error_to_lix_error)?;
    Ok(())
}

fn register_named_tables<const N: usize>(
    ctx: &SessionContext,
    tables: [(&str, Arc<dyn TableProvider>); N],
) -> Result<(), LixError> {
    for (relation_name, provider) in tables {
        register_named_table(ctx, relation_name, provider)?;
    }
    Ok(())
}

fn register_named_view(
    ctx: &SessionContext,
    relation_name: &str,
    plan: LogicalPlan,
) -> Result<(), LixError> {
    register_named_table(ctx, relation_name, view_provider_from_logical_plan(plan))
}

mod sql_fragments {
    use super::{
        escape_sql_string, filesystem_base_relation_schema_key, PreparedSql2FilesystemViewExpr,
        PreparedSql2FilesystemViewPlan, Sql2FilesystemViewBaseRelation,
    };
    use crate::catalog::SurfaceVariant;

    pub(super) fn filesystem_ranked_winner_sql(
        base_relation: Sql2FilesystemViewBaseRelation,
    ) -> String {
        let relation_name = base_relation.relation_name();
        let schema_key = filesystem_base_relation_schema_key(base_relation);
        let partition_by = match base_relation {
            Sql2FilesystemViewBaseRelation::FileDescriptorRows
            | Sql2FilesystemViewBaseRelation::FileDescriptorHistoryRows
            | Sql2FilesystemViewBaseRelation::DirectoryDescriptorRows
            | Sql2FilesystemViewBaseRelation::DirectoryDescriptorHistoryRows => {
                "version_id, entity_id, schema_key, file_id"
            }
            Sql2FilesystemViewBaseRelation::BinaryBlobRefRows
            | Sql2FilesystemViewBaseRelation::BinaryBlobRefHistoryRows => "version_id, entity_id",
        };

        format!(
            "SELECT * FROM ( \
           SELECT \
             r.*, \
             ROW_NUMBER() OVER ( \
               PARTITION BY {partition_by} \
               ORDER BY \
                 CASE \
                   WHEN untracked = true AND global = false THEN 1 \
                   WHEN untracked = false AND global = false THEN 2 \
                   WHEN untracked = true AND global = true THEN 3 \
                   ELSE 4 \
                 END ASC, \
                 updated_at DESC, \
                 created_at DESC, \
                 COALESCE(change_id, '') DESC \
             ) AS rn \
           FROM {relation_name} r \
           WHERE r.schema_key = '{schema_key}' \
         ) ranked \
         WHERE rn = 1"
        )
    }

    pub(super) fn filesystem_winner_relation_name(
        base_relation: Sql2FilesystemViewBaseRelation,
    ) -> &'static str {
        match base_relation {
            Sql2FilesystemViewBaseRelation::FileDescriptorRows => "lix_file_descriptor_winners",
            Sql2FilesystemViewBaseRelation::DirectoryDescriptorRows => {
                "lix_directory_descriptor_winners"
            }
            Sql2FilesystemViewBaseRelation::BinaryBlobRefRows => "lix_binary_blob_ref_winners",
            Sql2FilesystemViewBaseRelation::FileDescriptorHistoryRows => {
                "lix_file_descriptor_history_winners"
            }
            Sql2FilesystemViewBaseRelation::DirectoryDescriptorHistoryRows => {
                "lix_directory_descriptor_history_winners"
            }
            Sql2FilesystemViewBaseRelation::BinaryBlobRefHistoryRows => {
                "lix_binary_blob_ref_history_winners"
            }
        }
    }

    pub(super) fn filesystem_directory_path_sql(
        directory_relation: Sql2FilesystemViewBaseRelation,
        directory_winner_relation_name: &str,
    ) -> String {
        if matches!(
            directory_relation,
            Sql2FilesystemViewBaseRelation::DirectoryDescriptorHistoryRows
        ) {
            let directory_schema_key = filesystem_base_relation_schema_key(directory_relation);
            format!(
                "WITH RECURSIVE directory_paths AS ( \
               SELECT \
                 id, \
                 version_id, \
                 root_commit_id, \
                 depth, \
                 '/' || name || '/' AS path \
               FROM {directory_winner_relation_name} \
               WHERE parent_id IS NULL \
                 AND schema_key = '{directory_schema_key}' \
               UNION ALL \
               SELECT \
                 child.id, \
                 child.version_id, \
                 child.root_commit_id, \
                 child.depth, \
                 parent.path || child.name || '/' AS path \
               FROM {directory_winner_relation_name} child \
               JOIN directory_paths parent \
                 ON parent.id = child.parent_id \
                AND parent.version_id = child.version_id \
                AND parent.root_commit_id = child.root_commit_id \
                AND parent.depth = child.depth \
               WHERE child.schema_key = '{directory_schema_key}' \
             ) \
             SELECT * FROM directory_paths"
            )
        } else {
            format!(
                "WITH RECURSIVE directory_paths AS ( \
               SELECT \
                 id, \
                 version_id, \
                 '/' || name || '/' AS path \
               FROM {directory_winner_relation_name} \
               WHERE parent_id IS NULL \
               UNION ALL \
               SELECT \
                 child.id, \
                 child.version_id, \
                 parent.path || child.name || '/' AS path \
               FROM {directory_winner_relation_name} child \
               JOIN directory_paths parent \
                 ON parent.id = child.parent_id \
                AND parent.version_id = child.version_id \
             ) \
             SELECT * FROM directory_paths"
            )
        }
    }

    pub(super) fn filesystem_file_path_sql(
        file_relation: Sql2FilesystemViewBaseRelation,
        directory_relation: Sql2FilesystemViewBaseRelation,
        file_winner_relation_name: &str,
        directory_winner_relation_name: &str,
    ) -> String {
        if matches!(
            file_relation,
            Sql2FilesystemViewBaseRelation::FileDescriptorHistoryRows
        ) || matches!(
            directory_relation,
            Sql2FilesystemViewBaseRelation::DirectoryDescriptorHistoryRows
        ) {
            let file_schema_key = filesystem_base_relation_schema_key(file_relation);
            let directory_schema_key = filesystem_base_relation_schema_key(directory_relation);
            format!(
                "WITH RECURSIVE directory_paths AS ( \
               SELECT \
                 id, \
                 version_id, \
                 '/' || name || '/' AS path \
               FROM {directory_winner_relation_name} \
               WHERE parent_id IS NULL \
                 AND schema_key = '{directory_schema_key}' \
               UNION ALL \
               SELECT \
                 child.id, \
                 child.version_id, \
                 parent.path || child.name || '/' AS path \
               FROM {directory_winner_relation_name} child \
               JOIN directory_paths parent \
                 ON parent.id = child.parent_id \
                AND parent.version_id = child.version_id \
               WHERE child.schema_key = '{directory_schema_key}' \
             ) \
             SELECT \
               f.id, \
               f.version_id, \
               CASE \
                 WHEN f.directory_id IS NULL THEN \
                   CASE \
                     WHEN f.extension IS NULL OR f.extension = '' THEN '/' || f.name \
                     ELSE '/' || f.name || '.' || f.extension \
                   END \
                 WHEN dp.path IS NULL THEN NULL \
                 ELSE \
                   CASE \
                     WHEN f.extension IS NULL OR f.extension = '' THEN dp.path || f.name \
                     ELSE dp.path || f.name || '.' || f.extension \
                   END \
               END AS path \
             FROM {file_winner_relation_name} f \
             LEFT JOIN directory_paths dp \
               ON dp.id = f.directory_id \
              AND dp.version_id = f.version_id \
             WHERE f.schema_key = '{file_schema_key}'"
            )
        } else {
            format!(
                "WITH RECURSIVE directory_paths AS ( \
               SELECT \
                 id, \
                 version_id, \
                 '/' || name || '/' AS path \
               FROM {directory_winner_relation_name} \
               WHERE parent_id IS NULL \
               UNION ALL \
               SELECT \
                 child.id, \
                 child.version_id, \
                 parent.path || child.name || '/' AS path \
               FROM {directory_winner_relation_name} child \
               JOIN directory_paths parent \
                 ON parent.id = child.parent_id \
                AND parent.version_id = child.version_id \
             ) \
             SELECT \
               f.id, \
               f.version_id, \
               CASE \
                 WHEN f.directory_id IS NULL THEN \
                   CASE \
                     WHEN f.extension IS NULL OR f.extension = '' THEN '/' || f.name \
                     ELSE '/' || f.name || '.' || f.extension \
                   END \
                 WHEN dp.path IS NULL THEN NULL \
                 ELSE \
                   CASE \
                     WHEN f.extension IS NULL OR f.extension = '' THEN dp.path || f.name \
                     ELSE dp.path || f.name || '.' || f.extension \
                   END \
               END AS path \
             FROM {file_winner_relation_name} f \
             LEFT JOIN directory_paths dp \
               ON dp.id = f.directory_id \
              AND dp.version_id = f.version_id"
            )
        }
    }

    #[cfg(test)]
    pub(super) fn filesystem_file_data_sql(
        file_relation: Sql2FilesystemViewBaseRelation,
        blob_relation: Sql2FilesystemViewBaseRelation,
        file_winner_relation_name: &str,
        blob_winner_relation_name: &str,
    ) -> String {
        if matches!(
            file_relation,
            Sql2FilesystemViewBaseRelation::FileDescriptorHistoryRows
        ) || matches!(
            blob_relation,
            Sql2FilesystemViewBaseRelation::BinaryBlobRefHistoryRows
        ) {
            let file_schema_key = filesystem_base_relation_schema_key(file_relation);
            let blob_schema_key = filesystem_base_relation_schema_key(blob_relation);
            format!(
                "SELECT \
               f.id, \
               f.version_id, \
               b.blob_hash, \
               b.size_bytes \
             FROM {file_winner_relation_name} f \
             LEFT JOIN {blob_winner_relation_name} b \
               ON b.id = f.id \
              AND b.version_id = f.version_id \
              AND b.schema_key = '{blob_schema_key}' \
             WHERE f.schema_key = '{file_schema_key}'"
            )
        } else {
            format!(
                "SELECT \
               f.id, \
               f.version_id, \
               b.blob_hash, \
               b.size_bytes \
             FROM {file_winner_relation_name} f \
             LEFT JOIN {blob_winner_relation_name} b \
               ON b.id = f.id \
              AND b.version_id = f.version_id"
            )
        }
    }

    pub(super) fn filesystem_file_view_sql(
        plan: &PreparedSql2FilesystemViewPlan,
        file_winner_relation_name: &str,
        file_paths_relation_name: &str,
        file_data_relation_name: &str,
        active_version_id: &str,
    ) -> String {
        let select_list = plan
            .column_order
            .iter()
            .filter_map(|column_name| plan.column_plans.get(column_name))
            .map(|column| {
                let expr = match &column.expression {
                    PreparedSql2FilesystemViewExpr::FileDescriptorColumn { column_name } => {
                        format!("f.{column_name}")
                    }
                    PreparedSql2FilesystemViewExpr::DirectoryDescriptorColumn { column_name } => {
                        format!("fp.{column_name}")
                    }
                    PreparedSql2FilesystemViewExpr::BinaryBlobRefColumn { column_name } => {
                        format!("fd.{column_name}")
                    }
                    PreparedSql2FilesystemViewExpr::DerivedDirectoryPath
                    | PreparedSql2FilesystemViewExpr::DerivedFilePath => "fp.path".to_string(),
                    PreparedSql2FilesystemViewExpr::DerivedFileData => "fd.data".to_string(),
                };
                format!("{expr} AS {}", column.public_name)
            })
            .collect::<Vec<_>>()
            .join(", ");

        let default_filter = match plan.surface_variant {
            SurfaceVariant::Default => format!(
                "WHERE f.version_id = '{}'",
                escape_sql_string(active_version_id)
            ),
            _ => String::new(),
        };

        format!(
            "SELECT {select_list} \
         FROM {file_winner_relation_name} f \
         LEFT JOIN {file_paths_relation_name} fp \
           ON fp.id = f.id \
          AND fp.version_id = f.version_id \
         LEFT JOIN {file_data_relation_name} fd \
           ON fd.id = f.id \
          AND fd.version_id = f.version_id \
         {default_filter}"
        )
    }

    pub(super) fn filesystem_file_history_event_candidates_sql(
        file_history_rows_relation_name: &str,
        directory_history_rows_relation_name: &str,
        blob_history_rows_relation_name: &str,
    ) -> String {
        format!(
            "WITH descriptor_max_blob_depth AS ( \
           SELECT \
             fd.id, \
             fd.root_commit_id, \
             MAX(b.depth) AS max_blob_depth \
           FROM {file_history_rows_relation_name} fd \
           LEFT JOIN {blob_history_rows_relation_name} b \
             ON b.id = fd.id \
            AND b.root_commit_id = fd.root_commit_id \
           GROUP BY fd.id, fd.root_commit_id \
         ), \
         descriptor_directory_ids AS ( \
           SELECT DISTINCT \
             fd.id, \
             fd.root_commit_id, \
             fd.directory_id \
           FROM {file_history_rows_relation_name} fd \
           WHERE fd.directory_id IS NOT NULL \
         ), \
         file_candidates AS ( \
           SELECT \
             fd.id, \
             fd.file_id, \
             fd.plugin_key, \
             fd.root_commit_id, \
             fd.depth AS raw_depth, \
             fd.change_id, \
             fd.commit_id, \
             fd.commit_created_at, \
             1 AS candidate_source_priority \
           FROM {file_history_rows_relation_name} fd \
           LEFT JOIN descriptor_max_blob_depth mb \
             ON mb.id = fd.id \
            AND mb.root_commit_id = fd.root_commit_id \
           WHERE mb.max_blob_depth IS NULL \
              OR fd.depth <= mb.max_blob_depth \
         ), \
         directory_candidates AS ( \
           SELECT \
             ddi.id, \
             d.file_id, \
             d.plugin_key, \
             d.root_commit_id, \
             d.depth AS raw_depth, \
             d.change_id, \
             d.commit_id, \
             d.commit_created_at, \
             2 AS candidate_source_priority \
           FROM descriptor_directory_ids ddi \
           JOIN {directory_history_rows_relation_name} d \
             ON d.id = ddi.directory_id \
            AND d.root_commit_id = ddi.root_commit_id \
           LEFT JOIN descriptor_max_blob_depth mb \
             ON mb.id = ddi.id \
            AND mb.root_commit_id = ddi.root_commit_id \
           WHERE mb.max_blob_depth IS NULL \
              OR d.depth <= mb.max_blob_depth \
         ), \
         blob_candidates AS ( \
           SELECT \
             b.file_id AS id, \
             b.file_id, \
             b.plugin_key, \
             b.root_commit_id, \
             b.depth AS raw_depth, \
             b.change_id, \
             b.commit_id, \
             b.commit_created_at, \
             3 AS candidate_source_priority \
           FROM {blob_history_rows_relation_name} b \
           WHERE b.file_id IS NOT NULL \
         ), \
         all_candidates AS ( \
           SELECT * FROM file_candidates \
           UNION ALL \
           SELECT * FROM directory_candidates \
           UNION ALL \
           SELECT * FROM blob_candidates \
         ) \
         SELECT \
           id, \
           file_id, \
           plugin_key, \
           root_commit_id, \
           raw_depth, \
           change_id, \
           commit_id, \
           commit_created_at \
         FROM ( \
           SELECT \
             c.*, \
             ROW_NUMBER() OVER ( \
               PARTITION BY c.id, c.root_commit_id, c.raw_depth \
               ORDER BY \
                 c.commit_created_at DESC, \
                 c.commit_id DESC, \
                 COALESCE(c.change_id, '') DESC, \
                 c.candidate_source_priority ASC \
             ) AS candidate_rank \
           FROM all_candidates c \
         ) ranked \
         WHERE candidate_rank = 1"
        )
    }

    pub(super) fn filesystem_file_history_events_sql(
        event_candidates_relation_name: &str,
    ) -> String {
        format!(
            "SELECT \
           id, \
           file_id, \
           plugin_key, \
           root_commit_id, \
           raw_depth, \
           change_id, \
           commit_id, \
           commit_created_at, \
           ROW_NUMBER() OVER ( \
             PARTITION BY id, root_commit_id \
             ORDER BY \
               raw_depth ASC, \
               commit_id DESC, \
               COALESCE(change_id, '') DESC \
           ) - 1 AS depth \
         FROM {event_candidates_relation_name}"
        )
    }

    pub(super) fn filesystem_file_history_descriptor_resolution_sql(
        events_relation_name: &str,
        file_history_rows_relation_name: &str,
    ) -> String {
        format!(
            "SELECT \
           id, \
           file_id, \
           plugin_key, \
           root_commit_id, \
           raw_depth, \
           depth, \
           change_id, \
           commit_id, \
           commit_created_at, \
           entity_id, \
           schema_key, \
           version_id, \
           schema_version, \
           metadata, \
           directory_id, \
           name, \
           extension, \
           hidden, \
           descriptor_depth \
         FROM ( \
           SELECT \
             e.id, \
             fd.file_id, \
             fd.plugin_key, \
             e.root_commit_id, \
             e.raw_depth, \
             e.depth, \
             e.change_id, \
             e.commit_id, \
             e.commit_created_at, \
             fd.entity_id, \
             fd.schema_key, \
             fd.version_id, \
             fd.schema_version, \
             fd.metadata, \
             fd.directory_id, \
             fd.name, \
             fd.extension, \
             fd.hidden, \
             fd.depth AS descriptor_depth, \
             ROW_NUMBER() OVER ( \
               PARTITION BY e.id, e.root_commit_id, e.depth \
               ORDER BY \
                 fd.depth ASC, \
                 fd.commit_created_at DESC, \
                 fd.commit_id DESC, \
                 COALESCE(fd.change_id, '') DESC \
             ) AS descriptor_rank \
           FROM {events_relation_name} e \
           JOIN {file_history_rows_relation_name} fd \
             ON fd.id = e.id \
            AND fd.root_commit_id = e.root_commit_id \
            AND fd.depth >= e.raw_depth \
         ) ranked \
         WHERE descriptor_rank = 1"
        )
    }

    pub(super) fn filesystem_file_history_path_sql(
        descriptor_resolution_relation_name: &str,
        directory_history_rows_relation_name: &str,
    ) -> String {
        format!(
            "WITH RECURSIVE target_requests AS ( \
           SELECT DISTINCT \
             dr.id AS file_id, \
             dr.root_commit_id, \
             dr.depth AS event_depth, \
             dr.descriptor_depth AS target_depth, \
             dr.directory_id, \
             dr.version_id \
           FROM {descriptor_resolution_relation_name} dr \
           WHERE dr.directory_id IS NOT NULL \
         ), \
         visible_directory_rows AS ( \
           SELECT \
             file_id, \
             root_commit_id, \
             event_depth, \
             target_depth, \
             version_id, \
             id, \
             parent_id, \
             name \
           FROM ( \
             SELECT \
               tr.file_id, \
               tr.root_commit_id, \
               tr.event_depth, \
               tr.target_depth, \
               tr.version_id, \
               d.id, \
               d.parent_id, \
               d.name, \
               ROW_NUMBER() OVER ( \
                 PARTITION BY \
                   tr.file_id, \
                   tr.root_commit_id, \
                   tr.event_depth, \
                   tr.target_depth, \
                   tr.version_id, \
                   d.id \
                 ORDER BY \
                   d.depth ASC, \
                   d.commit_created_at DESC, \
                   d.commit_id DESC, \
                   COALESCE(d.change_id, '') DESC \
               ) AS visible_rank \
             FROM target_requests tr \
             JOIN {directory_history_rows_relation_name} d \
               ON d.root_commit_id = tr.root_commit_id \
              AND d.version_id = tr.version_id \
              AND d.depth >= tr.target_depth \
           ) ranked \
           WHERE visible_rank = 1 \
         ), \
         directory_paths AS ( \
           SELECT \
             vdr.file_id, \
             vdr.root_commit_id, \
             vdr.event_depth, \
             vdr.target_depth, \
             vdr.version_id, \
             vdr.id, \
             vdr.parent_id, \
             '/' || vdr.name || '/' AS path \
           FROM visible_directory_rows vdr \
           WHERE vdr.parent_id IS NULL \
           UNION ALL \
           SELECT \
             child.file_id, \
             child.root_commit_id, \
             child.event_depth, \
             child.target_depth, \
             child.version_id, \
             child.id, \
             child.parent_id, \
             parent.path || child.name || '/' AS path \
           FROM visible_directory_rows child \
           JOIN directory_paths parent \
             ON parent.id = child.parent_id \
            AND parent.file_id = child.file_id \
            AND parent.root_commit_id = child.root_commit_id \
            AND parent.event_depth = child.event_depth \
            AND parent.target_depth = child.target_depth \
            AND parent.version_id = child.version_id \
         ) \
         SELECT \
           dr.id, \
           dr.root_commit_id, \
           dr.depth AS event_depth, \
           CASE \
             WHEN dr.directory_id IS NULL THEN \
               CASE \
                 WHEN dr.extension IS NULL OR dr.extension = '' THEN '/' || dr.name \
                 ELSE '/' || dr.name || '.' || dr.extension \
               END \
             WHEN dp.path IS NULL THEN NULL \
             ELSE \
               CASE \
                 WHEN dr.extension IS NULL OR dr.extension = '' THEN dp.path || dr.name \
                 ELSE dp.path || dr.name || '.' || dr.extension \
               END \
           END AS path \
         FROM {descriptor_resolution_relation_name} dr \
         LEFT JOIN directory_paths dp \
           ON dp.id = dr.directory_id \
         AND dp.file_id = dr.id \
         AND dp.root_commit_id = dr.root_commit_id \
         AND dp.event_depth = dr.depth \
         AND dp.target_depth = dr.descriptor_depth \
         AND dp.version_id = dr.version_id"
        )
    }

    pub(super) fn filesystem_file_history_blob_resolution_sql(
        events_relation_name: &str,
        blob_history_rows_relation_name: &str,
    ) -> String {
        format!(
            "SELECT \
           id, \
           root_commit_id, \
           event_depth, \
           blob_hash, \
           size_bytes \
         FROM ( \
           SELECT \
             e.id, \
             e.root_commit_id, \
             e.depth AS event_depth, \
             b.blob_hash, \
             b.size_bytes, \
             ROW_NUMBER() OVER ( \
               PARTITION BY e.id, e.root_commit_id, e.depth \
               ORDER BY \
                 b.depth ASC, \
                 b.commit_created_at DESC, \
                 b.commit_id DESC, \
                 COALESCE(b.change_id, '') DESC \
             ) AS blob_rank \
           FROM {events_relation_name} e \
           JOIN {blob_history_rows_relation_name} b \
             ON b.id = e.id \
            AND b.root_commit_id = e.root_commit_id \
            AND b.depth >= e.raw_depth \
         ) ranked \
         WHERE blob_rank = 1"
        )
    }

    pub(super) fn filesystem_file_history_view_sql(
        plan: &PreparedSql2FilesystemViewPlan,
        events_relation_name: &str,
        descriptor_resolution_relation_name: &str,
        file_paths_relation_name: &str,
        file_blob_relation_name: &str,
    ) -> String {
        let select_list = plan
            .column_order
            .iter()
            .filter_map(|column_name| plan.column_plans.get(column_name))
            .map(|column| {
                let expr = match column.public_name.as_str() {
                    "lixcol_change_id" => "e.change_id".to_string(),
                    "lixcol_commit_id" => "e.commit_id".to_string(),
                    "lixcol_commit_created_at" => "e.commit_created_at".to_string(),
                    "lixcol_root_commit_id" => "e.root_commit_id".to_string(),
                    "lixcol_depth" => "e.depth".to_string(),
                    _ => match &column.expression {
                        PreparedSql2FilesystemViewExpr::FileDescriptorColumn { column_name } => {
                            format!("d.{column_name}")
                        }
                        PreparedSql2FilesystemViewExpr::DirectoryDescriptorColumn {
                            column_name,
                        } => {
                            format!("p.{column_name}")
                        }
                        PreparedSql2FilesystemViewExpr::BinaryBlobRefColumn { column_name } => {
                            format!("b.{column_name}")
                        }
                        PreparedSql2FilesystemViewExpr::DerivedDirectoryPath
                        | PreparedSql2FilesystemViewExpr::DerivedFilePath => "p.path".to_string(),
                        PreparedSql2FilesystemViewExpr::DerivedFileData => {
                            "b.blob_hash".to_string()
                        }
                    },
                };
                format!("{expr} AS {}", column.public_name)
            })
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "SELECT {select_list} \
         FROM {events_relation_name} e \
         JOIN {descriptor_resolution_relation_name} d \
           ON d.id = e.id \
          AND d.root_commit_id = e.root_commit_id \
          AND d.depth = e.depth \
         LEFT JOIN {file_paths_relation_name} p \
           ON p.id = e.id \
          AND p.root_commit_id = e.root_commit_id \
          AND p.event_depth = e.depth \
         LEFT JOIN {file_blob_relation_name} b \
           ON b.id = e.id \
          AND b.root_commit_id = e.root_commit_id \
          AND b.event_depth = e.depth"
        )
    }

    pub(super) fn filesystem_directory_view_sql(
        plan: &PreparedSql2FilesystemViewPlan,
        directory_winner_relation_name: &str,
        directory_paths_relation_name: &str,
        active_version_id: &str,
    ) -> String {
        let select_list = plan
            .column_order
            .iter()
            .filter_map(|column_name| plan.column_plans.get(column_name))
            .map(|column| {
                let expr = match &column.expression {
                    PreparedSql2FilesystemViewExpr::DirectoryDescriptorColumn { column_name } => {
                        format!("d.{column_name}")
                    }
                    PreparedSql2FilesystemViewExpr::DerivedDirectoryPath => "dp.path".to_string(),
                    PreparedSql2FilesystemViewExpr::FileDescriptorColumn { column_name } => {
                        format!("d.{column_name}")
                    }
                    PreparedSql2FilesystemViewExpr::BinaryBlobRefColumn { column_name } => {
                        format!("d.{column_name}")
                    }
                    PreparedSql2FilesystemViewExpr::DerivedFilePath => "dp.path".to_string(),
                    PreparedSql2FilesystemViewExpr::DerivedFileData => "NULL".to_string(),
                };
                format!("{expr} AS {}", column.public_name)
            })
            .collect::<Vec<_>>()
            .join(", ");

        let default_filter = match plan.surface_variant {
            SurfaceVariant::Default => format!(
                "WHERE d.version_id = '{}'",
                escape_sql_string(active_version_id)
            ),
            _ => String::new(),
        };

        let path_join = if matches!(plan.surface_variant, SurfaceVariant::History) {
            "ON dp.id = d.id \
          AND dp.version_id = d.version_id \
          AND dp.root_commit_id = d.root_commit_id \
          AND dp.depth = d.depth"
        } else {
            "ON dp.id = d.id \
          AND dp.version_id = d.version_id"
        };

        format!(
            "SELECT {select_list} \
         FROM {directory_winner_relation_name} d \
         LEFT JOIN {directory_paths_relation_name} dp \
           {path_join} \
         {default_filter}"
        )
    }
}

fn prepared_filesystem_view_expr_for_column(
    public_name: &str,
    surface_variant: SurfaceVariant,
    column_name: &str,
) -> PreparedSql2FilesystemViewExpr {
    let descriptor_relation = primary_descriptor_relation(public_name, surface_variant);

    if let Some(base_column_name) =
        base_relation_source_column_for_public_column(descriptor_relation, column_name)
    {
        return descriptor_expr_for_relation(descriptor_relation, base_column_name.to_string());
    }

    match column_name {
        "path" if public_name.starts_with("lix_file") => {
            PreparedSql2FilesystemViewExpr::DerivedFilePath
        }
        "path" => PreparedSql2FilesystemViewExpr::DerivedDirectoryPath,
        "data" => PreparedSql2FilesystemViewExpr::DerivedFileData,
        _ => descriptor_expr_for_relation(descriptor_relation, column_name.to_string()),
    }
}

fn is_file_history_event_owned_column(
    public_name: &str,
    surface_variant: SurfaceVariant,
    column_name: &str,
) -> bool {
    matches!(surface_variant, SurfaceVariant::History)
        && public_name.starts_with("lix_file")
        && matches!(
            column_name,
            "lixcol_change_id"
                | "lixcol_commit_id"
                | "lixcol_commit_created_at"
                | "lixcol_root_commit_id"
                | "lixcol_depth"
        )
}

fn required_source_columns_for_filesystem_column(
    public_name: &str,
    surface_variant: SurfaceVariant,
    column_name: &str,
) -> BTreeMap<Sql2FilesystemViewBaseRelation, Vec<String>> {
    let history = matches!(surface_variant, SurfaceVariant::History);
    let descriptor_relation = primary_descriptor_relation(public_name, surface_variant);

    let mut sources = BTreeMap::new();
    if is_file_history_event_owned_column(public_name, surface_variant, column_name) {
        let source_column_name = column_name.strip_prefix("lixcol_").unwrap_or(column_name);
        sources.insert(
            Sql2FilesystemViewBaseRelation::FileDescriptorHistoryRows,
            vec![source_column_name.to_string()],
        );
        sources.insert(
            Sql2FilesystemViewBaseRelation::DirectoryDescriptorHistoryRows,
            vec![source_column_name.to_string()],
        );
        sources.insert(
            Sql2FilesystemViewBaseRelation::BinaryBlobRefHistoryRows,
            vec![source_column_name.to_string()],
        );
        return sources;
    }
    match column_name {
        "path" if public_name.starts_with("lix_file") => {
            sources.insert(
                descriptor_relation,
                vec![
                    "directory_id".to_string(),
                    "name".to_string(),
                    "extension".to_string(),
                ],
            );
            sources.insert(
                if history {
                    Sql2FilesystemViewBaseRelation::DirectoryDescriptorHistoryRows
                } else {
                    Sql2FilesystemViewBaseRelation::DirectoryDescriptorRows
                },
                vec![
                    "id".to_string(),
                    "parent_id".to_string(),
                    "name".to_string(),
                    "path".to_string(),
                ],
            );
        }
        "path" => {
            sources.insert(
                descriptor_relation,
                vec![
                    "id".to_string(),
                    "parent_id".to_string(),
                    "name".to_string(),
                    "path".to_string(),
                ],
            );
        }
        "data" => {
            sources.insert(descriptor_relation, vec!["id".to_string()]);
            if history {
                sources.insert(
                    Sql2FilesystemViewBaseRelation::BinaryBlobRefHistoryRows,
                    vec![
                        "id".to_string(),
                        "blob_hash".to_string(),
                        "size_bytes".to_string(),
                    ],
                );
            } else {
                sources.insert(
                    Sql2FilesystemViewBaseRelation::BinaryBlobRefRows,
                    vec![
                        "id".to_string(),
                        "blob_hash".to_string(),
                        "size_bytes".to_string(),
                        "data".to_string(),
                    ],
                );
            }
        }
        other => {
            let source_column_name =
                base_relation_source_column_for_public_column(descriptor_relation, other)
                    .unwrap_or(other);
            sources.insert(descriptor_relation, vec![source_column_name.to_string()]);
        }
    }
    sources
}

fn winning_source_relation_for_filesystem_column(
    public_name: &str,
    surface_variant: SurfaceVariant,
    column_name: &str,
) -> Option<Sql2FilesystemViewBaseRelation> {
    let descriptor_relation = primary_descriptor_relation(public_name, surface_variant);
    match column_name {
        // These are derived compositions, not direct winner-row projections.
        "path" | "data" => None,
        _ if is_file_history_event_owned_column(public_name, surface_variant, column_name) => None,
        other => base_relation_source_column_for_public_column(descriptor_relation, other)
            .map(|_| descriptor_relation),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        prepared_filesystem_view_plans_for_registry, PreparedSql2FilesystemBaseRelationExpr,
        PreparedSql2FilesystemViewExpr, Sql2FilesystemProjectionType,
        Sql2FilesystemStateBaseRelation, Sql2FilesystemViewBaseRelation,
    };
    use crate::catalog::build_builtin_surface_registry;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::TableProvider;
    use datafusion::datasource::{MemTable, ViewTable};
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;
    use tokio::runtime::Builder;

    #[test]
    fn file_surface_plan_tracks_descriptor_blob_and_directory_base_relations() {
        let registry = build_builtin_surface_registry();
        let plans = prepared_filesystem_view_plans_for_registry(
            &registry,
            &["lix_file".to_string(), "lix_file_by_version".to_string()],
        );

        let file = plans.get("lix_file").expect("lix_file plan should exist");
        assert_eq!(
            file.base_relations,
            vec![
                Sql2FilesystemViewBaseRelation::FileDescriptorRows,
                Sql2FilesystemViewBaseRelation::DirectoryDescriptorRows,
                Sql2FilesystemViewBaseRelation::BinaryBlobRefRows,
            ]
        );

        let by_version = plans
            .get("lix_file_by_version")
            .expect("lix_file_by_version plan should exist");
        assert_eq!(by_version.base_relations, file.base_relations);
        assert_eq!(
            file.base_relation_plans
                .get(&Sql2FilesystemViewBaseRelation::FileDescriptorRows)
                .expect("file descriptor base relation should exist")
                .state_base_relation,
            Sql2FilesystemStateBaseRelation::LixStateByVersion
        );
    }

    #[test]
    fn file_surface_plan_tracks_public_exprs_and_required_source_columns() {
        let registry = build_builtin_surface_registry();
        let plans =
            prepared_filesystem_view_plans_for_registry(&registry, &["lix_file".to_string()]);
        let file = plans.get("lix_file").expect("lix_file plan should exist");

        let data = file.column_plan("data").expect("data column should exist");
        assert_eq!(
            data.expression,
            PreparedSql2FilesystemViewExpr::DerivedFileData
        );
        assert_eq!(data.projection_type, Sql2FilesystemProjectionType::Blob);
        assert_eq!(
            data.required_source_columns
                .get(&Sql2FilesystemViewBaseRelation::BinaryBlobRefRows)
                .expect("blob rows should be required"),
            &vec![
                "id".to_string(),
                "blob_hash".to_string(),
                "size_bytes".to_string(),
                "data".to_string()
            ]
        );

        let untracked = file
            .column_plan("lixcol_untracked")
            .expect("lixcol_untracked column should exist");
        assert_eq!(
            untracked.expression,
            PreparedSql2FilesystemViewExpr::FileDescriptorColumn {
                column_name: "untracked".to_string()
            }
        );
        assert_eq!(
            untracked.projection_type,
            Sql2FilesystemProjectionType::Boolean
        );
        assert_eq!(
            untracked.winner_source_relation,
            Some(Sql2FilesystemViewBaseRelation::FileDescriptorRows)
        );
        assert_eq!(
            untracked
                .required_source_columns
                .get(&Sql2FilesystemViewBaseRelation::FileDescriptorRows)
                .expect("untracked should read from file descriptor winners"),
            &vec!["untracked".to_string()]
        );

        let global = file
            .column_plan("lixcol_global")
            .expect("lixcol_global column should exist");
        assert_eq!(
            global.winner_source_relation,
            Some(Sql2FilesystemViewBaseRelation::FileDescriptorRows)
        );
        assert_eq!(
            global
                .required_source_columns
                .get(&Sql2FilesystemViewBaseRelation::FileDescriptorRows)
                .expect("global should read from file descriptor winners"),
            &vec!["global".to_string()]
        );

        let change_id = file
            .column_plan("lixcol_change_id")
            .expect("lixcol_change_id column should exist");
        assert_eq!(
            change_id.winner_source_relation,
            Some(Sql2FilesystemViewBaseRelation::FileDescriptorRows)
        );
        assert_eq!(
            change_id
                .required_source_columns
                .get(&Sql2FilesystemViewBaseRelation::FileDescriptorRows)
                .expect("change_id should read from file descriptor winners"),
            &vec!["change_id".to_string()]
        );

        let commit_id = file
            .column_plan("lixcol_commit_id")
            .expect("lixcol_commit_id column should exist");
        assert_eq!(
            commit_id.winner_source_relation,
            Some(Sql2FilesystemViewBaseRelation::FileDescriptorRows)
        );
        assert_eq!(
            commit_id
                .required_source_columns
                .get(&Sql2FilesystemViewBaseRelation::FileDescriptorRows)
                .expect("commit_id should read from file descriptor winners"),
            &vec!["commit_id".to_string()]
        );
        assert_eq!(
            file.base_relation_plans
                .get(&Sql2FilesystemViewBaseRelation::BinaryBlobRefRows)
                .expect("blob base relation should exist")
                .column_order,
            vec![
                "entity_id".to_string(),
                "schema_key".to_string(),
                "file_id".to_string(),
                "version_id".to_string(),
                "plugin_key".to_string(),
                "schema_version".to_string(),
                "global".to_string(),
                "change_id".to_string(),
                "created_at".to_string(),
                "updated_at".to_string(),
                "commit_id".to_string(),
                "commit_created_at".to_string(),
                "root_commit_id".to_string(),
                "depth".to_string(),
                "untracked".to_string(),
                "metadata".to_string(),
                "id".to_string(),
                "blob_hash".to_string(),
                "size_bytes".to_string(),
            ]
        );
        assert_eq!(
            file.base_relation_plans
                .get(&Sql2FilesystemViewBaseRelation::BinaryBlobRefRows)
                .expect("blob base relation should exist")
                .column_plans
                .get("size_bytes")
                .expect("size_bytes base column should exist")
                .projection_type,
            Sql2FilesystemProjectionType::Integer
        );
        assert_eq!(
            file.base_relation_plans
                .get(&Sql2FilesystemViewBaseRelation::FileDescriptorRows)
                .expect("file descriptor base relation should exist")
                .column_plans
                .get("hidden")
                .expect("hidden base column should exist")
                .expression,
            PreparedSql2FilesystemBaseRelationExpr::JsonPayloadProperty {
                property_name: "hidden".to_string()
            }
        );
    }

    #[test]
    fn history_directory_plan_uses_history_base_relations() {
        let registry = build_builtin_surface_registry();
        let plans = prepared_filesystem_view_plans_for_registry(
            &registry,
            &["lix_directory_history".to_string()],
        );
        let directory = plans
            .get("lix_directory_history")
            .expect("directory history plan should exist");

        assert_eq!(
            directory.base_relations,
            vec![Sql2FilesystemViewBaseRelation::DirectoryDescriptorHistoryRows]
        );
        assert_eq!(
            directory
                .base_relation_plans
                .get(&Sql2FilesystemViewBaseRelation::DirectoryDescriptorHistoryRows)
                .expect("history descriptor base relation should exist")
                .state_base_relation,
            Sql2FilesystemStateBaseRelation::LixStateHistory
        );

        let depth = directory
            .column_plan("lixcol_depth")
            .expect("history depth column should exist");
        assert_eq!(
            depth.expression,
            PreparedSql2FilesystemViewExpr::DirectoryDescriptorColumn {
                column_name: "depth".to_string()
            }
        );
        assert_eq!(depth.projection_type, Sql2FilesystemProjectionType::Integer);
        assert_eq!(
            depth
                .required_source_columns
                .get(&Sql2FilesystemViewBaseRelation::DirectoryDescriptorHistoryRows)
                .expect("history descriptor rows should be required"),
            &vec!["depth".to_string()]
        );

        let commit_id = directory
            .column_plan("lixcol_commit_id")
            .expect("history commit id column should exist");
        assert_eq!(
            commit_id.winner_source_relation,
            Some(Sql2FilesystemViewBaseRelation::DirectoryDescriptorHistoryRows)
        );
    }

    #[test]
    fn history_file_plan_uses_typed_history_base_relations() {
        let registry = build_builtin_surface_registry();
        let plans = prepared_filesystem_view_plans_for_registry(
            &registry,
            &["lix_file_history".to_string()],
        );
        let file = plans
            .get("lix_file_history")
            .expect("file history plan should exist");

        assert_eq!(
            file.base_relations,
            vec![
                Sql2FilesystemViewBaseRelation::FileDescriptorHistoryRows,
                Sql2FilesystemViewBaseRelation::DirectoryDescriptorHistoryRows,
                Sql2FilesystemViewBaseRelation::BinaryBlobRefHistoryRows,
            ]
        );

        let file_descriptor_history = file
            .base_relation_plans
            .get(&Sql2FilesystemViewBaseRelation::FileDescriptorHistoryRows)
            .expect("file descriptor history base relation should exist");
        assert_eq!(
            file_descriptor_history.state_base_relation,
            Sql2FilesystemStateBaseRelation::LixStateHistory
        );
        assert_eq!(file_descriptor_history.schema_key, "lix_file_descriptor");

        let directory_descriptor_history = file
            .base_relation_plans
            .get(&Sql2FilesystemViewBaseRelation::DirectoryDescriptorHistoryRows)
            .expect("directory descriptor history base relation should exist");
        assert_eq!(
            directory_descriptor_history.state_base_relation,
            Sql2FilesystemStateBaseRelation::LixStateHistory
        );
        assert_eq!(
            directory_descriptor_history.schema_key,
            "lix_directory_descriptor"
        );

        let blob_ref_history = file
            .base_relation_plans
            .get(&Sql2FilesystemViewBaseRelation::BinaryBlobRefHistoryRows)
            .expect("blob ref history base relation should exist");
        assert_eq!(
            blob_ref_history.state_base_relation,
            Sql2FilesystemStateBaseRelation::LixStateHistory
        );
        assert_eq!(blob_ref_history.schema_key, "lix_binary_blob_ref");
        let blob_id = blob_ref_history
            .column_plans
            .get("id")
            .expect("blob ref history id column should exist");
        assert_eq!(
            blob_id.expression,
            PreparedSql2FilesystemBaseRelationExpr::StateColumn {
                column_name: "file_id".to_string(),
            }
        );

        let schema_key = file
            .column_plan("lixcol_schema_key")
            .expect("history schema key column should exist");
        assert_eq!(
            schema_key.winner_source_relation,
            Some(Sql2FilesystemViewBaseRelation::FileDescriptorHistoryRows)
        );
        assert_eq!(
            schema_key
                .required_source_columns
                .get(&Sql2FilesystemViewBaseRelation::FileDescriptorHistoryRows)
                .expect("history schema key should derive from file descriptor history rows"),
            &vec!["schema_key".to_string()]
        );

        let version_id = file
            .column_plan("lixcol_version_id")
            .expect("history version id column should exist");
        assert_eq!(
            version_id.winner_source_relation,
            Some(Sql2FilesystemViewBaseRelation::FileDescriptorHistoryRows)
        );
        assert_eq!(
            version_id
                .required_source_columns
                .get(&Sql2FilesystemViewBaseRelation::FileDescriptorHistoryRows)
                .expect("history version id should derive from file descriptor history rows"),
            &vec!["version_id".to_string()]
        );

        let commit_id = file
            .column_plan("lixcol_commit_id")
            .expect("history commit id column should exist");
        assert_eq!(commit_id.winner_source_relation, None);
        assert_eq!(
            commit_id
                .required_source_columns
                .get(&Sql2FilesystemViewBaseRelation::FileDescriptorHistoryRows)
                .expect("history commit id should derive from file descriptor history rows"),
            &vec!["commit_id".to_string()]
        );
        assert_eq!(
            commit_id
                .required_source_columns
                .get(&Sql2FilesystemViewBaseRelation::DirectoryDescriptorHistoryRows)
                .expect("history commit id should derive from directory descriptor history rows"),
            &vec!["commit_id".to_string()]
        );
        assert_eq!(
            commit_id
                .required_source_columns
                .get(&Sql2FilesystemViewBaseRelation::BinaryBlobRefHistoryRows)
                .expect("history commit id should derive from blob history rows"),
            &vec!["commit_id".to_string()]
        );
    }

    #[test]
    fn base_relation_plan_compiles_over_state_relation() {
        let registry = build_builtin_surface_registry();
        let plans =
            prepared_filesystem_view_plans_for_registry(&registry, &["lix_file".to_string()]);
        let file = plans.get("lix_file").expect("lix_file plan should exist");
        let base = file
            .base_relation_plans
            .get(&Sql2FilesystemViewBaseRelation::FileDescriptorRows)
            .expect("file descriptor base relation should exist");

        let ctx = SessionContext::new();
        let provider = Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("entity_id", DataType::Utf8, false),
                    Field::new("schema_key", DataType::Utf8, false),
                    Field::new("file_id", DataType::Utf8, true),
                    Field::new("version_id", DataType::Utf8, true),
                    Field::new("plugin_key", DataType::Utf8, true),
                    Field::new("schema_version", DataType::Utf8, true),
                    Field::new("global", DataType::Boolean, false),
                    Field::new("change_id", DataType::Utf8, true),
                    Field::new("created_at", DataType::Utf8, true),
                    Field::new("updated_at", DataType::Utf8, true),
                    Field::new("commit_id", DataType::Utf8, true),
                    Field::new("commit_created_at", DataType::Utf8, true),
                    Field::new("root_commit_id", DataType::Utf8, true),
                    Field::new("depth", DataType::Int64, true),
                    Field::new("untracked", DataType::Boolean, false),
                    Field::new("metadata", DataType::Utf8, true),
                    Field::new("snapshot_content", DataType::Utf8, true),
                ])),
                vec![vec![]],
            )
            .expect("memtable should build"),
        );

        let plan = base
            .compiled_logical_plan(&ctx, provider)
            .expect("base relation plan should compile");
        let rendered = format!("{plan:?}");
        assert!(rendered.contains("schema_key"));
        assert!(rendered.contains("lix_file_descriptor"));
        assert!(rendered.contains("snapshot_content"));
    }

    #[test]
    fn history_base_relations_compile_as_schema_specific_views_over_lix_state_history() {
        let registry = build_builtin_surface_registry();
        let plans = prepared_filesystem_view_plans_for_registry(
            &registry,
            &["lix_file_history".to_string()],
        );
        let file = plans
            .get("lix_file_history")
            .expect("lix_file_history plan should exist");

        let history_provider: Arc<dyn TableProvider> = Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("entity_id", DataType::Utf8, false),
                    Field::new("schema_key", DataType::Utf8, false),
                    Field::new("file_id", DataType::Utf8, true),
                    Field::new("version_id", DataType::Utf8, true),
                    Field::new("plugin_key", DataType::Utf8, true),
                    Field::new("schema_version", DataType::Utf8, true),
                    Field::new("global", DataType::Boolean, false),
                    Field::new("change_id", DataType::Utf8, true),
                    Field::new("commit_id", DataType::Utf8, true),
                    Field::new("commit_created_at", DataType::Utf8, true),
                    Field::new("root_commit_id", DataType::Utf8, true),
                    Field::new("depth", DataType::Int64, true),
                    Field::new("untracked", DataType::Boolean, false),
                    Field::new("metadata", DataType::Utf8, true),
                    Field::new("snapshot_content", DataType::Utf8, true),
                ])),
                vec![vec![]],
            )
            .expect("history memtable should build"),
        );

        for (base_relation, expected_schema_key) in [
            (
                Sql2FilesystemViewBaseRelation::FileDescriptorHistoryRows,
                "lix_file_descriptor",
            ),
            (
                Sql2FilesystemViewBaseRelation::DirectoryDescriptorHistoryRows,
                "lix_directory_descriptor",
            ),
            (
                Sql2FilesystemViewBaseRelation::BinaryBlobRefHistoryRows,
                "lix_binary_blob_ref",
            ),
        ] {
            let plan = file
                .base_relation_plans
                .get(&base_relation)
                .expect("history base relation should exist");
            let ctx = SessionContext::new();
            let compiled = plan
                .compiled_logical_plan(&ctx, Arc::clone(&history_provider))
                .expect("history base relation should compile");
            let rendered = format!("{compiled:?}");
            assert!(rendered.contains("schema_key"));
            assert!(rendered.contains(expected_schema_key));
            assert!(rendered.contains("snapshot_content"));
        }
    }

    #[test]
    fn descriptor_winner_selection_compiles_with_precedence_ranking() {
        let registry = build_builtin_surface_registry();
        let plans =
            prepared_filesystem_view_plans_for_registry(&registry, &["lix_file".to_string()]);
        let file = plans.get("lix_file").expect("lix_file plan should exist");
        let base = file
            .base_relation_plans
            .get(&Sql2FilesystemViewBaseRelation::FileDescriptorRows)
            .expect("file descriptor base relation should exist");

        let ctx = SessionContext::new();
        let provider = Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("entity_id", DataType::Utf8, false),
                    Field::new("schema_key", DataType::Utf8, false),
                    Field::new("file_id", DataType::Utf8, true),
                    Field::new("version_id", DataType::Utf8, true),
                    Field::new("plugin_key", DataType::Utf8, true),
                    Field::new("schema_version", DataType::Utf8, true),
                    Field::new("global", DataType::Boolean, false),
                    Field::new("change_id", DataType::Utf8, true),
                    Field::new("created_at", DataType::Utf8, true),
                    Field::new("updated_at", DataType::Utf8, true),
                    Field::new("commit_id", DataType::Utf8, true),
                    Field::new("commit_created_at", DataType::Utf8, true),
                    Field::new("root_commit_id", DataType::Utf8, true),
                    Field::new("depth", DataType::Int64, true),
                    Field::new("untracked", DataType::Boolean, false),
                    Field::new("metadata", DataType::Utf8, true),
                    Field::new("id", DataType::Utf8, true),
                    Field::new("directory_id", DataType::Utf8, true),
                    Field::new("name", DataType::Utf8, true),
                    Field::new("extension", DataType::Utf8, true),
                    Field::new("hidden", DataType::Boolean, true),
                ])),
                vec![vec![]],
            )
            .expect("memtable should build"),
        );

        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");
        let plan = runtime
            .block_on(base.compiled_ranked_winner_logical_plan(&ctx, provider))
            .expect("winner plan should compile");
        let rendered = format!("{plan:?}");
        assert!(rendered.contains("ROW_NUMBER"));
        assert!(rendered.contains("untracked"));
        assert!(rendered.contains("global"));
        assert!(rendered.contains("updated_at"));
        assert!(rendered.contains("schema_key"));
        assert!(rendered.contains("lix_file_descriptor"));
    }

    #[test]
    fn blob_winner_selection_compiles_with_precedence_ranking() {
        let registry = build_builtin_surface_registry();
        let plans =
            prepared_filesystem_view_plans_for_registry(&registry, &["lix_file".to_string()]);
        let file = plans.get("lix_file").expect("lix_file plan should exist");
        let base = file
            .base_relation_plans
            .get(&Sql2FilesystemViewBaseRelation::BinaryBlobRefRows)
            .expect("blob base relation should exist");

        let ctx = SessionContext::new();
        let provider = Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("entity_id", DataType::Utf8, false),
                    Field::new("schema_key", DataType::Utf8, false),
                    Field::new("file_id", DataType::Utf8, true),
                    Field::new("version_id", DataType::Utf8, true),
                    Field::new("plugin_key", DataType::Utf8, true),
                    Field::new("schema_version", DataType::Utf8, true),
                    Field::new("global", DataType::Boolean, false),
                    Field::new("change_id", DataType::Utf8, true),
                    Field::new("created_at", DataType::Utf8, true),
                    Field::new("updated_at", DataType::Utf8, true),
                    Field::new("commit_id", DataType::Utf8, true),
                    Field::new("commit_created_at", DataType::Utf8, true),
                    Field::new("root_commit_id", DataType::Utf8, true),
                    Field::new("depth", DataType::Int64, true),
                    Field::new("untracked", DataType::Boolean, false),
                    Field::new("metadata", DataType::Utf8, true),
                    Field::new("id", DataType::Utf8, true),
                    Field::new("blob_hash", DataType::Utf8, true),
                    Field::new("size_bytes", DataType::Int64, true),
                ])),
                vec![vec![]],
            )
            .expect("memtable should build"),
        );

        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");
        let plan = runtime
            .block_on(base.compiled_ranked_winner_logical_plan(&ctx, provider))
            .expect("blob winner plan should compile");
        let rendered = format!("{plan:?}");
        assert!(rendered.contains("ROW_NUMBER"));
        assert!(rendered.contains("version_id"));
        assert!(rendered.contains("entity_id"));
        assert!(rendered.contains("updated_at"));
        assert!(rendered.contains("schema_key"));
        assert!(rendered.contains("lix_binary_blob_ref"));
    }

    #[test]
    fn directory_path_derivation_compiles_over_directory_winners() {
        let registry = build_builtin_surface_registry();
        let plans =
            prepared_filesystem_view_plans_for_registry(&registry, &["lix_directory".to_string()]);
        let directory = plans
            .get("lix_directory")
            .expect("lix_directory plan should exist");
        let base = directory
            .base_relation_plans
            .get(&Sql2FilesystemViewBaseRelation::DirectoryDescriptorRows)
            .expect("directory descriptor base relation should exist");

        let ctx = SessionContext::new();
        let provider = Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("entity_id", DataType::Utf8, false),
                    Field::new("schema_key", DataType::Utf8, false),
                    Field::new("file_id", DataType::Utf8, true),
                    Field::new("version_id", DataType::Utf8, true),
                    Field::new("plugin_key", DataType::Utf8, true),
                    Field::new("schema_version", DataType::Utf8, true),
                    Field::new("global", DataType::Boolean, false),
                    Field::new("change_id", DataType::Utf8, true),
                    Field::new("created_at", DataType::Utf8, true),
                    Field::new("updated_at", DataType::Utf8, true),
                    Field::new("commit_id", DataType::Utf8, true),
                    Field::new("commit_created_at", DataType::Utf8, true),
                    Field::new("root_commit_id", DataType::Utf8, true),
                    Field::new("depth", DataType::Int64, true),
                    Field::new("untracked", DataType::Boolean, false),
                    Field::new("metadata", DataType::Utf8, true),
                    Field::new("snapshot_content", DataType::Utf8, true),
                ])),
                vec![vec![]],
            )
            .expect("memtable should build"),
        );

        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");
        let winner_provider = runtime
            .block_on(base.compiled_ranked_winner_view_provider(&ctx, provider))
            .expect("winner provider should compile");
        let plan = runtime
            .block_on(directory.compiled_directory_path_logical_plan(&ctx, winner_provider))
            .expect("directory path plan should compile");
        let rendered = format!("{plan:?}");
        assert!(rendered.contains("directory_paths"));
        assert!(rendered.contains("parent_id"));
        assert!(rendered.contains("path"));
        assert!(rendered.contains("schema_key"));
        assert!(rendered.contains("lix_directory_descriptor"));
    }

    #[test]
    fn file_path_derivation_compiles_over_file_and_directory_winners() {
        let registry = build_builtin_surface_registry();
        let plans =
            prepared_filesystem_view_plans_for_registry(&registry, &["lix_file".to_string()]);
        let file = plans.get("lix_file").expect("lix_file plan should exist");

        let ctx = SessionContext::new();
        let file_provider = Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Utf8, false),
                    Field::new("directory_id", DataType::Utf8, true),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("extension", DataType::Utf8, true),
                    Field::new("version_id", DataType::Utf8, false),
                    Field::new("entity_id", DataType::Utf8, false),
                    Field::new("schema_key", DataType::Utf8, false),
                    Field::new("file_id", DataType::Utf8, true),
                    Field::new("plugin_key", DataType::Utf8, true),
                    Field::new("schema_version", DataType::Utf8, true),
                    Field::new("global", DataType::Boolean, false),
                    Field::new("change_id", DataType::Utf8, true),
                    Field::new("created_at", DataType::Utf8, true),
                    Field::new("updated_at", DataType::Utf8, true),
                    Field::new("commit_id", DataType::Utf8, true),
                    Field::new("untracked", DataType::Boolean, false),
                    Field::new("metadata", DataType::Utf8, true),
                    Field::new("hidden", DataType::Boolean, true),
                ])),
                vec![vec![]],
            )
            .expect("memtable should build"),
        );
        let directory_provider = Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Utf8, false),
                    Field::new("parent_id", DataType::Utf8, true),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("version_id", DataType::Utf8, false),
                ])),
                vec![vec![]],
            )
            .expect("memtable should build"),
        );

        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");
        let plan = runtime
            .block_on(file.compiled_file_path_logical_plan(&ctx, file_provider, directory_provider))
            .expect("file path plan should compile");
        let rendered = format!("{plan:?}");
        assert!(rendered.contains("directory_paths"));
        assert!(rendered.contains("directory_id"));
        assert!(rendered.contains("extension"));
        assert!(rendered.contains("path"));
        assert!(rendered.contains("schema_key"));
        assert!(rendered.contains("lix_file_descriptor"));
        assert!(rendered.contains("lix_directory_descriptor"));
    }

    #[test]
    fn file_data_join_compiles_over_file_and_blob_winners() {
        let registry = build_builtin_surface_registry();
        let plans =
            prepared_filesystem_view_plans_for_registry(&registry, &["lix_file".to_string()]);
        let file = plans.get("lix_file").expect("lix_file plan should exist");
        let file_base = file
            .base_relation_plans
            .get(&Sql2FilesystemViewBaseRelation::FileDescriptorRows)
            .expect("file descriptor base relation should exist");
        let blob_base = file
            .base_relation_plans
            .get(&Sql2FilesystemViewBaseRelation::BinaryBlobRefRows)
            .expect("blob base relation should exist");

        let ctx = SessionContext::new();
        let file_provider = Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("entity_id", DataType::Utf8, false),
                    Field::new("schema_key", DataType::Utf8, false),
                    Field::new("file_id", DataType::Utf8, true),
                    Field::new("version_id", DataType::Utf8, true),
                    Field::new("plugin_key", DataType::Utf8, true),
                    Field::new("schema_version", DataType::Utf8, true),
                    Field::new("global", DataType::Boolean, false),
                    Field::new("change_id", DataType::Utf8, true),
                    Field::new("created_at", DataType::Utf8, true),
                    Field::new("updated_at", DataType::Utf8, true),
                    Field::new("commit_id", DataType::Utf8, true),
                    Field::new("commit_created_at", DataType::Utf8, true),
                    Field::new("root_commit_id", DataType::Utf8, true),
                    Field::new("depth", DataType::Int64, true),
                    Field::new("untracked", DataType::Boolean, false),
                    Field::new("metadata", DataType::Utf8, true),
                    Field::new("snapshot_content", DataType::Utf8, true),
                ])),
                vec![vec![]],
            )
            .expect("memtable should build"),
        );
        let blob_provider = Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("entity_id", DataType::Utf8, false),
                    Field::new("schema_key", DataType::Utf8, false),
                    Field::new("file_id", DataType::Utf8, true),
                    Field::new("version_id", DataType::Utf8, true),
                    Field::new("plugin_key", DataType::Utf8, true),
                    Field::new("schema_version", DataType::Utf8, true),
                    Field::new("global", DataType::Boolean, false),
                    Field::new("change_id", DataType::Utf8, true),
                    Field::new("created_at", DataType::Utf8, true),
                    Field::new("updated_at", DataType::Utf8, true),
                    Field::new("commit_id", DataType::Utf8, true),
                    Field::new("commit_created_at", DataType::Utf8, true),
                    Field::new("root_commit_id", DataType::Utf8, true),
                    Field::new("depth", DataType::Int64, true),
                    Field::new("untracked", DataType::Boolean, false),
                    Field::new("metadata", DataType::Utf8, true),
                    Field::new("snapshot_content", DataType::Utf8, true),
                ])),
                vec![vec![]],
            )
            .expect("memtable should build"),
        );

        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");
        let file_winner_provider = runtime
            .block_on(file_base.compiled_ranked_winner_view_provider(&ctx, file_provider))
            .expect("file winner provider should compile");
        let blob_winner_provider = runtime
            .block_on(blob_base.compiled_ranked_winner_view_provider(&ctx, blob_provider))
            .expect("blob winner provider should compile");
        let plan = runtime
            .block_on(file.compiled_file_data_logical_plan(
                &ctx,
                file_winner_provider,
                blob_winner_provider,
            ))
            .expect("file data plan should compile");
        let rendered = format!("{plan:?}");
        assert!(rendered.contains("blob_hash"));
        assert!(rendered.contains("size_bytes"));
        assert!(rendered.contains("version_id"));
        assert!(rendered.contains("id"));
        assert!(rendered.contains("schema_key"));
        assert!(rendered.contains("lix_file_descriptor"));
        assert!(rendered.contains("lix_binary_blob_ref"));
    }

    #[test]
    fn file_surface_compiles_to_native_view() {
        let registry = build_builtin_surface_registry();
        let plans =
            prepared_filesystem_view_plans_for_registry(&registry, &["lix_file".to_string()]);
        let file = plans.get("lix_file").expect("lix_file plan should exist");

        let ctx = SessionContext::new();
        let file_provider = Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Utf8, false),
                    Field::new("directory_id", DataType::Utf8, true),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("extension", DataType::Utf8, true),
                    Field::new("version_id", DataType::Utf8, false),
                    Field::new("entity_id", DataType::Utf8, false),
                    Field::new("schema_key", DataType::Utf8, false),
                    Field::new("file_id", DataType::Utf8, true),
                    Field::new("plugin_key", DataType::Utf8, true),
                    Field::new("schema_version", DataType::Utf8, true),
                    Field::new("global", DataType::Boolean, false),
                    Field::new("change_id", DataType::Utf8, true),
                    Field::new("created_at", DataType::Utf8, true),
                    Field::new("updated_at", DataType::Utf8, true),
                    Field::new("commit_id", DataType::Utf8, true),
                    Field::new("untracked", DataType::Boolean, false),
                    Field::new("metadata", DataType::Utf8, true),
                    Field::new("hidden", DataType::Boolean, true),
                ])),
                vec![vec![]],
            )
            .expect("memtable should build"),
        );
        let directory_provider = Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Utf8, false),
                    Field::new("parent_id", DataType::Utf8, true),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("version_id", DataType::Utf8, false),
                ])),
                vec![vec![]],
            )
            .expect("memtable should build"),
        );
        let file_data_provider = Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Utf8, false),
                    Field::new("version_id", DataType::Utf8, false),
                    Field::new("data", DataType::Binary, true),
                    Field::new("blob_hash", DataType::Utf8, true),
                    Field::new("size_bytes", DataType::Int64, true),
                ])),
                vec![vec![]],
            )
            .expect("memtable should build"),
        );

        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");
        let view_provider = runtime
            .block_on(file.compiled_lix_file_view_provider(
                &ctx,
                "version-a",
                file_provider,
                directory_provider,
                file_data_provider,
            ))
            .expect("file view provider should compile");
        assert!(view_provider.as_any().downcast_ref::<ViewTable>().is_some());

        let view = view_provider
            .as_any()
            .downcast_ref::<ViewTable>()
            .expect("compiled provider should be a view table");
        let rendered = format!("{:?}", view.logical_plan());
        assert!(rendered.contains("lix_file_descriptor_winners"));
        assert!(rendered.contains("__lix_file_paths"));
        assert!(rendered.contains("__lix_file_data"));
        assert!(rendered.contains("path"));
        assert!(rendered.contains("data"));
    }

    #[test]
    fn file_by_version_surface_compiles_to_native_view() {
        let registry = build_builtin_surface_registry();
        let plans = prepared_filesystem_view_plans_for_registry(
            &registry,
            &["lix_file_by_version".to_string()],
        );
        let file = plans
            .get("lix_file_by_version")
            .expect("lix_file_by_version plan should exist");

        let ctx = SessionContext::new();
        let file_provider = Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Utf8, false),
                    Field::new("directory_id", DataType::Utf8, true),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("extension", DataType::Utf8, true),
                    Field::new("version_id", DataType::Utf8, false),
                    Field::new("entity_id", DataType::Utf8, false),
                    Field::new("schema_key", DataType::Utf8, false),
                    Field::new("file_id", DataType::Utf8, true),
                    Field::new("plugin_key", DataType::Utf8, true),
                    Field::new("schema_version", DataType::Utf8, true),
                    Field::new("global", DataType::Boolean, false),
                    Field::new("change_id", DataType::Utf8, true),
                    Field::new("created_at", DataType::Utf8, true),
                    Field::new("updated_at", DataType::Utf8, true),
                    Field::new("commit_id", DataType::Utf8, true),
                    Field::new("untracked", DataType::Boolean, false),
                    Field::new("metadata", DataType::Utf8, true),
                    Field::new("hidden", DataType::Boolean, true),
                ])),
                vec![vec![]],
            )
            .expect("memtable should build"),
        );
        let directory_provider = Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Utf8, false),
                    Field::new("parent_id", DataType::Utf8, true),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("version_id", DataType::Utf8, false),
                ])),
                vec![vec![]],
            )
            .expect("memtable should build"),
        );
        let file_data_provider = Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Utf8, false),
                    Field::new("version_id", DataType::Utf8, false),
                    Field::new("data", DataType::Binary, true),
                    Field::new("blob_hash", DataType::Utf8, true),
                    Field::new("size_bytes", DataType::Int64, true),
                ])),
                vec![vec![]],
            )
            .expect("memtable should build"),
        );

        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");
        let view_provider = runtime
            .block_on(file.compiled_lix_file_view_provider(
                &ctx,
                "version-a",
                file_provider,
                directory_provider,
                file_data_provider,
            ))
            .expect("file by version view provider should compile");
        assert!(view_provider.as_any().downcast_ref::<ViewTable>().is_some());

        let view = view_provider
            .as_any()
            .downcast_ref::<ViewTable>()
            .expect("compiled provider should be a view table");
        let rendered = format!("{:?}", view.logical_plan());
        assert!(rendered.contains("lix_file_descriptor_winners"));
        assert!(rendered.contains("__lix_file_paths"));
        assert!(rendered.contains("__lix_file_data"));
        assert!(rendered.contains("lixcol_version_id"));
        assert!(!rendered.contains("WHERE f.version_id = 'version-a'"));
    }

    #[test]
    fn file_history_surface_compiles_to_native_view() {
        let registry = build_builtin_surface_registry();
        let plans = prepared_filesystem_view_plans_for_registry(
            &registry,
            &["lix_file_history".to_string()],
        );
        let file = plans
            .get("lix_file_history")
            .expect("lix_file_history plan should exist");

        let ctx = SessionContext::new();
        let file_history_provider = Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Utf8, false),
                    Field::new("directory_id", DataType::Utf8, true),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("extension", DataType::Utf8, true),
                    Field::new("version_id", DataType::Utf8, false),
                    Field::new("root_commit_id", DataType::Utf8, false),
                    Field::new("depth", DataType::Int64, false),
                    Field::new("entity_id", DataType::Utf8, false),
                    Field::new("schema_key", DataType::Utf8, false),
                    Field::new("file_id", DataType::Utf8, true),
                    Field::new("plugin_key", DataType::Utf8, true),
                    Field::new("schema_version", DataType::Utf8, true),
                    Field::new("global", DataType::Boolean, false),
                    Field::new("change_id", DataType::Utf8, true),
                    Field::new("commit_id", DataType::Utf8, true),
                    Field::new("commit_created_at", DataType::Utf8, true),
                    Field::new("untracked", DataType::Boolean, false),
                    Field::new("metadata", DataType::Utf8, true),
                    Field::new("hidden", DataType::Boolean, true),
                ])),
                vec![vec![]],
            )
            .expect("memtable should build"),
        );
        let directory_history_provider = Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Utf8, false),
                    Field::new("parent_id", DataType::Utf8, true),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("version_id", DataType::Utf8, false),
                    Field::new("root_commit_id", DataType::Utf8, false),
                    Field::new("depth", DataType::Int64, false),
                    Field::new("entity_id", DataType::Utf8, false),
                    Field::new("schema_key", DataType::Utf8, false),
                    Field::new("file_id", DataType::Utf8, true),
                    Field::new("plugin_key", DataType::Utf8, true),
                    Field::new("schema_version", DataType::Utf8, true),
                    Field::new("global", DataType::Boolean, false),
                    Field::new("change_id", DataType::Utf8, true),
                    Field::new("commit_id", DataType::Utf8, true),
                    Field::new("commit_created_at", DataType::Utf8, true),
                    Field::new("untracked", DataType::Boolean, false),
                    Field::new("metadata", DataType::Utf8, true),
                    Field::new("hidden", DataType::Boolean, true),
                ])),
                vec![vec![]],
            )
            .expect("memtable should build"),
        );
        let blob_history_provider = Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Utf8, false),
                    Field::new("blob_hash", DataType::Utf8, true),
                    Field::new("size_bytes", DataType::Int64, true),
                    Field::new("version_id", DataType::Utf8, false),
                    Field::new("root_commit_id", DataType::Utf8, false),
                    Field::new("depth", DataType::Int64, false),
                    Field::new("entity_id", DataType::Utf8, false),
                    Field::new("schema_key", DataType::Utf8, false),
                    Field::new("file_id", DataType::Utf8, true),
                    Field::new("plugin_key", DataType::Utf8, true),
                    Field::new("schema_version", DataType::Utf8, true),
                    Field::new("global", DataType::Boolean, false),
                    Field::new("change_id", DataType::Utf8, true),
                    Field::new("commit_id", DataType::Utf8, true),
                    Field::new("commit_created_at", DataType::Utf8, true),
                    Field::new("untracked", DataType::Boolean, false),
                    Field::new("metadata", DataType::Utf8, true),
                ])),
                vec![vec![]],
            )
            .expect("memtable should build"),
        );

        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");
        let view_provider = runtime
            .block_on(file.compiled_lix_file_history_view_provider(
                &ctx,
                file_history_provider,
                directory_history_provider,
                blob_history_provider,
            ))
            .expect("file history view provider should compile");
        assert!(view_provider.as_any().downcast_ref::<ViewTable>().is_some());

        let view = view_provider
            .as_any()
            .downcast_ref::<ViewTable>()
            .expect("compiled provider should be a view table");
        let rendered = format!("{:?}", view.logical_plan());
        assert!(rendered.contains("__lix_file_history_events"));
        assert!(rendered.contains("__lix_file_history_descriptor_resolution"));
        assert!(rendered.contains("__lix_file_history_paths"));
        assert!(rendered.contains("__lix_file_history_blob_resolution"));
        assert!(rendered.contains("lixcol_root_commit_id"));
        assert!(rendered.contains("lixcol_depth"));
        assert!(rendered.contains("candidate_source_priority"));
    }

    #[test]
    fn file_history_event_candidate_sql_uses_blob_file_ids_and_explicit_source_precedence() {
        let rendered = super::sql_fragments::filesystem_file_history_event_candidates_sql(
            "__file_history_rows",
            "__directory_history_rows",
            "__blob_history_rows",
        );
        assert!(rendered.contains("b.file_id AS id"));
        assert!(rendered.contains("candidate_source_priority ASC"));
    }

    #[test]
    fn file_history_events_sql_normalizes_sparse_public_depth_from_deduped_events() {
        let rendered =
            super::sql_fragments::filesystem_file_history_events_sql("__event_candidates");
        assert!(rendered.contains("FROM __event_candidates"));
        assert!(rendered.contains("PARTITION BY id, root_commit_id"));
        assert!(rendered.contains("raw_depth ASC"));
        assert!(rendered.contains(") - 1 AS depth"));

        let final_view_sql = super::sql_fragments::filesystem_file_history_view_sql(
            &prepared_filesystem_view_plans_for_registry(
                &build_builtin_surface_registry(),
                &["lix_file_history".to_string()],
            )["lix_file_history"],
            "__events",
            "__descriptor_resolution",
            "__paths",
            "__blob_resolution",
        );
        assert!(final_view_sql.contains("e.depth AS lixcol_depth"));
    }

    #[test]
    fn file_history_descriptor_resolution_uses_nearest_descriptor_at_or_above_event_raw_depth() {
        let registry = build_builtin_surface_registry();
        let plans = prepared_filesystem_view_plans_for_registry(
            &registry,
            &["lix_file_history".to_string()],
        );
        let file = plans
            .get("lix_file_history")
            .expect("lix_file_history plan should exist");

        for column_name in [
            "hidden",
            "metadata",
            "lixcol_entity_id",
            "lixcol_schema_key",
            "lixcol_file_id",
            "lixcol_version_id",
            "lixcol_plugin_key",
            "lixcol_schema_version",
            "lixcol_metadata",
        ] {
            let column = file
                .column_plan(column_name)
                .expect("descriptor-backed history column should exist");
            assert_eq!(
                column.winner_source_relation,
                Some(Sql2FilesystemViewBaseRelation::FileDescriptorHistoryRows),
                "{column_name} should resolve from file descriptor history rows"
            );
        }

        let path = file
            .column_plan("path")
            .expect("history path column should exist");
        assert_eq!(
            path.required_source_columns
                .get(&Sql2FilesystemViewBaseRelation::FileDescriptorHistoryRows)
                .expect("history path should depend on file descriptor history rows"),
            &vec![
                "directory_id".to_string(),
                "name".to_string(),
                "extension".to_string(),
            ]
        );

        let rendered = super::sql_fragments::filesystem_file_history_descriptor_resolution_sql(
            "__events",
            "__file_history_rows",
        );
        assert!(rendered.contains("fd.depth >= e.raw_depth"));
        assert!(rendered.contains("PARTITION BY e.id, e.root_commit_id, e.depth"));
        assert!(rendered.contains("fd.name"));
        assert!(rendered.contains("fd.extension"));
        assert!(rendered.contains("fd.hidden"));
        assert!(rendered.contains("fd.metadata"));
        assert!(rendered.contains("fd.entity_id"));
        assert!(rendered.contains("fd.schema_key"));
        assert!(rendered.contains("fd.version_id"));
        assert!(rendered.contains("fd.file_id"));
        assert!(rendered.contains("fd.plugin_key"));
        assert!(rendered.contains("fd.schema_version"));
    }

    #[test]
    fn file_history_path_resolution_uses_visible_directory_stage_then_recursive_assembly() {
        let rendered = super::sql_fragments::filesystem_file_history_path_sql(
            "__descriptor_resolution",
            "__directory_history_rows",
        );
        assert!(rendered.contains("WITH RECURSIVE target_requests AS"));
        assert!(rendered.contains("visible_directory_rows AS"));
        assert!(rendered.contains("d.depth >= tr.target_depth"));
        assert!(rendered.contains("ROW_NUMBER() OVER"));
        assert!(rendered.contains("WHERE visible_rank = 1"));
        assert!(rendered.contains("directory_paths AS"));
        assert!(rendered.contains("FROM visible_directory_rows vdr"));
        assert!(rendered.contains("FROM visible_directory_rows child"));
        assert!(rendered.contains("JOIN directory_paths parent"));
        assert!(rendered.contains("dp.target_depth = dr.descriptor_depth"));

        let registry = build_builtin_surface_registry();
        let plans = prepared_filesystem_view_plans_for_registry(
            &registry,
            &["lix_file_history".to_string()],
        );
        let file = plans
            .get("lix_file_history")
            .expect("lix_file_history plan should exist");
        let path = file
            .column_plan("path")
            .expect("history path column should exist");
        assert_eq!(
            path.required_source_columns
                .get(&Sql2FilesystemViewBaseRelation::DirectoryDescriptorHistoryRows)
                .expect("history path should depend on directory history rows"),
            &vec![
                "id".to_string(),
                "parent_id".to_string(),
                "name".to_string(),
                "path".to_string(),
            ]
        );
    }

    #[test]
    fn file_history_blob_resolution_uses_nearest_blob_ref_at_or_above_event_raw_depth() {
        let rendered = super::sql_fragments::filesystem_file_history_blob_resolution_sql(
            "__events",
            "__blob_history_rows",
        );
        assert!(rendered.contains("FROM __events e"));
        assert!(rendered.contains("JOIN __blob_history_rows b"));
        assert!(rendered.contains("b.depth >= e.raw_depth"));
        assert!(rendered.contains("PARTITION BY e.id, e.root_commit_id, e.depth"));
        assert!(rendered.contains("blob_hash"));
        assert!(rendered.contains("size_bytes"));

        let registry = build_builtin_surface_registry();
        let plans = prepared_filesystem_view_plans_for_registry(
            &registry,
            &["lix_file_history".to_string()],
        );
        let file = plans
            .get("lix_file_history")
            .expect("lix_file_history plan should exist");
        let data = file
            .column_plan("data")
            .expect("history data column should exist");
        assert_eq!(
            data.winner_source_relation, None,
            "history data should not pretend to come from a winner row"
        );
        let blob_requirements = data
            .required_source_columns
            .get(&Sql2FilesystemViewBaseRelation::BinaryBlobRefHistoryRows)
            .expect("history data should depend on blob ref history rows");
        assert!(blob_requirements.contains(&"id".to_string()));
        assert!(blob_requirements.contains(&"blob_hash".to_string()));
        assert!(blob_requirements.contains(&"size_bytes".to_string()));
        assert!(!blob_requirements.contains(&"data".to_string()));
    }

    #[test]
    fn file_history_final_projection_has_truthful_ownership() {
        let registry = build_builtin_surface_registry();
        let plans = prepared_filesystem_view_plans_for_registry(
            &registry,
            &["lix_file_history".to_string()],
        );
        let file = plans
            .get("lix_file_history")
            .expect("lix_file_history plan should exist");

        for column_name in [
            "hidden",
            "metadata",
            "lixcol_entity_id",
            "lixcol_schema_key",
            "lixcol_file_id",
            "lixcol_version_id",
            "lixcol_plugin_key",
            "lixcol_schema_version",
            "lixcol_metadata",
        ] {
            let column = file
                .column_plan(column_name)
                .expect("descriptor-owned history column should exist");
            assert_eq!(
                column.winner_source_relation,
                Some(Sql2FilesystemViewBaseRelation::FileDescriptorHistoryRows),
                "{column_name} should come from resolved descriptor rows"
            );
        }

        for column_name in [
            "lixcol_change_id",
            "lixcol_commit_id",
            "lixcol_commit_created_at",
            "lixcol_root_commit_id",
            "lixcol_depth",
        ] {
            let column = file
                .column_plan(column_name)
                .expect("event-owned history column should exist");
            assert_eq!(
                column.winner_source_relation, None,
                "{column_name} should not pretend to come from descriptor winner rows"
            );
        }

        assert_eq!(
            file.column_plan("path")
                .expect("path column should exist")
                .winner_source_relation,
            None,
            "path should come from resolved directory ancestry"
        );
        assert_eq!(
            file.column_plan("data")
                .expect("data column should exist")
                .winner_source_relation,
            None,
            "data should not pretend to come from relational winner rows"
        );

        let rendered = super::sql_fragments::filesystem_file_history_view_sql(
            file,
            "__events",
            "__descriptor_resolution",
            "__paths",
            "__blob_resolution",
        );
        assert!(rendered.contains("e.change_id AS lixcol_change_id"));
        assert!(rendered.contains("e.commit_id AS lixcol_commit_id"));
        assert!(rendered.contains("e.commit_created_at AS lixcol_commit_created_at"));
        assert!(rendered.contains("e.root_commit_id AS lixcol_root_commit_id"));
        assert!(rendered.contains("e.depth AS lixcol_depth"));
        assert!(rendered.contains("d.schema_key AS lixcol_schema_key"));
        assert!(rendered.contains("d.version_id AS lixcol_version_id"));
        assert!(rendered.contains("d.hidden AS hidden"));
        assert!(rendered.contains("d.metadata AS metadata"));
        assert!(rendered.contains("p.path AS path"));
        assert!(rendered.contains("b.blob_hash AS data"));
    }

    #[test]
    fn file_history_by_version_surface_compiles_to_native_view() {
        let registry = build_builtin_surface_registry();
        let plans = prepared_filesystem_view_plans_for_registry(
            &registry,
            &["lix_file_history_by_version".to_string()],
        );
        let file = plans
            .get("lix_file_history_by_version")
            .expect("lix_file_history_by_version plan should exist");

        let ctx = SessionContext::new();
        let file_history_provider = Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Utf8, false),
                    Field::new("directory_id", DataType::Utf8, true),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("extension", DataType::Utf8, true),
                    Field::new("version_id", DataType::Utf8, false),
                    Field::new("root_commit_id", DataType::Utf8, false),
                    Field::new("depth", DataType::Int64, false),
                    Field::new("entity_id", DataType::Utf8, false),
                    Field::new("schema_key", DataType::Utf8, false),
                    Field::new("file_id", DataType::Utf8, true),
                    Field::new("plugin_key", DataType::Utf8, true),
                    Field::new("schema_version", DataType::Utf8, true),
                    Field::new("global", DataType::Boolean, false),
                    Field::new("change_id", DataType::Utf8, true),
                    Field::new("commit_id", DataType::Utf8, true),
                    Field::new("commit_created_at", DataType::Utf8, true),
                    Field::new("untracked", DataType::Boolean, false),
                    Field::new("metadata", DataType::Utf8, true),
                    Field::new("hidden", DataType::Boolean, true),
                ])),
                vec![vec![]],
            )
            .expect("memtable should build"),
        );
        let directory_history_provider = Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Utf8, false),
                    Field::new("parent_id", DataType::Utf8, true),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("version_id", DataType::Utf8, false),
                    Field::new("root_commit_id", DataType::Utf8, false),
                    Field::new("depth", DataType::Int64, false),
                    Field::new("entity_id", DataType::Utf8, false),
                    Field::new("schema_key", DataType::Utf8, false),
                    Field::new("file_id", DataType::Utf8, true),
                    Field::new("plugin_key", DataType::Utf8, true),
                    Field::new("schema_version", DataType::Utf8, true),
                    Field::new("global", DataType::Boolean, false),
                    Field::new("change_id", DataType::Utf8, true),
                    Field::new("commit_id", DataType::Utf8, true),
                    Field::new("commit_created_at", DataType::Utf8, true),
                    Field::new("untracked", DataType::Boolean, false),
                    Field::new("metadata", DataType::Utf8, true),
                    Field::new("hidden", DataType::Boolean, true),
                ])),
                vec![vec![]],
            )
            .expect("memtable should build"),
        );
        let blob_history_provider = Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Utf8, false),
                    Field::new("blob_hash", DataType::Utf8, true),
                    Field::new("size_bytes", DataType::Int64, true),
                    Field::new("version_id", DataType::Utf8, false),
                    Field::new("root_commit_id", DataType::Utf8, false),
                    Field::new("depth", DataType::Int64, false),
                    Field::new("entity_id", DataType::Utf8, false),
                    Field::new("schema_key", DataType::Utf8, false),
                    Field::new("file_id", DataType::Utf8, true),
                    Field::new("plugin_key", DataType::Utf8, true),
                    Field::new("schema_version", DataType::Utf8, true),
                    Field::new("global", DataType::Boolean, false),
                    Field::new("change_id", DataType::Utf8, true),
                    Field::new("commit_id", DataType::Utf8, true),
                    Field::new("commit_created_at", DataType::Utf8, true),
                    Field::new("untracked", DataType::Boolean, false),
                    Field::new("metadata", DataType::Utf8, true),
                ])),
                vec![vec![]],
            )
            .expect("memtable should build"),
        );

        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");
        let view_provider = runtime
            .block_on(file.compiled_lix_file_history_view_provider(
                &ctx,
                file_history_provider,
                directory_history_provider,
                blob_history_provider,
            ))
            .expect("file history by version view provider should compile");
        assert!(view_provider.as_any().downcast_ref::<ViewTable>().is_some());

        let view = view_provider
            .as_any()
            .downcast_ref::<ViewTable>()
            .expect("compiled provider should be a view table");
        let rendered = format!("{:?}", view.logical_plan());
        assert!(rendered.contains("__lix_file_history_events"));
        assert!(rendered.contains("lixcol_version_id"));
        assert!(rendered.contains("lixcol_root_commit_id"));
        assert!(rendered.contains("lixcol_depth"));
    }

    #[test]
    fn directory_surface_compiles_to_native_view() {
        let registry = build_builtin_surface_registry();
        let plans =
            prepared_filesystem_view_plans_for_registry(&registry, &["lix_directory".to_string()]);
        let directory = plans
            .get("lix_directory")
            .expect("lix_directory plan should exist");

        let ctx = SessionContext::new();
        let directory_provider = Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Utf8, false),
                    Field::new("parent_id", DataType::Utf8, true),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("version_id", DataType::Utf8, false),
                    Field::new("entity_id", DataType::Utf8, false),
                    Field::new("schema_key", DataType::Utf8, false),
                    Field::new("file_id", DataType::Utf8, true),
                    Field::new("plugin_key", DataType::Utf8, true),
                    Field::new("schema_version", DataType::Utf8, true),
                    Field::new("global", DataType::Boolean, false),
                    Field::new("change_id", DataType::Utf8, true),
                    Field::new("created_at", DataType::Utf8, true),
                    Field::new("updated_at", DataType::Utf8, true),
                    Field::new("commit_id", DataType::Utf8, true),
                    Field::new("untracked", DataType::Boolean, false),
                    Field::new("metadata", DataType::Utf8, true),
                    Field::new("hidden", DataType::Boolean, true),
                ])),
                vec![vec![]],
            )
            .expect("memtable should build"),
        );

        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");
        let view_provider = runtime
            .block_on(directory.compiled_lix_directory_view_provider(
                &ctx,
                "version-a",
                directory_provider,
            ))
            .expect("directory view provider should compile");
        assert!(view_provider.as_any().downcast_ref::<ViewTable>().is_some());

        let view = view_provider
            .as_any()
            .downcast_ref::<ViewTable>()
            .expect("compiled provider should be a view table");
        let rendered = format!("{:?}", view.logical_plan());
        assert!(rendered.contains("lix_directory_descriptor_winners"));
        assert!(rendered.contains("__lix_directory_paths"));
        assert!(rendered.contains("path"));
        assert!(rendered.contains("version-a"));
        assert!(rendered.contains("version_id"));
    }

    #[test]
    fn directory_by_version_surface_compiles_to_native_view() {
        let registry = build_builtin_surface_registry();
        let plans = prepared_filesystem_view_plans_for_registry(
            &registry,
            &["lix_directory_by_version".to_string()],
        );
        let directory = plans
            .get("lix_directory_by_version")
            .expect("lix_directory_by_version plan should exist");

        let ctx = SessionContext::new();
        let directory_provider = Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Utf8, false),
                    Field::new("parent_id", DataType::Utf8, true),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("version_id", DataType::Utf8, false),
                    Field::new("entity_id", DataType::Utf8, false),
                    Field::new("schema_key", DataType::Utf8, false),
                    Field::new("file_id", DataType::Utf8, true),
                    Field::new("plugin_key", DataType::Utf8, true),
                    Field::new("schema_version", DataType::Utf8, true),
                    Field::new("global", DataType::Boolean, false),
                    Field::new("change_id", DataType::Utf8, true),
                    Field::new("created_at", DataType::Utf8, true),
                    Field::new("updated_at", DataType::Utf8, true),
                    Field::new("commit_id", DataType::Utf8, true),
                    Field::new("untracked", DataType::Boolean, false),
                    Field::new("metadata", DataType::Utf8, true),
                    Field::new("hidden", DataType::Boolean, true),
                ])),
                vec![vec![]],
            )
            .expect("memtable should build"),
        );

        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");
        let view_provider = runtime
            .block_on(directory.compiled_lix_directory_view_provider(
                &ctx,
                "version-a",
                directory_provider,
            ))
            .expect("directory by version view provider should compile");
        assert!(view_provider.as_any().downcast_ref::<ViewTable>().is_some());

        let view = view_provider
            .as_any()
            .downcast_ref::<ViewTable>()
            .expect("compiled provider should be a view table");
        let rendered = format!("{:?}", view.logical_plan());
        assert!(rendered.contains("lix_directory_descriptor_winners"));
        assert!(rendered.contains("__lix_directory_paths"));
        assert!(rendered.contains("lixcol_version_id"));
        assert!(!rendered.contains("WHERE d.version_id = 'version-a'"));
    }

    #[test]
    fn directory_history_surface_compiles_to_native_view() {
        let registry = build_builtin_surface_registry();
        let plans = prepared_filesystem_view_plans_for_registry(
            &registry,
            &["lix_directory_history".to_string()],
        );
        let directory = plans
            .get("lix_directory_history")
            .expect("lix_directory_history plan should exist");

        let ctx = SessionContext::new();
        let directory_provider = Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Utf8, false),
                    Field::new("parent_id", DataType::Utf8, true),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("version_id", DataType::Utf8, false),
                    Field::new("root_commit_id", DataType::Utf8, false),
                    Field::new("depth", DataType::Int64, false),
                    Field::new("entity_id", DataType::Utf8, false),
                    Field::new("schema_key", DataType::Utf8, false),
                    Field::new("file_id", DataType::Utf8, true),
                    Field::new("plugin_key", DataType::Utf8, true),
                    Field::new("schema_version", DataType::Utf8, true),
                    Field::new("global", DataType::Boolean, false),
                    Field::new("change_id", DataType::Utf8, true),
                    Field::new("commit_id", DataType::Utf8, true),
                    Field::new("commit_created_at", DataType::Utf8, true),
                    Field::new("untracked", DataType::Boolean, false),
                    Field::new("metadata", DataType::Utf8, true),
                    Field::new("hidden", DataType::Boolean, true),
                ])),
                vec![vec![]],
            )
            .expect("memtable should build"),
        );

        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");
        let view_provider = runtime
            .block_on(directory.compiled_lix_directory_history_view_provider(
                &ctx,
                "version-a",
                directory_provider,
            ))
            .expect("directory history view provider should compile");
        assert!(view_provider.as_any().downcast_ref::<ViewTable>().is_some());

        let view = view_provider
            .as_any()
            .downcast_ref::<ViewTable>()
            .expect("compiled provider should be a view table");
        let rendered = format!("{:?}", view.logical_plan());
        assert!(rendered.contains("lix_directory_descriptor_history_winners"));
        assert!(rendered.contains("__lix_directory_paths"));
        assert!(rendered.contains("root_commit_id"));
        assert!(rendered.contains("depth"));
        assert!(rendered.contains("lixcol_root_commit_id"));
        assert!(rendered.contains("lixcol_depth"));
    }
}
