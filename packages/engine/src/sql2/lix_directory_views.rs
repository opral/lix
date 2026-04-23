use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::datasource::ViewTable;
use datafusion::logical_expr::expr_fn::col;
use datafusion::logical_expr::{lit, Expr, LogicalPlan};
use datafusion::prelude::SessionContext;

use crate::LixError;

use super::udf::{lix_json_extract_boolean_expr, lix_json_extract_text_expr};

const DIRECTORY_SCHEMA_KEY: &str = "lix_directory_descriptor";
const DIRECTORY_DESCRIPTOR_BY_VERSION_ROWS: &str = "__lix_directory_descriptor_by_version_rows";

pub(crate) async fn register_lix_directory_views(
    ctx: &SessionContext,
    active_version_id: &str,
) -> Result<(), LixError> {
    let lix_state_by_version = ctx
        .table_provider("lix_state_by_version")
        .await
        .map_err(datafusion_error_to_lix_error)?;

    let descriptor_provider =
        compiled_directory_descriptor_by_version_provider(ctx, lix_state_by_version)?;
    ctx.register_table(DIRECTORY_DESCRIPTOR_BY_VERSION_ROWS, descriptor_provider)
        .map_err(datafusion_error_to_lix_error)?;

    let by_version_provider = compiled_directory_by_version_provider(ctx).await?;
    ctx.register_table("lix_directory_by_version", by_version_provider)
        .map_err(datafusion_error_to_lix_error)?;

    let active_provider = compiled_directory_provider(ctx, active_version_id).await?;
    ctx.register_table("lix_directory", active_provider)
        .map_err(datafusion_error_to_lix_error)?;

    Ok(())
}

fn compiled_directory_descriptor_by_version_provider(
    ctx: &SessionContext,
    provider: Arc<dyn TableProvider>,
) -> Result<Arc<dyn TableProvider>, LixError> {
    Ok(Arc::new(ViewTable::new(
        compiled_directory_descriptor_by_version_logical_plan(ctx, provider)?,
        None,
    )))
}

fn compiled_directory_descriptor_by_version_logical_plan(
    ctx: &SessionContext,
    provider: Arc<dyn TableProvider>,
) -> Result<LogicalPlan, LixError> {
    let projection_exprs = directory_descriptor_by_version_projection_exprs();
    let dataframe = ctx
        .read_table(provider)
        .map_err(datafusion_error_to_lix_error)?
        .filter(col("schema_key").eq(lit(DIRECTORY_SCHEMA_KEY.to_string())))
        .map_err(datafusion_error_to_lix_error)?
        .select(projection_exprs)
        .map_err(datafusion_error_to_lix_error)?;
    Ok(dataframe.into_unoptimized_plan())
}

fn directory_descriptor_by_version_projection_exprs() -> Vec<Expr> {
    let snapshot_content = col("snapshot_content");
    vec![
        lix_json_extract_text_expr(snapshot_content.clone(), "id").alias("id"),
        lix_json_extract_text_expr(snapshot_content.clone(), "parent_id").alias("parent_id"),
        lix_json_extract_text_expr(snapshot_content.clone(), "name").alias("name"),
        lix_json_extract_boolean_expr(snapshot_content, "hidden").alias("hidden"),
        col("entity_id").alias("entity_id"),
        col("schema_key").alias("schema_key"),
        col("file_id").alias("file_id"),
        col("version_id").alias("version_id"),
        col("plugin_key").alias("plugin_key"),
        col("schema_version").alias("schema_version"),
        col("global").alias("global"),
        col("change_id").alias("change_id"),
        col("created_at").alias("created_at"),
        col("updated_at").alias("updated_at"),
        col("commit_id").alias("commit_id"),
        col("untracked").alias("untracked"),
        col("metadata").alias("metadata"),
    ]
}

async fn compiled_directory_by_version_provider(
    ctx: &SessionContext,
) -> Result<Arc<dyn TableProvider>, LixError> {
    Ok(Arc::new(ViewTable::new(
        compiled_directory_by_version_logical_plan(ctx).await?,
        None,
    )))
}

async fn compiled_directory_by_version_logical_plan(
    ctx: &SessionContext,
) -> Result<LogicalPlan, LixError> {
    let dataframe = ctx
        .sql(&directory_by_version_sql())
        .await
        .map_err(datafusion_error_to_lix_error)?;
    Ok(dataframe.into_unoptimized_plan())
}

async fn compiled_directory_provider(
    ctx: &SessionContext,
    active_version_id: &str,
) -> Result<Arc<dyn TableProvider>, LixError> {
    Ok(Arc::new(ViewTable::new(
        compiled_directory_logical_plan(ctx, active_version_id).await?,
        None,
    )))
}

async fn compiled_directory_logical_plan(
    ctx: &SessionContext,
    active_version_id: &str,
) -> Result<LogicalPlan, LixError> {
    let dataframe = ctx
        .sql(&directory_sql(active_version_id))
        .await
        .map_err(datafusion_error_to_lix_error)?;
    Ok(dataframe.into_unoptimized_plan())
}

fn directory_by_version_sql() -> String {
    format!(
        "WITH RECURSIVE directory_paths AS ( \
           SELECT \
             d.id, \
             d.version_id, \
             '/' || d.name || '/' AS path \
           FROM {rows} d \
           WHERE d.parent_id IS NULL \
           UNION ALL \
           SELECT \
             child.id, \
             child.version_id, \
             parent.path || child.name || '/' AS path \
           FROM {rows} child \
           JOIN directory_paths parent \
             ON parent.id = child.parent_id \
            AND parent.version_id = child.version_id \
         ) \
         SELECT \
           d.id AS id, \
           dp.path AS path, \
           d.parent_id AS parent_id, \
           d.name AS name, \
           d.hidden AS hidden, \
           d.entity_id AS lixcol_entity_id, \
           d.schema_key AS lixcol_schema_key, \
           d.file_id AS lixcol_file_id, \
           d.plugin_key AS lixcol_plugin_key, \
           d.schema_version AS lixcol_schema_version, \
           d.global AS lixcol_global, \
           d.change_id AS lixcol_change_id, \
           d.created_at AS lixcol_created_at, \
           d.updated_at AS lixcol_updated_at, \
           d.commit_id AS lixcol_commit_id, \
           d.untracked AS lixcol_untracked, \
           d.metadata AS lixcol_metadata, \
           d.version_id AS lixcol_version_id \
         FROM {rows} d \
         LEFT JOIN directory_paths dp \
           ON dp.id = d.id \
          AND dp.version_id = d.version_id",
        rows = DIRECTORY_DESCRIPTOR_BY_VERSION_ROWS,
    )
}

fn directory_sql(active_version_id: &str) -> String {
    let active_version_id = active_version_id.replace('\'', "''");
    format!(
        "SELECT \
           id, \
           path, \
           parent_id, \
           name, \
           hidden, \
           lixcol_entity_id, \
           lixcol_schema_key, \
           lixcol_file_id, \
           lixcol_plugin_key, \
           lixcol_schema_version, \
           lixcol_global, \
           lixcol_change_id, \
           lixcol_created_at, \
           lixcol_updated_at, \
           lixcol_commit_id, \
           lixcol_untracked, \
           lixcol_metadata \
         FROM lix_directory_by_version \
         WHERE lixcol_version_id = '{active_version_id}'"
    )
}

fn datafusion_error_to_lix_error(error: datafusion::error::DataFusionError) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("sql2 DataFusion error: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::{directory_by_version_sql, directory_sql};

    #[test]
    fn directory_by_version_sql_uses_recursive_paths() {
        let sql = directory_by_version_sql();
        assert!(sql.contains("WITH RECURSIVE directory_paths"));
        assert!(sql.contains("JOIN directory_paths parent"));
        assert!(sql.contains("__lix_directory_descriptor_by_version_rows"));
        assert!(sql.contains("AS lixcol_version_id"));
    }

    #[test]
    fn directory_sql_pins_active_version() {
        let sql = directory_sql("version-a");
        assert!(sql.contains("FROM lix_directory_by_version"));
        assert!(sql.contains("WHERE lixcol_version_id = 'version-a'"));
        assert!(!sql.contains("lixcol_version_id AS"));
    }
}
