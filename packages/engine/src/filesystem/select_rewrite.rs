use sqlparser::ast::{
    BinaryOperator, Expr, Ident, ObjectName, ObjectNamePart, Query, Select, Statement, TableAlias,
    TableFactor, TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::engine::sql::planning::rewrite_engine::rewrite_query_with_select_rewriter;
use crate::engine::sql::storage::sql_text::escape_sql_string;
use crate::filesystem::live_projection::build_live_file_projection_sql;
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
};
use crate::{LixError, Value};

const FILE_VIEW: &str = "lix_file";
const FILE_BY_VERSION_VIEW: &str = "lix_file_by_version";
const FILE_HISTORY_VIEW: &str = "lix_file_history";
const FILE_HISTORY_BY_VERSION_VIEW: &str = "lix_file_history_by_version";
const DIRECTORY_VIEW: &str = "lix_directory";
const DIRECTORY_BY_VERSION_VIEW: &str = "lix_directory_by_version";
const DIRECTORY_HISTORY_VIEW: &str = "lix_directory_history";

#[derive(Debug, Clone, Default)]
struct HistoryPredicatePushdown {
    predicates: Vec<HistoryPredicate>,
    has_root_predicate: bool,
}

#[derive(Debug, Clone)]
enum HistoryPredicate {
    Binary {
        source_column: &'static str,
        operator: BinaryOperator,
        rhs_sql: String,
    },
    InSubquery {
        source_column: &'static str,
        subquery_sql: String,
        negated: bool,
    },
    InList {
        source_column: &'static str,
        list_sql: Vec<String>,
        negated: bool,
    },
    IsNull {
        source_column: &'static str,
        negated: bool,
    },
}

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    rewrite_query_with_params(query, &[])
}

pub fn rewrite_query_with_params(
    query: Query,
    _params: &[Value],
) -> Result<Option<Query>, LixError> {
    rewrite_query_with_select_rewriter(query, &mut rewrite_select)
}

fn rewrite_select(select: &mut Select, changed: &mut bool) -> Result<(), LixError> {
    let allow_unqualified = select.from.len() == 1 && select.from[0].joins.is_empty();
    for table in &mut select.from {
        rewrite_table_with_joins(table, &select.selection, allow_unqualified, changed)?;
    }
    Ok(())
}

fn rewrite_table_with_joins(
    table: &mut TableWithJoins,
    selection: &Option<Expr>,
    allow_unqualified: bool,
    changed: &mut bool,
) -> Result<(), LixError> {
    rewrite_table_factor(&mut table.relation, selection, allow_unqualified, changed)?;
    for join in &mut table.joins {
        rewrite_table_factor(&mut join.relation, selection, false, changed)?;
    }
    Ok(())
}

fn rewrite_table_factor(
    relation: &mut TableFactor,
    selection: &Option<Expr>,
    allow_unqualified: bool,
    changed: &mut bool,
) -> Result<(), LixError> {
    match relation {
        TableFactor::Table { name, alias, .. } => {
            let Some(view_name) = object_name_terminal(name) else {
                return Ok(());
            };
            let relation_name = alias
                .as_ref()
                .map(|value| value.name.value.clone())
                .unwrap_or_else(|| view_name.clone());
            let pushdown =
                collect_history_pushdown_predicates(selection, &relation_name, allow_unqualified);
            let Some(source_query) = build_filesystem_projection_query(&view_name, &pushdown)?
            else {
                return Ok(());
            };
            let derived_alias = alias.clone().or_else(|| Some(default_alias(&view_name)));
            *relation = TableFactor::Derived {
                lateral: false,
                subquery: Box::new(source_query),
                alias: derived_alias,
            };
            *changed = true;
        }
        _ => {}
    }
    Ok(())
}

fn build_filesystem_projection_query(
    view_name: &str,
    pushdown: &HistoryPredicatePushdown,
) -> Result<Option<Query>, LixError> {
    let sql = match view_name.to_ascii_lowercase().as_str() {
        FILE_VIEW => {
            let by_version_sql = build_file_by_version_projection_sql();
            format!(
                "SELECT \
                    f.id, \
                    f.directory_id, \
                    f.name, \
                    f.extension, \
                    f.path, \
                    f.data, \
                    f.metadata, \
                    f.hidden, \
                    f.lixcol_entity_id, \
                    f.lixcol_schema_key, \
                    f.lixcol_file_id, \
                    f.lixcol_plugin_key, \
                    f.lixcol_schema_version, \
                    f.lixcol_global, \
                    f.lixcol_change_id, \
                    f.lixcol_created_at, \
                    f.lixcol_updated_at, \
                    {active_version_commit_id_sql} AS lixcol_commit_id, \
                    f.lixcol_writer_key, \
                    f.lixcol_untracked, \
                    f.lixcol_metadata \
                 FROM ({by_version_sql}) AS f \
                 WHERE {active_version_scope}",
                by_version_sql = by_version_sql,
                active_version_commit_id_sql = active_version_commit_id_sql(),
                active_version_scope = active_version_scope_predicate("f.lixcol_version_id")
            )
        }
        FILE_BY_VERSION_VIEW => build_file_by_version_projection_sql(),
        FILE_HISTORY_VIEW | FILE_HISTORY_BY_VERSION_VIEW => {
            let state_history_view =
                history_state_view_name(view_name).ok_or_else(|| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!("unsupported history view '{view_name}'"),
                })?;
            let force_active_scope = view_name.eq_ignore_ascii_case(FILE_HISTORY_VIEW);
            let state_history_predicates =
                render_history_pushdown_sql(pushdown, None, force_active_scope);
            let state_history_predicates_sh =
                render_history_pushdown_sql(pushdown, Some("sh"), force_active_scope);
            format!(
                "WITH RECURSIVE directory_history_base AS (\
                     SELECT \
                        lix_json_extract(snapshot_content, 'id') AS id, \
                        lix_json_extract(snapshot_content, 'parent_id') AS parent_id, \
                        lix_json_extract(snapshot_content, 'name') AS name, \
                        change_id AS lixcol_change_id, \
                        commit_id AS lixcol_commit_id, \
                        commit_created_at AS lixcol_commit_created_at, \
                        root_commit_id AS lixcol_root_commit_id, \
                        depth AS lixcol_depth \
                     FROM {state_history_view} \
                     WHERE schema_key = 'lix_directory_descriptor' \
                       AND snapshot_content IS NOT NULL \
                       {state_history_predicates}\
                 ), \
                 directory_history_path_walk AS (\
                     SELECT \
                        base.id AS target_id, \
                        base.lixcol_root_commit_id AS root_commit_id, \
                        base.lixcol_depth AS target_depth, \
                        base.parent_id AS next_parent_id, \
                        '/' || base.name || '/' AS path, \
                        0 AS step \
                     FROM directory_history_base base \
                     UNION ALL \
                     SELECT \
                        walk.target_id, \
                        walk.root_commit_id, \
                        walk.target_depth, \
                        parent.parent_id AS next_parent_id, \
                        '/' || parent.name || walk.path AS path, \
                        walk.step + 1 AS step \
                     FROM directory_history_path_walk walk \
                     JOIN directory_history_base parent \
                       ON parent.id = walk.next_parent_id \
                      AND parent.lixcol_root_commit_id = walk.root_commit_id \
                      AND parent.lixcol_depth = (\
                          SELECT MIN(candidate.lixcol_depth) \
                         FROM directory_history_base candidate \
                          WHERE candidate.id = walk.next_parent_id \
                            AND candidate.lixcol_root_commit_id = walk.root_commit_id \
                            AND candidate.lixcol_depth >= walk.target_depth\
                      ) \
                     WHERE walk.next_parent_id IS NOT NULL \
                       AND walk.step < 1024 \
                 ), \
                 directory_history_paths AS (\
                     SELECT \
                        walk.target_id, \
                        walk.root_commit_id, \
                        walk.target_depth, \
                        walk.path \
                     FROM directory_history_path_walk walk \
                     JOIN (\
                        SELECT \
                            target_id, \
                            root_commit_id, \
                            target_depth, \
                            MAX(step) AS max_step \
                        FROM directory_history_path_walk \
                        WHERE next_parent_id IS NULL \
                        GROUP BY target_id, root_commit_id, target_depth\
                     ) terminal \
                       ON terminal.target_id = walk.target_id \
                      AND terminal.root_commit_id = walk.root_commit_id \
                      AND terminal.target_depth = walk.target_depth \
                      AND terminal.max_step = walk.step \
                 ), \
                 file_history_descriptor_rows AS (\
                    SELECT \
                        lix_json_extract(snapshot_content, 'id') AS id, \
                        lix_json_extract(snapshot_content, 'directory_id') AS directory_id, \
                        lix_json_extract(snapshot_content, 'name') AS name, \
                        lix_json_extract(snapshot_content, 'extension') AS extension, \
                        lix_json_extract(snapshot_content, 'metadata') AS metadata, \
                        lix_json_extract(snapshot_content, 'hidden') AS hidden, \
                        entity_id AS lixcol_entity_id, \
                        schema_key AS lixcol_schema_key, \
                        file_id AS lixcol_file_id, \
                        version_id AS lixcol_version_id, \
                        plugin_key AS lixcol_plugin_key, \
                        schema_version AS lixcol_schema_version, \
                        change_id AS lixcol_change_id, \
                        commit_id AS lixcol_origin_commit_id, \
                        commit_created_at AS lixcol_commit_created_at, \
                        metadata AS lixcol_metadata, \
                        root_commit_id AS lixcol_root_commit_id, \
                        depth AS lixcol_depth \
                    FROM {state_history_view} \
                    WHERE schema_key = 'lix_file_descriptor' \
                      AND snapshot_content IS NOT NULL \
                      {state_history_predicates}\
                 ), \
                 binary_blob_ref_history_rows AS (\
                    SELECT \
                        sh.file_id AS id, \
                        sh.change_id AS lixcol_change_id, \
                        sh.commit_id AS lixcol_commit_id, \
                        sh.commit_created_at AS lixcol_commit_created_at, \
                        sh.root_commit_id AS lixcol_root_commit_id, \
                        sh.depth AS lixcol_depth, \
                        lix_json_extract(sh.snapshot_content, 'blob_hash') AS blob_hash \
                    FROM {state_history_view} sh \
                    WHERE sh.schema_key = 'lix_binary_blob_ref' \
                      AND sh.snapshot_content IS NOT NULL \
                      {state_history_predicates_sh}\
                 ), \
                 binary_blob_ref_history_depth_bounds AS (\
                    SELECT \
                        b.id, \
                        b.lixcol_root_commit_id, \
                        MAX(b.lixcol_depth) AS max_lixcol_depth \
                    FROM binary_blob_ref_history_rows b \
                    GROUP BY b.id, b.lixcol_root_commit_id\
                 ), \
                 file_history_directory_checkpoint_candidates AS (\
                    SELECT DISTINCT \
                        d.id, \
                        dir.lixcol_root_commit_id, \
                        dir.lixcol_depth, \
                        dir.lixcol_change_id, \
                        dir.lixcol_commit_id, \
                        dir.lixcol_commit_created_at \
                    FROM file_history_descriptor_rows d \
                    JOIN directory_history_base dir \
                      ON dir.id = d.directory_id \
                     AND dir.lixcol_root_commit_id = d.lixcol_root_commit_id \
                    LEFT JOIN binary_blob_ref_history_depth_bounds bounds \
                      ON bounds.id = d.id \
                     AND bounds.lixcol_root_commit_id = d.lixcol_root_commit_id \
                    WHERE bounds.max_lixcol_depth IS NULL \
                       OR dir.lixcol_depth <= bounds.max_lixcol_depth \
                 ), \
                 file_history_checkpoint_candidates AS (\
                    SELECT \
                        d.id, \
                        d.lixcol_root_commit_id, \
                        d.lixcol_depth, \
                        d.lixcol_change_id, \
                        d.lixcol_origin_commit_id AS lixcol_commit_id, \
                        d.lixcol_commit_created_at \
                    FROM file_history_descriptor_rows d \
                    LEFT JOIN binary_blob_ref_history_depth_bounds bounds \
                      ON bounds.id = d.id \
                     AND bounds.lixcol_root_commit_id = d.lixcol_root_commit_id \
                    WHERE bounds.max_lixcol_depth IS NULL \
                       OR d.lixcol_depth <= bounds.max_lixcol_depth \
                    UNION ALL \
                    SELECT \
                        b.id, \
                        b.lixcol_root_commit_id, \
                        b.lixcol_depth, \
                        b.lixcol_change_id, \
                        b.lixcol_commit_id, \
                        b.lixcol_commit_created_at \
                    FROM binary_blob_ref_history_rows b \
                    UNION ALL \
                    SELECT \
                        dir.id, \
                        dir.lixcol_root_commit_id, \
                        dir.lixcol_depth, \
                        dir.lixcol_change_id, \
                        dir.lixcol_commit_id, \
                        dir.lixcol_commit_created_at \
                    FROM file_history_directory_checkpoint_candidates dir\
                 ), \
                 file_history_checkpoint_rows AS (\
                    SELECT \
                        ranked.id, \
                        ranked.lixcol_root_commit_id, \
                        ranked.lixcol_depth AS lixcol_raw_depth, \
                        ranked.lixcol_change_id, \
                        ranked.lixcol_commit_id \
                    FROM (\
                        SELECT \
                            candidate.id, \
                            candidate.lixcol_root_commit_id, \
                            candidate.lixcol_depth, \
                            candidate.lixcol_change_id, \
                            candidate.lixcol_commit_id, \
                            ROW_NUMBER() OVER (\
                                PARTITION BY candidate.id, candidate.lixcol_root_commit_id, candidate.lixcol_depth \
                                ORDER BY candidate.lixcol_commit_created_at DESC, candidate.lixcol_commit_id DESC, candidate.lixcol_change_id DESC\
                            ) AS row_num \
                        FROM file_history_checkpoint_candidates candidate\
                    ) ranked \
                    WHERE ranked.row_num = 1\
                 ), \
                 file_history_ranked_checkpoints AS (\
                    SELECT \
                        checkpoint.id, \
                        checkpoint.lixcol_root_commit_id, \
                        checkpoint.lixcol_raw_depth, \
                        checkpoint.lixcol_change_id, \
                        checkpoint.lixcol_commit_id, \
                        ROW_NUMBER() OVER (\
                            PARTITION BY checkpoint.id, checkpoint.lixcol_root_commit_id \
                            ORDER BY checkpoint.lixcol_raw_depth ASC, checkpoint.lixcol_commit_id DESC, checkpoint.lixcol_change_id DESC\
                        ) - 1 AS lixcol_depth \
                    FROM file_history_checkpoint_rows checkpoint\
                 ) \
                 SELECT \
                    checkpoint.id, \
                    CASE \
                        WHEN descriptor.directory_id IS NULL THEN \
                            CASE \
                                WHEN descriptor.extension IS NULL OR descriptor.extension = '' THEN '/' || descriptor.name \
                                ELSE '/' || descriptor.name || '.' || descriptor.extension \
                            END \
                        WHEN dp.path IS NULL THEN NULL \
                        ELSE \
                            CASE \
                                WHEN descriptor.extension IS NULL OR descriptor.extension = '' THEN dp.path || descriptor.name \
                                ELSE dp.path || descriptor.name || '.' || descriptor.extension \
                            END \
                    END AS path, \
                    COALESCE(fd.data, bbs.data) AS data, \
                    descriptor.metadata, \
                    descriptor.hidden, \
                    checkpoint.id AS lixcol_entity_id, \
                    descriptor.lixcol_schema_key, \
                    CASE \
                        WHEN descriptor.lixcol_change_id = checkpoint.lixcol_change_id THEN descriptor.lixcol_file_id \
                        ELSE checkpoint.id \
                    END AS lixcol_file_id, \
                    descriptor.lixcol_version_id, \
                    descriptor.lixcol_plugin_key, \
                    descriptor.lixcol_schema_version, \
                    checkpoint.lixcol_change_id, \
                    descriptor.lixcol_metadata, \
                    checkpoint.lixcol_commit_id, \
                    checkpoint.lixcol_root_commit_id, \
                    checkpoint.lixcol_depth \
                 FROM file_history_ranked_checkpoints checkpoint \
                 JOIN file_history_descriptor_rows descriptor \
                   ON descriptor.id = checkpoint.id \
                  AND descriptor.lixcol_root_commit_id = checkpoint.lixcol_root_commit_id \
                  AND descriptor.lixcol_depth = (\
                      SELECT MIN(candidate.lixcol_depth) \
                      FROM file_history_descriptor_rows candidate \
                      WHERE candidate.id = checkpoint.id \
                        AND candidate.lixcol_root_commit_id = checkpoint.lixcol_root_commit_id \
                        AND candidate.lixcol_depth >= checkpoint.lixcol_raw_depth\
                  ) \
                 LEFT JOIN directory_history_paths dp \
                   ON dp.target_id = descriptor.directory_id \
                  AND dp.root_commit_id = checkpoint.lixcol_root_commit_id \
                  AND dp.target_depth = (\
                      SELECT MIN(candidate.lixcol_depth) \
                      FROM directory_history_base candidate \
                      WHERE candidate.id = descriptor.directory_id \
                        AND candidate.lixcol_root_commit_id = checkpoint.lixcol_root_commit_id \
                        AND candidate.lixcol_depth >= descriptor.lixcol_depth\
                  ) \
                 LEFT JOIN lix_internal_file_history_data_cache fd \
                   ON fd.file_id = checkpoint.id \
                  AND fd.root_commit_id = checkpoint.lixcol_root_commit_id \
                  AND fd.depth = checkpoint.lixcol_depth \
                 LEFT JOIN binary_blob_ref_history_rows bhr \
                   ON bhr.id = checkpoint.id \
                  AND bhr.lixcol_root_commit_id = checkpoint.lixcol_root_commit_id \
                  AND bhr.lixcol_depth = (\
                      SELECT MIN(candidate.lixcol_depth) \
                      FROM binary_blob_ref_history_rows candidate \
                      WHERE candidate.id = checkpoint.id \
                        AND candidate.lixcol_root_commit_id = checkpoint.lixcol_root_commit_id \
                        AND candidate.lixcol_depth >= checkpoint.lixcol_raw_depth\
                  ) \
                 LEFT JOIN lix_internal_binary_blob_store bbs \
                   ON bbs.blob_hash = bhr.blob_hash",
                state_history_view = state_history_view,
                state_history_predicates = state_history_predicates,
                state_history_predicates_sh = state_history_predicates_sh,
            )
        }
        DIRECTORY_VIEW => format!(
            "WITH RECURSIVE directory_descriptor_rows AS (\
                 SELECT \
                    lix_json_extract(snapshot_content, 'id') AS id, \
                    lix_json_extract(snapshot_content, 'parent_id') AS parent_id, \
                    lix_json_extract(snapshot_content, 'name') AS name, \
                    lix_json_extract(snapshot_content, 'hidden') AS hidden, \
                    entity_id AS lixcol_entity_id, \
                    schema_key AS lixcol_schema_key, \
                    schema_version AS lixcol_schema_version, \
                    version_id AS lixcol_version_id, \
                    global AS lixcol_global, \
                    change_id AS lixcol_change_id, \
                    metadata AS lixcol_metadata, \
                    created_at AS lixcol_created_at, \
                    updated_at AS lixcol_updated_at, \
                    commit_id AS lixcol_commit_id, \
                    untracked AS lixcol_untracked \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_directory_descriptor' \
                   AND snapshot_content IS NOT NULL\
             ), \
             directory_paths AS (\
                 SELECT \
                    d.id, \
                    d.lixcol_version_id, \
                    '/' || d.name || '/' AS path \
                 FROM directory_descriptor_rows d \
                 WHERE d.parent_id IS NULL \
                 UNION ALL \
                 SELECT \
                    child.id, \
                    child.lixcol_version_id, \
                    parent.path || child.name || '/' AS path \
                 FROM directory_descriptor_rows child \
                 JOIN directory_paths parent \
                   ON parent.id = child.parent_id \
                  AND parent.lixcol_version_id = child.lixcol_version_id\
             ) \
             SELECT \
                d.id, \
                d.parent_id, \
                d.name, \
                dp.path AS path, \
                d.hidden, \
                d.lixcol_entity_id, \
                d.lixcol_schema_key, \
                d.lixcol_schema_version, \
                d.lixcol_global, \
                d.lixcol_change_id, \
                d.lixcol_created_at, \
                d.lixcol_updated_at, \
                d.lixcol_commit_id, \
                d.lixcol_untracked, \
                d.lixcol_metadata \
             FROM directory_descriptor_rows d \
             LEFT JOIN directory_paths dp \
               ON dp.id = d.id \
              AND dp.lixcol_version_id = d.lixcol_version_id \
             WHERE {active_version_scope}",
            active_version_scope = active_version_scope_predicate("d.lixcol_version_id")
        ),
        DIRECTORY_BY_VERSION_VIEW => "WITH RECURSIVE directory_descriptor_rows AS (\
                 SELECT \
                    lix_json_extract(snapshot_content, 'id') AS id, \
                    lix_json_extract(snapshot_content, 'parent_id') AS parent_id, \
                    lix_json_extract(snapshot_content, 'name') AS name, \
                    lix_json_extract(snapshot_content, 'hidden') AS hidden, \
                    entity_id AS lixcol_entity_id, \
                    schema_key AS lixcol_schema_key, \
                    schema_version AS lixcol_schema_version, \
                    version_id AS lixcol_version_id, \
                    global AS lixcol_global, \
                    change_id AS lixcol_change_id, \
                    metadata AS lixcol_metadata, \
                    created_at AS lixcol_created_at, \
                    updated_at AS lixcol_updated_at, \
                    commit_id AS lixcol_commit_id, \
                    untracked AS lixcol_untracked \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_directory_descriptor' \
                   AND snapshot_content IS NOT NULL\
             ), \
             directory_paths AS (\
                 SELECT \
                    d.id, \
                    d.lixcol_version_id, \
                    '/' || d.name || '/' AS path \
                 FROM directory_descriptor_rows d \
                 WHERE d.parent_id IS NULL \
                 UNION ALL \
                 SELECT \
                    child.id, \
                    child.lixcol_version_id, \
                    parent.path || child.name || '/' AS path \
                 FROM directory_descriptor_rows child \
                 JOIN directory_paths parent \
                   ON parent.id = child.parent_id \
                  AND parent.lixcol_version_id = child.lixcol_version_id\
             ) \
             SELECT \
                d.id, \
                d.parent_id, \
                d.name, \
                dp.path AS path, \
                d.hidden, \
                d.lixcol_entity_id, \
                d.lixcol_schema_key, \
                d.lixcol_schema_version, \
                d.lixcol_version_id, \
                d.lixcol_global, \
                d.lixcol_change_id, \
                d.lixcol_created_at, \
                d.lixcol_updated_at, \
                d.lixcol_commit_id, \
                d.lixcol_untracked, \
                d.lixcol_metadata \
             FROM directory_descriptor_rows d \
             LEFT JOIN directory_paths dp \
               ON dp.id = d.id \
              AND dp.lixcol_version_id = d.lixcol_version_id"
            .to_string(),
        DIRECTORY_HISTORY_VIEW => {
            let state_history_view =
                history_state_view_name(view_name).ok_or_else(|| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!("unsupported history view '{view_name}'"),
                })?;
            let state_history_predicates = render_history_pushdown_sql(pushdown, None, true);
            format!(
                "WITH RECURSIVE directory_history_base AS (\
                     SELECT \
                        lix_json_extract(snapshot_content, 'id') AS id, \
                        lix_json_extract(snapshot_content, 'parent_id') AS parent_id, \
                        lix_json_extract(snapshot_content, 'name') AS name, \
                        lix_json_extract(snapshot_content, 'hidden') AS hidden, \
                        entity_id AS lixcol_entity_id, \
                        schema_key AS lixcol_schema_key, \
                        file_id AS lixcol_file_id, \
                        version_id AS lixcol_version_id, \
                        plugin_key AS lixcol_plugin_key, \
                        schema_version AS lixcol_schema_version, \
                        change_id AS lixcol_change_id, \
                        metadata AS lixcol_metadata, \
                        commit_id AS lixcol_commit_id, \
                        root_commit_id AS lixcol_root_commit_id, \
                        depth AS lixcol_depth \
                     FROM {state_history_view} \
                     WHERE schema_key = 'lix_directory_descriptor' \
                       AND snapshot_content IS NOT NULL \
                       {state_history_predicates}\
                 ), \
                 directory_history_path_walk AS (\
                     SELECT \
                        base.id AS target_id, \
                        base.lixcol_root_commit_id AS root_commit_id, \
                        base.lixcol_depth AS target_depth, \
                        base.parent_id AS next_parent_id, \
                        '/' || base.name || '/' AS path, \
                        0 AS step \
                     FROM directory_history_base base \
                     UNION ALL \
                     SELECT \
                        walk.target_id, \
                        walk.root_commit_id, \
                        walk.target_depth, \
                        parent.parent_id AS next_parent_id, \
                        '/' || parent.name || walk.path AS path, \
                        walk.step + 1 AS step \
                     FROM directory_history_path_walk walk \
                     JOIN directory_history_base parent \
                       ON parent.id = walk.next_parent_id \
                      AND parent.lixcol_root_commit_id = walk.root_commit_id \
                      AND parent.lixcol_depth = (\
                          SELECT MIN(candidate.lixcol_depth) \
                          FROM directory_history_base candidate \
                          WHERE candidate.id = walk.next_parent_id \
                            AND candidate.lixcol_root_commit_id = walk.root_commit_id \
                            AND candidate.lixcol_depth >= walk.target_depth\
                      ) \
                     WHERE walk.next_parent_id IS NOT NULL \
                       AND walk.step < 1024 \
                 ), \
                 directory_history_paths AS (\
                     SELECT \
                        walk.target_id, \
                        walk.root_commit_id, \
                        walk.target_depth, \
                        walk.path \
                     FROM directory_history_path_walk walk \
                     JOIN (\
                        SELECT \
                            target_id, \
                            root_commit_id, \
                            target_depth, \
                            MAX(step) AS max_step \
                        FROM directory_history_path_walk \
                        WHERE next_parent_id IS NULL \
                        GROUP BY target_id, root_commit_id, target_depth\
                     ) terminal \
                       ON terminal.target_id = walk.target_id \
                      AND terminal.root_commit_id = walk.root_commit_id \
                      AND terminal.target_depth = walk.target_depth \
                      AND terminal.max_step = walk.step \
                 ) \
                 SELECT \
                    d.id, \
                    d.parent_id, \
                    d.name, \
                    dp.path AS path, \
                    d.hidden, \
                    d.lixcol_entity_id, \
                    d.lixcol_schema_key, \
                    d.lixcol_file_id, \
                    d.lixcol_version_id, \
                    d.lixcol_plugin_key, \
                    d.lixcol_schema_version, \
                    d.lixcol_change_id, \
                    d.lixcol_metadata, \
                    d.lixcol_commit_id, \
                    d.lixcol_root_commit_id, \
                    d.lixcol_depth \
                 FROM directory_history_base d \
                 LEFT JOIN directory_history_paths dp \
                   ON dp.target_id = d.id \
                  AND dp.root_commit_id = d.lixcol_root_commit_id \
                  AND dp.target_depth = d.lixcol_depth",
                state_history_view = state_history_view,
                state_history_predicates = state_history_predicates,
            )
        }
        _ => return Ok(None),
    };

    Ok(Some(parse_single_query(&sql)?))
}

fn build_file_by_version_projection_sql() -> String {
    build_live_file_projection_sql()
}

fn collect_history_pushdown_predicates(
    selection: &Option<Expr>,
    relation_name: &str,
    allow_unqualified: bool,
) -> HistoryPredicatePushdown {
    let mut pushdown = HistoryPredicatePushdown::default();
    let Some(expr) = selection.as_ref() else {
        return pushdown;
    };
    collect_history_pushdown_predicates_from_expr(
        expr,
        relation_name,
        allow_unqualified,
        &mut pushdown.predicates,
    );
    pushdown.has_root_predicate = pushdown.predicates.iter().any(|predicate| match predicate {
        HistoryPredicate::Binary { source_column, .. }
        | HistoryPredicate::InSubquery { source_column, .. }
        | HistoryPredicate::InList { source_column, .. }
        | HistoryPredicate::IsNull { source_column, .. } => *source_column == "root_commit_id",
    });
    pushdown
}

fn collect_history_pushdown_predicates_from_expr(
    expr: &Expr,
    relation_name: &str,
    allow_unqualified: bool,
    predicates: &mut Vec<HistoryPredicate>,
) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_history_pushdown_predicates_from_expr(
                left,
                relation_name,
                allow_unqualified,
                predicates,
            );
            collect_history_pushdown_predicates_from_expr(
                right,
                relation_name,
                allow_unqualified,
                predicates,
            );
        }
        Expr::BinaryOp { left, op, right } => {
            if let Some(source_column) =
                extract_history_source_column(left, relation_name, allow_unqualified)
            {
                predicates.push(HistoryPredicate::Binary {
                    source_column,
                    operator: op.clone(),
                    rhs_sql: right.to_string(),
                });
            } else if let Some(source_column) =
                extract_history_source_column(right, relation_name, allow_unqualified)
            {
                if let Some(inverted) = invert_binary_operator(op.clone()) {
                    predicates.push(HistoryPredicate::Binary {
                        source_column,
                        operator: inverted,
                        rhs_sql: left.to_string(),
                    });
                }
            }
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            if let Some(source_column) =
                extract_history_source_column(expr, relation_name, allow_unqualified)
            {
                predicates.push(HistoryPredicate::InSubquery {
                    source_column,
                    subquery_sql: subquery.to_string(),
                    negated: *negated,
                });
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            if let Some(source_column) =
                extract_history_source_column(expr, relation_name, allow_unqualified)
            {
                predicates.push(HistoryPredicate::InList {
                    source_column,
                    list_sql: list.iter().map(ToString::to_string).collect(),
                    negated: *negated,
                });
            }
        }
        Expr::IsNull(inner) => {
            if let Some(source_column) =
                extract_history_source_column(inner, relation_name, allow_unqualified)
            {
                predicates.push(HistoryPredicate::IsNull {
                    source_column,
                    negated: false,
                });
            }
        }
        Expr::IsNotNull(inner) => {
            if let Some(source_column) =
                extract_history_source_column(inner, relation_name, allow_unqualified)
            {
                predicates.push(HistoryPredicate::IsNull {
                    source_column,
                    negated: true,
                });
            }
        }
        Expr::Nested(value) => collect_history_pushdown_predicates_from_expr(
            value,
            relation_name,
            allow_unqualified,
            predicates,
        ),
        _ => {}
    }
}

fn extract_history_source_column(
    expr: &Expr,
    relation_name: &str,
    allow_unqualified: bool,
) -> Option<&'static str> {
    let column = match expr {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            if !parts[0].value.eq_ignore_ascii_case(relation_name) {
                return None;
            }
            parts[1].value.as_str()
        }
        Expr::Identifier(identifier) if allow_unqualified => identifier.value.as_str(),
        _ => return None,
    };
    map_history_column(column)
}

fn map_history_column(column: &str) -> Option<&'static str> {
    match column {
        "lixcol_root_commit_id" | "root_commit_id" => Some("root_commit_id"),
        "lixcol_version_id" | "version_id" => Some("version_id"),
        _ => None,
    }
}

fn invert_binary_operator(op: BinaryOperator) -> Option<BinaryOperator> {
    match op {
        BinaryOperator::Eq => Some(BinaryOperator::Eq),
        BinaryOperator::NotEq => Some(BinaryOperator::NotEq),
        BinaryOperator::Gt => Some(BinaryOperator::Lt),
        BinaryOperator::GtEq => Some(BinaryOperator::LtEq),
        BinaryOperator::Lt => Some(BinaryOperator::Gt),
        BinaryOperator::LtEq => Some(BinaryOperator::GtEq),
        _ => None,
    }
}

fn render_history_pushdown_sql(
    pushdown: &HistoryPredicatePushdown,
    table_alias: Option<&str>,
    force_active_scope: bool,
) -> String {
    let prefix = table_alias.map_or(String::new(), |alias| format!("{alias}."));
    let mut rendered = pushdown
        .predicates
        .iter()
        .map(|predicate| match predicate {
            HistoryPredicate::Binary {
                source_column,
                operator,
                rhs_sql,
            } => format!(
                " AND {prefix}{column} {op} {rhs}",
                prefix = prefix,
                column = source_column,
                op = operator,
                rhs = rhs_sql
            ),
            HistoryPredicate::InSubquery {
                source_column,
                subquery_sql,
                negated,
            } => {
                let not_sql = if *negated { " NOT" } else { "" };
                format!(
                    " AND {prefix}{column}{not_sql} IN ({subquery})",
                    prefix = prefix,
                    column = source_column,
                    not_sql = not_sql,
                    subquery = subquery_sql
                )
            }
            HistoryPredicate::InList {
                source_column,
                list_sql,
                negated,
            } => {
                let not_sql = if *negated { " NOT" } else { "" };
                format!(
                    " AND {prefix}{column}{not_sql} IN ({list})",
                    prefix = prefix,
                    column = source_column,
                    not_sql = not_sql,
                    list = list_sql.join(", ")
                )
            }
            HistoryPredicate::IsNull {
                source_column,
                negated,
            } => {
                let is_not = if *negated { " NOT" } else { "" };
                format!(
                    " AND {prefix}{column} IS{is_not} NULL",
                    prefix = prefix,
                    column = source_column,
                    is_not = is_not
                )
            }
        })
        .collect::<Vec<_>>()
        .join("");
    rendered.push_str(&render_active_history_scope_sql(
        &prefix,
        pushdown.has_root_predicate,
        force_active_scope,
    ));
    rendered
}

fn history_state_view_name(view_name: &str) -> Option<&'static str> {
    if view_name.eq_ignore_ascii_case(FILE_HISTORY_VIEW)
        || view_name.eq_ignore_ascii_case(FILE_HISTORY_BY_VERSION_VIEW)
        || view_name.eq_ignore_ascii_case(DIRECTORY_HISTORY_VIEW)
    {
        Some("lix_state_history_by_version")
    } else {
        None
    }
}

fn render_active_history_scope_sql(
    column_prefix: &str,
    has_root_predicate: bool,
    force_active_scope: bool,
) -> String {
    if has_root_predicate || !force_active_scope {
        return String::new();
    }
    let version_column = format!("{column_prefix}version_id");
    format!(" AND {}", active_version_scope_predicate(&version_column))
}

fn active_version_scope_predicate(version_column: &str) -> String {
    format!(
        "{version_column} IN (\
         SELECT lix_json_extract(snapshot_content, 'version_id') \
         FROM lix_internal_state_untracked \
         WHERE schema_key = '{schema_key}' \
           AND file_id = '{file_id}' \
           AND version_id = '{storage_version_id}' \
           AND snapshot_content IS NOT NULL\
         )",
        version_column = version_column,
        schema_key = escape_sql_string(active_version_schema_key()),
        file_id = escape_sql_string(active_version_file_id()),
        storage_version_id = escape_sql_string(active_version_storage_version_id()),
    )
}

fn active_version_commit_id_sql() -> String {
    format!(
        "(\
         SELECT lix_json_extract(vp.snapshot_content, 'commit_id') \
         FROM lix_internal_state_materialized_v1_lix_version_pointer vp \
         WHERE vp.schema_key = 'lix_version_pointer' \
           AND vp.version_id = 'global' \
           AND vp.global = true \
           AND vp.snapshot_content IS NOT NULL \
           AND vp.entity_id = (\
               SELECT lix_json_extract(snapshot_content, 'version_id') \
               FROM lix_internal_state_untracked \
               WHERE schema_key = '{schema_key}' \
                 AND file_id = '{file_id}' \
                 AND version_id = '{storage_version_id}' \
                 AND snapshot_content IS NOT NULL \
               LIMIT 1\
           ) \
         LIMIT 1\
        )",
        schema_key = escape_sql_string(active_version_schema_key()),
        file_id = escape_sql_string(active_version_file_id()),
        storage_version_id = escape_sql_string(active_version_storage_version_id()),
    )
}

fn parse_single_query(sql: &str) -> Result<Query, LixError> {
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: error.to_string(),
    })?;
    if statements.len() != 1 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "expected a single SELECT statement".to_string(),
        });
    }
    let statement = statements.remove(0);
    match statement {
        Statement::Query(query) => Ok(*query),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "expected SELECT statement".to_string(),
        }),
    }
}

fn object_name_terminal(name: &ObjectName) -> Option<String> {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.clone())
}

fn default_alias(view_name: &str) -> TableAlias {
    TableAlias {
        explicit: false,
        name: Ident::new(view_name),
        columns: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::{rewrite_query, rewrite_query_with_params};
    use crate::engine::sql::ast::utils::parse_sql_statements;
    use crate::Value;

    #[test]
    fn rewrites_file_view_reads_to_descriptor_projection() {
        let sql = "SELECT id, data FROM lix_file WHERE path = '/src/index.ts'";
        let statements = parse_sql_statements(sql).expect("parse");
        let query = match statements.into_iter().next().expect("statement") {
            sqlparser::ast::Statement::Query(query) => *query,
            _ => panic!("expected query"),
        };
        let rewritten = rewrite_query(query)
            .expect("rewrite")
            .expect("query should be rewritten")
            .to_string();

        assert!(rewritten.contains("bbs.data AS data"));
        assert!(rewritten.contains("binary_blob_ref_rows"));
        assert!(rewritten.contains("schema_key = 'lix_binary_blob_ref'"));
        assert!(rewritten.contains("LEFT JOIN lix_internal_binary_blob_store bbs"));
        assert!(rewritten.contains("lix_internal_state_materialized_v1_lix_file_descriptor"));
        assert!(rewritten.contains("FROM lix_internal_state_untracked"));
        assert!(!rewritten.contains("FROM lix_state_by_version"));
        assert!(rewritten.contains("schema_key = 'lix_file_descriptor'"));
    }

    #[test]
    fn rewrites_simple_file_path_data_query_to_projection() {
        let sql = "SELECT path, data FROM lix_file ORDER BY path";
        let statements = parse_sql_statements(sql).expect("parse");
        let query = match statements.into_iter().next().expect("statement") {
            sqlparser::ast::Statement::Query(query) => *query,
            _ => panic!("expected query"),
        };
        let rewritten = rewrite_query(query)
            .expect("rewrite")
            .expect("query should be rewritten")
            .to_string();

        assert!(rewritten.contains("bbs.data AS data"));
        assert!(rewritten.contains("binary_blob_ref_rows"));
        assert!(rewritten.contains("schema_key = 'lix_binary_blob_ref'"));
        assert!(rewritten.contains("LEFT JOIN lix_internal_binary_blob_store bbs"));
        assert!(rewritten.contains("lix_internal_state_materialized_v1_lix_file_descriptor"));
        assert!(rewritten.contains("FROM lix_internal_state_untracked"));
        assert!(!rewritten.contains("FROM lix_state_by_version"));
        assert!(rewritten.contains("schema_key = 'lix_file_descriptor'"));
        assert!(rewritten.contains("commit_id AS lixcol_commit_id"));
        assert!(!rewritten.contains("LEFT JOIN lix_version v"));
    }

    #[test]
    fn rewrites_simple_file_path_data_point_query_with_param() {
        let sql = "SELECT path, data FROM lix_file WHERE path = ?";
        let statements = parse_sql_statements(sql).expect("parse");
        let query = match statements.into_iter().next().expect("statement") {
            sqlparser::ast::Statement::Query(query) => *query,
            _ => panic!("expected query"),
        };
        let rewritten = rewrite_query_with_params(
            query,
            &[Value::Text("/bench/read/07/file-00007.txt".to_string())],
        )
        .expect("rewrite")
        .expect("query should be rewritten")
        .to_string();

        assert!(rewritten.contains("WHERE path = ?"));
        assert!(rewritten.contains("directory_paths"));
        assert!(!rewritten.contains("dir_level_0"));
        assert!(!rewritten.contains("dir_level_1"));
    }

    #[test]
    fn rewrites_file_history_reads_with_binary_blob_fallback() {
        let sql = "SELECT id, data FROM lix_file_history WHERE id = 'f1'";
        let statements = parse_sql_statements(sql).expect("parse");
        let query = match statements.into_iter().next().expect("statement") {
            sqlparser::ast::Statement::Query(query) => *query,
            _ => panic!("expected query"),
        };
        let rewritten = rewrite_query(query)
            .expect("rewrite")
            .expect("query should be rewritten")
            .to_string();

        assert!(rewritten.contains("COALESCE(fd.data, bbs.data) AS data"));
        assert!(rewritten.contains("binary_blob_ref_history_rows"));
        assert!(rewritten.contains("schema_key = 'lix_binary_blob_ref'"));
        assert!(rewritten.contains("LEFT JOIN lix_internal_binary_blob_store bbs"));
    }

    #[test]
    fn leaves_non_filesystem_reads_untouched() {
        let sql = "SELECT * FROM lix_state";
        let statements = parse_sql_statements(sql).expect("parse");
        let query = match statements.into_iter().next().expect("statement") {
            sqlparser::ast::Statement::Query(query) => *query,
            _ => panic!("expected query"),
        };
        let rewritten = rewrite_query(query).expect("rewrite");
        assert!(rewritten.is_none());
    }

    #[test]
    fn rewrites_lix_file_in_in_subquery() {
        let sql = "SELECT wc.entity_id \
                   FROM lix_working_changes wc \
                   WHERE wc.file_id IN (SELECT f.id FROM lix_file f WHERE f.path = '/hello.md')";
        let statements = parse_sql_statements(sql).expect("parse");
        let query = match statements.into_iter().next().expect("statement") {
            sqlparser::ast::Statement::Query(query) => *query,
            _ => panic!("expected query"),
        };
        let rewritten = rewrite_query(query)
            .expect("rewrite")
            .expect("query should be rewritten")
            .to_string();

        assert!(!rewritten.contains("FROM lix_file"));
        assert!(rewritten.contains("schema_key = 'lix_file_descriptor'"));
    }

    #[test]
    fn rewrites_lix_file_in_exists_subquery() {
        let sql = "SELECT wc.entity_id \
                   FROM lix_working_changes wc \
                   WHERE EXISTS (SELECT 1 FROM lix_file f WHERE f.id = wc.file_id)";
        let statements = parse_sql_statements(sql).expect("parse");
        let query = match statements.into_iter().next().expect("statement") {
            sqlparser::ast::Statement::Query(query) => *query,
            _ => panic!("expected query"),
        };
        let rewritten = rewrite_query(query)
            .expect("rewrite")
            .expect("query should be rewritten")
            .to_string();

        assert!(!rewritten.contains("FROM lix_file"));
        assert!(rewritten.contains("schema_key = 'lix_file_descriptor'"));
    }
}
