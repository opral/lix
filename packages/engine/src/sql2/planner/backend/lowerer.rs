use crate::sql2::backend::{PushdownDecision, PushdownSupport, RejectedPredicate};
use crate::sql2::catalog::{
    SurfaceBinding, SurfaceFamily, SurfaceOverridePredicate, SurfaceOverrideValue, SurfaceVariant,
};
use crate::sql2::core::parser::parse_sql_script;
use crate::sql2::planner::canonicalize::CanonicalizedRead;
use crate::sql2::planner::semantics::effective_state_resolver::{
    EffectiveStatePlan, EffectiveStateRequest,
};
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
    version_descriptor_schema_key, GLOBAL_VERSION_ID,
};
use crate::LixError;
use sqlparser::ast::{
    BinaryOperator, Expr, Ident, Query, Select, SelectItem, SetExpr, Statement, TableAlias,
    TableFactor,
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LoweredReadProgram {
    pub(crate) statements: Vec<Statement>,
    pub(crate) pushdown_decision: PushdownDecision,
}

pub(crate) fn lower_read_for_execution(
    canonicalized: &CanonicalizedRead,
    effective_state_request: Option<&EffectiveStateRequest>,
    effective_state_plan: Option<&EffectiveStatePlan>,
) -> Result<Option<LoweredReadProgram>, LixError> {
    match canonicalized.surface_binding.descriptor.surface_family {
        SurfaceFamily::State => {
            let Some(effective_state_request) = effective_state_request else {
                return Ok(None);
            };
            let Some(effective_state_plan) = effective_state_plan else {
                return Ok(None);
            };
            lower_state_read_for_execution(
                canonicalized,
                effective_state_request,
                effective_state_plan,
            )
            .map(|statement| {
                statement.map(|statement| LoweredReadProgram {
                    statements: vec![statement],
                    pushdown_decision: build_pushdown_decision(effective_state_plan),
                })
            })
        }
        SurfaceFamily::Entity => {
            let Some(effective_state_request) = effective_state_request else {
                return Ok(None);
            };
            let Some(effective_state_plan) = effective_state_plan else {
                return Ok(None);
            };
            lower_entity_read_for_execution(canonicalized, effective_state_request).map(
                |statement| {
                    statement.map(|statement| LoweredReadProgram {
                        statements: vec![statement],
                        pushdown_decision: build_pushdown_decision(effective_state_plan),
                    })
                },
            )
        }
        SurfaceFamily::Change => lower_change_read_for_execution(canonicalized).map(|statement| {
            statement.map(|statement| LoweredReadProgram {
                statements: vec![statement],
                pushdown_decision: change_pushdown_decision(canonicalized),
            })
        }),
        SurfaceFamily::Filesystem | SurfaceFamily::Admin => Ok(None),
    }
}

fn lower_state_read_for_execution(
    canonicalized: &CanonicalizedRead,
    effective_state_request: &EffectiveStateRequest,
    effective_state_plan: &EffectiveStatePlan,
) -> Result<Option<Statement>, LixError> {
    if !state_read_references_exposed_columns(
        &canonicalized.surface_binding,
        effective_state_request,
    ) {
        return Ok(None);
    }

    let Statement::Query(mut query) = canonicalized.bound_statement.statement.clone() else {
        return Ok(None);
    };
    let select = select_mut(query.as_mut())?;
    let TableFactor::Table { alias, .. } = &mut select.from[0].relation else {
        return Ok(None);
    };

    let (pushdown_predicates, residual_selection) =
        split_state_selection_for_pushdown(select.selection.as_ref(), effective_state_plan);
    let Some(derived_query) = build_state_source_query(
        &canonicalized.surface_binding,
        effective_state_request,
        &pushdown_predicates,
    )?
    else {
        return Ok(None);
    };
    let derived_alias = alias.clone().or_else(|| {
        Some(TableAlias {
            explicit: false,
            name: Ident::new(&canonicalized.surface_binding.descriptor.public_name),
            columns: Vec::new(),
        })
    });
    select.from[0].relation = TableFactor::Derived {
        lateral: false,
        subquery: Box::new(derived_query),
        alias: derived_alias,
    };
    select.selection = residual_selection;

    Ok(Some(Statement::Query(query)))
}

fn state_read_references_exposed_columns(
    surface_binding: &SurfaceBinding,
    effective_state_request: &EffectiveStateRequest,
) -> bool {
    let exposed = surface_binding
        .exposed_columns
        .iter()
        .map(|column| column.to_ascii_lowercase())
        .collect::<std::collections::BTreeSet<_>>();
    effective_state_request
        .required_columns
        .iter()
        .all(|column| exposed.contains(&column.to_ascii_lowercase()))
}

fn lower_entity_read_for_execution(
    canonicalized: &CanonicalizedRead,
    effective_state_request: &EffectiveStateRequest,
) -> Result<Option<Statement>, LixError> {
    if query_uses_wildcard_projection(&canonicalized.bound_statement.statement) {
        return Ok(None);
    }

    let Statement::Query(mut query) = canonicalized.bound_statement.statement.clone() else {
        return Ok(None);
    };
    let select = select_mut(query.as_mut())?;
    let TableFactor::Table { alias, .. } = &mut select.from[0].relation else {
        return Ok(None);
    };

    let Some(derived_query) =
        build_entity_source_query(&canonicalized.surface_binding, effective_state_request)?
    else {
        return Ok(None);
    };
    let derived_alias = alias.clone().or_else(|| {
        Some(TableAlias {
            explicit: false,
            name: Ident::new(&canonicalized.surface_binding.descriptor.public_name),
            columns: Vec::new(),
        })
    });
    select.from[0].relation = TableFactor::Derived {
        lateral: false,
        subquery: Box::new(derived_query),
        alias: derived_alias,
    };

    Ok(Some(Statement::Query(query)))
}

fn lower_change_read_for_execution(
    canonicalized: &CanonicalizedRead,
) -> Result<Option<Statement>, LixError> {
    let Statement::Query(mut query) = canonicalized.bound_statement.statement.clone() else {
        return Ok(None);
    };
    let select = select_mut(query.as_mut())?;
    let TableFactor::Table { alias, .. } = &mut select.from[0].relation else {
        return Ok(None);
    };

    let derived_query = build_change_source_query()?;
    let derived_alias = alias.clone().or_else(|| {
        Some(TableAlias {
            explicit: false,
            name: Ident::new(&canonicalized.surface_binding.descriptor.public_name),
            columns: Vec::new(),
        })
    });
    select.from[0].relation = TableFactor::Derived {
        lateral: false,
        subquery: Box::new(derived_query),
        alias: derived_alias,
    };

    Ok(Some(Statement::Query(query)))
}

fn build_state_source_query(
    surface_binding: &SurfaceBinding,
    effective_state_request: &EffectiveStateRequest,
    pushdown_predicates: &[String],
) -> Result<Option<Query>, LixError> {
    let sql = match surface_binding.descriptor.surface_variant {
        SurfaceVariant::Default | SurfaceVariant::ByVersion => build_effective_state_source_sql(
            effective_state_request,
            surface_binding,
            pushdown_predicates,
        )?,
        SurfaceVariant::History => build_state_history_source_sql(pushdown_predicates),
        SurfaceVariant::Active | SurfaceVariant::WorkingChanges => return Ok(None),
    };
    parse_single_query(&sql).map(Some)
}

fn build_entity_source_query(
    surface_binding: &SurfaceBinding,
    effective_state_request: &EffectiveStateRequest,
) -> Result<Option<Query>, LixError> {
    let Some(schema_key) = surface_binding
        .implicit_overrides
        .fixed_schema_key
        .as_deref()
    else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "sql2 entity read lowerer requires fixed schema binding for '{}'",
                surface_binding.descriptor.public_name
            ),
        });
    };

    let projection = entity_projection_sql(surface_binding, effective_state_request);
    let projection = if projection.is_empty() {
        "entity_id AS lixcol_entity_id".to_string()
    } else {
        projection.join(", ")
    };

    let Some(state_source_query) =
        build_state_source_query(surface_binding, effective_state_request, &[])?
    else {
        return Ok(None);
    };
    let mut predicates = Vec::new();
    if !matches!(
        surface_binding.descriptor.surface_variant,
        SurfaceVariant::Default | SurfaceVariant::ByVersion | SurfaceVariant::History
    ) {
        predicates.push(format!(
            "{} = '{}'",
            render_identifier("schema_key"),
            escape_sql_string(schema_key)
        ));
    }
    for predicate in &surface_binding.implicit_overrides.predicate_overrides {
        predicates.push(render_override_predicate(predicate));
    }

    let source_sql = state_source_query.to_string();
    let sql = if predicates.is_empty() {
        format!("SELECT {projection} FROM ({source_sql}) AS state_source")
    } else {
        format!(
            "SELECT {projection} FROM ({source_sql}) AS state_source WHERE {}",
            predicates.join(" AND ")
        )
    };
    parse_single_query(&sql).map(Some)
}

fn build_effective_state_source_sql(
    effective_state_request: &EffectiveStateRequest,
    surface_binding: &SurfaceBinding,
    pushdown_predicates: &[String],
) -> Result<String, LixError> {
    let schema_keys = effective_state_request
        .schema_set
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    if schema_keys.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "sql2 state read lowerer requires a bounded schema set for '{}'",
                surface_binding.descriptor.public_name
            ),
        });
    }

    let (target_version_predicates, source_predicates) =
        split_effective_state_pushdown_predicates(pushdown_predicates);
    let target_versions_cte = match surface_binding.descriptor.surface_variant {
        SurfaceVariant::Default => active_target_versions_cte_sql(),
        SurfaceVariant::ByVersion => {
            explicit_target_versions_cte_sql(&schema_keys, &target_version_predicates)
        }
        _ => {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "state lowerer expected default or by-version surface".to_string(),
            });
        }
    };
    let candidate_rows_sql = effective_state_candidate_rows_sql(&schema_keys, &source_predicates);
    Ok(format!(
        "WITH \
           {target_versions_cte}, \
           commit_by_version AS ( \
             SELECT \
               entity_id AS commit_id, \
               lix_json_extract(snapshot_content, 'change_set_id') AS change_set_id \
             FROM lix_internal_state_materialized_v1_lix_commit \
             WHERE schema_key = 'lix_commit' \
               AND version_id = '{global_version}' \
               AND global = true \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
           ), \
           change_set_element_by_version AS ( \
             SELECT \
               lix_json_extract(snapshot_content, 'change_set_id') AS change_set_id, \
               lix_json_extract(snapshot_content, 'change_id') AS change_id \
             FROM lix_internal_state_materialized_v1_lix_change_set_element \
             WHERE schema_key = 'lix_change_set_element' \
               AND version_id = '{global_version}' \
               AND global = true \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
           ), \
           change_commit_by_change_id AS ( \
             SELECT \
               cse.change_id AS change_id, \
               MAX(cbv.commit_id) AS commit_id \
             FROM change_set_element_by_version cse \
             JOIN commit_by_version cbv \
               ON cbv.change_set_id = cse.change_set_id \
             WHERE cse.change_id IS NOT NULL \
             GROUP BY cse.change_id \
           ), \
           candidates AS ( \
             {candidate_rows_sql} \
           ), \
           ranked AS ( \
             SELECT \
               c.entity_id AS entity_id, \
               c.schema_key AS schema_key, \
               c.file_id AS file_id, \
               c.version_id AS version_id, \
               c.plugin_key AS plugin_key, \
               c.snapshot_content AS snapshot_content, \
               c.schema_version AS schema_version, \
               c.created_at AS created_at, \
               c.updated_at AS updated_at, \
               c.global AS global, \
               c.change_id AS change_id, \
               c.commit_id AS commit_id, \
               c.untracked AS untracked, \
               c.writer_key AS writer_key, \
               c.metadata AS metadata, \
               ROW_NUMBER() OVER ( \
                 PARTITION BY c.version_id, c.entity_id, c.schema_key, c.file_id \
                 ORDER BY \
                   c.precedence ASC, \
                   c.updated_at DESC, \
                   c.created_at DESC, \
                   COALESCE(c.change_id, '') DESC \
               ) AS rn \
             FROM candidates c \
           ) \
         SELECT \
           ranked.entity_id AS entity_id, \
           ranked.schema_key AS schema_key, \
           ranked.file_id AS file_id, \
           ranked.version_id AS version_id, \
           ranked.plugin_key AS plugin_key, \
           ranked.snapshot_content AS snapshot_content, \
           ranked.schema_version AS schema_version, \
           ranked.created_at AS created_at, \
           ranked.updated_at AS updated_at, \
           ranked.global AS global, \
           ranked.change_id AS change_id, \
           ranked.commit_id AS commit_id, \
           ranked.untracked AS untracked, \
           ranked.writer_key AS writer_key, \
           ranked.metadata AS metadata \
         FROM ranked \
         WHERE ranked.rn = 1 \
           AND ranked.snapshot_content IS NOT NULL",
        target_versions_cte = target_versions_cte,
        candidate_rows_sql = candidate_rows_sql,
        global_version = escape_sql_string(GLOBAL_VERSION_ID),
    ))
}

fn active_target_versions_cte_sql() -> String {
    format!(
        "target_versions AS ( \
           SELECT DISTINCT \
             lix_json_extract(snapshot_content, 'version_id') AS version_id \
           FROM lix_internal_state_untracked \
           WHERE schema_key = '{schema_key}' \
             AND file_id = '{file_id}' \
             AND version_id = '{storage_version_id}' \
             AND snapshot_content IS NOT NULL \
         )",
        schema_key = escape_sql_string(active_version_schema_key()),
        file_id = escape_sql_string(active_version_file_id()),
        storage_version_id = escape_sql_string(active_version_storage_version_id()),
    )
}

fn explicit_target_versions_cte_sql(
    schema_keys: &[String],
    target_version_predicates: &[String],
) -> String {
    let version_descriptor_predicates = vec![
        format!(
            "schema_key = '{}'",
            escape_sql_string(version_descriptor_schema_key())
        ),
        format!("version_id = '{}'", escape_sql_string(GLOBAL_VERSION_ID)),
        "global = true".to_string(),
        "is_tombstone = 0".to_string(),
        "snapshot_content IS NOT NULL".to_string(),
    ];
    let schema_local_rows = schema_keys
        .iter()
        .map(|schema_key| {
            format!(
                "SELECT DISTINCT version_id \
                 FROM {table_name} \
                 WHERE global = false \
                   AND version_id <> '{global_version}'",
                table_name =
                    quote_ident(&format!("lix_internal_state_materialized_v1_{schema_key}")),
                global_version = escape_sql_string(GLOBAL_VERSION_ID),
            )
        })
        .chain(schema_keys.iter().map(|schema_key| {
            format!(
                "SELECT DISTINCT version_id \
                 FROM lix_internal_state_untracked \
                 WHERE schema_key = '{schema_key}' \
                   AND global = false \
                   AND version_id <> '{global_version}'",
                schema_key = escape_sql_string(schema_key),
                global_version = escape_sql_string(GLOBAL_VERSION_ID),
            )
        }))
        .collect::<Vec<_>>();
    let all_target_versions = if schema_local_rows.is_empty() {
        String::new()
    } else {
        format!(" UNION {}", schema_local_rows.join(" UNION "))
    };
    let target_versions_where = render_where_clause_sql(target_version_predicates, " WHERE ");
    format!(
        "all_target_versions AS ( \
           SELECT DISTINCT entity_id AS version_id \
           FROM lix_internal_state_materialized_v1_lix_version_descriptor \
           WHERE {version_descriptor_predicates}\
           {all_target_versions} \
         ), \
         target_versions AS ( \
           SELECT version_id \
           FROM all_target_versions \
           {target_versions_where} \
         )",
        version_descriptor_predicates = version_descriptor_predicates.join(" AND "),
        all_target_versions = all_target_versions,
        target_versions_where = target_versions_where,
    )
}

fn effective_state_candidate_rows_sql(
    schema_keys: &[String],
    source_predicates: &[String],
) -> String {
    let tracked_predicates = render_where_clause_sql(source_predicates, " AND ");
    let untracked_predicates = render_where_clause_sql(source_predicates, " AND ");
    schema_keys
        .iter()
        .flat_map(|schema_key| {
            let table_name =
                quote_ident(&format!("lix_internal_state_materialized_v1_{schema_key}"));
            let schema_filter = format!("schema_key = '{}'", escape_sql_string(schema_key));
            [
                format!(
                    "SELECT \
                       t.entity_id AS entity_id, \
                       t.schema_key AS schema_key, \
                       t.file_id AS file_id, \
                       tv.version_id AS version_id, \
                       t.plugin_key AS plugin_key, \
                       t.snapshot_content AS snapshot_content, \
                       t.schema_version AS schema_version, \
                       t.created_at AS created_at, \
                       t.updated_at AS updated_at, \
                       t.global AS global, \
                       t.change_id AS change_id, \
                       cc.commit_id AS commit_id, \
                       false AS untracked, \
                       NULL AS writer_key, \
                       t.metadata AS metadata, \
                       2 AS precedence \
                     FROM {table_name} t \
                     JOIN target_versions tv \
                       ON tv.version_id = t.version_id \
                     LEFT JOIN change_commit_by_change_id cc \
                       ON cc.change_id = t.change_id \
                     WHERE t.global = false{tracked_predicates}",
                    table_name = table_name,
                    tracked_predicates = tracked_predicates,
                ),
                format!(
                    "SELECT \
                       t.entity_id AS entity_id, \
                       t.schema_key AS schema_key, \
                       t.file_id AS file_id, \
                       tv.version_id AS version_id, \
                       t.plugin_key AS plugin_key, \
                       t.snapshot_content AS snapshot_content, \
                       t.schema_version AS schema_version, \
                       t.created_at AS created_at, \
                       t.updated_at AS updated_at, \
                       t.global AS global, \
                       t.change_id AS change_id, \
                       cc.commit_id AS commit_id, \
                       false AS untracked, \
                       NULL AS writer_key, \
                       t.metadata AS metadata, \
                       4 AS precedence \
                     FROM {table_name} t \
                     JOIN target_versions tv \
                       ON t.version_id = '{global_version}' \
                     LEFT JOIN change_commit_by_change_id cc \
                       ON cc.change_id = t.change_id \
                     WHERE t.global = true{tracked_predicates}",
                    table_name = table_name,
                    global_version = escape_sql_string(GLOBAL_VERSION_ID),
                    tracked_predicates = tracked_predicates,
                ),
                format!(
                    "SELECT \
                       u.entity_id AS entity_id, \
                       u.schema_key AS schema_key, \
                       u.file_id AS file_id, \
                       tv.version_id AS version_id, \
                       u.plugin_key AS plugin_key, \
                       u.snapshot_content AS snapshot_content, \
                       u.schema_version AS schema_version, \
                       u.created_at AS created_at, \
                       u.updated_at AS updated_at, \
                       u.global AS global, \
                       NULL AS change_id, \
                       'untracked' AS commit_id, \
                       true AS untracked, \
                       u.writer_key AS writer_key, \
                       u.metadata AS metadata, \
                       1 AS precedence \
                     FROM lix_internal_state_untracked u \
                     JOIN target_versions tv \
                       ON tv.version_id = u.version_id \
                     WHERE {schema_filter} \
                       AND u.global = false{untracked_predicates}",
                    schema_filter = schema_filter,
                    untracked_predicates = untracked_predicates,
                ),
                format!(
                    "SELECT \
                       u.entity_id AS entity_id, \
                       u.schema_key AS schema_key, \
                       u.file_id AS file_id, \
                       tv.version_id AS version_id, \
                       u.plugin_key AS plugin_key, \
                       u.snapshot_content AS snapshot_content, \
                       u.schema_version AS schema_version, \
                       u.created_at AS created_at, \
                       u.updated_at AS updated_at, \
                       u.global AS global, \
                       NULL AS change_id, \
                       'untracked' AS commit_id, \
                       true AS untracked, \
                       u.writer_key AS writer_key, \
                       u.metadata AS metadata, \
                       3 AS precedence \
                     FROM lix_internal_state_untracked u \
                     JOIN target_versions tv \
                       ON u.version_id = '{global_version}' \
                     WHERE {schema_filter} \
                       AND u.global = true{untracked_predicates}",
                    schema_filter = schema_filter,
                    global_version = escape_sql_string(GLOBAL_VERSION_ID),
                    untracked_predicates = untracked_predicates,
                ),
            ]
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ")
}

fn build_state_history_source_sql(pushdown_predicates: &[String]) -> String {
    let requested_root_predicates = history_requested_root_predicates(pushdown_predicates);
    let requested_roots_where = render_where_clause_sql(&requested_root_predicates, " AND ");
    let default_root_scope = if requested_root_predicates.is_empty() {
        "AND ( \
           d.root_commit_id IS NOT NULL \
           OR c.entity_id IN (SELECT root_commit_id FROM default_root_commits) \
         )"
        .to_string()
    } else {
        String::new()
    };
    format!(
        "WITH \
           active_version_rows AS ( \
             SELECT DISTINCT \
               lix_json_extract(snapshot_content, 'version_id') AS version_id \
             FROM lix_internal_state_untracked \
             WHERE schema_key = '{active_schema_key}' \
               AND file_id = '{active_file_id}' \
               AND version_id = '{active_storage_version_id}' \
               AND snapshot_content IS NOT NULL \
           ), \
           default_root_commits AS ( \
             SELECT DISTINCT \
               lix_json_extract(vp.snapshot_content, 'commit_id') AS root_commit_id, \
               vp.entity_id AS root_version_id \
             FROM lix_internal_state_materialized_v1_lix_version_pointer vp \
             JOIN active_version_rows av \
               ON av.version_id = vp.entity_id \
             WHERE vp.schema_key = 'lix_version_pointer' \
               AND vp.version_id = '{global_version}' \
               AND vp.global = true \
               AND vp.is_tombstone = 0 \
               AND vp.snapshot_content IS NOT NULL \
           ), \
           requested_commits AS ( \
             SELECT DISTINCT \
               c.entity_id AS commit_id, \
               COALESCE(d.root_version_id, c.version_id) AS root_version_id \
             FROM lix_internal_state_materialized_v1_lix_commit c \
             LEFT JOIN default_root_commits d \
               ON d.root_commit_id = c.entity_id \
             WHERE c.schema_key = 'lix_commit' \
               AND c.version_id = '{global_version}' \
               AND c.global = true \
               AND c.is_tombstone = 0 \
               AND c.snapshot_content IS NOT NULL{requested_roots_where} \
               {default_root_scope} \
           ), \
           reachable_commits_from_requested AS ( \
             SELECT \
               ancestry.ancestor_id AS commit_id, \
               requested.commit_id AS root_commit_id, \
               requested.root_version_id AS root_version_id, \
               ancestry.depth AS commit_depth \
             FROM requested_commits requested \
             JOIN lix_internal_commit_ancestry ancestry \
               ON ancestry.commit_id = requested.commit_id \
             WHERE ancestry.depth <= 512 \
           ), \
           commit_changesets AS ( \
             SELECT \
               c.entity_id AS commit_id, \
               lix_json_extract(c.snapshot_content, 'change_set_id') AS change_set_id, \
               c.created_at AS commit_created_at, \
               rc.root_commit_id AS root_commit_id, \
               rc.root_version_id AS root_version_id, \
               rc.commit_depth AS commit_depth \
             FROM lix_internal_state_materialized_v1_lix_commit c \
             JOIN reachable_commits_from_requested rc \
               ON rc.commit_id = c.entity_id \
             WHERE c.schema_key = 'lix_commit' \
               AND c.version_id = '{global_version}' \
               AND c.global = true \
               AND c.is_tombstone = 0 \
               AND c.snapshot_content IS NOT NULL \
           ), \
           cse_in_reachable_commits AS ( \
             SELECT \
               lix_json_extract(cse.snapshot_content, 'entity_id') AS target_entity_id, \
               lix_json_extract(cse.snapshot_content, 'file_id') AS target_file_id, \
               lix_json_extract(cse.snapshot_content, 'schema_key') AS target_schema_key, \
               lix_json_extract(cse.snapshot_content, 'change_id') AS target_change_id, \
               cc.commit_id AS origin_commit_id, \
               cc.commit_created_at AS commit_created_at, \
               cc.root_commit_id AS root_commit_id, \
               cc.root_version_id AS root_version_id, \
               cc.commit_depth AS commit_depth \
             FROM lix_internal_state_materialized_v1_lix_change_set_element cse \
             JOIN commit_changesets cc \
               ON lix_json_extract(cse.snapshot_content, 'change_set_id') = cc.change_set_id \
             WHERE cse.schema_key = 'lix_change_set_element' \
               AND cse.version_id = '{global_version}' \
               AND cse.global = true \
               AND cse.is_tombstone = 0 \
               AND cse.snapshot_content IS NOT NULL \
           ), \
           ranked AS ( \
             SELECT \
               ch.entity_id AS entity_id, \
               ch.schema_key AS schema_key, \
               ch.file_id AS file_id, \
               ch.plugin_key AS plugin_key, \
               CASE \
                 WHEN ch.snapshot_id = 'no-content' THEN NULL \
                 ELSE s.content \
               END AS snapshot_content, \
               ch.metadata AS metadata, \
               ch.schema_version AS schema_version, \
               r.target_change_id AS change_id, \
               r.origin_commit_id AS commit_id, \
               r.commit_created_at AS commit_created_at, \
               r.root_commit_id AS root_commit_id, \
               r.root_version_id AS version_id, \
               r.commit_depth AS depth, \
               ROW_NUMBER() OVER ( \
                 PARTITION BY \
                   r.target_entity_id, \
                   r.target_file_id, \
                   r.target_schema_key, \
                   r.root_commit_id, \
                   r.commit_depth \
                 ORDER BY ch.created_at DESC, ch.id DESC \
               ) AS rn \
             FROM cse_in_reachable_commits r \
             JOIN lix_internal_change ch \
               ON ch.id = r.target_change_id \
             LEFT JOIN lix_internal_snapshot s \
               ON s.id = ch.snapshot_id \
           ) \
         SELECT \
           ranked.entity_id AS entity_id, \
           ranked.schema_key AS schema_key, \
           ranked.file_id AS file_id, \
           ranked.plugin_key AS plugin_key, \
           ranked.snapshot_content AS snapshot_content, \
           ranked.metadata AS metadata, \
           ranked.schema_version AS schema_version, \
           ranked.change_id AS change_id, \
           ranked.commit_id AS commit_id, \
           ranked.commit_created_at AS commit_created_at, \
           ranked.root_commit_id AS root_commit_id, \
           ranked.depth AS depth, \
           ranked.version_id AS version_id \
         FROM ranked \
         WHERE ranked.rn = 1 \
           AND ranked.snapshot_content IS NOT NULL",
        active_schema_key = escape_sql_string(active_version_schema_key()),
        active_file_id = escape_sql_string(active_version_file_id()),
        active_storage_version_id = escape_sql_string(active_version_storage_version_id()),
        global_version = escape_sql_string(GLOBAL_VERSION_ID),
        requested_roots_where = requested_roots_where,
        default_root_scope = default_root_scope,
    )
}

fn history_requested_root_predicates(pushdown_predicates: &[String]) -> Vec<String> {
    pushdown_predicates
        .iter()
        .filter(|predicate| predicate.contains("root_commit_id"))
        .map(|predicate| predicate.replace("root_commit_id", "c.entity_id"))
        .collect()
}

fn split_effective_state_pushdown_predicates(
    pushdown_predicates: &[String],
) -> (Vec<String>, Vec<String>) {
    let mut target_version_predicates = Vec::new();
    let mut source_predicates = Vec::new();
    for predicate in pushdown_predicates {
        if predicate.contains("version_id") && !predicate.contains("root_commit_id") {
            target_version_predicates.push(predicate.clone());
        } else {
            source_predicates.push(predicate.clone());
        }
    }
    (target_version_predicates, source_predicates)
}

fn render_where_clause_sql(predicates: &[String], prefix: &str) -> String {
    if predicates.is_empty() {
        String::new()
    } else {
        format!("{prefix}{}", predicates.join(" AND "))
    }
}

fn quote_ident(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn build_change_source_query() -> Result<Query, LixError> {
    parse_single_query(
        "SELECT \
            ch.id AS id, \
            ch.entity_id AS entity_id, \
            ch.schema_key AS schema_key, \
            ch.schema_version AS schema_version, \
            ch.file_id AS file_id, \
            ch.plugin_key AS plugin_key, \
            ch.metadata AS metadata, \
            ch.created_at AS created_at, \
            CASE \
                WHEN ch.snapshot_id = 'no-content' THEN NULL \
                ELSE s.content \
            END AS snapshot_content \
         FROM lix_internal_change ch \
         LEFT JOIN lix_internal_snapshot s \
            ON s.id = ch.snapshot_id",
    )
}

fn entity_projection_sql(
    surface_binding: &SurfaceBinding,
    effective_state_request: &EffectiveStateRequest,
) -> Vec<String> {
    let mut projections = Vec::new();
    for column in &effective_state_request.required_columns {
        let Some(expression) = entity_projection_sql_for_column(surface_binding, column) else {
            continue;
        };
        if !projections.iter().any(|existing| existing == &expression) {
            projections.push(expression);
        }
    }
    projections
}

fn entity_projection_sql_for_column(
    surface_binding: &SurfaceBinding,
    column: &str,
) -> Option<String> {
    if let Some(source_column) =
        entity_hidden_alias_source_column(column, surface_binding.descriptor.surface_variant)
    {
        let alias = render_identifier(column);
        return Some(format!("{source_column} AS {alias}"));
    }

    if surface_binding
        .exposed_columns
        .iter()
        .any(|candidate| candidate.eq_ignore_ascii_case(column))
    {
        let alias = render_identifier(column);
        let path = escape_sql_string(column);
        return Some(format!(
            "lix_json_extract(snapshot_content, '{path}') AS {alias}"
        ));
    }

    None
}

fn entity_hidden_alias_source_column(alias: &str, variant: SurfaceVariant) -> Option<&'static str> {
    match alias.to_ascii_lowercase().as_str() {
        "lixcol_entity_id" => Some("entity_id"),
        "lixcol_schema_key" => Some("schema_key"),
        "lixcol_file_id" => Some("file_id"),
        "lixcol_plugin_key" => Some("plugin_key"),
        "lixcol_schema_version" => Some("schema_version"),
        "lixcol_change_id" => Some("change_id"),
        "lixcol_created_at" => Some("created_at"),
        "lixcol_updated_at" => Some("updated_at"),
        "lixcol_global" => Some("global"),
        "lixcol_writer_key" => Some("writer_key"),
        "lixcol_untracked" => Some("untracked"),
        "lixcol_metadata" => Some("metadata"),
        "lixcol_version_id" if variant != SurfaceVariant::Default => Some("version_id"),
        "lixcol_commit_id" if variant == SurfaceVariant::History => Some("commit_id"),
        "lixcol_root_commit_id" if variant == SurfaceVariant::History => Some("root_commit_id"),
        "lixcol_depth" if variant == SurfaceVariant::History => Some("depth"),
        _ => None,
    }
}

fn entity_source_predicates(
    surface_binding: &SurfaceBinding,
    schema_key: &str,
) -> (String, Vec<String>) {
    let mut predicates = vec![format!(
        "{} = '{}'",
        render_identifier("schema_key"),
        escape_sql_string(schema_key)
    )];

    let source_table = match surface_binding.descriptor.surface_variant {
        SurfaceVariant::Default => {
            if let Some(version_id) = surface_binding
                .implicit_overrides
                .fixed_version_id
                .as_deref()
            {
                predicates.push(format!(
                    "{} = '{}'",
                    render_identifier("version_id"),
                    escape_sql_string(version_id)
                ));
                "lix_state_by_version".to_string()
            } else {
                "lix_state".to_string()
            }
        }
        SurfaceVariant::ByVersion => "lix_state_by_version".to_string(),
        SurfaceVariant::History => "lix_state_history".to_string(),
        SurfaceVariant::Active | SurfaceVariant::WorkingChanges => {
            surface_binding.descriptor.public_name.clone()
        }
    };

    (source_table, predicates)
}

fn render_override_predicate(predicate: &SurfaceOverridePredicate) -> String {
    match &predicate.value {
        SurfaceOverrideValue::Null => {
            format!("{} IS NULL", render_identifier(&predicate.column))
        }
        value => format!(
            "{} = {}",
            render_identifier(&predicate.column),
            render_override_value(value)
        ),
    }
}

fn render_override_value(value: &SurfaceOverrideValue) -> String {
    match value {
        SurfaceOverrideValue::Null => "NULL".to_string(),
        SurfaceOverrideValue::Boolean(value) => value.to_string(),
        SurfaceOverrideValue::Number(value) => value.clone(),
        SurfaceOverrideValue::String(value) => format!("'{}'", escape_sql_string(value)),
    }
}

fn build_pushdown_decision(effective_state_plan: &EffectiveStatePlan) -> PushdownDecision {
    PushdownDecision {
        accepted_predicates: effective_state_plan.pushdown_safe_predicates.clone(),
        rejected_predicates: effective_state_plan
            .residual_predicates
            .iter()
            .map(|predicate| RejectedPredicate {
                predicate: predicate.clone(),
                reason:
                    "day-1 sql2 read lowering keeps this predicate above effective-state resolution"
                        .to_string(),
                support: PushdownSupport::Unsupported,
            })
            .collect(),
        residual_predicates: effective_state_plan.residual_predicates.clone(),
    }
}

fn change_pushdown_decision(canonicalized: &CanonicalizedRead) -> PushdownDecision {
    let residual_predicates = read_predicates_from_query(canonicalized);
    PushdownDecision {
        accepted_predicates: Vec::new(),
        rejected_predicates: residual_predicates
            .iter()
            .map(|predicate| RejectedPredicate {
                predicate: predicate.clone(),
                reason: "sql2 change-scan lowering keeps change predicates above the derived change source".to_string(),
                support: PushdownSupport::Unsupported,
            })
            .collect(),
        residual_predicates,
    }
}

fn split_state_selection_for_pushdown(
    selection: Option<&Expr>,
    effective_state_plan: &EffectiveStatePlan,
) -> (Vec<String>, Option<Expr>) {
    let accepted = effective_state_plan
        .pushdown_safe_predicates
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let Some(selection) = selection else {
        return (Vec::new(), None);
    };

    let mut pushdown = Vec::new();
    let mut residual = Vec::new();
    for predicate in split_conjunctive_predicates(selection) {
        if accepted.contains(&predicate.to_string()) {
            pushdown.push(predicate.to_string());
        } else {
            residual.push(predicate);
        }
    }

    (pushdown, combine_conjunctive_predicates(residual))
}

fn split_conjunctive_predicates(expr: &Expr) -> Vec<Expr> {
    let mut predicates = Vec::new();
    collect_conjunctive_predicates(expr, &mut predicates);
    predicates
}

fn collect_conjunctive_predicates(expr: &Expr, predicates: &mut Vec<Expr>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_conjunctive_predicates(left, predicates);
            collect_conjunctive_predicates(right, predicates);
        }
        Expr::Nested(inner) => collect_conjunctive_predicates(inner, predicates),
        _ => predicates.push(expr.clone()),
    }
}

fn combine_conjunctive_predicates(predicates: Vec<Expr>) -> Option<Expr> {
    let mut predicates = predicates.into_iter();
    let first = predicates.next()?;
    Some(predicates.fold(first, |left, right| Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::And,
        right: Box::new(right),
    }))
}

fn read_predicates_from_query(canonicalized: &CanonicalizedRead) -> Vec<String> {
    let Statement::Query(query) = &canonicalized.bound_statement.statement else {
        return Vec::new();
    };
    let Some(select) = select_ref(query.as_ref()) else {
        return Vec::new();
    };
    let Some(selection) = &select.selection else {
        return Vec::new();
    };

    split_conjunctive_predicates(selection)
        .into_iter()
        .map(|predicate| predicate.to_string())
        .collect()
}

fn parse_single_query(sql: &str) -> Result<Query, LixError> {
    let mut statements = parse_sql_script(sql).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: error.to_string(),
    })?;
    if statements.len() != 1 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "expected a single lowered sql2 read statement".to_string(),
        });
    }
    let Statement::Query(query) = statements.remove(0) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "expected lowered sql2 read to parse as a query".to_string(),
        });
    };
    Ok(*query)
}

fn query_uses_wildcard_projection(statement: &Statement) -> bool {
    let Statement::Query(query) = statement else {
        return false;
    };
    let Some(select) = select_query(query.as_ref()) else {
        return false;
    };
    select.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
        )
    })
}

fn select_query(query: &Query) -> Option<&Select> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    Some(select.as_ref())
}

fn select_ref(query: &Query) -> Option<&Select> {
    select_query(query)
}

fn select_mut(query: &mut Query) -> Result<&mut Select, LixError> {
    let SetExpr::Select(select) = query.body.as_mut() else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "sql2 live read lowering requires a SELECT query".to_string(),
        });
    };
    Ok(select.as_mut())
}

fn render_identifier(value: &str) -> String {
    Ident::new(value).to_string()
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::{lower_read_for_execution, LoweredReadProgram};
    use crate::sql2::catalog::SurfaceRegistry;
    use crate::sql2::core::contracts::{BoundStatement, ExecutionContext};
    use crate::sql2::planner::canonicalize::canonicalize_read;
    use crate::sql2::planner::semantics::dependency_spec::derive_dependency_spec_from_canonicalized_read;
    use crate::sql2::planner::semantics::effective_state_resolver::build_effective_state;
    use crate::{SqlDialect, Value};

    fn lowered_program(registry: &SurfaceRegistry, sql: &str) -> Option<LoweredReadProgram> {
        let mut statements =
            crate::sql2::core::parser::parse_sql_script(sql).expect("SQL should parse");
        let statement = statements.pop().expect("single statement");
        let bound = BoundStatement::from_statement(
            statement,
            Vec::<Value>::new(),
            ExecutionContext::with_dialect(SqlDialect::Sqlite),
        );
        let canonicalized = canonicalize_read(bound, registry).expect("query should canonicalize");
        let dependency_spec = derive_dependency_spec_from_canonicalized_read(&canonicalized);
        let effective_state = build_effective_state(&canonicalized, dependency_spec.as_ref());
        lower_read_for_execution(
            &canonicalized,
            effective_state.as_ref().map(|(request, _)| request),
            effective_state.as_ref().map(|(_, plan)| plan),
        )
        .expect("lowering should succeed")
    }

    #[test]
    fn lowers_builtin_entity_reads_through_state_surfaces() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let lowered = lowered_program(
            &registry,
            "SELECT key, value FROM lix_key_value WHERE key = 'hello'",
        )
        .expect("builtin entity read should lower");
        let lowered_sql = lowered.statements[0].to_string();

        assert!(lowered_sql.contains("FROM (SELECT"));
        assert!(lowered_sql.contains("lix_internal_state_materialized_v1_lix_key_value"));
        assert!(lowered_sql.contains("FROM lix_internal_state_untracked"));
        assert!(lowered_sql.contains("file_id = 'lix'"));
        assert!(lowered_sql.contains("plugin_key = 'lix'"));
        assert_eq!(
            lowered.pushdown_decision.residual_predicates,
            vec!["key = 'hello'".to_string()]
        );
        assert_eq!(
            lowered.pushdown_decision.accepted_predicates,
            Vec::<String>::new()
        );
    }

    #[test]
    fn lowers_dynamic_entity_reads_with_scalar_override_predicates() {
        let mut registry = SurfaceRegistry::with_builtin_surfaces();
        registry.register_dynamic_entity_surfaces(crate::sql2::catalog::DynamicEntitySurfaceSpec {
            schema_key: "message".to_string(),
            visible_columns: vec!["body".to_string(), "id".to_string()],
            fixed_version_id: None,
            predicate_overrides: vec![
                crate::sql2::catalog::SurfaceOverridePredicate {
                    column: "file_id".to_string(),
                    value: crate::sql2::catalog::SurfaceOverrideValue::String("inlang".to_string()),
                },
                crate::sql2::catalog::SurfaceOverridePredicate {
                    column: "plugin_key".to_string(),
                    value: crate::sql2::catalog::SurfaceOverrideValue::String(
                        "inlang_sdk".to_string(),
                    ),
                },
                crate::sql2::catalog::SurfaceOverridePredicate {
                    column: "global".to_string(),
                    value: crate::sql2::catalog::SurfaceOverrideValue::Boolean(true),
                },
            ],
        });

        let lowered = lowered_program(
            &registry,
            "SELECT body, lixcol_global FROM message WHERE id = 'm1'",
        )
        .expect("dynamic entity read should lower");
        let lowered_sql = lowered.statements[0].to_string();

        assert!(lowered_sql.contains("lix_internal_state_materialized_v1_message"));
        assert!(lowered_sql.contains("file_id = 'inlang'"));
        assert!(lowered_sql.contains("plugin_key = 'inlang_sdk'"));
        assert!(lowered_sql.contains("global = true"));
    }

    #[test]
    fn rejects_entity_wildcard_reads_for_live_lowering() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        assert_eq!(
            lowered_program(&registry, "SELECT * FROM lix_key_value"),
            None
        );
    }

    #[test]
    fn lowers_state_reads_through_explicit_source_boundary() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let lowered = lowered_program(
            &registry,
            "SELECT entity_id FROM lix_state WHERE schema_key = 'lix_key_value'",
        )
        .expect("state read should lower");
        let lowered_sql = lowered.statements[0].to_string();

        assert!(lowered_sql.contains("lix_internal_state_materialized_v1_lix_key_value"));
        assert!(!lowered_sql.contains("FROM lix_state"));
        assert!(!lowered_sql.contains(") WHERE schema_key = 'lix_key_value'"));
        assert_eq!(
            lowered.pushdown_decision.accepted_predicates,
            vec!["schema_key = 'lix_key_value'".to_string()]
        );
        assert_eq!(
            lowered.pushdown_decision.residual_predicates,
            Vec::<String>::new()
        );
    }

    #[test]
    fn lowers_change_reads_through_internal_change_sources() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let lowered = lowered_program(
            &registry,
            "SELECT id, schema_key, snapshot_content FROM lix_change WHERE entity_id = 'entity-1'",
        )
        .expect("change read should lower");
        let lowered_sql = lowered.statements[0].to_string();

        assert!(lowered_sql.contains("FROM (SELECT ch.id AS id"));
        assert!(lowered_sql.contains("FROM lix_internal_change ch"));
        assert!(lowered_sql.contains("LEFT JOIN lix_internal_snapshot s"));
        assert_eq!(
            lowered.pushdown_decision.residual_predicates,
            vec!["entity_id = 'entity-1'".to_string()]
        );
    }
}
