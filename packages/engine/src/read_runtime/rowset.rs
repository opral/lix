use crate::contracts::artifacts::{
    DerivedRow, PendingViewFilter, PendingViewOrderClause, PendingViewProjection,
    ReadTimeProjectionRead,
};
use crate::contracts::projection::ProjectionRegistry;
use crate::live_state::projection::dispatch::derive_read_time_projection_rows_with_backend;
use crate::{LixBackend, LixError, QueryResult, Value};

/// Bounded rowset execution over engine-supplied `ReadTime` projection rows.
///
/// This runtime intentionally supports only the current compiled operator set:
/// projection, filter, order, limit, and `COUNT(*)`.
pub(crate) async fn execute_read_time_projection_read_with_registry(
    backend: &dyn LixBackend,
    registry: &ProjectionRegistry,
    artifact: &ReadTimeProjectionRead,
) -> Result<QueryResult, LixError> {
    let rows = derive_read_time_projection_rows_with_backend(backend, registry).await?;
    execute_read_time_projection_rows(rows, artifact)
}

fn execute_read_time_projection_rows(
    rows: Vec<DerivedRow>,
    artifact: &ReadTimeProjectionRead,
) -> Result<QueryResult, LixError> {
    let mut rows = rows
        .into_iter()
        .filter(|row| row.surface_name == artifact.surface.public_name())
        .filter(|row| {
            artifact
                .query
                .filters
                .iter()
                .all(|filter| read_time_projection_filter_matches_row(filter, row))
        })
        .collect::<Vec<_>>();

    rows.sort_by(|left, right| {
        compare_read_time_projection_rows(left, right, &artifact.query.order_by)
    });

    if let Some(limit) = artifact.query.limit {
        rows.truncate(limit);
    }

    let columns = artifact
        .query
        .projections
        .iter()
        .map(read_time_projection_output_column)
        .collect::<Vec<_>>();

    if artifact
        .query
        .projections
        .iter()
        .all(|projection| matches!(projection, PendingViewProjection::CountAll { .. }))
    {
        return Ok(QueryResult {
            columns,
            rows: vec![artifact
                .query
                .projections
                .iter()
                .map(|_| Value::Integer(rows.len() as i64))
                .collect()],
        });
    }

    Ok(QueryResult {
        columns,
        rows: rows
            .into_iter()
            .map(|row| {
                artifact
                    .query
                    .projections
                    .iter()
                    .map(|projection| read_time_projection_value(&row, projection))
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn read_time_projection_output_column(projection: &PendingViewProjection) -> String {
    match projection {
        PendingViewProjection::Column { output_column, .. }
        | PendingViewProjection::CountAll { output_column } => output_column.clone(),
    }
}

fn read_time_projection_filter_matches_row(filter: &PendingViewFilter, row: &DerivedRow) -> bool {
    match filter {
        PendingViewFilter::And(filters) => filters
            .iter()
            .all(|filter| read_time_projection_filter_matches_row(filter, row)),
        PendingViewFilter::Or(filters) => filters
            .iter()
            .any(|filter| read_time_projection_filter_matches_row(filter, row)),
        PendingViewFilter::Equals(column, expected) => {
            read_time_projection_row_value(row, column).is_some_and(|actual| actual == *expected)
        }
        PendingViewFilter::In(column, expected) => read_time_projection_row_value(row, column)
            .is_some_and(|actual| expected.iter().any(|candidate| candidate == &actual)),
        PendingViewFilter::IsNull(column) => {
            matches!(
                read_time_projection_row_value(row, column),
                Some(Value::Null) | None
            )
        }
        PendingViewFilter::IsNotNull(column) => !matches!(
            read_time_projection_row_value(row, column),
            Some(Value::Null) | None
        ),
        PendingViewFilter::Like {
            column,
            pattern,
            case_insensitive,
        } => read_time_projection_row_value(row, column)
            .and_then(|actual| projection_filter_text(&actual))
            .is_some_and(|actual| sql_like_matches(&actual, pattern, *case_insensitive)),
    }
}

fn read_time_projection_row_value(row: &DerivedRow, column: &str) -> Option<Value> {
    row.values.get(column).cloned()
}

fn read_time_projection_value(
    row: &DerivedRow,
    projection: &PendingViewProjection,
) -> Result<Value, LixError> {
    match projection {
        PendingViewProjection::Column { source_column, .. } => {
            read_time_projection_row_value(row, source_column).ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "read-time projection query requested unsupported column '{source_column}'"
                    ),
                )
            })
        }
        PendingViewProjection::CountAll { .. } => Ok(Value::Integer(1)),
    }
}

fn compare_read_time_projection_rows(
    left: &DerivedRow,
    right: &DerivedRow,
    order_by: &[PendingViewOrderClause],
) -> std::cmp::Ordering {
    for clause in order_by {
        let ordering = compare_projection_values(
            &read_time_projection_row_value(left, &clause.column),
            &read_time_projection_row_value(right, &clause.column),
        );
        if ordering != std::cmp::Ordering::Equal {
            return if clause.descending {
                ordering.reverse()
            } else {
                ordering
            };
        }
    }

    let identity_ordering = left.identity.cmp(&right.identity);
    if identity_ordering != std::cmp::Ordering::Equal {
        return identity_ordering;
    }

    format!("{:?}", left.values).cmp(&format!("{:?}", right.values))
}

fn compare_projection_values(left: &Option<Value>, right: &Option<Value>) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(left), Some(right)) => format!("{left:?}").cmp(&format!("{right:?}")),
    }
}

fn projection_filter_text(value: &Value) -> Option<String> {
    match value {
        Value::Text(text) => Some(text.clone()),
        Value::Integer(value) => Some(value.to_string()),
        Value::Boolean(value) => Some(if *value { "1" } else { "0" }.to_string()),
        Value::Real(value) => Some(value.to_string()),
        Value::Json(value) => Some(value.to_string()),
        Value::Null | Value::Blob(_) => None,
    }
}

fn sql_like_matches(actual: &str, pattern: &str, case_insensitive: bool) -> bool {
    let actual_chars = if case_insensitive {
        actual.to_ascii_lowercase().chars().collect::<Vec<_>>()
    } else {
        actual.chars().collect::<Vec<_>>()
    };
    let pattern_chars = if case_insensitive {
        pattern.to_ascii_lowercase().chars().collect::<Vec<_>>()
    } else {
        pattern.chars().collect::<Vec<_>>()
    };

    let mut dp = vec![false; actual_chars.len() + 1];
    dp[0] = true;

    for pattern_char in pattern_chars {
        let mut next = vec![false; actual_chars.len() + 1];
        match pattern_char {
            '%' => {
                let mut seen = false;
                for index in 0..=actual_chars.len() {
                    seen |= dp[index];
                    next[index] = seen;
                }
            }
            '_' => {
                for index in 0..actual_chars.len() {
                    if dp[index] {
                        next[index + 1] = true;
                    }
                }
            }
            literal => {
                for index in 0..actual_chars.len() {
                    if dp[index] && actual_chars[index] == literal {
                        next[index + 1] = true;
                    }
                }
            }
        }
        dp = next;
    }

    dp[actual_chars.len()]
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        execute_read_time_projection_read_with_registry, execute_read_time_projection_rows,
    };
    use crate::contracts::artifacts::{
        DerivedRow, PendingViewFilter, PendingViewOrderClause, PendingViewProjection,
        ReadTimeProjectionRead, ReadTimeProjectionReadQuery, ReadTimeProjectionSurface,
        RowIdentity,
    };
    use crate::live_state;
    use crate::projections::builtin_projection_registry;
    use crate::schema::builtin::types::LixCommit;
    use crate::test_support::{
        init_test_backend_core, seed_canonical_change_row, CanonicalChangeSeed, TestSqliteBackend,
    };
    use crate::version::{
        version_descriptor_file_id, version_descriptor_plugin_key, version_descriptor_schema_key,
        version_descriptor_schema_version, version_descriptor_snapshot_content,
        version_ref_file_id, version_ref_plugin_key, version_ref_schema_key,
        version_ref_schema_version, version_ref_snapshot_content,
    };
    use crate::version_inventory_sql::build_admin_version_source_sql_with_current_heads;
    use crate::{LixBackend, LixError, QueryResult, SqlDialect, TransactionMode, Value};

    #[derive(Debug, Clone)]
    struct VersionCaseDescriptor {
        id: &'static str,
        name: Option<&'static str>,
        hidden: bool,
        current_commit_id: Option<&'static str>,
    }

    #[test]
    fn executes_projection_filter_order_and_limit_over_supplied_rows() {
        let artifact = ReadTimeProjectionRead {
            surface: ReadTimeProjectionSurface::LixVersion,
            query: ReadTimeProjectionReadQuery {
                projections: vec![
                    PendingViewProjection::Column {
                        source_column: "id".into(),
                        output_column: "version_id".into(),
                    },
                    PendingViewProjection::Column {
                        source_column: "commit_id".into(),
                        output_column: "commit_id".into(),
                    },
                ],
                filters: vec![PendingViewFilter::And(vec![
                    PendingViewFilter::Like {
                        column: "name".into(),
                        pattern: "main%".into(),
                        case_insensitive: false,
                    },
                    PendingViewFilter::Equals("hidden".into(), Value::Boolean(false)),
                    PendingViewFilter::IsNotNull("commit_id".into()),
                ])],
                order_by: vec![PendingViewOrderClause {
                    column: "id".into(),
                    descending: true,
                }],
                limit: Some(1),
            },
        };

        let actual = execute_read_time_projection_rows(
            vec![
                derived_version_row("version-main", "main", false, Some("commit-main")),
                derived_version_row(
                    "version-main-hidden",
                    "main-hidden",
                    true,
                    Some("commit-hidden"),
                ),
                derived_version_row("version-dev", "dev", false, None),
                DerivedRow::new(
                    "other_surface",
                    BTreeMap::from([("id".into(), Value::Text("ignored".into()))]),
                ),
            ],
            &artifact,
        )
        .expect("bounded rowset query should execute");

        assert_eq!(
            actual,
            QueryResult {
                columns: vec!["version_id".into(), "commit_id".into()],
                rows: vec![vec![
                    Value::Text("version-main".into()),
                    Value::Text("commit-main".into()),
                ]],
            }
        );
    }

    #[test]
    fn counts_rows_after_filters_in_bounded_rowset_runtime() {
        let artifact = ReadTimeProjectionRead {
            surface: ReadTimeProjectionSurface::LixVersion,
            query: ReadTimeProjectionReadQuery {
                projections: vec![PendingViewProjection::CountAll {
                    output_column: "count".into(),
                }],
                filters: vec![PendingViewFilter::IsNotNull("commit_id".into())],
                order_by: vec![PendingViewOrderClause {
                    column: "id".into(),
                    descending: false,
                }],
                limit: None,
            },
        };

        let actual = execute_read_time_projection_rows(
            vec![
                derived_version_row("version-main", "main", false, Some("commit-main")),
                derived_version_row("version-dev", "dev", false, None),
                derived_version_row("version-hidden", "hidden", true, Some("commit-hidden")),
            ],
            &artifact,
        )
        .expect("bounded rowset count query should execute");

        assert_eq!(
            actual,
            QueryResult {
                columns: vec!["count".into()],
                rows: vec![vec![Value::Integer(2)]],
            }
        );
    }

    #[tokio::test]
    async fn executes_lix_version_read_time_projection_query_like_current_admin_sql() {
        let backend = TestSqliteBackend::new();
        let current_heads = seed_version_projection_case(
            &backend,
            &[
                VersionCaseDescriptor {
                    id: "version-main",
                    name: Some(crate::version::DEFAULT_ACTIVE_VERSION_NAME),
                    hidden: false,
                    current_commit_id: Some("commit-main"),
                },
                VersionCaseDescriptor {
                    id: "version-hidden",
                    name: Some("main-hidden"),
                    hidden: true,
                    current_commit_id: Some("commit-hidden"),
                },
            ],
        )
        .await
        .expect("version projection case should seed");

        let artifact = ReadTimeProjectionRead {
            surface: ReadTimeProjectionSurface::LixVersion,
            query: ReadTimeProjectionReadQuery {
                projections: vec![
                    PendingViewProjection::Column {
                        source_column: "id".into(),
                        output_column: "id".into(),
                    },
                    PendingViewProjection::Column {
                        source_column: "name".into(),
                        output_column: "name".into(),
                    },
                    PendingViewProjection::Column {
                        source_column: "hidden".into(),
                        output_column: "hidden".into(),
                    },
                    PendingViewProjection::Column {
                        source_column: "commit_id".into(),
                        output_column: "commit_id".into(),
                    },
                ],
                filters: vec![PendingViewFilter::Like {
                    column: "name".into(),
                    pattern: "main%".into(),
                    case_insensitive: false,
                }],
                order_by: vec![PendingViewOrderClause {
                    column: "id".into(),
                    descending: false,
                }],
                limit: Some(1),
            },
        };

        let actual = execute_read_time_projection_read_with_registry(
            &backend,
            builtin_projection_registry(),
            &artifact,
        )
        .await
        .expect("read-time projection query should execute");
        let expected = current_admin_sql_query_result(
            &backend,
            &current_heads,
            "SELECT id, name, hidden, commit_id \
             FROM ({source_sql}) versions \
             WHERE name LIKE 'main%' \
             ORDER BY id \
             LIMIT 1",
        )
        .await
        .expect("current admin sql should execute");

        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn counts_lix_version_rows_through_read_time_projection_query() {
        let backend = TestSqliteBackend::new();
        let current_heads = seed_version_projection_case(
            &backend,
            &[
                VersionCaseDescriptor {
                    id: "version-main",
                    name: Some(crate::version::DEFAULT_ACTIVE_VERSION_NAME),
                    hidden: false,
                    current_commit_id: Some("commit-main"),
                },
                VersionCaseDescriptor {
                    id: "version-dev",
                    name: Some("dev"),
                    hidden: false,
                    current_commit_id: None,
                },
                VersionCaseDescriptor {
                    id: "version-hidden",
                    name: Some("hidden"),
                    hidden: true,
                    current_commit_id: Some("commit-hidden"),
                },
            ],
        )
        .await
        .expect("version projection case should seed");

        let artifact = ReadTimeProjectionRead {
            surface: ReadTimeProjectionSurface::LixVersion,
            query: ReadTimeProjectionReadQuery {
                projections: vec![PendingViewProjection::CountAll {
                    output_column: "count".into(),
                }],
                filters: vec![PendingViewFilter::IsNotNull("commit_id".into())],
                order_by: Vec::new(),
                limit: None,
            },
        };

        let actual = execute_read_time_projection_read_with_registry(
            &backend,
            builtin_projection_registry(),
            &artifact,
        )
        .await
        .expect("read-time projection count query should execute");
        let expected = current_admin_sql_query_result(
            &backend,
            &current_heads,
            "SELECT COUNT(*) AS count \
             FROM ({source_sql}) versions \
             WHERE commit_id IS NOT NULL",
        )
        .await
        .expect("current admin sql count should execute");

        assert_eq!(actual, expected);
    }

    fn derived_version_row(
        id: &str,
        name: &str,
        hidden: bool,
        commit_id: Option<&str>,
    ) -> DerivedRow {
        DerivedRow::new(
            "lix_version",
            BTreeMap::from([
                ("id".into(), Value::Text(id.into())),
                ("name".into(), Value::Text(name.into())),
                ("hidden".into(), Value::Boolean(hidden)),
                (
                    "commit_id".into(),
                    commit_id
                        .map(|value| Value::Text(value.into()))
                        .unwrap_or(Value::Null),
                ),
            ]),
        )
        .with_identity(RowIdentity {
            schema_key: "lix_version_descriptor".into(),
            version_id: "global".into(),
            entity_id: id.into(),
            file_id: "lix".into(),
        })
    }

    async fn current_admin_sql_query_result(
        backend: &TestSqliteBackend,
        current_heads: &BTreeMap<String, String>,
        template: &str,
    ) -> Result<QueryResult, LixError> {
        let source_sql = build_admin_version_source_sql_with_current_heads(
            SqlDialect::Sqlite,
            Some(current_heads),
        );
        let sql = template.replace("{source_sql}", &source_sql);
        Ok(normalize_sqlite_version_hidden(
            backend.execute(&sql, &[]).await?,
        ))
    }

    fn normalize_sqlite_version_hidden(mut result: QueryResult) -> QueryResult {
        if result
            .columns
            .iter()
            .all(|column| !column.eq_ignore_ascii_case("hidden"))
        {
            return result;
        }

        let hidden_indexes = result
            .columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| column.eq_ignore_ascii_case("hidden").then_some(index))
            .collect::<Vec<_>>();

        for row in &mut result.rows {
            for hidden_index in &hidden_indexes {
                if let Some(value) = row.get_mut(*hidden_index) {
                    match value {
                        Value::Integer(0) => *value = Value::Boolean(false),
                        Value::Integer(1) => *value = Value::Boolean(true),
                        _ => {}
                    }
                }
            }
        }

        result
    }

    async fn seed_version_projection_case(
        backend: &TestSqliteBackend,
        descriptors: &[VersionCaseDescriptor],
    ) -> Result<BTreeMap<String, String>, LixError> {
        init_test_backend_core(backend).await?;
        live_state::register_schema(backend, version_descriptor_schema_key()).await?;
        live_state::register_schema(backend, version_ref_schema_key()).await?;

        let global_head_commit_id = "commit-global-head";
        let mut all_descriptors = vec![VersionCaseDescriptor {
            id: crate::version::GLOBAL_VERSION_ID,
            name: Some(crate::version::GLOBAL_VERSION_ID),
            hidden: true,
            current_commit_id: Some(global_head_commit_id),
        }];
        all_descriptors.extend(descriptors.iter().cloned());

        let mut transaction = backend.begin_transaction(TransactionMode::Write).await?;
        for (index, descriptor) in all_descriptors.iter().enumerate() {
            let timestamp = format!("2026-04-01T00:00:0{}Z", index);
            live_state::upsert_bootstrap_tracked_row_in_transaction(
                transaction.as_mut(),
                descriptor.id,
                version_descriptor_schema_key(),
                version_descriptor_schema_version(),
                version_descriptor_file_id(),
                crate::version::GLOBAL_VERSION_ID,
                version_descriptor_plugin_key(),
                &format!("change-{}", descriptor.id),
                &descriptor_snapshot_json(descriptor),
                &timestamp,
            )
            .await?;

            if let Some(commit_id) = descriptor.current_commit_id {
                live_state::upsert_bootstrap_untracked_row_in_transaction(
                    transaction.as_mut(),
                    descriptor.id,
                    version_ref_schema_key(),
                    version_ref_schema_version(),
                    version_ref_file_id(),
                    crate::version::GLOBAL_VERSION_ID,
                    version_ref_plugin_key(),
                    &version_ref_snapshot_content(descriptor.id, commit_id),
                    &format!("2026-04-01T00:00:1{}Z", index),
                )
                .await?;
            }
        }
        transaction.commit().await?;

        let mut current_heads = BTreeMap::from([(
            crate::version::GLOBAL_VERSION_ID.to_string(),
            global_head_commit_id.to_string(),
        )]);
        for descriptor in descriptors {
            if let Some(commit_id) = descriptor.current_commit_id {
                current_heads.insert(descriptor.id.to_string(), commit_id.to_string());
            }
        }

        let mut change_ids = Vec::new();
        for (index, descriptor) in all_descriptors.iter().enumerate() {
            let change_id = format!("change-{}", descriptor.id);
            let snapshot_id = format!("snapshot-{}", descriptor.id);
            change_ids.push(change_id.clone());
            let snapshot_content = descriptor_snapshot_json(descriptor);
            seed_canonical_change_row(
                backend,
                CanonicalChangeSeed {
                    id: &change_id,
                    entity_id: descriptor.id,
                    schema_key: version_descriptor_schema_key(),
                    schema_version: version_descriptor_schema_version(),
                    file_id: version_descriptor_file_id(),
                    plugin_key: version_descriptor_plugin_key(),
                    snapshot_id: &snapshot_id,
                    snapshot_content: Some(snapshot_content.as_str()),
                    metadata: None,
                    created_at: match index {
                        0 => "2026-04-01T01:00:00Z",
                        1 => "2026-04-01T01:00:01Z",
                        2 => "2026-04-01T01:00:02Z",
                        _ => "2026-04-01T01:00:03Z",
                    },
                },
            )
            .await?;
        }

        let commit_snapshot = serde_json::to_string(&LixCommit {
            id: global_head_commit_id.to_string(),
            change_set_id: Some("cs-global-head".to_string()),
            change_ids,
            author_account_ids: Vec::new(),
            parent_commit_ids: Vec::new(),
        })
        .expect("commit snapshot should serialize");
        seed_canonical_change_row(
            backend,
            CanonicalChangeSeed {
                id: "change-commit-global-head",
                entity_id: global_head_commit_id,
                schema_key: "lix_commit",
                schema_version: "1",
                file_id: "lix",
                plugin_key: "lix",
                snapshot_id: "snapshot-commit-global-head",
                snapshot_content: Some(commit_snapshot.as_str()),
                metadata: None,
                created_at: "2026-04-01T01:10:00Z",
            },
        )
        .await?;

        Ok(current_heads)
    }

    fn descriptor_snapshot_json(descriptor: &VersionCaseDescriptor) -> String {
        match descriptor.name {
            Some(name) => {
                version_descriptor_snapshot_content(descriptor.id, name, descriptor.hidden)
            }
            None => serde_json::json!({
                "id": descriptor.id,
                "hidden": descriptor.hidden,
            })
            .to_string(),
        }
    }
}
