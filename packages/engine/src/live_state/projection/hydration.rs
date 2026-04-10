use crate::catalog::{
    CatalogProjectionContext, CatalogProjectionDefinition, CatalogProjectionInput,
    CatalogProjectionInputRows, CatalogProjectionInputSpec, CatalogProjectionInputVersionScope,
    CatalogProjectionSourceRow, CatalogProjectionStorageKind,
};
use crate::common::text::escape_sql_string;
use crate::live_state::tracked::{
    scan_rows_with_backend as scan_tracked_rows_with_backend,
    scan_tombstones_with_backend as scan_tracked_tombstones_with_backend, TrackedScanRequest,
};
use crate::live_state::untracked::{
    scan_rows_with_backend as scan_untracked_rows_with_backend, UntrackedScanRequest,
};
use crate::live_state::{builtin_schema_storage_metadata, BuiltinSchemaStorageLane};
use crate::session::version_ops::load_current_committed_version_frontier_with_backend;
use crate::live_state::writer_key::load_writer_key_annotations;
use crate::{LixBackend, LixError, Value};

/// Hydrate the declared tracked/untracked source rows for one projection.
///
/// Projection definitions stay storage-free. `session::version_ops` owns the
/// committed frontier lookups and resolves per-input scope such as global-only
/// or current committed local-version rows.
pub(crate) async fn hydrate_projection_input_with_backend(
    backend: &dyn LixBackend,
    projection: &dyn CatalogProjectionDefinition,
    requested_version_id: Option<&str>,
) -> Result<CatalogProjectionInput, LixError> {
    let context =
        ProjectionHydrationContext::for_read_time_with_backend(backend, requested_version_id)
            .await?;
    hydrate_projection_input_with_context_with_backend(backend, &context, projection).await
}

async fn hydrate_projection_input_with_context_with_backend(
    backend: &dyn LixBackend,
    context: &ProjectionHydrationContext,
    projection: &dyn CatalogProjectionDefinition,
) -> Result<CatalogProjectionInput, LixError> {
    let mut inputs = Vec::new();
    for spec in projection.inputs() {
        inputs.push(hydrate_input_rows_with_backend(backend, context, spec).await?);
    }
    let projection_context =
        build_catalog_projection_context_with_backend(backend, context, &inputs).await?;
    Ok(CatalogProjectionInput::with_context(
        inputs,
        projection_context,
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProjectionHydrationContext {
    requested_version_id: Option<String>,
    current_committed_version_ids: Vec<String>,
    current_version_heads: std::collections::BTreeMap<String, String>,
}

impl ProjectionHydrationContext {
    async fn for_read_time_with_backend(
        backend: &dyn LixBackend,
        requested_version_id: Option<&str>,
    ) -> Result<Self, LixError> {
        let frontier = load_current_committed_version_frontier_with_backend(backend).await?;
        Ok(Self {
            requested_version_id: requested_version_id.map(str::to_string),
            current_committed_version_ids: frontier.version_heads.keys().cloned().collect(),
            current_version_heads: frontier.version_heads,
        })
    }

    fn version_ids_for_spec(
        &self,
        spec: &CatalogProjectionInputSpec,
    ) -> Result<Vec<String>, LixError> {
        match spec.version_scope {
            CatalogProjectionInputVersionScope::Global => {
                Ok(vec![crate::contracts::GLOBAL_VERSION_ID.to_string()])
            }
            CatalogProjectionInputVersionScope::RequestedVersion => self
                .requested_version_id
                .clone()
                .map(|value| vec![value])
                .ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_INVALID_ARGUMENT",
                        format!(
                            "projection input '{}' requires requested version scope",
                            spec.schema_key
                        ),
                    )
                }),
            CatalogProjectionInputVersionScope::CurrentCommittedFrontier => {
                Ok(self.current_committed_version_ids.clone())
            }
            CatalogProjectionInputVersionScope::SchemaDefault => Ok(schema_default_version_ids(
                spec,
                &self.current_committed_version_ids,
            )),
        }
    }
}

async fn hydrate_input_rows_with_backend(
    backend: &dyn LixBackend,
    context: &ProjectionHydrationContext,
    spec: CatalogProjectionInputSpec,
) -> Result<CatalogProjectionInputRows, LixError> {
    let mut rows = Vec::new();
    for version_id in context.version_ids_for_spec(&spec)? {
        match spec.storage {
            CatalogProjectionStorageKind::Tracked => {
                let request = TrackedScanRequest {
                    schema_key: spec.schema_key.clone(),
                    version_id,
                    constraints: Vec::new(),
                    required_columns: Vec::new(),
                };
                rows.extend(
                    scan_tracked_rows_with_backend(backend, &request)
                        .await?
                        .into_iter()
                        .map(|row| {
                            CatalogProjectionSourceRow::new(
                                CatalogProjectionStorageKind::Tracked,
                                crate::contracts::artifacts::RowIdentity::from_tracked_row(&row),
                                row.schema_key.clone(),
                                row.version_id.clone(),
                                row.values,
                            )
                            .with_tombstone(false)
                            .with_live_metadata(
                                row.schema_version,
                                row.plugin_key,
                                row.metadata,
                                row.change_id,
                                row.writer_key,
                                row.global,
                                Some(row.created_at),
                                Some(row.updated_at),
                            )
                        }),
                );
                rows.extend(
                    scan_tracked_tombstones_with_backend(backend, &request)
                        .await?
                        .into_iter()
                        .map(|row| {
                            CatalogProjectionSourceRow::new(
                                CatalogProjectionStorageKind::Tracked,
                                crate::contracts::artifacts::RowIdentity {
                                    entity_id: row.entity_id.clone(),
                                    schema_key: row.schema_key.clone(),
                                    version_id: row.version_id.clone(),
                                    file_id: row.file_id.clone(),
                                },
                                row.schema_key.clone(),
                                row.version_id.clone(),
                                std::collections::BTreeMap::new(),
                            )
                            .with_tombstone(true)
                            .with_live_metadata(
                                row.schema_version.unwrap_or_default(),
                                row.plugin_key.unwrap_or_default(),
                                row.metadata,
                                row.change_id,
                                row.writer_key,
                                row.global,
                                row.created_at,
                                row.updated_at,
                            )
                        }),
                );
            }
            CatalogProjectionStorageKind::Untracked => rows.extend(
                scan_untracked_rows_with_backend(
                    backend,
                    &UntrackedScanRequest {
                        schema_key: spec.schema_key.clone(),
                        version_id,
                        constraints: Vec::new(),
                        required_columns: Vec::new(),
                    },
                )
                .await?
                .into_iter()
                .map(|row| {
                    CatalogProjectionSourceRow::new(
                        CatalogProjectionStorageKind::Untracked,
                        crate::contracts::artifacts::RowIdentity::from_untracked_row(&row),
                        row.schema_key.clone(),
                        row.version_id.clone(),
                        row.values,
                    )
                    .with_tombstone(false)
                    .with_live_metadata(
                        row.schema_version,
                        row.plugin_key,
                        row.metadata,
                        None,
                        row.writer_key,
                        row.global,
                        Some(row.created_at),
                        Some(row.updated_at),
                    )
                }),
            ),
        }
    }

    overlay_writer_keys_on_source_rows_with_backend(backend, &mut rows).await?;

    Ok(CatalogProjectionInputRows::new(spec, rows))
}

async fn overlay_writer_keys_on_source_rows_with_backend(
    backend: &dyn LixBackend,
    rows: &mut [CatalogProjectionSourceRow],
) -> Result<(), LixError> {
    let row_identities = rows
        .iter()
        .map(|row| row.identity().clone())
        .collect::<std::collections::BTreeSet<_>>();
    let annotations = load_writer_key_annotations(backend, &row_identities).await?;
    for row in rows {
        row.set_writer_key(
            annotations
                .get(row.identity())
                .cloned()
                .flatten(),
        );
    }
    Ok(())
}

async fn build_catalog_projection_context_with_backend(
    backend: &dyn LixBackend,
    context: &ProjectionHydrationContext,
    inputs: &[CatalogProjectionInputRows],
) -> Result<CatalogProjectionContext, LixError> {
    let change_ids = inputs
        .iter()
        .flat_map(|input| input.rows.iter())
        .filter_map(|row| row.change_id().map(str::to_string))
        .collect::<std::collections::BTreeSet<_>>();
    let blob_hashes = inputs
        .iter()
        .flat_map(|input| input.rows.iter())
        .filter_map(|row| row.property_text("blob_hash"))
        .collect::<std::collections::BTreeSet<_>>();

    Ok(CatalogProjectionContext {
        requested_version_id: context.requested_version_id.clone(),
        current_committed_version_ids: context.current_committed_version_ids.clone(),
        current_version_heads: context.current_version_heads.clone(),
        change_commit_ids: load_change_commit_ids_with_backend(backend, &change_ids).await?,
        blob_data_by_hash: load_blob_data_by_hash_with_backend(backend, &blob_hashes).await?,
    })
}

async fn load_change_commit_ids_with_backend(
    backend: &dyn LixBackend,
    change_ids: &std::collections::BTreeSet<String>,
) -> Result<std::collections::BTreeMap<String, String>, LixError> {
    if change_ids.is_empty() {
        return Ok(std::collections::BTreeMap::new());
    }

    let in_list = change_ids
        .iter()
        .map(|change_id| format!("'{}'", escape_sql_string(change_id)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "WITH {change_commit_cte} \
         SELECT change_id, commit_id \
         FROM change_commit_by_change_id \
         WHERE change_id IN ({in_list})",
        change_commit_cte =
            crate::sql::build_lazy_change_commit_by_change_id_ctes_sql(backend.dialect(),),
        in_list = in_list,
    );
    let result = backend.execute(&sql, &[]).await?;
    let mut rows = std::collections::BTreeMap::new();
    for row in result.rows {
        let Some(Value::Text(change_id)) = row.first() else {
            continue;
        };
        let Some(Value::Text(commit_id)) = row.get(1) else {
            continue;
        };
        rows.insert(change_id.clone(), commit_id.clone());
    }
    Ok(rows)
}

async fn load_blob_data_by_hash_with_backend(
    backend: &dyn LixBackend,
    blob_hashes: &std::collections::BTreeSet<String>,
) -> Result<std::collections::BTreeMap<String, Option<Vec<u8>>>, LixError> {
    let mut rows = std::collections::BTreeMap::new();
    for blob_hash in blob_hashes {
        rows.insert(
            blob_hash.clone(),
            crate::binary_cas::read::load_binary_blob_data_by_hash(backend, blob_hash).await?,
        );
    }
    Ok(rows)
}

fn schema_default_version_ids(
    spec: &CatalogProjectionInputSpec,
    current_committed_version_ids: &[String],
) -> Vec<String> {
    match builtin_schema_storage_metadata(&spec.schema_key).map(|metadata| metadata.storage_lane) {
        Some(BuiltinSchemaStorageLane::Global) | None => {
            vec![crate::contracts::GLOBAL_VERSION_ID.to_string()]
        }
        Some(BuiltinSchemaStorageLane::Local) => current_committed_version_ids.to_vec(),
    }
}
#[cfg(test)]
mod tests {
    use super::hydrate_projection_input_with_backend;
    use crate::catalog::{
        CatalogDerivedRow, CatalogProjectionDefinition, CatalogProjectionInput,
        CatalogProjectionInputSpec, CatalogProjectionSurfaceSpec, SurfaceFamily, SurfaceVariant,
    };
    use crate::live_state;
    use crate::live_state::{builtin_schema_storage_metadata, LiveRow};
    use crate::schema::{LixVersionDescriptor, LixVersionRef};
    use crate::test_support::{init_test_backend_core, TestSqliteBackend};
    use crate::{LixBackend, TransactionMode};

    #[derive(Debug, Clone, Copy)]
    struct TestLixVersionProjection;

    #[derive(Debug, Clone, Copy)]
    struct TestLocalKeyValueProjection;

    impl CatalogProjectionDefinition for TestLixVersionProjection {
        fn name(&self) -> &'static str {
            "lix_version"
        }

        fn inputs(&self) -> Vec<CatalogProjectionInputSpec> {
            vec![
                CatalogProjectionInputSpec::tracked("lix_version_descriptor"),
                CatalogProjectionInputSpec::untracked("lix_version_ref"),
            ]
        }

        fn surfaces(&self) -> Vec<CatalogProjectionSurfaceSpec> {
            vec![CatalogProjectionSurfaceSpec::new(
                "lix_version",
                SurfaceFamily::Admin,
                SurfaceVariant::Default,
            )]
        }

        fn derive(
            &self,
            _input: &CatalogProjectionInput,
        ) -> Result<Vec<CatalogDerivedRow>, crate::LixError> {
            Ok(Vec::new())
        }
    }

    impl CatalogProjectionDefinition for TestLocalKeyValueProjection {
        fn name(&self) -> &'static str {
            "local_key_value_projection"
        }

        fn inputs(&self) -> Vec<CatalogProjectionInputSpec> {
            vec![CatalogProjectionInputSpec::tracked("lix_key_value")]
        }

        fn surfaces(&self) -> Vec<CatalogProjectionSurfaceSpec> {
            vec![CatalogProjectionSurfaceSpec::new(
                "local_key_value_projection",
                SurfaceFamily::Admin,
                SurfaceVariant::Default,
            )]
        }

        fn derive(
            &self,
            _input: &CatalogProjectionInput,
        ) -> Result<Vec<CatalogDerivedRow>, crate::LixError> {
            Ok(Vec::new())
        }
    }

    fn version_descriptor_schema_key() -> String {
        builtin_schema_storage_metadata("lix_version_descriptor")
            .expect("lix_version_descriptor metadata should exist")
            .schema_key
    }

    fn version_descriptor_schema_version() -> String {
        builtin_schema_storage_metadata("lix_version_descriptor")
            .expect("lix_version_descriptor metadata should exist")
            .schema_version
    }

    fn version_descriptor_file_id() -> String {
        builtin_schema_storage_metadata("lix_version_descriptor")
            .expect("lix_version_descriptor metadata should exist")
            .file_id
    }

    fn version_descriptor_plugin_key() -> String {
        builtin_schema_storage_metadata("lix_version_descriptor")
            .expect("lix_version_descriptor metadata should exist")
            .plugin_key
    }

    fn version_descriptor_snapshot_content(id: &str, name: &str, hidden: bool) -> String {
        serde_json::to_string(&LixVersionDescriptor {
            id: id.to_string(),
            name: Some(name.to_string()),
            hidden,
        })
        .expect("lix_version_descriptor snapshot serialization must succeed")
    }

    fn version_ref_schema_key() -> String {
        builtin_schema_storage_metadata("lix_version_ref")
            .expect("lix_version_ref metadata should exist")
            .schema_key
    }

    fn version_ref_schema_version() -> String {
        builtin_schema_storage_metadata("lix_version_ref")
            .expect("lix_version_ref metadata should exist")
            .schema_version
    }

    fn version_ref_file_id() -> String {
        builtin_schema_storage_metadata("lix_version_ref")
            .expect("lix_version_ref metadata should exist")
            .file_id
    }

    fn version_ref_plugin_key() -> String {
        builtin_schema_storage_metadata("lix_version_ref")
            .expect("lix_version_ref metadata should exist")
            .plugin_key
    }

    fn version_ref_snapshot_content(id: &str, commit_id: &str) -> String {
        serde_json::to_string(&LixVersionRef {
            id: id.to_string(),
            commit_id: commit_id.to_string(),
        })
        .expect("lix_version_ref snapshot serialization must succeed")
    }

    fn key_value_schema_key() -> String {
        builtin_schema_storage_metadata("lix_key_value")
            .expect("lix_key_value metadata should exist")
            .schema_key
    }

    fn key_value_schema_version() -> String {
        builtin_schema_storage_metadata("lix_key_value")
            .expect("lix_key_value metadata should exist")
            .schema_version
    }

    fn key_value_file_id() -> String {
        builtin_schema_storage_metadata("lix_key_value")
            .expect("lix_key_value metadata should exist")
            .file_id
    }

    fn key_value_plugin_key() -> String {
        builtin_schema_storage_metadata("lix_key_value")
            .expect("lix_key_value metadata should exist")
            .plugin_key
    }

    fn key_value_snapshot_content(key: &str, value: serde_json::Value) -> String {
        serde_json::json!({
            "key": key,
            "value": value,
        })
        .to_string()
    }

    fn bootstrap_tracked_live_row(
        entity_id: &str,
        schema_key: &str,
        schema_version: &str,
        file_id: &str,
        version_id: &str,
        plugin_key: &str,
        change_id: &str,
        snapshot_content: &str,
        timestamp: &str,
    ) -> LiveRow {
        LiveRow {
            entity_id: entity_id.to_string(),
            file_id: file_id.to_string(),
            schema_key: schema_key.to_string(),
            schema_version: schema_version.to_string(),
            version_id: version_id.to_string(),
            plugin_key: plugin_key.to_string(),
            metadata: None,
            change_id: Some(change_id.to_string()),
            writer_key: None,
            global: version_id == crate::contracts::GLOBAL_VERSION_ID,
            untracked: false,
            created_at: Some(timestamp.to_string()),
            updated_at: Some(timestamp.to_string()),
            snapshot_content: Some(snapshot_content.to_string()),
        }
    }

    fn bootstrap_untracked_live_row(
        entity_id: &str,
        schema_key: &str,
        schema_version: &str,
        file_id: &str,
        version_id: &str,
        plugin_key: &str,
        snapshot_content: &str,
        timestamp: &str,
    ) -> LiveRow {
        LiveRow {
            entity_id: entity_id.to_string(),
            file_id: file_id.to_string(),
            schema_key: schema_key.to_string(),
            schema_version: schema_version.to_string(),
            version_id: version_id.to_string(),
            plugin_key: plugin_key.to_string(),
            metadata: None,
            change_id: None,
            writer_key: None,
            global: version_id == crate::contracts::GLOBAL_VERSION_ID,
            untracked: true,
            created_at: Some(timestamp.to_string()),
            updated_at: Some(timestamp.to_string()),
            snapshot_content: Some(snapshot_content.to_string()),
        }
    }

    #[tokio::test]
    async fn hydrates_bootstrap_lix_version_projection_input_from_engine_owned_state() {
        let backend = TestSqliteBackend::new();
        init_test_backend_core(&backend)
            .await
            .expect("test backend init should succeed");
        live_state::register_schema(&backend, version_descriptor_schema_key())
            .await
            .expect("version descriptor schema should register");
        live_state::register_schema(&backend, version_ref_schema_key())
            .await
            .expect("version ref schema should register");
        let mut transaction = backend
            .begin_transaction(TransactionMode::Write)
            .await
            .expect("write transaction should begin");
        live_state::write_live_rows(
            transaction.as_mut(),
            &[bootstrap_tracked_live_row(
                crate::contracts::GLOBAL_VERSION_ID,
                &version_descriptor_schema_key(),
                &version_descriptor_schema_version(),
                &version_descriptor_file_id(),
                crate::contracts::GLOBAL_VERSION_ID,
                &version_descriptor_plugin_key(),
                "change-global",
                &version_descriptor_snapshot_content(
                    crate::contracts::GLOBAL_VERSION_ID,
                    crate::contracts::GLOBAL_VERSION_ID,
                    true,
                ),
                "2026-04-01T00:00:00Z",
            )],
        )
        .await
        .expect("global descriptor should seed");
        live_state::write_live_rows(
            transaction.as_mut(),
            &[bootstrap_tracked_live_row(
                "version-main",
                &version_descriptor_schema_key(),
                &version_descriptor_schema_version(),
                &version_descriptor_file_id(),
                crate::contracts::GLOBAL_VERSION_ID,
                &version_descriptor_plugin_key(),
                "change-main",
                &version_descriptor_snapshot_content(
                    "version-main",
                    crate::contracts::DEFAULT_ACTIVE_VERSION_NAME,
                    false,
                ),
                "2026-04-01T00:00:01Z",
            )],
        )
        .await
        .expect("main descriptor should seed");
        live_state::write_live_rows(
            transaction.as_mut(),
            &[bootstrap_untracked_live_row(
                crate::contracts::GLOBAL_VERSION_ID,
                &version_ref_schema_key(),
                &version_ref_schema_version(),
                &version_ref_file_id(),
                crate::contracts::GLOBAL_VERSION_ID,
                &version_ref_plugin_key(),
                &version_ref_snapshot_content(crate::contracts::GLOBAL_VERSION_ID, "commit-global"),
                "2026-04-01T00:00:02Z",
            )],
        )
        .await
        .expect("global ref should seed");
        live_state::write_live_rows(
            transaction.as_mut(),
            &[bootstrap_untracked_live_row(
                "version-main",
                &version_ref_schema_key(),
                &version_ref_schema_version(),
                &version_ref_file_id(),
                crate::contracts::GLOBAL_VERSION_ID,
                &version_ref_plugin_key(),
                &version_ref_snapshot_content("version-main", "commit-main"),
                "2026-04-01T00:00:03Z",
            )],
        )
        .await
        .expect("main ref should seed");
        transaction
            .commit()
            .await
            .expect("seed transaction should commit");
        let projection = TestLixVersionProjection;

        let input = hydrate_projection_input_with_backend(&backend, &projection, None)
            .await
            .expect("hydration should succeed");

        let descriptor_rows = input
            .rows_for(&CatalogProjectionInputSpec::tracked(
                "lix_version_descriptor",
            ))
            .expect("tracked descriptor rows should be present");
        let version_ref_rows = input
            .rows_for(&CatalogProjectionInputSpec::untracked("lix_version_ref"))
            .expect("untracked version ref rows should be present");

        assert!(
            descriptor_rows.len() >= 2,
            "seeded hydration should expose global and main descriptors"
        );
        assert!(
            version_ref_rows.len() >= 2,
            "seeded hydration should expose global and main local refs"
        );

        let descriptor_ids = descriptor_rows
            .iter()
            .map(|row| row.identity().entity_id.clone())
            .collect::<Vec<_>>();
        assert!(
            descriptor_ids
                .iter()
                .any(|id| id == crate::contracts::GLOBAL_VERSION_ID),
            "tracked descriptor hydration should include global"
        );

        let main_version_id = descriptor_rows
            .iter()
            .find_map(|row| {
                row.values().get("name").and_then(|value| match value {
                    crate::Value::Text(name)
                        if name == crate::contracts::DEFAULT_ACTIVE_VERSION_NAME =>
                    {
                        Some(row.identity().entity_id.clone())
                    }
                    _ => None,
                })
            })
            .expect("tracked descriptor hydration should include main");

        let ref_ids = version_ref_rows
            .iter()
            .map(|row| row.identity().entity_id.clone())
            .collect::<Vec<_>>();
        assert!(
            ref_ids
                .iter()
                .any(|id| id == crate::contracts::GLOBAL_VERSION_ID),
            "untracked ref hydration should include global"
        );
        assert!(
            ref_ids.iter().any(|id| id == &main_version_id),
            "untracked ref hydration should include the active main version"
        );
    }

    #[tokio::test]
    async fn hydrates_local_lane_projection_inputs_from_current_committed_frontier() {
        let backend = TestSqliteBackend::new();
        init_test_backend_core(&backend)
            .await
            .expect("test backend init should succeed");
        live_state::register_schema(&backend, version_ref_schema_key())
            .await
            .expect("version ref schema should register");
        live_state::register_schema(&backend, key_value_schema_key())
            .await
            .expect("key value schema should register");
        let mut transaction = backend
            .begin_transaction(TransactionMode::Write)
            .await
            .expect("write transaction should begin");
        live_state::write_live_rows(
            transaction.as_mut(),
            &[bootstrap_untracked_live_row(
                "version-main",
                &version_ref_schema_key(),
                &version_ref_schema_version(),
                &version_ref_file_id(),
                crate::contracts::GLOBAL_VERSION_ID,
                &version_ref_plugin_key(),
                &version_ref_snapshot_content("version-main", "commit-main"),
                "2026-04-02T00:00:00Z",
            )],
        )
        .await
        .expect("main ref should seed");
        live_state::write_live_rows(
            transaction.as_mut(),
            &[bootstrap_untracked_live_row(
                "version-dev",
                &version_ref_schema_key(),
                &version_ref_schema_version(),
                &version_ref_file_id(),
                crate::contracts::GLOBAL_VERSION_ID,
                &version_ref_plugin_key(),
                &version_ref_snapshot_content("version-dev", "commit-dev"),
                "2026-04-02T00:00:01Z",
            )],
        )
        .await
        .expect("dev ref should seed");
        live_state::write_live_rows(
            transaction.as_mut(),
            &[bootstrap_tracked_live_row(
                "theme",
                &key_value_schema_key(),
                &key_value_schema_version(),
                &key_value_file_id(),
                "version-main",
                &key_value_plugin_key(),
                "change-main",
                &key_value_snapshot_content("theme", serde_json::Value::String("dark".to_string())),
                "2026-04-02T00:00:02Z",
            )],
        )
        .await
        .expect("main key value row should seed");
        live_state::write_live_rows(
            transaction.as_mut(),
            &[bootstrap_tracked_live_row(
                "theme",
                &key_value_schema_key(),
                &key_value_schema_version(),
                &key_value_file_id(),
                "version-dev",
                &key_value_plugin_key(),
                "change-dev",
                &key_value_snapshot_content(
                    "theme",
                    serde_json::Value::String("light".to_string()),
                ),
                "2026-04-02T00:00:03Z",
            )],
        )
        .await
        .expect("dev key value row should seed");
        transaction
            .commit()
            .await
            .expect("seed transaction should commit");

        let input =
            hydrate_projection_input_with_backend(&backend, &TestLocalKeyValueProjection, None)
                .await
                .expect("hydration should succeed");
        let rows = input
            .rows_for(&CatalogProjectionInputSpec::tracked("lix_key_value"))
            .expect("tracked key value rows should be present");

        assert_eq!(
            rows.len(),
            2,
            "local-lane hydration should fan out per frontier version"
        );

        let version_ids = rows
            .iter()
            .map(|row| row.identity().version_id.clone())
            .collect::<Vec<_>>();
        assert!(
            version_ids
                .iter()
                .any(|version_id| version_id == "version-main"),
            "local-lane hydration should include the main version row"
        );
        assert!(
            version_ids
                .iter()
                .any(|version_id| version_id == "version-dev"),
            "local-lane hydration should include the dev version row"
        );
    }
}
