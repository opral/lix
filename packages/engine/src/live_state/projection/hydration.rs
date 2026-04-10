use crate::live_state::shared::version_heads::load_current_committed_version_frontier_with_backend;
use crate::live_state::tracked::{
    scan_rows_with_backend as scan_tracked_rows_with_backend, TrackedScanRequest,
};
use crate::live_state::untracked::{
    scan_rows_with_backend as scan_untracked_rows_with_backend, UntrackedScanRequest,
};
use crate::live_state::{builtin_schema_storage_metadata, BuiltinSchemaStorageLane};
use crate::projections::{
    ProjectionHydratedRow, ProjectionInput, ProjectionInputRows, ProjectionInputSpec,
    ProjectionInputVersionScope, ProjectionStorageKind, ProjectionTrait,
};
use crate::{LixBackend, LixError};

/// Hydrate the declared tracked/untracked source rows for one projection.
///
/// Projection definitions stay storage-free. `live_state` owns the committed
/// state lookups and resolves per-input scope such as global-only or current
/// committed local-version rows.
pub(crate) async fn hydrate_projection_input_with_backend(
    backend: &dyn LixBackend,
    projection: &dyn ProjectionTrait,
) -> Result<ProjectionInput, LixError> {
    let context = ProjectionHydrationContext::for_read_time_with_backend(backend).await?;
    hydrate_projection_input_with_context_with_backend(backend, &context, projection).await
}

async fn hydrate_projection_input_with_context_with_backend(
    backend: &dyn LixBackend,
    context: &ProjectionHydrationContext,
    projection: &dyn ProjectionTrait,
) -> Result<ProjectionInput, LixError> {
    let mut inputs = Vec::new();
    for spec in projection.inputs() {
        inputs.push(hydrate_input_rows_with_backend(backend, context, spec).await?);
    }
    Ok(ProjectionInput::new(inputs))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProjectionHydrationContext {
    current_committed_version_ids: Vec<String>,
}

impl ProjectionHydrationContext {
    async fn for_read_time_with_backend(backend: &dyn LixBackend) -> Result<Self, LixError> {
        let frontier = load_current_committed_version_frontier_with_backend(backend).await?;
        Ok(Self {
            current_committed_version_ids: frontier.version_heads.keys().cloned().collect(),
        })
    }

    fn version_ids_for_spec(&self, spec: &ProjectionInputSpec) -> Vec<String> {
        match spec.version_scope {
            ProjectionInputVersionScope::Global => {
                vec![crate::version_state::GLOBAL_VERSION_ID.to_string()]
            }
            ProjectionInputVersionScope::CurrentCommittedFrontier => {
                self.current_committed_version_ids.clone()
            }
            ProjectionInputVersionScope::SchemaDefault => {
                schema_default_version_ids(spec, &self.current_committed_version_ids)
            }
        }
    }
}

async fn hydrate_input_rows_with_backend(
    backend: &dyn LixBackend,
    context: &ProjectionHydrationContext,
    spec: ProjectionInputSpec,
) -> Result<ProjectionInputRows, LixError> {
    let mut rows = Vec::new();
    for version_id in context.version_ids_for_spec(&spec) {
        match spec.storage {
            ProjectionStorageKind::Tracked => rows.extend(
                scan_tracked_rows_with_backend(
                    backend,
                    &TrackedScanRequest {
                        schema_key: spec.schema_key.clone(),
                        version_id,
                        constraints: Vec::new(),
                        required_columns: Vec::new(),
                    },
                )
                .await?
                .into_iter()
                .map(ProjectionHydratedRow::Tracked),
            ),
            ProjectionStorageKind::Untracked => rows.extend(
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
                .map(ProjectionHydratedRow::Untracked),
            ),
        }
    }

    Ok(ProjectionInputRows::new(spec, rows))
}

fn schema_default_version_ids(
    spec: &ProjectionInputSpec,
    current_committed_version_ids: &[String],
) -> Vec<String> {
    match builtin_schema_storage_metadata(&spec.schema_key).map(|metadata| metadata.storage_lane) {
        Some(BuiltinSchemaStorageLane::Global) | None => {
            vec![crate::version_state::GLOBAL_VERSION_ID.to_string()]
        }
        Some(BuiltinSchemaStorageLane::Local) => current_committed_version_ids.to_vec(),
    }
}
#[cfg(test)]
mod tests {
    use super::hydrate_projection_input_with_backend;
    use crate::catalog::{SurfaceFamily, SurfaceVariant};
    use crate::live_state;
    use crate::live_state::{builtin_schema_storage_metadata, LiveRow};
    use crate::projections::{
        DerivedRow, ProjectionInput, ProjectionInputSpec, ProjectionSurfaceSpec, ProjectionTrait,
    };
    use crate::schema::{LixVersionDescriptor, LixVersionRef};
    use crate::test_support::{init_test_backend_core, TestSqliteBackend};
    use crate::{LixBackend, TransactionMode};

    #[derive(Debug, Clone, Copy)]
    struct TestLixVersionProjection;

    #[derive(Debug, Clone, Copy)]
    struct TestLocalKeyValueProjection;

    impl ProjectionTrait for TestLixVersionProjection {
        fn name(&self) -> &'static str {
            "lix_version"
        }

        fn inputs(&self) -> Vec<ProjectionInputSpec> {
            vec![
                ProjectionInputSpec::tracked("lix_version_descriptor"),
                ProjectionInputSpec::untracked("lix_version_ref"),
            ]
        }

        fn surfaces(&self) -> Vec<ProjectionSurfaceSpec> {
            vec![ProjectionSurfaceSpec::new(
                "lix_version",
                SurfaceFamily::Admin,
                SurfaceVariant::Default,
            )]
        }

        fn derive(&self, _input: &ProjectionInput) -> Result<Vec<DerivedRow>, crate::LixError> {
            Ok(Vec::new())
        }
    }

    impl ProjectionTrait for TestLocalKeyValueProjection {
        fn name(&self) -> &'static str {
            "local_key_value_projection"
        }

        fn inputs(&self) -> Vec<ProjectionInputSpec> {
            vec![ProjectionInputSpec::tracked("lix_key_value")]
        }

        fn surfaces(&self) -> Vec<ProjectionSurfaceSpec> {
            vec![ProjectionSurfaceSpec::new(
                "local_key_value_projection",
                SurfaceFamily::Admin,
                SurfaceVariant::Default,
            )]
        }

        fn derive(&self, _input: &ProjectionInput) -> Result<Vec<DerivedRow>, crate::LixError> {
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
            global: version_id == crate::version_state::GLOBAL_VERSION_ID,
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
            global: version_id == crate::version_state::GLOBAL_VERSION_ID,
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
                crate::version_state::GLOBAL_VERSION_ID,
                &version_descriptor_schema_key(),
                &version_descriptor_schema_version(),
                &version_descriptor_file_id(),
                crate::version_state::GLOBAL_VERSION_ID,
                &version_descriptor_plugin_key(),
                "change-global",
                &version_descriptor_snapshot_content(
                    crate::version_state::GLOBAL_VERSION_ID,
                    crate::version_state::GLOBAL_VERSION_ID,
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
                crate::version_state::GLOBAL_VERSION_ID,
                &version_descriptor_plugin_key(),
                "change-main",
                &version_descriptor_snapshot_content(
                    "version-main",
                    crate::version_state::DEFAULT_ACTIVE_VERSION_NAME,
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
                crate::version_state::GLOBAL_VERSION_ID,
                &version_ref_schema_key(),
                &version_ref_schema_version(),
                &version_ref_file_id(),
                crate::version_state::GLOBAL_VERSION_ID,
                &version_ref_plugin_key(),
                &version_ref_snapshot_content(
                    crate::version_state::GLOBAL_VERSION_ID,
                    "commit-global",
                ),
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
                crate::version_state::GLOBAL_VERSION_ID,
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

        let input = hydrate_projection_input_with_backend(&backend, &projection)
            .await
            .expect("hydration should succeed");

        let descriptor_rows = input
            .rows_for(&ProjectionInputSpec::tracked("lix_version_descriptor"))
            .expect("tracked descriptor rows should be present");
        let version_ref_rows = input
            .rows_for(&ProjectionInputSpec::untracked("lix_version_ref"))
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
            .map(|row| row.identity().entity_id)
            .collect::<Vec<_>>();
        assert!(
            descriptor_ids
                .iter()
                .any(|id| id == crate::version_state::GLOBAL_VERSION_ID),
            "tracked descriptor hydration should include global"
        );

        let main_version_id = descriptor_rows
            .iter()
            .find_map(|row| {
                row.values().get("name").and_then(|value| match value {
                    crate::Value::Text(name)
                        if name == crate::version_state::DEFAULT_ACTIVE_VERSION_NAME =>
                    {
                        Some(row.identity().entity_id)
                    }
                    _ => None,
                })
            })
            .expect("tracked descriptor hydration should include main");

        let ref_ids = version_ref_rows
            .iter()
            .map(|row| row.identity().entity_id)
            .collect::<Vec<_>>();
        assert!(
            ref_ids
                .iter()
                .any(|id| id == crate::version_state::GLOBAL_VERSION_ID),
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
                crate::version_state::GLOBAL_VERSION_ID,
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
                crate::version_state::GLOBAL_VERSION_ID,
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

        let input = hydrate_projection_input_with_backend(&backend, &TestLocalKeyValueProjection)
            .await
            .expect("hydration should succeed");
        let rows = input
            .rows_for(&ProjectionInputSpec::tracked("lix_key_value"))
            .expect("tracked key value rows should be present");

        assert_eq!(
            rows.len(),
            2,
            "local-lane hydration should fan out per frontier version"
        );

        let version_ids = rows
            .iter()
            .map(|row| row.identity().version_id)
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
