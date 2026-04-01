use crate::contracts::artifacts::{
    ProjectionHydratedRow, ProjectionInput, ProjectionInputRows, ProjectionInputSpec,
    ProjectionStorageKind,
};
use crate::contracts::traits::ProjectionTrait;
use crate::live_state::{
    scan_tracked_rows_with_backend, scan_untracked_rows_with_backend, TrackedScanRequest,
    UntrackedScanRequest,
};
use crate::{LixBackend, LixError};

/// Hydrate the declared tracked/untracked source rows for one projection.
///
/// Phase B is intentionally bounded to the current built-in projection slice,
/// which reads global schema-backed inputs only. The projection definition
/// remains storage-free; `live_state` owns the actual hydration.
pub(crate) async fn hydrate_projection_input_with_backend(
    backend: &dyn LixBackend,
    projection: &dyn ProjectionTrait,
) -> Result<ProjectionInput, LixError> {
    let mut inputs = Vec::new();
    for spec in projection.inputs() {
        inputs.push(hydrate_input_rows_with_backend(backend, spec).await?);
    }
    Ok(ProjectionInput::new(inputs))
}

async fn hydrate_input_rows_with_backend(
    backend: &dyn LixBackend,
    spec: ProjectionInputSpec,
) -> Result<ProjectionInputRows, LixError> {
    let rows = match spec.storage {
        ProjectionStorageKind::Tracked => scan_tracked_rows_with_backend(
            backend,
            &TrackedScanRequest {
                schema_key: spec.schema_key.clone(),
                version_id: crate::version::GLOBAL_VERSION_ID.to_string(),
                constraints: Vec::new(),
                required_columns: Vec::new(),
            },
        )
        .await?
        .into_iter()
        .map(ProjectionHydratedRow::Tracked)
        .collect(),
        ProjectionStorageKind::Untracked => scan_untracked_rows_with_backend(
            backend,
            &UntrackedScanRequest {
                schema_key: spec.schema_key.clone(),
                version_id: crate::version::GLOBAL_VERSION_ID.to_string(),
                constraints: Vec::new(),
                required_columns: Vec::new(),
            },
        )
        .await?
        .into_iter()
        .map(ProjectionHydratedRow::Untracked)
        .collect(),
    };

    Ok(ProjectionInputRows::new(spec, rows))
}
#[cfg(test)]
mod tests {
    use super::hydrate_projection_input_with_backend;
    use crate::contracts::artifacts::ProjectionInputSpec;
    use crate::live_state;
    use crate::projections::builtin_projection_registrations;
    use crate::test_support::{init_test_backend_core, TestSqliteBackend};
    use crate::version::{
        version_descriptor_file_id, version_descriptor_plugin_key, version_descriptor_schema_key,
        version_descriptor_schema_version, version_descriptor_snapshot_content,
        version_ref_file_id, version_ref_plugin_key, version_ref_schema_key,
        version_ref_schema_version, version_ref_snapshot_content,
    };
    use crate::{LixBackend, TransactionMode};

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
        live_state::upsert_bootstrap_tracked_row_in_transaction(
            transaction.as_mut(),
            crate::version::GLOBAL_VERSION_ID,
            version_descriptor_schema_key(),
            version_descriptor_schema_version(),
            version_descriptor_file_id(),
            crate::version::GLOBAL_VERSION_ID,
            version_descriptor_plugin_key(),
            "change-global",
            &version_descriptor_snapshot_content(
                crate::version::GLOBAL_VERSION_ID,
                crate::version::GLOBAL_VERSION_ID,
                true,
            ),
            "2026-04-01T00:00:00Z",
        )
        .await
        .expect("global descriptor should seed");
        live_state::upsert_bootstrap_tracked_row_in_transaction(
            transaction.as_mut(),
            "version-main",
            version_descriptor_schema_key(),
            version_descriptor_schema_version(),
            version_descriptor_file_id(),
            crate::version::GLOBAL_VERSION_ID,
            version_descriptor_plugin_key(),
            "change-main",
            &version_descriptor_snapshot_content(
                "version-main",
                crate::version::DEFAULT_ACTIVE_VERSION_NAME,
                false,
            ),
            "2026-04-01T00:00:01Z",
        )
        .await
        .expect("main descriptor should seed");
        live_state::upsert_bootstrap_untracked_row_in_transaction(
            transaction.as_mut(),
            crate::version::GLOBAL_VERSION_ID,
            version_ref_schema_key(),
            version_ref_schema_version(),
            version_ref_file_id(),
            crate::version::GLOBAL_VERSION_ID,
            version_ref_plugin_key(),
            &version_ref_snapshot_content(crate::version::GLOBAL_VERSION_ID, "commit-global"),
            "2026-04-01T00:00:02Z",
        )
        .await
        .expect("global ref should seed");
        live_state::upsert_bootstrap_untracked_row_in_transaction(
            transaction.as_mut(),
            "version-main",
            version_ref_schema_key(),
            version_ref_schema_version(),
            version_ref_file_id(),
            crate::version::GLOBAL_VERSION_ID,
            version_ref_plugin_key(),
            &version_ref_snapshot_content("version-main", "commit-main"),
            "2026-04-01T00:00:03Z",
        )
        .await
        .expect("main ref should seed");
        transaction
            .commit()
            .await
            .expect("seed transaction should commit");
        let registrations = builtin_projection_registrations();
        let projection = registrations
            .first()
            .expect("builtin registry should expose lix_version")
            .projection();

        let input = hydrate_projection_input_with_backend(&backend, projection)
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
                .any(|id| id == crate::version::GLOBAL_VERSION_ID),
            "tracked descriptor hydration should include global"
        );

        let main_version_id = descriptor_rows
            .iter()
            .find_map(|row| {
                row.values().get("name").and_then(|value| match value {
                    crate::Value::Text(name)
                        if name == crate::version::DEFAULT_ACTIVE_VERSION_NAME =>
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
                .any(|id| id == crate::version::GLOBAL_VERSION_ID),
            "untracked ref hydration should include global"
        );
        assert!(
            ref_ids.iter().any(|id| id == &main_version_id),
            "untracked ref hydration should include the active main version"
        );
    }
}
