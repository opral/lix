//! Neutral read preparation pipeline.
//!
//! `session/*` owns selector orchestration, but selector-read preparation and
//! active-history lookup should not live under the session owner.

use sqlparser::ast::Statement;

use crate::contracts::artifacts::PreparedPublicReadArtifact;
use crate::contracts::surface::SurfaceRegistry;
use crate::contracts::traits::{PendingView, SqlPreparationMetadataReader};
use crate::sql::prepare::{
    load_sql_compiler_metadata, prepare_public_read_artifact,
    try_prepare_public_read_with_registry_and_internal_access, SqlCompilerMetadata,
};
use crate::{LixBackend, LixError, Value};

pub(crate) struct PreparedPublicReadCollaborators {
    registry: SurfaceRegistry,
    compiler_metadata: SqlCompilerMetadata,
}

pub(crate) async fn bootstrap_prepared_public_read_collaborators(
    backend: &dyn LixBackend,
    pending_view: Option<&dyn PendingView>,
) -> Result<PreparedPublicReadCollaborators, LixError> {
    let registry =
        crate::live_state::pending_reads::bootstrap_public_surface_registry_with_pending_transaction_view(
            backend,
            pending_view,
        )
        .await?;
    let compiler_metadata = load_sql_compiler_metadata(backend, &registry).await?;
    Ok(PreparedPublicReadCollaborators {
        registry,
        compiler_metadata,
    })
}

pub(crate) async fn prepare_required_active_public_read_artifact_with_backend(
    backend: &dyn LixBackend,
    collaborators: &PreparedPublicReadCollaborators,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
) -> Result<PreparedPublicReadArtifact, LixError> {
    let mut metadata_reader = backend;
    prepare_required_active_public_read_artifact_with_reader(
        &mut metadata_reader,
        backend.dialect(),
        collaborators,
        parsed_statements,
        params,
        active_version_id,
        writer_key,
    )
    .await
}

async fn prepare_required_active_public_read_artifact_with_reader(
    metadata_reader: &mut dyn SqlPreparationMetadataReader,
    dialect: crate::SqlDialect,
    collaborators: &PreparedPublicReadCollaborators,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
) -> Result<PreparedPublicReadArtifact, LixError> {
    let active_history_root_commit_id = metadata_reader
        .load_active_history_root_commit_id_for_preparation(active_version_id)
        .await?;
    let prepared = try_prepare_public_read_with_registry_and_internal_access(
        dialect,
        &collaborators.registry,
        &collaborators.compiler_metadata,
        parsed_statements,
        params,
        active_version_id,
        active_history_root_commit_id.as_deref(),
        writer_key,
        false,
        None,
    )
    .await?;
    let Some(public_read) = prepared else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "public write selector resolver expected a public read plan",
        ));
    };
    prepare_public_read_artifact(&public_read, dialect)
}
