//! Neutral read preparation pipeline.
//!
//! `session/*` owns selector orchestration, but selector-read preparation and
//! active-history lookup should not live under the session owner.

use std::collections::BTreeMap;

use async_trait::async_trait;
use sqlparser::ast::Statement;

use crate::backend::QueryExecutor;
use crate::catalog::SurfaceRegistry;
use crate::contracts::PreparedPublicReadArtifact;
use crate::contracts::{PendingView, SqlPreparationMetadataReader};
use crate::session::version_ops::context::load_target_version_history_root_commit_id_with_executor;
use crate::session::version_ops::load_version_head_commit_map_with_executor;
use crate::sql::{
    load_sql_compiler_metadata, prepare_public_read, prepare_public_read_artifact,
    SqlCompilerMetadata,
};
use crate::{LixBackend, LixBackendTransaction, LixError, QueryResult, Value};

pub(crate) struct PreparedPublicReadCollaborators {
    registry: SurfaceRegistry,
    compiler_metadata: SqlCompilerMetadata,
}

pub(crate) async fn bootstrap_prepared_public_read_collaborators(
    backend: &dyn LixBackend,
    pending_view: Option<&dyn PendingView>,
) -> Result<PreparedPublicReadCollaborators, LixError> {
    let registry =
        crate::session::pending_reads::build_surface_registry(backend, pending_view).await?;
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
    let prepared = prepare_public_read(
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

// SqlPreparationMetadataReader blanket impls — session bridges backend to
// session-owned preparation metadata so that sql/ and backend/ stay free of
// session workflow internals.

#[async_trait(?Send)]
impl<T> SqlPreparationMetadataReader for &T
where
    T: LixBackend + ?Sized,
{
    async fn execute_preparation_query(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, LixError> {
        (*self).execute(sql, params).await
    }

    async fn load_current_version_heads_for_preparation(
        &mut self,
    ) -> Result<Option<BTreeMap<String, String>>, LixError> {
        load_current_version_heads_for_preparation_with_executor(self).await
    }

    async fn load_active_history_root_commit_id_for_preparation(
        &mut self,
        active_version_id: &str,
    ) -> Result<Option<String>, LixError> {
        load_active_history_root_commit_id_for_preparation_with_executor(self, active_version_id)
            .await
    }
}

#[async_trait(?Send)]
impl SqlPreparationMetadataReader for Box<dyn LixBackendTransaction + '_> {
    async fn execute_preparation_query(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, LixError> {
        self.as_mut().execute(sql, params).await
    }

    async fn load_current_version_heads_for_preparation(
        &mut self,
    ) -> Result<Option<BTreeMap<String, String>>, LixError> {
        load_current_version_heads_for_preparation_with_executor(self).await
    }

    async fn load_active_history_root_commit_id_for_preparation(
        &mut self,
        active_version_id: &str,
    ) -> Result<Option<String>, LixError> {
        load_active_history_root_commit_id_for_preparation_with_executor(self, active_version_id)
            .await
    }
}

#[async_trait(?Send)]
impl<T> SqlPreparationMetadataReader for &mut T
where
    T: LixBackendTransaction + ?Sized,
{
    async fn execute_preparation_query(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, LixError> {
        (**self).execute(sql, params).await
    }

    async fn load_current_version_heads_for_preparation(
        &mut self,
    ) -> Result<Option<BTreeMap<String, String>>, LixError> {
        load_current_version_heads_for_preparation_with_executor(self).await
    }

    async fn load_active_history_root_commit_id_for_preparation(
        &mut self,
        active_version_id: &str,
    ) -> Result<Option<String>, LixError> {
        load_active_history_root_commit_id_for_preparation_with_executor(self, active_version_id)
            .await
    }
}

async fn load_current_version_heads_for_preparation_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<Option<BTreeMap<String, String>>, LixError> {
    match load_version_head_commit_map_with_executor(executor).await {
        Ok(heads) => Ok(heads),
        Err(error)
            if error
                .description
                .contains("schema 'lix_version' is not stored") =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

async fn load_active_history_root_commit_id_for_preparation_with_executor(
    executor: &mut dyn QueryExecutor,
    active_version_id: &str,
) -> Result<Option<String>, LixError> {
    match load_target_version_history_root_commit_id_with_executor(
        executor,
        Some(active_version_id),
        "active_version_id",
    )
    .await
    {
        Ok(commit_id) => Ok(commit_id),
        Err(error)
            if error
                .description
                .contains("schema 'lix_version' is not stored") =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}
