use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use jsonschema::JSONSchema;

use crate::catalog::CatalogProjectionRegistry;
use crate::functions::{
    FunctionBindings, LixFunctionProvider, SharedFunctionProvider, SystemFunctionProvider,
};
use crate::schema::{CompiledSchemaCache, SchemaKey};
use crate::sql::{PreparedPublicRead as PreparedRead, SqlCompilerSeed};
use crate::transaction::{
    resolve_binary_blob_writes_in_transaction, BinaryBlobWrite, PendingCommitState, PendingOverlay,
    PublicCommitExecutionOutcome as CommitExecutionOutcome, PublicWriteTxnUnit as WriteTxnUnit,
    WriteExecutionContext,
};
use crate::{LixBackend, LixBackendTransaction, LixError, QueryResult, SqlDialect};

struct WriteCompiledSchemaCache {
    // Write validation can compile JSON schemas while flushing a transaction.
    // Keep the compiled artifacts on the engine-owned write services so later
    // write transactions can reuse them.
    inner: Mutex<HashMap<SchemaKey, Arc<JSONSchema>>>,
}

impl WriteCompiledSchemaCache {
    // Starts empty because builtin/registered schemas are loaded by the write
    // pipeline as needed. The cache stores compiled validators, not source
    // schema definitions.
    fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }
}

impl CompiledSchemaCache for WriteCompiledSchemaCache {
    // Treat a poisoned cache lock as a miss. Schema compilation can be retried
    // by the caller, and the cache should not become the source of truth for
    // write correctness.
    fn get_compiled_schema(&self, key: &SchemaKey) -> Option<Arc<JSONSchema>> {
        self.inner.lock().ok()?.get(key).cloned()
    }

    // Cache insertion is best-effort. If the lock is unavailable, the write can
    // continue; a later transaction may simply recompile the same schema.
    fn insert_compiled_schema(&self, key: SchemaKey, schema: Arc<JSONSchema>) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.insert(key, schema);
        }
    }
}

/// Engine-owned write services used by transaction commit.
///
/// This is concrete state owned by `Engine`, not an execution-scoped context
/// bag. It implements the existing `WriteExecutionContext` trait only because
/// `BufferedWriteTransaction` still expects that compatibility boundary.
pub(crate) struct WriteServices {
    dialect: SqlDialect,
    catalog_projection_registry: &'static CatalogProjectionRegistry,
    // Shared schema compilation cache for write execution.
    //
    // TODO(engine2): decide whether this should live on a dedicated schema
    // service once CanonicalStateContext and schema registry ownership are
    // modeled explicitly.
    schema_cache: WriteCompiledSchemaCache,
}

impl WriteServices {
    pub(crate) fn new(dialect: SqlDialect) -> Self {
        Self {
            dialect,
            catalog_projection_registry: crate::catalog::builtin_catalog_projection_registry(),
            schema_cache: WriteCompiledSchemaCache::new(),
        }
    }
}

#[async_trait(?Send)]
impl WriteExecutionContext for WriteServices {
    fn catalog_projection_registry(&self) -> &CatalogProjectionRegistry {
        self.catalog_projection_registry
    }

    fn compiled_schema_cache(&self) -> &dyn CompiledSchemaCache {
        &self.schema_cache
    }

    fn sql_compiler_seed<'a>(
        &'a self,
        functions: &'a crate::functions::DynFunctionProvider,
        surface_registry: &'a crate::catalog::SurfaceRegistry,
    ) -> SqlCompilerSeed<'a> {
        SqlCompilerSeed {
            dialect: self.dialect,
            functions: crate::functions::clone_boxed_function_provider(functions),
            surface_registry,
        }
    }

    async fn prepare_function_bindings(
        &self,
        _backend: &dyn LixBackend,
    ) -> Result<FunctionBindings, LixError> {
        // TODO(engine2): replace system-only functions with engine-owned
        // runtime function services when deterministic/runtime functions move
        // into the new DAG.
        Ok(FunctionBindings::from_prepared_parts(
            false,
            &SharedFunctionProvider::new(Box::new(SystemFunctionProvider)),
        ))
    }

    async fn execute_pending_overlay_public_read(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        pending_overlay: Option<&dyn PendingOverlay>,
        read: &PreparedRead,
    ) -> Result<QueryResult, LixError> {
        crate::transaction::execute_pending_overlay_public_read_in_transaction(
            transaction,
            pending_overlay,
            read,
        )
        .await
    }

    async fn persist_binary_blob_writes_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        writes: &[BinaryBlobWrite],
    ) -> Result<(), LixError> {
        let resolved = resolve_binary_blob_writes_in_transaction(transaction, writes).await?;
        let cas_writes = resolved
            .iter()
            .map(|write| crate::binary_cas::BinaryBlobWrite {
                file_id: write.file_id.as_str(),
                version_id: write.version_id.as_str(),
                data: write.data.as_slice(),
            })
            .collect::<Vec<_>>();
        crate::binary_cas::persist_blob_writes_in_transaction(transaction, &cas_writes).await
    }

    async fn garbage_collect_unreachable_binary_cas_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
    ) -> Result<(), LixError> {
        crate::binary_cas::garbage_collect_unreachable_in_transaction(transaction).await
    }

    async fn persist_runtime_sequence_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        functions: &SharedFunctionProvider<Box<dyn LixFunctionProvider + Send>>,
    ) -> Result<(), LixError> {
        crate::transaction::persist_runtime_sequence_in_transaction(transaction, functions).await
    }

    // TODO(engine2): rename this shared transaction trait method away from
    // "public". The new engine path should not model public/internal write
    // categories.
    async fn execute_public_commit_write_txn_with_transaction(
        &self,
        _transaction: &mut dyn LixBackendTransaction,
        _unit: &WriteTxnUnit,
        _pending_commit_state: Option<&mut Option<PendingCommitState>>,
    ) -> Result<CommitExecutionOutcome, LixError> {
        // TODO(engine2): rename the shared transaction trait method once the
        // lower buffered pipeline no longer exposes separate write categories.
        // sql2 currently stages semantic writes directly; commit creation is
        // not owned by engine2 yet.
        Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "engine2 write services do not support commit write transactions yet",
        ))
    }
}
