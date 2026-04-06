use crate::backend::{LixBackend, QueryExecutor};
use crate::canonical::read::{version_exists_with_backend, version_exists_with_executor};
use crate::runtime::TransactionBackendAdapter;
use crate::session::workspace::require_workspace_active_version_id;
use crate::version::load_committed_version_ref_with_executor;
use crate::write_runtime::commit::{
    CreateCommitExpectedHead, CreateCommitIdempotencyKey, CreateCommitPreconditions,
    CreateCommitWriteLane,
};
use crate::{LixError, SessionTransaction};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VersionContextSource {
    ExplicitArgument,
    SessionActiveVersion,
    WorkspaceActiveVersion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedVersionTarget {
    pub(crate) version_id: String,
    pub(crate) source: VersionContextSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VersionContext {
    pub(crate) target: ResolvedVersionTarget,
    head_commit_id: String,
    history_root_commit_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VersionContextPair {
    pub(crate) source: VersionContext,
    pub(crate) target: VersionContext,
}

impl VersionContext {
    pub(crate) fn version_id(&self) -> &str {
        &self.target.version_id
    }

    pub(crate) fn head_commit_id(&self) -> &str {
        &self.head_commit_id
    }

    pub(crate) fn history_root_commit_id(&self) -> &str {
        &self.history_root_commit_id
    }

    pub(crate) fn write_lane(&self) -> CreateCommitWriteLane {
        CreateCommitWriteLane::Version(self.version_id().to_string())
    }
}

pub(crate) fn normalize_required_version_id(
    version_id: &str,
    field_name: &str,
) -> Result<String, LixError> {
    if version_id.trim().is_empty() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("{field_name} must be a non-empty string"),
        ));
    }
    Ok(version_id.to_string())
}

pub(crate) fn normalize_optional_version_id(
    version_id: Option<&str>,
    field_name: &str,
) -> Result<Option<String>, LixError> {
    match version_id {
        Some(version_id) => Ok(Some(normalize_required_version_id(version_id, field_name)?)),
        None => Ok(None),
    }
}

pub(crate) async fn ensure_version_exists_with_backend(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<(), LixError> {
    if !version_exists_with_backend(backend, version_id).await? {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("version '{version_id}' does not exist"),
        ));
    }
    Ok(())
}

pub(crate) async fn ensure_version_exists_with_executor(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
) -> Result<(), LixError> {
    if !version_exists_with_executor(executor, version_id).await? {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("version '{version_id}' does not exist"),
        ));
    }
    Ok(())
}

pub(crate) async fn resolve_target_version_in_transaction(
    tx: &mut SessionTransaction<'_>,
    requested_version_id: Option<&str>,
    field_name: &str,
) -> Result<ResolvedVersionTarget, LixError> {
    match normalize_optional_version_id(requested_version_id, field_name)? {
        Some(version_id) => Ok(ResolvedVersionTarget {
            version_id,
            source: VersionContextSource::ExplicitArgument,
        }),
        None => Ok(ResolvedVersionTarget {
            version_id: tx.context.active_version_id.clone(),
            source: VersionContextSource::SessionActiveVersion,
        }),
    }
}

pub(crate) async fn resolve_target_version_with_backend(
    backend: &dyn LixBackend,
    requested_version_id: Option<&str>,
    field_name: &str,
) -> Result<ResolvedVersionTarget, LixError> {
    match normalize_optional_version_id(requested_version_id, field_name)? {
        Some(version_id) => Ok(ResolvedVersionTarget {
            version_id,
            source: VersionContextSource::ExplicitArgument,
        }),
        None => Ok(ResolvedVersionTarget {
            version_id: require_workspace_active_version_id(backend).await?,
            source: VersionContextSource::WorkspaceActiveVersion,
        }),
    }
}

pub(crate) async fn load_version_context_with_executor(
    executor: &mut dyn QueryExecutor,
    target: ResolvedVersionTarget,
) -> Result<Option<VersionContext>, LixError> {
    let Some(version_ref) =
        load_committed_version_ref_with_executor(executor, &target.version_id).await?
    else {
        return Ok(None);
    };
    if version_ref.commit_id.trim().is_empty() {
        return Ok(None);
    }
    // The replica-local version head currently names the committed tip that
    // also anchors version-scoped history reads, so the resolved head and
    // history root are explicit facts even though they coincide today.
    Ok(Some(VersionContext {
        target: ResolvedVersionTarget {
            version_id: version_ref.version_id,
            source: target.source,
        },
        history_root_commit_id: version_ref.commit_id.clone(),
        head_commit_id: version_ref.commit_id,
    }))
}

pub(crate) async fn require_version_context_with_executor(
    executor: &mut dyn QueryExecutor,
    target: ResolvedVersionTarget,
    subject: &str,
) -> Result<VersionContext, LixError> {
    ensure_version_exists_with_executor(executor, &target.version_id).await?;
    let version_id = target.version_id.clone();
    let Some(context) = load_version_context_with_executor(executor, target).await? else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("{subject} '{version_id}' has no committed head"),
        ));
    };
    Ok(context)
}

pub(crate) async fn require_target_version_context_in_transaction(
    tx: &mut SessionTransaction<'_>,
    requested_version_id: Option<&str>,
    field_name: &str,
    subject: &str,
) -> Result<VersionContext, LixError> {
    let target =
        resolve_target_version_in_transaction(tx, requested_version_id, field_name).await?;
    let mut executor = TransactionBackendAdapter::new(tx.backend_transaction_mut()?);
    require_version_context_with_executor(&mut executor, target, subject).await
}

pub(crate) async fn require_target_version_context_with_backend(
    backend: &dyn LixBackend,
    requested_version_id: Option<&str>,
    field_name: &str,
    subject: &str,
) -> Result<VersionContext, LixError> {
    let target =
        resolve_target_version_with_backend(backend, requested_version_id, field_name).await?;
    let mut executor = backend;
    require_version_context_with_executor(&mut executor, target, subject).await
}

pub(crate) async fn load_target_version_context_with_backend(
    backend: &dyn LixBackend,
    requested_version_id: Option<&str>,
    field_name: &str,
) -> Result<Option<VersionContext>, LixError> {
    let target =
        resolve_target_version_with_backend(backend, requested_version_id, field_name).await?;
    let mut executor = backend;
    load_version_context_with_executor(&mut executor, target).await
}

pub(crate) async fn load_target_version_history_root_commit_id_with_backend(
    backend: &dyn LixBackend,
    requested_version_id: Option<&str>,
    field_name: &str,
) -> Result<Option<String>, LixError> {
    Ok(
        load_target_version_context_with_backend(backend, requested_version_id, field_name)
            .await?
            .map(|context| context.history_root_commit_id().to_string()),
    )
}

pub(crate) async fn require_version_context_pair_in_transaction(
    tx: &mut SessionTransaction<'_>,
    source_version_id: &str,
    target_version_id: &str,
    source_field_name: &str,
    target_field_name: &str,
) -> Result<VersionContextPair, LixError> {
    let source =
        resolve_target_version_in_transaction(tx, Some(source_version_id), source_field_name)
            .await?;
    let target =
        resolve_target_version_in_transaction(tx, Some(target_version_id), target_field_name)
            .await?;
    let mut executor = TransactionBackendAdapter::new(tx.backend_transaction_mut()?);
    let source_context =
        require_version_context_with_executor(&mut executor, source, "source version").await?;
    let target_context =
        require_version_context_with_executor(&mut executor, target, "target version").await?;
    Ok(VersionContextPair {
        source: source_context,
        target: target_context,
    })
}

pub(crate) fn exact_current_head_preconditions(
    context: &VersionContext,
    idempotency_key: String,
) -> CreateCommitPreconditions {
    CreateCommitPreconditions {
        write_lane: context.write_lane(),
        expected_head: CreateCommitExpectedHead::CurrentHead,
        idempotency_key: CreateCommitIdempotencyKey::Exact(idempotency_key),
    }
}

pub(crate) fn exact_resolved_head_preconditions(
    context: &VersionContext,
    idempotency_key: String,
) -> CreateCommitPreconditions {
    CreateCommitPreconditions {
        write_lane: context.write_lane(),
        expected_head: CreateCommitExpectedHead::CommitId(context.head_commit_id().to_string()),
        idempotency_key: CreateCommitIdempotencyKey::Exact(idempotency_key),
    }
}

pub(crate) fn validate_expected_head_commit_id(
    expected_head_commit_id: Option<&str>,
    context: &VersionContext,
    operation_name: &str,
    version_role: &str,
) -> Result<(), LixError> {
    let Some(expected_head_commit_id) = expected_head_commit_id else {
        return Ok(());
    };
    if expected_head_commit_id == context.head_commit_id() {
        return Ok(());
    }
    Err(LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!(
            "{operation_name} expected {version_role} version '{}' head '{}' but found '{}'",
            context.version_id(),
            expected_head_commit_id,
            context.head_commit_id()
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        require_target_version_context_in_transaction, require_target_version_context_with_backend,
        require_version_context_pair_in_transaction, resolve_target_version_in_transaction,
        VersionContextSource,
    };
    use crate::test_support::boot_test_engine;
    use crate::{CreateVersionOptions, ExecuteOptions};

    #[test]
    fn resolves_session_active_version_target_by_default() {
        run_version_context_test(|| async move {
            let (_backend, _engine, session) = boot_test_engine()
                .await
                .expect("boot_test_engine should succeed");
            let active_version_id = session.active_version_id();

            let target = session
                .transaction(ExecuteOptions::default(), |tx| {
                    Box::pin(async move {
                        resolve_target_version_in_transaction(tx, None, "source_version_id").await
                    })
                })
                .await
                .expect("target resolution should succeed");

            assert_eq!(target.version_id, active_version_id);
            assert_eq!(target.source, VersionContextSource::SessionActiveVersion);
        });
    }

    #[test]
    fn resolves_explicit_version_context_in_transaction() {
        run_version_context_test(|| async move {
            let (_backend, _engine, session) = boot_test_engine()
                .await
                .expect("boot_test_engine should succeed");
            let created = session
                .create_version(CreateVersionOptions {
                    id: Some("branch-a".to_string()),
                    ..CreateVersionOptions::default()
                })
                .await
                .expect("create_version should succeed");

            let context = session
                .transaction(ExecuteOptions::default(), |tx| {
                    Box::pin(async move {
                        require_target_version_context_in_transaction(
                            tx,
                            Some("branch-a"),
                            "source_version_id",
                            "source version",
                        )
                        .await
                    })
                })
                .await
                .expect("context resolution should succeed");

            assert_eq!(context.version_id(), "branch-a");
            assert_eq!(context.head_commit_id(), created.parent_commit_id);
            assert_eq!(context.history_root_commit_id(), created.parent_commit_id);
            assert_eq!(
                context.target.source,
                VersionContextSource::ExplicitArgument
            );
        });
    }

    #[test]
    fn resolves_workspace_active_version_context_with_backend() {
        run_version_context_test(|| async move {
            let (backend, _engine, session) = boot_test_engine()
                .await
                .expect("boot_test_engine should succeed");
            session
                .create_version(CreateVersionOptions {
                    id: Some("branch-b".to_string()),
                    ..CreateVersionOptions::default()
                })
                .await
                .expect("create_version should succeed");
            session
                .switch_version("branch-b".to_string())
                .await
                .expect("switch_version should succeed");

            let context = require_target_version_context_with_backend(
                &backend,
                None,
                "active_version_id",
                "active version",
            )
            .await
            .expect("workspace-backed context resolution should succeed");

            assert_eq!(context.version_id(), "branch-b");
            assert_eq!(
                context.target.source,
                VersionContextSource::WorkspaceActiveVersion
            );
        });
    }

    #[test]
    fn resolves_paired_version_contexts_in_transaction() {
        run_version_context_test(|| async move {
            let (_backend, _engine, session) = boot_test_engine()
                .await
                .expect("boot_test_engine should succeed");
            session
                .create_version(CreateVersionOptions {
                    id: Some("source-branch".to_string()),
                    ..CreateVersionOptions::default()
                })
                .await
                .expect("create_version should succeed");
            session
                .create_version(CreateVersionOptions {
                    id: Some("target-branch".to_string()),
                    ..CreateVersionOptions::default()
                })
                .await
                .expect("create_version should succeed");

            let pair = session
                .transaction(ExecuteOptions::default(), |tx| {
                    Box::pin(async move {
                        require_version_context_pair_in_transaction(
                            tx,
                            "source-branch",
                            "target-branch",
                            "source_version_id",
                            "target_version_id",
                        )
                        .await
                    })
                })
                .await
                .expect("pair resolution should succeed");

            assert_eq!(pair.source.version_id(), "source-branch");
            assert_eq!(pair.target.version_id(), "target-branch");
            assert_eq!(
                pair.source.target.source,
                VersionContextSource::ExplicitArgument
            );
            assert_eq!(
                pair.target.target.source,
                VersionContextSource::ExplicitArgument
            );
        });
    }

    fn run_version_context_test<Factory, Future>(factory: Factory)
    where
        Factory: FnOnce() -> Future + Send + 'static,
        Future: std::future::Future<Output = ()> + 'static,
    {
        std::thread::Builder::new()
            .name("version-context-test".to_string())
            .stack_size(64 * 1024 * 1024)
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("version context test runtime should build")
                    .block_on(factory());
            })
            .expect("version context test thread should spawn")
            .join()
            .expect("version context test thread should join");
    }
}
