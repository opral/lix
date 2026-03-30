//! Workspace-state boundary.
//!
//! `workspace` owns mutable selectors that are distinct from canonical
//! committed refs.
//!
//! Workspace annotation data such as `writer_key` also belongs here. It may be
//! session-local or locally durable, but it is not a canonical change fact and
//! semantic replay must not require it.
//!
//! Durable workspace state:
//! - the persisted active workspace version id
//! - the persisted active workspace account ids
//! - workspace writer annotations keyed by `(version_id, schema_key, entity_id, file_id)`
//!
//! Session-local state:
//! - in-memory `Session` overrides
//! - transaction-local pending overlays that may be discarded without changing
//!   committed truth
//! - workspace annotation projected onto state-shaped reads
//!
//! None of these APIs own committed head semantics. Canonical refs remain the
//! durable source for committed heads and roots. If a read surface exposes
//! `writer_key`, it does so as workspace annotation data rather than
//! canonical committed meaning.

mod init;
mod metadata;
pub(crate) mod writer_key;

pub(crate) use init::init;
pub(crate) use metadata::{
    load_workspace_active_account_ids, persist_workspace_selectors,
    require_workspace_active_version_id,
};

#[cfg(test)]
mod tests {
    use super::require_workspace_active_version_id;
    use crate::test_support::boot_test_engine;
    use crate::{CreateVersionOptions, OpenSessionOptions};

    #[test]
    fn workspace_session_switch_persists_durable_workspace_selection() {
        run_workspace_test(|| async {
            let (backend, _engine, session) = boot_test_engine()
                .await
                .expect("boot test engine should succeed");
            session
                .create_version(CreateVersionOptions {
                    id: Some("version-b".to_string()),
                    name: Some("version-b".to_string()),
                    ..CreateVersionOptions::default()
                })
                .await
                .expect("create version should succeed");

            session
                .switch_version("version-b".to_string())
                .await
                .expect("workspace session switch should succeed");

            let workspace_version = require_workspace_active_version_id(&backend)
                .await
                .expect("workspace version lookup should succeed");
            assert_eq!(workspace_version, "version-b");
        });
    }

    #[test]
    fn ephemeral_session_override_does_not_change_durable_workspace_selection() {
        run_workspace_test(|| async {
            let (backend, _engine, session) = boot_test_engine()
                .await
                .expect("boot test engine should succeed");
            let initial_workspace_version = require_workspace_active_version_id(&backend)
                .await
                .expect("workspace version lookup should succeed");
            session
                .create_version(CreateVersionOptions {
                    id: Some("version-b".to_string()),
                    name: Some("version-b".to_string()),
                    ..CreateVersionOptions::default()
                })
                .await
                .expect("create version should succeed");

            let ephemeral = session
                .open_session(OpenSessionOptions {
                    active_version_id: Some("version-b".to_string()),
                    ..OpenSessionOptions::default()
                })
                .await
                .expect("ephemeral session open should succeed");

            assert_eq!(ephemeral.active_version_id(), "version-b");
            assert_eq!(session.active_version_id(), initial_workspace_version);
            let persisted_workspace_version = require_workspace_active_version_id(&backend)
                .await
                .expect("workspace version lookup should succeed");
            assert_eq!(persisted_workspace_version, initial_workspace_version);
        });
    }

    fn run_workspace_test<Factory, Future>(factory: Factory)
    where
        Factory: FnOnce() -> Future + Send + 'static,
        Future: std::future::Future<Output = ()> + 'static,
    {
        std::thread::Builder::new()
            .name("workspace-test".to_string())
            .stack_size(64 * 1024 * 1024)
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("workspace test runtime should build")
                    .block_on(factory());
            })
            .expect("workspace test thread should spawn")
            .join()
            .expect("workspace test thread should join");
    }
}
