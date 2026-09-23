//! Rust-owned repository upgrades and operator inspection.
//! Public opening coordinates supported upgrades before admitting the engine.
//! Operator tools use the same migration and preservation machinery.

mod account_amendment_witness;
mod api;
mod inspection;
mod public_api;
pub use public_api::{
    AuthorityActivationReport, AuthorityActivationWitness, RepositoryMigrationReport,
    migrate_repository, migrate_repository_with_options, prepare_authority_activation,
    restore_and_migrate_repository, verify_authority_activation,
};
pub use public_api::{RepositoryInspection, RepositoryLayout, RepositoryRole, inspect_repository};
mod authority_baseline_fence;
mod bounded_read;
pub use authority_baseline_fence::upgrade_authority_for_partial_sync;
pub(crate) use authority_baseline_fence::upgrade_candidate_if_authority;
pub(crate) use bounded_read::BoundedRead as MigrationBoundedRead;
mod checkpoint_metadata;
mod deterministic_witness;
mod epoch;
mod incorporation;
mod omitted_owners;
#[cfg(test)]
pub(crate) use incorporation::{
    downgrade_headers_for_test, mark_header_incorporation_unknown_for_test,
    migrate_headers_for_test,
};
mod selected_locators;
#[cfg(test)]
pub(crate) use epoch::stage_legacy_partial_epoch_for_test;
mod publish;
mod registry;
mod runtime_epoch;
mod hot_indexes;

pub use api::MigrationOptions;
pub(crate) use api::migrate_lix_with_adapter;
pub(crate) use epoch::{
    FreshEpochImport, RetainedReplicaSource, admit_current_repository, admit_existing_repository,
    admit_partial_epoch, admit_partial_repository, admit_repository_with_server, begin_fresh_epoch_import,
    has_partial_replica_marker, inspect_partial_replacement, install_fresh_partial_epoch,
    list_retained_replica_sources, open_retained_replica_source, partial_epoch_has_no_markers,
};
pub(crate) use inspection::{
    MigrationStatus, inspect_lix, inspect_lix_read, inspect_lix_with_adapter,
};
pub(crate) use registry::has_complete_migration_path;

pub(crate) use epoch::PendingConversionJournal;

pub(crate) use epoch::retry_published_conversion_cleanup;

pub(crate) use epoch::GlobalConversionJournal;

pub(crate) use epoch::MigrationPlanningRead;

#[cfg(test)]
pub(crate) use epoch::tests::CommitExpiringStorage;

pub(crate) use epoch::convert_clean_replica_to_partial;

#[cfg(test)]
pub(crate) use epoch::admit_repository;

mod older_witness;
