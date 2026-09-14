//! Explicit repository inspection and detached format migration.
//! Ordinary engine admission accepts only the current format; historical
//! decoders and converters compile only in the offline-migration artifact.

#[cfg(any(feature = "offline-migration", test))]
mod api;
mod inspection;
mod public_api;
#[cfg(feature = "offline-migration")]
pub use public_api::{
    AuthorityActivationReport, AuthorityActivationWitness, RepositoryMigrationReport,
    migrate_repository, migrate_repository_with_options, prepare_authority_activation,
    restore_and_migrate_repository, verify_authority_activation,
};
pub use public_api::{RepositoryInspection, RepositoryLayout, RepositoryRole, inspect_repository};
#[cfg(any(feature = "offline-migration", test))]
mod authority_baseline_fence;
mod bounded_read;
#[cfg(any(feature = "offline-migration", test))]
pub use authority_baseline_fence::upgrade_authority_for_partial_sync;
pub(crate) use bounded_read::BoundedRead as MigrationBoundedRead;
#[cfg(any(feature = "offline-migration", test))]
mod checkpoint_metadata;
#[cfg(any(feature = "offline-migration", test))]
mod deterministic_witness;
mod epoch;
#[cfg(any(feature = "offline-migration", test))]
mod incorporation;
#[cfg(any(feature = "offline-migration", test))]
mod omitted_owners;
#[cfg(test)]
pub(crate) use incorporation::{
    downgrade_headers_for_test, mark_header_incorporation_unknown_for_test,
    migrate_headers_for_test,
};
#[cfg(any(feature = "offline-migration", test))]
mod selected_locators;
#[cfg(test)]
pub(crate) use epoch::stage_legacy_partial_epoch_for_test;
#[cfg(any(feature = "offline-migration", test))]
mod publish;
mod registry;
#[cfg(any(feature = "offline-migration", test))]
mod runtime_epoch;

#[cfg(any(feature = "offline-migration", test))]
pub use api::MigrationOptions;
#[cfg(any(feature = "offline-migration", test))]
pub(crate) use api::migrate_lix_with_adapter;
pub(crate) use epoch::{
    FreshEpochImport, RetainedReplicaSource, admit_current_repository, admit_existing_repository,
    admit_partial_epoch, begin_fresh_epoch_import, has_partial_replica_marker,
    install_fresh_partial_epoch, list_retained_replica_sources, open_retained_replica_source,
    partial_epoch_has_no_markers,
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

#[cfg(any(feature = "offline-migration", test))]
pub(crate) use epoch::convert_clean_replica_to_partial;

#[cfg(test)]
pub(crate) use epoch::admit_repository;

#[cfg(feature = "offline-migration")]
mod older_witness;
