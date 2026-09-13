//! Engine-owned repository-format migration.
//!
//! Opening a Lix is the sole policy boundary. Keeping this module private
//! prevents applications from fragmenting the ecosystem with their own
//! migration ordering, limits, or recovery rules.

mod api;
mod authority_baseline_fence;
mod bounded_read;
pub use authority_baseline_fence::upgrade_authority_for_partial_sync;
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

pub(crate) use api::{
    MigrationOptions, MigrationStatus, inspect_lix, inspect_lix_read, inspect_lix_with_adapter,
    migrate_lix_with_adapter,
};
pub(crate) use epoch::{
    FreshEpochImport, RetainedReplicaSource, admit_existing_repository, admit_partial_epoch,
    admit_repository_with_server, begin_fresh_epoch_import, convert_clean_replica_to_partial,
    has_partial_replica_marker, install_fresh_partial_epoch, list_retained_replica_sources,
    open_retained_replica_source, partial_epoch_has_no_markers,
};
pub(crate) use registry::has_complete_migration_path;

pub(crate) use epoch::PendingConversionJournal;

pub(crate) use epoch::retry_published_conversion_cleanup;

pub(crate) use epoch::GlobalConversionJournal;

pub(crate) use epoch::MigrationPlanningRead;


#[cfg(test)]
pub(crate) use epoch::tests::CommitExpiringStorage;
