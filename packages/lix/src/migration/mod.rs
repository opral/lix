//! Engine-owned repository-format migration.
//!
//! Opening a Lix is the sole policy boundary. Keeping this module private
//! prevents applications from fragmenting the ecosystem with their own
//! migration ordering, limits, or recovery rules.

mod api;
mod epoch;
mod publish;
mod registry;

pub(crate) use api::{
    MigrationOptions, MigrationStatus, inspect_lix, inspect_lix_with_adapter,
    migrate_lix_with_adapter,
};
pub(crate) use epoch::admit_repository;
