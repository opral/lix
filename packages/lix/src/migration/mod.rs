//! Repository-format migration API.
//!
//! This module describes migration edges independently from their physical
//! implementation. Normal repository opening never runs migrations.

mod api;
mod commit_plan;
mod hot_plan;
mod publish;
mod retired_spaces;
mod registry;
mod row_rewrite;
mod standalone_plan;
pub(crate) mod schema_transition;
pub(crate) mod v68;
mod v68_to_v69_rows;

pub use api::{
    MigrationOptions, MigrationReport, MigrationStatus, inspect_repository, migrate_repository,
};
