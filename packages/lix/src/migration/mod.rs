//! Repository-format migration API.
//!
//! This module describes migration edges independently from their physical
//! implementation. Normal repository opening never runs migrations. Inspect
//! closed repositories before opening an engine. Migration is available only
//! when the registry has a complete path to the current format; intentional
//! hard cuts return an error:
//!
//! ```no_run
//! # async fn example(storage: lix::Memory) -> Result<(), lix::LixError> {
//! use lix::migration::{MigrationOptions, MigrationStatus, inspect_lix, migrate_lix};
//!
//! if matches!(inspect_lix(&storage).await?, MigrationStatus::Required { .. }) {
//!     migrate_lix(storage.clone(), MigrationOptions::default()).await?;
//! }
//!
//! let lix = lix::open_lix().with_storage(storage).await?;
//! # lix.close().await?;
//! # Ok(())
//! # }
//! ```

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
    MigrationOptions, MigrationReport, MigrationStatus, inspect_lix, migrate_lix,
};
