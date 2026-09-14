#![recursion_limit = "256"]

mod config;
mod routes;
mod store;
#[doc(hidden)]
pub mod telemetry;

pub use config::Config;
pub use routes::router;
pub use store::{AuthorityInventory, AuthorityInventoryEntry, LixRuntimeManager};

#[cfg(test)]
mod lifecycle_tests;

#[cfg(feature = "offline-migration")]
pub use store::AuthorityMigrationReport;
