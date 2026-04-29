//! DataFusion-first engine layering.
//!
//! This module is intentionally separate from the legacy `api::engine` and
//! `session` path while the new SQL execution DAG is assembled.

#![allow(dead_code)]

pub(crate) mod changelog;
pub(crate) mod commit_graph;
pub mod engine;
pub(crate) mod functions;
pub(crate) mod init;
pub(crate) mod live_state;
pub(crate) mod schema_registry;
pub mod session;
#[cfg(test)]
pub(crate) mod test_support;
pub(crate) mod tracked_state;
pub mod transaction;
pub(crate) mod untracked_state;
pub(crate) mod version_ref;

#[cfg(test)]
mod tests;

pub use engine::Engine;
pub use init::InitReceipt;
pub use session::{
    CreateVersionOptions, CreateVersionReceipt, MergeVersionOptions, MergeVersionReceipt,
    SessionContext, SwitchVersionOptions, SwitchVersionReceipt,
};
pub use session::{ExecuteResult, Row, RowRef, RowSet};
