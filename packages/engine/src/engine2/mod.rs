//! DataFusion-first engine layering.
//!
//! This module is intentionally separate from the legacy `api::engine` and
//! `session` path while the new SQL execution DAG is assembled.

#![allow(dead_code)]

pub(crate) mod changelog;
pub mod engine;
pub(crate) mod init;
pub(crate) mod live_state;
pub(crate) mod schema_registry;
pub mod session;
pub(crate) mod tracked_state;
pub mod transaction;
pub(crate) mod untracked_state;

pub use engine::Engine;
pub use session::SessionContext;
pub use session::{ExecuteResult, Row, RowRef, RowSet};
