//! DataFusion-first engine layering.
//!
//! This module is intentionally separate from the legacy `api::engine` and
//! `session` path while the new SQL execution DAG is assembled.

#![allow(dead_code)]

pub mod engine;
pub(crate) mod live_state;
pub(crate) mod schema_registry;
pub mod session;
pub mod transaction;

pub use engine::Engine;
pub use session::Session;
pub use session::{ExecuteResult, Row, RowRef, RowSet};
