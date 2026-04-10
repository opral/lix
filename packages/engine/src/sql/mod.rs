//! `sql/*` is the engine's SQL compiler subsystem.
//!
//! The long-term ownership model is stage-oriented:
//! parser -> binder -> semantic IR -> logical plan -> routing / optimizer
//! -> physical plan -> prepare -> explain.
//!
//! Post-Plan-20 dependency rules:
//!
//! - compiler-core SQL may depend on owner-owned contracts from
//!   `canonical/read/*`, `session/version_ops/*`,
//!   root-level `live_state`, and `live_state::writer_key::*` where
//!   row-serving writer-key facts are required
//! - compiler-core SQL must not depend on `commit/*`
//! - compiler-core SQL must not depend on `canonical/journal/*` or
//!   `canonical/graph/*` implementation details
//! - SQL should not grow a compiler-owned cross-subsystem capability hub
//! - cross-owner read glue should live in owner-owned contracts or
//!   stage-owned helpers, not in `sql/services/*`
//! - current-state access from compiler-core should use owner-owned logical
//!   `live_state` contracts, not concrete row/scan contracts
//! - direct `filesystem::*` imports inside compiler-core remain explicit
//!   tracked debt during the Plan 9 hardening work

pub(crate) mod analysis;
pub(crate) mod ast;
pub(crate) mod binder;
pub(crate) mod common;
pub(crate) mod explain;
pub(crate) mod logical_plan;
pub(crate) mod optimizer;
pub(crate) mod parser;
pub(crate) mod physical_plan;
pub(crate) mod prepare;
pub(crate) mod semantic_ir;
pub(crate) mod support;
