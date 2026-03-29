//! `sql/*` is the engine's SQL compiler subsystem.
//!
//! The long-term ownership model is stage-oriented:
//! parser -> binder -> semantic IR -> logical plan -> optimizer
//! -> physical plan -> executor -> explain.

pub(crate) mod analysis;
pub(crate) mod ast;
pub(crate) mod backend;
pub(crate) mod binder;
pub(crate) mod catalog;
pub(crate) mod common;
pub(crate) mod executor;
pub(crate) mod explain;
pub(crate) mod internal;
pub(crate) mod logical_plan;
pub(crate) mod optimizer;
pub(crate) mod parser;
pub(crate) mod physical_plan;
pub(crate) mod services;
pub(crate) mod semantic_ir;
pub(crate) mod storage;
