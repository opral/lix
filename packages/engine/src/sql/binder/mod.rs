#![allow(unused_imports)]

//! SQL binding ownership.
//!
//! This stage owns placeholder binding, runtime binding templates, and other
//! parameter/caller-specific AST binding concerns.

pub(crate) mod contracts;
pub(crate) mod public_bind;
pub(crate) mod runtime;

pub(crate) use contracts::{
    bind_statement_metadata, classify_statement, BoundStatement, BoundStatementMetadata,
    ExecutionContext, StatementKind,
};
pub(crate) use public_bind::*;
pub(crate) use runtime::*;
