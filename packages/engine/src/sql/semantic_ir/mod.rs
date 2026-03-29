//! Typed semantic statement ownership.
//!
//! These modules carry the public-surface semantic pipeline as compiler-owned
//! semantic IR.

pub(crate) mod canonicalize;
pub(crate) mod internal;
pub(crate) mod public;
pub(crate) mod semantics;
pub(crate) mod statement;
pub(crate) mod validation;

pub(crate) use internal::prepare_internal_statements_with_backend_to_plan;
pub(crate) use public::{
    analyze_public_write_semantics, augment_dependency_spec_for_broad_public_read,
    prepare_structured_public_read_analysis, BoundPublicLeaf, ExplainOptions,
    PublicExecutionDebugTrace, PublicReadSemantics, PublicWriteInvariantTrace,
    PublicWriteSemantics, unknown_public_state_schema_error,
};
pub(crate) use statement::{
    BoundStatement, BoundStatementMetadata, ExecutionContext, StatementKind,
};
