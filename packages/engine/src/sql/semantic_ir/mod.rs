//! Typed semantic statement ownership.
//!
//! These modules carry the public-surface semantic pipeline as compiler-owned
//! semantic IR.

pub(crate) mod canonicalize;
pub(crate) mod inline_functions;
pub(crate) mod internal;
pub(crate) mod param_context;
pub(crate) mod public;
pub(crate) mod semantics;
pub(crate) mod statement;

pub(crate) use internal::prepare_internal_statements_to_plan;
pub(crate) use public::{
    analyze_public_write_semantics, augment_dependency_spec_for_broad_public_read,
    prepare_structured_public_read_analysis, unknown_public_state_schema_error, BoundPublicLeaf,
    PublicReadSemantics, PublicWriteInvariantTrace, PublicWriteSemantics, SemanticStatement,
    StructuredPublicReadPreparation,
};
pub(crate) use statement::{
    BoundStatement, BoundStatementMetadata, ExecutionContext, StatementKind,
};
