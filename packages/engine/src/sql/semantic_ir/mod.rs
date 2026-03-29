//! Typed semantic statement ownership.
//!
//! These modules carry the public-surface semantic pipeline as compiler-owned
//! semantic IR.

pub(crate) mod canonicalize;
pub(crate) mod semantics;
pub(crate) mod validation;
