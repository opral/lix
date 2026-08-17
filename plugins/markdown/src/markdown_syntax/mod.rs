//! Markdown parser, AST, and canonical serializer used by this plugin.
//!
//! Originally based on `markdown-syntax` 0.2.0 by Plimeor
//! (<https://github.com/plimeor/markdown-syntax>) and since modified for Lix.
//! The original MIT copyright and license notice is retained in
//! [`LICENSE-MIT`](LICENSE-MIT) beside this module.

pub(crate) mod entities;
pub(crate) mod unicode_punctuation;

pub(crate) mod ast;
pub(crate) mod diagnostic;
pub(crate) mod options;
pub(crate) mod parse;
pub(crate) mod serialize;
pub(crate) mod span;
pub(crate) mod validate;

pub(crate) use diagnostic::DiagnosticSeverity;
pub(crate) use options::SyntaxOptions;
pub(crate) use serialize::{LineEnding, SerializeOptions};
pub(crate) use span::Span;
