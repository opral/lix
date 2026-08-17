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
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use {
    ast::*,
    diagnostic::{Diagnostic, DiagnosticCode},
    options::{Construct, Constructs, ParseOptions, SyntaxConfigError, WikiLinkOrder},
    parse::{ParseOutput, ParseStrictError, parse},
    serialize::SerializeError,
    span::{LineIndex, LinePosition},
};

#[cfg(test)]
pub(crate) mod prelude {
    pub(crate) use super::*;
}

#[cfg(test)]
mod upstream_tests;
