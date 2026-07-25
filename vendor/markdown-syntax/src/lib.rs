#![doc = include_str!("../README.md")]
#![no_std]
#![deny(missing_docs)]
// `markdown-syntax` 0.2.0 is vendored so its private parser-equivalence tests
// run in this workspace. These are existing upstream style lints under this
// repository's newer nightly Clippy; keep the allowlist narrow instead of
// rewriting unrelated third-party parser and serializer code.
#![allow(
    clippy::collapsible_if,
    clippy::derivable_impls,
    clippy::manual_pattern_char_comparison,
    clippy::manual_strip,
    clippy::question_mark,
    clippy::sliced_string_as_bytes,
    clippy::while_let_loop
)]

extern crate alloc;

mod entities;
mod unicode_punctuation;

pub mod ast;
pub mod diagnostic;
#[cfg(feature = "html")]
pub mod html;
pub mod options;
pub mod parse;
pub mod serialize;
pub mod span;
pub mod validate;

pub use ast::*;
pub use diagnostic::{Diagnostic, DiagnosticCode, DiagnosticSeverity};
#[cfg(feature = "html")]
pub use html::{HtmlError, HtmlOptions, SafeRawHtmlForm, TasklistAttrOrder};
pub use options::{
    Construct, Constructs, ParseOptions, SyntaxConfigError, SyntaxOptions, WikiLinkOrder,
};
pub use parse::{parse, ParseOutput, ParseStrictError};
pub use serialize::{LineEnding, SerializeError, SerializeOptions};
pub use span::{LineIndex, LinePosition, Span};

/// Common imports for working with `markdown-syntax`: `use
/// markdown_syntax::prelude::*;` brings the AST, options, diagnostics, parse
/// entry points, and serialize/span types (plus the HTML renderer under the
/// `html` feature) into scope.
pub mod prelude {
    pub use crate::ast::*;
    pub use crate::diagnostic::{Diagnostic, DiagnosticCode, DiagnosticSeverity};
    #[cfg(feature = "html")]
    pub use crate::html::{HtmlError, HtmlOptions, SafeRawHtmlForm, TasklistAttrOrder};
    pub use crate::options::{
        Construct, Constructs, ParseOptions, SyntaxConfigError, SyntaxOptions, WikiLinkOrder,
    };
    pub use crate::parse::{parse, ParseOutput, ParseStrictError};
    pub use crate::serialize::{LineEnding, SerializeError, SerializeOptions};
    pub use crate::span::{LineIndex, LinePosition, Span};
}
