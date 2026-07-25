#![doc = include_str!("../README.md")]
#![no_std]
#![deny(missing_docs)]

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
