//! The unified [`Diagnostic`] type shared by the parser, AST validation, and the
//! serialize/HTML pre-validation.

use alloc::string::String;

use crate::span::Span;

/// How serious a [`Diagnostic`] is.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DiagnosticSeverity {
    /// A non-fatal issue; tolerant parsing continues.
    Warning,
    /// A hard error (e.g. promoted by `parse_strict`, or an invalid AST).
    Error,
}

/// A machine-readable category for a [`Diagnostic`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DiagnosticCode {
    /// A directive name was malformed.
    InvalidDirectiveName,
    /// A container directive (`:::name`) was never closed.
    UnclosedDirectiveContainer,
    /// Malformed MDX syntax.
    InvalidMdx,
    /// A strict-mode parse promoted a configured extension diagnostic to an error.
    StrictParse,
    /// AST validation failure (an invalid or unsupported node shape), the single
    /// code used by `Document::validate` and by serialize/HTML pre-validation.
    InvalidDocument,
}

/// A single diagnostic across every domain — parser, AST validation, and the
/// serialize/HTML pre-validation that wraps it. `span` is optional because a
/// hand-built AST node may carry no source location.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Diagnostic {
    /// How serious the issue is.
    pub severity: DiagnosticSeverity,
    /// The machine-readable category.
    pub code: DiagnosticCode,
    /// The source location, if known.
    pub span: Option<Span>,
    /// A human-readable description.
    pub message: String,
}

impl Diagnostic {
    /// A parser diagnostic, which always carries a source span.
    pub fn new(
        severity: DiagnosticSeverity,
        code: DiagnosticCode,
        span: Span,
        message: impl Into<String>,
    ) -> Self {
        Self {
            severity,
            code,
            span: Some(span),
            message: message.into(),
        }
    }

    /// An AST-validation diagnostic (`Error` severity, `InvalidDocument` code),
    /// whose span is optional because a hand-built node may lack one.
    pub fn invalid(span: Option<Span>, message: impl Into<String>) -> Self {
        Self {
            severity: DiagnosticSeverity::Error,
            code: DiagnosticCode::InvalidDocument,
            span,
            message: message.into(),
        }
    }
}
