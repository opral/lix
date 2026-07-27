//! Git-style line semantics for NUL-free text files.
//!
//! A line is a durable entity rather than a display-only diff hunk. The
//! component preserves source bytes exactly, including invalid UTF-8 and final
//! unterminated lines, by storing each LF-delimited byte segment as base64.

mod bindings;
mod core;

pub const MANIFEST_JSON: &str = include_str!("../manifest.json");
pub const SCHEMAS: [(&str, &str); 1] = [(
    "schema/git_text_line_v2.json",
    include_str!("../schema/git_text_line_v2.json"),
)];

#[cfg(test)]
mod tests;
