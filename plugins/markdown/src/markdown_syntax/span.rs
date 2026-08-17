//! Source locations: byte-offset [`Span`]s and their [`LineIndex`] translation
//! into human-readable line/column [`LinePosition`]s.

use alloc::vec::Vec;

/// A half-open byte range `start..end` into the original source string. These are
/// absolute UTF-8 byte offsets from the start of the document (not line/column);
/// use [`LineIndex`] to translate an offset into a line and column.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct Span {
    /// Inclusive start byte offset.
    pub start: usize,
    /// Exclusive end byte offset (one past the last byte).
    pub end: usize,
}

impl Span {
    /// Construct a span from a start and end byte offset.
    pub const fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    /// The length of the span in bytes (`0` if `end <= start`).
    pub const fn len(self) -> usize {
        self.end.saturating_sub(self.start)
    }

    /// Whether the span covers zero bytes.
    pub const fn is_empty(self) -> bool {
        self.start == self.end
    }

    /// Whether `other` lies entirely within this span.
    pub const fn contains(self, other: Span) -> bool {
        self.start <= other.start && other.end <= self.end
    }

    /// Whether `start <= end` (a well-formed range).
    pub const fn is_valid(self) -> bool {
        self.start <= self.end
    }
}

/// A 1-based line and column, derived from a byte offset by [`LineIndex`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LinePosition {
    /// 1-based line number.
    pub line: usize,
    /// 1-based column number (counted in bytes from the line start).
    pub column: usize,
}

/// A precomputed map from byte offsets to line/column positions for one source
/// string. Build it once with [`LineIndex::new`], then query repeatedly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LineIndex {
    line_starts: Vec<usize>,
    len: usize,
}

impl LineIndex {
    /// Build a line index for `source`, scanning its line breaks
    /// (`\n`, `\r`, and `\r\n`).
    pub fn new(source: &str) -> Self {
        let bytes = source.as_bytes();
        let mut starts = Vec::new();
        starts.push(0);

        let mut index = 0;
        while index < bytes.len() {
            match bytes[index] {
                b'\r' => {
                    if index + 1 < bytes.len() && bytes[index + 1] == b'\n' {
                        index += 2;
                    } else {
                        index += 1;
                    }
                    starts.push(index);
                }
                b'\n' => {
                    index += 1;
                    starts.push(index);
                }
                _ => index += 1,
            }
        }

        Self {
            line_starts: starts,
            len: source.len(),
        }
    }

    /// Translate a byte `offset` into its 1-based line and column (clamped to the
    /// end of the source).
    pub fn position(&self, offset: usize) -> LinePosition {
        let offset = offset.min(self.len);
        let line_index = match self.line_starts.binary_search(&offset) {
            Ok(index) => index,
            Err(index) => index.saturating_sub(1),
        };
        let line_start = self.line_starts[line_index];

        LinePosition {
            line: line_index + 1,
            column: offset.saturating_sub(line_start) + 1,
        }
    }

    /// Translate a [`Span`] into its start and end [`LinePosition`]s.
    pub fn span(&self, span: Span) -> (LinePosition, LinePosition) {
        (self.position(span.start), self.position(span.end))
    }
}
