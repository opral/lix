//! Physical key windows for a typed primary-key interval.
//!
//! Tracked keys are ordered `(schema_key, file_id, row_pk)`. A primary-key
//! interval that names no file scope is therefore one contiguous key window
//! *per file scope*, not one window overall. These helpers let ordered
//! physical layouts — packed mutation parts, tree children, HOT key ranges —
//! route around spans that provably hold no key of the interval in any file
//! scope.
//!
//! Pruning is an access-path optimization, never a predicate: every reader
//! keeps its per-row bound check, and a span whose boundary keys cannot be
//! decoded, or that crosses a file-scope boundary, is always retained. A
//! schema with `F` file scopes therefore visits at most `F + 1` boundary spans
//! beyond the ones that actually intersect the interval.

use super::codec::{
    decode_key_borrowed, encode_key_ref, encode_schema_file_prefix, encode_schema_key_prefix,
};
use super::types::{RowPkRangeBound, TrackedStateKeyRef, row_pk_satisfies_bounds};

/// A typed primary-key interval applied in every file scope of a schema.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RowPkWindow {
    pub(crate) lower: Option<RowPkRangeBound>,
    pub(crate) upper: Option<RowPkRangeBound>,
}

/// One half-open encoded key range `[start, end)`; `end == None` is unbounded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RowPkWindowKeyRange {
    pub(crate) start: Vec<u8>,
    pub(crate) end: Option<Vec<u8>>,
}

impl RowPkWindow {
    /// Returns a window only when at least one side is bounded; an unbounded
    /// window is the ordinary schema scan.
    pub(crate) fn from_bounds(
        lower: Option<&RowPkRangeBound>,
        upper: Option<&RowPkRangeBound>,
    ) -> Option<Self> {
        (lower.is_some() || upper.is_some()).then(|| Self {
            lower: lower.cloned(),
            upper: upper.cloned(),
        })
    }

    pub(crate) fn contains(&self, row_pk: &crate::row_pk::RowPk) -> bool {
        row_pk_satisfies_bounds(row_pk, self.lower.as_ref(), self.upper.as_ref())
    }

    /// Whether the ordered key span `[first_key, last_key]` may contain a key
    /// of this window in some file scope.
    ///
    /// Only a span whose boundary keys share one `(schema, file)` scope can be
    /// excluded: such a span holds exactly the keys of that scope between its
    /// two primary keys. A span crossing a scope boundary may contain an
    /// entire intermediate scope and is retained.
    pub(crate) fn span_may_intersect(&self, first_key: &[u8], last_key: &[u8]) -> bool {
        span_may_intersect_row_pk_bounds(
            self.lower.as_ref(),
            self.upper.as_ref(),
            first_key,
            last_key,
        )
    }

    /// Sorted, disjoint encoded ranges covering every key of `schema_key`
    /// that this window can admit: the exact window in the unfiled scope, plus
    /// the complete remainder of the schema where file-scoped keys live.
    ///
    /// File-scoped spans are narrowed afterwards by [`Self::span_may_intersect`]
    /// because their scopes are not known before the read.
    pub(crate) fn schema_key_ranges(&self, schema_key: &str) -> Vec<RowPkWindowKeyRange> {
        let schema_prefix = encode_schema_key_prefix(schema_key);
        let schema_end = key_prefix_successor(&schema_prefix);
        let unfiled_prefix = encode_schema_file_prefix(schema_key, None);
        let unfiled_end = key_prefix_successor(&unfiled_prefix);

        let unfiled_start = self.lower.as_ref().map_or_else(
            || unfiled_prefix.clone(),
            |bound| {
                let key = encode_unfiled_key(schema_key, bound);
                if bound.inclusive {
                    key
                } else {
                    // Encoded keys are self-delimiting, so no other key has
                    // this one as a strict prefix: the prefix successor is
                    // the first key ordered after it.
                    key_prefix_successor(&key).unwrap_or(key)
                }
            },
        );
        let unfiled_window_end = match self.upper.as_ref() {
            None => unfiled_end.clone(),
            Some(bound) => {
                let key = encode_unfiled_key(schema_key, bound);
                if bound.inclusive {
                    key_prefix_successor(&key)
                } else {
                    Some(key)
                }
            }
        };

        let mut ranges = vec![
            RowPkWindowKeyRange {
                start: schema_prefix,
                end: Some(unfiled_prefix),
            },
            RowPkWindowKeyRange {
                start: unfiled_start,
                end: unfiled_window_end,
            },
        ];
        if let Some(unfiled_end) = unfiled_end {
            ranges.push(RowPkWindowKeyRange {
                start: unfiled_end,
                end: schema_end,
            });
        }
        normalize_row_pk_window_key_ranges(ranges)
    }
}

/// Sorts ranges, drops empty ones and merges overlapping or touching ones, so
/// the result is the canonical selector that ordered physical readers accept.
pub(crate) fn normalize_row_pk_window_key_ranges(
    mut ranges: Vec<RowPkWindowKeyRange>,
) -> Vec<RowPkWindowKeyRange> {
    ranges.retain(|range| {
        range
            .end
            .as_ref()
            .is_none_or(|end| range.start.as_slice() < end.as_slice())
    });
    ranges.sort_by(|left, right| left.start.cmp(&right.start));
    let mut merged: Vec<RowPkWindowKeyRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if let Some(last) = merged.last_mut()
            && last
                .end
                .as_ref()
                .is_none_or(|end| range.start.as_slice() <= end.as_slice())
        {
            last.end = match (last.end.take(), range.end) {
                (Some(left), Some(right)) => Some(left.max(right)),
                _ => None,
            };
            continue;
        }
        merged.push(range);
    }
    merged
}

/// [`RowPkWindow::span_may_intersect`] over borrowed bounds.
pub(crate) fn span_may_intersect_row_pk_bounds(
    lower: Option<&RowPkRangeBound>,
    upper: Option<&RowPkRangeBound>,
    first_key: &[u8],
    last_key: &[u8],
) -> bool {
    if lower.is_none() && upper.is_none() {
        return true;
    }
    let (Ok(first), Ok(last)) = (decode_key_borrowed(first_key), decode_key_borrowed(last_key))
    else {
        return true;
    };
    if first.schema_key != last.schema_key || first.file_id != last.file_id {
        return true;
    }
    // [first, last] intersects the window iff its upper end clears the lower
    // bound and its lower end clears the upper bound.
    row_pk_satisfies_bounds(&last.row_pk, lower, None)
        && row_pk_satisfies_bounds(&first.row_pk, None, upper)
}

/// The last primary key a stored span can hold in `schema_key`, when the
/// span is confined to one `(schema, file)` scope and ends at or past the
/// window's lower bound. A span crossing a scope boundary has no such key.
pub(crate) fn row_pk_span_end(
    schema_key: &str,
    window: &RowPkWindow,
    first_key: &[u8],
    last_key: &[u8],
) -> Option<crate::row_pk::RowPk> {
    let (Ok(first), Ok(last)) = (decode_key_borrowed(first_key), decode_key_borrowed(last_key))
    else {
        return None;
    };
    (first.schema_key == schema_key
        && last.schema_key == schema_key
        && first.file_id == last.file_id
        && row_pk_satisfies_bounds(&last.row_pk, window.lower.as_ref(), None))
    .then_some(last.row_pk)
}

/// Chooses the inclusive upper primary key of one ordered page from stored
/// spans a window read would visit — each given as the last primary key it
/// can hold (see [`row_pk_span_end`]) and its member count — or `None` when
/// those spans hold fewer than `target_rows` members.
///
/// A span holds no key beyond its end, so reading through the smallest end at
/// which the cumulative count reaches `target_rows` visits about that many
/// stored rows. Spans without an end can only complete the count. This is a
/// cost estimate, never a visibility claim: callers read the chosen interval
/// through the exact path.
pub(crate) fn row_pk_page_horizon(
    window: &RowPkWindow,
    mut ends: Vec<(Option<crate::row_pk::RowPk>, usize)>,
    target_rows: usize,
) -> Option<crate::row_pk::RowPk> {
    ends.sort_by(|left, right| match (&left.0, &right.0) {
        (Some(left), Some(right)) => left.cmp(right),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    });
    let mut covered = 0_usize;
    for (end, rows) in ends {
        covered = covered.saturating_add(rows);
        if covered >= target_rows {
            return end.filter(|end| {
                window
                    .upper
                    .as_ref()
                    .is_none_or(|upper| end < &upper.row_pk)
            });
        }
    }
    None
}

fn encode_unfiled_key(schema_key: &str, bound: &RowPkRangeBound) -> Vec<u8> {
    encode_key_ref(TrackedStateKeyRef {
        schema_key,
        file_id: None,
        row_pk: &bound.row_pk,
    })
}

fn key_prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut successor = prefix.to_vec();
    while let Some(last) = successor.last_mut() {
        if *last != u8::MAX {
            *last += 1;
            return Some(successor);
        }
        successor.pop();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row_pk::RowPk;

    fn key(schema_key: &str, file_id: Option<&str>, row_pk: &str) -> Vec<u8> {
        encode_key_ref(TrackedStateKeyRef {
            schema_key,
            file_id,
            row_pk: &RowPk::single(row_pk),
        })
    }

    fn window(lower: Option<(&str, bool)>, upper: Option<(&str, bool)>) -> RowPkWindow {
        RowPkWindow {
            lower: lower.map(|(row_pk, inclusive)| RowPkRangeBound {
                row_pk: RowPk::single(row_pk),
                inclusive,
            }),
            upper: upper.map(|(row_pk, inclusive)| RowPkRangeBound {
                row_pk: RowPk::single(row_pk),
                inclusive,
            }),
        }
    }

    fn in_ranges(ranges: &[RowPkWindowKeyRange], key: &[u8]) -> bool {
        ranges.iter().any(|range| {
            range.start.as_slice() <= key && range.end.as_ref().is_none_or(|end| key < end.as_slice())
        })
    }

    #[test]
    fn single_scope_spans_outside_the_window_are_excluded() {
        let window = window(Some(("m", false)), Some(("t", true)));
        assert!(!window.span_may_intersect(&key("s", None, "a"), &key("s", None, "m")));
        assert!(window.span_may_intersect(&key("s", None, "a"), &key("s", None, "n")));
        assert!(window.span_may_intersect(&key("s", None, "t"), &key("s", None, "z")));
        assert!(!window.span_may_intersect(&key("s", None, "u"), &key("s", None, "z")));
        assert!(!window.span_may_intersect(
            &key("s", Some("f"), "u"),
            &key("s", Some("f"), "z")
        ));
    }

    #[test]
    fn spans_crossing_a_scope_boundary_are_retained() {
        let window = window(Some(("m", false)), Some(("n", false)));
        assert!(window.span_may_intersect(&key("s", None, "x"), &key("s", Some("f"), "a")));
        assert!(window.span_may_intersect(&key("s", Some("f"), "x"), &key("s", Some("g"), "a")));
        assert!(window.span_may_intersect(&key("r", None, "x"), &key("t", None, "a")));
        assert!(window.span_may_intersect(b"not a key", b"not a key either"));
    }

    #[test]
    fn page_horizon_reads_through_the_target_count_of_scope_local_spans() {
        let spans = [
            (key("s", None, "a"), key("s", None, "c"), 10_usize),
            (key("s", None, "d"), key("s", None, "f"), 10),
            (key("s", Some("x"), "b"), key("s", Some("x"), "e"), 10),
            (key("s", None, "g"), key("t", None, "a"), 10),
        ];
        let horizon = |window: &RowPkWindow, target| {
            row_pk_page_horizon(
                window,
                spans
                    .iter()
                    .map(|(first, last, rows)| (row_pk_span_end("s", window, first, last), *rows))
                    .collect(),
                target,
            )
        };
        let open = window(None, None);
        assert_eq!(horizon(&open, 5), Some(RowPk::single("c")));
        assert_eq!(horizon(&open, 15), Some(RowPk::single("e")));
        assert_eq!(horizon(&open, 30), Some(RowPk::single("f")));
        // Only the schema-crossing span is left: no key bounds the page.
        assert_eq!(horizon(&open, 31), None);
        // A horizon at or past the upper bound is the remainder.
        assert_eq!(horizon(&window(None, Some(("e", true))), 15), None);
        // Spans that end before the lower bound cannot end a page.
        assert_eq!(horizon(&window(Some(("c", false)), None), 5), Some(RowPk::single("e")));
    }

    #[test]
    fn half_open_windows_produce_canonical_selectors() {
        for window in [
            window(None, Some(("m", true))),
            window(Some(("m", false)), None),
            window(None, Some(("a", false))),
        ] {
            let ranges = window.schema_key_ranges("s");
            assert!(ranges.windows(2).all(|pair| pair[0]
                .end
                .as_ref()
                .is_some_and(|end| end.as_slice() < pair[1].start.as_slice())));
            assert!(ranges.iter().all(|range| range
                .end
                .as_ref()
                .is_none_or(|end| range.start.as_slice() < end.as_slice())));
        }
    }

    #[test]
    fn schema_key_ranges_admit_every_window_key_in_every_scope() {
        let window = window(Some(("m", false)), Some(("t", true)));
        let ranges = window.schema_key_ranges("s");
        assert!(ranges.windows(2).all(|pair| pair[0]
            .end
            .as_ref()
            .is_some_and(|end| end.as_slice() < pair[1].start.as_slice())));
        for row_pk in ["m0", "n", "t"] {
            assert!(in_ranges(&ranges, &key("s", None, row_pk)), "{row_pk}");
            assert!(in_ranges(&ranges, &key("s", Some("f"), row_pk)), "{row_pk}");
        }
        for row_pk in ["a", "m", "t0", "z"] {
            assert!(!in_ranges(&ranges, &key("s", None, row_pk)), "{row_pk}");
        }
        assert!(!in_ranges(&ranges, &key("r", None, "n")));
        assert!(!in_ranges(&ranges, &key("s0", None, "n")));
    }
}
