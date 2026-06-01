use imara_diff::{Algorithm, Diff, InternedInput, Interner};
use itertools::Itertools;
use std::hash::Hash;
use std::{cmp, iter};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Op {
    Equal,
    Replace,
    Insert,
    Delete,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct OpRun {
    pub(crate) op: Op,
    pub(crate) len: usize,
}

#[expect(dead_code)]
fn diff_runs_by<'a, T, U>(
    a: &'a [T],
    b: &'a [U],
    eq: impl FnMut(&T, &U) -> bool + 'a,
) -> impl Iterator<Item = OpRun> + 'a {
    diff_by(a, b, eq)
        .map(|op| OpRun { op, len: 1 })
        .coalesce(|left, right| {
            if left.op == right.op {
                Ok(OpRun {
                    op: left.op,
                    len: left.len + right.len,
                })
            } else {
                Err((left, right))
            }
        })
}

#[expect(dead_code)]
fn diff_by<'a, T, U>(
    a: &'a [T],
    b: &'a [U],
    mut eq: impl FnMut(&T, &U) -> bool + 'a,
) -> impl Iterator<Item = Op> + 'a {
    let prefix = a.iter().zip(b.iter()).take_while(|(a, b)| eq(a, b)).count();

    let a_rest = &a[prefix..];
    let b_rest = &b[prefix..];
    let suffix = a_rest
        .iter()
        .rev()
        .zip(b_rest.iter().rev())
        .take_while(|(a, b)| eq(a, b))
        .count()
        .min(a_rest.len())
        .min(b_rest.len());

    let a_mid = a.len() - prefix - suffix;
    let b_mid = b.len() - prefix - suffix;
    let replace = cmp::min(a_mid, b_mid);

    iter::empty()
        .chain((0..prefix).map(|_| Op::Equal))
        .chain(
            a[prefix..prefix + replace]
                .iter()
                .zip_eq(&b[prefix..prefix + replace])
                .map(move |(a, b)| if eq(a, b) { Op::Equal } else { Op::Replace }),
        )
        .chain((replace..a_mid).map(|_| Op::Delete))
        .chain((replace..b_mid).map(|_| Op::Insert))
        .chain((0..suffix).map(|_| Op::Equal))
}

pub(crate) fn imara_diff_runs<'a, T: Eq + Hash + ?Sized + 'a>(
    a: impl ExactSizeIterator<Item = &'a T>,
    b: impl ExactSizeIterator<Item = &'a T>,
) -> impl Iterator<Item = OpRun> {
    let before_capacity = a.len();
    let after_capacity = b.len();
    let mut input = InternedInput {
        before: Vec::with_capacity(before_capacity),
        after: Vec::with_capacity(after_capacity),
        interner: Interner::new(before_capacity + after_capacity),
    };
    input.update_before(a);
    input.update_after(b);

    let mut diff = Diff::default();
    diff.compute_with(
        Algorithm::Histogram,
        &input.before,
        &input.after,
        input.interner.num_tokens(),
    );
    let before_len = u32::try_from(input.before.len()).unwrap();
    let after_len = u32::try_from(input.after.len()).unwrap();
    let mut before_pos = 0u32;
    let mut after_pos = 0u32;
    let mut pending = [None; 3];
    let mut pending_index = 0usize;
    let mut pending_len = 0usize;

    iter::from_fn(move || {
        if pending_index < pending_len {
            let run = pending[pending_index];
            pending_index += 1;
            return run;
        }
        pending_index = 0;
        pending_len = 0;

        let equal_start = before_pos;
        while before_pos < before_len
            && after_pos < after_len
            && !diff.is_removed(before_pos)
            && !diff.is_added(after_pos)
        {
            before_pos += 1;
            after_pos += 1;
        }
        let equal_len = before_pos - equal_start;
        if equal_len != 0 {
            return Some(OpRun {
                op: Op::Equal,
                len: usize::try_from(equal_len).unwrap(),
            });
        }

        let before_start = before_pos;
        while before_pos < before_len && diff.is_removed(before_pos) {
            before_pos += 1;
        }
        let before_run_len = before_pos - before_start;

        let after_start = after_pos;
        while after_pos < after_len && diff.is_added(after_pos) {
            after_pos += 1;
        }
        let after_run_len = after_pos - after_start;

        let replace_len = before_run_len.min(after_run_len);
        for run in [
            OpRun {
                op: Op::Replace,
                len: usize::try_from(replace_len).unwrap(),
            },
            OpRun {
                op: Op::Delete,
                len: usize::try_from(before_run_len - replace_len).unwrap(),
            },
            OpRun {
                op: Op::Insert,
                len: usize::try_from(after_run_len - replace_len).unwrap(),
            },
        ] {
            if run.len != 0 {
                pending[pending_len] = Some(run);
                pending_len += 1;
            }
        }

        if pending_len != 0 {
            pending_index = 1;
            return pending[0].take();
        }

        debug_assert_eq!(before_pos, before_len);
        debug_assert_eq!(after_pos, after_len);
        None
    })
}
