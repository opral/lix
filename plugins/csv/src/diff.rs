use imara_diff::{Algorithm, Diff, InternedInput, Interner};
use std::hash::Hash;
use std::iter;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum DiffRun {
    Equal { len: usize },
    Replace { old: usize, new: usize },
}

pub(crate) fn imara_diff_runs<'a, T: Eq + Hash + ?Sized + 'a>(
    a: impl ExactSizeIterator<Item = &'a T>,
    b: impl ExactSizeIterator<Item = &'a T>,
) -> impl Iterator<Item = DiffRun> {
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

    iter::from_fn(move || {
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
            return Some(DiffRun::Equal {
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

        if before_run_len != 0 || after_run_len != 0 {
            return Some(DiffRun::Replace {
                old: usize::try_from(before_run_len).unwrap(),
                new: usize::try_from(after_run_len).unwrap(),
            });
        }

        debug_assert_eq!(before_pos, before_len);
        debug_assert_eq!(after_pos, after_len);
        None
    })
}
