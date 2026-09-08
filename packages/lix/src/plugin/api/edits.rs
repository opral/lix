use super::{Error, FileEdit, Result, Snapshot};

/// Validated byte edits in the coordinates of one accepted file.
///
/// Starts must be strictly increasing and deletion ranges must not overlap.
/// Adjacent edits are valid; two insertions at the same offset are not.
/// Validation never sorts or changes the edits. Text encoding and syntax
/// boundaries remain the plugin's responsibility.
#[derive(Debug)]
pub struct EditSet<'a> {
    edits: &'a [FileEdit],
    before_len: u64,
    after_len: u64,
}

impl<'a> EditSet<'a> {
    pub fn new(edits: &'a [FileEdit], before_len: u64) -> Result<Self> {
        let mut previous_start = None;
        let mut previous_end = 0;
        let mut deleted = 0_u64;
        let mut inserted = 0_u64;
        for edit in edits {
            let end = edit
                .offset
                .checked_add(edit.delete_len)
                .ok_or_else(|| Error::invalid_input("file edit range overflowed"))?;
            if previous_start == Some(edit.offset) || edit.offset < previous_end || end > before_len
            {
                return Err(Error::invalid_input(
                    "file edits must have increasing starts, not overlap, and stay within the accepted file",
                ));
            }
            deleted += edit.delete_len;
            inserted = inserted
                .checked_add(edit.insert.len() as u64)
                .ok_or_else(|| Error::limit_exceeded("edited file length overflowed"))?;
            previous_start = Some(edit.offset);
            previous_end = end;
        }
        let after_len = (before_len - deleted)
            .checked_add(inserted)
            .ok_or_else(|| Error::limit_exceeded("edited file length overflowed"))?;
        Ok(Self {
            edits,
            before_len,
            after_len,
        })
    }

    /// Length in bytes of the resulting file.
    pub fn len(&self) -> u64 {
        self.after_len
    }

    /// Whether the resulting file has no bytes.
    pub fn is_empty(&self) -> bool {
        self.after_len == 0
    }

    /// Applies all edits after checking that the base has the validated length.
    pub fn apply(&self, before: &[u8]) -> Result<Vec<u8>> {
        self.check_base(before.len() as u64)?;
        self.read_with(0, self.after_len, |offset, length| {
            Ok(before[offset as usize..(offset + length) as usize].to_vec())
        })
    }

    /// Reads a range in successor coordinates without materializing the file.
    /// Only unchanged bytes intersecting the requested range are read from the
    /// snapshot; inserted bytes are read directly from the edit set.
    pub fn read_range(&self, before: &Snapshot<'_>, offset: u64, length: u64) -> Result<Vec<u8>> {
        self.check_base(before.len())?;
        self.read_with(offset, length, |start, len| before.read_range(start, len))
    }

    fn check_base(&self, length: u64) -> Result<()> {
        if length != self.before_len {
            return Err(Error::invalid_input("file edit base length changed"));
        }
        Ok(())
    }

    fn read_with(
        &self,
        offset: u64,
        length: u64,
        mut read: impl FnMut(u64, u64) -> Result<Vec<u8>>,
    ) -> Result<Vec<u8>> {
        let end = offset
            .checked_add(length)
            .filter(|end| *end <= self.after_len)
            .ok_or_else(|| Error::invalid_input("edited file read exceeds the resulting file"))?;
        let capacity = usize::try_from(length)
            .map_err(|_| Error::limit_exceeded("edited file read exceeds guest address space"))?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(capacity)
            .map_err(|_| Error::limit_exceeded("edited file read allocation failed"))?;
        let mut before_cursor = 0;
        let mut after_cursor = 0;
        for edit in self.edits {
            let unchanged = edit.offset - before_cursor;
            if let Some((start, len)) = intersection(after_cursor, unchanged, offset, end) {
                output.extend(read(before_cursor + start, len)?);
            }
            after_cursor += unchanged;
            if let Some((start, len)) =
                intersection(after_cursor, edit.insert.len() as u64, offset, end)
            {
                output.extend_from_slice(&edit.insert[start as usize..(start + len) as usize]);
            }
            after_cursor += edit.insert.len() as u64;
            before_cursor = edit.offset + edit.delete_len;
            if after_cursor >= end {
                break;
            }
        }
        if after_cursor < end {
            if let Some((start, len)) =
                intersection(after_cursor, self.before_len - before_cursor, offset, end)
            {
                output.extend(read(before_cursor + start, len)?);
            }
        }
        if output.len() != capacity {
            return Err(Error::invalid_input(
                "edited file source returned a short read",
            ));
        }
        Ok(output)
    }
}

// Returns an offset relative to the segment and its intersecting length.
fn intersection(segment: u64, length: u64, start: u64, end: u64) -> Option<(u64, u64)> {
    let from = segment.max(start);
    let to = (segment + length).min(end);
    (from < to).then(|| (from - segment, to - from))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn edit(offset: u64, delete_len: u64, insert: &[u8]) -> FileEdit {
        FileEdit {
            offset,
            delete_len,
            insert: insert.to_vec(),
        }
    }

    #[test]
    fn invalid_edits_fail_before_reading() {
        for edits in [
            vec![edit(2, 0, b"a"), edit(1, 0, b"b")],
            vec![edit(1, 0, b"a"), edit(1, 0, b"b")],
            vec![edit(0, 2, b""), edit(1, 1, b"")],
            vec![edit(4, 0, b"")],
            vec![edit(u64::MAX, 1, b"")],
        ] {
            assert!(EditSet::new(&edits, 3).is_err());
        }
        let edits = [edit(3, 0, b"!")];
        assert_eq!(
            EditSet::new(&edits, 3).unwrap().apply(b"abc").unwrap(),
            b"abc!"
        );
        assert!(EditSet::new(&edits, 3).unwrap().apply(b"ab").is_err());
    }

    #[test]
    fn all_small_edit_pairs_and_ranges_match_splice_oracle() {
        let before = b"a\r\n\xffb";
        let n = before.len();
        for first in 0..=n {
            for first_end in first..=n {
                for second in (first + 1).max(first_end)..=n {
                    for second_end in second..=n {
                        for insert in [b"".as_slice(), b"x", b"\r\n!"] {
                            let edits = [
                                edit(first as u64, (first_end - first) as u64, insert),
                                edit(second as u64, (second_end - second) as u64, b"z"),
                            ];
                            let mut expected = before.to_vec();
                            for edit in edits.iter().rev() {
                                expected.splice(
                                    edit.offset as usize..(edit.offset + edit.delete_len) as usize,
                                    edit.insert.iter().copied(),
                                );
                            }
                            let set = EditSet::new(&edits, n as u64).unwrap();
                            assert_eq!(set.apply(before).unwrap(), expected);
                            for start in 0..=expected.len() {
                                for end in start..=expected.len() {
                                    let actual = set
                                        .read_with(
                                            start as u64,
                                            (end - start) as u64,
                                            |offset, len| {
                                                Ok(before[offset as usize..(offset + len) as usize]
                                                    .to_vec())
                                            },
                                        )
                                        .unwrap();
                                    assert_eq!(actual, expected[start..end]);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn range_reads_only_fetch_intersecting_base_bytes() {
        let edits = [edit(4, 2, b"XYZ")];
        let set = EditSet::new(&edits, 1_000_000).unwrap();
        let mut reads = Vec::new();
        assert_eq!(
            set.read_with(3, 5, |offset, len| {
                reads.push((offset, len));
                Ok(vec![b'.'; len as usize])
            })
            .unwrap(),
            b".XYZ."
        );
        assert_eq!(reads, [(3, 1), (6, 1)]);
        assert_eq!(
            set.read_with(4, 3, |_, _| panic!("insertion must not read base"))
                .unwrap(),
            b"XYZ"
        );
        assert!(set.read_with(set.len(), 1, |_, _| unreachable!()).is_err());
        assert!(set.read_with(u64::MAX, 1, |_, _| unreachable!()).is_err());
    }

    #[test]
    fn empty_noop_and_full_deletion() {
        assert!(EditSet::new(&[], 0).unwrap().apply(b"").unwrap().is_empty());
        assert_eq!(EditSet::new(&[], 3).unwrap().apply(b"abc").unwrap(), b"abc");
        let edits = [edit(0, 3, b"")];
        let set = EditSet::new(&edits, 3).unwrap();
        assert!(set.is_empty());
        assert!(set.apply(b"abc").unwrap().is_empty());
    }

    #[test]
    fn large_coordinates_and_read_failures_are_checked() {
        let growing = [edit(u64::MAX, 0, b"!")];
        assert!(EditSet::new(&growing, u64::MAX).is_err());
        let deleting = [edit(0, u64::MAX, b"!")];
        let set = EditSet::new(&deleting, u64::MAX).unwrap();
        assert_eq!(set.len(), 1);
        assert_eq!(set.read_with(0, 1, |_, _| unreachable!()).unwrap(), b"!");
        let set = EditSet::new(&[], 3).unwrap();
        assert!(set.read_with(0, 3, |_, _| Ok(vec![0])).is_err());
        let error = Error::internal("source failed");
        assert_eq!(set.read_with(0, 3, |_, _| Err(error.clone())), Err(error));
    }
}
