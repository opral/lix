//! Native tests for a plugin's actual [`FileProjection`] implementation.
//!
//! Calls use the same typed page codecs and buffered outputs as the Component
//! adapter. Successful output is staged: commit it by assigning
//! `file = transition.into_snapshot()`. Errors leave the input snapshot intact.
//! [`Transition::apply_to_rows`] resolves generated identities for small native
//! fixtures. This host does not validate SQL schemas or emulate transactions;
//! qualify SQL behavior with the compiled plugin and the real engine as well.

use super::*;
use std::cell::RefCell;
use std::collections::BTreeMap;

/// Accepted bytes and plugin-private state for one file.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    pub file_id: String,
    pub path: String,
    pub bytes: Vec<u8>,
    pub state: BTreeMap<Vec<u8>, Vec<u8>>,
}

/// A successful, uncommitted plugin transition.
#[derive(Debug)]
pub struct Transition {
    pub metrics: Metrics,
    pub row_changes: Vec<TypedRowChange>,
    pub replaces_all_rows: bool,
    pub file_edits: Vec<FileEdit>,
    pub file_replacement: Option<Vec<u8>>,
    snapshot: Snapshot,
}

impl Transition {
    /// Apply emitted mutations to a small fixture, resolving generated UUIDs.
    ///
    /// `generated_column` maps a schema key to its generated UUID primary-key
    /// column. Non-generated schemas need no mapping. The fixture is updated
    /// only after every mutation succeeds. This is deliberately a fixture
    /// helper, not schema validation or a database implementation.
    pub fn apply_to_rows<'a>(
        &self,
        rows: &mut Vec<TypedRowRecord>,
        creates: CreateContext,
        generated_column: impl Fn(&str) -> Option<&'a str>,
    ) -> Result<()> {
        let mut next = if self.replaces_all_rows {
            Vec::new()
        } else {
            rows.clone()
        };
        for change in &self.row_changes {
            let mut row = change.row.clone();
            let primary_key = if let Some(local_ref) = change.local_ref {
                if !change.primary_key.is_empty() {
                    return Err(Error::invalid_input("create has an explicit primary key"));
                }
                let column = generated_column(&change.schema_key)
                    .ok_or_else(|| Error::invalid_input("missing generated identity column"))?;
                let value = TypedValue::Uuid(creates.id(local_ref));
                let row = row
                    .as_mut()
                    .ok_or_else(|| Error::invalid_input("create has no row"))?;
                if row.get(column).is_some_and(|existing| existing != &value) {
                    return Err(Error::invalid_input(
                        "create identity differs from its local reference",
                    ));
                }
                row.insert(column.to_owned(), value.clone());
                vec![value]
            } else {
                if change.primary_key.is_empty() {
                    return Err(Error::invalid_input("mutation has no primary key"));
                }
                change.primary_key.clone()
            };
            let position = next.iter().position(|row: &TypedRowRecord| {
                row.schema_key == change.schema_key && row.primary_key == primary_key
            });
            if change.local_ref.is_some() && position.is_some() {
                return Err(Error::invalid_input("duplicate generated fixture identity"));
            }
            if let Some(row) = row {
                let record = TypedRowRecord {
                    schema_key: change.schema_key.clone(),
                    schema_fingerprint: change.schema_fingerprint,
                    primary_key,
                    row,
                };
                if let Some(position) = position {
                    next[position] = record;
                } else {
                    next.push(record);
                }
            } else if let Some(position) = position {
                next.remove(position);
            }
        }
        *rows = next;
        Ok(())
    }

    /// Inspect the successor without committing it.
    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    /// Commit by replacing the caller's accepted snapshot with this value.
    pub fn into_snapshot(self) -> Snapshot {
        self.snapshot
    }
}

/// Guest-visible I/O for one successful native transition, excluding fixture
/// setup, snapshot cloning, and output decoding. File/state counts measure
/// payload bytes, excluding state keys. Row bytes include encoded pages and
/// attachments, including their in-page metadata.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Metrics {
    pub file_bytes_read: u64,
    pub file_read_calls: u64,
    pub state_bytes_read: u64,
    pub state_read_calls: u64,
    pub state_bytes_written: u64,
    /// Includes put, delete, and prefix deletion calls.
    pub state_write_calls: u64,
    pub file_bytes_written: u64,
    pub file_write_calls: u64,
    pub row_bytes_emitted: u64,
    pub row_pages_emitted: u64,
}

struct CountedSnapshot<'a> {
    snapshot: &'a Snapshot,
    metrics: &'a RefCell<Metrics>,
}

impl SnapshotHost for CountedSnapshot<'_> {
    fn file_len(&self) -> u64 {
        self.snapshot.file_len()
    }

    fn read_file(&self, offset: u64, length: u32) -> std::result::Result<Vec<u8>, CommonHostError> {
        self.metrics.borrow_mut().file_read_calls += 1;
        let bytes = self.snapshot.read_file(offset, length)?;
        self.metrics.borrow_mut().file_bytes_read += bytes.len() as u64;
        Ok(bytes)
    }

    fn read_state(
        &self,
        key: &[u8],
        offset: u64,
        max_bytes: u32,
    ) -> std::result::Result<Option<CommonRecordChunk>, CommonHostError> {
        self.metrics.borrow_mut().state_read_calls += 1;
        let chunk = self.snapshot.read_state(key, offset, max_bytes)?;
        if let Some(chunk) = &chunk {
            self.metrics.borrow_mut().state_bytes_read += chunk.bytes.len() as u64;
        }
        Ok(chunk)
    }
}

/// In-memory driver for all four file-projection hooks.
#[derive(Debug)]
pub struct Harness<F> {
    pub max_batch_bytes: u32,
    plugin: PhantomData<F>,
}

impl<F> Default for Harness<F> {
    fn default() -> Self {
        Self {
            max_batch_bytes: 1024 * 1024,
            plugin: PhantomData,
        }
    }
}

impl<F: FileProjection> Harness<F> {
    pub fn parse(&self, file: &Snapshot, creates: CreateContext) -> Result<Transition> {
        let host = RecordingHost::new(file, self.max_batch_bytes);
        let counted = CountedSnapshot {
            snapshot: file,
            metrics: &host.metrics,
        };
        let mut output = TransitionOutput::new(&host)?;
        F::parse(
            ParseInput {
                file_id: &file.file_id,
                path: &file.path,
                file: super::Snapshot { inner: &counted },
                creates,
            },
            &mut RowOutput { inner: &mut output },
        )?;
        output.finish()?;
        host.finish(file.clone())
    }

    pub fn parse_changes(
        &self,
        before: &Snapshot,
        after_path: &str,
        edits: &[FileEdit],
        cold_rows: Option<&[TypedRowRecord]>,
        creates: CreateContext,
    ) -> Result<Transition> {
        let bytes = EditSet::new(edits, before.bytes.len() as u64)?.apply(&before.bytes)?;
        let rows = cold_rows
            .map(|rows| Rows::complete(rows, self.max_batch_bytes))
            .transpose()?;
        let host = RecordingHost::new(before, self.max_batch_bytes);
        let counted = CountedSnapshot {
            snapshot: before,
            metrics: &host.metrics,
        };
        let mut output = TransitionOutput::new(&host)?;
        F::parse_changes(
            ParseChangesInput {
                file_id: &before.file_id,
                before_path: &before.path,
                after_path,
                before: super::Snapshot { inner: &counted },
                file_edits: FileEditReader { edits },
                typed_rows: rows
                    .as_ref()
                    .map(|rows| TypedRowReader::new(rows, self.max_batch_bytes)),
                creates,
            },
            &mut RowChangeOutput { inner: &mut output },
        )?;
        output.finish()?;
        let mut successor = before.clone();
        successor.bytes = bytes;
        successor.path = after_path.to_owned();
        host.finish(successor)
    }

    pub fn serialize(
        &self,
        file_id: &str,
        path: &str,
        rows: &[TypedRowRecord],
        before: Option<&Snapshot>,
    ) -> Result<Transition> {
        let rows = Rows::complete(rows, self.max_batch_bytes)?;
        let mut successor = before.cloned().unwrap_or_default();
        successor.file_id = file_id.to_owned();
        successor.path = path.to_owned();
        let host = RecordingHost::new(&successor, self.max_batch_bytes);
        let counted = before.map(|snapshot| CountedSnapshot {
            snapshot,
            metrics: &host.metrics,
        });
        let mut output = TransitionOutput::new(&host)?;
        F::serialize(
            SerializeInput {
                file_id,
                path,
                typed_rows: TypedRowReader::new(&rows, self.max_batch_bytes),
                before: counted
                    .as_ref()
                    .map(|snapshot| super::Snapshot { inner: snapshot }),
            },
            &mut FileOutput { inner: &mut output },
        )?;
        output.finish()?;
        if before.is_none() && host.record.borrow().replacement.is_none() {
            return Err(Error::invalid_input(
                "cold serialize did not emit a file replacement",
            ));
        }
        host.finish(successor)
    }

    pub fn serialize_changes(
        &self,
        before: &Snapshot,
        changes: &[TypedRowChange],
    ) -> Result<Transition> {
        let rows = Rows::changes(changes, self.max_batch_bytes)?;
        let host = RecordingHost::new(before, self.max_batch_bytes);
        let counted = CountedSnapshot {
            snapshot: before,
            metrics: &host.metrics,
        };
        let mut output = TransitionOutput::new(&host)?;
        F::serialize_changes(
            SerializeChangesInput {
                file_id: &before.file_id,
                path: &before.path,
                before: super::Snapshot { inner: &counted },
                typed_row_changes: TypedRowChangeReader::new(&rows, self.max_batch_bytes),
            },
            &mut FileEditOutput { inner: &mut output },
        )?;
        output.finish()?;
        host.finish(before.clone())
    }
}

impl SnapshotHost for Snapshot {
    fn file_len(&self) -> u64 {
        self.bytes.len() as u64
    }

    fn read_file(&self, offset: u64, length: u32) -> std::result::Result<Vec<u8>, CommonHostError> {
        read_slice(&self.bytes, offset, length, false)
    }

    fn read_state(
        &self,
        key: &[u8],
        offset: u64,
        max_bytes: u32,
    ) -> std::result::Result<Option<CommonRecordChunk>, CommonHostError> {
        self.state
            .get(key)
            .map(|bytes| {
                Ok(CommonRecordChunk {
                    total_len: bytes.len() as u64,
                    bytes: read_slice(bytes, offset, max_bytes, true)?,
                })
            })
            .transpose()
    }
}

fn read_slice(
    bytes: &[u8],
    offset: u64,
    length: u32,
    truncate: bool,
) -> std::result::Result<Vec<u8>, CommonHostError> {
    let start = usize::try_from(offset).map_err(|_| CommonHostError::InvalidRange)?;
    let end = start
        .checked_add(length as usize)
        .ok_or(CommonHostError::InvalidRange)?;
    let end = if truncate { end.min(bytes.len()) } else { end };
    bytes
        .get(start..end)
        .map(<[u8]>::to_vec)
        .ok_or(CommonHostError::InvalidRange)
}

type Page = (Vec<u8>, Vec<Vec<u8>>);

struct Rows(RefCell<VecDeque<Page>>);

impl Rows {
    fn complete(rows: &[TypedRowRecord], max_bytes: u32) -> Result<Self> {
        let host = RecordingHost::new(&Snapshot::default(), max_bytes);
        let mut output = TransitionOutput::new(&host)?;
        for row in rows {
            output.typed_row(
                &row.schema_key,
                row.schema_fingerprint,
                TypedMutation::Upsert {
                    row_pk: &row.primary_key,
                    row: &row.row,
                    effect: TypedChangeEffect::Content,
                },
            )?;
        }
        output.finish()?;
        Ok(Self(RefCell::new(host.record.into_inner().pages.into())))
    }

    fn changes(changes: &[TypedRowChange], max_bytes: u32) -> Result<Self> {
        let host = RecordingHost::new(&Snapshot::default(), max_bytes);
        let mut output = TransitionOutput::new(&host)?;
        for change in changes {
            let mutation = match (&change.row, change.local_ref) {
                (Some(row), Some(local_ref)) if change.primary_key.is_empty() => {
                    TypedMutation::Create { local_ref, row }
                }
                (Some(row), None) => TypedMutation::Upsert {
                    row_pk: &change.primary_key,
                    row,
                    effect: match change.effect {
                        ChangeEffect::Content => TypedChangeEffect::Content,
                        ChangeEffect::FormatOnly => TypedChangeEffect::FormatOnly,
                    },
                },
                (None, None) => TypedMutation::Delete {
                    row_pk: &change.primary_key,
                },
                _ => {
                    return Err(Error::invalid_input(
                        "inconsistent test row change identity",
                    ));
                }
            };
            output.typed_row(&change.schema_key, change.schema_fingerprint, mutation)?;
        }
        output.finish()?;
        Ok(Self(RefCell::new(host.record.into_inner().pages.into())))
    }
}

impl RowSourceHost for Rows {
    fn next_page(&self, _: u32) -> std::result::Result<Option<Page>, CommonHostError> {
        Ok(self.0.borrow_mut().pop_front())
    }
}

#[derive(Default)]
struct Record {
    state: BTreeMap<Vec<u8>, Vec<u8>>,
    pages: Vec<Page>,
    replaces_all_rows: bool,
    edits: Vec<FileEdit>,
    replacement: Option<Vec<u8>>,
    pending_replacement: Option<(u64, Vec<u8>)>,
}

struct RecordingHost {
    max_bytes: u32,
    metrics: RefCell<Metrics>,
    record: RefCell<Record>,
}

impl RecordingHost {
    fn new(before: &Snapshot, max_bytes: u32) -> Self {
        Self {
            max_bytes,
            metrics: RefCell::new(Metrics::default()),
            record: RefCell::new(Record {
                state: before.state.clone(),
                ..Record::default()
            }),
        }
    }

    fn check_state_size(
        &self,
        key_bytes: usize,
        value_bytes: usize,
    ) -> std::result::Result<(), CommonHostError> {
        if key_bytes
            .checked_add(value_bytes)
            .is_none_or(|size| size > self.max_bytes as usize)
        {
            return Err(CommonHostError::LimitExceeded(
                "state operation exceeds max-batch-bytes".to_owned(),
            ));
        }
        Ok(())
    }

    fn finish(self, mut snapshot: Snapshot) -> Result<Transition> {
        let record = self.record.into_inner();
        if record.pending_replacement.is_some() {
            return Err(Error::invalid_input("unfinished test file replacement"));
        }
        snapshot.bytes = match &record.replacement {
            Some(bytes) => bytes.clone(),
            None => {
                EditSet::new(&record.edits, snapshot.bytes.len() as u64)?.apply(&snapshot.bytes)?
            }
        };
        snapshot.state = record.state;
        let pages = Rows(RefCell::new(record.pages.into()));
        let mut reader = TypedRowChangeReader::new(&pages, self.max_bytes);
        let mut row_changes = Vec::new();
        while let Some(change) = reader.next()? {
            row_changes.push(change);
        }
        Ok(Transition {
            metrics: self.metrics.into_inner(),
            row_changes,
            replaces_all_rows: record.replaces_all_rows,
            file_edits: record.edits,
            file_replacement: record.replacement,
            snapshot,
        })
    }
}

impl TransitionHost for RecordingHost {
    fn max_batch_bytes(&self) -> u32 {
        self.max_bytes
    }

    fn put_state(&self, key: &[u8], value: &[u8]) -> std::result::Result<(), CommonHostError> {
        self.metrics.borrow_mut().state_write_calls += 1;
        if key.starts_with(b"\0lix/") {
            return Err(CommonHostError::Rejected(
                "host-reserved state key".to_owned(),
            ));
        }
        self.check_state_size(key.len(), value.len())?;
        self.metrics.borrow_mut().state_bytes_written += value.len() as u64;
        self.record
            .borrow_mut()
            .state
            .insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn delete_state(&self, key: &[u8]) -> std::result::Result<(), CommonHostError> {
        self.metrics.borrow_mut().state_write_calls += 1;
        if key.starts_with(b"\0lix/") {
            return Err(CommonHostError::Rejected(
                "host-reserved state key".to_owned(),
            ));
        }
        self.check_state_size(key.len(), 0)?;
        self.record.borrow_mut().state.remove(key);
        Ok(())
    }

    fn delete_state_prefix(&self, prefix: &[u8]) -> std::result::Result<(), CommonHostError> {
        self.metrics.borrow_mut().state_write_calls += 1;
        if prefix.is_empty() || prefix.starts_with(b"\0lix/") || b"\0lix/".starts_with(prefix) {
            return Err(CommonHostError::Rejected(
                "reserved or empty state prefix".to_owned(),
            ));
        }
        self.check_state_size(prefix.len(), 0)?;
        self.record
            .borrow_mut()
            .state
            .retain(|key, _| !key.starts_with(prefix));
        Ok(())
    }

    fn emit_rows(
        &self,
        payload: Vec<u8>,
        attachments: Vec<Vec<u8>>,
    ) -> std::result::Result<(), CommonHostError> {
        self.metrics.borrow_mut().row_pages_emitted += 1;
        self.metrics.borrow_mut().row_bytes_emitted +=
            (payload.len() + attachments.iter().map(Vec::len).sum::<usize>()) as u64;
        self.record.borrow_mut().pages.push((payload, attachments));
        Ok(())
    }

    fn replace_all_rows(&self) -> std::result::Result<(), CommonHostError> {
        let mut record = self.record.borrow_mut();
        if record.replaces_all_rows {
            return Err(CommonHostError::Rejected(
                "replace-all-rows was already requested".to_owned(),
            ));
        }
        record.replaces_all_rows = true;
        Ok(())
    }

    fn emit_file_edit(
        &self,
        offset: u64,
        delete_len: u64,
        insert: &[u8],
    ) -> std::result::Result<(), CommonHostError> {
        self.metrics.borrow_mut().file_write_calls += 1;
        self.check_state_size(0, insert.len())?;
        let mut record = self.record.borrow_mut();
        if record.replacement.is_some() || record.pending_replacement.is_some() {
            return Err(CommonHostError::Rejected(
                "cannot mix replacement and file edits".to_owned(),
            ));
        }
        record.edits.push(FileEdit {
            offset,
            delete_len,
            insert: insert.to_vec(),
        });
        self.metrics.borrow_mut().file_bytes_written += insert.len() as u64;
        Ok(())
    }

    fn begin_file_replacement(&self, length: u64) -> std::result::Result<(), CommonHostError> {
        let mut record = self.record.borrow_mut();
        if record.replacement.is_some()
            || record.pending_replacement.is_some()
            || !record.edits.is_empty()
        {
            return Err(CommonHostError::Rejected(
                "file output already started".to_owned(),
            ));
        }
        record.pending_replacement = Some((length, Vec::new()));
        Ok(())
    }

    fn write_file_replacement(&self, chunk: &[u8]) -> std::result::Result<(), CommonHostError> {
        self.metrics.borrow_mut().file_write_calls += 1;
        let mut record = self.record.borrow_mut();
        let Some((expected, bytes)) = &mut record.pending_replacement else {
            return Err(CommonHostError::Rejected(
                "replacement not started".to_owned(),
            ));
        };
        if chunk.len() > self.max_bytes as usize
            || bytes.len() as u64 + chunk.len() as u64 > *expected
        {
            return Err(CommonHostError::InvalidRange);
        }
        bytes.extend_from_slice(chunk);
        self.metrics.borrow_mut().file_bytes_written += chunk.len() as u64;
        Ok(())
    }

    fn finish_file_replacement(&self) -> std::result::Result<(), CommonHostError> {
        let mut record = self.record.borrow_mut();
        let Some((expected, bytes)) = record.pending_replacement.take() else {
            return Err(CommonHostError::Rejected(
                "replacement not started".to_owned(),
            ));
        };
        if bytes.len() as u64 != expected {
            return Err(CommonHostError::InvalidRange);
        }
        record.replacement = Some(bytes);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uuid_index_fixture(ids: impl IntoIterator<Item = uuid::Uuid>) -> Result<Snapshot> {
        let before = Snapshot::default();
        let host = RecordingHost::new(&before, 1024 * 1024);
        let mut output = TransitionOutput::new(&host)?;
        UuidIndex::build(&mut RowOutput { inner: &mut output }, b"test/index/", ids)?;
        output.finish()?;
        Ok(host.finish(before)?.into_snapshot())
    }

    #[test]
    fn uuid_index_preserves_arbitrary_input_ordinals_across_pages() {
        let ids: Vec<_> = (0..5_000_u128)
            .map(|i| uuid::Uuid::from_u128(((i * 3_571) % 5_000) << 64 | i))
            .collect();
        let before = uuid_index_fixture(ids.iter().copied()).unwrap();
        let metrics = RefCell::new(Metrics::default());
        let counted = CountedSnapshot {
            snapshot: &before,
            metrics: &metrics,
        };
        let snapshot = super::super::Snapshot { inner: &counted };
        for (ordinal, id) in ids.iter().enumerate() {
            assert_eq!(
                UuidIndex::lookup(&snapshot, b"test/index/", *id).unwrap(),
                Some(ordinal as u64)
            );
        }
        assert_eq!(
            UuidIndex::lookup(&snapshot, b"test/index/", uuid::Uuid::from_u128(u128::MAX)).unwrap(),
            None
        );
        assert!(metrics.borrow().state_bytes_read < 5_001 * 400);
        assert!(uuid_index_fixture([ids[0], ids[0]]).is_err());
    }

    #[test]
    fn uuid_index_missing_and_corrupt_state_are_explicit() {
        let mut before = Snapshot::default();
        let lookup = |before: &Snapshot| {
            UuidIndex::lookup(
                &super::super::Snapshot { inner: before },
                b"test/index/",
                uuid::Uuid::nil(),
            )
        };
        assert_eq!(lookup(&before).unwrap(), None);
        before = uuid_index_fixture([uuid::Uuid::nil()]).unwrap();
        let page = before
            .state
            .keys()
            .find(|key| key.starts_with(b"test/index/page/"))
            .unwrap()
            .clone();
        let saved = before.clone();
        before.state.remove(&page);
        assert!(lookup(&before).is_err());
        before = saved.clone();
        before.state.get_mut(&page).unwrap().truncate(23);
        assert!(lookup(&before).is_err());
        before = saved.clone();
        before.state.get_mut(&page).unwrap()[16..24].copy_from_slice(&1_u64.to_le_bytes());
        assert!(lookup(&before).is_err());
        before = saved;
        before.state.get_mut(b"test/index/root".as_slice()).unwrap()[0] = b'X';
        assert!(lookup(&before).is_err());
    }

    #[test]
    fn metrics_count_actual_range_payloads_and_state_operations() {
        let before = Snapshot {
            bytes: b"abcdefgh".to_vec(),
            state: BTreeMap::from([(b"index".to_vec(), b"12345".to_vec())]),
            ..Snapshot::default()
        };
        let host = RecordingHost::new(&before, 1024);
        let counted = CountedSnapshot {
            snapshot: &before,
            metrics: &host.metrics,
        };
        assert_eq!(counted.read_file(2, 3).unwrap(), b"cde");
        assert_eq!(
            counted.read_state(b"index", 3, 100).unwrap().unwrap().bytes,
            b"45"
        );
        assert!(counted.read_state(b"missing", 0, 100).unwrap().is_none());
        host.put_state(b"new", b"sixsix").unwrap();
        host.delete_state(b"missing").unwrap();
        host.emit_file_edit(0, 1, b"XY").unwrap();
        let transition = host.finish(before).unwrap();
        assert_eq!(
            transition.metrics,
            Metrics {
                file_bytes_read: 3,
                file_read_calls: 1,
                state_bytes_read: 2,
                state_read_calls: 2,
                state_bytes_written: 6,
                state_write_calls: 2,
                file_bytes_written: 2,
                file_write_calls: 1,
                ..Metrics::default()
            }
        );
    }

    #[test]
    fn fixture_identity_resolution_is_atomic_and_supports_upsert_delete() {
        let creates = CreateContext::from_namespace_bytes([1; 12]);
        let before = Snapshot::default();
        let host = RecordingHost::new(&before, 1024);
        let mut output = TransitionOutput::new(&host).unwrap();
        output
            .typed_row(
                "test",
                [1; 32],
                TypedMutation::Create {
                    local_ref: 0,
                    row: &TypedRow::default(),
                },
            )
            .unwrap();
        output.finish().unwrap();
        let mut transition = host.finish(before).unwrap();
        let mut rows = Vec::new();
        assert!(
            transition
                .apply_to_rows(&mut rows, creates, |_| None)
                .is_err()
        );
        assert!(rows.is_empty());
        transition
            .apply_to_rows(&mut rows, creates, |_| Some("id"))
            .unwrap();
        assert_eq!(
            rows[0].row.get("id"),
            Some(&TypedValue::Uuid(creates.id(0)))
        );
        let saved = rows.clone();
        let mut valid_create = transition.row_changes[0].clone();
        valid_create.local_ref = Some(1);
        transition.row_changes.insert(0, valid_create);
        assert!(
            transition
                .apply_to_rows(&mut rows, creates, |_| Some("id"))
                .is_err()
        );
        assert_eq!(rows, saved);
        transition.row_changes.remove(0);
        transition.row_changes[0].local_ref = None;
        transition.row_changes[0].primary_key = rows[0].primary_key.clone();
        transition.row_changes[0].row = Some(rows[0].row.clone());
        transition
            .apply_to_rows(&mut rows, creates, |_| None)
            .unwrap();
        assert_eq!(rows, saved);
        transition.row_changes[0].row = None;
        transition
            .apply_to_rows(&mut rows, creates, |_| None)
            .unwrap();
        assert!(rows.is_empty());
    }

    struct FailingPlugin;

    impl FileProjection for FailingPlugin {
        fn parse(_: ParseInput<'_>, output: &mut RowOutput<'_, '_>) -> Result<()> {
            output.put_state(b"test/new", b"staged")?;
            output.delete_state_prefix(b"test/old/")?;
            output.create("test", [1; 32], 0, &TypedRow::default())?;
            output.flush_page()?;
            Err(Error::invalid_input("intentional failure"))
        }

        fn parse_changes(
            _: ParseChangesInput<'_>,
            output: &mut RowChangeOutput<'_, '_>,
        ) -> Result<()> {
            output.put_state(b"test/new", b"staged")?;
            output.delete_state_prefix(b"test/old/")?;
            Err(Error::invalid_input("intentional failure"))
        }

        fn serialize(_: SerializeInput<'_>, output: &mut FileOutput<'_, '_>) -> Result<()> {
            output.put_state(b"test/new", b"staged")?;
            output.write(b"replacement")?;
            Err(Error::invalid_input("intentional failure"))
        }

        fn serialize_changes(
            _: SerializeChangesInput<'_>,
            output: &mut FileEditOutput<'_, '_>,
        ) -> Result<()> {
            output.put_state(b"test/new", b"staged")?;
            output.replace(0, 1, b"replacement")?;
            Err(Error::invalid_input("intentional failure"))
        }
    }

    #[test]
    fn failed_hooks_discard_staged_rows_bytes_and_state() {
        let harness = Harness::<FailingPlugin>::default();
        let before = Snapshot {
            file_id: "test-file".into(),
            path: "test.txt".into(),
            bytes: b"accepted".to_vec(),
            state: BTreeMap::from([(b"test/old/key".to_vec(), b"accepted state".to_vec())]),
        };
        let saved = before.clone();
        let creates = CreateContext::from_namespace_bytes([1; 12]);
        assert!(harness.parse(&before, creates).is_err());
        assert!(
            harness
                .parse_changes(
                    &before,
                    "changed.txt",
                    &[FileEdit {
                        offset: 0,
                        delete_len: 1,
                        insert: b"x".to_vec()
                    }],
                    None,
                    creates
                )
                .is_err()
        );
        assert!(
            harness
                .serialize("test-file", "test.txt", &[], Some(&before))
                .is_err()
        );
        assert!(harness.serialize_changes(&before, &[]).is_err());
        assert_eq!(before, saved);
    }

    #[test]
    fn state_operations_enforce_the_host_batch_boundary() {
        let host = RecordingHost::new(&Snapshot::default(), 4);
        host.put_state(b"ab", b"12").unwrap();
        assert!(host.put_state(b"ab", b"123").is_err());
        assert!(host.delete_state(b"abcde").is_err());
        assert!(host.delete_state_prefix(b"abcde").is_err());
        assert_eq!(
            host.record.borrow().state.get(b"ab".as_slice()).unwrap(),
            b"12"
        );
        host.delete_state(b"abcd").unwrap();
        host.delete_state_prefix(b"abcd").unwrap();
    }

    #[test]
    fn file_edits_enforce_each_insertion_batch_boundary() {
        let host = RecordingHost::new(&Snapshot::default(), 4);
        host.emit_file_edit(0, 0, b"1234").unwrap();
        assert!(host.emit_file_edit(0, 0, b"12345").is_err());
        assert_eq!(host.record.borrow().edits.len(), 1);
        // The boundary is per insertion, not the sum of separate calls.
        host.emit_file_edit(0, 0, b"5678").unwrap();
        host.emit_file_edit(0, 0, b"").unwrap();
        assert_eq!(host.record.borrow().edits.len(), 3);
    }

    #[test]
    fn native_host_preserves_emitted_rows_and_rejects_reserved_state() {
        let before = Snapshot::default();
        let host = RecordingHost::new(&before, 1024 * 1024);
        let mut output = TransitionOutput::new(&host).unwrap();
        output
            .typed_row(
                "test",
                [1; 32],
                TypedMutation::Create {
                    local_ref: 0,
                    row: &TypedRow::default(),
                },
            )
            .unwrap();
        output.replace_all_rows().unwrap();
        assert!(output.replace_all_rows().is_err());
        assert!(output.put_state(b"\0lix/reserved", b"value").is_err());
        assert!(output.delete_state(b"\0lix/reserved").is_err());
        output.finish().unwrap();
        let result = host.finish(before).unwrap();
        assert!(result.replaces_all_rows);
        assert_eq!(result.row_changes.len(), 1);
        assert_eq!(result.row_changes[0].local_ref, Some(0));
    }

    #[test]
    fn prefix_deletion_respects_call_order_and_snapshot_is_explicitly_committed() {
        let before = Snapshot {
            state: BTreeMap::from([
                (b"index/old".to_vec(), b"old".to_vec()),
                (b"index-other/keep".to_vec(), b"keep".to_vec()),
            ]),
            ..Snapshot::default()
        };
        let host = RecordingHost::new(&before, 1024 * 1024);
        let output = TransitionOutput::new(&host).unwrap();
        output.put_state(b"index/staged", b"staged").unwrap();
        output.delete_state_prefix(b"index/").unwrap();
        output.put_state(b"index/new", b"new").unwrap();
        let transition = host.finish(before.clone()).unwrap();
        assert_eq!(before.state.len(), 2);
        let committed = transition.into_snapshot();
        assert_eq!(
            committed.state,
            BTreeMap::from([
                (b"index/new".to_vec(), b"new".to_vec()),
                (b"index-other/keep".to_vec(), b"keep".to_vec()),
            ])
        );
    }
}
