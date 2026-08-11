use std::collections::{BTreeMap, BTreeSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::ops::Range;
use std::sync::Arc;

use crate::model as lix;
use crate::order_key::OrderKey;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::Deserialize;

pub(crate) const LINE_SCHEMA_KEY: &str = "text_line";
const TEXT_PREFIX_SCAN_BYTES: usize = 8_000;

/// One verified base-relative splice received from the Component adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileEdit {
    pub(crate) offset: u64,
    pub(crate) delete_len: u64,
    pub(crate) insert: Vec<u8>,
}

/// Immutable, byte-exact state for a byte-exact text document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Document(Arc<DocumentInner>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LineIdentity {
    pub(crate) id: String,
    pub(crate) order_key: String,
}

#[derive(Debug, PartialEq, Eq)]
struct DocumentInner {
    bytes: Arc<Vec<u8>>,
    lines: Vec<Arc<Line>>,
}

/// A durable line row. `bytes` includes its trailing LF when present.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Line {
    id: String,
    order_key: OrderKey,
    bytes: LineBytes,
}

#[derive(Debug, Clone)]
struct LineBytes {
    backing: Arc<Vec<u8>>,
    range: Range<usize>,
}

impl LineBytes {
    fn new(backing: Arc<Vec<u8>>, range: Range<usize>) -> Self {
        Self { backing, range }
    }

    fn owned(bytes: Vec<u8>) -> Self {
        let end = bytes.len();
        Self::new(Arc::new(bytes), 0..end)
    }

    fn as_slice(&self) -> &[u8] {
        &self.backing[self.range.clone()]
    }

    fn len(&self) -> usize {
        self.range.len()
    }
}

impl PartialEq for LineBytes {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for LineBytes {}

pub(crate) struct AllUpserts {
    document: Document,
    index: usize,
}

impl Iterator for AllUpserts {
    type Item = Result<lix::RowChange, String>;

    fn next(&mut self) -> Option<Self::Item> {
        let line = self.document.lines().get(self.index)?;
        self.index += 1;
        Some(line.upsert_change())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.document.lines().len().saturating_sub(self.index);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for AllUpserts {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LineSnapshot {
    id: String,
    order_key: String,
    content_base64: String,
}

impl Document {
    pub(crate) fn open_file(
        bytes: Vec<u8>,
        mut id_for_ordinal: impl FnMut(u64) -> String,
    ) -> Result<(Self, AllUpserts), String> {
        validate_text(&bytes)?;
        let bytes = Arc::new(bytes);
        let chunks = split_lines(Arc::clone(&bytes));
        let order_keys = OrderKey::evenly_between(None, None, chunks.len())?;
        let mut lines = Vec::with_capacity(chunks.len());
        for (ordinal, (bytes, order_key)) in chunks.into_iter().zip(order_keys).enumerate() {
            lines.push(Arc::new(Line {
                id: id_for_ordinal(usize_to_u64(ordinal, "line ID ordinal")?),
                order_key,
                bytes,
            }));
        }
        // A cold open constructs lines in strict order from one already
        // validated byte stream. Host ordinal IDs and evenly-spaced order
        // keys are unique by construction, so routing this through
        // `from_lines` would only revalidate every line, populate two trees,
        // sort the already-sorted vector, render the original file again, and
        // validate that duplicate byte buffer a second time.
        let document = Self(Arc::new(DocumentInner { bytes, lines }));
        let changes = AllUpserts {
            document: document.clone(),
            index: 0,
        };
        Ok((document, changes))
    }

    pub(crate) fn open_file_with_identities(
        bytes: Vec<u8>,
        identities: Vec<LineIdentity>,
    ) -> Result<Self, String> {
        validate_text(&bytes)?;
        let bytes = Arc::new(bytes);
        let chunks = split_lines(Arc::clone(&bytes));
        if chunks.len() != identities.len() {
            return Err("Text identity state has the wrong line count".to_owned());
        }
        let lines = chunks
            .into_iter()
            .zip(identities)
            .map(|(bytes, identity)| {
                Ok(Line {
                    id: identity.id,
                    order_key: OrderKey::from_snapshot_string(&identity.order_key)
                        .map_err(|error| format!("invalid line order key: {error}"))?,
                    bytes,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let document = Self::from_lines(lines)?;
        if document.bytes() != bytes.as_slice() {
            return Err("Text identity order does not match accepted bytes".to_owned());
        }
        Ok(document)
    }

    pub(crate) fn identities(&self) -> Vec<LineIdentity> {
        self.lines()
            .iter()
            .map(|line| LineIdentity {
                id: line.id.clone(),
                order_key: line.order_key.to_snapshot_string(),
            })
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn open_rows(
        records: impl IntoIterator<Item = lix::RowRecord>,
    ) -> Result<Self, String> {
        Self::open_rows_fallible(records.into_iter().map(Ok))
    }

    pub(crate) fn open_rows_fallible(
        records: impl IntoIterator<Item = Result<lix::RowRecord, String>>,
    ) -> Result<Self, String> {
        let mut lines = Vec::new();
        for record in records {
            let record = record?;
            validate_row_key(&record.schema_key, &record.row_pk)?;
            let line = Line::from_snapshot(&record.snapshot)?;
            if record.row_pk[0] != line.id {
                return Err(format!(
                    "line row primary key '{}' does not match snapshot id '{}'",
                    record.row_pk[0], line.id
                ));
            }
            lines.push(line);
        }
        Self::from_lines(lines)
    }

    pub(crate) fn file_changed(
        &self,
        splices: &[FileEdit],
        mut id_for_ordinal: impl FnMut(u64) -> String,
    ) -> Result<(Self, Vec<lix::RowChange>), String> {
        let bytes = Arc::new(apply_splices(self.bytes(), splices)?);
        validate_text(&bytes)?;
        let chunks = split_lines(Arc::clone(&bytes));

        // Preserve the overwhelmingly common unchanged prefix and suffix
        // without indexing the complete document. Generated bundles can have
        // hundreds of thousands of lines with a localized edit; putting every
        // line into the reorder matcher needlessly exhausts the guest heap.
        let prefix_len = self
            .lines()
            .iter()
            .zip(&chunks)
            .take_while(|(old, new)| old.bytes.as_slice() == new.as_slice())
            .count();
        let suffix_len = self.lines()[prefix_len..]
            .iter()
            .rev()
            .zip(chunks[prefix_len..].iter().rev())
            .take_while(|(old, new)| old.bytes.as_slice() == new.as_slice())
            .count();

        let mut old_for_new = vec![None; chunks.len()];
        let mut old_used = vec![false; self.lines().len()];
        for index in 0..prefix_len {
            old_for_new[index] = Some(index);
            old_used[index] = true;
        }
        for offset in 0..suffix_len {
            let old_index = self.lines().len() - suffix_len + offset;
            let new_index = chunks.len() - suffix_len + offset;
            old_for_new[new_index] = Some(old_index);
            old_used[old_index] = true;
        }

        // Exact byte matches in the changed middle preserve their row
        // identity even across a reorder. Remaining old/new positions are
        // paired in order, preserving an edited line's ID without inventing a
        // parser-specific identity rule for arbitrary text.
        let mut exact =
            Vec::with_capacity(self.lines().len().saturating_sub(prefix_len + suffix_len));
        for (old_index, line) in self.lines()[prefix_len..self.lines().len() - suffix_len]
            .iter()
            .enumerate()
            .map(|(index, line)| (prefix_len + index, line))
        {
            exact.push((line_hash(line.bytes.as_slice()), old_index));
        }
        exact.sort_unstable();
        for (new_index, bytes) in chunks[prefix_len..chunks.len() - suffix_len]
            .iter()
            .enumerate()
            .map(|(index, bytes)| (prefix_len + index, bytes))
        {
            let hash = line_hash(bytes.as_slice());
            let start = exact.partition_point(|(candidate, _)| *candidate < hash);
            let end = exact.partition_point(|(candidate, _)| *candidate <= hash);
            let Some(old_index) = exact[start..end]
                .iter()
                .map(|(_, old_index)| *old_index)
                .find(|old_index| {
                    !old_used[*old_index]
                        && self.lines()[*old_index].bytes.as_slice() == bytes.as_slice()
                })
            else {
                continue;
            };
            old_for_new[new_index] = Some(old_index);
            old_used[old_index] = true;
        }

        let unmatched_old = old_used
            .iter()
            .enumerate()
            .filter_map(|(index, used)| (!used).then_some(index))
            .collect::<Vec<_>>();
        let unmatched_new = old_for_new
            .iter()
            .enumerate()
            .filter_map(|(index, old)| old.is_none().then_some(index))
            .collect::<Vec<_>>();
        for (old_index, new_index) in unmatched_old.into_iter().zip(unmatched_new) {
            old_for_new[new_index] = Some(old_index);
            old_used[old_index] = true;
        }

        let order_keys = self.reconciled_order_keys(&old_for_new)?;
        let mut known_ids = self
            .lines()
            .iter()
            .map(|line| line.id.as_str())
            .collect::<Vec<_>>();
        known_ids.sort_unstable();
        let mut new_ids = BTreeSet::new();
        let mut next_ordinal = 0u64;
        let mut lines = Vec::with_capacity(chunks.len());
        for ((bytes, old_index), order_key) in chunks.into_iter().zip(old_for_new).zip(order_keys) {
            let id = match old_index {
                Some(old_index) => self.lines()[old_index].id.clone(),
                None => {
                    let id = id_for_ordinal(next_ordinal);
                    next_ordinal = next_ordinal
                        .checked_add(1)
                        .ok_or_else(|| "line ID ordinal overflow".to_owned())?;
                    if known_ids.binary_search(&id.as_str()).is_ok() || !new_ids.insert(id.clone())
                    {
                        return Err(format!(
                            "new line ID '{id}' collides with an existing line identity"
                        ));
                    }
                    id
                }
            };
            if let Some(old_index) = old_index
                && self.lines()[old_index].order_key == order_key
                && self.lines()[old_index].bytes == bytes
            {
                lines.push(Arc::clone(&self.lines()[old_index]));
            } else {
                lines.push(Arc::new(Line {
                    id,
                    order_key,
                    bytes,
                }));
            }
        }

        // The splice result and each constructed line were validated above.
        // Reconciled order keys follow successor byte order, while reused and
        // Schema-defaulted UUIDs are already collision checked. Preserve that
        // exact owned state instead of sorting, rendering, and validating the
        // complete document a second time.
        let document = Self(Arc::new(DocumentInner { bytes, lines }));
        let changes = self.changes_to(&document)?;
        Ok((document, changes))
    }

    pub(crate) fn rows_changed(
        &self,
        changes: impl IntoIterator<Item = lix::RowChange>,
    ) -> Result<(Self, Vec<lix::ByteEdit>), String> {
        let mut lines = self
            .lines()
            .iter()
            .map(|line| (line.id.clone(), line.as_ref().clone()))
            .collect::<BTreeMap<_, _>>();

        for change in changes {
            validate_row_key(&change.schema_key, &change.row_pk)?;
            let id = &change.row_pk[0];
            match change.snapshot {
                Some(snapshot) => {
                    let line = Line::from_snapshot(&snapshot)?;
                    if line.id != *id {
                        return Err(format!(
                            "line row primary key '{id}' does not match snapshot id '{}'",
                            line.id
                        ));
                    }
                    lines.insert(id.clone(), line);
                }
                None => {
                    if lines.remove(id).is_none() {
                        return Err(format!("cannot delete unknown line row '{id}'"));
                    }
                }
            }
        }

        let document = Self::from_lines(lines.into_values().collect())?;
        let edits = self.renderer_edits_to(&document)?;
        Ok((document, edits))
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        self.0.bytes.as_slice()
    }

    pub(crate) fn lines(&self) -> &[Arc<Line>] {
        &self.0.lines
    }

    fn from_lines(mut lines: Vec<Line>) -> Result<Self, String> {
        let mut ids = BTreeSet::new();
        let mut order_keys = BTreeSet::new();
        for line in &lines {
            if line.id.is_empty() {
                return Err("line IDs must not be empty".to_owned());
            }
            validate_line_bytes(line.bytes.as_slice())?;
            if !ids.insert(line.id.clone()) {
                return Err(format!("duplicate line row ID '{}'", line.id));
            }
            if !order_keys.insert(line.order_key.clone()) {
                return Err(format!(
                    "duplicate line order key '{}'",
                    line.order_key.to_snapshot_string()
                ));
            }
        }
        lines.sort_by(|left, right| {
            left.order_key
                .cmp(&right.order_key)
                .then_with(|| left.id.cmp(&right.id))
        });
        let bytes = Arc::new(render_lines(&lines)?);
        validate_text(&bytes)?;
        let mut offset = 0usize;
        for line in &mut lines {
            let end = offset
                .checked_add(line.bytes.len())
                .ok_or_else(|| "line byte range overflow".to_owned())?;
            line.bytes = LineBytes::new(Arc::clone(&bytes), offset..end);
            offset = end;
        }
        Ok(Self(Arc::new(DocumentInner {
            bytes,
            lines: lines.into_iter().map(Arc::new).collect(),
        })))
    }

    fn changes_to(&self, after: &Self) -> Result<Vec<lix::RowChange>, String> {
        let mut before = self
            .lines()
            .iter()
            .map(|line| (line.id.as_str(), line))
            .collect::<Vec<_>>();
        let mut after_by_id = after
            .lines()
            .iter()
            .map(|line| (line.id.as_str(), line))
            .collect::<Vec<_>>();
        before.sort_unstable_by_key(|(id, _)| *id);
        after_by_id.sort_unstable_by_key(|(id, _)| *id);

        let mut changes = Vec::new();
        let mut before_index = 0usize;
        let mut after_index = 0usize;
        while before_index < before.len() || after_index < after_by_id.len() {
            match (before.get(before_index), after_by_id.get(after_index)) {
                (Some((before_id, before_line)), Some((after_id, after_line))) => {
                    match before_id.cmp(after_id) {
                        std::cmp::Ordering::Less => {
                            changes.push(lix::RowChange::delete(
                                LINE_SCHEMA_KEY,
                                vec![(*before_id).to_owned()],
                            ));
                            before_index += 1;
                        }
                        std::cmp::Ordering::Greater => {
                            changes.push(after_line.upsert_change()?);
                            after_index += 1;
                        }
                        std::cmp::Ordering::Equal => {
                            if before_line != after_line {
                                changes.push(after_line.upsert_change()?);
                            }
                            before_index += 1;
                            after_index += 1;
                        }
                    }
                }
                (Some((before_id, _)), None) => {
                    changes.push(lix::RowChange::delete(
                        LINE_SCHEMA_KEY,
                        vec![(*before_id).to_owned()],
                    ));
                    before_index += 1;
                }
                (None, Some((_, after_line))) => {
                    changes.push(after_line.upsert_change()?);
                    after_index += 1;
                }
                (None, None) => break,
            }
        }
        Ok(changes)
    }

    fn reconciled_order_keys(
        &self,
        old_for_new: &[Option<usize>],
    ) -> Result<Vec<OrderKey>, String> {
        let anchors = longest_increasing_old_indexes(old_for_new);
        let mut order_keys = vec![None; old_for_new.len()];
        for &position in &anchors {
            let old_index =
                old_for_new[position].expect("an order anchor always references an existing line");
            order_keys[position] = Some(self.lines()[old_index].order_key.clone());
        }

        let mut previous = None;
        let mut cursor = 0usize;
        for anchor in anchors {
            let next = order_keys[anchor]
                .as_ref()
                .expect("an order anchor key was assigned")
                .clone();
            let allocated =
                OrderKey::evenly_between(previous.as_ref(), Some(&next), anchor - cursor)?;
            for (position, key) in (cursor..anchor).zip(allocated) {
                order_keys[position] = Some(key);
            }
            previous = Some(next);
            cursor = anchor
                .checked_add(1)
                .ok_or_else(|| "line order cursor overflow".to_owned())?;
        }
        let allocated =
            OrderKey::evenly_between(previous.as_ref(), None, old_for_new.len() - cursor)?;
        for (position, key) in (cursor..old_for_new.len()).zip(allocated) {
            order_keys[position] = Some(key);
        }

        order_keys
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| "failed to assign an order key to every line".to_owned())
    }

    fn renderer_edits_to(&self, after: &Self) -> Result<Vec<lix::ByteEdit>, String> {
        if self.bytes() == after.bytes() {
            return Ok(Vec::new());
        }

        let same_layout = self.lines().len() == after.lines().len()
            && self
                .lines()
                .iter()
                .zip(after.lines())
                .all(|(before, after)| {
                    before.id == after.id && before.order_key == after.order_key
                });
        if same_layout {
            let mut offset = 0u64;
            let mut edits = Vec::new();
            for (before, after) in self.lines().iter().zip(after.lines()) {
                if before.bytes != after.bytes {
                    edits.push(lix::ByteEdit::new(
                        offset,
                        usize_to_u64(before.bytes.len(), "line byte length")?,
                        after.bytes.as_slice().to_vec(),
                    ));
                }
                offset = offset
                    .checked_add(usize_to_u64(before.bytes.len(), "line byte length")?)
                    .ok_or_else(|| "line offset overflow".to_owned())?;
            }
            return Ok(edits);
        }

        let (prefix, suffix) = common_prefix_and_suffix(self.bytes(), after.bytes());
        let delete_len = self
            .bytes()
            .len()
            .checked_sub(prefix)
            .and_then(|length| length.checked_sub(suffix))
            .ok_or_else(|| "invalid common byte range".to_owned())?;
        let insert_end = after
            .bytes()
            .len()
            .checked_sub(suffix)
            .ok_or_else(|| "invalid common byte suffix".to_owned())?;
        Ok(vec![lix::ByteEdit::new(
            usize_to_u64(prefix, "render edit offset")?,
            usize_to_u64(delete_len, "render edit delete length")?,
            after.bytes()[prefix..insert_end].to_vec(),
        )])
    }
}

impl Line {
    #[cfg(test)]
    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    #[cfg(test)]
    pub(crate) fn order_key(&self) -> String {
        self.order_key.to_snapshot_string()
    }

    #[cfg(test)]
    pub(crate) fn bytes(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    pub(crate) fn snapshot_bytes(&self) -> Result<Vec<u8>, String> {
        let id = serde_json::to_vec(&self.id)
            .map_err(|error| format!("failed to serialize line ID: {error}"))?;
        let order_key = serde_json::to_vec(&self.order_key.to_snapshot_string())
            .map_err(|error| format!("failed to serialize line order key: {error}"))?;
        let content_len = base64::encoded_len(self.bytes.len(), false)
            .ok_or_else(|| "base64 line snapshot length overflow".to_owned())?;
        let prefix_len = b"{\"content_base64\":\"".len()
            + b",\"id\":".len()
            + id.len()
            + b",\"order_key\":".len()
            + order_key.len();
        let capacity = prefix_len
            .checked_add(content_len)
            .and_then(|length| length.checked_add(b"\"}".len()))
            .ok_or_else(|| "line snapshot length overflow".to_owned())?;

        let mut snapshot = Vec::with_capacity(capacity);
        // Certified snapshots use the engine's canonical lexicographic object
        // key order, avoiding a host-side normalization allocation.
        snapshot.extend_from_slice(b"{\"content_base64\":\"");
        let content_start = snapshot.len();
        snapshot.resize(content_start + content_len, 0);
        let written = URL_SAFE_NO_PAD
            .encode_slice(self.bytes.as_slice(), &mut snapshot[content_start..])
            .map_err(|error| format!("failed to base64-encode line snapshot: {error}"))?;
        snapshot.truncate(content_start + written);
        snapshot.extend_from_slice(b"\",\"id\":");
        snapshot.extend_from_slice(&id);
        snapshot.extend_from_slice(b",\"order_key\":");
        snapshot.extend_from_slice(&order_key);
        snapshot.push(b'}');
        Ok(snapshot)
    }

    fn from_snapshot(snapshot: &[u8]) -> Result<Self, String> {
        let snapshot = serde_json::from_slice::<LineSnapshot>(snapshot)
            .map_err(|error| format!("line snapshot must be valid JSON: {error}"))?;
        if snapshot.id.is_empty() {
            return Err("line snapshot id must not be empty".to_owned());
        }
        let order_key = OrderKey::from_snapshot_string(&snapshot.order_key)
            .map_err(|error| format!("invalid line order key: {error}"))?;
        let bytes = URL_SAFE_NO_PAD
            .decode(snapshot.content_base64)
            .map_err(|error| format!("invalid line content_base64: {error}"))?;
        validate_line_bytes(&bytes)?;
        Ok(Self {
            id: snapshot.id,
            order_key,
            bytes: LineBytes::owned(bytes),
        })
    }

    fn upsert_change(&self) -> Result<lix::RowChange, String> {
        Ok(lix::RowChange::upsert(
            LINE_SCHEMA_KEY,
            vec![self.id.clone()],
            self.snapshot_bytes()?,
        ))
    }
}

fn line_hash(bytes: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

fn split_lines(bytes: Arc<Vec<u8>>) -> Vec<LineBytes> {
    if bytes.is_empty() {
        return Vec::new();
    }
    let mut lines = Vec::new();
    let mut start = 0usize;
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b'\n' {
            let end = index + 1;
            lines.push(LineBytes::new(Arc::clone(&bytes), start..end));
            start = end;
        }
    }
    if start < bytes.len() {
        lines.push(LineBytes::new(Arc::clone(&bytes), start..bytes.len()));
    }
    lines
}

fn validate_line_bytes(bytes: &[u8]) -> Result<(), String> {
    let Some((_, prefix)) = bytes.split_last() else {
        return Err("line rows must contain at least one byte".to_owned());
    };
    if prefix.contains(&b'\n') {
        return Err(
            "one Text line may contain one trailing LF but no embedded LF bytes".to_owned(),
        );
    }
    Ok(())
}

fn apply_splices(before: &[u8], splices: &[FileEdit]) -> Result<Vec<u8>, String> {
    let mut after = Vec::with_capacity(before.len());
    let mut cursor = 0usize;
    for splice in splices {
        let start = usize::try_from(splice.offset)
            .map_err(|_| "splice offset exceeds this platform's address space".to_owned())?;
        let delete_len = usize::try_from(splice.delete_len)
            .map_err(|_| "splice delete length exceeds this platform's address space".to_owned())?;
        let end = start
            .checked_add(delete_len)
            .ok_or_else(|| "splice range overflows".to_owned())?;
        if start < cursor || end > before.len() {
            return Err("file splices must be sorted, non-overlapping, and in bounds".to_owned());
        }
        after.extend_from_slice(&before[cursor..start]);
        after.extend_from_slice(&splice.insert);
        cursor = end;
    }
    after.extend_from_slice(&before[cursor..]);
    Ok(after)
}

fn render_lines(lines: &[Line]) -> Result<Vec<u8>, String> {
    let total = lines.iter().try_fold(0usize, |total, line| {
        total
            .checked_add(line.bytes.len())
            .ok_or_else(|| "rendered text document is too large".to_owned())
    })?;
    let mut bytes = Vec::with_capacity(total);
    for line in lines {
        bytes.extend_from_slice(line.bytes.as_slice());
    }
    Ok(bytes)
}

fn validate_row_key(schema_key: &str, row_pk: &[String]) -> Result<(), String> {
    if schema_key != LINE_SCHEMA_KEY {
        return Err(format!(
            "Text plugin only accepts schema '{LINE_SCHEMA_KEY}', got '{schema_key}'"
        ));
    }
    let [id] = row_pk else {
        return Err("Text line rows need exactly one primary-key component".to_owned());
    };
    if id.is_empty() {
        return Err("Text line row primary key must not be empty".to_owned());
    }
    Ok(())
}

fn validate_text(bytes: &[u8]) -> Result<(), String> {
    if bytes[..bytes.len().min(TEXT_PREFIX_SCAN_BYTES)].contains(&0) {
        return Err("Text documents cannot contain NUL in their first 8 KiB".to_owned());
    }
    Ok(())
}

fn longest_increasing_old_indexes(old_for_new: &[Option<usize>]) -> Vec<usize> {
    let mut tails = Vec::<usize>::new();
    let mut predecessors = vec![None; old_for_new.len()];

    for (position, old_index) in old_for_new.iter().enumerate() {
        let Some(old_index) = old_index else {
            continue;
        };
        let insertion = tails.partition_point(|tail_position| {
            old_for_new[*tail_position].expect("LIS tails only contain matched positions")
                < *old_index
        });
        if insertion != 0 {
            predecessors[position] = Some(tails[insertion - 1]);
        }
        if insertion == tails.len() {
            tails.push(position);
        } else {
            tails[insertion] = position;
        }
    }

    let mut anchors = Vec::with_capacity(tails.len());
    let mut current = tails.last().copied();
    while let Some(position) = current {
        anchors.push(position);
        current = predecessors[position];
    }
    anchors.reverse();
    anchors
}

fn common_prefix_and_suffix(before: &[u8], after: &[u8]) -> (usize, usize) {
    let prefix = before
        .iter()
        .zip(after)
        .take_while(|(left, right)| left == right)
        .count();
    let suffix = before[prefix..]
        .iter()
        .rev()
        .zip(after[prefix..].iter().rev())
        .take_while(|(left, right)| left == right)
        .count();
    (prefix, suffix)
}

fn usize_to_u64(value: usize, context: &str) -> Result<u64, String> {
    u64::try_from(value).map_err(|_| format!("{context} exceeds u64"))
}
