use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use lix_order_key::OrderKey;
use lix_plugin_api_v2 as lix;
use serde::{Deserialize, Serialize};

pub(crate) const LINE_SCHEMA_KEY: &str = "git_text_line_v2";
const GIT_TEXT_SCAN_BYTES: usize = 8_000;

/// One verified base-relative splice received from the Component adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InputSplice {
    pub(crate) offset: u64,
    pub(crate) delete_len: u64,
    pub(crate) insert: Vec<u8>,
}

/// Immutable, byte-exact state for a Git-style text document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Document(Arc<DocumentInner>);

#[derive(Debug, PartialEq, Eq)]
struct DocumentInner {
    bytes: Arc<Vec<u8>>,
    lines: Vec<Line>,
}

/// A durable line entity. `bytes` includes its trailing LF when present.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Line {
    id: String,
    order_key: OrderKey,
    bytes: Arc<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
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
    ) -> Result<(Self, Vec<lix::EntityChange>), String> {
        validate_git_text(&bytes)?;
        let chunks = split_lines(&bytes);
        let order_keys = OrderKey::evenly_between(None, None, chunks.len())?;
        let mut lines = Vec::with_capacity(chunks.len());
        for (ordinal, (bytes, order_key)) in chunks.into_iter().zip(order_keys).enumerate() {
            lines.push(Line {
                id: id_for_ordinal(usize_to_u64(ordinal, "line ID ordinal")?),
                order_key,
                bytes: Arc::new(bytes),
            });
        }
        // A cold open constructs lines in strict order from one already
        // validated byte stream. Host ordinal IDs and evenly-spaced order
        // keys are unique by construction, so routing this through
        // `from_lines` would only revalidate every line, populate two trees,
        // sort the already-sorted vector, render the original file again, and
        // validate that duplicate byte buffer a second time.
        let document = Self(Arc::new(DocumentInner {
            bytes: Arc::new(bytes),
            lines,
        }));
        let changes = document.all_upserts()?;
        Ok((document, changes))
    }

    pub(crate) fn open_entities(
        records: impl IntoIterator<Item = lix::EntityRecord>,
    ) -> Result<Self, String> {
        let mut lines = Vec::new();
        for record in records {
            validate_entity_key(&record.schema_key, &record.entity_pk)?;
            let line = Line::from_snapshot(&record.snapshot)?;
            if record.entity_pk[0] != line.id {
                return Err(format!(
                    "line entity primary key '{}' does not match snapshot id '{}'",
                    record.entity_pk[0], line.id
                ));
            }
            lines.push(line);
        }
        Self::from_lines(lines)
    }

    pub(crate) fn file_changed(
        &self,
        splices: &[InputSplice],
        mut id_for_ordinal: impl FnMut(u64) -> String,
    ) -> Result<(Self, Vec<lix::EntityChange>), String> {
        let bytes = apply_splices(self.bytes(), splices)?;
        validate_git_text(&bytes)?;
        let chunks = split_lines(&bytes);

        // Exact byte matches preserve their entity identity even across a
        // reorder. Remaining old/new positions are paired in order, which
        // preserves an edited line's ID without inventing a parser-specific
        // identity rule for arbitrary text.
        let mut exact = BTreeMap::<&[u8], VecDeque<usize>>::new();
        for (old_index, line) in self.lines().iter().enumerate() {
            exact
                .entry(line.bytes.as_slice())
                .or_default()
                .push_back(old_index);
        }
        let mut old_for_new = vec![None; chunks.len()];
        let mut old_used = vec![false; self.lines().len()];
        for (new_index, bytes) in chunks.iter().enumerate() {
            let Some(candidates) = exact.get_mut(bytes.as_slice()) else {
                continue;
            };
            // A candidate entry remains in the map after its queue is
            // exhausted. Further equal successor lines are new lines, not an
            // invariant violation.
            let Some(old_index) = candidates.pop_front() else {
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
            .map(|line| line.id.clone())
            .collect::<BTreeSet<_>>();
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
                    if !known_ids.insert(id.clone()) {
                        return Err(format!(
                            "new line ID '{id}' collides with an existing line identity"
                        ));
                    }
                    id
                }
            };
            lines.push(Line {
                id,
                order_key,
                bytes: Arc::new(bytes),
            });
        }

        // The splice result and each constructed line were validated above.
        // Reconciled order keys follow successor byte order, while reused and
        // Schema-defaulted UUIDs are already collision checked. Preserve that
        // exact owned state instead of sorting, rendering, and validating the
        // complete document a second time.
        let document = Self(Arc::new(DocumentInner {
            bytes: Arc::new(bytes),
            lines,
        }));
        let changes = self.changes_to(&document)?;
        Ok((document, changes))
    }

    pub(crate) fn entities_changed(
        &self,
        changes: impl IntoIterator<Item = lix::EntityChange>,
    ) -> Result<(Self, Vec<lix::ByteEdit>), String> {
        let mut lines = self
            .lines()
            .iter()
            .cloned()
            .map(|line| (line.id.clone(), line))
            .collect::<BTreeMap<_, _>>();

        for change in changes {
            validate_entity_key(&change.schema_key, &change.entity_pk)?;
            let id = &change.entity_pk[0];
            match change.snapshot {
                Some(snapshot) => {
                    let line = Line::from_snapshot(&snapshot)?;
                    if line.id != *id {
                        return Err(format!(
                            "line entity primary key '{id}' does not match snapshot id '{}'",
                            line.id
                        ));
                    }
                    lines.insert(id.clone(), line);
                }
                None => {
                    if lines.remove(id).is_none() {
                        return Err(format!("cannot delete unknown line entity '{id}'"));
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

    pub(crate) fn lines(&self) -> &[Line] {
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
                return Err(format!("duplicate line entity ID '{}'", line.id));
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
        let bytes = render_lines(&lines)?;
        validate_git_text(&bytes)?;
        Ok(Self(Arc::new(DocumentInner {
            bytes: Arc::new(bytes),
            lines,
        })))
    }

    fn all_upserts(&self) -> Result<Vec<lix::EntityChange>, String> {
        self.lines()
            .iter()
            .map(Line::upsert_change)
            .collect::<Result<Vec<_>, _>>()
    }

    fn changes_to(&self, after: &Self) -> Result<Vec<lix::EntityChange>, String> {
        let before = self
            .lines()
            .iter()
            .map(|line| (line.id.as_str(), line))
            .collect::<BTreeMap<_, _>>();
        let after_by_id = after
            .lines()
            .iter()
            .map(|line| (line.id.as_str(), line))
            .collect::<BTreeMap<_, _>>();

        let mut changes = Vec::new();
        for line in self.lines() {
            if !after_by_id.contains_key(line.id.as_str()) {
                changes.push(lix::EntityChange::delete(
                    LINE_SCHEMA_KEY,
                    vec![line.id.clone()],
                ));
            }
        }
        for line in after.lines() {
            let changed = before
                .get(line.id.as_str())
                .is_none_or(|before_line| before_line != &line);
            if changed {
                changes.push(line.upsert_change()?);
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
                        after.bytes.as_ref().clone(),
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
        serde_json::to_vec(&LineSnapshot {
            id: self.id.clone(),
            order_key: self.order_key.to_snapshot_string(),
            content_base64: URL_SAFE_NO_PAD.encode(self.bytes.as_slice()),
        })
        .map_err(|error| format!("failed to serialize line snapshot: {error}"))
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
            bytes: Arc::new(bytes),
        })
    }

    fn upsert_change(&self) -> Result<lix::EntityChange, String> {
        Ok(lix::EntityChange::upsert(
            LINE_SCHEMA_KEY,
            vec![self.id.clone()],
            self.snapshot_bytes()?,
        ))
    }
}

fn split_lines(bytes: &[u8]) -> Vec<Vec<u8>> {
    if bytes.is_empty() {
        return Vec::new();
    }
    bytes
        .split_inclusive(|byte| *byte == b'\n')
        .map(ToOwned::to_owned)
        .collect()
}

fn validate_line_bytes(bytes: &[u8]) -> Result<(), String> {
    let Some((_, prefix)) = bytes.split_last() else {
        return Err("line entities must contain at least one byte".to_owned());
    };
    if prefix.contains(&b'\n') {
        return Err(
            "one Git text line may contain one trailing LF but no embedded LF bytes".to_owned(),
        );
    }
    Ok(())
}

fn apply_splices(before: &[u8], splices: &[InputSplice]) -> Result<Vec<u8>, String> {
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

fn validate_entity_key(schema_key: &str, entity_pk: &[String]) -> Result<(), String> {
    if schema_key != LINE_SCHEMA_KEY {
        return Err(format!(
            "Git text plugin only accepts schema '{LINE_SCHEMA_KEY}', got '{schema_key}'"
        ));
    }
    let [id] = entity_pk else {
        return Err("Git text line entities need exactly one primary-key component".to_owned());
    };
    if id.is_empty() {
        return Err("Git text line entity primary key must not be empty".to_owned());
    }
    Ok(())
}

fn validate_git_text(bytes: &[u8]) -> Result<(), String> {
    if bytes[..bytes.len().min(GIT_TEXT_SCAN_BYTES)].contains(&0) {
        return Err("Git text documents cannot contain NUL in their first 8 KiB".to_owned());
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
