//! Disposable line pages and page-length sums for content-only SQL edits.
use super::{Document, RowChange, StateOutput, core, sdk};
use std::collections::BTreeMap;

const PREFIX: &[u8] = b"text/point/";
const IDS: &[u8] = b"text/point/ids/";
const ROOT: &[u8] = b"text/point/root";
const PAGE: usize = 4096;

#[derive(Clone)]
struct Entry {
    length: u64,
    order: String,
}

pub(super) fn build(output: &mut impl StateOutput, document: &Document) -> sdk::Result<()> {
    output.delete_state_prefix(PREFIX)?;
    // Very large fractional keys remain supported by the document fallback.
    if document
        .lines()
        .iter()
        .any(|line| line.order_key().len() > 128)
    {
        return Ok(());
    }
    sdk::UuidIndex::build(output, IDS, document.lines().iter().map(|line| line.id()))?;
    let count = document.lines().len();
    let mut root = b"LTP1".to_vec();
    root.extend_from_slice(&(count as u64).to_le_bytes());
    output.put_state(ROOT, &root)?;
    let mut sums = vec![0u64; count.div_ceil(PAGE) + 1];
    for (page, lines) in document.lines().chunks(PAGE).enumerate() {
        let entries: Vec<_> = lines
            .iter()
            .map(|line| Entry {
                length: line.bytes().len() as u64,
                order: line.order_key(),
            })
            .collect();
        sums[page + 1] = entries.iter().map(|entry| entry.length).sum();
        output.put_state(&key(b"lines/", page as u64), &encode(&entries))?;
    }
    // Fenwick summaries over line pages avoid shifting every later byte offset.
    for i in 1..sums.len() {
        let parent = i + (i & i.wrapping_neg());
        if parent < sums.len() {
            sums[parent] += sums[i];
        }
    }
    for (page, values) in sums.chunks(PAGE).enumerate() {
        let bytes: Vec<_> = values.iter().flat_map(|n| n.to_le_bytes()).collect();
        output.put_state(&key(b"sums/", page as u64), &bytes)?;
    }
    Ok(())
}

pub(super) fn serialize_changes(
    before: &sdk::Snapshot<'_>,
    changes: &[RowChange],
    output: &mut sdk::FileEditOutput<'_, '_>,
) -> sdk::Result<bool> {
    if changes.is_empty() {
        return Ok(true);
    }
    if changes.len() > 64 {
        return Ok(false);
    }
    let Some(root) = before.read_state_range(ROOT, 0, 13)? else {
        return Ok(false);
    };
    if root.len() != 12 || &root[..4] != b"LTP1" {
        return Err(invalid());
    }
    let count = u64::from_le_bytes(root[4..].try_into().unwrap());
    if count > before.len() {
        return Err(invalid());
    }
    let page_count = count.div_ceil(PAGE as u64);
    let mut pages = BTreeMap::<u64, Vec<Entry>>::new();
    let mut edits = Vec::new();
    let mut updates = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for change in changes {
        if change.schema_key.as_ref() != core::LINE_SCHEMA_KEY {
            return Ok(false);
        }
        let (Some(row), [sdk::TypedValue::Uuid(id)]) = (&change.row, change.row_pk.as_slice())
        else {
            return Ok(false);
        };
        if !seen.insert(*id) {
            return Ok(false);
        }
        let Some(ordinal) = sdk::UuidIndex::lookup(before, IDS, *id)? else {
            return Ok(false);
        };
        if ordinal >= count {
            return Err(invalid());
        }
        let line = core::Line::from_typed_row(row).map_err(sdk::Error::invalid_input)?;
        if line.id() != *id {
            return Err(sdk::Error::invalid_input(
                "line row primary key does not match row id",
            ));
        }
        let page = ordinal / PAGE as u64;
        if let std::collections::btree_map::Entry::Vacant(slot) = pages.entry(page) {
            let bytes = before
                .get_state(&key(b"lines/", page))?
                .ok_or_else(invalid)?;
            let entries = decode(&bytes)?;
            let expected = (count - page * PAGE as u64).min(PAGE as u64) as usize;
            if entries.len() != expected {
                return Err(invalid());
            }
            slot.insert(entries);
        }
        let entries = &pages[&page];
        let within = (ordinal % PAGE as u64) as usize;
        let entry = &entries[within];
        if entry.order != line.order_key() {
            return Ok(false);
        }
        if ordinal + 1 < count && !line.bytes().ends_with(b"\n") {
            return Err(sdk::Error::invalid_input(
                "every nonfinal text line must end with LF",
            ));
        }
        let mut offset = entries[..within].iter().try_fold(0u64, |sum, entry| {
            sum.checked_add(entry.length).ok_or_else(invalid)
        })?;
        let mut i = page;
        while i > 0 {
            offset = offset
                .checked_add(read_sum(before, i)?)
                .ok_or_else(invalid)?;
            i -= i & i.wrapping_neg();
        }
        if offset
            .checked_add(entry.length)
            .is_none_or(|end| end > before.len())
        {
            return Err(invalid());
        }
        // Only read the affected line; no accepted content is stored in the index.
        if before.read_range(offset, entry.length)? != line.bytes() {
            edits.push(sdk::FileEdit {
                offset,
                delete_len: entry.length,
                insert: line.bytes().to_vec(),
            });
        }
        updates.push((page, within, line.bytes().len() as u64, entry.length));
    }
    edits.sort_by_key(|edit| edit.offset);
    let edited = sdk::EditSet::new(&edits, before.len())?;
    if edited
        .read_range(before, 0, edited.len().min(8000))?
        .contains(&0)
    {
        return Err(sdk::Error::invalid_input(
            "Text files cannot contain NUL in the first 8000 bytes",
        ));
    }
    let mut sums = BTreeMap::<u64, Vec<u8>>::new();
    let mut changed_pages = std::collections::BTreeSet::new();
    for (page, within, length, old_length) in updates {
        if length == old_length {
            continue;
        }
        pages.get_mut(&page).unwrap()[within].length = length;
        changed_pages.insert(page);
        let mut i = page + 1;
        while i <= page_count {
            let sum_page = i / PAGE as u64;
            if let std::collections::btree_map::Entry::Vacant(slot) = sums.entry(sum_page) {
                let bytes = before
                    .get_state(&key(b"sums/", sum_page))?
                    .ok_or_else(invalid)?;
                let expected =
                    (page_count + 1 - sum_page * PAGE as u64).min(PAGE as u64) as usize * 8;
                if bytes.len() != expected {
                    return Err(invalid());
                }
                slot.insert(bytes);
            }
            let bytes = sums.get_mut(&sum_page).unwrap();
            let start = (i % PAGE as u64) as usize * 8;
            let value = u64::from_le_bytes(bytes[start..start + 8].try_into().unwrap());
            let value = value
                .checked_sub(old_length)
                .and_then(|v| v.checked_add(length))
                .ok_or_else(invalid)?;
            bytes[start..start + 8].copy_from_slice(&value.to_le_bytes());
            i = i.checked_add(i & i.wrapping_neg()).ok_or_else(invalid)?;
        }
    }
    for edit in edits {
        output.replace(edit.offset, edit.delete_len, &edit.insert)?;
    }
    for page in changed_pages {
        output.put_state(&key(b"lines/", page), &encode(&pages[&page]))?;
    }
    for (page, bytes) in sums {
        output.put_state(&key(b"sums/", page), &bytes)?;
    }
    Ok(true)
}

fn read_sum(snapshot: &sdk::Snapshot<'_>, i: u64) -> sdk::Result<u64> {
    let bytes = snapshot
        .read_state_range(&key(b"sums/", i / PAGE as u64), (i % PAGE as u64) * 8, 8)?
        .ok_or_else(invalid)?;
    Ok(u64::from_le_bytes(
        bytes.as_slice().try_into().map_err(|_| invalid())?,
    ))
}

fn key(kind: &[u8], page: u64) -> Vec<u8> {
    [PREFIX, kind, &page.to_be_bytes()].concat()
}
fn invalid() -> sdk::Error {
    sdk::Error::invalid_input("invalid Text point index")
}

fn encode(entries: &[Entry]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for entry in entries {
        bytes.extend_from_slice(&entry.length.to_le_bytes());
        bytes.extend_from_slice(&(entry.order.len() as u32).to_le_bytes());
        bytes.extend_from_slice(entry.order.as_bytes());
    }
    bytes
}

fn decode(mut bytes: &[u8]) -> sdk::Result<Vec<Entry>> {
    let mut entries = Vec::new();
    while !bytes.is_empty() {
        if bytes.len() < 12 || entries.len() == PAGE {
            return Err(invalid());
        }
        let length = u64::from_le_bytes(bytes[..8].try_into().unwrap());
        let order_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
        bytes = &bytes[12..];
        if length == 0 || order_len > 128 || order_len > bytes.len() {
            return Err(invalid());
        }
        let order = std::str::from_utf8(&bytes[..order_len])
            .map_err(|_| invalid())?
            .to_owned();
        bytes = &bytes[order_len..];
        entries.push(Entry { length, order });
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sdk::testing::{Harness, Snapshot};

    fn setup(count: usize) -> (Snapshot, Vec<sdk::TypedRowChange>) {
        let snapshot = Snapshot {
            file_id: "text-index".into(),
            path: "/index.txt".into(),
            bytes: b"ordinary content\n".repeat(count),
            ..Snapshot::default()
        };
        let parsed = Harness::<crate::TextPlugin>::default()
            .parse(&snapshot, sdk::CreateContext::from_namespace_bytes([1; 12]))
            .unwrap();
        let rows = parsed
            .row_changes
            .iter()
            .map(|row| {
                let mut row = row.clone();
                row.primary_key = vec![row.row.as_ref().unwrap().get("id").unwrap().clone()];
                row.local_ref = None;
                row
            })
            .collect();
        (parsed.into_snapshot(), rows)
    }

    #[test]
    fn indexed_length_changes_match_full_render_across_pages_and_reopen() {
        let (mut snapshot, mut rows) = setup(9000);
        let driver = Harness::<crate::TextPlugin>::default();
        for changes in [vec![0, 4096, 8999], vec![4095, 4096, 8192], vec![0, 8191]] {
            let mut batch = Vec::new();
            for (iteration, ordinal) in changes.into_iter().enumerate() {
                let mut change = rows[ordinal].clone();
                change.row.as_mut().unwrap().insert(
                    "content",
                    sdk::TypedValue::Text("replacement".repeat(iteration + 1)),
                );
                rows[ordinal] = change.clone();
                batch.push(change);
            }
            let mut cold = snapshot.clone();
            cold.state.retain(|key, _| !key.starts_with(PREFIX));
            let expected = driver.serialize_changes(&cold, &batch).unwrap();
            let actual = driver.serialize_changes(&snapshot, &batch).unwrap();
            assert_eq!(actual.snapshot().bytes, expected.snapshot().bytes);
            assert!(actual.metrics.file_bytes_read < 9000);
            assert!(actual.metrics.state_bytes_written < 300_000);
            snapshot = actual.into_snapshot();
        }
    }

    #[test]
    fn same_length_point_edit_writes_no_state_and_invalid_line_rolls_back() {
        let (snapshot, mut rows) = setup(10_000);
        let mut change = rows.remove(5000);
        change
            .row
            .as_mut()
            .unwrap()
            .insert("content", sdk::TypedValue::Text("Ordinary content".into()));
        let driver = Harness::<crate::TextPlugin>::default();
        let result = driver
            .serialize_changes(&snapshot, &[change.clone()])
            .unwrap();
        assert_eq!(result.metrics.state_bytes_written, 0);
        assert!(result.metrics.file_bytes_read < 9000);
        assert!(result.metrics.state_bytes_read < 100_000);
        change
            .row
            .as_mut()
            .unwrap()
            .insert("line_ending", sdk::TypedValue::Text(String::new()));
        assert!(driver.serialize_changes(&snapshot, &[change]).is_err());
        let file_edit = sdk::FileEdit {
            offset: 5000 * 17,
            delete_len: 1,
            insert: b"O".to_vec(),
        };
        let result = driver
            .parse_changes(
                &snapshot,
                "/index.txt",
                &[file_edit],
                None,
                sdk::CreateContext::from_namespace_bytes([2; 12]),
            )
            .unwrap();
        assert_eq!(result.metrics.state_bytes_written, 0);
    }
}
