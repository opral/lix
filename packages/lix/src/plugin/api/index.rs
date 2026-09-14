use super::{Error, Result, Snapshot, StateOutput};
use uuid::Uuid;

const PAGE_RECORDS: usize = 2048;
const RECORD_BYTES: usize = 24;

/// Rebuildable, paged UUID-to-ordinal index in plugin-private state.
///
/// Ordinals refer to the input order supplied to `build`. Rebuild after inserting,
/// deleting or reordering records; content-only edits do not change this index.
/// The prefix must end in `/` and belong exclusively to this index.
#[derive(Debug, Clone, Copy)]
pub struct UuidIndex;

impl UuidIndex {
    /// Builds bounded pages and stages their replacement atomically with output.
    /// Building collects and sorts identities; lookups read O(log n) small ranges.
    pub fn build(
        output: &mut impl StateOutput,
        prefix: &[u8],
        ids: impl IntoIterator<Item = Uuid>,
    ) -> Result<()> {
        validate_prefix(prefix)?;
        let mut entries: Vec<_> = ids
            .into_iter()
            .enumerate()
            .map(|(i, id)| (id, i as u64))
            .collect();
        entries.sort_unstable_by_key(|entry| entry.0);
        if entries.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(Error::invalid_input(
                "UUID index contains duplicate identities",
            ));
        }
        output.delete_state_prefix(prefix)?;
        let mut manifest = b"LUI1".to_vec();
        manifest.extend_from_slice(&(entries.len() as u64).to_le_bytes());
        output.put_state(&key(prefix, b"root"), &manifest)?;
        for (page, entries) in entries.chunks(PAGE_RECORDS).enumerate() {
            let mut bytes = Vec::with_capacity(entries.len() * RECORD_BYTES);
            for (id, ordinal) in entries {
                bytes.extend_from_slice(id.as_bytes());
                bytes.extend_from_slice(&ordinal.to_le_bytes());
            }
            output.put_state(&page_key(prefix, page as u64), &bytes)?;
        }
        Ok(())
    }

    /// Looks up an accepted identity. A missing index or identity returns `None`.
    pub fn lookup(snapshot: &Snapshot<'_>, prefix: &[u8], id: Uuid) -> Result<Option<u64>> {
        validate_prefix(prefix)?;
        let Some(manifest) = snapshot.read_state_range(&key(prefix, b"root"), 0, 13)? else {
            return Ok(None);
        };
        if manifest.len() != 12 || &manifest[..4] != b"LUI1" {
            return Err(Error::invalid_input("invalid UUID index manifest"));
        }
        let count = u64::from_le_bytes(manifest[4..].try_into().unwrap());
        let mut low = 0;
        let mut high = count;
        while low < high {
            let middle = low + (high - low) / 2;
            let bytes = snapshot
                .read_state_range(
                    &page_key(prefix, middle / PAGE_RECORDS as u64),
                    (middle % PAGE_RECORDS as u64) * RECORD_BYTES as u64,
                    RECORD_BYTES as u32,
                )?
                .ok_or_else(|| Error::invalid_input("UUID index page is missing"))?;
            if bytes.len() != RECORD_BYTES {
                return Err(Error::invalid_input("UUID index record is truncated"));
            }
            let candidate = Uuid::from_slice(&bytes[..16]).unwrap();
            match candidate.cmp(&id) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => {
                    let ordinal = u64::from_le_bytes(bytes[16..].try_into().unwrap());
                    if ordinal >= count {
                        return Err(Error::invalid_input("UUID index ordinal is out of bounds"));
                    }
                    return Ok(Some(ordinal));
                }
            }
        }
        Ok(None)
    }
}

fn validate_prefix(prefix: &[u8]) -> Result<()> {
    super::validate_state_prefix(prefix)?;
    if !prefix.ends_with(b"/") {
        return Err(Error::invalid_input("UUID index prefix must end in '/'"));
    }
    Ok(())
}

fn key(prefix: &[u8], suffix: &[u8]) -> Vec<u8> {
    [prefix, suffix].concat()
}

fn page_key(prefix: &[u8], page: u64) -> Vec<u8> {
    [prefix, b"page/", &page.to_be_bytes()].concat()
}
