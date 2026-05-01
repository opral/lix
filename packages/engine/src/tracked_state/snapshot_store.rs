use crate::backend::{KvStore, KvWriter};
use crate::tracked_state::codec::encoded_value_len;
use crate::tracked_state::storage;
use crate::tracked_state::tree_types::{StoredSnapshot, TrackedStateKey, TrackedStateValue};
use crate::LixError;

pub(crate) const DEFAULT_MAX_INLINE_ENCODED_VALUE_BYTES: usize = 1024;
const MIN_REF_TREE_SAVINGS_BYTES: usize = 128;

#[derive(Debug, Clone, Copy)]
pub(crate) struct SnapshotStore {
    max_inline_encoded_value_bytes: usize,
}

impl SnapshotStore {
    pub(crate) fn new() -> Self {
        Self {
            max_inline_encoded_value_bytes: DEFAULT_MAX_INLINE_ENCODED_VALUE_BYTES,
        }
    }

    #[cfg(feature = "storage-benches")]
    pub(crate) fn with_max_inline_encoded_value_bytes(
        max_inline_encoded_value_bytes: usize,
    ) -> Self {
        Self {
            max_inline_encoded_value_bytes,
        }
    }

    pub(crate) async fn store_value(
        &self,
        writer: &mut impl KvWriter,
        mut value: TrackedStateValue,
    ) -> Result<TrackedStateValue, LixError> {
        if let StoredSnapshot::Inline(snapshot_content) = &value.snapshot {
            let inline_encoded_bytes = encoded_value_len(&value);
            let snapshot_ref = storage::raw_snapshot_ref_for_content(snapshot_content);
            let mut ref_value = value.clone();
            ref_value.snapshot = StoredSnapshot::Ref(snapshot_ref.clone());
            let ref_encoded_bytes = encoded_value_len(&ref_value);
            if self.should_store_ref(inline_encoded_bytes, ref_encoded_bytes) {
                let encoded_snapshot =
                    storage::encode_snapshot_content_with_ref(snapshot_content, snapshot_ref)?;
                ref_value.snapshot = StoredSnapshot::Ref(encoded_snapshot.snapshot_ref.clone());
                storage::store_encoded_snapshot(writer, &encoded_snapshot).await?;
                value = ref_value;
            }
        }
        Ok(value)
    }

    fn should_store_ref(&self, inline_encoded_bytes: usize, ref_encoded_bytes: usize) -> bool {
        inline_encoded_bytes > self.max_inline_encoded_value_bytes
            && inline_encoded_bytes.saturating_sub(ref_encoded_bytes) >= MIN_REF_TREE_SAVINGS_BYTES
    }

    pub(crate) async fn resolve_value(
        store: &mut impl KvStore,
        mut value: TrackedStateValue,
    ) -> Result<TrackedStateValue, LixError> {
        if let StoredSnapshot::Ref(snapshot_ref) = &value.snapshot {
            value.snapshot = StoredSnapshot::Inline(
                storage::load_snapshot(store, snapshot_ref)
                    .await?
                    .ok_or_else(|| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!(
                                "tracked-state snapshot ref '{}' is missing",
                                snapshot_ref.hash_hex
                            ),
                        )
                    })?,
            );
        }
        Ok(value)
    }

    pub(crate) async fn resolve_rows(
        store: &mut impl KvStore,
        rows: Vec<(TrackedStateKey, TrackedStateValue)>,
        needs_snapshot_content: bool,
    ) -> Result<Vec<(TrackedStateKey, TrackedStateValue)>, LixError> {
        if !needs_snapshot_content {
            return Ok(rows);
        }
        let mut resolved = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            resolved.push((key, Self::resolve_value(store, value).await?));
        }
        Ok(resolved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adaptive_rule_keeps_small_encoded_values_inline() {
        let store = SnapshotStore::new();

        assert!(!store.should_store_ref(256, 96));
    }

    #[test]
    fn adaptive_rule_refs_large_values_when_tree_savings_are_meaningful() {
        let store = SnapshotStore::new();

        assert!(store.should_store_ref(4096, 128));
    }

    #[test]
    fn adaptive_rule_keeps_values_inline_when_ref_saves_too_little() {
        let store = SnapshotStore::new();

        assert!(!store.should_store_ref(1100, 1000));
    }
}
