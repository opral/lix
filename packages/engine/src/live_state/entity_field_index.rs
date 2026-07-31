use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of;
use std::sync::Mutex;

use bytes::Bytes;

const MAX_FIELD_INDEX_CACHE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, PartialEq, Eq)]
struct FieldIndexKey {
    branch_id: String,
    generation: String,
    schema_key: String,
    column: String,
}

#[derive(Debug)]
struct CachedFieldIndex {
    key: FieldIndexKey,
    rows_by_value: BTreeMap<String, Vec<Option<Bytes>>>,
    bytes: usize,
}

/// Bounded, current-generation secondary indexes for registered string
/// columns. A generation change replaces the prior index for the same
/// branch/schema/column, so readers never observe stale rows and historical
/// generations cannot accumulate without bound.
#[derive(Debug, Default)]
pub(crate) struct EntitySnapshotFieldIndexCache {
    entries: Mutex<Vec<CachedFieldIndex>>,
}

impl EntitySnapshotFieldIndexCache {
    pub(crate) fn get(
        &self,
        branch_id: &str,
        generation: &str,
        schema_key: &str,
        column: &str,
        values: &[String],
    ) -> Option<Vec<Option<Bytes>>> {
        let mut entries = self
            .entries
            .lock()
            .expect("entity field index cache lock poisoned");
        let position = entries.iter().position(|entry| {
            entry.key.branch_id == branch_id
                && entry.key.generation == generation
                && entry.key.schema_key == schema_key
                && entry.key.column == column
        })?;
        let entry = entries.remove(position);
        let result = collect_index_values(&entry.rows_by_value, values);
        entries.push(entry);
        Some(result)
    }

    pub(crate) fn insert(
        &self,
        branch_id: &str,
        generation: &str,
        schema_key: &str,
        column: &str,
        rows_by_value: BTreeMap<String, Vec<Option<Bytes>>>,
        values: &[String],
    ) -> Vec<Option<Bytes>> {
        self.insert_with_limit(
            branch_id,
            generation,
            schema_key,
            column,
            rows_by_value,
            values,
            MAX_FIELD_INDEX_CACHE_BYTES,
        )
    }

    #[expect(clippy::too_many_arguments)]
    fn insert_with_limit(
        &self,
        branch_id: &str,
        generation: &str,
        schema_key: &str,
        column: &str,
        rows_by_value: BTreeMap<String, Vec<Option<Bytes>>>,
        values: &[String],
        max_bytes: usize,
    ) -> Vec<Option<Bytes>> {
        let mut entries = self
            .entries
            .lock()
            .expect("entity field index cache lock poisoned");
        if let Some(entry) = entries.iter().find(|entry| {
            entry.key.branch_id == branch_id
                && entry.key.generation == generation
                && entry.key.schema_key == schema_key
                && entry.key.column == column
        }) {
            return collect_index_values(&entry.rows_by_value, values);
        }
        let key = FieldIndexKey {
            branch_id: branch_id.to_owned(),
            generation: generation.to_owned(),
            schema_key: schema_key.to_owned(),
            column: column.to_owned(),
        };
        let bytes = size_of::<CachedFieldIndex>()
            + key.branch_id.capacity()
            + key.generation.capacity()
            + key.schema_key.capacity()
            + key.column.capacity()
            + rows_by_value
                .iter()
                .map(|(value, rows)| {
                    value.capacity()
                        + rows.capacity() * size_of::<Option<Bytes>>()
                        + rows.iter().flatten().map(Bytes::len).sum::<usize>()
                })
                .sum::<usize>();
        let result = collect_index_values(&rows_by_value, values);
        entries.retain(|entry| {
            entry.key.branch_id != branch_id
                || entry.key.schema_key != schema_key
                || entry.key.column != column
        });
        if bytes > max_bytes {
            return result;
        }
        entries.push(CachedFieldIndex {
            key,
            rows_by_value,
            bytes,
        });
        while entries.iter().map(|entry| entry.bytes).sum::<usize>() > max_bytes {
            entries.remove(0);
        }
        result
    }
}

fn collect_index_values(
    rows_by_value: &BTreeMap<String, Vec<Option<Bytes>>>,
    values: &[String],
) -> Vec<Option<Bytes>> {
    values
        .iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .flat_map(|value| rows_by_value.get(value).into_iter().flatten().cloned())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::EntitySnapshotFieldIndexCache;
    use bytes::Bytes;
    use std::collections::BTreeMap;

    #[test]
    fn field_index_is_value_scoped_and_replaced_by_new_revision() {
        let cache = EntitySnapshotFieldIndexCache::default();
        let first = BTreeMap::from([
            ("a".to_string(), vec![Some(Bytes::from_static(b"a1"))]),
            ("b".to_string(), vec![Some(Bytes::from_static(b"b1"))]),
        ]);
        assert_eq!(
            cache.insert("main", "g:1", "message", "bundleId", first, &["b".into()]),
            [Some(Bytes::from_static(b"b1"))]
        );
        assert_eq!(
            cache.get(
                "main",
                "g:1",
                "message",
                "bundleId",
                &["a".into(), "a".into()]
            ),
            Some(vec![Some(Bytes::from_static(b"a1"))])
        );

        let second = BTreeMap::from([("a".to_string(), vec![Some(Bytes::from_static(b"a2"))])]);
        cache.insert("main", "g:2", "message", "bundleId", second, &[]);
        assert!(
            cache
                .get("main", "g:1", "message", "bundleId", &["a".into()])
                .is_none(),
            "the superseded branch/schema/column revision must be evicted"
        );
        assert_eq!(
            cache.get("main", "g:2", "message", "bundleId", &["a".into()]),
            Some(vec![Some(Bytes::from_static(b"a2"))])
        );
    }

    #[test]
    fn oversized_field_index_is_returned_without_being_cached() {
        let cache = EntitySnapshotFieldIndexCache::default();
        let rows = BTreeMap::from([(
            "a".to_string(),
            vec![Some(Bytes::from_static(b"oversized"))],
        )]);
        assert_eq!(
            cache.insert_with_limit("main", "g:1", "message", "bundleId", rows, &["a".into()], 1),
            [Some(Bytes::from_static(b"oversized"))]
        );
        assert_eq!(
            cache.get("main", "g:1", "message", "bundleId", &["a".into()]),
            None
        );
    }
}
