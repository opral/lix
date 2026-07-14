use std::collections::BTreeMap;
use std::ops::Bound;

/// Single space used by these fixtures; cross-space isolation is pinned
/// by the baseline cross-space tests.
const TEST_SPACE: SpaceId = SpaceId(7);

use bytes::Bytes;

use crate::storage::conformance::{
    ConformanceReport, ConformanceResult, StorageFactory,
    fixtures::{full_put, key, put_batch},
    model::ReferenceModel,
    open_storage,
};
use crate::storage::{
    GetOptions, Key, KeyRange, ProjectedValue, ReadEntry, ReadOptions, ScanChunk, ScanOptions,
    SpaceId, Storage, StorageRead, StorageWrite, WriteOptions,
};

pub(crate) async fn register<F>(report: &mut ConformanceReport, factory: &F)
where
    F: StorageFactory,
{
    report
        .run(
            "model::deterministic_history_matches_reference_model",
            deterministic_history_matches_reference_model(factory),
        )
        .await;
}

async fn deterministic_history_matches_reference_model<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let mut model = ReferenceModel::default();
    let mut rng = TinyRng::new(0x51ce_deed);
    let keys = [
        key("a"),
        key("b"),
        key("c"),
        key("d"),
        key("aa"),
        key("ab"),
        key("ba"),
    ];

    for step in 0..48 {
        let old_read = storage
            .begin_read(ReadOptions::default())
            .await
            .map_err(|error| format!("step {step}: begin old read failed: {error}"))?;
        let old_model = model.clone();
        let mut write = storage
            .begin_write(WriteOptions::default())
            .await
            .map_err(|error| format!("step {step}: begin write failed: {error}"))?;
        let mut staged = model.clone();

        let mutation_count = 1 + rng.usize(4);
        for mutation_index in 0..mutation_count {
            let target_key = keys[rng.usize(keys.len())].clone();
            if rng.bool() {
                let value = Bytes::from(format!("v{step}-{mutation_index}"));
                write
                    .put_many(
                        TEST_SPACE,
                        put_batch([full_put(target_key.clone(), value.clone())]),
                    )
                    .await
                    .map_err(|error| format!("step {step}: put_many failed: {error}"))?;
                staged.put(target_key, value);
            } else {
                write
                    .delete_many(TEST_SPACE, std::slice::from_ref(&target_key))
                    .await
                    .map_err(|error| format!("step {step}: delete_many failed: {error}"))?;
                staged.delete(&target_key);
            }
        }

        if rng.bool() {
            write
                .commit()
                .await
                .map_err(|error| format!("step {step}: commit failed: {error}"))?;
            model = staged;
        } else {
            write
                .rollback()
                .await
                .map_err(|error| format!("step {step}: rollback failed: {error}"))?;
        }

        compare_read_to_model(
            &old_read,
            &old_model,
            &keys,
            &mut rng,
            &format!("step {step} old snapshot"),
        )
        .await?;

        let new_read = storage
            .begin_read(ReadOptions::default())
            .await
            .map_err(|error| format!("step {step}: begin new read failed: {error}"))?;
        compare_read_to_model(
            &new_read,
            &model,
            &keys,
            &mut rng,
            &format!("step {step} new snapshot"),
        )
        .await?;
    }

    Ok(())
}

async fn compare_read_to_model<R>(
    read: &R,
    model: &ReferenceModel,
    keys: &[Key],
    rng: &mut TinyRng,
    label: &str,
) -> ConformanceResult
where
    R: StorageRead,
{
    let point_keys = [
        keys[rng.usize(keys.len())].clone(),
        keys[rng.usize(keys.len())].clone(),
        key("missing"),
        keys[rng.usize(keys.len())].clone(),
    ];
    let result = read
        .get_many(TEST_SPACE, &point_keys, GetOptions::default())
        .await
        .map_err(|error| format!("{label}: get_many failed: {error}"))?;
    let actual = entries_to_map(&result.entries_for_requested_keys(&point_keys));
    let expected = point_keys
        .iter()
        .filter_map(|key| model.get(key).map(|value| (key.clone(), value.clone())))
        .collect::<BTreeMap<_, _>>();
    if actual != expected {
        return Err(format!(
            "{label}: get_many mismatch: expected {expected:?}, got {actual:?}"
        ));
    }

    let lower_key = keys[rng.usize(keys.len())].clone();
    let upper_key = keys[rng.usize(keys.len())].clone();
    let (lower, upper) = if lower_key <= upper_key {
        (lower_key, upper_key)
    } else {
        (upper_key, lower_key)
    };
    let range = KeyRange {
        lower: Bound::Included(lower),
        upper: Bound::Included(upper),
    };
    let chunk = scan_range(
        read,
        range.clone(),
        ScanOptions {
            limit_rows: 3,
            ..Default::default()
        },
    )
    .await
    .map_err(|error| format!("{label}: scan_range failed: {error}"))?;
    let actual_scan = chunk_entries(&chunk.entries);
    let expected_scan = model_scan(model, &range, Some(3));
    if actual_scan != expected_scan {
        return Err(format!(
            "{label}: scan_range mismatch: expected {expected_scan:?}, got {actual_scan:?}"
        ));
    }

    Ok(())
}

async fn scan_range<R>(
    read: &R,
    range: KeyRange,
    opts: ScanOptions,
) -> Result<ScanChunk, crate::storage::StorageError>
where
    R: StorageRead,
{
    read.scan(TEST_SPACE, range, opts).await
}

fn model_scan(model: &ReferenceModel, range: &KeyRange, limit: Option<usize>) -> Vec<(Key, Bytes)> {
    model
        .iter()
        .filter(|(key, _)| range_contains(range, key))
        .take(limit.unwrap_or(usize::MAX))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn range_contains(range: &KeyRange, key: &Key) -> bool {
    let lower_matches = match &range.lower {
        Bound::Included(lower) => key >= lower,
        Bound::Excluded(lower) => key > lower,
        Bound::Unbounded => true,
    };
    let upper_matches = match &range.upper {
        Bound::Included(upper) => key <= upper,
        Bound::Excluded(upper) => key < upper,
        Bound::Unbounded => true,
    };
    lower_matches && upper_matches
}

fn chunk_entries(entries: &[ReadEntry]) -> Vec<(Key, Bytes)> {
    entries
        .iter()
        .map(|entry| (entry.key.clone(), projected_value_bytes(&entry.value)))
        .collect()
}

fn entries_to_map(entries: &[ReadEntry]) -> BTreeMap<Key, Bytes> {
    chunk_entries(entries).into_iter().collect()
}

fn projected_value_bytes(value: &ProjectedValue) -> Bytes {
    match value {
        ProjectedValue::FullValue(bytes) => bytes.clone(),
        ProjectedValue::KeyOnly => Bytes::new(),
    }
}

struct TinyRng {
    state: u64,
}

impl TinyRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.state
    }

    #[expect(clippy::cast_possible_truncation)]
    fn usize(&mut self, upper: usize) -> usize {
        (self.next() as usize) % upper
    }

    fn bool(&mut self) -> bool {
        self.next() & 1 == 0
    }
}
