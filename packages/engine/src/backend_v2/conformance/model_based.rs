use std::collections::BTreeMap;
use std::ops::Bound;

use bytes::Bytes;

use crate::backend_v2::conformance::{
    fixtures::{full_put, key, put_batch},
    model::ReferenceModel,
    open_backend, BackendFactory, ConformanceReport, ConformanceResult,
};
use crate::backend_v2::{
    get_many as backend_get_many, visit_range as backend_visit_range, Backend, BackendRead,
    BackendWrite, GetOptions, Key, KeyRange, KeyRef, ProjectedValue, ProjectedValueRef, ReadBatch,
    ReadEntry, ReadOptions, ScanChunk, ScanOptions, WriteOptions,
};

pub(crate) fn register<F>(report: &mut ConformanceReport, factory: &F)
where
    F: BackendFactory,
{
    report.run(
        "model::deterministic_history_matches_reference_model",
        || deterministic_history_matches_reference_model(factory),
    );
}

fn deterministic_history_matches_reference_model<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let mut model = ReferenceModel::default();
    let mut rng = TinyRng::new(0x51cedeed);
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
        let old_read = backend
            .begin_read(ReadOptions::default())
            .map_err(|error| format!("step {step}: begin old read failed: {error}"))?;
        let old_model = model.clone();
        let mut write = backend
            .begin_write(WriteOptions::default())
            .map_err(|error| format!("step {step}: begin write failed: {error}"))?;
        let mut staged = model.clone();

        let mutation_count = 1 + rng.usize(4);
        for mutation_index in 0..mutation_count {
            let target_key = keys[rng.usize(keys.len())].clone();
            if rng.bool() {
                let value = Bytes::from(format!("v{step}-{mutation_index}"));
                write
                    .put_many(put_batch([full_put(target_key.clone(), value.clone())]))
                    .map_err(|error| format!("step {step}: put_many failed: {error}"))?;
                staged.put(target_key, value);
            } else {
                write
                    .delete_many(&[target_key.clone()])
                    .map_err(|error| format!("step {step}: delete_many failed: {error}"))?;
                staged.delete(&target_key);
            }
        }

        if rng.bool() {
            write
                .commit()
                .map_err(|error| format!("step {step}: commit failed: {error}"))?;
            model = staged;
        } else {
            write
                .rollback()
                .map_err(|error| format!("step {step}: rollback failed: {error}"))?;
        }

        compare_read_to_model(
            &old_read,
            &old_model,
            &keys,
            &mut rng,
            &format!("step {step} old snapshot"),
        )?;

        let new_read = backend
            .begin_read(ReadOptions::default())
            .map_err(|error| format!("step {step}: begin new read failed: {error}"))?;
        compare_read_to_model(
            &new_read,
            &model,
            &keys,
            &mut rng,
            &format!("step {step} new snapshot"),
        )?;
    }

    Ok(())
}

fn compare_read_to_model<R>(
    read: &R,
    model: &ReferenceModel,
    keys: &[Key],
    rng: &mut TinyRng,
    label: &str,
) -> ConformanceResult
where
    R: BackendRead,
{
    let point_keys = [
        keys[rng.usize(keys.len())].clone(),
        keys[rng.usize(keys.len())].clone(),
        key("missing"),
        keys[rng.usize(keys.len())].clone(),
    ];
    let result = backend_get_many(read, &point_keys, GetOptions::default())
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
    .map_err(|error| format!("{label}: scan_range failed: {error}"))?;
    let actual_scan = chunk_entries(&chunk.entries.entries);
    let expected_scan = model_scan(model, &range, Some(3));
    if actual_scan != expected_scan {
        return Err(format!(
            "{label}: scan_range mismatch: expected {expected_scan:?}, got {actual_scan:?}"
        ));
    }

    Ok(())
}

fn scan_range<R>(
    read: &R,
    range: KeyRange,
    opts: ScanOptions<'_>,
) -> Result<ScanChunk, crate::backend_v2::BackendError>
where
    R: BackendRead,
{
    let mut entries = Vec::with_capacity(opts.limit_rows);
    let result = backend_visit_range(
        read,
        range,
        opts,
        &mut |key: KeyRef<'_>, value: ProjectedValueRef<'_>| {
            entries.push(ReadEntry {
                key: key.to_owned_key(),
                value: value.to_owned(),
            });
            Ok(())
        },
    )?;
    Ok(ScanChunk {
        entries: ReadBatch { entries },
        has_more: result.has_more,
    })
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

fn chunk_entries(entries: &[crate::backend_v2::ReadEntry]) -> Vec<(Key, Bytes)> {
    entries
        .iter()
        .map(|entry| (entry.key.clone(), projected_value_bytes(&entry.value)))
        .collect()
}

fn entries_to_map(entries: &[crate::backend_v2::ReadEntry]) -> BTreeMap<Key, Bytes> {
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
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }

    fn usize(&mut self, upper: usize) -> usize {
        (self.next() as usize) % upper
    }

    fn bool(&mut self) -> bool {
        self.next() & 1 == 0
    }
}
