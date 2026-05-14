use std::ops::Bound;

use bytes::Bytes;

use crate::backend_v2::conformance::{
    fixtures::{full_put, key, put_batch, space},
    model::ReferenceModel,
    BackendFactory, ConformanceReport, ConformanceResult,
};
use crate::backend_v2::{
    Backend, BackendRead, BackendWrite, GetOptions, Key, KeyRange, Prefix, ProjectedValue,
    ReadOptions, ScanOptions, SpaceId, WriteOptions,
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
    let backend = factory.fresh();
    let mut model = ReferenceModel::default();
    let mut rng = TinyRng::new(0x51cedeed);
    let spaces = [space(1), space(2)];
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
            let target_space = spaces[rng.usize(spaces.len())];
            let target_key = keys[rng.usize(keys.len())].clone();
            if rng.bool() {
                let value = Bytes::from(format!("v{step}-{mutation_index}"));
                write
                    .put_many(
                        target_space,
                        put_batch([full_put(target_key.clone(), value.clone())]),
                    )
                    .map_err(|error| format!("step {step}: put_many failed: {error}"))?;
                staged.put(target_space, target_key, value);
            } else {
                write
                    .delete_many(target_space, &[target_key.clone()])
                    .map_err(|error| format!("step {step}: delete_many failed: {error}"))?;
                staged.delete(target_space, &target_key);
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
            spaces,
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
            spaces,
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
    spaces: [SpaceId; 2],
    keys: &[Key],
    rng: &mut TinyRng,
    label: &str,
) -> ConformanceResult
where
    R: BackendRead,
{
    let target_space = spaces[rng.usize(spaces.len())];
    let point_keys = [
        keys[rng.usize(keys.len())].clone(),
        keys[rng.usize(keys.len())].clone(),
        key("missing"),
        keys[rng.usize(keys.len())].clone(),
    ];
    let result = read
        .get_many(target_space, &point_keys, GetOptions::default())
        .map_err(|error| format!("{label}: get_many failed: {error}"))?;
    let actual = result
        .entries
        .iter()
        .map(|slot| (slot.requested_index, slot.key.clone(), slot_value(slot)))
        .collect::<Vec<_>>();
    let expected = point_keys
        .iter()
        .enumerate()
        .map(|(index, key)| {
            (
                Some(index),
                key.clone(),
                model.get(target_space, key).cloned(),
            )
        })
        .collect::<Vec<_>>();
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
    let page = read
        .scan_range(
            target_space,
            range.clone(),
            ScanOptions {
                limit_rows: Some(3),
                ..Default::default()
            },
        )
        .map_err(|error| format!("{label}: scan_range failed: {error}"))?;
    let actual_scan = page_entries(&page.entries.entries);
    let expected_scan = model_scan(model, target_space, &range, Some(3));
    if actual_scan != expected_scan {
        return Err(format!(
            "{label}: scan_range mismatch: expected {expected_scan:?}, got {actual_scan:?}"
        ));
    }

    let prefix = Prefix {
        bytes: if rng.bool() {
            Bytes::from_static(b"a")
        } else {
            Bytes::new()
        },
    };
    let page = read
        .scan_prefix(
            target_space,
            prefix.clone(),
            ScanOptions {
                limit_rows: Some(4),
                ..Default::default()
            },
        )
        .map_err(|error| format!("{label}: scan_prefix failed: {error}"))?;
    let prefix_range = prefix
        .to_range()
        .map_err(|error| format!("{label}: prefix range conversion failed: {error}"))?;
    let actual_prefix = page_entries(&page.entries.entries);
    let expected_prefix = model_scan(model, target_space, &prefix_range, Some(4));
    if actual_prefix != expected_prefix {
        return Err(format!(
            "{label}: scan_prefix mismatch: expected {expected_prefix:?}, got {actual_prefix:?}"
        ));
    }

    Ok(())
}

fn model_scan(
    model: &ReferenceModel,
    target_space: SpaceId,
    range: &KeyRange,
    limit: Option<usize>,
) -> Vec<(Key, Bytes)> {
    model
        .iter_space(target_space)
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

fn page_entries(entries: &[crate::backend_v2::ReadEntry]) -> Vec<(Key, Bytes)> {
    entries
        .iter()
        .map(|entry| (entry.key.clone(), projected_value_bytes(&entry.value)))
        .collect()
}

fn slot_value(slot: &crate::backend_v2::GetSlot) -> Option<Bytes> {
    slot.value.as_ref().map(projected_value_bytes)
}

fn projected_value_bytes(value: &ProjectedValue) -> Bytes {
    match value {
        ProjectedValue::FullValue(bytes)
        | ProjectedValue::Header(bytes)
        | ProjectedValue::Refs(bytes)
        | ProjectedValue::Payload(bytes) => bytes.clone(),
        ProjectedValue::HeaderAndRefs { header, refs } => {
            let mut bytes = Vec::with_capacity(header.len() + refs.len());
            bytes.extend_from_slice(header);
            bytes.extend_from_slice(refs);
            Bytes::from(bytes)
        }
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
