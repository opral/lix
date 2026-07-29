use std::collections::BTreeMap;
use std::ops::Bound;

use bytes::Bytes;

use crate::storage::conformance::{
    ConformanceReport, ConformanceResult, StorageFactory,
    fixtures::{full_put, key, put_batch},
    model::ReferenceModel,
    open_storage,
};
use crate::storage::{
    CoreProjection, GetManyRequest, GetOptions, Key, KeyRange, ProjectedValue, ReadEntry,
    ReadOptions, ScanOptions, SpaceId, Storage, StorageRead, StorageWrite, WriteOptions,
};

const TEST_SPACES: [SpaceId; 4] = [
    SpaceId(0),
    SpaceId(7),
    SpaceId(0x0102_0304),
    SpaceId(u32::MAX),
];
const SEQUENTIAL_MODEL_SEEDS: u64 = 64;
const BOUNDARY_MODEL_SEEDS: [u64; 4] = [0x51ce_deed, 0xdead_beef_cafe_babe, u64::MAX - 1, u64::MAX];
const STEPS_PER_SEED: usize = 64;

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
    let keys = vec![
        Key(Bytes::new()),
        Key(Bytes::from_static(b"\0")),
        Key(Bytes::from_static(b"\0\0")),
        key("a"),
        key("aa"),
        key("ab"),
        key("b"),
        Key(Bytes::from_static(b"\x7f")),
        Key(Bytes::from_static(b"\xff")),
        Key(Bytes::from_static(b"\xff\0")),
    ];

    for seed in (0..SEQUENTIAL_MODEL_SEEDS).chain(BOUNDARY_MODEL_SEEDS) {
        run_seed(factory, seed, &keys).await?;
    }

    Ok(())
}

async fn run_seed<F>(factory: &F, seed: u64, keys: &[Key]) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let mut models = TEST_SPACES
        .into_iter()
        .map(|space| (space, ReferenceModel::default()))
        .collect::<BTreeMap<_, _>>();
    let mut rng = TinyRng::new(seed);

    for step in 0..STEPS_PER_SEED {
        let label = format!("seed {seed:#018x}, step {step}");
        let old_read = storage
            .begin_read(ReadOptions::default())
            .await
            .map_err(|error| format!("{label}: begin old read failed: {error}"))?;
        let old_models = models.clone();
        let mut write = storage
            .begin_write(WriteOptions::default())
            .await
            .map_err(|error| format!("{label}: begin write failed: {error}"))?;
        let mut staged = models.clone();

        let mutation_count = 1 + rng.usize(8);
        for mutation_index in 0..mutation_count {
            let space = TEST_SPACES[rng.usize(TEST_SPACES.len())];
            let target_key = keys[rng.usize(keys.len())].clone();
            match rng.usize(4) {
                0 | 1 => {
                    let value = random_value(&mut rng, step, mutation_index);
                    write
                        .put_many(
                            space,
                            put_batch([full_put(target_key.clone(), value.clone())]),
                        )
                        .await
                        .map_err(|error| {
                            format!("{label}, mutation {mutation_index}: put_many failed: {error}")
                        })?;
                    staged
                        .get_mut(&space)
                        .expect("test space has a model")
                        .put(target_key, value);
                }
                2 => {
                    write
                        .delete_many(space, std::slice::from_ref(&target_key))
                        .await
                        .map_err(|error| {
                            format!(
                                "{label}, mutation {mutation_index}: delete_many failed: {error}"
                            )
                        })?;
                    staged
                        .get_mut(&space)
                        .expect("test space has a model")
                        .delete(&target_key);
                }
                _ => {
                    let range = random_range(&mut rng, keys);
                    write
                        .delete_range(space, range.clone())
                        .await
                        .map_err(|error| {
                            format!(
                                "{label}, mutation {mutation_index}: delete_range failed: {error}"
                            )
                        })?;
                    staged
                        .get_mut(&space)
                        .expect("test space has a model")
                        .delete_range(&range);
                }
            }
        }

        if rng.usize(3) != 0 {
            write
                .commit()
                .await
                .map_err(|error| format!("{label}: commit failed: {error}"))?;
            models = staged;
        } else {
            write
                .rollback()
                .await
                .map_err(|error| format!("{label}: rollback failed: {error}"))?;
        }

        compare_read_to_model(
            &old_read,
            &old_models,
            &keys,
            &mut rng,
            &format!("{label}, old snapshot"),
        )
        .await?;

        let new_read = storage
            .begin_read(ReadOptions::default())
            .await
            .map_err(|error| format!("{label}: begin new read failed: {error}"))?;
        compare_read_to_model(
            &new_read,
            &models,
            &keys,
            &mut rng,
            &format!("{label}, new snapshot"),
        )
        .await?;
    }

    Ok(())
}

async fn compare_read_to_model<R>(
    read: &R,
    models: &BTreeMap<SpaceId, ReferenceModel>,
    keys: &[Key],
    rng: &mut TinyRng,
    label: &str,
) -> ConformanceResult
where
    R: StorageRead,
{
    for (space, model) in models {
        let chunk = read
            .scan(
                *space,
                KeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                ScanOptions {
                    limit_rows: usize::MAX,
                    ..ScanOptions::default()
                },
            )
            .await
            .map_err(|error| format!("{label}: full scan in {space:?} failed: {error}"))?;
        let actual = chunk_entries(&chunk.entries);
        let expected = model
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        if actual != expected || chunk.has_more {
            return Err(format!(
                "{label}: full scan in {space:?} mismatch: expected {expected:?} with \
                 has_more=false, got {actual:?} with has_more={}",
                chunk.has_more
            ));
        }
    }

    let point_space = TEST_SPACES[rng.usize(TEST_SPACES.len())];
    let point_keys = vec![
        keys[rng.usize(keys.len())].clone(),
        keys[rng.usize(keys.len())].clone(),
        Key(Bytes::from_static(b"missing")),
        keys[rng.usize(keys.len())].clone(),
        keys[rng.usize(keys.len())].clone(),
    ];
    let point_projection = random_projection(rng);
    let result = read
        .get_many(&[GetManyRequest {
            space: point_space,
            keys: &point_keys,
            opts: GetOptions {
                projection: point_projection,
            },
        }])
        .await
        .map_err(|error| format!("{label}: get_many failed: {error}"))?;
    let point_model = models.get(&point_space).expect("test space has a model");
    let expected = point_keys
        .iter()
        .map(|key| {
            point_model
                .get(key)
                .map(|value| project_value(value, point_projection))
        })
        .collect::<Vec<_>>();
    if result.values != expected {
        return Err(format!(
            "{label}: get_many in {point_space:?} mismatch: expected {expected:?}, got {:?}",
            result.values
        ));
    }

    let scan_space = TEST_SPACES[rng.usize(TEST_SPACES.len())];
    let scan_model = models.get(&scan_space).expect("test space has a model");
    let range = random_range(rng, keys);
    let resume_after = if rng.usize(3) == 0 {
        None
    } else {
        Some(keys[rng.usize(keys.len())].clone())
    };
    let limit_rows = rng.usize(keys.len() + 2);
    let projection = random_projection(rng);
    let chunk = read
        .scan(
            scan_space,
            range.clone(),
            ScanOptions {
                projection,
                limit_rows,
                resume_after: resume_after.clone(),
            },
        )
        .await
        .map_err(|error| format!("{label}: randomized scan failed: {error}"))?;
    let eligible = scan_model
        .iter()
        .filter(|(key, _)| {
            range_contains(&range, key)
                && resume_after
                    .as_ref()
                    .is_none_or(|resume_after| *key > resume_after)
        })
        .map(|(key, value)| ReadEntry {
            key: key.clone(),
            value: project_value(value, projection),
        })
        .collect::<Vec<_>>();
    let expected_has_more = limit_rows != 0 && eligible.len() > limit_rows;
    let expected_scan = eligible.into_iter().take(limit_rows).collect::<Vec<_>>();
    if chunk.entries != expected_scan || chunk.has_more != expected_has_more {
        return Err(format!(
            "{label}: randomized scan in {scan_space:?} mismatch for range {range:?}, \
             resume_after {resume_after:?}, limit {limit_rows}, projection {projection:?}: \
             expected {expected_scan:?} with has_more={expected_has_more}, got {:?} with \
             has_more={}",
            chunk.entries, chunk.has_more
        ));
    }

    Ok(())
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

fn random_projection(rng: &mut TinyRng) -> CoreProjection {
    if rng.bool() {
        CoreProjection::FullValue
    } else {
        CoreProjection::KeyOnly
    }
}

fn project_value(value: &Bytes, projection: CoreProjection) -> ProjectedValue {
    match projection {
        CoreProjection::FullValue => ProjectedValue::FullValue(value.clone()),
        CoreProjection::KeyOnly => ProjectedValue::KeyOnly,
    }
}

fn random_range(rng: &mut TinyRng, keys: &[Key]) -> KeyRange {
    KeyRange {
        lower: random_bound(rng, keys),
        upper: random_bound(rng, keys),
    }
}

fn random_bound(rng: &mut TinyRng, keys: &[Key]) -> Bound<Key> {
    let key = keys[rng.usize(keys.len())].clone();
    match rng.usize(4) {
        0 => Bound::Unbounded,
        1 => Bound::Included(key),
        2 | 3 => Bound::Excluded(key),
        _ => unreachable!("bounded random choice"),
    }
}

fn random_value(rng: &mut TinyRng, step: usize, mutation_index: usize) -> Bytes {
    match rng.usize(8) {
        0 => return Bytes::new(),
        1 => return Bytes::from_static(b"\0"),
        2 => return Bytes::from_static(b"\xff"),
        _ => {}
    }
    let len = rng.usize(33);
    let mut value = Vec::with_capacity(len + 2 * size_of::<usize>());
    value.extend_from_slice(&step.to_be_bytes());
    value.extend_from_slice(&mutation_index.to_be_bytes());
    for _ in 0..len {
        value.push(rng.next() as u8);
    }
    Bytes::from(value)
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
        // The low bits of an LCG have short, predictable cycles. Draw from the
        // upper half so consecutive power-of-two choices (notably range-bound
        // kinds) remain independently varied.
        ((self.next() >> 32) as usize) % upper
    }

    fn bool(&mut self) -> bool {
        self.next() & 1 == 0
    }
}
