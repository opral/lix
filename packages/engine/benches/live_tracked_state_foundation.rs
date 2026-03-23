use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use lix_engine::{
    live_tracked_state::{
        LiveTrackedEntityKey, LiveTrackedEntityValue, LiveTrackedFieldValue, LiveTrackedMutation,
        LiveTrackedPayloadColumn, LiveTrackedRangeBound, LiveTrackedRangeField,
        LiveTrackedRangeRequest, LiveTrackedRootId, LiveTrackedRow, LiveTrackedState,
    },
    CanonicalPluginKey, CanonicalSchemaKey, CanonicalSchemaVersion, EntityId, FileId, LixError,
};
use std::hint::black_box;
use tempfile::TempDir;
use tokio::runtime::Runtime;

#[path = "support/mod.rs"]
mod support;

use support::sqlite_backend::BenchSqliteBackend;

const ENTITY_COUNT: usize = 10_000;
const SPARSE_BASE_ENTITY_COUNT: usize = 50_000;
const SPARSE_MUTATION_COUNT: usize = 100;
const SCAN_WIDTH: usize = 100;
const SCHEMA_KEY: &str = "bench.schema";
const FILE_ID: &str = "bench-file";
const PLUGIN_KEY: &str = "plugin.bench";

fn bench_live_tracked_state_foundation(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create tokio runtime");
    let mut group = c.benchmark_group("live_tracked_state");
    group.sample_size(10);

    group.throughput(Throughput::Elements(128));
    group.bench_function("codec/leaf_profile_128", |b| {
        let fixture = CodecFixture::new(128).expect("codec fixture");
        let backend = BenchSqliteBackend::in_memory().expect("in-memory backend");
        let state = LiveTrackedState::new(&backend);
        b.iter(|| {
            black_box(
                runtime
                    .block_on(state.profile_leaf_codec(black_box(&fixture.rows)))
                    .expect("codec profile"),
            )
        });
    });

    group.throughput(Throughput::Elements(1));
    group.bench_function("root/store_only", |b| {
        b.iter_batched_ref(
            || RootStoreFixture::new(&runtime),
            |fixture| {
                let commit_id = format!("commit-store-{}", fixture.next_commit_id());
                black_box(fixture.store_root(&runtime, &commit_id));
            },
            BatchSize::SmallInput,
        );
    });

    group.throughput(Throughput::Elements(ENTITY_COUNT as u64));
    group.bench_function("flush/chunk_flush_empty_10000", |b| {
        b.iter_batched_ref(
            || WriteFixture::new_empty(&runtime, ENTITY_COUNT),
            |fixture| {
                black_box(fixture.apply_flush_only(&runtime));
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("mutation/apply_and_store_root_empty_10000", |b| {
        b.iter_batched_ref(
            || WriteFixture::new_empty(&runtime, ENTITY_COUNT),
            |fixture| {
                black_box(fixture.apply_and_store_root(&runtime, "commit-empty"));
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("mutation/apply_and_store_root_rewrite_10000", |b| {
        b.iter_batched_ref(
            || WriteFixture::new_rewrite(&runtime, ENTITY_COUNT),
            |fixture| {
                black_box(fixture.apply_and_store_root(&runtime, "commit-rewrite"));
            },
            BatchSize::SmallInput,
        );
    });

    group.throughput(Throughput::Elements(SPARSE_MUTATION_COUNT as u64));
    group.bench_function(
        "mutation/apply_and_store_root_sparse_rewrite_100_of_50000",
        |b| {
            b.iter_batched_ref(
                || {
                    WriteFixture::new_sparse_rewrite(
                        &runtime,
                        SPARSE_BASE_ENTITY_COUNT,
                        SPARSE_MUTATION_COUNT,
                    )
                },
                |fixture| {
                    black_box(fixture.apply_and_store_root(&runtime, "commit-sparse-rewrite"));
                },
                BatchSize::SmallInput,
            );
        },
    );

    let warm_read_fixture = ReadFixture::new(&runtime, ENTITY_COUNT);
    let warm_state = LiveTrackedState::new(&warm_read_fixture.backend);
    runtime
        .block_on(warm_state.get(&warm_read_fixture.root_id, &warm_read_fixture.hot_key))
        .expect("warmup get");
    group.throughput(Throughput::Elements(1));
    group.bench_function("read/get_warm_exact", |b| {
        b.iter(|| {
            black_box(
                runtime
                    .block_on(warm_state.get(
                        &warm_read_fixture.root_id,
                        black_box(&warm_read_fixture.hot_key),
                    ))
                    .expect("warm get"),
            )
        });
    });

    group.bench_function("read/get_coldish_exact", |b| {
        b.iter_batched_ref(
            || ReadFixture::new(&runtime, ENTITY_COUNT),
            |fixture| {
                let state = LiveTrackedState::new(&fixture.backend);
                black_box(
                    runtime
                        .block_on(state.get(&fixture.root_id, &fixture.hot_key))
                        .expect("cold-ish get"),
                );
            },
            BatchSize::SmallInput,
        );
    });

    let scan_state = LiveTrackedState::new(&warm_read_fixture.backend);
    runtime
        .block_on(scan_state.scan(&warm_read_fixture.root_id, &warm_read_fixture.scan_range))
        .expect("warmup scan");
    group.throughput(Throughput::Elements(SCAN_WIDTH as u64));
    group.bench_function("read/scan_adjacent_100", |b| {
        b.iter(|| {
            black_box(
                runtime
                    .block_on(scan_state.scan(
                        &warm_read_fixture.root_id,
                        black_box(&warm_read_fixture.scan_range),
                    ))
                    .expect("scan"),
            )
        });
    });

    group.finish();
}

struct CodecFixture {
    rows: Vec<LiveTrackedRow>,
}

impl CodecFixture {
    fn new(count: usize) -> Result<Self, LixError> {
        let rows = (0..count)
            .map(|index| {
                Ok(LiveTrackedRow::new(
                    key_for(index)?,
                    value_for(index, 0, index % 17 == 0)?,
                ))
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        Ok(Self { rows })
    }
}

struct RootStoreFixture {
    _tempdir: TempDir,
    backend: BenchSqliteBackend,
    root_id: LiveTrackedRootId,
    next_commit: std::sync::atomic::AtomicUsize,
}

impl RootStoreFixture {
    fn new(runtime: &Runtime) -> Self {
        let tempdir = TempDir::new().expect("tempdir");
        let backend =
            BenchSqliteBackend::file_backed(&tempdir.path().join("bench.sqlite")).expect("backend");
        let state = LiveTrackedState::new(&backend);
        runtime.block_on(state.ensure_schema()).expect("schema");
        let result = runtime
            .block_on(state.apply_mutations(
                None,
                vec![LiveTrackedMutation::put(
                    key_for(0).expect("key"),
                    value_for(0, 0, false).expect("value"),
                )],
            ))
            .expect("seed root");
        Self {
            _tempdir: tempdir,
            backend,
            root_id: result.root_id,
            next_commit: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn next_commit_id(&self) -> usize {
        self.next_commit
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    fn store_root(&self, runtime: &Runtime, commit_id: &str) -> LiveTrackedRootId {
        let state = LiveTrackedState::new(&self.backend);
        runtime
            .block_on(state.store_root(commit_id, &self.root_id))
            .expect("store root");
        self.root_id.clone()
    }
}

struct WriteFixture {
    _tempdir: TempDir,
    backend: BenchSqliteBackend,
    base_root: Option<LiveTrackedRootId>,
    mutations: Vec<LiveTrackedMutation>,
}

impl WriteFixture {
    fn new_empty(runtime: &Runtime, count: usize) -> Self {
        Self::new(runtime, count, false)
    }

    fn new_rewrite(runtime: &Runtime, count: usize) -> Self {
        Self::new(runtime, count, true)
    }

    fn new_sparse_rewrite(runtime: &Runtime, base_count: usize, mutation_count: usize) -> Self {
        let tempdir = TempDir::new().expect("tempdir");
        let backend =
            BenchSqliteBackend::file_backed(&tempdir.path().join("bench.sqlite")).expect("backend");
        let state = LiveTrackedState::new(&backend);
        runtime.block_on(state.ensure_schema()).expect("schema");

        let base_mutations = (0..base_count)
            .map(|index| {
                Ok(LiveTrackedMutation::put(
                    key_for(index)?,
                    value_for(index, 0, index % 257 == 0)?,
                ))
            })
            .collect::<Result<Vec<_>, LixError>>()
            .expect("base mutations");
        let base_root = runtime
            .block_on(state.apply_mutations_and_store_root("commit-base", None, base_mutations))
            .expect("base apply")
            .root_id;

        let step = (base_count / mutation_count.max(1)).max(1);
        let mutations = (0..mutation_count)
            .map(|slot| {
                let index = (slot * step).min(base_count.saturating_sub(1));
                Ok(LiveTrackedMutation::put(
                    key_for(index)?,
                    value_for(index, 1, slot % 7 == 0)?,
                ))
            })
            .collect::<Result<Vec<_>, LixError>>()
            .expect("sparse mutations");

        Self {
            _tempdir: tempdir,
            backend,
            base_root: Some(base_root),
            mutations,
        }
    }

    fn new(runtime: &Runtime, count: usize, with_base: bool) -> Self {
        let tempdir = TempDir::new().expect("tempdir");
        let backend =
            BenchSqliteBackend::file_backed(&tempdir.path().join("bench.sqlite")).expect("backend");
        let state = LiveTrackedState::new(&backend);
        runtime.block_on(state.ensure_schema()).expect("schema");

        let base_root = if with_base {
            let base_mutations = (0..count)
                .map(|index| {
                    Ok(LiveTrackedMutation::put(
                        key_for(index)?,
                        value_for(index, 0, index % 257 == 0)?,
                    ))
                })
                .collect::<Result<Vec<_>, LixError>>()
                .expect("base mutations");
            Some(
                runtime
                    .block_on(state.apply_mutations_and_store_root(
                        "commit-base",
                        None,
                        base_mutations,
                    ))
                    .expect("base apply")
                    .root_id,
            )
        } else {
            None
        };

        let mutations = (0..count)
            .map(|index| {
                Ok(LiveTrackedMutation::put(
                    key_for(index)?,
                    value_for(index, 1, index % 257 == 0)?,
                ))
            })
            .collect::<Result<Vec<_>, LixError>>()
            .expect("mutations");

        Self {
            _tempdir: tempdir,
            backend,
            base_root,
            mutations,
        }
    }

    fn apply_flush_only(&self, runtime: &Runtime) -> LiveTrackedRootId {
        let state = LiveTrackedState::new(&self.backend);
        runtime
            .block_on(state.apply_mutations(self.base_root.as_ref(), self.mutations.clone()))
            .expect("flush apply")
            .root_id
    }

    fn apply_and_store_root(&self, runtime: &Runtime, commit_id: &str) -> LiveTrackedRootId {
        let state = LiveTrackedState::new(&self.backend);
        runtime
            .block_on(state.apply_mutations_and_store_root(
                commit_id,
                self.base_root.as_ref(),
                self.mutations.clone(),
            ))
            .expect("write apply")
            .root_id
    }
}

struct ReadFixture {
    _tempdir: TempDir,
    backend: BenchSqliteBackend,
    root_id: LiveTrackedRootId,
    hot_key: LiveTrackedEntityKey,
    scan_range: LiveTrackedRangeRequest,
}

impl ReadFixture {
    fn new(runtime: &Runtime, count: usize) -> Self {
        let tempdir = TempDir::new().expect("tempdir");
        let backend =
            BenchSqliteBackend::file_backed(&tempdir.path().join("bench.sqlite")).expect("backend");
        let state = LiveTrackedState::new(&backend);
        runtime.block_on(state.ensure_schema()).expect("schema");
        let mutations = (0..count)
            .map(|index| {
                Ok(LiveTrackedMutation::put(
                    key_for(index)?,
                    value_for(index, 0, index % 257 == 0)?,
                ))
            })
            .collect::<Result<Vec<_>, LixError>>()
            .expect("read fixture mutations");
        let root_id = runtime
            .block_on(state.apply_mutations_and_store_root("commit-read", None, mutations))
            .expect("read fixture root")
            .root_id;

        let hot_index = count / 2;
        let scan_start = hot_index.saturating_sub(SCAN_WIDTH / 2);
        let scan_end = (scan_start + SCAN_WIDTH - 1).min(count - 1);
        Self {
            _tempdir: tempdir,
            backend,
            root_id,
            hot_key: key_for(hot_index).expect("hot key"),
            scan_range: LiveTrackedRangeRequest {
                fields: vec![
                    LiveTrackedRangeField::exact(
                        lix_engine::live_tracked_state::LiveTrackedKeyComponent::SchemaKey(
                            CanonicalSchemaKey::try_from(SCHEMA_KEY).expect("schema"),
                        ),
                    ),
                    LiveTrackedRangeField::exact(
                        lix_engine::live_tracked_state::LiveTrackedKeyComponent::FileId(
                            FileId::try_from(FILE_ID).expect("file"),
                        ),
                    ),
                    LiveTrackedRangeField::interval(
                        lix_engine::live_tracked_state::LiveTrackedKeyField::EntityId,
                        Some(LiveTrackedRangeBound::inclusive(
                            lix_engine::live_tracked_state::LiveTrackedKeyComponent::EntityId(
                                EntityId::try_from(format!("entity-{scan_start:05}"))
                                    .expect("range start"),
                            ),
                        )),
                        Some(LiveTrackedRangeBound::inclusive(
                            lix_engine::live_tracked_state::LiveTrackedKeyComponent::EntityId(
                                EntityId::try_from(format!("entity-{scan_end:05}"))
                                    .expect("range end"),
                            ),
                        )),
                    )
                    .expect("range"),
                ],
                contiguous: true,
            },
        }
    }
}

fn key_for(index: usize) -> Result<LiveTrackedEntityKey, LixError> {
    Ok(LiveTrackedEntityKey::new(
        CanonicalSchemaKey::try_from(SCHEMA_KEY)?,
        FileId::try_from(FILE_ID)?,
        EntityId::try_from(format!("entity-{index:05}").as_str())?,
    ))
}

fn value_for(
    index: usize,
    revision: usize,
    include_large_blob: bool,
) -> Result<LiveTrackedEntityValue, LixError> {
    let mut columns = vec![
        LiveTrackedPayloadColumn::new(
            "label",
            LiveTrackedFieldValue::Text(format!("entity-{index:05}-rev-{revision}")),
        )?,
        LiveTrackedPayloadColumn::new(
            "ordinal",
            LiveTrackedFieldValue::Integer(((revision * ENTITY_COUNT) + index) as i64),
        )?,
    ];
    if include_large_blob {
        columns.push(LiveTrackedPayloadColumn::new(
            "large_payload",
            LiveTrackedFieldValue::Blob(vec![u8::try_from(index % 251).expect("byte"); 4096]),
        )?);
    }
    LiveTrackedEntityValue::new(
        format!("change-{revision}-{index}"),
        CanonicalSchemaVersion::try_from("1")?,
        CanonicalPluginKey::try_from(PLUGIN_KEY)?,
        Some(format!("{{\"rev\":{revision},\"index\":{index}}}")),
        columns,
    )
}

criterion_group!(benches, bench_live_tracked_state_foundation);
criterion_main!(benches);
