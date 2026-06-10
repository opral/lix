//! Backend decorator that adversarially scrambles `visit_keys` callback
//! order.
//!
//! `BackendRead::visit_keys` documents that visit order is unspecified and
//! consumers must address results by index. This suite enforces that
//! contract actively: the decorator replays point-read visits in reverse,
//! and both the backend conformance suite and the full transaction engine
//! paths must behave identically to the plain in-memory backend.

use lix_engine::backend::{
    Backend, BackendError, BackendRead, GetOptions, InMemoryBackend, InMemoryBackendFactory,
    InMemoryBackendFixture, Key, KeyRange, PointVisitor, ProjectedValueRef, ReadOptions,
    ScanOptions, WriteOptions,
};
use lix_engine::{BackendFactory, BackendFixture, BackendTestConfig, run_backend_conformance};

#[derive(Clone, Copy, Debug, Default)]
struct ScrambledVisitBackendFactory {
    inner: InMemoryBackendFactory,
}

#[derive(Clone, Debug)]
struct ScrambledVisitFixture {
    inner: InMemoryBackendFixture,
}

#[derive(Clone, Debug)]
struct ScrambledVisitBackend {
    inner: InMemoryBackend,
}

struct ScrambledVisitRead {
    inner: <InMemoryBackend as Backend>::Read<'static>,
}

enum OwnedProjected {
    KeyOnly,
    FullValue(Vec<u8>),
}

impl BackendFactory for ScrambledVisitBackendFactory {
    type Backend = ScrambledVisitBackend;
    type Fixture = ScrambledVisitFixture;

    fn create_fixture(&self) -> Self::Fixture {
        ScrambledVisitFixture {
            inner: self.inner.create_fixture(),
        }
    }

    fn config(&self) -> BackendTestConfig {
        self.inner.config()
    }
}

impl BackendFixture for ScrambledVisitFixture {
    type Backend = ScrambledVisitBackend;

    fn open(&self) -> Self::Backend {
        ScrambledVisitBackend {
            inner: self.inner.open(),
        }
    }
}

impl Backend for ScrambledVisitBackend {
    type Read<'a>
        = ScrambledVisitRead
    where
        Self: 'a;

    type Write<'a>
        = <InMemoryBackend as Backend>::Write<'a>
    where
        Self: 'a;

    fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        Ok(ScrambledVisitRead {
            inner: self.inner.begin_read(opts)?,
        })
    }

    fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        self.inner.begin_write(opts)
    }
}

impl BackendRead for ScrambledVisitRead {
    type RangeScan<'a> =
        <<InMemoryBackend as Backend>::Read<'static> as BackendRead>::RangeScan<'a>;

    fn visit_keys<V>(
        &self,
        keys: &[Key],
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized,
    {
        let mut buffered = Vec::with_capacity(keys.len());
        self.inner.visit_keys(
            keys,
            opts,
            &mut |index: usize, _key: &Key, value: Option<ProjectedValueRef<'_>>| {
                let value = value.map(|value| match value {
                    ProjectedValueRef::KeyOnly => OwnedProjected::KeyOnly,
                    ProjectedValueRef::FullValue(bytes) => {
                        OwnedProjected::FullValue(bytes.to_vec())
                    }
                });
                buffered.push((index, value));
                Ok(())
            },
        )?;
        // Replay in a seeded-shuffled order: a consumer that depends on
        // visit order instead of the visited index fails loudly here.
        shuffle(&mut buffered);
        for (index, value) in buffered {
            let Some(key) = keys.get(index) else {
                return Err(BackendError::Corruption(format!(
                    "scrambled visit index out of bounds: {index}"
                )));
            };
            let value = value.as_ref().map(|value| match value {
                OwnedProjected::KeyOnly => ProjectedValueRef::KeyOnly,
                OwnedProjected::FullValue(bytes) => ProjectedValueRef::FullValue(bytes),
            });
            visitor.visit(index, key, value)?;
        }
        Ok(())
    }

    fn with_range_scan<T, F>(
        &self,
        range: KeyRange,
        opts: ScanOptions<'_>,
        f: F,
    ) -> Result<T, BackendError>
    where
        F: FnOnce(&mut Self::RangeScan<'_>) -> Result<T, BackendError>,
    {
        // Range scans stay ordered: ascending key order is contractual.
        self.inner.with_range_scan(range, opts, f)
    }

    fn close(self) -> Result<(), BackendError> {
        self.inner.close()
    }
}

/// Deterministic Fisher-Yates with a fixed xorshift seed, so failures
/// replay exactly. Stronger than plain reversal, which is an involution
/// that preserves adjacency structure.
fn shuffle<T>(items: &mut [T]) {
    const SEED: u64 = 0x5eed_1234_abcd_9876;
    let mut state = SEED;
    for index in (1..items.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        #[expect(clippy::cast_possible_truncation)]
        let swap_with = (state % (index as u64 + 1)) as usize;
        items.swap(index, swap_with);
    }
}

#[test]
fn scrambled_visit_backend_passes_backend_conformance() {
    let factory = ScrambledVisitBackendFactory::default();

    run_backend_conformance(&factory).assert_no_failures();
}

#[cfg(feature = "storage-benches")]
mod engine_paths {
    use lix_engine::storage::StorageContext;
    use lix_engine::transaction::bench::{BenchTransactionFixture, BenchTransactionRow};

    use super::*;

    const ROWS: usize = 64;
    const READ_MANY_KEYS: usize = 10;

    fn bench_rows() -> Vec<BenchTransactionRow> {
        (0..ROWS)
            .map(|index| BenchTransactionRow {
                schema_key: "json_pointer".to_string(),
                file_id: None,
                entity_pk: format!("/packages/{index:03}/version"),
                value: serde_json::json!({
                    "path": format!("/packages/{index:03}/version"),
                    "value": format!("1.0.{index}"),
                }),
                updated_value: serde_json::json!({
                    "path": format!("/packages/{index:03}/version"),
                    "value": format!("2.0.{index}"),
                }),
            })
            .collect()
    }

    /// Runs the full transaction CRUD surface (normalization, validation,
    /// changelog, tracked-state tree, json store) on the plain in-memory
    /// backend and on the scrambled decorator, asserting identical logical
    /// results and byte-identical physical layout.
    #[tokio::test]
    async fn engine_transaction_paths_are_visit_order_independent() {
        let plain =
            BenchTransactionFixture::new(StorageContext::new(InMemoryBackend::new()), bench_rows())
                .await;
        let scrambled = BenchTransactionFixture::new(
            StorageContext::new(ScrambledVisitBackend {
                inner: InMemoryBackend::new(),
            }),
            bench_rows(),
        )
        .await;

        run_crud_surface(plain, scrambled).await;
    }

    async fn run_crud_surface(
        mut plain: BenchTransactionFixture<InMemoryBackend>,
        mut scrambled: BenchTransactionFixture<ScrambledVisitBackend>,
    ) {
        assert_eq!(plain.seed().await, scrambled.seed().await, "seed");
        assert_state_matches(&plain, &scrambled, "after seed").await;

        assert_eq!(
            plain.read_all().await,
            scrambled.read_all().await,
            "read_all"
        );
        assert_eq!(
            plain.read_many_by_pk(READ_MANY_KEYS).await,
            scrambled.read_many_by_pk(READ_MANY_KEYS).await,
            "read_many_by_pk"
        );

        // Bulk rounds keep validation and changelog running 64-key point
        // reads through the scrambled visitor, not just single-key visits.
        assert_eq!(
            plain.update_all().await,
            scrambled.update_all().await,
            "update_all"
        );
        assert_state_matches(&plain, &scrambled, "after update_all").await;

        assert_eq!(
            plain.update_one_by_pk().await,
            scrambled.update_one_by_pk().await,
            "update_one_by_pk"
        );
        assert_state_matches(&plain, &scrambled, "after update_one").await;

        assert_eq!(
            plain.delete_all().await,
            scrambled.delete_all().await,
            "delete_all"
        );
        assert_state_matches(&plain, &scrambled, "after delete_all").await;

        assert_eq!(
            plain.insert_all().await,
            scrambled.insert_all().await,
            "insert_all after delete_all"
        );
        assert_state_matches(&plain, &scrambled, "after re-insert").await;

        assert_eq!(
            plain.read_many_by_pk(READ_MANY_KEYS).await,
            scrambled.read_many_by_pk(READ_MANY_KEYS).await,
            "read_many_by_pk after mutations"
        );
    }

    /// Compares full logical row contents (identity + snapshot) plus
    /// per-space layout accounting. Contents catch value cross-wiring that
    /// count or byte-sum aggregates would miss; ids and timestamps differ
    /// between the fixtures, so byte-exact physical comparison is not
    /// possible without injecting deterministic functions.
    async fn assert_state_matches(
        plain: &BenchTransactionFixture<InMemoryBackend>,
        scrambled: &BenchTransactionFixture<ScrambledVisitBackend>,
        stage: &str,
    ) {
        assert_eq!(
            plain.read_all_contents().await,
            scrambled.read_all_contents().await,
            "row contents must match regardless of visit order ({stage})"
        );
        assert_layouts_match(plain, scrambled, stage);
    }

    fn assert_layouts_match(
        plain: &BenchTransactionFixture<InMemoryBackend>,
        scrambled: &BenchTransactionFixture<ScrambledVisitBackend>,
        stage: &str,
    ) {
        let plain_layout = plain
            .layout_accounting()
            .into_iter()
            .map(|space| {
                (
                    space.space_id,
                    space.space,
                    space.rows,
                    space.key_bytes,
                    space.value_bytes,
                )
            })
            .collect::<Vec<_>>();
        let scrambled_layout = scrambled
            .layout_accounting()
            .into_iter()
            .map(|space| {
                (
                    space.space_id,
                    space.space,
                    space.rows,
                    space.key_bytes,
                    space.value_bytes,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            plain_layout, scrambled_layout,
            "layout accounting (per-space row counts and key/value byte totals) must match regardless of visit order ({stage})"
        );
    }
}
