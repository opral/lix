use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use lix_sdk::{
    FsWriteOptions, InMemoryBackend, LixError, OpenLixOptions, SqliteBackend, Value,
    WasmPluginDetectedChange, WasmPluginEntityState, WasmPluginFile, open_lix,
};
use plugin_csv::exports::lix::plugin::api::EntityState as CsvEntityState;
use plugin_csv::exports::lix::plugin::api::Guest as _;
use plugin_csv::{CsvPlugin, File as CsvFile};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fmt::Write as _;
use std::hint::black_box;
use std::io::{Cursor, Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tokio::runtime::Builder;
use wasmtime::component::{Component, Linker};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

mod plugin_bindings {
    wasmtime::component::bindgen!({
        path: "../engine/wit",
        world: "plugin",
    });
}

type BindingSnapshotContent = HashMap<String, plugin_bindings::exports::lix::plugin::api::Scalar>;

const INITIAL_ROW_COUNT: usize = 10_000;
const NEW_ROW_COUNT: usize = 10_000;
const CSV_PATH: &str = "/large-merge.csv";
const CSV_PLUGIN_WARMUP_PATH: &str = "/.csv-plugin-warmup.csv";

fn bench_e2e(c: &mut Criterion) {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to create Tokio runtime for e2e bench");
    let plugin = build_csv_plugin();
    let plugin_wasm = csv_plugin_wasm_from_archive(&plugin);
    let initial_rows = random_csv_rows("initial", INITIAL_ROW_COUNT, 0x8ae7_b4b1_9f4c_d215);
    let new_rows = random_csv_rows("new", NEW_ROW_COUNT, 0xf3bb_91d4_6a8c_2e73);
    let initial_csv = csv_bytes_from_rows(&initial_rows);
    let updated_csv = csv_bytes_from_rows(&randomly_merge_csv_rows(
        &initial_rows,
        &new_rows,
        0x6449_2c6f_179d_31b5,
    ));
    let initial_wasm_changes = runtime.block_on(async {
        wasm_plugin_file_changes(
            wasm_plugin_diff_fixture(&plugin_wasm, Vec::new(), &initial_csv).await,
        )
        .await
    });
    assert_detected_csv_changes(&initial_wasm_changes, INITIAL_ROW_COUNT, 1);
    let initial_wasm_state = plugin_entity_state_from_file_changes(&initial_wasm_changes);
    let merge_wasm_changes = runtime.block_on(async {
        wasm_plugin_file_changes(
            wasm_plugin_diff_fixture(&plugin_wasm, initial_wasm_state.clone(), &updated_csv).await,
        )
        .await
    });
    assert_detected_csv_changes(&merge_wasm_changes, NEW_ROW_COUNT, 0);

    let initial_native_changes =
        native_csv_file_changes(NativeCsvDiffFixture::new(Vec::new(), initial_csv.clone()));
    assert_file_changes_equivalent(&initial_wasm_changes, &initial_native_changes);
    let initial_native_state = csv_entity_state_from_file_changes(&initial_native_changes);
    let merge_native_changes = native_csv_file_changes(NativeCsvDiffFixture::new(
        initial_native_state.clone(),
        updated_csv.clone(),
    ));
    assert_file_changes_equivalent(&merge_wasm_changes, &merge_native_changes);

    runtime.block_on(async {
        validate_overwrite_large_csv(
            large_csv_fixture(open_lix_with_plugin(&plugin).await, &initial_csv).await,
            &updated_csv,
        )
        .await;
        validate_manual_bulk_insert_schema_changes(
            csv_schema_changes_insert_fixture(
                open_lix_with_plugin(&plugin).await,
                &[],
                &initial_wasm_changes,
                INITIAL_ROW_COUNT,
            )
            .await,
        )
        .await;
        validate_manual_bulk_insert_file_changes(
            csv_changes_insert_fixture(
                open_lix_with_plugin(&plugin).await,
                &[],
                &initial_wasm_changes,
                INITIAL_ROW_COUNT,
            )
            .await,
        )
        .await;
        validate_manual_bulk_insert_schema_changes(
            csv_schema_changes_insert_fixture(
                open_lix_with_plugin(&plugin).await,
                &initial_wasm_changes,
                &merge_wasm_changes,
                INITIAL_ROW_COUNT + NEW_ROW_COUNT,
            )
            .await,
        )
        .await;
        validate_manual_bulk_insert_file_changes(
            csv_changes_insert_fixture(
                open_lix_with_plugin(&plugin).await,
                &initial_wasm_changes,
                &merge_wasm_changes,
                INITIAL_ROW_COUNT + NEW_ROW_COUNT,
            )
            .await,
        )
        .await;
    });

    let mut group = c.benchmark_group("e2e/csv_plugin");
    group.sample_size(10);
    group.throughput(Throughput::Elements(NEW_ROW_COUNT as u64));
    group.bench_function("setup", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let lix = open_lix_with_warmed_csv_plugin(&plugin).await;
                black_box(lix).close().await.unwrap();
            });
        });
    });
    group.bench_function("setup_inmemory", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let lix = open_lix_with_warmed_csv_plugin_inmemory(&plugin).await;
                black_box(lix).close().await.unwrap();
            });
        });
    });
    group.bench_function("setup_wasm", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let fixture =
                    wasm_plugin_diff_fixture(&plugin_wasm, Vec::new(), &initial_csv).await;
                black_box(fixture);
            });
        });
    });
    group.bench_function("insert_10k", |b| {
        b.iter_batched(
            || runtime.block_on(open_lix_with_warmed_csv_plugin(&plugin)),
            |lix| runtime.block_on(insert_large_csv(lix, &initial_csv)),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("insert_10k_inmemory", |b| {
        b.iter_batched(
            || runtime.block_on(open_lix_with_warmed_csv_plugin_inmemory(&plugin)),
            |lix| runtime.block_on(insert_large_csv(lix, &initial_csv)),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("insert_10k_plugin", |b| {
        b.iter_batched(
            || {
                runtime.block_on(wasm_plugin_diff_fixture(
                    &plugin_wasm,
                    Vec::new(),
                    &initial_csv,
                ))
            },
            |fixture| runtime.block_on(bench_wasm_plugin_diff(fixture)),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("insert_10k_plugin_diff", |b| {
        b.iter_batched(
            || NativeCsvDiffFixture::new(Vec::new(), initial_csv.clone()),
            bench_native_csv_diff,
            BatchSize::SmallInput,
        );
    });
    group.bench_function("insert_10k_insert", |b| {
        b.iter_batched(
            || {
                runtime.block_on(async {
                    let lix = open_lix_with_plugin(&plugin).await;
                    csv_schema_changes_insert_fixture(
                        lix,
                        &[],
                        &initial_wasm_changes,
                        INITIAL_ROW_COUNT,
                    )
                    .await
                })
            },
            |fixture| runtime.block_on(manual_bulk_insert_schema_changes(fixture)),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("insert_10k_insert_inmemory", |b| {
        b.iter_batched(
            || {
                runtime.block_on(async {
                    let lix = open_lix_with_plugin_inmemory(&plugin).await;
                    csv_schema_changes_insert_fixture(
                        lix,
                        &[],
                        &initial_wasm_changes,
                        INITIAL_ROW_COUNT,
                    )
                    .await
                })
            },
            |fixture| runtime.block_on(manual_bulk_insert_schema_changes(fixture)),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("insert_10k_insert_state", |b| {
        b.iter_batched(
            || {
                runtime.block_on(async {
                    let lix = open_lix_with_plugin(&plugin).await;
                    csv_changes_insert_fixture(lix, &[], &initial_wasm_changes, INITIAL_ROW_COUNT)
                        .await
                })
            },
            |fixture| runtime.block_on(manual_bulk_insert_file_changes(fixture)),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("insert_10k_insert_state_inmemory", |b| {
        b.iter_batched(
            || {
                runtime.block_on(async {
                    let lix = open_lix_with_plugin_inmemory(&plugin).await;
                    csv_changes_insert_fixture(lix, &[], &initial_wasm_changes, INITIAL_ROW_COUNT)
                        .await
                })
            },
            |fixture| runtime.block_on(manual_bulk_insert_file_changes(fixture)),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("merge_10k", |b| {
        b.iter_batched(
            || {
                runtime.block_on(async {
                    let lix = open_lix_with_plugin(&plugin).await;
                    large_csv_fixture(lix, &initial_csv).await
                })
            },
            |fixture| runtime.block_on(overwrite_large_csv(fixture, &updated_csv)),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("merge_10k_inmemory", |b| {
        b.iter_batched(
            || {
                runtime.block_on(async {
                    let lix = open_lix_with_plugin_inmemory(&plugin).await;
                    large_csv_fixture(lix, &initial_csv).await
                })
            },
            |fixture| runtime.block_on(overwrite_large_csv(fixture, &updated_csv)),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("merge_10k_plugin", |b| {
        b.iter_batched(
            || {
                runtime.block_on(wasm_plugin_diff_fixture(
                    &plugin_wasm,
                    initial_wasm_state.clone(),
                    &updated_csv,
                ))
            },
            |fixture| runtime.block_on(bench_wasm_plugin_diff(fixture)),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("merge_10k_plugin_diff", |b| {
        b.iter_batched(
            || NativeCsvDiffFixture::new(initial_native_state.clone(), updated_csv.clone()),
            bench_native_csv_diff,
            BatchSize::SmallInput,
        );
    });
    group.bench_function("merge_10k_insert", |b| {
        b.iter_batched(
            || {
                runtime.block_on(async {
                    let lix = open_lix_with_plugin(&plugin).await;
                    csv_schema_changes_insert_fixture(
                        lix,
                        &initial_wasm_changes,
                        &merge_wasm_changes,
                        INITIAL_ROW_COUNT + NEW_ROW_COUNT,
                    )
                    .await
                })
            },
            |fixture| runtime.block_on(manual_bulk_insert_schema_changes(fixture)),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("merge_10k_insert_inmemory", |b| {
        b.iter_batched(
            || {
                runtime.block_on(async {
                    let lix = open_lix_with_plugin_inmemory(&plugin).await;
                    csv_schema_changes_insert_fixture(
                        lix,
                        &initial_wasm_changes,
                        &merge_wasm_changes,
                        INITIAL_ROW_COUNT + NEW_ROW_COUNT,
                    )
                    .await
                })
            },
            |fixture| runtime.block_on(manual_bulk_insert_schema_changes(fixture)),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("merge_10k_insert_state", |b| {
        b.iter_batched(
            || {
                runtime.block_on(async {
                    let lix = open_lix_with_plugin(&plugin).await;
                    csv_changes_insert_fixture(
                        lix,
                        &initial_wasm_changes,
                        &merge_wasm_changes,
                        INITIAL_ROW_COUNT + NEW_ROW_COUNT,
                    )
                    .await
                })
            },
            |fixture| runtime.block_on(manual_bulk_insert_file_changes(fixture)),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("merge_10k_insert_state_inmemory", |b| {
        b.iter_batched(
            || {
                runtime.block_on(async {
                    let lix = open_lix_with_plugin_inmemory(&plugin).await;
                    csv_changes_insert_fixture(
                        lix,
                        &initial_wasm_changes,
                        &merge_wasm_changes,
                        INITIAL_ROW_COUNT + NEW_ROW_COUNT,
                    )
                    .await
                })
            },
            |fixture| runtime.block_on(manual_bulk_insert_file_changes(fixture)),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

enum BenchLix {
    Sqlite {
        lix: lix_sdk::Lix<SqliteBackend>,
        _temp_dir: TempDir,
    },
    InMemory {
        lix: lix_sdk::Lix<InMemoryBackend>,
    },
}

impl BenchLix {
    async fn execute(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<lix_sdk::ExecuteResult, LixError> {
        match self {
            Self::Sqlite { lix, .. } => lix.execute(sql, params).await,
            Self::InMemory { lix } => lix.execute(sql, params).await,
        }
    }

    async fn write_file(
        &self,
        path: &str,
        data: Vec<u8>,
        options: FsWriteOptions,
    ) -> Result<(), LixError> {
        match self {
            Self::Sqlite { lix, .. } => lix.write_file(path, data, options).await,
            Self::InMemory { lix } => lix.write_file(path, data, options).await,
        }
    }

    async fn read_file(&self, path: &str) -> Result<Option<Vec<u8>>, LixError> {
        match self {
            Self::Sqlite { lix, .. } => lix.read_file(path).await,
            Self::InMemory { lix } => lix.read_file(path).await,
        }
    }

    async fn close(&self) -> Result<(), LixError> {
        match self {
            Self::Sqlite { lix, .. } => lix.close().await,
            Self::InMemory { lix } => lix.close().await,
        }
    }
}

struct LargeCsvFixture {
    lix: BenchLix,
    file_id: String,
}

struct ExtractedCsvChangesFixture {
    lix: BenchLix,
    file_id: String,
    change_count: usize,
    state_insert_sql: String,
    state_params: Vec<Value>,
    expected_active_row_count: usize,
}

struct ExtractedCsvSchemaChangesFixture {
    lix: BenchLix,
    table_insert_sql: Option<String>,
    table_params: Vec<Value>,
    table_count: usize,
    row_insert_sql: String,
    row_params: Vec<Value>,
    row_count: usize,
    expected_active_row_count: usize,
}

struct WasmPluginDiffFixture {
    component: Arc<dyn lix_sdk::WasmComponentInstance>,
    state: Vec<WasmPluginEntityState>,
    file: WasmPluginFile,
}

impl WasmPluginDiffFixture {
    fn new(
        component: Arc<dyn lix_sdk::WasmComponentInstance>,
        state: Vec<WasmPluginEntityState>,
        file_data: &[u8],
    ) -> Self {
        Self {
            component,
            state,
            file: WasmPluginFile {
                data: file_data.to_vec(),
            },
        }
    }
}

async fn wasm_plugin_diff_fixture(
    wasm: &[u8],
    state: Vec<WasmPluginEntityState>,
    file_data: &[u8],
) -> WasmPluginDiffFixture {
    let runtime = WasmtimePluginRuntime::new().expect("failed to create Wasmtime plugin runtime");
    let component = <WasmtimePluginRuntime as lix_sdk::WasmRuntime>::init_component(
        &runtime,
        wasm.to_vec(),
        lix_sdk::WasmLimits::default(),
    )
    .await
    .unwrap();
    warm_wasm_csv_plugin_component(&component).await;
    WasmPluginDiffFixture::new(component, state, file_data)
}

struct NativeCsvDiffFixture {
    state: Vec<CsvEntityState>,
    file_data: Vec<u8>,
}

impl NativeCsvDiffFixture {
    fn new(state: Vec<CsvEntityState>, file_data: Vec<u8>) -> Self {
        Self { state, file_data }
    }
}

async fn open_lix_with_plugin(plugin: &[u8]) -> BenchLix {
    let temp_dir = tempfile::tempdir().expect("failed to create sqlite bench tempdir");
    let path = temp_dir.path().join("bench.lix");
    let lix = open_lix(OpenLixOptions::new(
        SqliteBackend::open(path).expect("failed to open sqlite bench backend"),
    ))
    .await
    .unwrap();

    lix.install_plugin_archive(plugin).await.unwrap();

    BenchLix::Sqlite {
        lix,
        _temp_dir: temp_dir,
    }
}

async fn open_lix_with_plugin_inmemory(plugin: &[u8]) -> BenchLix {
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();

    lix.install_plugin_archive(plugin).await.unwrap();

    BenchLix::InMemory { lix }
}

async fn open_lix_with_warmed_csv_plugin(plugin: &[u8]) -> BenchLix {
    let lix = open_lix_with_plugin(plugin).await;
    warm_lix_csv_plugin(&lix).await;
    lix
}

async fn open_lix_with_warmed_csv_plugin_inmemory(plugin: &[u8]) -> BenchLix {
    let lix = open_lix_with_plugin_inmemory(plugin).await;
    warm_lix_csv_plugin(&lix).await;
    lix
}

async fn warm_lix_csv_plugin(lix: &BenchLix) {
    lix.write_file(
        CSV_PLUGIN_WARMUP_PATH,
        Vec::new(),
        FsWriteOptions::default(),
    )
    .await
    .unwrap();
    let removed = lix
        .execute(
            "DELETE FROM lix_file WHERE path = $1",
            &[Value::Text(CSV_PLUGIN_WARMUP_PATH.to_string())],
        )
        .await
        .unwrap();
    assert_eq!(removed.rows_affected(), 1);
}

async fn warm_wasm_csv_plugin_component(component: &Arc<dyn lix_sdk::WasmComponentInstance>) {
    let fixture = WasmPluginDiffFixture::new(component.clone(), Vec::new(), &[]);
    let changes = wasm_plugin_detect_changes_output(fixture).await;
    assert!(changes.is_empty());
}

async fn large_csv_fixture(lix: BenchLix, initial_csv: &[u8]) -> LargeCsvFixture {
    lix.write_file(CSV_PATH, initial_csv.to_vec(), FsWriteOptions::default())
        .await
        .unwrap();

    let file_id = lix
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text(CSV_PATH.to_string())],
        )
        .await
        .unwrap();
    assert_eq!(file_id.len(), 1);
    let file_id = file_id.rows()[0].get::<String>("id").unwrap();

    LargeCsvFixture { lix, file_id }
}

async fn csv_changes_insert_fixture(
    lix: BenchLix,
    existing_changes: &[FileChange],
    insert_changes: &[FileChange],
    expected_active_row_count: usize,
) -> ExtractedCsvChangesFixture {
    let file_id = "bench-csv-file".to_string();
    lix.execute(
        "INSERT INTO lix_file (id, path) VALUES ($1, $2)",
        &[
            Value::Text(file_id.clone()),
            Value::Text(CSV_PATH.to_string()),
        ],
    )
    .await
    .unwrap();
    assert_eq!(lix.read_file(CSV_PATH).await.unwrap(), Some(Vec::new()));

    if !existing_changes.is_empty() {
        let existing_insert_sql = bulk_insert_file_changes_sql(existing_changes.len());
        let existing_params = bulk_insert_file_changes_params(&file_id, existing_changes);
        let result = lix
            .execute(&existing_insert_sql, &existing_params)
            .await
            .unwrap();
        assert_eq!(result.rows_affected(), existing_changes.len() as u64);
    }

    let existing_row_count = existing_changes
        .iter()
        .filter(|change| change.schema_key == "csv_row" && change.snapshot_content.is_some())
        .count();
    assert_eq!(
        active_csv_row_count(&lix, &file_id).await,
        existing_row_count
    );

    let state_insert_sql = bulk_insert_file_changes_sql(insert_changes.len());
    let state_params = bulk_insert_file_changes_params(&file_id, insert_changes);
    ExtractedCsvChangesFixture {
        lix,
        file_id,
        change_count: insert_changes.len(),
        state_insert_sql,
        state_params,
        expected_active_row_count,
    }
}

async fn csv_schema_changes_insert_fixture(
    lix: BenchLix,
    existing_changes: &[FileChange],
    insert_changes: &[FileChange],
    expected_active_row_count: usize,
) -> ExtractedCsvSchemaChangesFixture {
    if !existing_changes.is_empty() {
        bulk_insert_schema_changes(&lix, existing_changes).await;
    }

    let existing_row_count = existing_changes
        .iter()
        .filter(|change| change.schema_key == "csv_row" && change.snapshot_content.is_some())
        .count();
    assert_eq!(active_csv_schema_row_count(&lix).await, existing_row_count);

    let table_changes = insert_changes
        .iter()
        .filter(|change| change.schema_key == "csv_table")
        .collect::<Vec<_>>();
    let row_changes = insert_changes
        .iter()
        .filter(|change| change.schema_key == "csv_row")
        .collect::<Vec<_>>();
    assert!(!row_changes.is_empty());

    ExtractedCsvSchemaChangesFixture {
        lix,
        table_insert_sql: (!table_changes.is_empty())
            .then(|| bulk_insert_csv_table_sql(table_changes.len())),
        table_params: bulk_insert_csv_table_params(&table_changes),
        table_count: table_changes.len(),
        row_insert_sql: bulk_insert_csv_row_sql(row_changes.len()),
        row_params: bulk_insert_csv_row_params(&row_changes),
        row_count: row_changes.len(),
        expected_active_row_count,
    }
}

async fn insert_large_csv(lix: BenchLix, initial_csv: &[u8]) {
    lix.write_file(CSV_PATH, initial_csv.to_vec(), FsWriteOptions::default())
        .await
        .unwrap();
    black_box(initial_csv);
    lix.close().await.unwrap();
}

async fn overwrite_large_csv(fixture: LargeCsvFixture, updated_csv: &[u8]) {
    write_updated_csv(&fixture, updated_csv).await;
    black_box(updated_csv);
    fixture.lix.close().await.unwrap();
}

async fn validate_overwrite_large_csv(fixture: LargeCsvFixture, updated_csv: &[u8]) {
    write_updated_csv(&fixture, updated_csv).await;
    assert_large_csv_overwrite_result(&fixture, updated_csv).await;
    fixture.lix.close().await.unwrap();
}

async fn write_updated_csv(fixture: &LargeCsvFixture, updated_csv: &[u8]) {
    fixture
        .lix
        .write_file(CSV_PATH, updated_csv.to_vec(), FsWriteOptions::default())
        .await
        .unwrap();
}

async fn assert_large_csv_overwrite_result(fixture: &LargeCsvFixture, updated_csv: &[u8]) {
    assert_eq!(
        fixture.lix.read_file(CSV_PATH).await.unwrap().as_deref(),
        Some(updated_csv)
    );

    let changes = file_changes(&fixture.lix, &fixture.file_id).await;
    assert_eq!(
        changes
            .iter()
            .filter(|change| change.schema_key == "csv_row" && change.snapshot_content.is_some())
            .count(),
        INITIAL_ROW_COUNT + NEW_ROW_COUNT
    );
    assert_eq!(
        changes
            .iter()
            .filter(|change| change.schema_key == "csv_row" && change.snapshot_content.is_none())
            .count(),
        0
    );
    assert_eq!(
        changes
            .iter()
            .filter(|change| change.schema_key == "csv_table")
            .count(),
        1
    );
    assert!(
        !changes
            .iter()
            .any(|change| change.schema_key == "lix_binary_blob_ref")
    );
    assert_eq!(
        changes
            .iter()
            .filter(|change| {
                change.schema_key == "csv_row"
                    && csv_row_first_cell(change).is_some_and(|cell| cell.starts_with("initial-"))
            })
            .count(),
        INITIAL_ROW_COUNT
    );
    assert_eq!(
        changes
            .iter()
            .filter(|change| {
                change.schema_key == "csv_row"
                    && csv_row_first_cell(change).is_some_and(|cell| cell.starts_with("new-"))
            })
            .count(),
        NEW_ROW_COUNT
    );

    let active_rows = fixture
        .lix
        .execute(
            "SELECT COUNT(*) AS row_count \
             FROM lix_state \
             WHERE file_id = $1 AND schema_key = 'csv_row'",
            &[Value::Text(fixture.file_id.clone())],
        )
        .await
        .unwrap();
    assert_eq!(
        active_rows.rows()[0].get::<i64>("row_count").unwrap(),
        20_000
    );
}

async fn active_csv_row_count(lix: &BenchLix, file_id: &str) -> usize {
    let active_rows = lix
        .execute(
            "SELECT COUNT(*) AS row_count \
             FROM lix_state \
             WHERE file_id = $1 AND schema_key = 'csv_row'",
            &[Value::Text(file_id.to_string())],
        )
        .await
        .unwrap();
    usize::try_from(active_rows.rows()[0].get::<i64>("row_count").unwrap()).unwrap()
}

async fn active_csv_schema_row_count(lix: &BenchLix) -> usize {
    let active_rows = lix
        .execute("SELECT COUNT(*) AS row_count FROM csv_row", &[])
        .await
        .unwrap();
    usize::try_from(active_rows.rows()[0].get::<i64>("row_count").unwrap()).unwrap()
}

async fn manual_bulk_insert_file_changes(fixture: ExtractedCsvChangesFixture) {
    let rows_affected = execute_bulk_insert_file_changes(&fixture).await;
    black_box(rows_affected);
    black_box(fixture.state_insert_sql);
    black_box(fixture.state_params);
    fixture.lix.close().await.unwrap();
}

async fn validate_manual_bulk_insert_file_changes(fixture: ExtractedCsvChangesFixture) {
    let rows_affected = execute_bulk_insert_file_changes(&fixture).await;
    assert_eq!(rows_affected, fixture.change_count as u64);

    assert_eq!(
        active_csv_row_count(&fixture.lix, &fixture.file_id).await,
        fixture.expected_active_row_count
    );

    fixture.lix.close().await.unwrap();
}

async fn execute_bulk_insert_file_changes(fixture: &ExtractedCsvChangesFixture) -> u64 {
    let result = fixture
        .lix
        .execute(&fixture.state_insert_sql, &fixture.state_params)
        .await
        .unwrap();
    result.rows_affected()
}

async fn manual_bulk_insert_schema_changes(fixture: ExtractedCsvSchemaChangesFixture) {
    let (table_rows_affected, row_rows_affected) =
        execute_bulk_insert_schema_changes(&fixture).await;
    black_box(table_rows_affected);
    black_box(row_rows_affected);
    black_box(fixture.table_insert_sql);
    black_box(fixture.table_params);
    black_box(fixture.row_insert_sql);
    black_box(fixture.row_params);
    fixture.lix.close().await.unwrap();
}

async fn validate_manual_bulk_insert_schema_changes(fixture: ExtractedCsvSchemaChangesFixture) {
    let (table_rows_affected, row_rows_affected) =
        execute_bulk_insert_schema_changes(&fixture).await;
    assert_eq!(
        table_rows_affected,
        fixture
            .table_insert_sql
            .as_ref()
            .map(|_| fixture.table_count as u64)
    );
    assert_eq!(row_rows_affected, fixture.row_count as u64);
    assert_eq!(
        active_csv_schema_row_count(&fixture.lix).await,
        fixture.expected_active_row_count
    );

    fixture.lix.close().await.unwrap();
}

async fn execute_bulk_insert_schema_changes(
    fixture: &ExtractedCsvSchemaChangesFixture,
) -> (Option<u64>, u64) {
    let table_rows_affected = if let Some(table_insert_sql) = fixture.table_insert_sql.as_ref() {
        let table_result = fixture
            .lix
            .execute(table_insert_sql, &fixture.table_params)
            .await
            .unwrap();
        Some(table_result.rows_affected())
    } else {
        None
    };

    let row_result = fixture
        .lix
        .execute(&fixture.row_insert_sql, &fixture.row_params)
        .await
        .unwrap();
    (table_rows_affected, row_result.rows_affected())
}

fn bulk_insert_file_changes_sql(change_count: usize) -> String {
    assert!(change_count > 0);
    let mut sql = String::from(
        "INSERT INTO lix_state (entity_pk, schema_key, file_id, snapshot_content) VALUES ",
    );
    for index in 0..change_count {
        if index > 0 {
            sql.push_str(", ");
        }
        let base = 2 + index * 3;
        sql.push('(');
        write!(sql, "${}, ${}, $1, ${}", base, base + 1, base + 2)
            .expect("writing to String cannot fail");
        sql.push(')');
    }
    sql
}

fn bulk_insert_file_changes_params(file_id: &str, changes: &[FileChange]) -> Vec<Value> {
    let mut params = Vec::with_capacity(1 + changes.len() * 3);
    params.push(Value::Text(file_id.to_string()));
    for change in changes {
        params.push(Value::Json(change.entity_pk.clone()));
        params.push(Value::Text(change.schema_key.clone()));
        params.push(
            change
                .snapshot_content
                .as_ref()
                .map_or(Value::Null, |snapshot_content| {
                    Value::Json(snapshot_content.clone())
                }),
        );
    }
    params
}

async fn bulk_insert_schema_changes(lix: &BenchLix, changes: &[FileChange]) {
    let table_changes = changes
        .iter()
        .filter(|change| change.schema_key == "csv_table")
        .collect::<Vec<_>>();
    if !table_changes.is_empty() {
        let table_insert_sql = bulk_insert_csv_table_sql(table_changes.len());
        let table_params = bulk_insert_csv_table_params(&table_changes);
        let result = lix.execute(&table_insert_sql, &table_params).await.unwrap();
        assert_eq!(result.rows_affected(), table_changes.len() as u64);
    }

    let row_changes = changes
        .iter()
        .filter(|change| change.schema_key == "csv_row")
        .collect::<Vec<_>>();
    if !row_changes.is_empty() {
        let row_insert_sql = bulk_insert_csv_row_sql(row_changes.len());
        let row_params = bulk_insert_csv_row_params(&row_changes);
        let result = lix.execute(&row_insert_sql, &row_params).await.unwrap();
        assert_eq!(result.rows_affected(), row_changes.len() as u64);
    }
}

fn bulk_insert_csv_table_sql(change_count: usize) -> String {
    assert!(change_count > 0);
    let mut sql = String::from("INSERT INTO csv_table (id, dialect) VALUES ");
    for index in 0..change_count {
        if index > 0 {
            sql.push_str(", ");
        }
        let base = 1 + index * 2;
        sql.push('(');
        write!(sql, "${}, ${}", base, base + 1).expect("writing to String cannot fail");
        sql.push(')');
    }
    sql
}

fn bulk_insert_csv_table_params(changes: &[&FileChange]) -> Vec<Value> {
    let mut params = Vec::with_capacity(changes.len() * 2);
    for change in changes {
        let snapshot = change
            .snapshot_content
            .as_ref()
            .expect("csv_table insert requires snapshot_content");
        params.push(Value::Text(
            snapshot
                .get("id")
                .and_then(serde_json::Value::as_str)
                .expect("csv_table snapshot requires id")
                .to_string(),
        ));
        params.push(Value::Json(
            snapshot
                .get("dialect")
                .expect("csv_table snapshot requires dialect")
                .clone(),
        ));
    }
    params
}

fn bulk_insert_csv_row_sql(change_count: usize) -> String {
    assert!(change_count > 0);
    let mut sql = String::from("INSERT INTO csv_row (id, order_key, cells) VALUES ");
    for index in 0..change_count {
        if index > 0 {
            sql.push_str(", ");
        }
        let base = 1 + index * 3;
        sql.push('(');
        write!(sql, "${}, ${}, ${}", base, base + 1, base + 2)
            .expect("writing to String cannot fail");
        sql.push(')');
    }
    sql
}

fn bulk_insert_csv_row_params(changes: &[&FileChange]) -> Vec<Value> {
    let mut params = Vec::with_capacity(changes.len() * 3);
    for change in changes {
        let snapshot = change
            .snapshot_content
            .as_ref()
            .expect("csv_row insert requires snapshot_content");
        params.push(Value::Text(
            snapshot
                .get("id")
                .and_then(serde_json::Value::as_str)
                .expect("csv_row snapshot requires id")
                .to_string(),
        ));
        params.push(Value::Text(
            snapshot
                .get("order_key")
                .and_then(serde_json::Value::as_str)
                .expect("csv_row snapshot requires order_key")
                .to_string(),
        ));
        params.push(Value::Json(
            snapshot
                .get("cells")
                .expect("csv_row snapshot requires cells")
                .clone(),
        ));
    }
    params
}

async fn bench_wasm_plugin_diff(fixture: WasmPluginDiffFixture) {
    let output = wasm_plugin_detect_changes_output(fixture).await;
    black_box(output);
}

async fn wasm_plugin_file_changes(fixture: WasmPluginDiffFixture) -> Vec<FileChange> {
    let changes = wasm_plugin_detect_changes_output(fixture).await;
    plugin_detected_changes_to_file_changes(changes)
}

async fn wasm_plugin_detect_changes_output(
    fixture: WasmPluginDiffFixture,
) -> Vec<WasmPluginDetectedChange> {
    fixture
        .component
        .detect_changes(fixture.state, fixture.file)
        .await
        .unwrap()
}

fn bench_native_csv_diff(fixture: NativeCsvDiffFixture) {
    let changes = native_csv_detected_changes(fixture);
    black_box(changes);
}

fn native_csv_file_changes(fixture: NativeCsvDiffFixture) -> Vec<FileChange> {
    csv_detected_changes_to_file_changes(native_csv_detected_changes(fixture))
}

fn native_csv_detected_changes(fixture: NativeCsvDiffFixture) -> Vec<plugin_csv::DetectedChange> {
    CsvPlugin::detect_changes(
        fixture.state,
        CsvFile {
            data: fixture.file_data,
        },
    )
    .unwrap()
}

fn plugin_detected_changes_to_file_changes(
    changes: Vec<WasmPluginDetectedChange>,
) -> Vec<FileChange> {
    changes
        .into_iter()
        .map(|change| {
            assert!(change.metadata.is_none());
            FileChange {
                schema_key: change.schema_key,
                entity_pk: serde_json::Value::Array(
                    change
                        .entity_pk
                        .into_iter()
                        .map(serde_json::Value::String)
                        .collect(),
                ),
                snapshot_content: change
                    .snapshot_content
                    .map(|snapshot| serde_json::from_str(&snapshot).unwrap()),
            }
        })
        .collect()
}

fn csv_detected_changes_to_file_changes(
    changes: Vec<plugin_csv::DetectedChange>,
) -> Vec<FileChange> {
    changes
        .into_iter()
        .map(|change| {
            assert!(change.metadata.is_none());
            FileChange {
                schema_key: change.schema_key,
                entity_pk: serde_json::Value::Array(
                    change
                        .entity_pk
                        .into_iter()
                        .map(serde_json::Value::String)
                        .collect(),
                ),
                snapshot_content: change
                    .snapshot_content
                    .map(|snapshot| csv_snapshot_content_value(&snapshot)),
            }
        })
        .collect()
}

fn plugin_entity_state_from_file_changes(changes: &[FileChange]) -> Vec<WasmPluginEntityState> {
    changes
        .iter()
        .filter_map(|change| {
            change
                .snapshot_content
                .as_ref()
                .map(|snapshot_content| WasmPluginEntityState {
                    entity_pk: entity_pk_parts(change),
                    schema_key: change.schema_key.clone(),
                    snapshot_content: snapshot_content.to_string(),
                    metadata: None,
                })
        })
        .collect()
}

fn csv_entity_state_from_file_changes(changes: &[FileChange]) -> Vec<CsvEntityState> {
    changes
        .iter()
        .filter_map(|change| {
            change
                .snapshot_content
                .as_ref()
                .map(|snapshot_content| CsvEntityState {
                    entity_pk: entity_pk_parts(change),
                    schema_key: change.schema_key.clone(),
                    snapshot_content: csv_snapshot_content_from_value(snapshot_content),
                    metadata: None,
                })
        })
        .collect()
}

fn csv_snapshot_content_value(
    snapshot_content: &BTreeMap<String, plugin_csv::Scalar>,
) -> serde_json::Value {
    let object = snapshot_content
        .iter()
        .map(|(key, value)| (key.clone(), value_from_csv_scalar(value)))
        .collect::<serde_json::Map<_, _>>();
    serde_json::Value::Object(object)
}

fn csv_snapshot_content_from_value(
    value: &serde_json::Value,
) -> BTreeMap<String, plugin_csv::Scalar> {
    let serde_json::Value::Object(object) = value else {
        panic!("CSV snapshot_content should be a JSON object");
    };

    object
        .iter()
        .map(|(key, value)| (key.clone(), csv_scalar_from_value(value.clone())))
        .collect()
}

fn csv_scalar_from_value(value: serde_json::Value) -> plugin_csv::Scalar {
    match value {
        serde_json::Value::Null => plugin_csv::Scalar::Nil,
        serde_json::Value::Bool(value) => plugin_csv::Scalar::Boolean(value),
        serde_json::Value::String(value) => plugin_csv::Scalar::Text(value),
        serde_json::Value::Number(_)
        | serde_json::Value::Array(_)
        | serde_json::Value::Object(_) => plugin_csv::Scalar::Json(
            serde_json::to_string(&value).expect("CSV snapshot scalar should encode"),
        ),
    }
}

fn value_from_csv_scalar(value: &plugin_csv::Scalar) -> serde_json::Value {
    match value {
        plugin_csv::Scalar::Nil => serde_json::Value::Null,
        plugin_csv::Scalar::Boolean(value) => serde_json::Value::Bool(*value),
        plugin_csv::Scalar::Number(value) => serde_json::Value::Number(
            serde_json::Number::from_f64(*value).expect("finite CSV snapshot number"),
        ),
        plugin_csv::Scalar::Text(value) => serde_json::Value::String(value.clone()),
        plugin_csv::Scalar::Json(value) => {
            serde_json::from_str(value).expect("CSV snapshot JSON scalar should parse")
        }
    }
}

fn entity_pk_parts(change: &FileChange) -> Vec<String> {
    change
        .entity_pk
        .as_array()
        .expect("entity_pk should be a JSON array")
        .iter()
        .map(|part| {
            part.as_str()
                .expect("entity_pk parts should be strings")
                .to_string()
        })
        .collect()
}

fn assert_detected_csv_changes(
    changes: &[FileChange],
    expected_row_upserts: usize,
    expected_table_upserts: usize,
) {
    let row_upserts = changes
        .iter()
        .filter(|change| change.schema_key == "csv_row" && change.snapshot_content.is_some())
        .count();
    let row_deletes = changes
        .iter()
        .filter(|change| change.schema_key == "csv_row" && change.snapshot_content.is_none())
        .count();
    let table_upserts = changes
        .iter()
        .filter(|change| change.schema_key == "csv_table" && change.snapshot_content.is_some())
        .count();
    let table_deletes = changes
        .iter()
        .filter(|change| change.schema_key == "csv_table" && change.snapshot_content.is_none())
        .count();

    assert_eq!(row_upserts, expected_row_upserts);
    assert_eq!(row_deletes, 0);
    assert_eq!(table_upserts, expected_table_upserts);
    assert_eq!(table_deletes, 0);
    assert_eq!(
        row_upserts + row_deletes + table_upserts + table_deletes,
        changes.len()
    );
}

fn assert_file_changes_equivalent(direct: &[FileChange], extracted: &[FileChange]) {
    assert_eq!(
        csv_table_dialect(direct),
        csv_table_dialect(extracted),
        "direct plugin csv_table dialect should match extracted dialect"
    );
    assert_eq!(
        normalized_csv_rows(direct),
        normalized_csv_rows(extracted),
        "direct plugin csv_row order/cells should match extracted rows"
    );
}

fn csv_table_dialect(changes: &[FileChange]) -> Option<serde_json::Value> {
    let tables = changes
        .iter()
        .filter(|change| change.schema_key == "csv_table")
        .collect::<Vec<_>>();
    assert!(tables.len() <= 1);
    tables.first().map(|table| {
        table
            .snapshot_content
            .as_ref()
            .expect("csv_table should have snapshot_content")
            .get("dialect")
            .expect("csv_table snapshot should have dialect")
            .clone()
    })
}

fn normalized_csv_rows(changes: &[FileChange]) -> Vec<(String, String)> {
    let mut normalized = changes
        .iter()
        .filter(|change| change.schema_key == "csv_row")
        .map(|change| {
            let snapshot = change
                .snapshot_content
                .as_ref()
                .expect("csv_row should have snapshot_content");
            (
                snapshot
                    .get("order_key")
                    .and_then(serde_json::Value::as_str)
                    .expect("csv_row snapshot should have order_key")
                    .to_string(),
                snapshot
                    .get("cells")
                    .expect("csv_row snapshot should have cells")
                    .to_string(),
            )
        })
        .collect::<Vec<_>>();
    normalized.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    normalized
}

#[derive(Debug, Clone, PartialEq)]
struct FileChange {
    schema_key: String,
    entity_pk: serde_json::Value,
    snapshot_content: Option<serde_json::Value>,
}

async fn file_changes(lix: &BenchLix, file_id: &str) -> Vec<FileChange> {
    let changes = lix
        .execute(
            "SELECT schema_key, entity_pk, snapshot_content \
             FROM lix_change \
             WHERE file_id = $1 \
             ORDER BY created_at, id",
            &[Value::Text(file_id.to_string())],
        )
        .await
        .unwrap();

    changes
        .rows()
        .iter()
        .map(|row| {
            let snapshot_content = match row.value("snapshot_content").unwrap() {
                Value::Json(value) => Some(value.clone()),
                Value::Null => None,
                other => panic!("expected JSON or null snapshot_content, got {other:?}"),
            };
            FileChange {
                schema_key: row.get::<String>("schema_key").unwrap(),
                entity_pk: row.get::<serde_json::Value>("entity_pk").unwrap(),
                snapshot_content,
            }
        })
        .collect()
}

fn random_csv_rows(prefix: &str, count: usize, seed: u64) -> Vec<String> {
    let mut rng = SmallRng::seed_from_u64(seed);
    (0..count)
        .map(|offset| {
            format!(
                "{prefix}-{offset:05},{:016x},{:016x}",
                rng.random::<u64>(),
                rng.random::<u64>()
            )
        })
        .collect()
}

fn randomly_merge_csv_rows(initial_rows: &[String], new_rows: &[String], seed: u64) -> Vec<String> {
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut merged = Vec::with_capacity(initial_rows.len() + new_rows.len());
    let mut initial_index = 0usize;
    let mut new_index = 0usize;

    while initial_index < initial_rows.len() || new_index < new_rows.len() {
        let take_initial = if initial_index == initial_rows.len() {
            false
        } else if new_index == new_rows.len() {
            true
        } else {
            let remaining_initial = initial_rows.len() - initial_index;
            let remaining_new = new_rows.len() - new_index;
            rng.random_range(0..(remaining_initial + remaining_new)) < remaining_initial
        };

        if take_initial {
            merged.push(initial_rows[initial_index].clone());
            initial_index += 1;
        } else {
            merged.push(new_rows[new_index].clone());
            new_index += 1;
        }
    }

    merged
}

fn csv_bytes_from_rows(rows: &[String]) -> Vec<u8> {
    let mut csv = String::with_capacity(rows.iter().map(|row| row.len() + 1).sum());
    for row in rows {
        csv.push_str(row);
        csv.push('\n');
    }
    csv.into_bytes()
}

fn csv_row_first_cell(change: &FileChange) -> Option<&str> {
    change
        .snapshot_content
        .as_ref()?
        .get("cells")?
        .as_array()?
        .first()?
        .as_str()
}

struct WasmtimePluginRuntime {
    engine: Engine,
}

impl WasmtimePluginRuntime {
    fn new() -> Result<Self, LixError> {
        let mut config = Config::new();
        config.wasm_component_model(true);
        config.wasm_component_model_map(true);
        let engine = Engine::new(&config)
            .map_err(|error| wasm_runtime_error("failed to create Wasmtime engine", error))?;
        Ok(Self { engine })
    }
}

struct WasmtimePluginComponent {
    store: Mutex<Store<WasiHostState>>,
    bindings: plugin_bindings::Plugin,
}

struct WasiHostState {
    ctx: WasiCtx,
    table: ResourceTable,
}

impl WasiHostState {
    fn new() -> Self {
        Self {
            ctx: WasiCtxBuilder::new().build(),
            table: ResourceTable::new(),
        }
    }
}

impl WasiView for WasiHostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.ctx,
            table: &mut self.table,
        }
    }
}

#[async_trait::async_trait]
impl lix_sdk::WasmRuntime for WasmtimePluginRuntime {
    async fn init_component(
        &self,
        bytes: Vec<u8>,
        _limits: lix_sdk::WasmLimits,
    ) -> Result<Arc<dyn lix_sdk::WasmComponentInstance>, LixError> {
        let component = Component::new(&self.engine, bytes)
            .map_err(|error| wasm_runtime_error("failed to compile plugin component", error))?;
        let mut linker = Linker::<WasiHostState>::new(&self.engine);
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker)
            .map_err(|error| wasm_runtime_error("failed to configure WASI linker", error))?;
        let mut store = Store::new(&self.engine, WasiHostState::new());
        let bindings = plugin_bindings::Plugin::instantiate(&mut store, &component, &linker)
            .map_err(|error| wasm_runtime_error("failed to instantiate plugin component", error))?;
        Ok(Arc::new(WasmtimePluginComponent {
            store: Mutex::new(store),
            bindings,
        }))
    }
}

#[async_trait::async_trait]
impl lix_sdk::WasmComponentInstance for WasmtimePluginComponent {
    async fn detect_changes(
        &self,
        state: Vec<WasmPluginEntityState>,
        file: WasmPluginFile,
    ) -> Result<Vec<WasmPluginDetectedChange>, LixError> {
        let mut store = self.store("detect-changes")?;
        let state = state
            .into_iter()
            .map(binding_entity_state_from_wasm)
            .collect::<Result<Vec<_>, _>>()?;
        let file = file.into();
        match self
            .bindings
            .lix_plugin_api()
            .call_detect_changes(&mut *store, &state, &file)
            .map_err(|error| wasm_runtime_error("failed to call detect-changes", error))?
        {
            Ok(changes) => changes
                .into_iter()
                .map(wasm_detected_change_from_binding)
                .collect(),
            Err(error) => Err(plugin_error_from_binding("detect-changes", error)),
        }
    }

    async fn render(&self, state: Vec<WasmPluginEntityState>) -> Result<Vec<u8>, LixError> {
        let mut store = self.store("render")?;
        let state = state
            .into_iter()
            .map(binding_entity_state_from_wasm)
            .collect::<Result<Vec<_>, _>>()?;
        match self
            .bindings
            .lix_plugin_api()
            .call_render(&mut *store, &state)
            .map_err(|error| wasm_runtime_error("failed to call render", error))?
        {
            Ok(bytes) => Ok(bytes),
            Err(error) => Err(plugin_error_from_binding("render", error)),
        }
    }
}

impl WasmtimePluginComponent {
    fn store(
        &self,
        export_name: &str,
    ) -> Result<std::sync::MutexGuard<'_, Store<WasiHostState>>, LixError> {
        self.store.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("Wasmtime store lock poisoned before calling {export_name}"),
            )
        })
    }
}

impl From<WasmPluginFile> for plugin_bindings::exports::lix::plugin::api::File {
    fn from(file: WasmPluginFile) -> Self {
        Self { data: file.data }
    }
}

fn binding_entity_state_from_wasm(
    state: WasmPluginEntityState,
) -> Result<plugin_bindings::exports::lix::plugin::api::EntityState, LixError> {
    Ok(plugin_bindings::exports::lix::plugin::api::EntityState {
        entity_pk: state.entity_pk,
        schema_key: state.schema_key,
        snapshot_content: snapshot_content_from_json(
            &state.snapshot_content,
            "plugin state snapshot_content",
        )?,
        metadata: state.metadata,
    })
}

fn wasm_detected_change_from_binding(
    change: plugin_bindings::exports::lix::plugin::api::DetectedChange,
) -> Result<WasmPluginDetectedChange, LixError> {
    Ok(WasmPluginDetectedChange {
        entity_pk: change.entity_pk,
        schema_key: change.schema_key,
        snapshot_content: change
            .snapshot_content
            .as_ref()
            .map(|snapshot_content| {
                snapshot_content_to_json(snapshot_content, "plugin emitted snapshot_content")
            })
            .transpose()?,
        metadata: change.metadata,
    })
}

fn snapshot_content_from_json(raw: &str, label: &str) -> Result<BindingSnapshotContent, LixError> {
    let value: serde_json::Value = serde_json::from_str(raw).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("{label} is invalid JSON: {error}"),
        )
    })?;
    let serde_json::Value::Object(object) = value else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("{label} must be a JSON object"),
        ));
    };

    object
        .into_iter()
        .map(|(key, value)| Ok((key, scalar_from_json_value(value)?)))
        .collect()
}

fn snapshot_content_to_json(
    snapshot_content: &BindingSnapshotContent,
    label: &str,
) -> Result<String, LixError> {
    let object = snapshot_content
        .iter()
        .map(|(key, value)| Ok((key.clone(), json_value_from_scalar(value, label)?)))
        .collect::<Result<serde_json::Map<_, _>, LixError>>()?;
    serde_json::to_string(&serde_json::Value::Object(object)).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to encode {label} JSON: {error}"),
        )
    })
}

fn scalar_from_json_value(
    value: serde_json::Value,
) -> Result<plugin_bindings::exports::lix::plugin::api::Scalar, LixError> {
    match value {
        serde_json::Value::Null => Ok(plugin_bindings::exports::lix::plugin::api::Scalar::Nil),
        serde_json::Value::Bool(value) => Ok(
            plugin_bindings::exports::lix::plugin::api::Scalar::Boolean(value),
        ),
        serde_json::Value::String(value) => Ok(
            plugin_bindings::exports::lix::plugin::api::Scalar::Text(value),
        ),
        serde_json::Value::Number(_)
        | serde_json::Value::Array(_)
        | serde_json::Value::Object(_) => serde_json::to_string(&value)
            .map(plugin_bindings::exports::lix::plugin::api::Scalar::Json)
            .map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("failed to encode snapshot scalar JSON: {error}"),
                )
            }),
    }
}

fn json_value_from_scalar(
    value: &plugin_bindings::exports::lix::plugin::api::Scalar,
    label: &str,
) -> Result<serde_json::Value, LixError> {
    match value {
        plugin_bindings::exports::lix::plugin::api::Scalar::Nil => Ok(serde_json::Value::Null),
        plugin_bindings::exports::lix::plugin::api::Scalar::Boolean(value) => {
            Ok(serde_json::Value::Bool(*value))
        }
        plugin_bindings::exports::lix::plugin::api::Scalar::Number(value) => {
            serde_json::Number::from_f64(*value)
                .map(serde_json::Value::Number)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("{label} contains NaN or infinite number"),
                    )
                })
        }
        plugin_bindings::exports::lix::plugin::api::Scalar::Text(value) => {
            Ok(serde_json::Value::String(value.clone()))
        }
        plugin_bindings::exports::lix::plugin::api::Scalar::Json(value) => {
            serde_json::from_str(value).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("{label} contains invalid JSON scalar: {error}"),
                )
            })
        }
    }
}

fn plugin_error_from_binding(
    export_name: &str,
    error: plugin_bindings::exports::lix::plugin::api::PluginError,
) -> LixError {
    let (kind, message) = match error {
        plugin_bindings::exports::lix::plugin::api::PluginError::InvalidInput(message) => {
            ("invalid-input", message)
        }
        plugin_bindings::exports::lix::plugin::api::PluginError::Internal(message) => {
            ("internal", message)
        }
    };
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("{export_name} returned plugin error {kind}: {message}"),
    )
}

fn wasm_runtime_error(context: impl Into<String>, error: impl fmt::Display) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("{}: {error}", context.into()),
    )
}

fn build_csv_plugin() -> Vec<u8> {
    let wasm = csv_plugin_wasm();
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        (
            "manifest.json",
            include_str!("../../../plugins/csv/manifest.json").as_bytes(),
        ),
        (
            "schema/csv_table.json",
            include_str!("../../../plugins/csv/schema/csv_table.json").as_bytes(),
        ),
        (
            "schema/csv_row.json",
            include_str!("../../../plugins/csv/schema/csv_row.json").as_bytes(),
        ),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        writer.start_file(path, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

fn csv_plugin_wasm() -> Vec<u8> {
    let wasm_path = Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_plugin_csv"));
    std::fs::read(wasm_path).unwrap_or_else(|error| {
        panic!(
            "failed to read bindep-built CSV plugin wasm at {}: {error}",
            wasm_path.display()
        )
    })
}

fn csv_plugin_wasm_from_archive(archive_bytes: &[u8]) -> Vec<u8> {
    let mut plugin = zip::ZipArchive::new(Cursor::new(archive_bytes)).unwrap();
    let mut entry = plugin.by_name("plugin.wasm").unwrap();
    let mut wasm = Vec::with_capacity(usize::try_from(entry.size()).unwrap_or(0));
    entry.read_to_end(&mut wasm).unwrap();
    wasm
}

criterion_group!(benches, bench_e2e);
criterion_main!(benches);
