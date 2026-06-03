use criterion::{BatchSize, Criterion, Throughput, black_box, criterion_group, criterion_main};
use lix_sdk::{FsWriteOptions, InMemoryBackend, LixError, OpenLixOptions, Value, open_lix};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Write as _;
use std::io::{Cursor, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::runtime::Builder;
use wasmtime::component::types::ComponentItem;
use wasmtime::component::{Component, ComponentExportIndex, Instance, Linker, Val};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{IoView, ResourceTable, WasiCtx, WasiCtxBuilder, WasiView};

const INITIAL_ROW_COUNT: usize = 10_000;
const NEW_ROW_COUNT: usize = 10_000;
const CSV_PATH: &str = "/large-merge.csv";

fn bench_e2e(c: &mut Criterion) {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to create Tokio runtime for e2e bench");
    let archive = build_csv_plugin_archive();
    let initial_rows = random_csv_rows("initial", INITIAL_ROW_COUNT, 0x8ae7_b4b1_9f4c_d215);
    let new_rows = random_csv_rows("new", NEW_ROW_COUNT, 0xf3bb_91d4_6a8c_2e73);
    let initial_csv = csv_bytes_from_rows(&initial_rows);
    let updated_csv = csv_bytes_from_rows(&randomly_merge_csv_rows(
        &initial_rows,
        &new_rows,
        0x6449_2c6f_179d_31b5,
    ));

    let mut group = c.benchmark_group("e2e/csv_plugin");
    group.sample_size(10);
    group.throughput(Throughput::Elements(NEW_ROW_COUNT as u64));
    group.bench_function("insert_10k", |b| {
        b.iter_batched(
            || runtime.block_on(csv_plugin_fixture(&archive)),
            |fixture| runtime.block_on(insert_large_csv(fixture, &initial_csv)),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("overwrite_random_merge_10k_to_20k", |b| {
        b.iter_batched(
            || runtime.block_on(large_csv_fixture(&archive, &initial_csv)),
            |fixture| runtime.block_on(overwrite_large_csv(fixture, &updated_csv)),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("manual_bulk_insert_extracted_10k", |b| {
        b.iter_batched(
            || runtime.block_on(extracted_csv_changes_fixture(&archive, &initial_csv)),
            |fixture| runtime.block_on(manual_bulk_insert_file_changes(fixture)),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("manual_bulk_insert_entities_extracted_10k", |b| {
        b.iter_batched(
            || runtime.block_on(extracted_csv_changes_fixture(&archive, &initial_csv)),
            |fixture| runtime.block_on(manual_bulk_insert_entities(fixture)),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("manual_bulk_insert_entities_direct_plugin_10k", |b| {
        b.iter_batched(
            || runtime.block_on(direct_plugin_csv_entities_fixture(&archive, &initial_csv)),
            |fixture| runtime.block_on(manual_bulk_insert_entities(fixture)),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

struct LargeCsvFixture {
    lix: lix_sdk::Lix,
    file_id: String,
}

struct CsvPluginFixture {
    lix: lix_sdk::Lix,
}

struct ExtractedCsvChangesFixture {
    lix: lix_sdk::Lix,
    file_id: String,
    change_count: usize,
    state_insert_sql: String,
    state_params: Vec<Value>,
    table_insert_sql: String,
    table_params: Vec<Value>,
    row_insert_sql: String,
    row_params: Vec<Value>,
    row_count: usize,
    changes: Vec<FileChange>,
}

async fn csv_plugin_fixture(archive: &[u8]) -> CsvPluginFixture {
    let lix = open_lix(OpenLixOptions {
        backend: InMemoryBackend::new(),
        wasm_runtime: Some(Arc::new(
            WasmtimePluginRuntime::new().expect("failed to create Wasmtime plugin runtime"),
        )),
    })
    .await
    .unwrap();

    lix.install_plugin_archive(archive).await.unwrap();

    CsvPluginFixture { lix }
}

async fn large_csv_fixture(archive: &[u8], initial_csv: &[u8]) -> LargeCsvFixture {
    let fixture = csv_plugin_fixture(archive).await;
    fixture
        .lix
        .write_file(CSV_PATH, initial_csv.to_vec(), FsWriteOptions::default())
        .await
        .unwrap();

    let file_id = fixture
        .lix
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text(CSV_PATH.to_string())],
        )
        .await
        .unwrap();
    assert_eq!(file_id.len(), 1);
    let file_id = file_id.rows()[0].get::<String>("id").unwrap();

    LargeCsvFixture {
        lix: fixture.lix,
        file_id,
    }
}

async fn extracted_csv_changes_fixture(
    archive: &[u8],
    initial_csv: &[u8],
) -> ExtractedCsvChangesFixture {
    let fixture = large_csv_fixture(archive, initial_csv).await;
    let changes = file_changes(&fixture.lix, &fixture.file_id)
        .await
        .into_iter()
        .filter(|change| change.schema_key == "csv_row" || change.schema_key == "csv_table")
        .collect::<Vec<_>>();
    assert_eq!(
        changes
            .iter()
            .filter(|change| change.schema_key == "csv_row" && change.snapshot_content.is_some())
            .count(),
        INITIAL_ROW_COUNT
    );
    assert_eq!(
        changes
            .iter()
            .filter(|change| change.schema_key == "csv_table" && change.snapshot_content.is_some())
            .count(),
        1
    );

    fixture
        .lix
        .execute(
            "DELETE FROM lix_file WHERE path = $1",
            &[Value::Text(CSV_PATH.to_string())],
        )
        .await
        .unwrap();
    assert_eq!(fixture.lix.read_file(CSV_PATH).await.unwrap(), None);
    fixture
        .lix
        .execute(
            "INSERT INTO lix_file (id, path) VALUES ($1, $2)",
            &[
                Value::Text(fixture.file_id.clone()),
                Value::Text(CSV_PATH.to_string()),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        fixture.lix.read_file(CSV_PATH).await.unwrap(),
        Some(Vec::new())
    );
    let active_rows = fixture
        .lix
        .execute(
            "SELECT COUNT(*) AS row_count \
             FROM lix_state \
             WHERE file_id = $1 AND schema_key IN ('csv_row', 'csv_table')",
            &[Value::Text(fixture.file_id.clone())],
        )
        .await
        .unwrap();
    assert_eq!(active_rows.rows()[0].get::<i64>("row_count").unwrap(), 0);

    let state_insert_sql = bulk_insert_file_changes_sql(changes.len());
    let state_params = bulk_insert_file_changes_params(&fixture.file_id, &changes);
    let table_changes = changes
        .iter()
        .filter(|change| change.schema_key == "csv_table")
        .collect::<Vec<_>>();
    let row_changes = changes
        .iter()
        .filter(|change| change.schema_key == "csv_row")
        .collect::<Vec<_>>();
    let table_insert_sql = bulk_insert_csv_table_sql(table_changes.len());
    let table_params = bulk_insert_csv_table_params(&table_changes);
    let row_insert_sql = bulk_insert_csv_row_sql(row_changes.len());
    let row_params = bulk_insert_csv_row_params(&row_changes);
    let change_count = changes.len();
    let row_count = row_changes.len();

    ExtractedCsvChangesFixture {
        lix: fixture.lix,
        file_id: fixture.file_id,
        change_count,
        state_insert_sql,
        state_params,
        table_insert_sql,
        table_params,
        row_insert_sql,
        row_params,
        row_count,
        changes,
    }
}

async fn direct_plugin_csv_entities_fixture(
    archive: &[u8],
    initial_csv: &[u8],
) -> ExtractedCsvChangesFixture {
    let extracted_fixture = extracted_csv_changes_fixture(archive, initial_csv).await;
    let direct_changes = direct_plugin_csv_changes(initial_csv).await;
    assert_file_changes_equivalent(&direct_changes, &extracted_fixture.changes);

    let table_changes = direct_changes
        .iter()
        .filter(|change| change.schema_key == "csv_table")
        .collect::<Vec<_>>();
    let row_changes = direct_changes
        .iter()
        .filter(|change| change.schema_key == "csv_row")
        .collect::<Vec<_>>();
    let table_insert_sql = bulk_insert_csv_table_sql(table_changes.len());
    let table_params = bulk_insert_csv_table_params(&table_changes);
    let row_insert_sql = bulk_insert_csv_row_sql(row_changes.len());
    let row_params = bulk_insert_csv_row_params(&row_changes);
    let state_insert_sql = bulk_insert_file_changes_sql(direct_changes.len());
    let state_params = bulk_insert_file_changes_params(&extracted_fixture.file_id, &direct_changes);
    let change_count = direct_changes.len();
    let row_count = row_changes.len();

    ExtractedCsvChangesFixture {
        lix: extracted_fixture.lix,
        file_id: extracted_fixture.file_id,
        change_count,
        state_insert_sql,
        state_params,
        table_insert_sql,
        table_params,
        row_insert_sql,
        row_params,
        row_count,
        changes: direct_changes,
    }
}

async fn insert_large_csv(fixture: CsvPluginFixture, initial_csv: &[u8]) {
    fixture
        .lix
        .write_file(CSV_PATH, initial_csv.to_vec(), FsWriteOptions::default())
        .await
        .unwrap();
    black_box(initial_csv);
    fixture.lix.close().await.unwrap();
}

async fn overwrite_large_csv(fixture: LargeCsvFixture, updated_csv: &[u8]) {
    fixture
        .lix
        .write_file(CSV_PATH, updated_csv.to_vec(), FsWriteOptions::default())
        .await
        .unwrap();
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
            &[Value::Text(fixture.file_id)],
        )
        .await
        .unwrap();
    assert_eq!(
        active_rows.rows()[0].get::<i64>("row_count").unwrap(),
        20_000
    );

    black_box(changes);
    fixture.lix.close().await.unwrap();
}

async fn manual_bulk_insert_file_changes(fixture: ExtractedCsvChangesFixture) {
    let result = fixture
        .lix
        .execute(&fixture.state_insert_sql, &fixture.state_params)
        .await
        .unwrap();
    assert_eq!(result.rows_affected(), fixture.change_count as u64);

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
        i64::try_from(INITIAL_ROW_COUNT).unwrap()
    );

    black_box(fixture.state_insert_sql);
    black_box(fixture.state_params);
    fixture.lix.close().await.unwrap();
}

async fn manual_bulk_insert_entities(fixture: ExtractedCsvChangesFixture) {
    let table_result = fixture
        .lix
        .execute(&fixture.table_insert_sql, &fixture.table_params)
        .await
        .unwrap();
    assert_eq!(table_result.rows_affected(), 1);

    let row_result = fixture
        .lix
        .execute(&fixture.row_insert_sql, &fixture.row_params)
        .await
        .unwrap();
    assert_eq!(row_result.rows_affected(), fixture.row_count as u64);

    let active_rows = fixture
        .lix
        .execute(
            "SELECT COUNT(*) AS row_count \
             FROM csv_row",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        active_rows.rows()[0].get::<i64>("row_count").unwrap(),
        i64::try_from(INITIAL_ROW_COUNT).unwrap()
    );

    black_box(fixture.table_insert_sql);
    black_box(fixture.table_params);
    black_box(fixture.row_insert_sql);
    black_box(fixture.row_params);
    fixture.lix.close().await.unwrap();
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

async fn direct_plugin_csv_changes(initial_csv: &[u8]) -> Vec<FileChange> {
    let runtime = WasmtimePluginRuntime::new().expect("failed to create Wasmtime plugin runtime");
    let component = <WasmtimePluginRuntime as lix_sdk::WasmRuntime>::init_component(
        &runtime,
        csv_plugin_wasm(),
        lix_sdk::WasmLimits::default(),
    )
    .await
    .unwrap();
    let payload = serde_json::json!({
        "state": [],
        "file": { "data": initial_csv },
    });
    let output = component
        .call("detect-changes", &serde_json::to_vec(&payload).unwrap())
        .await
        .unwrap();
    let changes = serde_json::from_slice::<Vec<PluginDetectedChangePayload>>(&output).unwrap();
    plugin_detected_changes_to_file_changes(changes)
}

fn plugin_detected_changes_to_file_changes(
    changes: Vec<PluginDetectedChangePayload>,
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

fn csv_table_dialect(changes: &[FileChange]) -> serde_json::Value {
    let tables = changes
        .iter()
        .filter(|change| change.schema_key == "csv_table")
        .collect::<Vec<_>>();
    assert_eq!(tables.len(), 1);
    tables[0]
        .snapshot_content
        .as_ref()
        .expect("csv_table should have snapshot_content")
        .get("dialect")
        .expect("csv_table snapshot should have dialect")
        .clone()
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

async fn file_changes(lix: &lix_sdk::Lix, file_id: &str) -> Vec<FileChange> {
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
        let engine = Engine::new(&config)
            .map_err(|error| wasm_runtime_error("failed to create Wasmtime engine", error))?;
        Ok(Self { engine })
    }
}

struct WasmtimePluginComponent {
    store: Mutex<Store<WasiHostState>>,
    instance: Instance,
    exports: WasmtimePluginExports,
}

#[derive(Clone, Copy)]
struct WasmtimePluginExports {
    detect_changes: ComponentExportIndex,
    render: ComponentExportIndex,
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

impl IoView for WasiHostState {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

impl WasiView for WasiHostState {
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.ctx
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
        let exports = WasmtimePluginExports::from_component(&self.engine, &component)?;
        let mut linker = Linker::<WasiHostState>::new(&self.engine);
        wasmtime_wasi::add_to_linker_sync(&mut linker)
            .map_err(|error| wasm_runtime_error("failed to configure WASI linker", error))?;
        let mut store = Store::new(&self.engine, WasiHostState::new());
        let instance = linker
            .instantiate(&mut store, &component)
            .map_err(|error| wasm_runtime_error("failed to instantiate plugin component", error))?;
        Ok(Arc::new(WasmtimePluginComponent {
            store: Mutex::new(store),
            instance,
            exports,
        }))
    }
}

#[async_trait::async_trait]
impl lix_sdk::WasmComponentInstance for WasmtimePluginComponent {
    async fn call(&self, export: &str, input: &[u8]) -> Result<Vec<u8>, LixError> {
        match export {
            "detect-changes" | "api#detect-changes" => self.detect_changes(input),
            "render" | "api#render" => self.render(input),
            other => Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("Wasmtime test runtime does not implement export '{other}'"),
            )),
        }
    }
}

impl WasmtimePluginExports {
    fn from_component(engine: &Engine, component: &Component) -> Result<Self, LixError> {
        Ok(Self {
            detect_changes: find_plugin_func_export(engine, component, "detect-changes")?,
            render: find_plugin_func_export(engine, component, "render")?,
        })
    }
}

impl WasmtimePluginComponent {
    fn detect_changes(&self, input: &[u8]) -> Result<Vec<u8>, LixError> {
        let payload: PluginDetectChangesPayload =
            serde_json::from_slice(input).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("plugin detect-changes payload is invalid JSON: {error}"),
                )
            })?;
        let params = [
            entity_state_list_to_val(payload.state),
            Val::Record(vec![("data".to_string(), bytes_to_val(payload.file.data))]),
        ];
        let result =
            self.call_component_func(self.exports.detect_changes, &params, "detect-changes")?;
        let changes = expect_detected_changes_result(result, "detect-changes")?;
        serde_json::to_vec(&changes).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to encode plugin detect-changes output: {error}"),
            )
        })
    }

    fn render(&self, input: &[u8]) -> Result<Vec<u8>, LixError> {
        let payload: PluginRenderPayload = serde_json::from_slice(input).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("plugin render payload is invalid JSON: {error}"),
            )
        })?;
        let params = [entity_state_list_to_val(payload.state)];
        let result = self.call_component_func(self.exports.render, &params, "render")?;
        expect_render_result(result, "render")
    }

    fn call_component_func(
        &self,
        export: ComponentExportIndex,
        params: &[Val],
        export_name: &str,
    ) -> Result<Val, LixError> {
        let mut store = self.store.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "Wasmtime store lock poisoned",
            )
        })?;
        let func = self.instance.get_func(&mut *store, export).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("plugin component export '{export_name}' is not a function"),
            )
        })?;
        let mut results = [Val::Result(Ok(None))];
        func.call(&mut *store, params, &mut results)
            .map_err(|error| wasm_runtime_error(format!("failed to call {export_name}"), error))?;
        func.post_return(&mut *store).map_err(|error| {
            wasm_runtime_error(format!("failed to finish {export_name} call"), error)
        })?;
        Ok(results.into_iter().next().unwrap())
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct PluginDetectChangesPayload {
    state: Vec<PluginEntityStatePayload>,
    file: PluginFilePayload,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct PluginRenderPayload {
    state: Vec<PluginEntityStatePayload>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct PluginFilePayload {
    data: Vec<u8>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct PluginEntityStatePayload {
    entity_pk: Vec<String>,
    schema_key: String,
    snapshot_content: String,
    metadata: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
struct PluginDetectedChangePayload {
    entity_pk: Vec<String>,
    schema_key: String,
    snapshot_content: Option<String>,
    metadata: Option<String>,
}

fn find_plugin_func_export(
    engine: &Engine,
    component: &Component,
    func_name: &str,
) -> Result<ComponentExportIndex, LixError> {
    if let Some((ComponentItem::ComponentFunc(_), export)) = component.export_index(None, func_name)
    {
        return Ok(export);
    }

    let component_type = component.component_type();
    for (instance_name, item) in component_type.exports(engine) {
        if !matches!(item, ComponentItem::ComponentInstance(_)) {
            continue;
        }
        let Some((ComponentItem::ComponentInstance(_), instance_export)) =
            component.export_index(None, instance_name)
        else {
            continue;
        };
        if let Some((ComponentItem::ComponentFunc(_), export)) =
            component.export_index(Some(&instance_export), func_name)
        {
            return Ok(export);
        }
    }

    Err(LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!(
            "plugin component is missing export '{func_name}'. Available exports: {}",
            component_exports_summary(engine, component)
        ),
    ))
}

fn component_exports_summary(engine: &Engine, component: &Component) -> String {
    let component_type = component.component_type();
    let mut exports = Vec::new();
    for (name, item) in component_type.exports(engine) {
        match item {
            ComponentItem::ComponentInstance(instance) => {
                let nested = instance
                    .exports(engine)
                    .map(|(nested_name, _)| nested_name.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                exports.push(format!("{name}({nested})"));
            }
            _ => exports.push(name.to_string()),
        }
    }
    exports.join(", ")
}

fn entity_state_list_to_val(state: Vec<PluginEntityStatePayload>) -> Val {
    Val::List(state.into_iter().map(entity_state_to_val).collect())
}

fn entity_state_to_val(state: PluginEntityStatePayload) -> Val {
    Val::Record(vec![
        ("entity-pk".to_string(), string_list_to_val(state.entity_pk)),
        ("schema-key".to_string(), Val::String(state.schema_key)),
        (
            "snapshot-content".to_string(),
            Val::String(state.snapshot_content),
        ),
        (
            "metadata".to_string(),
            optional_string_to_val(state.metadata),
        ),
    ])
}

fn string_list_to_val(values: Vec<String>) -> Val {
    Val::List(values.into_iter().map(Val::String).collect())
}

fn bytes_to_val(bytes: Vec<u8>) -> Val {
    Val::List(bytes.into_iter().map(Val::U8).collect())
}

fn optional_string_to_val(value: Option<String>) -> Val {
    Val::Option(value.map(|value| Box::new(Val::String(value))))
}

fn expect_detected_changes_result(
    result: Val,
    export_name: &str,
) -> Result<Vec<PluginDetectedChangePayload>, LixError> {
    let output = expect_plugin_ok_result(result, export_name)?;
    let Val::List(values) = output else {
        return Err(plugin_abi_error(format!(
            "{export_name} returned {}, expected list",
            val_type_name(&output)
        )));
    };
    values.into_iter().map(detected_change_from_val).collect()
}

fn expect_render_result(result: Val, export_name: &str) -> Result<Vec<u8>, LixError> {
    let output = expect_plugin_ok_result(result, export_name)?;
    expect_u8_list(output, export_name)
}

fn expect_plugin_ok_result(result: Val, export_name: &str) -> Result<Val, LixError> {
    match result {
        Val::Result(Ok(Some(output))) => Ok(*output),
        Val::Result(Ok(None)) => Err(plugin_abi_error(format!(
            "{export_name} returned ok without a payload"
        ))),
        Val::Result(Err(error)) => Err(plugin_error_from_val(export_name, error.map(|v| *v))),
        other => Err(plugin_abi_error(format!(
            "{export_name} returned {}, expected result",
            val_type_name(&other)
        ))),
    }
}

fn detected_change_from_val(value: Val) -> Result<PluginDetectedChangePayload, LixError> {
    let Val::Record(fields) = value else {
        return Err(plugin_abi_error(format!(
            "detect-changes item was {}, expected record",
            val_type_name(&value)
        )));
    };
    let mut fields = fields.into_iter();
    let entity_pk = expect_string_list(
        expect_next_field(&mut fields, "entity-pk", "detected-change")?,
        "detected-change.entity-pk",
    )?;
    let schema_key = expect_string(
        expect_next_field(&mut fields, "schema-key", "detected-change")?,
        "detected-change.schema-key",
    )?;
    let snapshot_content = expect_optional_string(
        expect_next_field(&mut fields, "snapshot-content", "detected-change")?,
        "detected-change.snapshot-content",
    )?;
    let metadata = expect_optional_string(
        expect_next_field(&mut fields, "metadata", "detected-change")?,
        "detected-change.metadata",
    )?;
    if let Some((field, _)) = fields.next() {
        return Err(plugin_abi_error(format!(
            "detected-change returned unexpected field '{field}'"
        )));
    }
    Ok(PluginDetectedChangePayload {
        entity_pk,
        schema_key,
        snapshot_content,
        metadata,
    })
}

fn expect_next_field(
    fields: &mut impl Iterator<Item = (String, Val)>,
    expected: &str,
    label: &str,
) -> Result<Val, LixError> {
    let Some((field, value)) = fields.next() else {
        return Err(plugin_abi_error(format!(
            "{label} is missing field '{expected}'"
        )));
    };
    if field != expected {
        return Err(plugin_abi_error(format!(
            "{label} returned field '{field}', expected '{expected}'"
        )));
    }
    Ok(value)
}

fn expect_string_list(value: Val, label: &str) -> Result<Vec<String>, LixError> {
    let Val::List(values) = value else {
        return Err(plugin_abi_error(format!(
            "{label} was {}, expected list<string>",
            val_type_name(&value)
        )));
    };
    values
        .into_iter()
        .map(|value| expect_string(value, label))
        .collect()
}

fn expect_u8_list(value: Val, label: &str) -> Result<Vec<u8>, LixError> {
    let Val::List(values) = value else {
        return Err(plugin_abi_error(format!(
            "{label} was {}, expected list<u8>",
            val_type_name(&value)
        )));
    };
    values
        .into_iter()
        .map(|value| match value {
            Val::U8(value) => Ok(value),
            other => Err(plugin_abi_error(format!(
                "{label} list item was {}, expected u8",
                val_type_name(&other)
            ))),
        })
        .collect()
}

fn expect_string(value: Val, label: &str) -> Result<String, LixError> {
    match value {
        Val::String(value) => Ok(value),
        other => Err(plugin_abi_error(format!(
            "{label} was {}, expected string",
            val_type_name(&other)
        ))),
    }
}

fn expect_optional_string(value: Val, label: &str) -> Result<Option<String>, LixError> {
    match value {
        Val::Option(None) => Ok(None),
        Val::Option(Some(value)) => expect_string(*value, label).map(Some),
        other => Err(plugin_abi_error(format!(
            "{label} was {}, expected option<string>",
            val_type_name(&other)
        ))),
    }
}

fn plugin_error_from_val(export_name: &str, value: Option<Val>) -> LixError {
    let message = match value {
        Some(Val::Variant(kind, Some(payload))) => match *payload {
            Val::String(message) => {
                format!("{export_name} returned plugin error {kind}: {message}")
            }
            other => format!(
                "{export_name} returned plugin error {kind} with {} payload",
                val_type_name(&other)
            ),
        },
        Some(Val::Variant(kind, None)) => {
            format!("{export_name} returned plugin error {kind} without payload")
        }
        Some(other) => format!(
            "{export_name} returned malformed plugin error {}",
            val_type_name(&other)
        ),
        None => format!("{export_name} returned plugin error without payload"),
    };
    LixError::new(LixError::CODE_INTERNAL_ERROR, message)
}

fn val_type_name(value: &Val) -> &'static str {
    match value {
        Val::Bool(_) => "bool",
        Val::S8(_) => "s8",
        Val::U8(_) => "u8",
        Val::S16(_) => "s16",
        Val::U16(_) => "u16",
        Val::S32(_) => "s32",
        Val::U32(_) => "u32",
        Val::S64(_) => "s64",
        Val::U64(_) => "u64",
        Val::Float32(_) => "float32",
        Val::Float64(_) => "float64",
        Val::Char(_) => "char",
        Val::String(_) => "string",
        Val::List(_) => "list",
        Val::Record(_) => "record",
        Val::Tuple(_) => "tuple",
        Val::Variant(_, _) => "variant",
        Val::Enum(_) => "enum",
        Val::Option(_) => "option",
        Val::Result(_) => "result",
        Val::Flags(_) => "flags",
        Val::Resource(_) => "resource",
    }
}

fn plugin_abi_error(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INTERNAL_ERROR, message.into())
}

fn wasm_runtime_error(context: impl Into<String>, error: impl fmt::Display) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("{}: {error}", context.into()),
    )
}

fn build_csv_plugin_archive() -> Vec<u8> {
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
    let wasm_path = Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_WASM_plugin_csv"));
    std::fs::read(wasm_path).unwrap_or_else(|error| {
        panic!(
            "failed to read bindep-built CSV plugin wasm at {}: {error}",
            wasm_path.display()
        )
    })
}

criterion_group!(benches, bench_e2e);
criterion_main!(benches);
