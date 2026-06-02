use lix_sdk::{
    CreateBranchOptions, FsWriteOptions, InMemoryBackend, LixError, MergeBranchOptions,
    MergeBranchOutcome, OpenLixOptions, SwitchBranchOptions, Value, open_lix,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::io::{Cursor, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use wasmtime::component::types::ComponentItem;
use wasmtime::component::{Component, ComponentExportIndex, Instance, Linker, Val};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{IoView, ResourceTable, WasiCtx, WasiCtxBuilder, WasiView};

#[tokio::test]
async fn rs_sdk_installs_built_csv_plugin_archive_and_uses_schema() {
    let archive = build_csv_plugin_archive();
    let lix = open_lix(OpenLixOptions {
        backend: None,
        wasm_runtime: Some(Arc::new(
            WasmtimePluginRuntime::new().expect("failed to create Wasmtime plugin runtime"),
        )),
    })
    .await
    .unwrap();

    lix.install_plugin_archive(&archive).await.unwrap();
    let plugins = lix.list_installed_plugins().await.unwrap();
    assert_eq!(plugins.len(), 1);
    assert_eq!(plugins[0].key, "plugin_csv");
    assert_eq!(
        plugins[0].schema_keys,
        vec!["csv_table".to_string(), "csv_row".to_string()]
    );

    let stored_archive = lix
        .read_file("/.lix/plugins/plugin_csv.lixplugin")
        .await
        .unwrap();
    assert_eq!(stored_archive.as_deref(), Some(archive.as_slice()));

    let schemas = lix
        .execute(
            "SELECT table_name \
             FROM information_schema.tables \
             WHERE table_name IN ('csv_row', 'csv_table') \
             ORDER BY table_name",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        schemas
            .rows()
            .iter()
            .map(|row| row.get::<String>("table_name").unwrap())
            .collect::<Vec<_>>(),
        vec!["csv_row".to_string(), "csv_table".to_string()]
    );

    let original_csv = b"name,age\nAda,37\n".to_vec();
    lix.write_file(
        "/people.csv",
        original_csv.clone(),
        FsWriteOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        lix.read_file("/people.csv").await.unwrap().as_deref(),
        Some(original_csv.as_slice())
    );

    let file_id = lix
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text("/people.csv".to_string())],
        )
        .await
        .unwrap();
    assert_eq!(file_id.len(), 1);
    let file_id = file_id.rows()[0].get::<String>("id").unwrap();
    let file_changes_before_update = file_changes(&lix, &file_id).await;

    let updated_csv = b"name,age\nAda,37\nGrace,85\n".to_vec();
    lix.write_file(
        "/people.csv",
        updated_csv.clone(),
        FsWriteOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        lix.read_file("/people.csv").await.unwrap().as_deref(),
        Some(updated_csv.as_slice())
    );

    let file_changes_after_update = file_changes(&lix, &file_id).await;
    let resulting_diff_changes = file_changes_after_update
        .into_iter()
        .skip(file_changes_before_update.len())
        .collect::<Vec<_>>();
    assert_eq!(resulting_diff_changes.len(), 1);
    let change = &resulting_diff_changes[0];
    assert_eq!(change.schema_key, "csv_row");
    let snapshot = change
        .snapshot_content
        .as_ref()
        .expect("updated file write should produce a csv row snapshot");
    assert_eq!(
        snapshot
            .get("cells")
            .and_then(serde_json::Value::as_array)
            .unwrap(),
        &vec![
            serde_json::Value::String("Grace".to_string()),
            serde_json::Value::String("85".to_string())
        ]
    );

    let files = lix
        .execute(
            "SELECT path, data FROM lix_file WHERE path = $1",
            &[Value::Text("/people.csv".to_string())],
        )
        .await
        .unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(
        files.rows()[0].values(),
        &[
            Value::Text("/people.csv".to_string()),
            Value::Blob(updated_csv.clone())
        ]
    );

    let sql_csv = b"name,age\nLin,44\n".to_vec();
    lix.execute(
        "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
        &[
            Value::Text("/sql-people.csv".to_string()),
            Value::Blob(sql_csv.clone()),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        lix.read_file("/sql-people.csv").await.unwrap().as_deref(),
        Some(sql_csv.as_slice())
    );

    let sql_file_id = lix
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text("/sql-people.csv".to_string())],
        )
        .await
        .unwrap();
    assert_eq!(sql_file_id.len(), 1);
    let sql_file_id = sql_file_id.rows()[0].get::<String>("id").unwrap();
    let sql_insert_changes = file_changes(&lix, &sql_file_id).await;
    assert!(
        sql_insert_changes
            .iter()
            .any(|change| change.schema_key == "csv_table")
    );
    assert!(
        sql_insert_changes
            .iter()
            .any(|change| change.schema_key == "csv_row")
    );
    assert!(
        !sql_insert_changes
            .iter()
            .any(|change| change.schema_key == "lix_binary_blob_ref")
    );

    let sql_changes_before_update = sql_insert_changes.len();
    let sql_updated_csv = b"name,age\nLin,44\nMina,29\n".to_vec();
    lix.execute(
        "UPDATE lix_file SET data = $1 WHERE path = $2",
        &[
            Value::Blob(sql_updated_csv.clone()),
            Value::Text("/sql-people.csv".to_string()),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        lix.read_file("/sql-people.csv").await.unwrap().as_deref(),
        Some(sql_updated_csv.as_slice())
    );
    let sql_update_changes = file_changes(&lix, &sql_file_id)
        .await
        .into_iter()
        .skip(sql_changes_before_update)
        .collect::<Vec<_>>();
    assert!(sql_update_changes.iter().any(|change| {
        change.schema_key == "csv_row"
            && change
                .snapshot_content
                .as_ref()
                .and_then(|snapshot| snapshot.get("cells"))
                .and_then(serde_json::Value::as_array)
                == Some(&vec![
                    serde_json::Value::String("Mina".to_string()),
                    serde_json::Value::String("29".to_string()),
                ])
    }));
    assert!(
        !sql_update_changes
            .iter()
            .any(|change| change.schema_key == "lix_binary_blob_ref")
    );

    let sql_changes_before_delete = sql_changes_before_update + sql_update_changes.len();
    lix.execute(
        "DELETE FROM lix_file WHERE path = $1",
        &[Value::Text("/sql-people.csv".to_string())],
    )
    .await
    .unwrap();
    assert_eq!(lix.read_file("/sql-people.csv").await.unwrap(), None);
    let sql_delete_changes = file_changes(&lix, &sql_file_id)
        .await
        .into_iter()
        .skip(sql_changes_before_delete)
        .collect::<Vec<_>>();
    assert!(
        sql_delete_changes.iter().any(|change| {
            change.schema_key == "csv_table" && change.snapshot_content.is_none()
        })
    );
    assert!(
        sql_delete_changes
            .iter()
            .filter(|change| change.schema_key == "csv_row" && change.snapshot_content.is_none())
            .count()
            >= 2
    );
    let active_plugin_rows_after_delete = lix
        .execute(
            "SELECT schema_key FROM lix_state \
             WHERE file_id = $1 AND schema_key IN ('csv_table', 'csv_row')",
            &[Value::Text(sql_file_id.clone())],
        )
        .await
        .unwrap();
    assert_eq!(active_plugin_rows_after_delete.len(), 0);

    lix.close().await.unwrap();
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

#[derive(Debug, Serialize)]
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

#[tokio::test]
async fn rs_sdk_open_register_write_query_branch_and_merge_flow() {
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    let main_branch_id = lix.active_branch_id().await.unwrap();

    register_crm_task_schema(&lix).await;

    lix.execute(
        "INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
        &[
            Value::Text("task-1".to_string()),
            Value::Text("Draft RS SDK flow".to_string()),
            Value::Boolean(false),
            Value::Text(r#"{"priority":"high","tags":["sdk","json"]}"#.to_string()),
        ],
    )
    .await
    .unwrap();

    let projected = lix
        .execute(
            "SELECT title, done, meta, lixcol_snapshot_content FROM crm_task WHERE id = $1",
            &[Value::Text("task-1".to_string())],
        )
        .await
        .unwrap();
    assert_crm_task_projection(&projected);

    assert!(!task_done(&lix, "task-1").await);

    let draft = lix
        .create_branch(CreateBranchOptions {
            id: Some("draft-branch".to_string()),
            name: "Draft".to_string(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    assert_eq!(draft.id, "draft-branch");
    assert_eq!(draft.name, "Draft");
    assert!(!draft.hidden);

    lix.switch_branch(SwitchBranchOptions {
        branch_id: draft.id.clone(),
    })
    .await
    .unwrap();

    lix.execute(
        "UPDATE crm_task SET done = $1 WHERE id = $2",
        &[Value::Boolean(true), Value::Text("task-1".to_string())],
    )
    .await
    .unwrap();

    assert!(task_done(&lix, "task-1").await);

    lix.switch_branch(SwitchBranchOptions {
        branch_id: main_branch_id.clone(),
    })
    .await
    .unwrap();

    assert!(!task_done(&lix, "task-1").await);

    let merge = lix
        .merge_branch(MergeBranchOptions {
            source_branch_id: draft.id,
        })
        .await
        .unwrap();

    assert_eq!(merge.outcome, MergeBranchOutcome::FastForward);
    assert_eq!(merge.target_branch_id, main_branch_id);
    assert_eq!(merge.change_stats.total, 1);
    assert_eq!(merge.change_stats.modified, 1);
    assert_eq!(merge.created_merge_commit_id, None);
    assert!(task_done(&lix, "task-1").await);

    lix.close().await.unwrap();
}

#[tokio::test]
async fn rs_sdk_close_is_idempotent_and_rejects_later_operations() {
    let lix = open_lix(OpenLixOptions {
        backend: Some(InMemoryBackend::new()),
        ..Default::default()
    })
    .await
    .unwrap();

    lix.close().await.unwrap();
    lix.close().await.unwrap();

    let error = lix
        .execute("SELECT value FROM lix_key_value WHERE key = 'lix_id'", &[])
        .await
        .expect_err("execute after close should fail");
    assert_closed(error);

    let error = lix
        .active_branch_id()
        .await
        .expect_err("active_branch_id after close should fail");
    assert_closed(error);
}

#[tokio::test]
async fn rs_sdk_close_does_not_destroy_committed_data() {
    let backend = InMemoryBackend::new();
    let first = open_lix(OpenLixOptions {
        backend: Some(backend.clone()),
        ..Default::default()
    })
    .await
    .unwrap();

    first
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('close-key', 'close-value')",
            &[],
        )
        .await
        .unwrap();
    first.close().await.unwrap();

    let error = first
        .execute(
            "SELECT value FROM lix_key_value WHERE key = 'close-key'",
            &[],
        )
        .await
        .expect_err("closed handle should not be usable");
    assert_closed(error);

    let second = open_lix(OpenLixOptions {
        backend: Some(backend),
        ..Default::default()
    })
    .await
    .unwrap();
    let result = second
        .execute(
            "SELECT key FROM lix_key_value WHERE key = 'close-key' AND value = lix_json('\"close-value\"')",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Text("close-key".to_string())]
    );
    second.close().await.unwrap();
}

#[tokio::test]
async fn failed_write_validation_does_not_poison_backend_transaction() {
    let lix = open_lix(OpenLixOptions {
        backend: Some(InMemoryBackend::new()),
        ..Default::default()
    })
    .await
    .unwrap();

    register_poison_task_schema(&lix).await;

    let error = lix
        .execute(
            "INSERT INTO poison_task (id, title) VALUES ($1, $2)",
            &[
                Value::Text("bad-task".to_string()),
                Value::Text("missing meta".to_string()),
            ],
        )
        .await
        .expect_err("schema validation should reject missing required field");
    assert_eq!(error.code, "LIX_ERROR_SCHEMA_VALIDATION");

    let result = lix.execute("SELECT 1 AS ok", &[]).await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].values(), &[Value::Integer(1)]);

    lix.execute(
        "INSERT INTO poison_task (id, title, meta) VALUES ($1, $2, lix_json($3))",
        &[
            Value::Text("good-task".to_string()),
            Value::Text("valid".to_string()),
            Value::Text(r#"{"priority":"high"}"#.to_string()),
        ],
    )
    .await
    .expect("valid write after failed write should succeed");

    lix.close().await.unwrap();
}

#[tokio::test]
async fn transaction_commits_multiple_statements_together() {
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    register_crm_task_schema(&lix).await;

    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
        &[
            Value::Text("tx-task-1".to_string()),
            Value::Text("First".to_string()),
            Value::Boolean(false),
            Value::Text(r#"{"batch":1}"#.to_string()),
        ],
    )
    .await
    .unwrap();
    tx.execute(
        "INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
        &[
            Value::Text("tx-task-2".to_string()),
            Value::Text("Second".to_string()),
            Value::Boolean(true),
            Value::Text(r#"{"batch":1}"#.to_string()),
        ],
    )
    .await
    .unwrap();

    let staged = tx
        .execute(
            "SELECT id FROM crm_task WHERE id IN ($1, $2) ORDER BY id",
            &[
                Value::Text("tx-task-1".to_string()),
                Value::Text("tx-task-2".to_string()),
            ],
        )
        .await
        .unwrap();
    assert_eq!(staged.len(), 2);

    tx.commit().await.unwrap();

    let committed = lix
        .execute(
            "SELECT id FROM crm_task WHERE id IN ($1, $2) ORDER BY id",
            &[
                Value::Text("tx-task-1".to_string()),
                Value::Text("tx-task-2".to_string()),
            ],
        )
        .await
        .unwrap();
    assert_eq!(committed.len(), 2);
    lix.close().await.unwrap();
}

#[tokio::test]
async fn transaction_rollback_discards_staged_writes() {
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    register_crm_task_schema(&lix).await;

    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
        &[
            Value::Text("rolled-back-task".to_string()),
            Value::Text("Rollback".to_string()),
            Value::Boolean(false),
            Value::Text(r#"{"batch":1}"#.to_string()),
        ],
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();

    let result = lix
        .execute(
            "SELECT id FROM crm_task WHERE id = $1",
            &[Value::Text("rolled-back-task".to_string())],
        )
        .await
        .unwrap();
    assert_eq!(result.len(), 0);
    lix.close().await.unwrap();
}

#[tokio::test]
async fn transaction_blocks_session_execute_on_same_handle() {
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    register_crm_task_schema(&lix).await;

    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
        &[
            Value::Text("tx-only-task".to_string()),
            Value::Text("Inside tx".to_string()),
            Value::Boolean(false),
            Value::Text(r#"{"batch":1}"#.to_string()),
        ],
    )
    .await
    .unwrap();

    let error = lix
        .execute(
            "INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
            &[
                Value::Text("outside-task".to_string()),
                Value::Text("Outside tx".to_string()),
                Value::Boolean(false),
                Value::Text(r#"{"batch":1}"#.to_string()),
            ],
        )
        .await
        .expect_err("session writes should be blocked while explicit transaction is active");
    assert_eq!(error.code, "LIX_INVALID_TRANSACTION_STATE");

    let error = lix
        .execute("SELECT 1 AS ok", &[])
        .await
        .expect_err("session reads should be blocked while explicit transaction is active");
    assert_eq!(error.code, "LIX_INVALID_TRANSACTION_STATE");

    let tx_read = tx
        .execute("SELECT 1 AS ok", &[])
        .await
        .expect("transaction reads should remain available");
    assert_eq!(tx_read.rows()[0].get::<i64>("ok").unwrap(), 1);

    tx.commit().await.unwrap();

    let committed = lix
        .execute(
            "SELECT id FROM crm_task WHERE id IN ($1, $2) ORDER BY id",
            &[
                Value::Text("outside-task".to_string()),
                Value::Text("tx-only-task".to_string()),
            ],
        )
        .await
        .unwrap();
    assert_eq!(committed.len(), 1);
    assert_eq!(
        committed.rows()[0].values(),
        &[Value::Text("tx-only-task".to_string())]
    );
    lix.close().await.unwrap();
}

fn build_csv_plugin_archive() -> Vec<u8> {
    let wasm_path = Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_WASM_plugin_csv"));
    let wasm = std::fs::read(wasm_path).unwrap_or_else(|error| {
        panic!(
            "failed to read bindep-built CSV plugin wasm at {}: {error}",
            wasm_path.display()
        )
    });
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

async fn register_crm_task_schema(lix: &lix_sdk::Lix) {
    let schema = r#"{
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "x-lix-key": "crm_task",
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "required": ["id", "title", "done", "meta"],
        "properties": {
            "id": { "type": "string" },
            "title": { "type": "string" },
            "done": { "type": "boolean" },
            "meta": { "type": "object" }
        },
        "additionalProperties": false
    }"#;

    lix.execute(
        "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
        &[Value::Text(schema.to_string())],
    )
    .await
    .unwrap();
}

fn assert_crm_task_projection(result: &lix_sdk::ExecuteResult) {
    assert_eq!(result.len(), 1);
    let row = &result.rows()[0];
    assert_eq!(
        row.get::<String>("title").unwrap(),
        "Draft RS SDK flow".to_string()
    );
    assert!(!row.get::<bool>("done").unwrap());

    let meta = row.get::<Value>("meta").unwrap();
    let Value::Json(meta) = meta else {
        panic!("expected meta JSON value, got {meta:?}");
    };
    assert_eq!(
        meta.get("priority").and_then(|value| value.as_str()),
        Some("high")
    );
    assert_eq!(
        meta.get("tags")
            .and_then(|value| value.as_array())
            .map(Vec::len),
        Some(2)
    );

    let snapshot = row.get::<Value>("lixcol_snapshot_content").unwrap();
    let Value::Json(snapshot) = snapshot else {
        panic!("expected snapshot JSON value, got {snapshot:?}");
    };
    assert_eq!(
        snapshot.get("id").and_then(|value| value.as_str()),
        Some("task-1")
    );
    assert_eq!(
        snapshot
            .get("meta")
            .and_then(|value| value.get("priority"))
            .and_then(|value| value.as_str()),
        Some("high")
    );

    let missing = row
        .value("missing")
        .expect_err("missing column should return a structured error");
    assert_eq!(missing.code, "LIX_COLUMN_NOT_FOUND");
}

async fn register_poison_task_schema(lix: &lix_sdk::Lix) {
    let schema = r#"{
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "x-lix-key": "poison_task",
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "required": ["id", "title", "meta"],
        "properties": {
            "id": { "type": "string" },
            "title": { "type": "string" },
            "meta": { "type": "object" }
        },
        "additionalProperties": false
    }"#;

    lix.execute(
        "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
        &[Value::Text(schema.to_string())],
    )
    .await
    .unwrap();
}

async fn task_done(lix: &lix_sdk::Lix, task_id: &str) -> bool {
    let result = lix
        .execute(
            "SELECT done FROM crm_task WHERE id = $1",
            &[Value::Text(task_id.to_string())],
        )
        .await
        .unwrap();

    let rows = result;
    assert_eq!(rows.len(), 1);

    match rows.rows()[0].values().first() {
        Some(Value::Boolean(done)) => *done,
        value => panic!("expected boolean done value, got {value:?}"),
    }
}

fn assert_closed(error: LixError) {
    assert_eq!(error.code, LixError::CODE_CLOSED);
}
