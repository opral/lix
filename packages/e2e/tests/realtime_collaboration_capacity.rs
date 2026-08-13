//! Client-observed real-time collaboration capacity workload.
//!
//! The ignored release-mode benchmark models 50-100 active collaborators on
//! one hot document. Transactions arrive in five-edit waves. Four edits commit
//! concurrently and a fifth same-base marker edit closes the wave. Every live
//! client must observe that marker before the wave has converged.

use std::collections::BTreeMap;
use std::fs;
use std::io::{Cursor, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use lix::ObserveEvents;
use lix::storage::Storage;
use lix::{Lix, LixTransaction, Memory, Value, open_lix};
use lix_collaboration_test_support::{
    CapacityConfig, CollaborationCapacityBackend, WavePlan, run_capacity_workload,
};

const DEFAULT_CLIENTS: usize = 100;
const WAVE_SIZE: usize = 5;
const CONFLICT_WAVE_INTERVAL: usize = 4;
const DEFAULT_ARRIVAL_INTERVAL: Duration = Duration::from_millis(50);
const DEFAULT_GATE: Duration = Duration::from_millis(100);
const OBSERVE_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_SOAK_ROUNDS: usize = 10;
const DEFAULT_RSS_GROWTH_LIMIT_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
enum DocumentFormat {
    Json,
    Csv,
    Markdown,
    Text,
}

impl DocumentFormat {
    fn from_env() -> Self {
        match std::env::var("LIX_COLLAB_FORMAT")
            .unwrap_or_else(|_| "json".to_owned())
            .as_str()
        {
            "json" => Self::Json,
            "csv" => Self::Csv,
            "markdown" | "md" => Self::Markdown,
            "text" | "txt" => Self::Text,
            value => panic!("unsupported LIX_COLLAB_FORMAT {value:?}"),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Csv => "csv",
            Self::Markdown => "markdown",
            Self::Text => "text",
        }
    }

    const fn extension(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Csv => "csv",
            Self::Markdown => "md",
            Self::Text => "txt",
        }
    }

    const fn plugin_key(self) -> &'static str {
        match self {
            Self::Json => "plugin_json",
            Self::Csv => "plugin_csv",
            Self::Markdown => "plugin_markdown",
            Self::Text => "plugin_text",
        }
    }

    fn plugin_archive(self) -> Vec<u8> {
        match self {
            Self::Json => build_json_plugin_archive(),
            Self::Csv => build_csv_plugin_archive(),
            Self::Markdown => build_markdown_plugin_archive(),
            Self::Text => build_text_plugin_archive(),
        }
    }

    fn base_document(self, slots: usize) -> Vec<u8> {
        match self {
            Self::Json => {
                let object = (0..slots)
                    .map(|slot| (format!("k{slot}"), serde_json::Value::String("base".into())))
                    .collect::<serde_json::Map<_, _>>();
                let mut bytes = serde_json::to_vec(&object).expect("serialize JSON base");
                bytes.push(b'\n');
                bytes
            }
            Self::Csv => {
                let mut document = String::from("key,value\n");
                for slot in 0..slots {
                    document.push_str(&format!("k{slot},base\n"));
                }
                document.into_bytes()
            }
            Self::Markdown => {
                let mut document = String::new();
                for slot in 0..slots {
                    document.push_str(&format!("## k{slot}\n\nbase\n\n"));
                }
                document.into_bytes()
            }
            Self::Text => (0..slots)
                .map(|slot| format!("k{slot}=base\n"))
                .collect::<String>()
                .into_bytes(),
        }
    }

    fn edit(self, base: &[u8], slot: usize, token: &str) -> Vec<u8> {
        match self {
            Self::Json => {
                let mut object =
                    serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(base)
                        .expect("parse JSON wave base");
                object.insert(format!("k{slot}"), serde_json::Value::String(token.into()));
                let mut bytes = serde_json::to_vec(&object).expect("serialize JSON edit");
                bytes.push(b'\n');
                bytes
            }
            Self::Csv => replace_line(base, &format!("k{slot},"), &format!("k{slot},{token}")),
            Self::Markdown => replace_markdown_value(base, slot, token),
            Self::Text => replace_line(base, &format!("k{slot}="), &format!("k{slot}={token}")),
        }
    }
}

#[tokio::test]
#[ignore = "manual release-mode 50-100 collaborator capacity gate"]
async fn realtime_collaboration_commit_to_convergence_capacity() {
    let clients = env_usize("LIX_COLLAB_CLIENTS", DEFAULT_CLIENTS);
    let operations = env_usize("LIX_COLLAB_OPERATIONS", clients);
    let gate = Duration::from_millis(env_usize(
        "LIX_COLLAB_GATE_MS",
        usize::try_from(DEFAULT_GATE.as_millis()).expect("gate fits usize"),
    ) as u64);
    let arrival_interval = Duration::from_millis(env_usize(
        "LIX_COLLAB_ARRIVAL_MS",
        usize::try_from(DEFAULT_ARRIVAL_INTERVAL.as_millis()).expect("arrival interval fits usize"),
    ) as u64);
    let format = DocumentFormat::from_env();
    assert!(
        (50..=100).contains(&clients),
        "capacity gate requires 50-100 clients"
    );
    assert!(operations >= WAVE_SIZE && operations.is_multiple_of(WAVE_SIZE));

    let mut backend = LocalCapacityBackend::open(format, clients, operations).await;
    let report = run_capacity_workload(
        &mut backend,
        CapacityConfig {
            clients,
            operations,
            wave_size: WAVE_SIZE,
            conflict_wave_interval: CONFLICT_WAVE_INTERVAL,
            arrival_interval,
            convergence_gate: gate,
        },
    )
    .await;
    report.emit_json();
    backend.close().await;
}

struct LocalCapacityBackend {
    format: DocumentFormat,
    path: String,
    root: Lix<Memory>,
    peers: Vec<Lix<Memory>>,
    observations: Vec<ObserveEvents<Memory>>,
}

impl LocalCapacityBackend {
    async fn open(format: DocumentFormat, clients: usize, operations: usize) -> Self {
        let root = open_lix().await.expect("capacity workspace should open");
        install_plugin(&root, format.plugin_key(), &format.plugin_archive()).await;
        let path = format!("/collaboration-capacity.{}", format.extension());
        write_file(&root, &path, &format.base_document(operations + 1)).await;

        let mut peers = Vec::with_capacity(clients);
        let mut observations = Vec::with_capacity(clients);
        for _ in 0..clients {
            let peer = root
                .open_another_session()
                .await
                .expect("collaborator session should open");
            let mut events = peer
                .observe(
                    "SELECT content FROM lix_file WHERE path = $1",
                    &[Value::Text(path.clone())],
                )
                .expect("collaborator observation should open");
            events
                .next()
                .await
                .expect("initial observation should evaluate")
                .expect("initial observation should stay open");
            peers.push(peer);
            observations.push(events);
        }
        Self {
            format,
            path,
            root,
            peers,
            observations,
        }
    }

    async fn close(mut self) {
        self.observations.clear();
        for peer in self.peers {
            peer.close().await.expect("collaborator should close");
        }
        self.root
            .close()
            .await
            .expect("capacity workspace should close");
    }
}

#[async_trait(?Send)]
impl CollaborationCapacityBackend for LocalCapacityBackend {
    type StagedWave = Vec<LixTransaction<Memory>>;

    fn backend_name(&self) -> &'static str {
        "local"
    }

    fn format_name(&self) -> &'static str {
        self.format.name()
    }

    async fn read_base(&self) -> Vec<u8> {
        read_file(&self.root, &self.path).await
    }

    async fn stage_wave(&mut self, wave: &WavePlan, base: &[u8]) -> Self::StagedWave {
        let mut transactions = Vec::with_capacity(wave.edits.len());
        for edit in &wave.edits {
            let mut transaction = self.peers[edit.operation % self.peers.len()]
                .begin_transaction()
                .await
                .expect("wave transaction should open");
            transaction
                .execute(
                    "UPDATE lix_file SET content = $1 WHERE path = $2",
                    &[
                        Value::Blob(self.format.edit(base, edit.slot, &edit.token).into()),
                        Value::Text(self.path.clone()),
                    ],
                )
                .await
                .expect("wave edit should stage");
            transactions.push(transaction);
        }
        transactions
    }

    async fn commit_wave(&mut self, mut staged: Self::StagedWave) -> Vec<Duration> {
        let marker_transaction = staged
            .pop()
            .expect("wave should contain marker transaction");
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                let mut commit_tasks = tokio::task::JoinSet::new();
                for transaction in staged {
                    commit_tasks.spawn_local(async move {
                        let started = Instant::now();
                        let result = transaction.commit().await;
                        (started.elapsed(), result)
                    });
                }
                let mut services = Vec::with_capacity(WAVE_SIZE);
                while let Some(joined) = commit_tasks.join_next().await {
                    let (elapsed, result) = joined.expect("commit task should not panic");
                    result.expect("concurrent wave edit should commit");
                    services.push(elapsed);
                }
                let marker_started = Instant::now();
                marker_transaction
                    .commit()
                    .await
                    .expect("same-base marker edit should commit");
                services.push(marker_started.elapsed());
                services
            })
            .await
    }

    async fn await_convergence(&mut self, marker: &[u8], wave_started: Instant) -> Vec<Duration> {
        // A Lix session deliberately rejects reads while its explicit
        // transaction is active. Delivery begins as soon as the wave reaches
        // terminal commit states; the clock still starts at scheduled arrival.
        let mut observations = std::mem::take(&mut self.observations);
        let clients = self.peers.len();
        let marker = marker.to_vec();
        let local = tokio::task::LocalSet::new();
        let (next_observations, convergences) = local
            .run_until(async move {
                let mut receipt_events = observations
                    .pop()
                    .expect("capacity workload has an observation");
                let (receipt_generation, receipt_elapsed) = loop {
                    let event = tokio::time::timeout(OBSERVE_TIMEOUT, receipt_events.next())
                        .await
                        .expect("receipt observation timed out")
                        .expect("receipt observation should evaluate")
                        .expect("receipt observation should stay open");
                    let data = event.rows.rows()[0]
                        .get::<Vec<u8>>("content")
                        .expect("receipt file data should be bytes");
                    if data.windows(marker.len()).any(|window| window == marker) {
                        break (event.mutation_sequence, wave_started.elapsed());
                    }
                };
                let mut observer_tasks = tokio::task::JoinSet::new();
                for mut events in observations {
                    observer_tasks.spawn_local(async move {
                        loop {
                            let event = tokio::time::timeout(OBSERVE_TIMEOUT, events.next())
                                .await
                                .expect("observation timed out")
                                .expect("observation should evaluate")
                                .expect("observation should stay open");
                            if event.mutation_sequence >= receipt_generation {
                                return (events, wave_started.elapsed());
                            }
                        }
                    });
                }
                let mut next_observations = Vec::with_capacity(clients);
                let mut convergences = Vec::with_capacity(clients);
                next_observations.push(receipt_events);
                convergences.push(receipt_elapsed);
                while let Some(joined) = observer_tasks.join_next().await {
                    let (events, elapsed) = joined.expect("observer task should not panic");
                    next_observations.push(events);
                    convergences.push(elapsed);
                }
                (next_observations, convergences)
            })
            .await;
        self.observations = next_observations;
        convergences
    }

    async fn assert_final_state(&self, expected_tokens: &[String]) {
        let final_bytes = read_file(&self.root, &self.path).await;
        for token in expected_tokens {
            assert!(
                final_bytes
                    .windows(token.len())
                    .any(|window| window == token.as_bytes()),
                "non-overlapping edit {token} was not retained"
            );
        }
        for peer in &self.peers {
            assert_eq!(read_file(peer, &self.path).await, final_bytes);
        }
    }

    fn resource_counters(&self) -> BTreeMap<String, u64> {
        BTreeMap::from([
            ("open_peer_sessions".to_owned(), self.peers.len() as u64),
            (
                "open_observations".to_owned(),
                self.observations.len() as u64,
            ),
        ])
    }
}

#[tokio::test]
#[ignore = "manual release-mode abandoned transaction and session soak"]
async fn abandoned_transactions_and_sessions_release_resources() {
    let clients = env_usize("LIX_COLLAB_CLIENTS", DEFAULT_CLIENTS);
    let rounds = env_usize("LIX_COLLAB_SOAK_ROUNDS", DEFAULT_SOAK_ROUNDS);
    let rss_limit = env_usize(
        "LIX_COLLAB_RSS_GROWTH_LIMIT_BYTES",
        usize::try_from(DEFAULT_RSS_GROWTH_LIMIT_BYTES).expect("RSS limit fits usize"),
    ) as u64;
    assert!((50..=100).contains(&clients));
    assert!(
        rounds >= 2,
        "soak needs a warmup and at least one measured round"
    );

    let lix = open_lix().await.expect("soak workspace should open");
    install_plugin(&lix, "plugin_json", &build_json_plugin_archive()).await;
    let path = "/abandoned-transaction-soak.json";
    let base = br#"{"value":"base"}
"#;
    write_file(&lix, path, base).await;
    let mut warm_rss = None;
    let mut peak_rss = resident_set_bytes();

    for round in 0..rounds {
        let mut peers = Vec::with_capacity(clients);
        let mut transactions = Vec::with_capacity(clients);
        for client in 0..clients {
            let peer = lix
                .open_another_session()
                .await
                .expect("soak session should open");
            let mut transaction = peer
                .begin_transaction()
                .await
                .expect("soak transaction should open");
            let edit = format!("{{\"value\":\"round-{round}-client-{client}\"}}\n");
            transaction
                .execute(
                    "UPDATE lix_file SET content = $1 WHERE path = $2",
                    &[
                        Value::Blob(edit.into_bytes().into()),
                        Value::Text(path.to_owned()),
                    ],
                )
                .await
                .expect("abandoned edit should stage");
            peers.push(peer);
            transactions.push(transaction);
        }
        peak_rss = peak_rss.max(resident_set_bytes());
        drop(transactions);
        for peer in peers {
            peer.close().await.expect("abandoned session should close");
        }
        tokio::task::yield_now().await;
        assert_eq!(read_file(&lix, path).await, base);
        if round == 0 {
            warm_rss = Some(resident_set_bytes());
        }
    }

    let final_rss = resident_set_bytes();
    let warm_rss = warm_rss.expect("warmup RSS should be captured");
    let growth = final_rss.saturating_sub(warm_rss);
    let abandoned_transactions = clients * rounds;
    println!(
        "{}",
        serde_json::json!({
            "schema": "lix.collaboration-soak.v1",
            "clients_per_round": clients,
            "rounds": rounds,
            "sessions_opened": abandoned_transactions,
            "sessions_closed": abandoned_transactions,
            "transactions_staged": abandoned_transactions,
            "transactions_abandoned": abandoned_transactions,
            "staged_writes_visible": 0,
            "warm_rss_bytes": warm_rss,
            "peak_rss_bytes": peak_rss,
            "final_rss_bytes": final_rss,
            "post_warmup_growth_bytes": growth,
            "growth_limit_bytes": rss_limit,
        })
    );
    assert!(
        growth <= rss_limit,
        "post-warmup RSS grew by {growth} bytes, above {rss_limit} bytes"
    );
    lix.close().await.expect("soak workspace should close");
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .map(|value| {
            value
                .parse()
                .unwrap_or_else(|_| panic!("{key} should be numeric"))
        })
        .unwrap_or(default)
}

fn resident_set_bytes() -> u64 {
    let status = fs::read_to_string("/proc/self/status").expect("Linux procfs should expose RSS");
    let rss_kib = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|value| value.split_ascii_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .expect("VmRSS should be numeric");
    rss_kib * 1024
}

fn replace_line(base: &[u8], prefix: &str, replacement: &str) -> Vec<u8> {
    let source = std::str::from_utf8(base).expect("document should be UTF-8");
    let mut found = false;
    let mut output = String::with_capacity(source.len() + replacement.len());
    for line in source.lines() {
        if line.starts_with(prefix) {
            output.push_str(replacement);
            found = true;
        } else {
            output.push_str(line);
        }
        output.push('\n');
    }
    assert!(found, "slot line {prefix:?} should exist");
    output.into_bytes()
}

fn replace_markdown_value(base: &[u8], slot: usize, token: &str) -> Vec<u8> {
    let source = std::str::from_utf8(base).expect("Markdown should be UTF-8");
    let heading = format!("## k{slot}\n\n");
    let offset = source.find(&heading).expect("Markdown slot should exist") + heading.len();
    let value_end = source[offset..]
        .find('\n')
        .map(|relative| offset + relative)
        .expect("Markdown slot value should end");
    let mut output = String::with_capacity(source.len() + token.len());
    output.push_str(&source[..offset]);
    output.push_str(token);
    output.push_str(&source[value_end..]);
    output.into_bytes()
}

async fn install_plugin<StorageImpl>(lix: &Lix<StorageImpl>, key: &str, archive: &[u8])
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
        &[
            Value::Text(format!("/.lix/plugins/{key}.lixplugin")),
            Value::Blob(archive.to_vec().into()),
        ],
    )
    .await
    .expect("reference plugin should install");
}

async fn write_file<StorageImpl>(lix: &Lix<StorageImpl>, path: &str, bytes: &[u8])
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
        &[
            Value::Text(path.to_owned()),
            Value::Blob(bytes.to_vec().into()),
        ],
    )
    .await
    .expect("capacity document should write");
}

async fn read_file<StorageImpl>(lix: &Lix<StorageImpl>, path: &str) -> Vec<u8>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "SELECT content FROM lix_file WHERE path = $1",
        &[Value::Text(path.to_owned())],
    )
    .await
    .expect("capacity document should read")
    .rows()[0]
        .get::<Vec<u8>>("content")
        .expect("capacity document should contain bytes")
}

fn build_json_plugin_archive() -> Vec<u8> {
    build_plugin_archive(
        Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_JSON_plugin_json")),
        include_str!("../../../plugins/json/manifest.json"),
        &[
            (
                "schema/json_root.json",
                include_str!("../../../plugins/json/schema/json_root.json"),
            ),
            (
                "schema/json_object_member.json",
                include_str!("../../../plugins/json/schema/json_object_member.json"),
            ),
            (
                "schema/json_array_item.json",
                include_str!("../../../plugins/json/schema/json_array_item.json"),
            ),
        ],
    )
}

fn build_csv_plugin_archive() -> Vec<u8> {
    build_plugin_archive(
        Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_plugin_csv")),
        include_str!("../../../plugins/csv/manifest.json"),
        &[
            (
                "schema/csv_table.json",
                include_str!("../../../plugins/csv/schema/csv_table.json"),
            ),
            (
                "schema/csv_row.json",
                include_str!("../../../plugins/csv/schema/csv_row.json"),
            ),
        ],
    )
}

fn build_markdown_plugin_archive() -> Vec<u8> {
    build_plugin_archive(
        Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_MARKDOWN_plugin_markdown")),
        include_str!("../../../plugins/markdown/manifest.json"),
        &[(
            "schema/markdown_node.json",
            include_str!("../../../plugins/markdown/schema/markdown_node.json"),
        )],
    )
}

fn build_text_plugin_archive() -> Vec<u8> {
    build_plugin_archive(
        Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_TEXT_plugin_text")),
        include_str!("../../../plugins/text/manifest.json"),
        &[(
            "schema/text_line.json",
            include_str!("../../../plugins/text/schema/text_line.json"),
        )],
    )
}

fn build_plugin_archive(wasm_path: &Path, manifest: &str, schemas: &[(&str, &str)]) -> Vec<u8> {
    let wasm = fs::read(wasm_path).unwrap_or_else(|error| {
        panic!(
            "failed to read plugin component at {}: {error}",
            wasm_path.display()
        )
    });
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    writer.start_file("manifest.json", options).unwrap();
    writer.write_all(manifest.as_bytes()).unwrap();
    for (path, schema) in schemas {
        writer.start_file(path, options).unwrap();
        writer.write_all(schema.as_bytes()).unwrap();
    }
    writer.start_file("plugin.wasm", options).unwrap();
    writer.write_all(&wasm).unwrap();
    writer.finish().unwrap().into_inner()
}
