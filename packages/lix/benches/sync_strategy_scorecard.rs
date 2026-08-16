//! Deterministic protocol scorecard for the row-first sync architecture.
//!
//! This is intentionally a protocol simulator rather than a second Lix
//! implementation. Every strategy receives the same seeded sequence of
//! semantic row/file writes. The simulator records deterministic wire and
//! storage counters; Criterion measures the runtime of the same trace. The
//! e2e sync tests remain the authority for plugin execution and durable
//! storage behavior.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::hint::black_box;
use std::time::Instant;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use lix::sync::{
    SyncAdmission, SyncCanonicalEvent, SyncFileMutation, SyncRowMutation, SyncTransactionPack,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

const BRANCH: &str = "branch-main";
const SERVER_START: &str = "server-0";

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
enum Strategy {
    TransactionAdmissionCanonicalEvent,
    TransactionAdmissionCommitPack,
    CommitPackOnly,
}

impl Strategy {
    const ALL: [Self; 3] = [
        Self::TransactionAdmissionCanonicalEvent,
        Self::TransactionAdmissionCommitPack,
        Self::CommitPackOnly,
    ];

    fn short_name(self) -> &'static str {
        match self {
            Self::TransactionAdmissionCanonicalEvent => "tx_admit_event_pull",
            Self::TransactionAdmissionCommitPack => "tx_admit_commit_pull",
            Self::CommitPackOnly => "commit_pack_both_ways",
        }
    }
}

impl fmt::Display for Strategy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.short_name())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Shape {
    DirectRow,
    PluginRow,
    FileProjection,
}

impl Shape {
    fn schema_key(self) -> &'static str {
        match self {
            Self::DirectRow => "scorecard.direct_row",
            Self::PluginRow => "scorecard.plugin_row",
            Self::FileProjection => "lix.file_descriptor",
        }
    }
}

#[derive(Clone, Debug)]
struct Scenario {
    name: &'static str,
    seed: u64,
    clients: usize,
    operations: usize,
    shared_keys: usize,
    shape: Shape,
    offline_client: Option<usize>,
    retry_first_admission: bool,
    crash_after_first_admission: bool,
    wrong_branch_write: bool,
}

fn scenarios() -> [Scenario; 7] {
    [
        Scenario {
            name: "disjoint_rows",
            seed: 0x51_1a_7e,
            clients: 2,
            operations: 64,
            shared_keys: 0,
            shape: Shape::DirectRow,
            offline_client: None,
            retry_first_admission: false,
            crash_after_first_admission: false,
            wrong_branch_write: false,
        },
        Scenario {
            name: "hot_row_conflicts",
            seed: 0x51_1a_7f,
            clients: 2,
            operations: 64,
            shared_keys: 4,
            shape: Shape::DirectRow,
            offline_client: None,
            retry_first_admission: false,
            crash_after_first_admission: false,
            wrong_branch_write: false,
        },
        Scenario {
            name: "offline_queue",
            seed: 0x51_1a_80,
            clients: 2,
            operations: 96,
            shared_keys: 8,
            shape: Shape::DirectRow,
            offline_client: Some(1),
            retry_first_admission: true,
            crash_after_first_admission: false,
            wrong_branch_write: false,
        },
        Scenario {
            name: "plugin_rows",
            seed: 0x51_1a_81,
            clients: 2,
            operations: 48,
            shared_keys: 6,
            shape: Shape::PluginRow,
            offline_client: None,
            retry_first_admission: false,
            crash_after_first_admission: false,
            wrong_branch_write: false,
        },
        Scenario {
            name: "filesystem_projection",
            seed: 0x51_1a_82,
            clients: 2,
            operations: 32,
            shared_keys: 4,
            shape: Shape::FileProjection,
            offline_client: None,
            retry_first_admission: false,
            crash_after_first_admission: false,
            wrong_branch_write: false,
        },
        Scenario {
            name: "branch_isolation",
            seed: 0x51_1a_83,
            clients: 2,
            operations: 32,
            shared_keys: 4,
            shape: Shape::DirectRow,
            offline_client: None,
            retry_first_admission: false,
            crash_after_first_admission: false,
            wrong_branch_write: true,
        },
        Scenario {
            name: "crash_after_ack_loss",
            seed: 0x51_1a_84,
            clients: 2,
            operations: 48,
            shared_keys: 6,
            shape: Shape::DirectRow,
            offline_client: None,
            retry_first_admission: false,
            crash_after_first_admission: true,
            wrong_branch_write: false,
        },
    ]
}

#[derive(Clone, Debug)]
struct WriteAction {
    client: usize,
    ordinal: usize,
    row_key: String,
    value: String,
    branch_id: String,
}

#[derive(Clone, Debug)]
enum Action {
    SetOnline { client: usize, online: bool },
    Write(WriteAction),
    Flush { client: usize },
}

#[derive(Clone, Debug, Default, Serialize)]
struct Counters {
    operations: usize,
    accepted_operations: usize,
    rejected_operations: usize,
    duplicate_admissions: usize,
    crash_recoveries: usize,
    stale_base_admissions: usize,
    overlapping_rows: usize,
    fast_forwards: usize,
    divergent_applies: usize,
    overlay_replays: usize,
    overlay_rows_replayed: usize,
    applied_rows: usize,
    applied_files: usize,
    upload_bytes: usize,
    download_bytes: usize,
    server_storage_reads: usize,
    server_storage_writes: usize,
    server_fsyncs: usize,
    client_storage_reads: usize,
    client_storage_writes: usize,
    client_fsyncs: usize,
    #[serde(skip)]
    action_latencies_ns: Vec<u128>,
    #[serde(skip)]
    fast_forward_latencies_ns: Vec<u128>,
    #[serde(skip)]
    divergent_apply_latencies_ns: Vec<u128>,
    elapsed_ns: u128,
}

#[derive(Clone, Debug, Serialize)]
struct ScorecardResult {
    scenario: &'static str,
    strategy: Strategy,
    seed: u64,
    converged: bool,
    branch_isolated: bool,
    duplicate_safe: bool,
    retry_idempotent: bool,
    crash_safe: bool,
    no_lost_disjoint_writes: bool,
    server_operations_per_second: u64,
    catch_up_bytes_per_accepted_operation: usize,
    fast_forward_p95_ns: u128,
    divergent_apply_p95_ns: u128,
    p50_action_ns: u128,
    p95_action_ns: u128,
    counters: Counters,
}

#[derive(Clone, Debug, Serialize)]
struct CommitProposal {
    operation_id: String,
    branch_id: String,
    parent_commit_id: String,
    local_commit_id: String,
    rows: Vec<SyncRowMutation>,
    files: Vec<SyncFileMutation>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CommitPack {
    cursor: u64,
    branch_id: String,
    parent_commit_id: String,
    canonical_commit_id: String,
    operation_id: String,
    rows: Vec<SyncRowMutation>,
    files: Vec<SyncFileMutation>,
}

#[derive(Clone, Debug)]
struct CanonicalRecord {
    event: SyncCanonicalEvent,
    commit: CommitPack,
}

#[derive(Clone, Debug)]
struct Server {
    state: BTreeMap<String, String>,
    files: BTreeMap<String, Vec<u8>>,
    head_commit: String,
    cursor: u64,
    last_modified: HashMap<String, u64>,
    events: Vec<CanonicalRecord>,
    receipts: HashMap<String, SyncAdmission>,
}

#[derive(Clone, Debug)]
struct Replica {
    confirmed: BTreeMap<String, String>,
    visible: BTreeMap<String, String>,
    files: BTreeMap<String, Vec<u8>>,
    confirmed_files: BTreeMap<String, Vec<u8>>,
    confirmed_commit: String,
    cursor: u64,
    pending: VecDeque<SyncTransactionPack>,
    online: bool,
}

#[derive(Clone, Debug)]
struct Simulation {
    strategy: Strategy,
    scenario: Scenario,
    server: Server,
    replicas: Vec<Replica>,
    counters: Counters,
    first_admission: bool,
    crash_recovery_done: bool,
}

fn build_actions(scenario: &Scenario) -> Vec<Action> {
    let mut actions = Vec::with_capacity(scenario.operations + scenario.clients + 2);
    if let Some(client) = scenario.offline_client {
        actions.push(Action::SetOnline {
            client,
            online: false,
        });
    }
    for ordinal in 0..scenario.operations {
        let client = ordinal % scenario.clients;
        let row_key = if scenario.shared_keys == 0 {
            format!("client-{client}-row-{ordinal}")
        } else {
            format!("shared-row-{}", ordinal % scenario.shared_keys)
        };
        let value = format!("seed-{}-client-{client}-write-{ordinal}", scenario.seed);
        let branch_id = if scenario.wrong_branch_write && ordinal == scenario.operations / 2 {
            "branch-other".to_owned()
        } else {
            BRANCH.to_owned()
        };
        actions.push(Action::Write(WriteAction {
            client,
            ordinal,
            row_key,
            value,
            branch_id,
        }));
    }
    if let Some(client) = scenario.offline_client {
        actions.push(Action::SetOnline {
            client,
            online: true,
        });
    }
    for client in 0..scenario.clients {
        actions.push(Action::Flush { client });
    }
    actions
}

impl Simulation {
    fn new(strategy: Strategy, scenario: Scenario) -> Self {
        let seed = BTreeMap::from([(
            format!("{}:seed", scenario.shape.schema_key()),
            "base".into(),
        )]);
        let files = BTreeMap::new();
        let server = Server {
            state: seed.clone(),
            files: files.clone(),
            head_commit: SERVER_START.to_owned(),
            cursor: 0,
            last_modified: HashMap::new(),
            events: Vec::new(),
            receipts: HashMap::new(),
        };
        let replicas = (0..scenario.clients)
            .map(|_| Replica {
                confirmed: seed.clone(),
                visible: seed.clone(),
                files: files.clone(),
                confirmed_files: files.clone(),
                confirmed_commit: SERVER_START.to_owned(),
                cursor: 0,
                pending: VecDeque::new(),
                online: true,
            })
            .collect();
        Self {
            strategy,
            scenario,
            server,
            replicas,
            counters: Counters::default(),
            first_admission: true,
            crash_recovery_done: false,
        }
    }

    fn run(mut self) -> ScorecardResult {
        let started = Instant::now();
        for action in build_actions(&self.scenario) {
            let action_started = Instant::now();
            match action {
                Action::SetOnline { client, online } => self.replicas[client].online = online,
                Action::Write(write) => self.local_write(write),
                Action::Flush { client } => self.flush(client),
            }
            self.counters
                .action_latencies_ns
                .push(action_started.elapsed().as_nanos());
        }
        for client in 0..self.replicas.len() {
            self.replicas[client].online = true;
            self.flush(client);
        }
        self.counters.elapsed_ns = started.elapsed().as_nanos();

        let converged = self.replicas.iter().all(|replica| {
            replica.visible == self.server.state && replica.files == self.server.files
        });
        let branch_isolated = !self
            .server
            .state
            .keys()
            .any(|key| key.contains("branch-other"));
        let duplicate_safe = self.counters.duplicate_admissions <= 1
            && self.server.events.len() == self.counters.accepted_operations;
        let retry_idempotent = !self.scenario.retry_first_admission
            || (self.counters.duplicate_admissions == 1
                && self.server.events.len() == self.counters.accepted_operations);
        let crash_safe = !self.scenario.crash_after_first_admission
            || (self.counters.crash_recoveries == 1
                && self.counters.duplicate_admissions == 1
                && self.server.events.len() == self.counters.accepted_operations);
        let no_lost_disjoint_writes = self.scenario.shared_keys != 0
            || self.server.state.len() >= self.scenario.operations + 1;
        assert!(converged, "{} did not converge", self.strategy);
        assert!(
            branch_isolated,
            "{} crossed a branch boundary",
            self.strategy
        );
        assert!(
            duplicate_safe,
            "{} admitted a duplicate operation",
            self.strategy
        );
        assert!(
            retry_idempotent,
            "{} did not make retry idempotent",
            self.strategy
        );
        assert!(
            crash_safe,
            "{} did not recover a lost acknowledgement",
            self.strategy
        );
        assert!(
            no_lost_disjoint_writes,
            "{} lost a disjoint write",
            self.strategy
        );

        ScorecardResult {
            scenario: self.scenario.name,
            strategy: self.strategy,
            seed: self.scenario.seed,
            converged,
            branch_isolated,
            duplicate_safe,
            retry_idempotent,
            crash_safe,
            no_lost_disjoint_writes,
            server_operations_per_second: if self.counters.elapsed_ns == 0 {
                0
            } else {
                u64::try_from(
                    self.counters.accepted_operations as u128 * 1_000_000_000
                        / self.counters.elapsed_ns,
                )
                .unwrap_or(u64::MAX)
            },
            catch_up_bytes_per_accepted_operation: self
                .counters
                .download_bytes
                .checked_div(self.counters.accepted_operations.max(1))
                .unwrap_or_default(),
            fast_forward_p95_ns: percentile(&self.counters.fast_forward_latencies_ns, 95),
            divergent_apply_p95_ns: percentile(&self.counters.divergent_apply_latencies_ns, 95),
            p50_action_ns: percentile(&self.counters.action_latencies_ns, 50),
            p95_action_ns: percentile(&self.counters.action_latencies_ns, 95),
            counters: self.counters,
        }
    }

    fn local_write(&mut self, write: WriteAction) {
        let replica = &mut self.replicas[write.client];
        let operation_id = format!("client-{}:op-{}", write.client, write.ordinal);
        let local_commit_id = format!("local-{}-{}", write.client, write.ordinal);
        let key = state_key(self.scenario.shape, &write.row_key);
        let row = SyncRowMutation {
            schema_key: self.scenario.shape.schema_key().to_owned(),
            file_id: (self.scenario.shape != Shape::DirectRow)
                .then(|| format!("file-{}", write.row_key)),
            row_pk: json!([write.row_key]),
            snapshot: Some(json!({"id": write.row_key, "value": write.value})),
            metadata: None,
        };
        let files = if self.scenario.shape == Shape::FileProjection {
            vec![SyncFileMutation {
                file_id: format!("file-{}", write.row_key),
                path: Some(format!("/{}.txt", write.row_key)),
                filename: Some(format!("{}.txt", write.row_key)),
                global: false,
                untracked: false,
                content: write.value.as_bytes().to_vec(),
            }]
        } else {
            Vec::new()
        };
        replica.visible.insert(key, write.value);
        for file in &files {
            replica
                .files
                .insert(file.file_id.clone(), file.content.clone());
        }
        replica.pending.push_back(SyncTransactionPack {
            operation_id,
            branch_id: write.branch_id,
            base_server_commit_id: replica.confirmed_commit.clone(),
            local_commit_id,
            rows: vec![row],
            files,
        });
        self.counters.operations += 1;
        self.counters.client_storage_writes += 2;
        self.counters.client_fsyncs += 1;
    }

    fn flush(&mut self, client: usize) {
        if !self.replicas[client].online {
            return;
        }
        while let Some(pack) = self.replicas[client].pending.front().cloned() {
            let admission = self.admit(&pack);
            match admission {
                Ok(receipt) => {
                    if self.scenario.crash_after_first_admission && !self.crash_recovery_done {
                        // Model a process death after the server committed but
                        // before the client persisted the acknowledgement.
                        // The next loop iteration is the restarted client
                        // retrying the same durable pending operation.
                        self.crash_recovery_done = true;
                        self.counters.crash_recoveries += 1;
                        continue;
                    }
                    if self.scenario.retry_first_admission && self.first_admission {
                        self.first_admission = false;
                        let retry = self.admit(&pack).expect("retry should return receipt");
                        assert_eq!(retry, receipt);
                    }
                    self.pull(client);
                    if self.replicas[client]
                        .pending
                        .front()
                        .is_some_and(|pending| pending.operation_id == pack.operation_id)
                    {
                        panic!("canonical receipt did not remove pending operation");
                    }
                }
                Err(()) => {
                    self.replicas[client].pending.pop_front();
                    self.rebuild_visible(client);
                    self.counters.rejected_operations += 1;
                }
            }
        }
        self.pull(client);
    }

    fn admit(&mut self, pack: &SyncTransactionPack) -> Result<SyncAdmission, ()> {
        let request = match self.strategy {
            Strategy::CommitPackOnly => serde_json::to_vec(&CommitProposal {
                operation_id: pack.operation_id.clone(),
                branch_id: pack.branch_id.clone(),
                parent_commit_id: pack.base_server_commit_id.clone(),
                local_commit_id: pack.local_commit_id.clone(),
                rows: pack.rows.clone(),
                files: pack.files.clone(),
            })
            .expect("commit proposal serializes"),
            Strategy::TransactionAdmissionCanonicalEvent
            | Strategy::TransactionAdmissionCommitPack => {
                serde_json::to_vec(pack).expect("transaction pack serializes")
            }
        };
        self.counters.upload_bytes += request.len();
        self.counters.server_storage_reads += 1;
        if pack.branch_id != BRANCH {
            return Err(());
        }
        if let Some(receipt) = self.server.receipts.get(&pack.operation_id).cloned() {
            self.counters.duplicate_admissions += 1;
            self.counters.server_storage_reads += 1;
            return Ok(receipt);
        }
        let base_cursor = commit_cursor(&pack.base_server_commit_id);
        if base_cursor < self.server.cursor {
            self.counters.stale_base_admissions += 1;
            self.counters.overlapping_rows += pack
                .rows
                .iter()
                .filter(|row| {
                    self.server
                        .last_modified
                        .get(&mutation_key(row))
                        .is_some_and(|cursor| *cursor > base_cursor)
                })
                .count();
        }
        for row in &pack.rows {
            let key = mutation_key(row);
            let value = row
                .snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.get("value"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned();
            self.server.state.insert(key.clone(), value);
            self.server
                .last_modified
                .insert(key, self.server.cursor + 1);
        }
        for file in &pack.files {
            self.server
                .files
                .insert(file.file_id.clone(), file.content.clone());
        }
        let parent_commit_id = self.server.head_commit.clone();
        self.server.cursor += 1;
        self.server.head_commit = format!("server-{}", self.server.cursor);
        let receipt = SyncAdmission {
            operation_id: pack.operation_id.clone(),
            branch_id: BRANCH.to_owned(),
            canonical_commit_id: self.server.head_commit.clone(),
            cursor: self.server.cursor,
        };
        let canonical_pack = SyncTransactionPack {
            base_server_commit_id: parent_commit_id.clone(),
            ..pack.clone()
        };
        let event = SyncCanonicalEvent {
            cursor: self.server.cursor,
            canonical_commit_id: self.server.head_commit.clone(),
            pack_fingerprint: String::new(),
            pack: canonical_pack.clone(),
        };
        let commit = CommitPack {
            cursor: self.server.cursor,
            branch_id: BRANCH.to_owned(),
            parent_commit_id,
            canonical_commit_id: self.server.head_commit.clone(),
            operation_id: pack.operation_id.clone(),
            rows: pack.rows.clone(),
            files: pack.files.clone(),
        };
        self.server.events.push(CanonicalRecord { event, commit });
        self.server
            .receipts
            .insert(pack.operation_id.clone(), receipt.clone());
        self.counters.accepted_operations += 1;
        self.counters.server_storage_writes += 3;
        self.counters.server_fsyncs += 1;
        let response = serde_json::to_vec(&receipt).expect("admission response serializes");
        self.counters.download_bytes += response.len();
        Ok(receipt)
    }

    fn pull(&mut self, client: usize) {
        while self.replicas[client].cursor < self.server.cursor {
            let record = self.server.events[self.replicas[client].cursor as usize].clone();
            let response_bytes = match self.strategy {
                Strategy::TransactionAdmissionCanonicalEvent => {
                    serde_json::to_vec(&record.event).expect("canonical event serializes")
                }
                Strategy::TransactionAdmissionCommitPack | Strategy::CommitPackOnly => {
                    serde_json::to_vec(&record.commit).expect("commit pack serializes")
                }
            };
            self.counters.download_bytes += response_bytes.len();
            self.counters.server_storage_reads += 1;
            let replica = &self.replicas[client];
            let fast_forward = match self.strategy {
                Strategy::TransactionAdmissionCanonicalEvent => replica.pending.is_empty(),
                Strategy::TransactionAdmissionCommitPack | Strategy::CommitPackOnly => {
                    replica.confirmed_commit == record.commit.parent_commit_id
                }
            };
            let has_pending = !replica.pending.is_empty();
            if fast_forward {
                self.counters.fast_forwards += 1;
            } else if has_pending {
                self.counters.divergent_applies += 1;
            }
            let apply_started = Instant::now();
            self.apply_record(client, &record);
            let apply_elapsed = apply_started.elapsed().as_nanos();
            if fast_forward {
                self.counters.fast_forward_latencies_ns.push(apply_elapsed);
            } else if has_pending {
                self.counters
                    .divergent_apply_latencies_ns
                    .push(apply_elapsed);
            }
        }
    }

    fn apply_record(&mut self, client: usize, record: &CanonicalRecord) {
        let replica = &mut self.replicas[client];
        for row in &record.event.pack.rows {
            let key = mutation_key(row);
            let value = row
                .snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.get("value"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned();
            replica.confirmed.insert(key, value);
            self.counters.applied_rows += 1;
        }
        for file in &record.event.pack.files {
            replica
                .confirmed_files
                .insert(file.file_id.clone(), file.content.clone());
            self.counters.applied_files += 1;
        }
        replica.cursor = record.event.cursor;
        replica.confirmed_commit = record.event.canonical_commit_id.clone();
        self.counters.client_storage_reads += 1;
        self.counters.client_storage_writes +=
            record.event.pack.rows.len() + record.event.pack.files.len() + 1;
        self.counters.client_fsyncs += 1;
        if let Some(position) = replica
            .pending
            .iter()
            .position(|pending| pending.operation_id == record.event.pack.operation_id)
        {
            replica.pending.remove(position);
        }
        self.rebuild_visible(client);
    }

    fn rebuild_visible(&mut self, client: usize) {
        let replica = &mut self.replicas[client];
        replica.visible = replica.confirmed.clone();
        replica.files = replica.confirmed_files.clone();
        if !replica.pending.is_empty() {
            self.counters.overlay_replays += 1;
            self.counters.overlay_rows_replayed += replica
                .pending
                .iter()
                .map(|pack| pack.rows.len())
                .sum::<usize>();
        }
        for pack in &replica.pending {
            for row in &pack.rows {
                let key = mutation_key(row);
                let value = row
                    .snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.get("value"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                replica.visible.insert(key, value);
            }
            for file in &pack.files {
                replica
                    .files
                    .insert(file.file_id.clone(), file.content.clone());
            }
        }
    }
}

fn state_key(shape: Shape, row_key: &str) -> String {
    format!("{}:{row_key}", shape.schema_key())
}

fn mutation_key(row: &SyncRowMutation) -> String {
    format!("{}:{}", row.schema_key, row.row_pk)
}

fn commit_cursor(commit_id: &str) -> u64 {
    commit_id
        .strip_prefix("server-")
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

fn percentile(values: &[u128], percentile: usize) -> u128 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let index = ((sorted.len() - 1) * percentile / 100).min(sorted.len() - 1);
    sorted[index]
}

fn strategy_scorecard(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_strategy_scorecard");
    for scenario in scenarios() {
        for strategy in Strategy::ALL {
            let result = Simulation::new(strategy, scenario.clone()).run();
            println!(
                "{}",
                serde_json::to_string(&result).expect("scorecard result serializes")
            );
            group.bench_with_input(
                BenchmarkId::new(strategy.short_name(), scenario.name),
                &scenario,
                |benchmark, scenario| {
                    benchmark.iter(|| {
                        black_box(Simulation::new(strategy, scenario.clone()).run());
                    });
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, strategy_scorecard);
criterion_main!(benches);
