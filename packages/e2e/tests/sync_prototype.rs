//! Rust-only prototype for same-branch optimistic synchronization.
//!
//! This deliberately keeps transport in the test harness. Each replica is a
//! real, independent `Lix<Memory>` cloned from the authoritative repository
//! snapshot. Local writes create ordinary Lix commits, then a proposal carries
//! their semantic mutations to a server sequencer. Canonical events update the
//! confirmed replica state and pending local proposals are replayed as an
//! optimistic overlay.

#![recursion_limit = "256"]

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::io::{Cursor, Write as _};
use std::path::Path;
use std::sync::Arc;

use http::{Request, StatusCode, header::CONTENT_TYPE};
use http_body_util::BodyExt as _;
use lix::server_protocol::{
    IDEMPOTENCY_KEY_HEADER, LixServerProtocol, SESSION_ID_HEADER, ServerProtocolBody,
    ServerProtocolContext,
};
use lix::storage::Storage;
use lix::sync::{
    SyncAdmission, SyncCanonicalEvent, SyncPullResponse, SyncTransactionPack, SyncTransport,
    SyncTransportFuture,
};
use lix::{
    CreateBranchOptions, Lix, LixError, Memory, MergeBranchOptions, SwitchBranchOptions, Value,
    open_lix,
};
use lix_storage_filesystem::FilesystemStorage;

const SYNC_ROW_SCHEMA: &str = r#"{
  "$schema": "https://lix.dev/schema-v1.json",
  "key": "prototype_sync_row",
  "columns": [
    { "name": "row_id", "type": "text", "nullable": false },
    { "name": "value", "type": "text", "nullable": false }
  ],
  "primary_key": ["row_id"]
}"#;

#[derive(Clone, Debug, PartialEq, Eq)]
struct RowMutation {
    row_id: String,
    base: Option<String>,
    proposed: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SyncProposal {
    operation_id: String,
    branch_id: String,
    base_server_commit_id: String,
    local_commit_id: String,
    mutations: Vec<RowMutation>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CanonicalEvent {
    sequence: u64,
    operation_id: String,
    branch_id: String,
    base_server_commit_id: String,
    local_commit_id: String,
    canonical_commit_id: String,
    mutations: Vec<RowMutation>,
    rebased: bool,
    overlap_count: usize,
}

#[derive(Debug, PartialEq, Eq)]
enum AdmissionError {
    BranchMismatch { expected: String, actual: String },
    IdempotencyConflict { operation_id: String },
}

struct PrototypeServer {
    lix: Lix<Memory>,
    branch_id: String,
    sequence: u64,
    receipts: HashMap<String, (SyncProposal, CanonicalEvent)>,
    events: Vec<CanonicalEvent>,
}

struct PackAdmissionServer {
    lix: Lix<Memory>,
    branch_id: String,
    known_target_commits: HashSet<String>,
    receipts: HashMap<String, (SyncTransactionPack, String)>,
}

#[derive(Clone)]
struct ProtocolSyncTransport {
    protocol: LixServerProtocol<Memory>,
    session_id: String,
}

struct OfflineSyncTransport;

impl SyncTransport for OfflineSyncTransport {
    fn remote_id(&self) -> &str {
        "in-process://sync-prototype"
    }

    fn admit<'a>(
        &'a self,
        _pack: &'a SyncTransactionPack,
    ) -> SyncTransportFuture<'a, SyncAdmission> {
        Box::pin(async {
            Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "prototype transport is offline",
            ))
        })
    }

    fn pull<'a>(
        &'a self,
        _branch_id: &'a str,
        _after_cursor: u64,
        _limit: usize,
        _schema_keys: &'a [String],
    ) -> SyncTransportFuture<'a, SyncPullResponse> {
        Box::pin(async {
            Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "prototype transport is offline",
            ))
        })
    }
}

struct CollisionSyncTransport {
    response: SyncPullResponse,
}

impl SyncTransport for CollisionSyncTransport {
    fn remote_id(&self) -> &str {
        "in-process://sync-prototype"
    }

    fn admit<'a>(
        &'a self,
        _pack: &'a SyncTransactionPack,
    ) -> SyncTransportFuture<'a, SyncAdmission> {
        Box::pin(async {
            Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "collision test must fail during pull",
            ))
        })
    }

    fn pull<'a>(
        &'a self,
        _branch_id: &'a str,
        _after_cursor: u64,
        _limit: usize,
        _schema_keys: &'a [String],
    ) -> SyncTransportFuture<'a, SyncPullResponse> {
        Box::pin(async { Ok(self.response.clone()) })
    }
}

impl SyncTransport for ProtocolSyncTransport {
    fn remote_id(&self) -> &str {
        "in-process://sync-prototype"
    }

    fn admit<'a>(
        &'a self,
        pack: &'a SyncTransactionPack,
    ) -> SyncTransportFuture<'a, SyncAdmission> {
        Box::pin(async move {
            let response = self
                .protocol
                .handle(
                    Request::builder()
                        .method("POST")
                        .uri("/lix/v1/sync/admit")
                        .header(CONTENT_TYPE, "application/json")
                        .header(SESSION_ID_HEADER, &self.session_id)
                        .header(IDEMPOTENCY_KEY_HEADER, &pack.operation_id)
                        .body(ServerProtocolBody::from(serde_json::to_vec(pack).unwrap()))
                        .unwrap(),
                    ServerProtocolContext::anonymous(),
                )
                .await;
            decode_protocol_json(response, "sync admission").await
        })
    }

    fn pull<'a>(
        &'a self,
        _branch_id: &'a str,
        after_cursor: u64,
        limit: usize,
        schema_keys: &'a [String],
    ) -> SyncTransportFuture<'a, SyncPullResponse> {
        Box::pin(async move {
            let scope = if schema_keys.is_empty() {
                String::new()
            } else {
                format!("&schemas={}", schema_keys.join(","))
            };
            let response = self
                .protocol
                .handle(
                    Request::builder()
                        .uri(format!(
                            "/lix/v1/sync/pull?after={after_cursor}&limit={limit}{scope}"
                        ))
                        .header(SESSION_ID_HEADER, &self.session_id)
                        .body(ServerProtocolBody::empty())
                        .unwrap(),
                    ServerProtocolContext::anonymous(),
                )
                .await;
            decode_protocol_json(response, "sync pull").await
        })
    }
}

async fn decode_protocol_json<T>(
    response: http::Response<ServerProtocolBody>,
    operation: &str,
) -> Result<T, LixError>
where
    T: serde::de::DeserializeOwned,
{
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("collect {operation} response: {error}"),
            )
        })?
        .to_bytes();
    if status != StatusCode::OK {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "{operation} failed with {status}: {}",
                String::from_utf8_lossy(&body)
            ),
        ));
    }
    serde_json::from_slice(&body).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("decode {operation} response: {error}"),
        )
    })
}

impl PackAdmissionServer {
    async fn new(lix: Lix<Memory>, known_target_commits: &[&str]) -> Self {
        let branch_id = lix.active_branch_id().await.unwrap();
        let current = active_commit_id(&lix).await;
        let mut known = known_target_commits
            .iter()
            .map(|commit_id| (*commit_id).to_owned())
            .collect::<HashSet<_>>();
        known.insert(current);
        Self {
            lix,
            branch_id,
            known_target_commits: known,
            receipts: HashMap::new(),
        }
    }

    async fn admit(&mut self, pack: &SyncTransactionPack) -> Result<String, LixError> {
        if pack.branch_id != self.branch_id {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync pack targets another branch",
            ));
        }
        if let Some((accepted, canonical_commit_id)) = self.receipts.get(&pack.operation_id) {
            if accepted != pack {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "sync operation ID was reused with another payload",
                ));
            }
            return Ok(canonical_commit_id.clone());
        }
        if !self
            .known_target_commits
            .contains(&pack.base_server_commit_id)
        {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync pack base is not a known commit of the target branch",
            ));
        }

        // Mutable admission serializes staging and target-branch merge as one
        // authoritative ordering decision in this prototype server.
        let canonical_commit_id = admit_pack_unchecked(&self.lix, pack).await;
        self.known_target_commits
            .insert(canonical_commit_id.clone());
        self.receipts.insert(
            pack.operation_id.clone(),
            (pack.clone(), canonical_commit_id.clone()),
        );
        Ok(canonical_commit_id)
    }
}

impl PrototypeServer {
    async fn admit(&mut self, proposal: SyncProposal) -> Result<CanonicalEvent, AdmissionError> {
        if proposal.branch_id != self.branch_id {
            return Err(AdmissionError::BranchMismatch {
                expected: self.branch_id.clone(),
                actual: proposal.branch_id,
            });
        }
        if let Some((accepted, receipt)) = self.receipts.get(&proposal.operation_id) {
            if accepted != &proposal {
                return Err(AdmissionError::IdempotencyConflict {
                    operation_id: proposal.operation_id,
                });
            }
            return Ok(receipt.clone());
        }

        let accepted = proposal.clone();
        let current_commit_id = active_commit_id(&self.lix).await;
        let rebased = proposal.base_server_commit_id != current_commit_id;
        let current = read_state(&self.lix).await;
        let overlap_count = proposal
            .mutations
            .iter()
            .filter(|mutation| current.get(&mutation.row_id).cloned() != mutation.base)
            .count();

        // The proposal accepted later by the server wins only the rows it
        // contains. All other current rows remain intact.
        apply_mutations(&self.lix, &proposal.mutations).await;
        let canonical_commit_id = active_commit_id(&self.lix).await;
        self.sequence += 1;
        let event = CanonicalEvent {
            sequence: self.sequence,
            operation_id: proposal.operation_id.clone(),
            branch_id: proposal.branch_id.clone(),
            base_server_commit_id: proposal.base_server_commit_id.clone(),
            local_commit_id: proposal.local_commit_id,
            canonical_commit_id,
            mutations: proposal.mutations,
            rebased,
            overlap_count,
        };
        self.receipts
            .insert(proposal.operation_id, (accepted, event.clone()));
        self.events.push(event.clone());
        Ok(event)
    }
}

struct PrototypeReplica {
    client_id: String,
    lix: Lix<Memory>,
    branch_id: String,
    confirmed_commit_id: String,
    confirmed_sequence: u64,
    confirmed: BTreeMap<String, String>,
    pending: VecDeque<SyncProposal>,
    online: bool,
}

impl PrototypeReplica {
    async fn commit(&mut self, writes: &[(&str, Option<&str>)]) -> String {
        let before = read_state(&self.lix).await;
        let mutations = writes
            .iter()
            .map(|(row_id, proposed)| RowMutation {
                row_id: (*row_id).to_owned(),
                base: before.get(*row_id).cloned(),
                proposed: proposed.map(str::to_owned),
            })
            .collect::<Vec<_>>();
        apply_mutations(&self.lix, &mutations).await;
        let local_commit_id = active_commit_id(&self.lix).await;
        // A local commit ID survives replica restarts and is unique per write,
        // unlike an in-memory counter that can be reused after reopening.
        let operation_id = format!("{}:{local_commit_id}", self.client_id);
        self.pending.push_back(SyncProposal {
            operation_id: operation_id.clone(),
            branch_id: self.branch_id.clone(),
            base_server_commit_id: self.confirmed_commit_id.clone(),
            local_commit_id,
            mutations,
        });
        operation_id
    }

    async fn apply_canonical(&mut self, event: &CanonicalEvent) {
        if event.branch_id != self.branch_id {
            return;
        }
        if event.sequence <= self.confirmed_sequence {
            // A retry can receive an old receipt after the canonical cursor was
            // persisted but removal from the durable pending queue was not.
            // Acknowledgement must therefore be idempotent independently of
            // canonical state application.
            self.remove_pending(event);
            return;
        }
        assert_eq!(event.sequence, self.confirmed_sequence + 1);

        apply_to_map(&mut self.confirmed, &event.mutations);
        self.confirmed_sequence = event.sequence;
        self.confirmed_commit_id = event.canonical_commit_id.clone();
        self.remove_pending(event);

        let mut visible = self.confirmed.clone();
        for proposal in &self.pending {
            apply_to_map(&mut visible, &proposal.mutations);
        }
        replace_state(&self.lix, &visible).await;
    }

    fn remove_pending(&mut self, event: &CanonicalEvent) {
        if let Some(position) = self.pending.iter().position(|proposal| {
            proposal.operation_id == event.operation_id
                && proposal.branch_id == event.branch_id
                && proposal.base_server_commit_id == event.base_server_commit_id
                && proposal.local_commit_id == event.local_commit_id
                && proposal.mutations == event.mutations
        }) {
            self.pending.remove(position);
        }
    }
}

struct SyncPrototype {
    server: PrototypeServer,
    replicas: Vec<PrototypeReplica>,
}

impl SyncPrototype {
    async fn open(seed: &[(&str, &str)], clients: usize) -> Self {
        let storage = Memory::new();
        let server_lix = open_lix()
            .with_storage(storage.clone())
            .await
            .expect("prototype server should open");
        register_schema(&server_lix).await;
        replace_state(
            &server_lix,
            &seed
                .iter()
                .map(|(id, value)| ((*id).to_owned(), (*value).to_owned()))
                .collect(),
        )
        .await;
        let branch_id = server_lix
            .active_branch_id()
            .await
            .expect("prototype server branch should resolve");
        let confirmed_commit_id = active_commit_id(&server_lix).await;
        let confirmed = read_state(&server_lix).await;
        let snapshot = storage
            .export_snapshot()
            .expect("prototype bootstrap snapshot should export");

        let mut replicas = Vec::with_capacity(clients);
        for client in 0..clients {
            let lix = open_lix()
                .with_storage(
                    Memory::from_snapshot(&snapshot)
                        .expect("prototype bootstrap snapshot should import"),
                )
                .await
                .expect("prototype replica should open");
            assert_eq!(
                lix.active_branch_id().await.unwrap(),
                branch_id,
                "replicas must retain the authoritative branch identity"
            );
            replicas.push(PrototypeReplica {
                client_id: format!("client-{client}"),
                lix,
                branch_id: branch_id.clone(),
                confirmed_commit_id: confirmed_commit_id.clone(),
                confirmed_sequence: 0,
                confirmed: confirmed.clone(),
                pending: VecDeque::new(),
                online: true,
            });
        }

        Self {
            server: PrototypeServer {
                lix: server_lix,
                branch_id,
                sequence: 0,
                receipts: HashMap::new(),
                events: Vec::new(),
            },
            replicas,
        }
    }

    async fn set_online(&mut self, client: usize, online: bool) {
        self.replicas[client].online = online;
        if online {
            self.catch_up(client).await;
        }
    }

    async fn catch_up(&mut self, client: usize) {
        let after = self.replicas[client].confirmed_sequence;
        let events = self
            .server
            .events
            .iter()
            .filter(|event| event.sequence > after)
            .cloned()
            .collect::<Vec<_>>();
        for event in events {
            self.replicas[client].apply_canonical(&event).await;
        }
    }

    async fn flush(&mut self, client: usize) -> Result<(), AdmissionError> {
        assert!(self.replicas[client].online, "offline flush must not start");
        self.catch_up(client).await;
        while let Some(proposal) = self.replicas[client].pending.front().cloned() {
            let event = self.server.admit(proposal).await?;
            self.broadcast(&event).await;
        }
        Ok(())
    }

    async fn broadcast(&mut self, event: &CanonicalEvent) {
        for replica in &mut self.replicas {
            if replica.online && replica.branch_id == event.branch_id {
                replica.apply_canonical(event).await;
            }
        }
    }

    async fn assert_converged(&self, expected: &[(&str, &str)]) {
        let expected = expected
            .iter()
            .map(|(id, value)| ((*id).to_owned(), (*value).to_owned()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(read_state(&self.server.lix).await, expected);
        for replica in &self.replicas {
            assert!(replica.pending.is_empty());
            assert_eq!(read_state(&replica.lix).await, expected);
            assert_eq!(replica.confirmed, expected);
            assert_eq!(replica.confirmed_sequence, self.server.sequence);
            assert_eq!(
                replica.confirmed_commit_id,
                active_commit_id(&self.server.lix).await
            );
            assert_eq!(replica.branch_id, self.server.branch_id);
        }
    }
}

fn run_sync_test<F, Fut>(name: &str, test: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + 'static,
{
    std::thread::Builder::new()
        .name(name.to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("sync prototype runtime should build")
                .block_on(test());
        })
        .expect("sync prototype thread should spawn")
        .join()
        .expect("sync prototype thread should finish");
}

#[test]
fn same_branch_sync_preserves_disjoint_edits_and_uses_server_order_for_overlap() {
    run_sync_test("same-branch-sync", || async {
        let mut sync = SyncPrototype::open(&[("title", "base"), ("status", "base")], 2).await;

        sync.replicas[0].commit(&[("title", Some("alice"))]).await;
        sync.replicas[1].commit(&[("status", Some("bob"))]).await;
        sync.flush(0).await.unwrap();
        sync.flush(1).await.unwrap();
        sync.assert_converged(&[("status", "bob"), ("title", "alice")])
            .await;
        assert_eq!(sync.server.events[0].overlap_count, 0);
        assert_eq!(sync.server.events[1].overlap_count, 0);
        assert!(!sync.server.events[0].rebased);
        assert!(sync.server.events[1].rebased);

        sync.replicas[0]
            .commit(&[("title", Some("alice-second"))])
            .await;
        sync.replicas[1]
            .commit(&[("title", Some("bob-second"))])
            .await;
        sync.flush(0).await.unwrap();

        // Alice's canonical event must not hide Bob's newer pending overlay.
        assert_eq!(
            read_state(&sync.replicas[1].lix).await["title"],
            "bob-second"
        );

        sync.flush(1).await.unwrap();
        sync.assert_converged(&[("status", "bob"), ("title", "bob-second")])
            .await;
        assert_eq!(sync.server.events[3].overlap_count, 1);
    });
}

#[test]
fn offline_replica_catches_up_then_replays_its_pending_transaction() {
    run_sync_test("offline-sync", || async {
        let mut sync = SyncPrototype::open(
            &[
                ("description", "base"),
                ("status", "base"),
                ("title", "base"),
            ],
            2,
        )
        .await;
        sync.set_online(1, false).await;

        sync.replicas[1]
            .commit(&[
                ("description", Some("offline-description")),
                ("title", Some("offline-title")),
            ])
            .await;
        sync.replicas[0]
            .commit(&[
                ("status", Some("online-status")),
                ("title", Some("online-title")),
            ])
            .await;
        sync.flush(0).await.unwrap();

        assert_eq!(
            read_state(&sync.replicas[1].lix).await["title"],
            "offline-title"
        );
        sync.set_online(1, true).await;
        let caught_up = read_state(&sync.replicas[1].lix).await;
        assert_eq!(caught_up["status"], "online-status");
        assert_eq!(caught_up["title"], "offline-title");

        sync.flush(1).await.unwrap();
        sync.assert_converged(&[
            ("description", "offline-description"),
            ("status", "online-status"),
            ("title", "offline-title"),
        ])
        .await;
        assert_eq!(sync.server.events[1].overlap_count, 1);
    });
}

#[test]
fn retry_is_idempotent_and_multi_row_proposal_is_one_canonical_commit() {
    run_sync_test("idempotent-sync", || async {
        let mut sync = SyncPrototype::open(&[("left", "base"), ("right", "base")], 1).await;
        sync.replicas[0]
            .commit(&[("left", Some("updated")), ("right", None)])
            .await;
        let proposal = sync.replicas[0]
            .pending
            .front()
            .expect("local commit should enqueue a proposal")
            .clone();
        let local_commit_id = proposal.local_commit_id.clone();

        let first = sync.server.admit(proposal.clone()).await.unwrap();
        let retried = sync.server.admit(proposal).await.unwrap();
        assert_eq!(retried, first);
        assert_eq!(sync.server.sequence, 1);
        assert_eq!(sync.server.events.len(), 1);
        assert_eq!(first.local_commit_id, local_commit_id);
        assert_eq!(
            first.canonical_commit_id,
            active_commit_id(&sync.server.lix).await
        );

        sync.broadcast(&first).await;
        sync.assert_converged(&[("left", "updated")]).await;
    });
}

#[test]
fn several_pending_commits_keep_the_latest_overlay_and_flush_in_fifo_order() {
    run_sync_test("pending-fifo-sync", || async {
        let mut sync = SyncPrototype::open(&[("status", "base"), ("title", "base")], 2).await;
        sync.set_online(1, false).await;

        let first_operation = sync.replicas[1]
            .commit(&[("title", Some("offline-one"))])
            .await;
        let second_operation = sync.replicas[1]
            .commit(&[
                ("status", Some("offline-status")),
                ("title", Some("offline-two")),
            ])
            .await;
        sync.replicas[0].commit(&[("title", Some("remote"))]).await;
        sync.flush(0).await.unwrap();

        sync.set_online(1, true).await;
        assert_eq!(sync.replicas[1].pending.len(), 2);
        assert_eq!(
            read_state(&sync.replicas[1].lix).await["title"],
            "offline-two",
            "catch-up must replay every pending proposal over confirmed state"
        );

        sync.flush(1).await.unwrap();
        sync.assert_converged(&[("status", "offline-status"), ("title", "offline-two")])
            .await;
        assert_eq!(sync.server.events[1].operation_id, first_operation);
        assert_eq!(sync.server.events[2].operation_id, second_operation);
        assert_eq!(sync.server.events[1].overlap_count, 1);
        assert_eq!(sync.server.events[2].overlap_count, 0);
    });
}

#[test]
fn duplicate_canonical_delivery_is_ignored_after_ordered_catch_up() {
    run_sync_test("duplicate-delivery-sync", || async {
        let mut sync = SyncPrototype::open(&[("status", "base"), ("title", "base")], 2).await;
        sync.set_online(1, false).await;

        sync.replicas[0].commit(&[("title", Some("first"))]).await;
        sync.flush(0).await.unwrap();
        sync.replicas[0].commit(&[("status", Some("second"))]).await;
        sync.flush(0).await.unwrap();
        let events = sync.server.events.clone();

        sync.set_online(1, true).await;
        let caught_up = read_state(&sync.replicas[1].lix).await;
        let local_commit_id = active_commit_id(&sync.replicas[1].lix).await;
        sync.replicas[1].apply_canonical(&events[0]).await;
        sync.replicas[1].apply_canonical(&events[1]).await;

        assert_eq!(read_state(&sync.replicas[1].lix).await, caught_up);
        assert_eq!(
            active_commit_id(&sync.replicas[1].lix).await,
            local_commit_id,
            "duplicate delivery must not create another local commit"
        );
        assert_eq!(sync.replicas[1].confirmed_sequence, 2);
        sync.assert_converged(&[("status", "second"), ("title", "first")])
            .await;
    });
}

#[test]
fn server_order_resolves_update_delete_conflicts_in_both_directions() {
    run_sync_test("update-delete-sync", || async {
        let mut delete_last = SyncPrototype::open(&[("item", "base")], 2).await;
        delete_last.replicas[0].commit(&[("item", None)]).await;
        delete_last.replicas[1]
            .commit(&[("item", Some("updated"))])
            .await;
        delete_last.flush(1).await.unwrap();
        delete_last.flush(0).await.unwrap();
        delete_last.assert_converged(&[]).await;
        assert_eq!(delete_last.server.events[1].overlap_count, 1);

        let mut update_last = SyncPrototype::open(&[("item", "base")], 2).await;
        update_last.replicas[0].commit(&[("item", None)]).await;
        update_last.replicas[1]
            .commit(&[("item", Some("updated"))])
            .await;
        update_last.flush(0).await.unwrap();
        update_last.flush(1).await.unwrap();
        update_last.assert_converged(&[("item", "updated")]).await;
        assert_eq!(update_last.server.events[1].overlap_count, 1);
    });
}

#[test]
fn proposal_for_another_branch_is_rejected_before_idempotency_lookup() {
    run_sync_test("branch-isolation-sync", || async {
        let mut sync = SyncPrototype::open(&[("title", "base")], 1).await;
        sync.replicas[0].commit(&[("title", Some("local"))]).await;
        let proposal = sync.replicas[0].pending.front().unwrap().clone();
        sync.server.admit(proposal.clone()).await.unwrap();
        let mut wrong_branch_retry = proposal;
        wrong_branch_retry.branch_id = "another-branch".to_owned();

        let error = sync.server.admit(wrong_branch_retry).await.unwrap_err();
        assert_eq!(
            error,
            AdmissionError::BranchMismatch {
                expected: sync.server.branch_id.clone(),
                actual: "another-branch".to_owned(),
            }
        );
        assert_eq!(sync.server.sequence, 1);
        assert_eq!(sync.server.events.len(), 1);
        assert_eq!(read_state(&sync.server.lix).await["title"], "local");
        assert_eq!(sync.replicas[0].pending.len(), 1);
    });
}

#[test]
fn lost_ack_and_partially_persisted_ack_do_not_duplicate_the_canonical_commit() {
    run_sync_test("lost-ack-sync", || async {
        let mut sync = SyncPrototype::open(&[("title", "base")], 1).await;
        sync.replicas[0].commit(&[("title", Some("local"))]).await;
        let proposal = sync.replicas[0].pending.front().unwrap().clone();

        // The server commits, but the response and broadcast are lost.
        let receipt = sync.server.admit(proposal.clone()).await.unwrap();
        assert_eq!(sync.replicas[0].pending.len(), 1);
        sync.flush(0).await.unwrap();
        assert!(sync.replicas[0].pending.is_empty());
        assert_eq!(sync.server.sequence, 1);
        assert_eq!(sync.server.events.len(), 1);

        // Simulate a crash after persisting the canonical cursor but before
        // durably removing the acknowledged proposal.
        sync.replicas[0].pending.push_back(proposal);
        sync.flush(0).await.unwrap();
        assert!(sync.replicas[0].pending.is_empty());
        assert_eq!(sync.server.sequence, 1);
        assert_eq!(sync.server.events, vec![receipt]);
        sync.assert_converged(&[("title", "local")]).await;
    });
}

#[test]
fn reused_operation_id_with_different_payload_is_rejected() {
    run_sync_test("idempotency-conflict-sync", || async {
        let mut sync = SyncPrototype::open(&[("title", "base")], 1).await;
        sync.replicas[0]
            .commit(&[("title", Some("accepted"))])
            .await;
        let proposal = sync.replicas[0].pending.front().unwrap().clone();
        sync.server.admit(proposal.clone()).await.unwrap();

        let mut conflicting_retry = proposal;
        conflicting_retry.mutations[0].proposed = Some("different".to_owned());
        let error = sync.server.admit(conflicting_retry).await.unwrap_err();
        assert_eq!(
            error,
            AdmissionError::IdempotencyConflict {
                operation_id: sync.server.events[0].operation_id.clone(),
            }
        );
        assert_eq!(sync.server.sequence, 1);
        assert_eq!(sync.server.events.len(), 1);
        assert_eq!(read_state(&sync.server.lix).await["title"], "accepted");
    });
}

#[test]
fn canonical_ack_only_removes_the_exact_pending_proposal() {
    run_sync_test("ack-fingerprint-sync", || async {
        let mut sync = SyncPrototype::open(&[("title", "base")], 2).await;
        sync.replicas[0]
            .commit(&[("title", Some("accepted"))])
            .await;
        sync.replicas[1]
            .commit(&[("title", Some("conflicting"))])
            .await;
        let accepted = sync.replicas[0].pending.front().unwrap().clone();

        // Simulate a reused client identity/idempotency key. Delivery of the
        // first proposal's event must not acknowledge the second payload.
        sync.replicas[1].pending[0].operation_id = accepted.operation_id.clone();
        let event = sync.server.admit(accepted).await.unwrap();
        sync.broadcast(&event).await;

        assert_eq!(sync.replicas[1].pending.len(), 1);
        assert_eq!(
            read_state(&sync.replicas[1].lix).await["title"],
            "conflicting"
        );
        let error = sync.flush(1).await.unwrap_err();
        assert_eq!(
            error,
            AdmissionError::IdempotencyConflict {
                operation_id: event.operation_id,
            }
        );
        assert_eq!(sync.replicas[1].pending.len(), 1);
        assert_eq!(sync.server.sequence, 1);
    });
}

#[test]
fn misrouted_canonical_event_does_not_cross_branch_boundary() {
    run_sync_test("event-branch-isolation-sync", || async {
        let mut sync = SyncPrototype::open(&[("title", "base")], 1).await;
        sync.replicas[0].commit(&[("title", Some("local"))]).await;
        let proposal = sync.replicas[0].pending.front().unwrap().clone();
        let event = sync.server.admit(proposal).await.unwrap();
        let mut misrouted = event.clone();
        misrouted.branch_id = "another-branch".to_owned();

        sync.broadcast(&misrouted).await;
        assert_eq!(sync.replicas[0].confirmed_sequence, 0);
        assert_eq!(sync.replicas[0].pending.len(), 1);
        assert_eq!(read_state(&sync.replicas[0].lix).await["title"], "local");

        sync.broadcast(&event).await;
        sync.assert_converged(&[("title", "local")]).await;
    });
}

#[test]
fn transaction_packs_sync_direct_rows_without_file_payloads() {
    run_sync_test("direct-row-transaction-packs", || async {
        let seed_storage = Memory::new();
        let seed = open_lix().with_storage(seed_storage.clone()).await.unwrap();
        register_schema(&seed).await;
        apply_mutations(
            &seed,
            &[
                RowMutation {
                    row_id: "left".to_owned(),
                    base: None,
                    proposed: Some("base".to_owned()),
                },
                RowMutation {
                    row_id: "right".to_owned(),
                    base: None,
                    proposed: Some("base".to_owned()),
                },
                RowMutation {
                    row_id: "obsolete".to_owned(),
                    base: None,
                    proposed: Some("base".to_owned()),
                },
            ],
        )
        .await;
        let base = active_commit_id(&seed).await;
        let snapshot = seed_storage.export_snapshot().unwrap();
        let mut server = PackAdmissionServer::new(open_snapshot(&snapshot).await, &[&base]).await;
        let alice = open_snapshot(&snapshot).await;
        let bob = open_snapshot(&snapshot).await;
        let observer = open_snapshot(&snapshot).await;

        alice
            .execute(
                "UPDATE prototype_sync_row \
                 SET value = 'alice', lixcol_metadata = CAST($1 AS JSONB) \
                 WHERE row_id = 'left'",
                &[Value::Text(r#"{"source":"alice"}"#.to_owned())],
            )
            .await
            .unwrap();
        let mut bob_transaction = bob.begin_transaction().await.unwrap();
        bob_transaction
            .execute(
                "UPDATE prototype_sync_row SET value = 'bob' WHERE row_id = 'right'",
                &[],
            )
            .await
            .unwrap();
        bob_transaction
            .execute(
                "DELETE FROM prototype_sync_row WHERE row_id = 'obsolete'",
                &[],
            )
            .await
            .unwrap();
        bob_transaction.commit().await.unwrap();
        let alice_pack =
            semantic_pack(&alice, "alice", &base, &base, &["prototype_sync_row"]).await;
        let bob_pack = semantic_pack(&bob, "bob", &base, &base, &["prototype_sync_row"]).await;

        assert_eq!(alice_pack.rows.len(), 1);
        assert_eq!(
            alice_pack.rows[0].metadata,
            Some(serde_json::json!({"source": "alice"}))
        );
        assert_eq!(bob_pack.rows.len(), 2);
        assert!(
            bob_pack
                .rows
                .iter()
                .any(|row| row.row_pk == serde_json::json!(["obsolete"]) && row.snapshot.is_none())
        );
        assert!(alice_pack.rows.iter().all(|row| row.file_id.is_none()));
        let first_server_head = server.admit(&alice_pack).await.unwrap();
        let first_canonical = server
            .lix
            .create_sync_transaction_pack(
                "canonical-first",
                &base,
                &base,
                &first_server_head,
                &["prototype_sync_row"],
            )
            .await
            .unwrap();
        observer
            .apply_sync_transaction_pack(&first_canonical)
            .await
            .unwrap();

        let final_head = server.admit(&bob_pack).await.unwrap();
        let second_canonical = server
            .lix
            .create_sync_transaction_pack(
                "canonical-second",
                &first_server_head,
                &first_server_head,
                &final_head,
                &["prototype_sync_row"],
            )
            .await
            .unwrap();
        observer
            .apply_sync_transaction_pack(&second_canonical)
            .await
            .unwrap();
        assert_eq!(
            read_state(&server.lix).await,
            BTreeMap::from([
                ("left".to_owned(), "alice".to_owned()),
                ("right".to_owned(), "bob".to_owned()),
            ])
        );
        assert_eq!(read_state(&observer).await, read_state(&server.lix).await);
        let metadata = observer
            .execute(
                "SELECT lixcol_metadata FROM prototype_sync_row WHERE row_id = 'left'",
                &[],
            )
            .await
            .unwrap()
            .rows()[0]
            .get::<serde_json::Value>("lixcol_metadata")
            .unwrap();
        assert_eq!(metadata, serde_json::json!({"source": "alice"}));

        for lix in [&observer, &bob, &alice, &server.lix, &seed] {
            lix.close().await.unwrap();
        }
    });
}

#[test]
fn server_endpoint_uses_admission_order_for_the_same_plugin_row() {
    run_sync_test("plugin-row-server-order", || async {
        const ID: &str = "0198b7a1-0000-7000-8000-000000000001";
        let seed_storage = Memory::new();
        let seed = open_lix().with_storage(seed_storage.clone()).await.unwrap();
        install_plugin_archive(&seed, "plugin_conversation", &conversation_plugin_archive()).await;
        seed.execute(
            "INSERT INTO conversation (id, title, body) VALUES ($1, 'Planning', $2)",
            &[
                Value::Text(ID.to_owned()),
                Value::Text("Alice said hello.\n\nBob said goodbye.".to_owned()),
            ],
        )
        .await
        .unwrap();
        let base = active_commit_id(&seed).await;
        let snapshot = seed_storage.export_snapshot().unwrap();
        let server = Arc::new(open_snapshot(&snapshot).await);
        let protocol = LixServerProtocol::new(Arc::clone(&server));
        let session_id = open_protocol_session(&protocol).await;
        let alice = open_snapshot(&snapshot).await;
        let bob = open_snapshot(&snapshot).await;

        alice
            .execute(
                "UPDATE conversation SET body = $1 WHERE id = $2",
                &[
                    Value::Text("Alice said HELLO.\n\nBob said goodbye.".to_owned()),
                    Value::Text(ID.to_owned()),
                ],
            )
            .await
            .unwrap();
        bob.execute(
            "UPDATE conversation SET body = $1 WHERE id = $2",
            &[
                Value::Text("Alice said hello.\n\nBob said GOODBYE.".to_owned()),
                Value::Text(ID.to_owned()),
            ],
        )
        .await
        .unwrap();
        let alice_pack = semantic_pack(&alice, "alice", &base, &base, &["conversation"]).await;
        let bob_pack = semantic_pack(&bob, "bob", &base, &base, &["conversation"]).await;
        assert_eq!(alice_pack.rows.len(), 1);
        assert_eq!(bob_pack.rows.len(), 1);

        admit_pack_via_protocol(&protocol, &session_id, &alice_pack).await;
        admit_pack_via_protocol(&protocol, &session_id, &bob_pack).await;
        let body = server
            .execute(
                "SELECT body FROM conversation WHERE id = $1",
                &[Value::Text(ID.to_owned())],
            )
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("body")
            .unwrap();
        assert_eq!(body, "Alice said hello.\n\nBob said GOODBYE.");

        protocol.close().await.unwrap();
        for lix in [&bob, &alice, &seed] {
            lix.close().await.unwrap();
        }
    });
}

#[test]
fn sync_client_flushes_two_filesystem_replicas_and_restores_a_durable_queue() {
    run_sync_test("sync-client-direct-rows", || async {
        let seed_storage = Memory::new();
        let seed = open_lix().with_storage(seed_storage.clone()).await.unwrap();
        register_schema(&seed).await;
        apply_mutations(
            &seed,
            &[
                RowMutation {
                    row_id: "title".to_owned(),
                    base: None,
                    proposed: Some("base".to_owned()),
                },
                RowMutation {
                    row_id: "status".to_owned(),
                    base: None,
                    proposed: Some("base".to_owned()),
                },
            ],
        )
        .await;
        let server = Arc::new(open_snapshot(&seed_storage.export_snapshot().unwrap()).await);
        let protocol = LixServerProtocol::new(Arc::clone(&server));
        let branch_id = server.active_branch_id().await.unwrap();
        let project_dirs = tempfile::tempdir().unwrap();
        let alice_path = project_dirs.path().join("alice");
        let bob_path = project_dirs.path().join("bob");
        let (alice, _alice_storage) = open_filesystem_replica(&alice_path, &branch_id).await;
        let (bob, bob_storage) = open_filesystem_replica(&bob_path, &branch_id).await;
        register_schema(&alice).await;
        register_schema(&bob).await;
        let initial_rows = [
            RowMutation {
                row_id: "title".to_owned(),
                base: None,
                proposed: Some("base".to_owned()),
            },
            RowMutation {
                row_id: "status".to_owned(),
                base: None,
                proposed: Some("base".to_owned()),
            },
        ];
        apply_mutations(&alice, &initial_rows).await;
        apply_mutations(&bob, &initial_rows).await;
        let alice_transport = ProtocolSyncTransport {
            session_id: open_protocol_session(&protocol).await,
            protocol: protocol.clone(),
        };
        let bob_transport = ProtocolSyncTransport {
            session_id: open_protocol_session(&protocol).await,
            protocol: protocol.clone(),
        };
        let mut alice_sync = alice.sync(alice_transport).await.unwrap();
        let mut bob_sync = bob.sync(bob_transport.clone()).await.unwrap();
        alice_sync.flush().await.unwrap();
        bob_sync.flush().await.unwrap();
        drop(bob_sync);
        let alice_before = active_commit_id(&alice).await;
        let bob_before = active_commit_id(&bob).await;

        alice
            .execute(
                "UPDATE prototype_sync_row SET value = 'alice' WHERE row_id = 'title'",
                &[],
            )
            .await
            .unwrap();
        let mut bob_transaction = bob.begin_transaction().await.unwrap();
        bob_transaction
            .execute(
                "UPDATE prototype_sync_row SET value = 'bob-title' WHERE row_id = 'title'",
                &[],
            )
            .await
            .unwrap();
        bob_transaction
            .execute(
                "UPDATE prototype_sync_row SET value = 'bob-status' WHERE row_id = 'status'",
                &[],
            )
            .await
            .unwrap();
        bob_transaction.commit().await.unwrap();
        let alice_after = active_commit_id(&alice).await;
        let bob_after = active_commit_id(&bob).await;
        assert!(
            alice_sync
                .enqueue_transaction(
                    "alice-direct-row",
                    &alice_before,
                    &alice_after,
                    &["prototype_sync_row"],
                )
                .await
                .unwrap()
        );
        let mut bob_sync = bob.sync(OfflineSyncTransport).await.unwrap();
        assert!(
            bob_sync
                .enqueue_transaction(
                    "bob-direct-row",
                    &bob_before,
                    &bob_after,
                    &["prototype_sync_row"],
                )
                .await
                .unwrap()
        );
        let simulated_canonical = alice
            .create_sync_transaction_pack(
                "simulated-canonical-before-crash",
                "server-base-not-used-for-local-apply",
                &alice_before,
                &alice_after,
                &["prototype_sync_row"],
            )
            .await
            .unwrap();
        bob.apply_sync_transaction_pack(&simulated_canonical)
            .await
            .unwrap();
        assert_eq!(read_state(&bob).await["title"], "alice");

        drop(bob_sync);
        bob.close().await.unwrap();
        drop(bob);
        drop(bob_storage);
        let bob_storage = FilesystemStorage::new(&bob_path).open().unwrap();
        let bob = open_lix().with_storage(bob_storage.clone()).await.unwrap();
        bob.switch_branch(SwitchBranchOptions {
            branch_id: branch_id.clone(),
        })
        .await
        .unwrap();
        let bob_offline = bob.sync(OfflineSyncTransport).await.unwrap();
        assert_eq!(bob_offline.pending_operations(), 1);
        assert_eq!(
            read_state(&bob).await["title"],
            "bob-title",
            "opening sync must restore the pending overlay without network access"
        );
        drop(bob_offline);
        let mut bob_sync = bob.sync(bob_transport).await.unwrap();
        assert_eq!(bob_sync.pending_operations(), 1);
        assert_eq!(alice_sync.flush().await.unwrap().pending_operations, 0);
        assert_eq!(bob_sync.flush().await.unwrap().pending_operations, 0);
        alice_sync.flush().await.unwrap();

        let expected = BTreeMap::from([
            ("status".to_owned(), "bob-status".to_owned()),
            ("title".to_owned(), "bob-title".to_owned()),
        ]);
        assert_eq!(read_state(&server).await, expected);
        assert_eq!(read_state(&alice).await, expected);
        assert_eq!(read_state(&bob).await, expected);
        assert_eq!(alice_sync.cursor(), 2);
        assert_eq!(bob_sync.cursor(), 2);

        protocol.close().await.unwrap();
        bob.close().await.unwrap();
        alice.close().await.unwrap();
        seed.close().await.unwrap();
    });
}

#[test]
fn sync_client_does_not_acknowledge_an_operation_id_with_another_payload() {
    run_sync_test("sync-client-operation-collision", || async {
        let storage = Memory::new();
        let lix = open_lix().with_storage(storage).await.unwrap();
        register_schema(&lix).await;
        apply_mutations(
            &lix,
            &[RowMutation {
                row_id: "shared".to_owned(),
                base: None,
                proposed: Some("base".to_owned()),
            }],
        )
        .await;
        let branch_id = lix.active_branch_id().await.unwrap();
        let base = active_commit_id(&lix).await;
        let bootstrap = SyncPullResponse {
            branch_id: branch_id.clone(),
            events: Vec::new(),
            next_cursor: 0,
            head_cursor: 0,
            head_commit_id: base.clone(),
        };
        let mut sync = lix
            .sync(CollisionSyncTransport {
                response: bootstrap,
            })
            .await
            .unwrap();
        sync.flush().await.unwrap();
        drop(sync);

        lix.execute(
            "UPDATE prototype_sync_row SET value = 'mine' WHERE row_id = 'shared'",
            &[],
        )
        .await
        .unwrap();
        let local_head = active_commit_id(&lix).await;
        let mut sync = lix.sync(OfflineSyncTransport).await.unwrap();
        sync.enqueue_transaction(
            "colliding-operation",
            &base,
            &local_head,
            &["prototype_sync_row"],
        )
        .await
        .unwrap();
        drop(sync);

        let mut other_pack = lix
            .create_sync_transaction_pack(
                "colliding-operation",
                &base,
                &base,
                &local_head,
                &["prototype_sync_row"],
            )
            .await
            .unwrap();
        other_pack.rows[0].snapshot = Some(serde_json::json!({
            "row_id": "shared",
            "value": "someone-else"
        }));
        let response = SyncPullResponse {
            branch_id,
            events: vec![SyncCanonicalEvent {
                cursor: 1,
                canonical_commit_id: local_head.clone(),
                parent_commit_ids: Vec::new(),
                pack_fingerprint: String::new(),
                pack: other_pack,
            }],
            next_cursor: 1,
            head_cursor: 1,
            head_commit_id: local_head,
        };
        let mut sync = lix.sync(CollisionSyncTransport { response }).await.unwrap();
        let error = sync.flush().await.unwrap_err();
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert_eq!(sync.pending_operations(), 1);
        assert_eq!(read_state(&lix).await["shared"], "mine");
        lix.close().await.unwrap();
    });
}

#[test]
fn sync_client_composes_plugin_rows_and_renders_each_local_file() {
    run_sync_test("sync-client-plugin-rows", || async {
        let seed_storage = Memory::new();
        let seed = open_lix().with_storage(seed_storage.clone()).await.unwrap();
        install_plugin_archive(&seed, "plugin_markdown", &markdown_plugin_archive()).await;
        write_file(
            &seed,
            "/row-first-client.md",
            b"First paragraph.\n\nSecond paragraph.\n",
        )
        .await;
        let paragraphs = seed
            .execute(
                "SELECT id, payload_json FROM markdown_node WHERE kind = 'paragraph'",
                &[],
            )
            .await
            .unwrap();
        let paragraph_id = |needle: &str| {
            paragraphs
                .rows()
                .iter()
                .find(|row| row.get::<String>("payload_json").unwrap().contains(needle))
                .unwrap()
                .get::<String>("id")
                .unwrap()
        };
        let first_id = paragraph_id("First paragraph.");
        let second_id = paragraph_id("Second paragraph.");
        let snapshot = seed_storage.export_snapshot().unwrap();
        let server = Arc::new(open_snapshot(&snapshot).await);
        let protocol = LixServerProtocol::new(Arc::clone(&server));
        let alice = open_snapshot(&snapshot).await;
        let bob = open_snapshot(&snapshot).await;
        let alice_before = active_commit_id(&alice).await;
        let bob_before = active_commit_id(&bob).await;
        update_markdown_paragraph(&alice, &first_id, "First from Alice.").await;
        update_markdown_paragraph(&bob, &second_id, "Second from Bob.").await;
        let alice_after = active_commit_id(&alice).await;
        let bob_after = active_commit_id(&bob).await;
        let mut alice_sync = alice
            .sync(ProtocolSyncTransport {
                session_id: open_protocol_session(&protocol).await,
                protocol: protocol.clone(),
            })
            .await
            .unwrap();
        let mut bob_sync = bob
            .sync(ProtocolSyncTransport {
                session_id: open_protocol_session(&protocol).await,
                protocol: protocol.clone(),
            })
            .await
            .unwrap();
        alice_sync
            .enqueue_transaction(
                "alice-plugin-row",
                &alice_before,
                &alice_after,
                &["markdown_node"],
            )
            .await
            .unwrap();
        bob_sync
            .enqueue_transaction(
                "bob-plugin-row",
                &bob_before,
                &bob_after,
                &["markdown_node"],
            )
            .await
            .unwrap();
        alice_sync.flush().await.unwrap();
        bob_sync.flush().await.unwrap();
        alice_sync.flush().await.unwrap();

        let expected = b"First from Alice.\n\nSecond from Bob.\n";
        assert_eq!(read_file(&server, "/row-first-client.md").await, expected);
        assert_eq!(read_file(&alice, "/row-first-client.md").await, expected);
        assert_eq!(read_file(&bob, "/row-first-client.md").await, expected);

        protocol.close().await.unwrap();
        for lix in [&bob, &alice, &seed] {
            lix.close().await.unwrap();
        }
    });
}

#[test]
fn plugin_row_packs_render_file_bytes_as_a_secondary_effect() {
    run_sync_test("plugin-file-render-transaction-packs", || async {
        let seed_storage = Memory::new();
        let seed = open_lix().with_storage(seed_storage.clone()).await.unwrap();
        install_plugin_archive(&seed, "plugin_markdown", &markdown_plugin_archive()).await;
        write_file(
            &seed,
            "/row-first.md",
            b"First paragraph.\n\nSecond paragraph.\n",
        )
        .await;
        let paragraphs = seed
            .execute(
                "SELECT id, payload_json FROM markdown_node WHERE kind = 'paragraph'",
                &[],
            )
            .await
            .unwrap();
        let paragraph_id = |needle: &str| {
            paragraphs
                .rows()
                .iter()
                .find(|row| row.get::<String>("payload_json").unwrap().contains(needle))
                .unwrap()
                .get::<String>("id")
                .unwrap()
        };
        let first_id = paragraph_id("First paragraph.");
        let second_id = paragraph_id("Second paragraph.");
        let base = active_commit_id(&seed).await;
        let snapshot = seed_storage.export_snapshot().unwrap();
        let server = Arc::new(open_snapshot(&snapshot).await);
        let protocol = LixServerProtocol::new(Arc::clone(&server));
        let session_id = open_protocol_session(&protocol).await;
        let alice = open_snapshot(&snapshot).await;
        let bob = open_snapshot(&snapshot).await;
        let observer = open_snapshot(&snapshot).await;

        update_markdown_paragraph(&alice, &first_id, "First from Alice.").await;
        update_markdown_paragraph(&bob, &second_id, "Second from Bob.").await;
        let alice_pack = semantic_pack(&alice, "alice", &base, &base, &["markdown_node"]).await;
        let bob_pack = semantic_pack(&bob, "bob", &base, &base, &["markdown_node"]).await;
        assert!(
            alice_pack
                .rows
                .iter()
                .all(|row| row.schema_key == "markdown_node")
        );
        assert!(
            bob_pack
                .rows
                .iter()
                .all(|row| row.schema_key == "markdown_node")
        );
        assert!(alice_pack.rows.iter().all(|row| row.file_id.is_some()));

        admit_pack_via_protocol(&protocol, &session_id, &alice_pack).await;
        admit_pack_via_protocol(&protocol, &session_id, &bob_pack).await;
        let expected = b"First from Alice.\n\nSecond from Bob.\n";
        assert_eq!(read_file(&server, "/row-first.md").await, expected);

        let final_head = active_commit_id(&server).await;
        let canonical = server
            .create_sync_transaction_pack(
                "canonical",
                &base,
                &base,
                &final_head,
                &["markdown_node"],
            )
            .await
            .unwrap();
        observer
            .apply_sync_transaction_pack(&canonical)
            .await
            .unwrap();
        assert_eq!(read_file(&observer, "/row-first.md").await, expected);

        protocol.close().await.unwrap();
        for lix in [&observer, &bob, &alice, &seed] {
            lix.close().await.unwrap();
        }
    });
}

#[test]
fn real_pack_admission_is_payload_bound_and_idempotent() {
    run_sync_test("real-pack-idempotency", || async {
        let seed_storage = Memory::new();
        let seed = open_lix().with_storage(seed_storage.clone()).await.unwrap();
        register_schema(&seed).await;
        apply_mutations(
            &seed,
            &[RowMutation {
                row_id: "value".to_owned(),
                base: None,
                proposed: Some("base".to_owned()),
            }],
        )
        .await;
        let base = active_commit_id(&seed).await;
        let snapshot = seed_storage.export_snapshot().unwrap();
        let client = open_snapshot(&snapshot).await;
        client
            .execute(
                "UPDATE prototype_sync_row SET value = 'accepted' WHERE row_id = 'value'",
                &[],
            )
            .await
            .unwrap();
        let pack = semantic_pack(
            &client,
            "stable-operation",
            &base,
            &base,
            &["prototype_sync_row"],
        )
        .await;
        let mut server = PackAdmissionServer::new(open_snapshot(&snapshot).await, &[&base]).await;

        let first = server.admit(&pack).await.unwrap();
        let retry = server.admit(&pack).await.unwrap();
        assert_eq!(retry, first);
        assert_eq!(active_commit_id(&server.lix).await, first);
        assert_eq!(server.receipts.len(), 1);

        let mut conflicting = pack.clone();
        conflicting.rows[0].snapshot = Some(serde_json::json!({
            "row_id": "value",
            "value": "different"
        }));
        let error = server.admit(&conflicting).await.unwrap_err();
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert_eq!(active_commit_id(&server.lix).await, first);

        let mut unknown_base = pack.clone();
        unknown_base.operation_id = "foreign-base".to_owned();
        unknown_base.base_server_commit_id = "01900000-0000-7000-8000-000000000999".to_owned();
        let error = server.admit(&unknown_base).await.unwrap_err();
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);

        for lix in [&client, &server.lix, &seed] {
            lix.close().await.unwrap();
        }
    });
}

#[test]
fn offline_row_packs_rebase_fifo_and_preserve_a_local_reversal() {
    run_sync_test("offline-real-pack-fifo-rebase", || async {
        let seed_storage = Memory::new();
        let seed = open_lix().with_storage(seed_storage.clone()).await.unwrap();
        register_schema(&seed).await;
        apply_mutations(
            &seed,
            &[
                RowMutation {
                    row_id: "local".to_owned(),
                    base: None,
                    proposed: Some("base".to_owned()),
                },
                RowMutation {
                    row_id: "remote".to_owned(),
                    base: None,
                    proposed: Some("base".to_owned()),
                },
            ],
        )
        .await;
        let base = active_commit_id(&seed).await;
        let snapshot = seed_storage.export_snapshot().unwrap();
        let offline = open_snapshot(&snapshot).await;

        offline
            .execute(
                "UPDATE prototype_sync_row SET value = 'offline-a' WHERE row_id = 'local'",
                &[],
            )
            .await
            .unwrap();
        let first_local_commit = active_commit_id(&offline).await;
        let first = offline
            .create_sync_transaction_pack(
                "offline-1",
                &base,
                &base,
                &first_local_commit,
                &["prototype_sync_row"],
            )
            .await
            .unwrap();
        offline
            .execute(
                "UPDATE prototype_sync_row SET value = 'base' WHERE row_id = 'local'",
                &[],
            )
            .await
            .unwrap();
        let second_local_commit = active_commit_id(&offline).await;
        let mut second = offline
            .create_sync_transaction_pack(
                "offline-2",
                &base,
                &first_local_commit,
                &second_local_commit,
                &["prototype_sync_row"],
            )
            .await
            .unwrap();
        assert_eq!(first.rows.len(), 1);
        assert_eq!(second.rows.len(), 1);
        assert_eq!(second.rows[0].snapshot.as_ref().unwrap()["value"], "base");

        let server_lix = open_snapshot(&snapshot).await;
        server_lix
            .execute(
                "UPDATE prototype_sync_row SET value = 'server' WHERE row_id = 'remote'",
                &[],
            )
            .await
            .unwrap();
        let mut server = PackAdmissionServer::new(server_lix, &[&base]).await;
        let first_canonical_head = server.admit(&first).await.unwrap();

        // A queued transaction keeps its semantic row write, but its server
        // base advances after the preceding acknowledgement.
        second.base_server_commit_id = first_canonical_head;
        server.admit(&second).await.unwrap();
        assert_eq!(
            read_state(&server.lix).await,
            BTreeMap::from([
                ("local".to_owned(), "base".to_owned()),
                ("remote".to_owned(), "server".to_owned()),
            ])
        );

        for lix in [&offline, &server.lix, &seed] {
            lix.close().await.unwrap();
        }
    });
}

#[test]
fn serialized_real_pack_admission_defines_default_same_row_order() {
    run_sync_test("real-pack-default-same-row-order", || async {
        let seed_storage = Memory::new();
        let seed = open_lix().with_storage(seed_storage.clone()).await.unwrap();
        register_schema(&seed).await;
        apply_mutations(
            &seed,
            &[RowMutation {
                row_id: "shared".to_owned(),
                base: None,
                proposed: Some("base".to_owned()),
            }],
        )
        .await;
        let base = active_commit_id(&seed).await;
        let snapshot = seed_storage.export_snapshot().unwrap();
        let bob = open_snapshot(&snapshot).await;
        let alice = open_snapshot(&snapshot).await;
        bob.execute(
            "UPDATE prototype_sync_row SET value = 'bob' WHERE row_id = 'shared'",
            &[],
        )
        .await
        .unwrap();
        alice
            .execute(
                "UPDATE prototype_sync_row SET value = 'alice' WHERE row_id = 'shared'",
                &[],
            )
            .await
            .unwrap();
        let bob_pack = semantic_pack(&bob, "bob", &base, &base, &["prototype_sync_row"]).await;
        let alice_pack =
            semantic_pack(&alice, "alice", &base, &base, &["prototype_sync_row"]).await;

        let mut alice_then_bob =
            PackAdmissionServer::new(open_snapshot(&snapshot).await, &[&base]).await;
        alice_then_bob.admit(&alice_pack).await.unwrap();
        alice_then_bob.admit(&bob_pack).await.unwrap();
        assert_eq!(read_state(&alice_then_bob.lix).await["shared"], "bob");

        let mut bob_then_alice =
            PackAdmissionServer::new(open_snapshot(&snapshot).await, &[&base]).await;
        bob_then_alice.admit(&bob_pack).await.unwrap();
        bob_then_alice.admit(&alice_pack).await.unwrap();
        assert_eq!(read_state(&bob_then_alice.lix).await["shared"], "alice");

        for lix in [
            &bob_then_alice.lix,
            &alice_then_bob.lix,
            &alice,
            &bob,
            &seed,
        ] {
            lix.close().await.unwrap();
        }
    });
}

async fn open_snapshot(snapshot: &[u8]) -> Lix<Memory> {
    open_lix()
        .with_storage(Memory::from_snapshot(snapshot).unwrap())
        .await
        .unwrap()
}

async fn open_filesystem_replica(
    path: &Path,
    branch_id: &str,
) -> (Lix<FilesystemStorage>, FilesystemStorage) {
    let storage = FilesystemStorage::new(path).open().unwrap();
    let lix = open_lix().with_storage(storage.clone()).await.unwrap();
    let branch = lix
        .create_branch(CreateBranchOptions {
            id: Some(branch_id.to_owned()),
            name: "sync target".to_owned(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    lix.switch_branch(SwitchBranchOptions {
        branch_id: branch.id,
    })
    .await
    .unwrap();
    (lix, storage)
}

async fn open_protocol_session(protocol: &LixServerProtocol<Memory>) -> String {
    let response = protocol
        .handle(
            Request::builder()
                .uri("/lix/v1")
                .body(ServerProtocolBody::empty())
                .unwrap(),
            ServerProtocolContext::anonymous(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice::<serde_json::Value>(&body).unwrap()["sessionId"]
        .as_str()
        .unwrap()
        .to_owned()
}

async fn admit_pack_via_protocol(
    protocol: &LixServerProtocol<Memory>,
    session_id: &str,
    pack: &SyncTransactionPack,
) {
    let response = protocol
        .handle(
            Request::builder()
                .method("POST")
                .uri("/lix/v1/sync/admit")
                .header(CONTENT_TYPE, "application/json")
                .header(SESSION_ID_HEADER, session_id)
                .header(IDEMPOTENCY_KEY_HEADER, &pack.operation_id)
                .body(ServerProtocolBody::from(serde_json::to_vec(pack).unwrap()))
                .unwrap(),
            ServerProtocolContext::anonymous(),
        )
        .await;
    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        panic!(
            "sync admission failed with {status}: {}",
            String::from_utf8_lossy(&body)
        );
    }
}

async fn semantic_pack(
    lix: &Lix<Memory>,
    operation_id: &str,
    base_server_commit_id: &str,
    from_local_commit_id: &str,
    semantic_schema_keys: &[&str],
) -> SyncTransactionPack {
    let local_head = active_commit_id(lix).await;
    lix.create_sync_transaction_pack(
        operation_id,
        base_server_commit_id,
        from_local_commit_id,
        &local_head,
        semantic_schema_keys,
    )
    .await
    .unwrap()
}

async fn admit_pack_unchecked(server: &Lix<Memory>, pack: &SyncTransactionPack) -> String {
    assert_eq!(server.active_branch_id().await.unwrap(), pack.branch_id);
    let proposal_branch = server
        .create_branch(CreateBranchOptions {
            id: None,
            name: format!("sync proposal {}", pack.operation_id),
            from_commit_id: Some(pack.base_server_commit_id.clone()),
        })
        .await
        .unwrap();
    let proposal = server
        .open_another_session()
        .with_branch(&proposal_branch.id)
        .await
        .unwrap();
    proposal
        .stage_sync_transaction_pack(pack, &server.active_branch_id().await.unwrap())
        .await
        .unwrap();
    proposal.close().await.unwrap();
    server
        .merge_branch(MergeBranchOptions {
            source_branch_id: proposal_branch.id,
        })
        .await
        .unwrap()
        .target_head_after_commit_id
}

async fn install_plugin_archive(lix: &Lix<Memory>, key: &str, archive: &[u8]) {
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
        &[
            Value::Text(format!("/.lix/plugins/{key}.lixplugin")),
            Value::Blob(archive.to_vec().into()),
        ],
    )
    .await
    .unwrap();
}

async fn write_file(lix: &Lix<Memory>, path: &str, content: &[u8]) {
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
         ON CONFLICT(path) DO UPDATE SET content = excluded.content",
        &[
            Value::Text(path.to_owned()),
            Value::Blob(content.to_vec().into()),
        ],
    )
    .await
    .unwrap();
}

async fn read_file(lix: &Lix<Memory>, path: &str) -> Vec<u8> {
    lix.execute(
        "SELECT content FROM lix_file WHERE path = $1",
        &[Value::Text(path.to_owned())],
    )
    .await
    .unwrap()
    .rows()[0]
        .get::<Vec<u8>>("content")
        .unwrap()
}

async fn update_markdown_paragraph(lix: &Lix<Memory>, id: &str, text: &str) {
    lix.execute(
        "UPDATE markdown_node SET payload_json = $1 WHERE id = $2",
        &[
            Value::Text(serde_json::json!({"inline":[{"type":"text","value":text}]}).to_string()),
            Value::Text(id.to_owned()),
        ],
    )
    .await
    .unwrap();
}

fn conversation_plugin_archive() -> Vec<u8> {
    plugin_archive(
        env!("CARGO_CDYLIB_FILE_PLUGIN_CONVERSATION_plugin_conversation"),
        include_bytes!("../../../plugins/conversation/manifest.json"),
        &[(
            "schema/conversation.json",
            include_bytes!("../../../plugins/conversation/schema/conversation.json"),
        )],
    )
}

fn markdown_plugin_archive() -> Vec<u8> {
    plugin_archive(
        env!("CARGO_CDYLIB_FILE_PLUGIN_MARKDOWN_plugin_markdown"),
        include_bytes!("../../../plugins/markdown/manifest.json"),
        &[(
            "schema/markdown_node.json",
            include_bytes!("../../../plugins/markdown/schema/markdown_node.json"),
        )],
    )
}

fn plugin_archive(wasm_path: &str, manifest: &[u8], schemas: &[(&str, &[u8])]) -> Vec<u8> {
    let wasm = std::fs::read(Path::new(wasm_path)).unwrap();
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    writer.start_file("manifest.json", options).unwrap();
    writer.write_all(manifest).unwrap();
    for (path, schema) in schemas {
        writer.start_file(*path, options).unwrap();
        writer.write_all(schema).unwrap();
    }
    writer.start_file("plugin.wasm", options).unwrap();
    writer.write_all(&wasm).unwrap();
    writer.finish().unwrap().into_inner()
}

async fn register_schema<S>(lix: &Lix<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
        &[Value::Text(SYNC_ROW_SCHEMA.to_owned())],
    )
    .await
    .expect("prototype schema should register");
}

async fn apply_mutations<S>(lix: &Lix<S>, mutations: &[RowMutation])
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let mut transaction = lix
        .begin_transaction()
        .await
        .expect("prototype transaction should open");
    for mutation in mutations {
        match &mutation.proposed {
            Some(value) => {
                transaction
                    .execute(
                        "INSERT INTO prototype_sync_row (row_id, value) VALUES ($1, $2) \
                         ON CONFLICT (row_id) DO UPDATE SET value = excluded.value",
                        &[
                            Value::Text(mutation.row_id.clone()),
                            Value::Text(value.clone()),
                        ],
                    )
                    .await
                    .expect("prototype row should upsert");
            }
            None => {
                transaction
                    .execute(
                        "DELETE FROM prototype_sync_row WHERE row_id = $1",
                        &[Value::Text(mutation.row_id.clone())],
                    )
                    .await
                    .expect("prototype row should delete");
            }
        }
    }
    transaction
        .commit()
        .await
        .expect("prototype transaction should commit");
}

fn apply_to_map(state: &mut BTreeMap<String, String>, mutations: &[RowMutation]) {
    for mutation in mutations {
        match &mutation.proposed {
            Some(value) => {
                state.insert(mutation.row_id.clone(), value.clone());
            }
            None => {
                state.remove(&mutation.row_id);
            }
        }
    }
}

async fn replace_state<S>(lix: &Lix<S>, state: &BTreeMap<String, String>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let current = read_state(lix).await;
    let mut mutations = current
        .keys()
        .filter(|row_id| !state.contains_key(*row_id))
        .map(|row_id| RowMutation {
            row_id: row_id.clone(),
            base: current.get(row_id).cloned(),
            proposed: None,
        })
        .collect::<Vec<_>>();
    mutations.extend(state.iter().filter_map(|(row_id, value)| {
        (current.get(row_id) != Some(value)).then(|| RowMutation {
            row_id: row_id.clone(),
            base: current.get(row_id).cloned(),
            proposed: Some(value.clone()),
        })
    }));
    if !mutations.is_empty() {
        apply_mutations(lix, &mutations).await;
    }
}

async fn read_state<S>(lix: &Lix<S>) -> BTreeMap<String, String>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "SELECT row_id, value FROM prototype_sync_row ORDER BY row_id",
        &[],
    )
    .await
    .expect("prototype state should read")
    .rows()
    .iter()
    .map(|row| {
        (
            row.get::<String>("row_id").expect("row id should be text"),
            row.get::<String>("value")
                .expect("row value should be text"),
        )
    })
    .collect()
}

async fn active_commit_id<S>(lix: &Lix<S>) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
        .await
        .expect("prototype commit id should read")
        .rows()[0]
        .get::<String>("commit_id")
        .expect("prototype commit id should be text")
}
