//! Fast deterministic coverage for the distributed sync state machine.
//!
//! The authority and replicas are real `Lix<Memory>` repositories. The only
//! fake is transport delivery: tests advance one complete sync iteration with
//! `pump()` and can deterministically disconnect or lose one push response.

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::engine::Engine;
use crate::storage::Memory;
use crate::support::fuzz_seeds;
use crate::support::simulation_test::engine::{Simulation, SimulationMode, SimulationOptions};
use crate::{ExecuteBatchStatement, Lix, LixError, Value, open_lix};

use super::runtime::{
    fetch_repository_snapshot, hydrate_error_for_test, register_blob_manifests, sync_iteration,
};
use super::{
    SyncBlobManifest, SyncBlobRegistration, SyncHistoryResponse, SyncPushRequest, SyncPushResponse,
    SyncRepositoryPullResponse, SyncRole, SyncSnapshotRowPage, SyncTransport, SyncTransportFuture,
};

const REMOTE_ID: &str = "memory://deterministic-sync-authority";
const COMMANDS_PER_SEED: usize = 16;

/// Pins both explicit v2 diff coordinates without issuing an additional SQL
/// read, preserving the HOT-path probes armed by these sync simulations.
async fn working_diff_sql(lix: &Lix<Memory>, relation: &str, sql: &str) -> String {
    let branch_id = lix.active_branch_id().await.expect("active branch should resolve");
    let adapter = lix.storage_adapter();
    let read = adapter
        .begin_read(crate::storage_adapter::StorageReadOptions::default())
        .await
        .expect("branch control read should open");
    let control = crate::branch::BranchHeadControlContext::new()
        .reader(&read)
        .load(&branch_id)
        .await
        .expect("branch control should load")
        .expect("branch control should exist");
    let checkpoint = control
        .working_diff_checkpoint_commit_id
        .expect("branch checkpoint cursor should exist");
    sql.replace(
        "__LIX_RELATION_DIFF__",
        &format!(
            "lix_diff('{relation}', '{checkpoint}', '{}')",
            control.head_commit_id
        ),
    )
}

#[derive(Clone, Default)]
struct DeliveryScript {
    offline: Arc<AtomicBool>,
    lose_next_push_response: Arc<AtomicBool>,
}

#[derive(Clone)]
struct AuthorityTransport {
    authority: Lix<Memory>,
    script: DeliveryScript,
}

impl fmt::Debug for AuthorityTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorityTransport")
            .field("offline", &self.script.offline.load(Ordering::SeqCst))
            .finish_non_exhaustive()
    }
}

impl AuthorityTransport {
    fn connected(authority: Lix<Memory>) -> Self {
        Self {
            authority,
            script: DeliveryScript::default(),
        }
    }

    fn set_offline(&self, offline: bool) {
        self.script.offline.store(offline, Ordering::SeqCst);
    }

    fn lose_next_push_response(&self) {
        self.script
            .lose_next_push_response
            .store(true, Ordering::SeqCst);
    }

    fn check_connected(&self) -> Result<(), LixError> {
        if self.script.offline.load(Ordering::SeqCst) {
            return Err(transport_error("scripted disconnect"));
        }
        Ok(())
    }
}

impl SyncTransport for AuthorityTransport {
    fn active_account_id(&self) -> &str {
        crate::ANONYMOUS_ACCOUNT_ID
    }

    fn push<'a>(
        &'a self,
        request: &'a SyncPushRequest,
    ) -> SyncTransportFuture<'a, SyncPushResponse> {
        Box::pin(async move {
            self.check_connected()?;
            let response = self.authority.push_sync_repository(request).await?;
            if self
                .script
                .lose_next_push_response
                .swap(false, Ordering::SeqCst)
            {
                return Err(transport_error("scripted lost push response"));
            }
            Ok(response)
        })
    }

    fn pull(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> SyncTransportFuture<'_, SyncRepositoryPullResponse> {
        Box::pin(async move {
            self.check_connected()?;
            self.authority.pull_sync_repository(after, limit).await
        })
    }

    fn snapshot_rows<'a>(
        &'a self,
        branch_id: &'a str,
        head_commit_id: &'a str,
        continuation: Option<&'a str>,
        limit: usize,
    ) -> SyncTransportFuture<'a, SyncSnapshotRowPage> {
        Box::pin(async move {
            self.check_connected()?;
            self.authority
                .pull_sync_snapshot_rows(branch_id, head_commit_id, continuation, limit)
                .await
        })
    }

    fn history<'a>(
        &'a self,
        head: &'a str,
        limit: usize,
    ) -> SyncTransportFuture<'a, SyncHistoryResponse> {
        Box::pin(async move {
            self.check_connected()?;
            self.authority.sync_history(head, limit).await
        })
    }

    fn get_blobs<'a>(
        &'a self,
        blob_ids: &'a [String],
    ) -> SyncTransportFuture<'a, Vec<SyncBlobManifest>> {
        Box::pin(async move {
            self.check_connected()?;
            let mut manifests = Vec::new();
            for blob_id in blob_ids {
                if let Some(manifest) = self.authority.get_sync_blob_manifest(blob_id).await? {
                    manifests.push(manifest);
                }
            }
            Ok(manifests)
        })
    }

    fn register_blob<'a>(
        &'a self,
        manifest: &'a SyncBlobManifest,
    ) -> SyncTransportFuture<'a, SyncBlobRegistration> {
        Box::pin(async move {
            self.check_connected()?;
            self.authority.register_sync_blob_manifest(manifest).await
        })
    }

    fn get_chunk<'a>(&'a self, chunk_id: &'a str) -> SyncTransportFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            self.check_connected()?;
            self.authority.get_sync_chunk(chunk_id).await
        })
    }

    fn put_chunk<'a>(&'a self, chunk_id: &'a str, bytes: &'a [u8]) -> SyncTransportFuture<'a, ()> {
        Box::pin(async move {
            self.check_connected()?;
            self.authority.put_sync_chunk(chunk_id, bytes).await
        })
    }
}

struct Replica {
    lix: Lix<Memory>,
    storage: Memory,
    transport: AuthorityTransport,
    change_watcher: tokio::sync::watch::Receiver<u64>,
    _demand_tx: tokio::sync::mpsc::Sender<super::runtime::SyncDemand>,
    demand_rx: tokio::sync::mpsc::Receiver<super::runtime::SyncDemand>,
    pending_demands: Vec<super::runtime::SyncDemand>,
    push_item_limit: usize,
    pull_item_limit: usize,
}

impl Replica {
    async fn bootstrap(transport: AuthorityTransport) -> Self {
        let (snapshot, authority_lix_id, default_branch_id) = fetch_repository_snapshot(&transport)
            .await
            .expect("simulation snapshot should load");
        let storage = Memory::new();
        Engine::initialize_with_main_branch_id(storage.clone(), Some(&default_branch_id))
            .await
            .expect("simulation replica should initialize");
        let mut lix = open_lix()
            .with_storage(storage.clone())
            .await
            .expect("simulation replica should open");
        lix.set_sync_role(SyncRole::Replica)
            .expect("simulation replica role should install");
        register_blob_manifests(&lix, &transport, &snapshot.commits, &snapshot.rows)
            .await
            .expect("simulation snapshot blob manifests should register");
        lix.try_install_initial_sync_snapshot(
            REMOTE_ID,
            transport.active_account_id(),
            &snapshot.metadata,
            &snapshot.commits,
            &snapshot.commit_headers,
            &snapshot.rows,
            &snapshot.checkpoint_roots,
        )
        .await
        .expect("simulation snapshot should install");
        lix.align_repository_identity_for_sync(authority_lix_id)
            .expect("simulation repository identity should align");
        lix.align_primary_account_for_sync(transport.active_account_id())
            .await
            .expect("simulation account should align");
        Self::from_open_lix(lix, storage, transport)
    }

    fn from_open_lix(lix: Lix<Memory>, storage: Memory, transport: AuthorityTransport) -> Self {
        let change_watcher = lix.sync_mode_state().change_watcher();
        let (demand_tx, demand_rx) = tokio::sync::mpsc::channel(8);
        Self {
            lix,
            storage,
            transport,
            change_watcher,
            _demand_tx: demand_tx,
            demand_rx,
            pending_demands: Vec::new(),
            push_item_limit: super::MAX_SYNC_REQUEST_ITEMS,
            pull_item_limit: super::MAX_SYNC_REQUEST_ITEMS,
        }
    }

    async fn pump(&mut self) -> Result<(), LixError> {
        sync_iteration(
            &self.lix,
            REMOTE_ID,
            &self.transport,
            &mut self.push_item_limit,
            &mut self.pull_item_limit,
            &mut self.change_watcher,
            &mut self.demand_rx,
            &mut self.pending_demands,
        )
        .await
    }

    async fn restart(&mut self) {
        let placeholder = open_lix()
            .await
            .expect("temporary simulation handle should open");
        let previous = std::mem::replace(&mut self.lix, placeholder);
        previous
            .close()
            .await
            .expect("simulation replica should close before restart");
        drop(previous);
        let lix = open_lix()
            .with_storage(self.storage.clone())
            .await
            .expect("simulation replica should reopen");
        lix.set_sync_role(SyncRole::Replica)
            .expect("reopened simulation replica role should install");
        *self = Self::from_open_lix(lix, self.storage.clone(), self.transport.clone());
    }

    async fn write(&self, key: &str, value: &str) {
        write_key_value(&self.lix, key, value).await;
    }

    async fn hydrate_and_retry(&self, sql: &str) {
        let error = self
            .lix
            .execute(sql, &[])
            .await
            .expect_err("sparse read should request hydration");
        hydrate_error_for_test(&self.lix, &self.transport, error)
            .await
            .expect("simulation demand should hydrate");
        self.lix
            .execute(sql, &[])
            .await
            .expect("hydrated read should succeed");
    }
}

fn transport_error(message: &str) -> LixError {
    LixError::new(super::http::SYNC_TRANSPORT_ERROR_CODE, message)
}

async fn write_key_value(lix: &Lix<Memory>, key: &str, value: &str) {
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ($1, $2) \
         ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        &[Value::Text(key.to_owned()), Value::Text(value.to_owned())],
    )
    .await
    .expect("simulation write should commit");
}

async fn hot_digest(lix: &Lix<Memory>) -> Vec<(String, String)> {
    lix.execute(
        "SELECT key, value FROM lix_key_value \
         WHERE lixcol_global = false ORDER BY key",
        &[],
    )
    .await
    .expect("simulation digest should query")
    .rows()
    .iter()
    .map(|row| {
        (
            row.get::<String>("key").expect("digest key"),
            row.get::<serde_json::Value>("value")
                .expect("digest value")
                .to_string(),
        )
    })
    .collect()
}

async fn converge(authority: &Lix<Memory>, replicas: &mut [&mut Replica]) -> Result<(), LixError> {
    for _ in 0..12 {
        for replica in replicas.iter_mut() {
            replica.pump().await?;
        }
        let expected = hot_digest(authority).await;
        let mut equal = true;
        for replica in replicas.iter() {
            equal &= hot_digest(&replica.lix).await == expected;
        }
        if equal {
            return Ok(());
        }
    }
    panic!("replicas did not converge within the deterministic pump budget");
}

async fn authority_for_simulation(sim: &Simulation) -> Lix<Memory> {
    let authority = open_lix()
        .with_storage(sim.storage())
        .await
        .expect("simulation authority should open");
    authority
        .set_sync_role(SyncRole::Authority)
        .expect("simulation authority role should install");
    authority
}

async fn fresh_authority() -> Lix<Memory> {
    let authority = open_lix().await.expect("fresh authority should open");
    authority
        .set_sync_role(SyncRole::Authority)
        .expect("fresh authority role should install");
    authority
}

fn run_sync_simulation<F, Fut>(case_id: &str, test_fn: F)
where
    F: Fn(Simulation) -> Fut,
    Fut: Future<Output = ()>,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("sync simulation runtime should build");
    runtime.block_on(
        crate::support::simulation_test::engine::run_simulation_test(
            SimulationMode::Base,
            SimulationOptions {
                deterministic: true,
            },
            case_id,
            test_fn,
        ),
    );
}

macro_rules! sync_simulation_test {
    ($name:ident, $scenario:ident) => {
        paste::paste! {
            #[test]
            fn [<$name _base>]() {
                run_sync_simulation(
                    concat!(module_path!(), "::", stringify!($name)),
                    $scenario,
                );
            }
        }
    };
}

async fn deterministic_replica_scenarios(sim: Simulation) {
    let authority = authority_for_simulation(&sim).await;
    let mut left = Replica::bootstrap(AuthorityTransport::connected(authority.clone())).await;
    let mut right = Replica::bootstrap(AuthorityTransport::connected(authority.clone())).await;

    left.write("left", "one").await;
    left.pump().await.expect("left push should succeed");
    right.pump().await.expect("right pull should succeed");
    right.write("right", "two").await;
    right.pump().await.expect("right push should succeed");
    converge(&authority, &mut [&mut left, &mut right])
        .await
        .expect("two replicas should converge");

    left.transport.lose_next_push_response();
    left.write("lost-ack", "retry-me").await;
    let error = left
        .pump()
        .await
        .expect_err("the scripted push response should be lost");
    assert_eq!(error.code, super::http::SYNC_TRANSPORT_ERROR_CODE);
    converge(&authority, &mut [&mut left, &mut right])
        .await
        .expect("lost acknowledgement should converge");
    assert_eq!(
        hot_digest(&authority)
            .await
            .iter()
            .filter(|(key, _)| key == "lost-ack")
            .count(),
        1,
        "idempotent retry must publish one logical row",
    );

    right.transport.set_offline(true);
    right.write("offline", "durable-outbox").await;
    let error = right
        .pump()
        .await
        .expect_err("offline push should remain queued");
    assert_eq!(error.code, super::http::SYNC_TRANSPORT_ERROR_CODE);
    right.restart().await;
    right.transport.set_offline(false);
    converge(&authority, &mut [&mut left, &mut right])
        .await
        .expect("offline restart should converge");

    for index in 0..3 {
        right
            .write(&format!("divergent-right-{index}"), "right")
            .await;
    }
    left.write("divergent-left-a", "left").await;
    left.write("divergent-left-b", "left").await;
    right.pump().await.expect("right divergence should publish");
    left.pump()
        .await
        .expect("left divergence should pull and reconcile");
    left.restart().await;
    right.restart().await;
    converge(&authority, &mut [&mut left, &mut right])
        .await
        .expect("reconciled offline chains should survive restart");
}

async fn lazy_history_and_binary_cas_scenarios(sim: Simulation) {
    let authority = authority_for_simulation(&sim).await;
    write_key_value(&authority, "history-0", "0").await;
    let cold_commit_id = authority
        .execute("SELECT lix_active_branch_commit_id() AS id", &[])
        .await
        .expect("cold history coordinate should load")
        .rows()[0]
        .get::<String>("id")
        .expect("cold history coordinate should decode");
    for index in 1..8 {
        write_key_value(&authority, &format!("history-{index}"), &index.to_string()).await;
    }
    authority
        .create_checkpoint()
        .await
        .expect("authority checkpoint should commit");
    let bytes = vec![b'x'; 96 * 1024];
    authority
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
            &[
                Value::Text("/large.bin".to_owned()),
                Value::Blob(bytes.clone().into()),
            ],
        )
        .await
        .expect("large binary file should commit");

    let replica = Replica::bootstrap(AuthorityTransport::connected(authority.clone())).await;
    let history_sql = format!("SELECT * FROM lix_history('lix_key_value', '{cold_commit_id}')");
    replica.hydrate_and_retry(&history_sql).await;
    replica
        .hydrate_and_retry("SELECT content FROM lix_file WHERE path = '/large.bin'")
        .await;
    let result = replica
        .lix
        .execute(
            "SELECT content FROM lix_file WHERE path = '/large.bin'",
            &[],
        )
        .await
        .expect("hydrated binary read should succeed");
    assert_eq!(
        result.rows()[0]
            .get::<Vec<u8>>("content")
            .expect("binary content"),
        bytes,
    );
    let checkpoints = replica
        .lix
        .execute("SELECT commit_id FROM lix_checkpoint", &[])
        .await
        .expect("checkpoint rows should remain visible");
    assert!(!checkpoints.rows().is_empty());
}

async fn sparse_partial_checkpoint_uses_hot_working_diff(_sim: Simulation) {
    let authority = fresh_authority().await;
    write_key_value(&authority, "selected", "baseline").await;
    write_key_value(&authority, "remaining", "baseline").await;
    authority
        .create_checkpoint()
        .await
        .expect("baseline checkpoint should commit");
    let mut replica = Replica::bootstrap(AuthorityTransport::connected(authority.clone())).await;

    // Advance the shared checkpoint only after this replica bootstrapped. The
    // following interval therefore arrives through sparse delta
    // reconciliation instead of the initial complete checkpoint snapshot.
    write_key_value(&authority, "selected", "checkpointed").await;
    write_key_value(&authority, "remaining", "checkpointed").await;
    authority
        .create_checkpoint()
        .await
        .expect("remote checkpoint should commit");
    replica
        .pump()
        .await
        .expect("replica should import the remote checkpoint");

    for index in 0..24 {
        write_key_value(&authority, "selected", &format!("selected-{index}")).await;
        write_key_value(&authority, "remaining", &format!("remaining-{index}")).await;
    }
    replica
        .pump()
        .await
        .expect("replica should import the sparse working interval");

    let selected_row_pk = replica
        .lix
        .execute(
            &working_diff_sql(
                &replica.lix,
                "lix_key_value",
                "SELECT lixcol_row_pk FROM __LIX_RELATION_DIFF__ \
                 WHERE lixcol_row_pk = CAST('[\"selected\"]' AS JSONB)",
            )
            .await,
            &[],
        )
        .await
        .expect("selected working diff should be hot")
        .rows()[0]
        .get::<serde_json::Value>("lixcol_row_pk")
        .expect("selected primary key should decode");
    let head_commit_id_text = replica
        .lix
        .execute("SELECT lix_active_branch_commit_id() AS id", &[])
        .await
        .expect("working head coordinate should load")
        .rows()[0]
        .get::<String>("id")
        .expect("working head coordinate should decode");
    let branch_id = replica
        .lix
        .active_branch_id()
        .await
        .expect("active branch id should load");
    let adapter = replica.lix.storage_adapter();
    let checkpoint_read = adapter
        .begin_read(crate::storage_adapter::StorageReadOptions::default())
        .await
        .expect("checkpoint cursor read should open");
    let head_commit_id = crate::changelog::CommitId::parse_lix(
        &head_commit_id_text,
        "working head fixture",
    )
    .expect("working head id should be canonical");
    let checkpoint_commit_id = crate::checkpoint::checkpoint_commit_id_at_head(
        &checkpoint_read,
        &branch_id,
        head_commit_id,
    )
    .await
    .expect("working checkpoint coordinate should load")
    .to_string();
    drop(checkpoint_read);

    // The selected relation identity is frozen before the checkpoint command. Probe the
    // canonical endpoint diff directly: a selective checkpoint must consume
    // the already-certified hot working diff and never reconstruct the same
    // interval through diff_commits(checkpoint, head).
    crate::tracked_state::arm_diff_commits_test_probe(
        &checkpoint_commit_id,
        &head_commit_id_text,
    );

    let checkpoint = replica
        .lix
        .execute(
            "INSERT INTO lix_create_checkpoint (relation, row_pk) \
             SELECT 'lix_key_value', $1 RETURNING commit_id",
            &[Value::Jsonb(selected_row_pk.into())],
        )
        .await
        .expect("partial checkpoint should use the hot working-diff authority");
    assert_eq!(
        crate::tracked_state::take_diff_commits_test_probe(
            &checkpoint_commit_id,
            &head_commit_id_text,
        ),
        0,
        "partial checkpoint must not reconstruct the certified working diff",
    );
    assert_eq!(checkpoint.rows_affected(), 1);
    let checkpoint_commit_id = checkpoint.rows()[0]
        .get::<String>("commit_id")
        .expect("checkpoint commit id should decode");

    let checkpoint_state = replica
        .lix
        .execute(
            &format!(
                "SELECT key, value FROM lix_history('lix_key_value', '{checkpoint_commit_id}') \
                 WHERE lixcol_depth = 0 ORDER BY key"
            ),
            &[],
        )
        .await
        .expect("partial checkpoint state should remain readable");
    let checkpoint_values = checkpoint_state
        .rows()
        .iter()
        .map(|row| {
            (
                row.get::<String>("key").expect("checkpoint state key"),
                row.get::<serde_json::Value>("value")
                    .expect("checkpoint state value"),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        checkpoint_values,
        vec![("selected".to_owned(), serde_json::json!("selected-23"))]
    );

    let remaining = replica
        .lix
        .execute(
            &working_diff_sql(
                &replica.lix,
                "lix_key_value",
                "SELECT lixcol_row_pk FROM __LIX_RELATION_DIFF__ ORDER BY lixcol_row_pk",
            )
            .await,
            &[],
        )
        .await
        .expect("unselected working diff should remain readable");
    assert_eq!(remaining.rows().len(), 1);
    assert_eq!(
        remaining.rows()[0]
            .get::<serde_json::Value>("lixcol_row_pk")
            .expect("remaining row key"),
        serde_json::json!(["remaining"]),
    );
    let final_values = hot_digest(&replica.lix)
        .await
        .into_iter()
        .filter(|(key, _)| key == "remaining" || key == "selected")
        .collect::<Vec<_>>();
    assert_eq!(
        final_values,
        vec![
            ("remaining".to_owned(), "\"remaining-23\"".to_owned()),
            ("selected".to_owned(), "\"selected-23\"".to_owned()),
        ]
    );
}

async fn snapshot_partial_checkpoint_uses_local_selected_payloads(_sim: Simulation) {
	let authority = fresh_authority().await;
	write_key_value(&authority, "checkpoint-base", "baseline").await;
	authority
		.create_checkpoint()
		.await
		.expect("baseline checkpoint should commit");
	for index in 0..50 {
		write_key_value(&authority, &format!("working-{index:02}"), "working").await;
	}
	let replica = Replica::bootstrap(AuthorityTransport::connected(authority)).await;

	// Snapshot bootstrap deliberately keeps each live change payload locally
	// while leaving its owning historical commit cold. Working-diff
	// checkpoint materialization must use those local snapshot payloads instead
	// of turning one checkpoint into a history-hydration demand per source
	// commit.
	let checkpoint = replica
		.lix
		.execute(
			&working_diff_sql(
				&replica.lix,
				"lix_key_value",
				"INSERT INTO lix_create_checkpoint (relation, row_pk) \
				 SELECT 'lix_key_value', lixcol_row_pk FROM __LIX_RELATION_DIFF__ \
				 WHERE lixcol_row_pk = CAST('[\"working-00\"]' AS JSONB) \
				 RETURNING commit_id",
			)
			.await,
			&[],
		)
		.await
		.expect("partial checkpoint should use the snapshot-local selected payload");
	assert_eq!(checkpoint.rows_affected(), 1);
	let remaining = replica
		.lix
		.execute(
			&working_diff_sql(
				&replica.lix,
				"lix_key_value",
				"SELECT lixcol_row_pk FROM __LIX_RELATION_DIFF__ ORDER BY lixcol_row_pk",
			)
			.await,
			&[],
		)
		.await
		.expect("unselected snapshot-local diff should remain readable");
	assert_eq!(remaining.rows().len(), 49);
	assert_eq!(
		remaining.rows()[0]
			.get::<serde_json::Value>("lixcol_row_pk")
			.unwrap(),
		serde_json::json!(["working-01"]),
	);
}

async fn partial_checkpoint_rebases_hot_epoch_without_cold_history(_sim: Simulation) {
    const OWNER_COUNT: usize = 50;
    let authority = fresh_authority().await;
    for index in 0..OWNER_COUNT {
        write_key_value(
            &authority,
            &format!("owner-{index:02}"),
            &format!("baseline-{index:02}"),
        )
        .await;
    }
    authority
        .create_checkpoint()
        .await
        .expect("baseline checkpoint should commit");
    let mut replica = Replica::bootstrap(AuthorityTransport::connected(authority.clone())).await;

    // Every updated payload is present in the replica's current HOT state,
    // while the fifty physical commits that authored those rows remain cold.
    for index in 0..OWNER_COUNT {
        write_key_value(
            &authority,
            &format!("owner-{index:02}"),
            &format!("updated-{index:02}"),
        )
        .await;
    }
    replica
        .pump()
        .await
        .expect("replica should import every sparse update");

    crate::tracked_state::arm_point_replay_authority_batch_probe_for_test();
    let selected_row_pk = replica
        .lix
        .execute(
            &working_diff_sql(
                &replica.lix,
                "lix_key_value",
                "SELECT lixcol_row_pk FROM __LIX_RELATION_DIFF__ \
                 WHERE lixcol_row_pk = CAST('[\"owner-00\"]' AS JSONB)",
            )
            .await,
            &[],
        )
        .await
        .expect("working diff should already be HOT")
        .rows()[0]
        .get::<serde_json::Value>("lixcol_row_pk")
        .expect("selected primary key should decode");
    assert!(
        crate::tracked_state::take_point_replay_authority_batch_probe_for_test().is_empty(),
        "working-diff classification must not touch cold commit owners",
    );

    crate::tracked_state::arm_point_replay_authority_batch_probe_for_test();
    let checkpoint = replica
        .lix
        .execute(
            "INSERT INTO lix_create_checkpoint (relation, row_pk) \
             SELECT 'lix_key_value', $1 RETURNING commit_id",
            &[Value::Jsonb(selected_row_pk.into())],
        )
        .await
        .expect("checkpoint should use the exact selected HOT payload");
    let authority_batches =
        crate::tracked_state::take_point_replay_authority_batch_probe_for_test();

    assert_eq!(checkpoint.rows_affected(), 1);
    let checkpoint_commit_id = checkpoint.rows()[0]
        .get::<String>("commit_id")
        .expect("checkpoint commit id should decode");
    assert!(
        authority_batches.is_empty(),
        "checkpoint publication must not re-read cold selected owners: {authority_batches:?}",
    );
    crate::tracked_state::arm_point_replay_authority_batch_probe_for_test();
    let remaining = replica
        .lix
        .execute(
            &working_diff_sql(
                &replica.lix,
                "lix_key_value",
                "SELECT count(*) AS count FROM __LIX_RELATION_DIFF__",
            )
            .await,
            &[],
        )
        .await
        .expect("remaining working diff should load")
        .rows()[0]
        .get::<i64>("count")
        .expect("remaining count should decode");
    assert_eq!(remaining, (OWNER_COUNT - 1) as i64);
    assert!(
        crate::tracked_state::take_point_replay_authority_batch_probe_for_test().is_empty(),
        "the first reactive working-diff read must stay HOT",
    );

    let checkpoint_state = replica
        .lix
        .execute(
            &format!(
                "SELECT key, value FROM lix_history('lix_key_value', '{checkpoint_commit_id}') \
                 WHERE lixcol_depth = 0"
            ),
            &[],
        )
        .await
        .expect("partial checkpoint state should remain readable");
    assert_eq!(checkpoint_state.rows().len(), 1);
    assert_eq!(
        checkpoint_state.rows()[0].get::<String>("key").unwrap(),
        "owner-00",
    );
    assert_eq!(
        checkpoint_state.rows()[0]
            .get::<serde_json::Value>("value")
            .unwrap(),
        serde_json::json!("updated-00"),
    );

    replica.restart().await;
    crate::tracked_state::arm_point_replay_authority_batch_probe_for_test();
    let reopened_remaining = replica
        .lix
        .execute(
            &working_diff_sql(
                &replica.lix,
                "lix_key_value",
                "SELECT count(*) AS count FROM __LIX_RELATION_DIFF__",
            )
            .await,
            &[],
        )
        .await
        .expect("reopened working diff should remain HOT")
        .rows()[0]
        .get::<i64>("count")
        .unwrap();
    assert_eq!(reopened_remaining, (OWNER_COUNT - 1) as i64);
    assert!(
        crate::tracked_state::take_point_replay_authority_batch_probe_for_test().is_empty(),
        "reopened working-diff read must not reconstruct cold owners",
    );
}

async fn partial_file_checkpoint_rebases_hot_epoch(_sim: Simulation) {
    let authority = fresh_authority().await;
    for path in ["/selected.md", "/remaining.md"] {
        authority
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                &[
                    Value::Text(path.to_owned()),
                    Value::Blob(b"baseline".to_vec().into()),
                ],
            )
            .await
            .expect("baseline file should commit");
    }
    authority
        .create_checkpoint()
        .await
        .expect("baseline checkpoint should commit");
    let mut replica = Replica::bootstrap(AuthorityTransport::connected(authority.clone())).await;

    for (path, content) in [
        ("/selected.md", b"selected update".as_slice()),
        ("/remaining.md", b"remaining update".as_slice()),
    ] {
        authority
            .execute(
                "UPDATE lix_file SET content = $1 WHERE path = $2",
                &[
                    Value::Blob(content.to_vec().into()),
                    Value::Text(path.to_owned()),
                ],
            )
            .await
            .expect("file update should commit");
    }
    replica
        .pump()
        .await
        .expect("replica should import both file updates");

    let selected_file_id = replica
        .lix
        .execute(
            "SELECT id FROM lix_file WHERE path = '/selected.md'",
            &[],
        )
        .await
        .expect("selected file should load")
        .rows()[0]
        .get::<String>("id")
        .expect("selected file id should decode");
    crate::tracked_state::arm_point_replay_authority_batch_probe_for_test();
    let checkpoint = replica
        .lix
        .execute(
            &working_diff_sql(
                &replica.lix,
                "lix_file",
                "INSERT INTO lix_create_checkpoint (relation, row_pk) \
                 SELECT 'lix_file', lixcol_row_pk FROM __LIX_RELATION_DIFF__ \
                 WHERE lixcol_row_pk ->> 0 = $1 RETURNING commit_id",
            )
            .await,
            &[Value::Text(selected_file_id)],
        )
        .await
        .expect("partial file checkpoint should stay snapshot-local");
    assert_eq!(checkpoint.rows_affected(), 1);
    let authority_batches =
        crate::tracked_state::take_point_replay_authority_batch_probe_for_test();
    assert!(
        authority_batches.is_empty(),
        "partial file checkpoint must not re-read cold owners: {authority_batches:?}",
    );

    let files = replica
        .lix
        .execute("SELECT path, content FROM lix_file ORDER BY path", &[])
        .await
        .expect("files should remain readable after checkpoint");
    assert_eq!(
        files.rows()[0].get::<Vec<u8>>("content").unwrap(),
        b"remaining update",
    );
    assert_eq!(
        files.rows()[1].get::<Vec<u8>>("content").unwrap(),
        b"selected update",
    );
    let remaining_paths = replica
        .lix
        .execute(
            &working_diff_sql(
                &replica.lix,
                "lix_file",
                "SELECT coalesce(to_path, from_path) AS path FROM __LIX_RELATION_DIFF__ ORDER BY path",
            )
            .await,
            &[],
        )
        .await
        .expect("remaining file diff should load");
    assert_eq!(remaining_paths.rows().len(), 1);
    assert_eq!(
        remaining_paths.rows()[0].get::<String>("path").unwrap(),
        "/remaining.md",
    );
}

async fn packed_recreate_partial_checkpoint_stays_hot(_sim: Simulation) {
    const PACKED_ROW_COUNT: usize = 512;
    let authority = fresh_authority().await;
    write_key_value(&authority, "selected-recreate", "baseline").await;
    write_key_value(&authority, "remaining", "baseline").await;
    authority
        .create_checkpoint()
        .await
        .expect("baseline checkpoint should commit");
    let mut replica = Replica::bootstrap(AuthorityTransport::connected(authority)).await;

    replica
        .lix
        .execute(
            "DELETE FROM lix_key_value WHERE key = 'selected-recreate'",
            &[],
        )
        .await
        .expect("selected row should leave a HOT checkpoint baseline");
    replica.write("remaining", "working").await;
    let mut packed_rows = vec![
        ExecuteBatchStatement {
            label: None,
            sql: "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)".to_owned(),
            params: vec![
                Value::Text("selected-recreate".to_owned()),
                Value::Text("recreated".to_owned()),
            ],
        },
    ];
    packed_rows.extend((packed_rows.len()..PACKED_ROW_COUNT).map(|index| {
        ExecuteBatchStatement {
            label: None,
            sql: "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)".to_owned(),
            params: vec![
                Value::Text(format!("packed-filler-{index:03}")),
                Value::Text("filler".to_owned()),
            ],
        }
    }));
    replica
        .lix
        .execute_batch(&packed_rows)
        .await
        .expect("recreate batch should publish a packed current base");
    replica.restart().await;

    let branch_id = replica.lix.active_branch_id().await.unwrap();
    let adapter = replica.lix.storage_adapter();
    let read = adapter
        .begin_read(crate::storage_adapter::StorageReadOptions::default())
        .await
        .unwrap();
    let control = crate::branch::BranchHeadControlContext::new()
        .reader(&read)
        .load(&branch_id)
        .await
        .unwrap()
        .unwrap();
    drop(read);
    let checkpoint_commit_id = control
        .working_diff_checkpoint_commit_id
        .expect("working checkpoint should remain active")
        .to_string();
    let head_commit_id = control.head_commit_id.to_string();
    crate::tracked_state::arm_diff_commits_test_probe(&checkpoint_commit_id, &head_commit_id);
    crate::tracked_state::reset_commit_delta_scan_probe_for_test();
    let checkpoint = replica
        .lix
        .execute(
            &working_diff_sql(
                &replica.lix,
                "lix_key_value",
                "INSERT INTO lix_create_checkpoint (relation, row_pk) \
                 SELECT 'lix_key_value', lixcol_row_pk FROM __LIX_RELATION_DIFF__ \
                 WHERE lixcol_row_pk ->> 0 = 'selected-recreate' RETURNING commit_id",
            )
            .await,
            &[],
        )
        .await
        .expect("packed recreate partial checkpoint should stay HOT");
    assert_eq!(checkpoint.rows_affected(), 1);
    assert_eq!(
        crate::tracked_state::take_diff_commits_test_probe(
            &checkpoint_commit_id,
            &head_commit_id,
        ),
        0,
        "one ambiguous packed recreate must not fall back to canonical history",
    );
    let (compact_scans, payload_scans) =
        crate::tracked_state::take_commit_delta_scan_probe_for_test();
    assert!(compact_scans > 0, "the packed compact route was not exercised");
    assert_eq!(
        payload_scans, 0,
        "partial checkpoint performed a broad commit-payload scan",
    );
    let selected_remaining = replica
        .lix
        .execute(
            &working_diff_sql(
                &replica.lix,
                "lix_key_value",
                "SELECT count(*) AS count FROM __LIX_RELATION_DIFF__ \
                 WHERE lixcol_row_pk ->> 0 = 'selected-recreate'",
            )
            .await,
            &[],
        )
        .await
        .unwrap()
        .rows()[0]
        .get::<i64>("count")
        .unwrap();
    assert_eq!(selected_remaining, 0);
    let remaining = replica
        .lix
        .execute(
            &working_diff_sql(
                &replica.lix,
                "lix_key_value",
                "SELECT count(*) AS count FROM __LIX_RELATION_DIFF__ \
                 WHERE lixcol_row_pk ->> 0 = 'remaining'",
            )
            .await,
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        remaining.rows()[0].get::<i64>("count").unwrap(),
        1,
        "the unselected change must remain dirty",
    );
    replica.restart().await;
    let reopened_remaining = replica
        .lix
        .execute(
            &working_diff_sql(
                &replica.lix,
                "lix_key_value",
                "SELECT count(*) AS count FROM __LIX_RELATION_DIFF__ \
                 WHERE lixcol_row_pk ->> 0 = 'remaining'",
            )
            .await,
            &[],
        )
        .await
        .unwrap();
    assert_eq!(reopened_remaining.rows()[0].get::<i64>("count").unwrap(), 1);
}

async fn packed_snapshot_partial_file_checkpoint_stays_payload_local(_sim: Simulation) {
    const HISTORICAL_CHECKPOINT_COUNT: usize = 50;
    const FILE_COUNT: usize = 90;
    const PACKED_ROW_COUNT: usize = 512;
    let authority = fresh_authority().await;
    for checkpoint_index in 0..HISTORICAL_CHECKPOINT_COUNT {
        write_key_value(
            &authority,
            "history",
            &format!("checkpoint-{checkpoint_index:02}"),
        )
        .await;
        authority
            .create_checkpoint()
            .await
            .expect("historical checkpoint should commit");
    }
    for index in 0..FILE_COUNT {
        authority
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                &[
                    Value::Text(format!("/packed-{index:02}.md")),
                    Value::Blob(format!("packed-{index:02}").into_bytes().into()),
                ],
            )
            .await
            .expect("post-checkpoint file should commit");
    }
    let mut replica = Replica::bootstrap(AuthorityTransport::connected(authority)).await;
    let packed_rows = (0..PACKED_ROW_COUNT)
        .map(|index| ExecuteBatchStatement {
            label: None,
            sql: "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)".to_owned(),
            params: vec![
                Value::Text(format!("packed-row-{index:03}")),
                Value::Text(format!("value-{index:03}")),
            ],
        })
        .collect::<Vec<_>>();
    replica
        .lix
        .execute_batch(&packed_rows)
        .await
        .expect("large post-checkpoint batch should publish a packed current base");
    replica
        .lix
        .execute(
            "UPDATE lix_file SET content = $1 WHERE path = '/packed-02.md'",
            &[Value::Blob(b"hot update".to_vec().into())],
        )
        .await
        .expect("packed snapshot should accept a HOT file update");
    replica
        .lix
        .execute("DELETE FROM lix_file WHERE path = '/packed-03.md'", &[])
        .await
        .expect("packed snapshot should accept a HOT file delete");

    let mut compact_scan_counts = Vec::new();
    for (selected_path, expected_remaining) in
        [("/packed-00.md", FILE_COUNT - 2), ("/packed-01.md", FILE_COUNT - 3)]
    {
        let selected_file_id = replica
            .lix
            .execute("SELECT id FROM lix_file WHERE path = $1", &[Value::Text(selected_path.to_owned())])
            .await
            .expect("selected packed file should be locally readable")
            .rows()[0]
            .get::<String>("id")
            .expect("selected packed file id should decode");
        crate::tracked_state::reset_commit_delta_scan_probe_for_test();
        let direct_file_diff = replica
            .lix
            .execute(
                &working_diff_sql(
                    &replica.lix,
                    "lix_file",
                    "SELECT count(*) AS count FROM __LIX_RELATION_DIFF__ WHERE lixcol_row_pk ->> 0 = $1",
                )
                .await,
                &[Value::Text(selected_file_id.clone())],
            )
            .await
            .expect("direct file-filtered packed diff should stay local");
        assert!(
            direct_file_diff.rows()[0].get::<i64>("count").unwrap() > 0,
            "selected packed file should have a direct file-scoped diff",
        );
        let (direct_compact_scans, payload_scans) =
            crate::tracked_state::take_commit_delta_scan_probe_for_test();
        assert_eq!(payload_scans, 0, "direct file filter hydrated payload owners");

        crate::tracked_state::reset_commit_delta_scan_probe_for_test();
        let checkpoint = replica
            .lix
            .execute(
                &working_diff_sql(
                    &replica.lix,
                    "lix_file",
                    "INSERT INTO lix_create_checkpoint (relation, row_pk) \
                     SELECT 'lix_file', lixcol_row_pk FROM __LIX_RELATION_DIFF__ \
                     WHERE lixcol_row_pk ->> 0 = $1 RETURNING commit_id",
                )
                .await,
                &[Value::Text(selected_file_id.clone())],
            )
            .await
            .expect("packed partial checkpoint should stay local");
        assert!(checkpoint.rows_affected() > 0);
        let checkpoint_ids = checkpoint
            .rows()
            .iter()
            .map(|row| row.get::<String>("commit_id").unwrap())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(checkpoint_ids.len(), 1, "one partial checkpoint was created");
        let (checkpoint_compact_scans, payload_scans) =
            crate::tracked_state::take_commit_delta_scan_probe_for_test();
        assert_eq!(
            payload_scans, 0,
            "partial checkpoint hydrated historical payload owners",
        );
        compact_scan_counts.push((direct_compact_scans, checkpoint_compact_scans));

        let selected_remaining = replica
            .lix
            .execute(
                &working_diff_sql(
                    &replica.lix,
                    "lix_file",
                    "SELECT count(*) AS count FROM __LIX_RELATION_DIFF__ WHERE lixcol_row_pk ->> 0 = $1",
                )
                .await,
                &[Value::Text(selected_file_id)],
            )
            .await
            .expect("selected packed file diff should be cleared");
        assert_eq!(
            selected_remaining.rows()[0].get::<i64>("count").unwrap(),
            0,
            "partial checkpoint cleared the wrong file",
        );

        let remaining = replica
            .lix
            .execute(
                &working_diff_sql(
                    &replica.lix,
                    "lix_file",
                    "SELECT count(*) AS count FROM __LIX_RELATION_DIFF__",
                )
                .await,
                &[],
            )
            .await
            .expect("remaining packed working diff should stay locally readable")
            .rows()[0]
            .get::<i64>("count")
            .expect("remaining packed file count should decode");
        assert_eq!(remaining, expected_remaining as i64);
        replica.restart().await;
    }
    assert!(
        compact_scan_counts[0].0 > 0 && compact_scan_counts[0].1 > 0,
        "the first direct filter and checkpoint must both traverse compact packed authority",
    );
}

async fn partial_checkpoint_after_partial_checkpoint_snapshot_stays_hot(_sim: Simulation) {
	let authority = fresh_authority().await;
	write_key_value(&authority, "selected", "baseline").await;
	write_key_value(&authority, "remaining", "baseline").await;
	authority.create_checkpoint().await.unwrap();
	let mut replica = Replica::bootstrap(AuthorityTransport::connected(authority)).await;
	replica.write("selected", "local-selected").await;
	replica.write("remaining", "local-remaining").await;
	let first_selected_row_pk = replica
		.lix
		.execute(
			&working_diff_sql(
				&replica.lix,
				"lix_key_value",
				"SELECT lixcol_row_pk FROM __LIX_RELATION_DIFF__ \
				 WHERE lixcol_row_pk = CAST('[\"selected\"]' AS JSONB)",
			)
			.await,
			&[],
		)
		.await
		.unwrap()
		.rows()[0]
		.get::<serde_json::Value>("lixcol_row_pk")
		.unwrap();
	replica
		.lix
		.execute(
			"INSERT INTO lix_create_checkpoint (relation, row_pk) \
			 SELECT 'lix_key_value', $1 RETURNING commit_id",
			&[Value::Jsonb(first_selected_row_pk.into())],
		)
		.await
		.unwrap();
	replica.pump().await.unwrap();

	assert_eq!(
		replica
			.lix
			.execute(
				&working_diff_sql(
					&replica.lix,
					"lix_key_value",
					"SELECT COUNT(*) AS count FROM __LIX_RELATION_DIFF__",
				)
				.await,
				&[],
			)
			.await
			.unwrap()
			.rows()[0]
			.get::<i64>("count")
			.unwrap(),
		1,
		"the unselected change must remain dirty after the first partial checkpoint",
	);

	// The next write used to fail because the branch control pointed at the new
	// checkpoint while the sparse working-diff epoch still pointed at the old
	// checkpoint and generation.
	replica.write("selected", "local-selected-again").await;
	let selected_row_pk = replica
		.lix
		.execute(
			&working_diff_sql(
				&replica.lix,
				"lix_key_value",
				"SELECT lixcol_row_pk FROM __LIX_RELATION_DIFF__ \
				 WHERE lixcol_row_pk = CAST('[\"selected\"]' AS JSONB)",
			)
			.await,
			&[],
		)
		.await
		.unwrap()
		.rows()[0]
		.get::<serde_json::Value>("lixcol_row_pk")
		.unwrap();
	let head_commit_id = replica
		.lix
		.execute("SELECT lix_active_branch_commit_id() AS id", &[])
		.await
		.unwrap()
		.rows()[0]
		.get::<String>("id")
		.unwrap();
	let branch_id = replica.lix.active_branch_id().await.unwrap();
	let adapter = replica.lix.storage_adapter();
	let read = adapter
		.begin_read(crate::storage_adapter::StorageReadOptions::default())
		.await
		.unwrap();
	let checkpoint_commit_id = crate::checkpoint::checkpoint_commit_id_at_head(
		&read,
		&branch_id,
		crate::changelog::CommitId::parse_lix(&head_commit_id, "partial snapshot head").unwrap(),
	)
	.await
	.unwrap()
	.to_string();
	drop(read);
	crate::tracked_state::arm_diff_commits_test_probe(&checkpoint_commit_id, &head_commit_id);

	replica
		.lix
		.execute(
			"INSERT INTO lix_create_checkpoint (relation, row_pk) \
			 SELECT 'lix_key_value', $1 RETURNING commit_id",
			&[Value::Jsonb(selected_row_pk.into())],
		)
		.await
		.unwrap();
	assert_eq!(
		crate::tracked_state::take_diff_commits_test_probe(
			&checkpoint_commit_id,
			&head_commit_id,
		),
		0,
		"a partial-checkpoint snapshot must not reconstruct working diff history",
	);
	assert_eq!(
		replica
			.lix
			.execute(
				&working_diff_sql(
					&replica.lix,
					"lix_key_value",
					"SELECT COUNT(*) AS count FROM __LIX_RELATION_DIFF__",
				)
				.await,
				&[],
			)
			.await
			.unwrap()
			.rows()[0]
			.get::<i64>("count")
			.unwrap(),
		1,
		"the second partial checkpoint must retain only the unselected change",
	);
}

async fn partial_checkpoint_uncertified_index_uses_hot_primary_fallback(_sim: Simulation) {
    let authority = fresh_authority().await;
    write_key_value(&authority, "selected", "baseline").await;
    write_key_value(&authority, "remaining", "baseline").await;
    authority.create_checkpoint().await.unwrap();
    write_key_value(&authority, "selected", "selected-update").await;
    write_key_value(&authority, "remaining", "remaining-update").await;
    let replica = Replica::bootstrap(AuthorityTransport::connected(authority)).await;

    let branch_id = replica.lix.active_branch_id().await.unwrap();
    let adapter = replica.lix.storage_adapter();
    let read = adapter
        .begin_read(crate::storage_adapter::StorageReadOptions::default())
        .await
        .unwrap();
    let control = crate::branch::BranchHeadControlContext::new()
        .reader(&read)
        .load(&branch_id)
        .await
        .unwrap()
        .unwrap();
    let checkpoint_commit_id = control.working_diff_checkpoint_commit_id.unwrap();
    drop(read);
    let mut writes = adapter.new_write_set();
    crate::hot_state::stage_tracked_working_diff_epoch(
        &mut writes,
        &branch_id,
        crate::hot_state::TrackedWorkingDiffEpoch {
            checkpoint_commit_id,
            generation: control.tracked_generation,
            coverage: crate::hot_state::WorkingDiffIndexCoverage::default(),
        },
    )
    .unwrap();
    adapter
        .commit_write_set(
            writes,
            crate::storage_adapter::StorageWriteOptions::default(),
        )
        .await
        .unwrap();

    let read = adapter
        .begin_read(crate::storage_adapter::StorageReadOptions::default())
        .await
        .unwrap();
    let direct = crate::hot_state::TrackedHeadContext::new()
        .reader(read)
        .working_diff_for_control(
            &branch_id,
            control,
            &crate::tracked_state::TrackedStateDiffRequest::default(),
        )
        .await
        .unwrap()
        .expect("authoritative HOT rows must survive an uncertified sparse index");
    assert_eq!(direct.diff.entries.len(), 2);

    let head_commit_id = control.head_commit_id.to_string();
    crate::tracked_state::arm_diff_commits_test_probe(
        &checkpoint_commit_id.to_string(),
        &head_commit_id,
    );
    crate::tracked_state::arm_point_replay_authority_batch_probe_for_test();
    replica
        .lix
        .execute(
            // Keep the source intentionally non-pushdownable so the provider
            // must answer a broad working-diff read before SQL filters it.
            &working_diff_sql(
                &replica.lix,
                "lix_key_value",
                "INSERT INTO lix_create_checkpoint (relation, row_pk) \
                 SELECT 'lix_key_value', lixcol_row_pk FROM __LIX_RELATION_DIFF__ \
                 WHERE coalesce(lixcol_row_pk ->> 0, '') = 'selected' RETURNING commit_id",
            )
            .await,
            &[],
        )
        .await
        .expect("HOT primary fallback must retain complete lifecycle publication");
    assert_eq!(
        crate::tracked_state::take_diff_commits_test_probe(
            &checkpoint_commit_id.to_string(),
            &head_commit_id,
        ),
        0,
        "an uncertified derived index must not hydrate canonical history while HOT rows are authoritative",
    );
    assert!(
        crate::tracked_state::take_point_replay_authority_batch_probe_for_test().is_empty(),
        "HOT primary fallback must not demand cold owner snapshots",
    );
    let remaining = replica
        .lix
        .execute(
            &working_diff_sql(
                &replica.lix,
                "lix_key_value",
                "SELECT lixcol_row_pk ->> 0 AS key FROM __LIX_RELATION_DIFF__",
            )
            .await,
            &[],
        )
        .await
        .unwrap();
    assert_eq!(remaining.rows().len(), 1);
    assert_eq!(remaining.rows()[0].get::<String>("key").unwrap(), "remaining");
}

async fn partial_checkpoint_rebases_unselected_tombstone(_sim: Simulation) {
    let authority = fresh_authority().await;
    write_key_value(&authority, "selected", "baseline").await;
    write_key_value(&authority, "removed", "baseline").await;
    authority.create_checkpoint().await.unwrap();
    let mut replica = Replica::bootstrap(AuthorityTransport::connected(authority.clone())).await;
    write_key_value(&authority, "selected", "selected-update").await;
    authority
        .execute("DELETE FROM lix_key_value WHERE key = 'removed'", &[])
        .await
        .unwrap();
    replica.pump().await.unwrap();
    let selected_row_pk = replica
        .lix
        .execute(
            &working_diff_sql(
                &replica.lix,
                "lix_key_value",
                "SELECT lixcol_row_pk FROM __LIX_RELATION_DIFF__ \
                 WHERE lixcol_row_pk = CAST('[\"selected\"]' AS JSONB)",
            )
            .await,
            &[],
        )
        .await
        .unwrap()
        .rows()[0]
        .get::<serde_json::Value>("lixcol_row_pk")
        .unwrap();

    crate::tracked_state::arm_point_replay_authority_batch_probe_for_test();
    replica
        .lix
        .execute(
            "INSERT INTO lix_create_checkpoint (relation, row_pk) \
             SELECT 'lix_key_value', $1 RETURNING commit_id",
            &[Value::Jsonb(selected_row_pk.into())],
        )
        .await
        .unwrap();
    assert!(
        crate::tracked_state::take_point_replay_authority_batch_probe_for_test().is_empty(),
        "partial checkpoint tombstone rebase must stay HOT",
    );
    let remaining = replica
        .lix
        .execute(
            &working_diff_sql(
                &replica.lix,
                "lix_key_value",
                "SELECT lixcol_diff_type, lixcol_row_pk FROM __LIX_RELATION_DIFF__",
            )
            .await,
            &[],
        )
        .await
        .unwrap();
    assert_eq!(remaining.rows().len(), 1);
    assert_eq!(
        remaining.rows()[0]
            .get::<String>("lixcol_diff_type")
            .unwrap(),
        "removed"
    );
    assert_eq!(
        remaining.rows()[0]
            .get::<serde_json::Value>("lixcol_row_pk")
            .unwrap(),
        serde_json::json!(["removed"]),
    );
    replica.restart().await;
    assert_eq!(
        replica
            .lix
            .execute(
                &working_diff_sql(
                    &replica.lix,
                    "lix_key_value",
                    "SELECT COUNT(*) AS count FROM __LIX_RELATION_DIFF__ \
                     WHERE lixcol_diff_type = 'removed'",
                )
                .await,
                &[],
            )
            .await
            .unwrap()
            .rows()[0]
            .get::<i64>("count")
            .unwrap(),
        1,
    );
}

async fn pre_v75_partial_checkpoint_repository_is_rejected(_sim: Simulation) {
	let authority = fresh_authority().await;
	write_key_value(&authority, "selected", "baseline").await;
	write_key_value(&authority, "remaining", "baseline").await;
	authority.create_checkpoint().await.unwrap();
	let mut replica = Replica::bootstrap(AuthorityTransport::connected(authority)).await;
	let branch_id = replica.lix.active_branch_id().await.unwrap();
	let adapter = replica.lix.storage_adapter();
	let migration_adapter = adapter.clone();
	let read = adapter
		.begin_read(crate::storage_adapter::StorageReadOptions::default())
		.await
		.unwrap();
	let old_epoch = crate::hot_state::TrackedHeadContext::new()
		.reader(&read)
		.working_diff_epoch(&branch_id)
		.await
		.unwrap()
		.unwrap();
	drop(read);

	replica.write("selected", "local-selected").await;
	replica.write("remaining", "local-remaining").await;
	let selected_row_pk = replica
		.lix
		.execute(
			&working_diff_sql(
				&replica.lix,
				"lix_key_value",
				"SELECT lixcol_row_pk FROM __LIX_RELATION_DIFF__ \
				 WHERE lixcol_row_pk = CAST('[\"selected\"]' AS JSONB)",
			)
			.await,
			&[],
		)
		.await
		.unwrap()
		.rows()[0]
		.get::<serde_json::Value>("lixcol_row_pk")
		.unwrap();
	replica
		.lix
		.execute(
			"INSERT INTO lix_create_checkpoint (relation, row_pk) \
			 SELECT 'lix_key_value', $1 RETURNING commit_id",
			&[Value::Jsonb(selected_row_pk.into())],
		)
		.await
		.unwrap();
	replica.pump().await.unwrap();

	let read = adapter
		.begin_read(crate::storage_adapter::StorageReadOptions::default())
		.await
		.unwrap();
	let control = crate::branch::BranchHeadControlContext::new()
		.reader(&read)
		.load(&branch_id)
		.await
		.unwrap()
		.unwrap();
	drop(read);
	let checkpoint_commit_id = control
		.working_diff_checkpoint_commit_id
		.unwrap()
		.to_string();
	let head_commit_id = control.head_commit_id.to_string();
	let mut writes = adapter.new_write_set();
	crate::hot_state::stage_tracked_working_diff_epoch(
		&mut writes,
		&branch_id,
		old_epoch,
	)
	.unwrap();
	writes.put(
		crate::init::REPOSITORY_PROTOCOL_SPACE,
		crate::storage_adapter::StorageKey(bytes::Bytes::from_static(
			crate::init::REPOSITORY_PROTOCOL_KEY,
		)),
		crate::storage_adapter::StorageValue {
			bytes: bytes::Bytes::from_static(b"tracked-default-branch.v71"),
		},
	);
	adapter
		.commit_write_set(
			writes,
			crate::storage_adapter::StorageWriteOptions::default(),
		)
		.await
		.unwrap();
	crate::tracked_state::arm_diff_commits_test_probe(&checkpoint_commit_id, &head_commit_id);

	replica.lix.close().await.unwrap();
	let error = crate::migration::migrate_lix_with_adapter(
		replica.storage.clone(),
		migration_adapter,
		crate::migration::MigrationOptions::default(),
	)
	.await
	.expect_err("the complete-snapshot hard cut must reject a pre-v72 repository");
	assert_eq!(error.code, "LIX_ERROR_MIGRATION_FAILED");
	assert!(error.message.contains("predates the v76 complete-snapshot"));
	let _ = crate::tracked_state::take_diff_commits_test_probe(
		&checkpoint_commit_id,
		&head_commit_id,
	);
}

async fn deterministic_sync_command_traces(_sim: Simulation) {
    for seed in fuzz_seeds(&(0_u64..32).collect::<Vec<_>>()) {
        // Each seed starts from an independent repository, so an override can
        // reproduce that seed without replaying every smaller seed first.
        let authority = fresh_authority().await;
        let mut left = Replica::bootstrap(AuthorityTransport::connected(authority.clone())).await;
        let mut right = Replica::bootstrap(AuthorityTransport::connected(authority.clone())).await;
        let mut random = XorShift64::new(seed);
        for step in 0..COMMANDS_PER_SEED {
            let command = random.next() % 8;
            match command {
                0 | 1 => {
                    left.write(&format!("seed-{seed}-left-{step}"), "left")
                        .await
                }
                2 | 3 => {
                    right
                        .write(&format!("seed-{seed}-right-{step}"), "right")
                        .await
                }
                4 => {
                    left.pump().await.unwrap_or_else(|error| {
                        panic!("seed {seed} step {step} left pump failed: {error:?}")
                    });
                }
                5 => {
                    right.pump().await.unwrap_or_else(|error| {
                        panic!("seed {seed} step {step} right pump failed: {error:?}")
                    });
                }
                6 => left.restart().await,
                _ => right.restart().await,
            }
        }
        if let Err(error) = converge(&authority, &mut [&mut left, &mut right]).await {
            panic!("seed {seed} failed after {COMMANDS_PER_SEED} commands: {error:?}");
        }
    }
}

sync_simulation_test!(
    deterministic_replica_scenarios,
    deterministic_replica_scenarios
);
sync_simulation_test!(
    lazy_history_and_binary_cas_scenarios,
    lazy_history_and_binary_cas_scenarios
);
sync_simulation_test!(
    deterministic_sync_command_traces,
    deterministic_sync_command_traces
);
sync_simulation_test!(
    sparse_partial_checkpoint_uses_hot_working_diff,
    sparse_partial_checkpoint_uses_hot_working_diff
);
sync_simulation_test!(
	snapshot_partial_checkpoint_uses_local_selected_payloads,
	snapshot_partial_checkpoint_uses_local_selected_payloads
);
sync_simulation_test!(
	partial_checkpoint_rebases_hot_epoch_without_cold_history,
	partial_checkpoint_rebases_hot_epoch_without_cold_history
);
sync_simulation_test!(
    partial_file_checkpoint_rebases_hot_epoch,
    partial_file_checkpoint_rebases_hot_epoch
);
sync_simulation_test!(
    packed_recreate_partial_checkpoint_stays_hot,
    packed_recreate_partial_checkpoint_stays_hot
);
sync_simulation_test!(
    packed_snapshot_partial_file_checkpoint_stays_payload_local,
    packed_snapshot_partial_file_checkpoint_stays_payload_local
);
sync_simulation_test!(
	partial_checkpoint_after_partial_checkpoint_snapshot_stays_hot,
	partial_checkpoint_after_partial_checkpoint_snapshot_stays_hot
);
sync_simulation_test!(
    partial_checkpoint_uncertified_index_uses_hot_primary_fallback,
    partial_checkpoint_uncertified_index_uses_hot_primary_fallback
);
sync_simulation_test!(
    partial_checkpoint_rebases_unselected_tombstone,
    partial_checkpoint_rebases_unselected_tombstone
);
sync_simulation_test!(
	pre_v75_partial_checkpoint_repository_is_rejected,
	pre_v75_partial_checkpoint_repository_is_rejected
);

struct XorShift64(u64);

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_add(0x9e37_79b9_7f4a_7c15))
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}
