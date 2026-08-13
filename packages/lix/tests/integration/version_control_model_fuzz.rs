//! Differential correctness harness for the version-control semantics of the
//! storage and transaction layer.
//!
//! # Why this exists next to the suites that already fuzz
//!
//! `storage::conformance::model_based` already runs a reference model against
//! the raw key/value storage adapters, and `merge_fuzz` already replays random
//! branch traffic. Neither models the surfaces that make lix *unlike* a
//! key/value store: what `lix_working_diff` reports relative to the last
//! checkpoint, what `lix_<schema>_history()` reports per commit, what survives
//! a reboot that replays the commit log, and what survives checkpoint GC.
//! Those are the surfaces a physical-layout break silently gets wrong, so
//! those are the surfaces this file models.
//!
//! # The model
//!
//! [`BranchModel`] is an in-memory map with the same *intended* semantics:
//!
//! - `state` is what `SELECT ... FROM lix_key_value` must return.
//! - `checkpoint_state` is the branch state as of the last checkpoint, which is
//!   the basis `lix_working_diff` compares the working state against.
//! - `history` is, per key, the newest-first sequence of values the key took at
//!   the checkpoints where it changed. The engine collapses a whole
//!   un-checkpointed interval into one commit, so a checkpoint contributes at
//!   most one history entry per key, and contributes none for a key whose value
//!   at the checkpoint equals its value at the previous checkpoint.
//!
//! History is therefore only asserted immediately after a checkpoint, where the
//! open interval is empty and the model's rule is exact. Asserting a modelled
//! `lixcol_depth` is deliberately out of scope: depth is commit-graph distance,
//! which merges make multi-parent. The harness asserts the *sequence* of values
//! and that depths increase strictly, which is what catches a dropped,
//! duplicated, or reordered history entry.
//!
//! # Verifying by inversion
//!
//! A property that cannot fail is worse than no property. `LIX_VC_MODEL_INJECT`
//! deliberately corrupts the model so every assertion below can be shown to
//! fire. See [`InjectedFault`]. The switch is read once per test and defaults
//! to [`InjectedFault::None`], so a normal run is unaffected.
//!
//! ```text
//! for fault in state working_diff history merge reboot reclaim gc_history; do
//!   LIX_VC_MODEL_INJECT=$fault cargo test --profile test -p lix \
//!     --test integration --all-features -- --test-threads=1 vc_model_
//! done   # every one of these must fail
//! ```
//!
//! # A defect this found
//!
//! On its first green run this harness found a checkpoint GC sweep truncating
//! entity history that `gc.rs` documents as load-bearing. That is fixed;
//! [`assert_history_survived_gc`] is now the guard, and asserts equality.

use std::collections::BTreeMap;

use lix::{CreateBranchOptions, MergeBranchOptions, MergeBranchOutcome, Value};
use serde_json::Value as JsonValue;
use tokio::time::{Duration, Instant};

use crate::support;
use crate::support::simulation_test::engine::SimSession;

/// Seeds replayed by the randomized suites. Overridable through
/// `LIX_FUZZ_SEEDS` / `LIX_FUZZ_SEED_START`, like the other `*_fuzz` suites.
const DEFAULT_SEEDS: [u64; 6] = [0, 1, 2, 0x51ce_deed, u64::MAX - 1, u64::MAX];
const STEPS_PER_SEED: usize = 40;
const KEYS_PER_LANE: usize = 5;

/// Checkpoints the engine keeps before a sweep becomes eligible. Mirrors
/// `CHECKPOINT_GC_MIN_AGE` in `session::checkpoint`, which is private.
const CHECKPOINT_GC_INTERVAL: usize = 64;

/// Deliberate model corruptions used to prove each property can fail.
///
/// Each variant breaks the *model* rather than the engine, which is what makes
/// it safe to ship: it proves the assertion reads real engine output and
/// compares it against the modelled value, so a divergence in either direction
/// is reported rather than swallowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InjectedFault {
    None,
    /// Drop the newest write from the modelled state.
    State,
    /// Report a modified row as added in the modelled working diff.
    WorkingDiff,
    /// Drop the newest modelled history entry.
    History,
    /// Forget one merged key in the modelled post-merge state.
    Merge,
    /// Assert the pre-reboot state against a mutated expectation.
    Reboot,
    /// Assert the post-GC snapshot against a mutated expectation.
    Reclaim,
    /// Mutate the pre-sweep history the post-sweep history is compared against.
    GcHistory,
}

impl InjectedFault {
    fn from_env() -> Self {
        match std::env::var("LIX_VC_MODEL_INJECT")
            .unwrap_or_default()
            .trim()
        {
            "" | "none" => Self::None,
            "state" => Self::State,
            "working_diff" => Self::WorkingDiff,
            "history" => Self::History,
            "merge" => Self::Merge,
            "reboot" => Self::Reboot,
            "reclaim" => Self::Reclaim,
            "gc_history" => Self::GcHistory,
            other => panic!(
                "LIX_VC_MODEL_INJECT must be one of \
                 none|state|working_diff|history|merge|reboot|reclaim|gc_history, \
                 got {other:?}"
            ),
        }
    }
}

/// In-memory reference model for one branch.
#[derive(Debug, Default, Clone)]
struct BranchModel {
    state: BTreeMap<String, JsonValue>,
    checkpoint_state: BTreeMap<String, JsonValue>,
    /// Newest-first per-key value sequence across checkpoints. `None` is a
    /// delete, which the engine reports as a null value with
    /// `lixcol_is_deleted = true`.
    history: BTreeMap<String, Vec<Option<JsonValue>>>,
    /// Keys written since the last checkpoint. Presence, not value: the value
    /// that matters at collapse time is the one in `state`.
    touched_since_checkpoint: Vec<String>,
}

impl BranchModel {
    fn upsert(&mut self, key: &str, value: JsonValue) {
        self.state.insert(key.to_string(), value);
        self.touch(key);
    }

    fn delete(&mut self, key: &str) {
        self.state.remove(key);
        self.touch(key);
    }

    fn touch(&mut self, key: &str) {
        if !self.touched_since_checkpoint.iter().any(|seen| seen == key) {
            self.touched_since_checkpoint.push(key.to_string());
        }
    }

    /// Collapses the open interval the way `create_checkpoint` does: one
    /// history entry per key whose value actually moved since the previous
    /// checkpoint, and none for a key that came back to where it started.
    fn checkpoint(&mut self) {
        let touched = std::mem::take(&mut self.touched_since_checkpoint);
        for key in touched {
            let before = self.checkpoint_state.get(&key).cloned();
            let after = self.state.get(&key).cloned();
            if before == after {
                continue;
            }
            self.history.entry(key).or_default().insert(0, after);
        }
        self.checkpoint_state = self.state.clone();
    }

    /// The rows `lix_working_diff` must report, as `(key, diff_type)` sorted by
    /// key.
    fn working_diff(&self) -> Vec<(String, &'static str)> {
        let mut diff = Vec::new();
        for (key, value) in &self.state {
            match self.checkpoint_state.get(key) {
                None => diff.push((key.clone(), "added")),
                Some(before) if before != value => diff.push((key.clone(), "modified")),
                Some(_) => {}
            }
        }
        for key in self.checkpoint_state.keys() {
            if !self.state.contains_key(key) {
                diff.push((key.clone(), "removed"));
            }
        }
        diff.sort();
        diff
    }
}

// ---------------------------------------------------------------------------
// Property 1/2/3: state, working diff and history against the model.
// ---------------------------------------------------------------------------

simulation_test!(
    vc_model_single_branch_state_diff_and_history_match_the_model,
    |sim| async move {
        let fault = InjectedFault::from_env();
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );

        for seed in support::fuzz_seeds(&DEFAULT_SEEDS) {
            let prefix = format!("vcm-single-{seed:016x}-");
            let keys = lane_keys(&prefix, "k");
            let mut model = BranchModel::default();
            let mut rng = TinyRng::new(seed);

            for step in 0..STEPS_PER_SEED {
                let label = format!("seed {seed:#018x}, step {step}");
                match rng.usize(10) {
                    0..=5 => {
                        let key = keys[rng.usize(keys.len())].clone();
                        let value = random_value(&mut rng);
                        upsert(&main, &key, &value, &label).await;
                        model.upsert(&key, value);
                        if fault == InjectedFault::State {
                            model.state.remove(&key);
                        }
                    }
                    6..=7 => {
                        let key = keys[rng.usize(keys.len())].clone();
                        delete(&main, &key, &label).await;
                        model.delete(&key);
                    }
                    _ => {
                        main.create_checkpoint()
                            .await
                            .unwrap_or_else(|error| panic!("{label}: checkpoint failed: {error:?}"));
                        model.checkpoint();
                        if fault == InjectedFault::History {
                            if let Some(entries) =
                                model.history.values_mut().find(|entries| !entries.is_empty())
                            {
                                entries.remove(0);
                            }
                        }
                        assert_history(&main, &prefix, &model, &label).await;
                    }
                }

                assert_state(&main, &prefix, &model.state, &label).await;
                let mut expected_diff = model.working_diff();
                if fault == InjectedFault::WorkingDiff {
                    if let Some(entry) = expected_diff
                        .iter_mut()
                        .find(|(_, verb)| *verb == "modified")
                    {
                        entry.1 = "added";
                    }
                }
                assert_working_diff(&main, &prefix, &expected_diff, &label).await;
            }

            // Land the tail of the sequence so the next seed starts from a
            // checkpointed branch, and assert the collapsed history one final
            // time.
            main.create_checkpoint()
                .await
                .unwrap_or_else(|error| panic!("seed {seed:#018x}: final checkpoint: {error:?}"));
            model.checkpoint();
            assert_state(&main, &prefix, &model.state, "final").await;
            assert_history(&main, &prefix, &model, "final").await;
        }
    }
);

// ---------------------------------------------------------------------------
// Property 4: branch divergence and merge.
// ---------------------------------------------------------------------------

simulation_test!(
    vc_model_branch_divergence_and_merge_match_the_model,
    |sim| async move {
        let fault = InjectedFault::from_env();
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );

        for seed in support::fuzz_seeds(&DEFAULT_SEEDS) {
            let prefix = format!("vcm-merge-{seed:016x}-");
            // Disjoint lanes: the merge result is then the exact union, with no
            // three-way resolution to model. Overlapping-write merges are left
            // to `merge_fuzz`, which already covers conflict reporting.
            let main_keys = lane_keys(&prefix, "main");
            let side_keys = lane_keys(&prefix, "side");
            let mut main_model = BranchModel::default();
            let mut side_model = BranchModel::default();
            let mut rng = TinyRng::new(seed ^ 0x9e37_79b9_7f4a_7c15);

            let branch_id = format!(
                "01930001-{:04x}-7000-8000-{:012x}",
                seed & 0xffff,
                seed & 0xffff_ffff_ffff
            );
            let receipt = main
                .create_branch(CreateBranchOptions {
                    id: Some(branch_id.clone()),
                    name: format!("vcm-merge-{seed:016x}"),
                    from_commit_id: None,
                })
                .await
                .unwrap_or_else(|error| panic!("seed {seed:#018x}: create branch: {error:?}"));
            let side = sim.wrap_session(
                engine
                    .open_session_at(receipt.id.clone())
                    .await
                    .unwrap_or_else(|error| panic!("seed {seed:#018x}: open branch: {error:?}")),
                &engine,
            );
            // The fork inherits main's state, so the side model starts from it.
            side_model.state = main_model.state.clone();
            side_model.checkpoint_state = main_model.checkpoint_state.clone();

            let mut main_advanced_since_merge_base = false;
            let mut side_advanced_since_merge_base = false;

            for step in 0..STEPS_PER_SEED {
                let label = format!("seed {seed:#018x}, step {step}");
                let on_side = rng.usize(2) == 0;
                let (session, model, keys, advanced) = if on_side {
                    (
                        &side,
                        &mut side_model,
                        &side_keys,
                        &mut side_advanced_since_merge_base,
                    )
                } else {
                    (
                        &main,
                        &mut main_model,
                        &main_keys,
                        &mut main_advanced_since_merge_base,
                    )
                };

                match rng.usize(10) {
                    0..=6 => {
                        let key = keys[rng.usize(keys.len())].clone();
                        let value = random_value(&mut rng);
                        upsert(session, &key, &value, &label).await;
                        // Values are random, so an upsert always moves the row
                        // and therefore always creates a commit.
                        model.upsert(&key, value);
                        *advanced = true;
                    }
                    7..=8 => {
                        let key = keys[rng.usize(keys.len())].clone();
                        delete(session, &key, &label).await;
                        // A delete that matches no row is not a change and does
                        // not advance the branch, so it must not count towards
                        // fast-forward eligibility. Establishing this cost one
                        // failing run; it is the kind of thing the model exists
                        // to pin down.
                        let existed = model.state.contains_key(&key);
                        model.delete(&key);
                        *advanced |= existed;
                    }
                    _ => {
                        let receipt = main
                            .merge_branch(MergeBranchOptions {
                                source_branch_id: branch_id.clone(),
                            })
                            .await
                            .unwrap_or_else(|error| panic!("{label}: merge failed: {error:?}"));

                        let expected_outcome = if !side_advanced_since_merge_base {
                            MergeBranchOutcome::AlreadyUpToDate
                        } else if main_advanced_since_merge_base {
                            MergeBranchOutcome::MergeCommitted
                        } else {
                            MergeBranchOutcome::FastForward
                        };
                        assert_eq!(
                            receipt.outcome, expected_outcome,
                            "{label}: merge outcome diverged from the model"
                        );

                        // Disjoint lanes: main keeps its own lane and adopts the
                        // side lane wholesale.
                        for key in &side_keys {
                            main_model.state.remove(key);
                        }
                        for key in &side_keys {
                            if let Some(value) = side_model.state.get(key) {
                                main_model.state.insert(key.clone(), value.clone());
                            }
                        }
                        if fault == InjectedFault::Merge {
                            if let Some(key) = side_keys
                                .iter()
                                .find(|key| main_model.state.contains_key(*key))
                                .cloned()
                            {
                                main_model.state.remove(&key);
                            }
                        }
                        main_advanced_since_merge_base = false;
                        side_advanced_since_merge_base = false;
                    }
                }

                assert_state(&main, &prefix, &main_model.state, &format!("{label} main")).await;
                assert_state(&side, &prefix, &side_model.state, &format!("{label} side")).await;
            }
        }
    }
);

// ---------------------------------------------------------------------------
// Property 5: replay determinism across a reboot, including past the replay
// fence at COMMIT_STATE_MAX_REPLAY_DEPTH = 32.
// ---------------------------------------------------------------------------

simulation_test!(vc_model_reboot_replays_to_identical_state, |sim| async move {
    let fault = InjectedFault::from_env();
    let engine = sim.boot_engine().await;
    let main = sim.wrap_session(
        engine.open_session().await.expect("session should open"),
        &engine,
    );

    // One commit per write, no checkpoint: this is what pushes the replay debt
    // past COMMIT_STATE_MAX_REPLAY_DEPTH (32) and exercises the fence rather
    // than the checkpointed fast path.
    const UNCHECKPOINTED_COMMITS: usize = 48;

    let prefix = "vcm-replay-";
    let keys = lane_keys(prefix, "k");
    let mut model = BranchModel::default();
    let mut rng = TinyRng::new(0xd1ce_f00d);

    for step in 0..UNCHECKPOINTED_COMMITS {
        let label = format!("pre-reboot step {step}");
        let key = keys[rng.usize(keys.len())].clone();
        if rng.usize(5) == 0 {
            delete(&main, &key, &label).await;
            model.delete(&key);
        } else {
            let value = random_value(&mut rng);
            upsert(&main, &key, &value, &label).await;
            model.upsert(&key, value);
        }
    }
    assert_state(&main, prefix, &model.state, "before reboot").await;

    if fault == InjectedFault::Reboot {
        model.state.insert(
            format!("{prefix}k-0"),
            JsonValue::String("injected".to_string()),
        );
    }

    // Replay once...
    let rebooted = sim
        .reboot_engine_from_current_snapshot()
        .await
        .expect("engine should reboot from the current snapshot");
    let reopened = sim.wrap_session(
        rebooted
            .open_session()
            .await
            .expect("rebooted session should open"),
        &rebooted,
    );
    assert_state(&reopened, prefix, &model.state, "after first reboot").await;

    // ...and again: replay must be idempotent, not merely correct once.
    let rebooted_again = sim
        .reboot_engine_from_current_snapshot()
        .await
        .expect("engine should reboot a second time");
    let reopened_again = sim.wrap_session(
        rebooted_again
            .open_session()
            .await
            .expect("second rebooted session should open"),
        &rebooted_again,
    );
    assert_state(&reopened_again, prefix, &model.state, "after second reboot").await;

    // A checkpoint after the fence-crossing replay must still land, and the
    // collapsed history must be the model's.
    reopened_again
        .create_checkpoint()
        .await
        .expect("checkpoint after replay should succeed");
    model.checkpoint();
    assert_history(&reopened_again, prefix, &model, "after replay checkpoint").await;
});

// ---------------------------------------------------------------------------
// Property 6: reclaim safety. State and history reachable before a sweep must
// be readable after it.
// ---------------------------------------------------------------------------

simulation_test!(
    vc_model_state_and_history_survive_checkpoint_gc,
    |sim| async move {
        let fault = InjectedFault::from_env();
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );

        let prefix = "vcm-reclaim-";
        let keys = lane_keys(prefix, "k");
        let mut model = BranchModel::default();
        let mut rng = TinyRng::new(0x0bad_c0de);

        // Build history the sweep must not damage, and record intra-interval
        // commits the sweep is expected to reclaim. Those recorded commits are
        // the engagement check: without them a passing run cannot distinguish
        // "GC ran and preserved everything" from "GC never ran".
        let mut reclaimable_commits = Vec::new();
        for round in 0..6 {
            for write in 0..3 {
                let key = keys[rng.usize(keys.len())].clone();
                let label = format!("seed round {round}, write {write}");
                if rng.usize(4) == 0 {
                    delete(&main, &key, &label).await;
                    model.delete(&key);
                } else {
                    let value = random_value(&mut rng);
                    upsert(&main, &key, &value, &label).await;
                    model.upsert(&key, value);
                }
                // Round 0's first commit is deliberately not recorded: it is
                // the branch's oldest interval anchor and the collector keeps
                // it, so requiring its removal would make this check fail for
                // a reason that has nothing to do with reclaim safety. Every
                // later round's intra-interval commit is required to go, which
                // is what proves a sweep actually ran.
                if write == 0 && round > 0 {
                    reclaimable_commits.push(branch_head(&engine, sim.main_branch_id()).await);
                }
            }
            main.create_checkpoint()
                .await
                .expect("round checkpoint should succeed");
            model.checkpoint();
        }

        assert_state(&main, prefix, &model.state, "before gc").await;
        assert_history(&main, prefix, &model, "before gc").await;
        let mut history_before = read_history(&main, prefix).await;

        // Cross the collection interval.
        for _ in 0..CHECKPOINT_GC_INTERVAL {
            main.create_checkpoint()
                .await
                .expect("padding checkpoint should succeed");
            model.checkpoint();
        }

        // Engagement check: the sweep is asynchronous, so wait for it, then
        // assert. A run where nothing was ever reclaimed proves nothing about
        // reclaim safety and must fail here rather than pass silently.
        wait_until_reclaimed(&main, &reclaimable_commits).await;

        if fault == InjectedFault::Reclaim {
            model
                .state
                .insert(format!("{prefix}k-0"), JsonValue::String("injected".into()));
        }

        // Current state must survive the sweep exactly. This is the strongest
        // reclaim property and it holds today.
        assert_state(&main, prefix, &model.state, "after gc").await;

        if fault == InjectedFault::GcHistory {
            if let Some((_, entries)) = history_before.iter_mut().next() {
                entries.remove(0);
            }
        }

        let history_after = read_history(&main, prefix).await;
        assert_history_survived_gc(&history_before, &history_after, "after gc");

        // And the same after a cold reopen, which reads the reclaimed layout
        // rather than any in-memory residue of the pre-sweep one.
        let rebooted = sim
            .reboot_engine_from_current_snapshot()
            .await
            .expect("engine should reboot after gc");
        let reopened = sim.wrap_session(
            rebooted
                .open_session()
                .await
                .expect("rebooted session should open"),
            &rebooted,
        );
        assert_state(&reopened, prefix, &model.state, "after gc, cold reopen").await;
        assert_history_survived_gc(
            &history_before,
            &read_history(&reopened, prefix).await,
            "after gc, cold reopen",
        );
    }
);

// ---------------------------------------------------------------------------
// Engine interaction helpers.
// ---------------------------------------------------------------------------

fn lane_keys(prefix: &str, lane: &str) -> Vec<String> {
    (0..KEYS_PER_LANE)
        .map(|index| format!("{prefix}{lane}-{index}"))
        .collect()
}

fn random_value(rng: &mut TinyRng) -> JsonValue {
    serde_json::json!({
        "sequence": rng.next(),
        "text": format!("{:016x}", rng.next()),
    })
}

async fn upsert(session: &SimSession, key: &str, value: &JsonValue, label: &str) {
    session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ($1, $2) \
             ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            &[
                Value::Text(key.to_string()),
                Value::Json(value.clone().into()),
            ],
        )
        .await
        .unwrap_or_else(|error| panic!("{label}: upsert {key} failed: {error:?}"));
}

async fn delete(session: &SimSession, key: &str, label: &str) {
    session
        .execute(
            "DELETE FROM lix_key_value WHERE key = $1",
            &[Value::Text(key.to_string())],
        )
        .await
        .unwrap_or_else(|error| panic!("{label}: delete {key} failed: {error:?}"));
}

async fn assert_state(
    session: &SimSession,
    prefix: &str,
    expected: &BTreeMap<String, JsonValue>,
    label: &str,
) {
    let rows = session
        .execute(
            "SELECT key, value FROM lix_key_value WHERE key LIKE $1 ORDER BY key",
            &[Value::Text(format!("{prefix}%"))],
        )
        .await
        .unwrap_or_else(|error| panic!("{label}: state read failed: {error:?}"));
    let actual = rows
        .rows()
        .iter()
        .map(|row| row.values().to_vec())
        .collect::<Vec<_>>();
    let expected = expected
        .iter()
        .map(|(key, value)| vec![Value::Text(key.clone()), Value::Json(value.clone().into())])
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{label}: state diverged from the model");
}

async fn assert_working_diff(
    session: &SimSession,
    prefix: &str,
    expected: &[(String, &'static str)],
    label: &str,
) {
    let rows = session
        .execute(
            "SELECT entity_pk, diff_type FROM lix_working_diff \
             WHERE schema_key = 'lix_key_value' ORDER BY entity_pk",
            &[],
        )
        .await
        .unwrap_or_else(|error| panic!("{label}: working diff read failed: {error:?}"));
    let mut actual = Vec::new();
    for row in rows.rows() {
        // `entity_pk` is the JSON primary-key tuple, `["<key>"]` for
        // `lix_key_value`.
        let entity_pk = row
            .get::<JsonValue>("entity_pk")
            .unwrap_or_else(|error| panic!("{label}: entity_pk should be json: {error:?}"));
        let Some(key) = entity_pk
            .as_array()
            .and_then(|components| components.first())
            .and_then(JsonValue::as_str)
        else {
            panic!("{label}: unexpected entity_pk shape {entity_pk:?}");
        };
        if !key.starts_with(prefix) {
            continue;
        }
        let diff_type = row
            .get::<String>("diff_type")
            .unwrap_or_else(|error| panic!("{label}: diff_type should be text: {error:?}"));
        actual.push((key.to_string(), diff_type));
    }
    actual.sort();
    let expected = expected
        .iter()
        .map(|(key, verb)| (key.clone(), (*verb).to_string()))
        .collect::<Vec<_>>();
    assert_eq!(
        actual, expected,
        "{label}: working diff diverged from the model"
    );
}

/// Reads the newest-first `(value, is_deleted)` sequence per key, and asserts
/// `lixcol_depth` is strictly increasing within a key.
async fn read_history(
    session: &SimSession,
    prefix: &str,
) -> BTreeMap<String, Vec<Option<JsonValue>>> {
    let rows = session
        .execute(
            "SELECT key, value, lixcol_depth, lixcol_is_deleted \
             FROM lix_key_value_history() WHERE key LIKE $1 \
             ORDER BY key, lixcol_depth",
            &[Value::Text(format!("{prefix}%"))],
        )
        .await
        .unwrap_or_else(|error| panic!("history read failed: {error:?}"));

    let mut history: BTreeMap<String, Vec<Option<JsonValue>>> = BTreeMap::new();
    let mut last_depth: BTreeMap<String, i64> = BTreeMap::new();
    for row in rows.rows() {
        let key = row
            .get::<String>("key")
            .unwrap_or_else(|error| panic!("history key should be text: {error:?}"));
        let depth = row
            .get::<i64>("lixcol_depth")
            .unwrap_or_else(|error| panic!("lixcol_depth should be an integer: {error:?}"));
        if let Some(previous) = last_depth.insert(key.clone(), depth) {
            assert!(
                depth > previous,
                "history for {key} repeated or reversed lixcol_depth: {previous} then {depth}"
            );
        }
        let deleted = row.get::<bool>("lixcol_is_deleted").unwrap_or(false);
        let value = if deleted {
            None
        } else {
            Some(
                row.get::<JsonValue>("value")
                    .unwrap_or_else(|error| panic!("history value should be json: {error:?}")),
            )
        };
        history.entry(key).or_default().push(value);
    }
    history
}

async fn assert_history(session: &SimSession, prefix: &str, model: &BranchModel, label: &str) {
    let actual = read_history(session, prefix).await;
    let expected = model
        .history
        .iter()
        .filter(|(_, entries)| !entries.is_empty())
        .map(|(key, entries)| (key.clone(), entries.clone()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(actual, expected, "{label}: history diverged from the model");
}

/// Asserts that a checkpoint GC sweep does not change observable history.
///
/// # The defect this guards
///
/// A sweep used to damage entity history. `collect_ref_reachable_commit_ids`
/// fed its result to the *semantic* retention only, so a graph-reachable commit
/// kept its projection and lost its delta segments — and an entity history row
/// is served out of the delta.
/// `load_commit_delta_members_with_payloads_for_schemas` returns an empty
/// member list for a commit whose replay state is gone, so the damage raised no
/// error: the swept commits read as commits that changed nothing. Current state
/// was unaffected throughout, which is why nothing else caught it.
///
/// # Why equality, and not "a sweep may drop the oldest entries"
///
/// That weaker invariant — what remains must be an exact newest-first prefix of
/// what was there before — was the first form of this assertion, chosen to be a
/// gate the tree could pass while the defect was open. Measuring the defect
/// disproved it. At six checkpointed rounds the sweep truncated; at twelve it
/// dropped entries out of the *middle*, so the surviving sequence closed up and
/// an older value appeared at the position a newer one had occupied:
///
/// ```text
/// before: [ ...7189b9e18, 1582bff8a4dd11c8 ]
/// after:  [ ...7189b9e18, b1bba5cbd88ff978 ]   <- wrong value, same depth
/// ```
///
/// So the weak form was not merely weak, it was false, and a blame query could
/// return a wrong answer rather than a missing one.
///
/// Equality is also not over-strong. A sweep frees the *unreachable* interior —
/// the intra-interval commits a checkpoint superseded — and those never
/// contributed a history entry, because the engine collapses an
/// un-checkpointed interval into one commit. Nothing a sweep is entitled to
/// collect is observable here, so any change at all is a defect.
///
/// `LIX_VC_MODEL_INJECT=gc_history` proves this assertion can fail.
fn assert_history_survived_gc(
    before: &BTreeMap<String, Vec<Option<JsonValue>>>,
    after: &BTreeMap<String, Vec<Option<JsonValue>>>,
    label: &str,
) {
    assert_eq!(
        before, after,
        "{label}: checkpoint GC changed the observable history"
    );
}

async fn branch_head(engine: &lix::integration::Engine, branch_id: &str) -> String {
    engine
        .load_branch_head_commit_id(branch_id)
        .await
        .expect("branch head should load")
        .expect("branch head should exist")
}

/// Waits until every recorded intra-interval commit has been reclaimed, then
/// asserts it. Collection is asynchronous, so this bounds the wait; it does not
/// tolerate a run in which nothing was collected.
async fn wait_until_reclaimed(session: &SimSession, commit_ids: &[String]) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let remaining = present_commits(session, commit_ids).await;
        if remaining.is_empty() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "checkpoint GC did not reclaim intra-interval commits at rounds {remaining:?} of \
             {}; without a sweep this test proves nothing about reclaim safety",
            commit_ids.len()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn present_commits(session: &SimSession, commit_ids: &[String]) -> Vec<usize> {
    let mut present = Vec::new();
    for (index, commit_id) in commit_ids.iter().enumerate() {
        let result = session
            .execute(
                "SELECT id FROM lix_commit WHERE id = $1",
                &[Value::Text(commit_id.clone())],
            )
            .await
            .expect("commit existence query should succeed");
        if !result.is_empty() {
            present.push(index);
        }
    }
    present
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
        (self.next() as usize) % upper
    }
}
