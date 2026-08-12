use std::collections::BTreeMap;

use lix::{CreateBranchOptions, MergeBranchOptions, MergeBranchOutcome, Value};

const DEFAULT_SEEDS: [u64; 6] = [0, 1, 2, 0x51ce_deed, u64::MAX - 1, u64::MAX];
const STEPS_PER_SEED: usize = 48;
const KEYS_PER_LANE: usize = 6;

simulation_test!(
    repeated_disjoint_branch_merges_preserve_both_histories,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for seed in crate::support::fuzz_seeds(&DEFAULT_SEEDS) {
            let prefix = format!("merge-fuzz-{seed:016x}-");
            let branch_id = format!(
                "01930000-{:04x}-7000-8000-{:012x}",
                seed & 0xffff,
                seed & 0xffff_ffff_ffff
            );
            let receipt = main
                .create_branch(CreateBranchOptions {
                    id: Some(branch_id.clone()),
                    name: format!("merge-fuzz-{seed:016x}"),
                    from_commit_id: None,
                })
                .await
                .unwrap_or_else(|error| panic!("seed {seed:#018x}: create failed: {error:?}"));
            let source = sim.wrap_session(
                engine
                    .open_session(receipt.id)
                    .await
                    .unwrap_or_else(|error| {
                        panic!("seed {seed:#018x}: source open failed: {error:?}")
                    }),
                &engine,
            );
            let main_keys = lane_keys(&prefix, "main");
            let source_keys = lane_keys(&prefix, "source");
            let mut expected_main = BTreeMap::new();
            let mut expected_source = BTreeMap::new();
            let mut rng = TinyRng::new(seed);

            let fast_forward_key = source_keys[0].clone();
            let fast_forward_value = serde_json::json!({
                "phase": "fast-forward",
                "seed": seed,
            });
            source
                .execute(
                    "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
                    &[
                        Value::Text(fast_forward_key.clone()),
                        Value::Json(fast_forward_value.clone().into()),
                    ],
                )
                .await
                .unwrap_or_else(|error| {
                    panic!("seed {seed:#018x}: fast-forward setup failed: {error:?}")
                });
            expected_source.insert(fast_forward_key, fast_forward_value);
            let fast_forward = main
                .merge_branch(MergeBranchOptions {
                    source_branch_id: branch_id.clone(),
                })
                .await
                .unwrap_or_else(|error| {
                    panic!("seed {seed:#018x}: fast-forward merge failed: {error:?}")
                });
            assert_eq!(
                fast_forward.outcome,
                MergeBranchOutcome::FastForward,
                "seed {seed:#018x}: deterministic first merge must exercise fast-forward"
            );
            expected_main.extend(expected_source.clone());

            for step in 0..STEPS_PER_SEED {
                let label = format!("seed {seed:#018x}, step {step}");
                match rng.usize(5) {
                    0 | 1 => {
                        mutate_lane(
                            &source,
                            &source_keys,
                            &mut expected_source,
                            &mut rng,
                            &label,
                        )
                        .await;
                    }
                    2 | 3 => {
                        mutate_lane(&main, &main_keys, &mut expected_main, &mut rng, &label).await;
                    }
                    _ => {
                        main.merge_branch(MergeBranchOptions {
                            source_branch_id: branch_id.clone(),
                        })
                        .await
                        .unwrap_or_else(|error| panic!("{label}: merge failed: {error:?}"));
                        for key in &source_keys {
                            expected_main.remove(key);
                        }
                        expected_main.extend(expected_source.clone());
                    }
                }

                assert_state(&main, &prefix, &expected_main, &format!("{label}, main")).await;
                assert_state(
                    &source,
                    &prefix,
                    &expected_source,
                    &format!("{label}, source"),
                )
                .await;

                if step % 12 == 11 {
                    let reopened_engine = sim
                        .reboot_engine_from_current_snapshot()
                        .await
                        .unwrap_or_else(|error| panic!("{label}: reopen failed: {error:?}"));
                    let reopened_main = sim.wrap_session(
                        reopened_engine
                            .open_workspace_session()
                            .await
                            .unwrap_or_else(|error| {
                                panic!("{label}: reopened main failed: {error:?}")
                            }),
                        &reopened_engine,
                    );
                    let reopened_source = sim.wrap_session(
                        reopened_engine
                            .open_session(branch_id.clone())
                            .await
                            .unwrap_or_else(|error| {
                                panic!("{label}: reopened source failed: {error:?}")
                            }),
                        &reopened_engine,
                    );
                    assert_state(&reopened_main, &prefix, &expected_main, &label).await;
                    assert_state(&reopened_source, &prefix, &expected_source, &label).await;
                }
            }

            main.merge_branch(MergeBranchOptions {
                source_branch_id: branch_id,
            })
            .await
            .unwrap_or_else(|error| panic!("seed {seed:#018x}: final merge failed: {error:?}"));
            for key in &source_keys {
                expected_main.remove(key);
            }
            expected_main.extend(expected_source);
            assert_state(&main, &prefix, &expected_main, "after final merge").await;
        }
    }
);

fn lane_keys(prefix: &str, lane: &str) -> Vec<String> {
    (0..KEYS_PER_LANE)
        .map(|index| format!("{prefix}{lane}-{index}"))
        .collect()
}

async fn mutate_lane(
    session: &crate::support::simulation_test::engine::SimSession,
    keys: &[String],
    expected: &mut BTreeMap<String, serde_json::Value>,
    rng: &mut TinyRng,
    label: &str,
) {
    let key = keys[rng.usize(keys.len())].clone();
    if rng.usize(4) == 0 {
        session
            .execute(
                "DELETE FROM lix_key_value WHERE key = $1",
                &[Value::Text(key.clone())],
            )
            .await
            .unwrap_or_else(|error| panic!("{label}: delete failed: {error:?}"));
        expected.remove(&key);
    } else {
        let value = serde_json::json!({
            "sequence": rng.next(),
            "text": format!("{:016x}", rng.next()),
        });
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ($1, $2) \
                 ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                &[Value::Text(key.clone()), Value::Json(value.clone().into())],
            )
            .await
            .unwrap_or_else(|error| panic!("{label}: upsert failed: {error:?}"));
        expected.insert(key, value);
    }
}

async fn assert_state(
    session: &crate::support::simulation_test::engine::SimSession,
    prefix: &str,
    expected: &BTreeMap<String, serde_json::Value>,
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
    assert_eq!(actual, expected, "{label}: branch state diverged");
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
