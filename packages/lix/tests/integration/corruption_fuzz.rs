use std::collections::BTreeMap;

use lix::{LixError, Value};

const DEFAULT_SEEDS: [u64; 4] = [0, 1, 0x51ce_deed, u64::MAX];
const TRANSACTIONS_PER_SEED: usize = 32;
const KEYS_PER_SEED: usize = 8;

simulation_test!(
    transaction_sequences_preserve_the_reference_state,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for seed in crate::support::fuzz_seeds(&DEFAULT_SEEDS) {
            let prefix = format!("corruption-fuzz-{seed:016x}-");
            let keys = (0..KEYS_PER_SEED)
                .map(|index| format!("{prefix}{index}"))
                .collect::<Vec<_>>();
            let mut expected = BTreeMap::new();
            let mut rng = TinyRng::new(seed);

            for step in 0..TRANSACTIONS_PER_SEED {
                let label = format!("seed {seed:#018x}, transaction {step}");
                let mut transaction = session
                    .begin_transaction()
                    .await
                    .unwrap_or_else(|error| panic!("{label}: begin failed: {error:?}"));
                let mut staged = expected.clone();

                for operation in 0..(1 + rng.usize(6)) {
                    let key = keys[rng.usize(keys.len())].clone();
                    let value = random_json(&mut rng, step, operation);
                    match rng.usize(5) {
                        0 | 1 => {
                            transaction
                                .execute(
                                    "INSERT INTO lix_key_value (key, value) VALUES ($1, $2) \
                                     ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                                    &[Value::Text(key.clone()), Value::Json(value.clone().into())],
                                )
                                .await
                                .unwrap_or_else(|error| {
                                    panic!(
                                        "{label}, operation {operation}: upsert failed: {error:?}"
                                    )
                                });
                            staged.insert(key, value);
                        }
                        2 => {
                            transaction
                                .execute(
                                    "UPDATE lix_key_value SET value = $2 WHERE key = $1",
                                    &[Value::Text(key.clone()), Value::Json(value.clone().into())],
                                )
                                .await
                                .unwrap_or_else(|error| {
                                    panic!(
                                        "{label}, operation {operation}: update failed: {error:?}"
                                    )
                                });
                            if let Some(current) = staged.get_mut(&key) {
                                *current = value;
                            }
                        }
                        3 => {
                            transaction
                                .execute(
                                    "DELETE FROM lix_key_value WHERE key = $1",
                                    &[Value::Text(key.clone())],
                                )
                                .await
                                .unwrap_or_else(|error| {
                                    panic!(
                                        "{label}, operation {operation}: delete failed: {error:?}"
                                    )
                                });
                            staged.remove(&key);
                        }
                        _ => {
                            exercise_rejected_batch(
                                &mut transaction,
                                &mut staged,
                                &keys,
                                &prefix,
                                &label,
                                step,
                                operation,
                                &mut rng,
                            )
                            .await;
                        }
                    }
                }

                assert_transaction_state(&mut transaction, &prefix, &staged, &label).await;
                if rng.usize(3) == 0 {
                    transaction
                        .rollback()
                        .await
                        .unwrap_or_else(|error| panic!("{label}: rollback failed: {error:?}"));
                } else {
                    transaction
                        .commit()
                        .await
                        .unwrap_or_else(|error| panic!("{label}: commit failed: {error:?}"));
                    expected = staged;
                }
                assert_session_state(&session, &prefix, &expected, &label).await;

                if step % 8 == 7 {
                    let reopened_engine = sim
                        .reboot_engine_from_current_snapshot()
                        .await
                        .unwrap_or_else(|error| panic!("{label}: reopen failed: {error:?}"));
                    let reopened = sim.wrap_session(
                        reopened_engine
                            .open_session()
                            .await
                            .unwrap_or_else(|error| {
                                panic!("{label}: reopened session failed: {error:?}")
                            }),
                        &reopened_engine,
                    );
                    assert_session_state(&reopened, &prefix, &expected, &label).await;
                }
            }
        }
    }
);

async fn exercise_rejected_batch(
    transaction: &mut crate::support::simulation_test::engine::SimTransaction,
    staged: &mut BTreeMap<String, serde_json::Value>,
    keys: &[String],
    prefix: &str,
    label: &str,
    step: usize,
    operation: usize,
    rng: &mut TinyRng,
) {
    if rng.bool() {
        let duplicate_key = keys[rng.usize(keys.len())].clone();
        let value = random_json(rng, step, operation);
        let error = transaction
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ($1, $2), ($1, $2)",
                &[
                    Value::Text(duplicate_key.clone()),
                    Value::Json(value.into()),
                ],
            )
            .await
            .expect_err("same-batch duplicate must be rejected");
        assert_eq!(error.code, LixError::CODE_UNIQUE, "{label}");
    } else {
        let duplicate_key = format!("{prefix}staged-{step}-{operation}");
        let staged_value = random_json(rng, step, operation);
        transaction
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ($1, $2) \
                 ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                &[
                    Value::Text(duplicate_key.clone()),
                    Value::Json(staged_value.clone().into()),
                ],
            )
            .await
            .unwrap_or_else(|error| panic!("{label}: probe upsert failed: {error:?}"));
        staged.insert(duplicate_key.clone(), staged_value);

        let fresh_key = format!("{prefix}rejected-{step}-{operation}");
        let rejected_value = random_json(rng, step, operation);
        let result = transaction
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ($1, $3), ($2, $3)",
                &[
                    Value::Text(fresh_key.clone()),
                    Value::Text(duplicate_key),
                    Value::Json(rejected_value.into()),
                ],
            )
            .await;
        let error = match result {
            Err(error) => error,
            Ok(result) => {
                panic!(
                    "{label}: batch ending in a transaction-staged key must be rejected \
                     atomically, got {result:?}"
                )
            }
        };
        assert_eq!(error.code, LixError::CODE_UNIQUE, "{label}");
        assert!(
            !staged.contains_key(&fresh_key),
            "{label}: rejected batch changed the model"
        );
    }
}

async fn assert_transaction_state(
    transaction: &mut crate::support::simulation_test::engine::SimTransaction,
    prefix: &str,
    expected: &BTreeMap<String, serde_json::Value>,
    label: &str,
) {
    let actual = transaction
        .execute(
            "SELECT key, value FROM lix_key_value WHERE key LIKE $1 ORDER BY key",
            &[Value::Text(format!("{prefix}%"))],
        )
        .await
        .unwrap_or_else(|error| panic!("{label}: staged state read failed: {error:?}"));
    assert_rows(actual.rows(), expected, label);
}

async fn assert_session_state(
    session: &crate::support::simulation_test::engine::SimSession,
    prefix: &str,
    expected: &BTreeMap<String, serde_json::Value>,
    label: &str,
) {
    let actual = session
        .execute(
            "SELECT key, value FROM lix_key_value WHERE key LIKE $1 ORDER BY key",
            &[Value::Text(format!("{prefix}%"))],
        )
        .await
        .unwrap_or_else(|error| panic!("{label}: committed state read failed: {error:?}"));
    assert_rows(actual.rows(), expected, label);
}

fn assert_rows(actual: &[lix::Row], expected: &BTreeMap<String, serde_json::Value>, label: &str) {
    let expected = expected
        .iter()
        .map(|(key, value)| {
            vec![
                Value::Text(key.clone()),
                if value.is_null() {
                    Value::Null
                } else {
                    Value::Json(value.clone().into())
                },
            ]
        })
        .collect::<Vec<_>>();
    let actual = actual
        .iter()
        .map(|row| row.values().to_vec())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{label}: visible rows diverged");
}

fn random_json(rng: &mut TinyRng, step: usize, operation: usize) -> serde_json::Value {
    match rng.usize(5) {
        0 => serde_json::Value::Null,
        1 => serde_json::Value::Bool(rng.bool()),
        2 => serde_json::json!(rng.next() as i64),
        3 => serde_json::json!(format!("value-{step}-{operation}-{:016x}", rng.next())),
        _ => serde_json::json!({
            "operation": operation,
            "seeded": rng.next(),
            "step": step,
        }),
    }
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

    fn bool(&mut self) -> bool {
        self.next() & 1 == 0
    }
}
