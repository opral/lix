use std::collections::BTreeMap;

use lix::{LixError, Value};

/// `0x7dc` is the seed on which tracked-state rebuild was caught staging one
/// content-addressed tree chunk twice in a single write set, which failed every
/// later read with `LIX_STORAGE_ERROR`.
///
/// It is kept here as coverage, not as the guard for that bug: the trigger needs
/// a replay interval deep enough to contain a rooted boundary, so the seed only
/// reproduces behind the accumulated history of the window it was found in
/// (2006..2013 passes, 2000..2013 fails). The cheap guard is the unit test
/// `tracked_state::storage::tests::promoting_a_chunk_the_write_set_already_staged_is_not_a_duplicate_mutation`;
/// the end-to-end reproduction, which needs no source edit now that the seed
/// budget is env-overridable, is:
///
/// ```text
/// LIX_FUZZ_SEEDS=48 LIX_FUZZ_SEED_START=2000 cargo test -p lix --all-features \
///   --test integration -- --test-threads=1 filesystem_fuzz
/// ```
const DEFAULT_SEEDS: [u64; 6] = [0, 1, 0x7dc, 0x51ce_deed, u64::MAX - 1, u64::MAX];
const STEPS_PER_SEED: usize = 40;

simulation_test!(
    randomized_filesystem_mutations_preserve_bytes_and_atomicity,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for seed in crate::support::fuzz_seeds(&DEFAULT_SEEDS) {
            let root = format!("/corruption-fuzz-{seed:016x}");
            let paths = (0..3)
                .flat_map(|directory| {
                    (0..4).map({
                        let root = root.clone();
                        move |file| format!("{root}/d{directory}/f{file}.bin")
                    })
                })
                .collect::<Vec<_>>();
            let directories = (0..3)
                .map(|directory| format!("{root}/d{directory}"))
                .collect::<Vec<_>>();
            let mut expected = BTreeMap::<String, Vec<u8>>::new();
            let mut rng = TinyRng::new(seed);

            for step in 0..STEPS_PER_SEED {
                let label = format!("seed {seed:#018x}, step {step}");
                let source = paths[rng.usize(paths.len())].clone();
                let target = paths[rng.usize(paths.len())].clone();
                let bytes = random_bytes(&mut rng, step);

                match rng.usize(6) {
                    0 | 1 => {
                        session
                            .execute(
                                "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
                                 ON CONFLICT (path) DO UPDATE SET content = excluded.content",
                                &[
                                    Value::Text(source.clone()),
                                    Value::Blob(bytes.clone().into()),
                                ],
                            )
                            .await
                            .unwrap_or_else(|error| {
                                panic!("{label}: file upsert failed: {error:?}")
                            });
                        expected.insert(source, bytes);
                    }
                    2 => {
                        session
                            .execute(
                                "UPDATE lix_file SET content = $2 WHERE path = $1",
                                &[
                                    Value::Text(source.clone()),
                                    Value::Blob(bytes.clone().into()),
                                ],
                            )
                            .await
                            .unwrap_or_else(|error| {
                                panic!("{label}: file data update failed: {error:?}")
                            });
                        if let Some(current) = expected.get_mut(&source) {
                            *current = bytes;
                        }
                    }
                    3 => {
                        let expected_error = expected.contains_key(&source)
                            && source != target
                            && expected.contains_key(&target);
                        let result = session
                            .execute(
                                "UPDATE lix_file SET path = $2 WHERE path = $1",
                                &[Value::Text(source.clone()), Value::Text(target.clone())],
                            )
                            .await;
                        match (expected_error, result) {
                            (true, Err(error)) => {
                                assert_eq!(error.code, LixError::CODE_UNIQUE, "{label}");
                            }
                            (true, Ok(result)) => {
                                panic!("{label}: colliding move succeeded: {result:?}");
                            }
                            (false, Err(error)) => {
                                panic!("{label}: valid move failed: {error:?}");
                            }
                            (false, Ok(_)) => {
                                if source != target
                                    && let Some(data) = expected.remove(&source)
                                {
                                    expected.insert(target, data);
                                }
                            }
                        }
                    }
                    4 => {
                        if rng.bool() {
                            session
                                .execute(
                                    "DELETE FROM lix_file WHERE path = $1",
                                    &[Value::Text(source.clone())],
                                )
                                .await
                                .unwrap_or_else(|error| {
                                    panic!("{label}: file delete failed: {error:?}")
                                });
                            expected.remove(&source);
                        } else {
                            let directory = &directories[rng.usize(directories.len())];
                            session
                                .execute(
                                    "DELETE FROM lix_directory WHERE path = $1",
                                    &[Value::Text(directory.clone())],
                                )
                                .await
                                .unwrap_or_else(|error| {
                                    panic!("{label}: recursive directory delete failed: {error:?}")
                                });
                            let descendant_prefix = format!("{directory}/");
                            expected.retain(|path, _| !path.starts_with(&descendant_prefix));
                        }
                    }
                    _ => {
                        exercise_rejected_file_batch(
                            &session, &root, step, &expected, &bytes, &label,
                        )
                        .await;
                    }
                }

                assert_files(&session, &root, &expected, &label).await;
                if step % 16 == 15 {
                    let reopened_engine = sim
                        .reboot_engine_from_current_snapshot()
                        .await
                        .unwrap_or_else(|error| panic!("{label}: reopen failed: {error:?}"));
                    let reopened = sim.wrap_session(
                        reopened_engine
                            .open_workspace_session()
                            .await
                            .unwrap_or_else(|error| {
                                panic!("{label}: reopened session failed: {error:?}")
                            }),
                        &reopened_engine,
                    );
                    assert_files(&reopened, &root, &expected, &label).await;
                }
            }
        }
    }
);

async fn exercise_rejected_file_batch(
    session: &crate::support::simulation_test::engine::SimSession,
    root: &str,
    step: usize,
    expected: &BTreeMap<String, Vec<u8>>,
    bytes: &[u8],
    label: &str,
) {
    let fresh = format!("{root}/rejected/fresh-{step}.bin");
    let duplicate = expected
        .first_key_value()
        .map_or_else(|| fresh.clone(), |(path, _)| path.clone());
    let error = session
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ($1, $3), ($2, $3)",
            &[
                Value::Text(fresh),
                Value::Text(duplicate),
                Value::Blob(bytes.to_vec().into()),
            ],
        )
        .await
        .expect_err("duplicate file batch must be rejected atomically");
    assert_eq!(error.code, LixError::CODE_UNIQUE, "{label}");
}

async fn assert_files(
    session: &crate::support::simulation_test::engine::SimSession,
    root: &str,
    expected: &BTreeMap<String, Vec<u8>>,
    label: &str,
) {
    let result = session
        .execute(
            "SELECT path, content FROM lix_file WHERE path LIKE $1 ORDER BY path",
            &[Value::Text(format!("{root}/%"))],
        )
        .await
        .unwrap_or_else(|error| panic!("{label}: file state read failed: {error:?}"));
    let actual = result
        .rows()
        .iter()
        .map(|row| row.values().to_vec())
        .collect::<Vec<_>>();
    let expected = expected
        .iter()
        .map(|(path, bytes)| vec![Value::Text(path.clone()), Value::Blob(bytes.clone().into())])
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{label}: filesystem state diverged");

    let rejected_directory = format!("{root}/rejected");
    let directories = session
        .execute(
            "SELECT path FROM lix_directory WHERE path = $1",
            &[Value::Text(rejected_directory.clone())],
        )
        .await
        .unwrap_or_else(|error| panic!("{label}: directory state read failed: {error:?}"));
    assert!(
        directories.is_empty(),
        "{label}: rejected file batch leaked implicit directory {rejected_directory}"
    );
}

fn random_bytes(rng: &mut TinyRng, step: usize) -> Vec<u8> {
    match rng.usize(5) {
        0 => Vec::new(),
        1 => vec![0],
        2 => vec![0xff],
        _ => {
            let len = 1 + rng.usize(128);
            let mut bytes = Vec::with_capacity(len + size_of::<usize>());
            bytes.extend_from_slice(&step.to_be_bytes());
            while bytes.len() < len {
                bytes.extend_from_slice(&rng.next().to_be_bytes());
            }
            bytes.truncate(len);
            bytes
        }
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
