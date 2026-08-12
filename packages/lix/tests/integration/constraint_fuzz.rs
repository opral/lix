use std::collections::BTreeMap;

use lix::{LixError, Value};

const DEFAULT_SEEDS: [u64; 4] = [0, 1, 0x51ce_deed, u64::MAX];
const STEPS_PER_SEED: usize = 48;
const IDS: usize = 8;

simulation_test!(
    randomized_constraints_never_publish_partial_state,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        register_constraint_schemas(&session).await;
        let mut operations_seen = [false; 8];

        for seed in crate::support::fuzz_seeds(&DEFAULT_SEEDS) {
            let prefix = format!("constraint-fuzz-{seed:016x}-");
            let mut parents = BTreeMap::<String, String>::new();
            let mut children = BTreeMap::<String, String>::new();
            let mut rng = TinyRng::new(seed);
            let prelude_label = format!("seed {seed:#018x}, deterministic prelude");
            seed_constraint_state(&session, &prefix, &mut parents, &mut children).await;
            exercise_partially_invalid_parent_batch(&session, &prefix, 0, &parents, &prelude_label)
                .await;
            exercise_restricted_parent_delete_batch(&session, &parents, &children, &prelude_label)
                .await;
            assert_constraint_state(&session, &prefix, &parents, &children, &prelude_label).await;

            for step in 0..STEPS_PER_SEED {
                let label = format!("seed {seed:#018x}, step {step}");
                let parent_id = format!("{prefix}parent-{}", rng.usize(IDS));
                let child_id = format!("{prefix}child-{}", rng.usize(IDS));
                let slug = format!("{prefix}slug-{}", rng.usize(IDS / 2));
                let operation = rng.usize(8);
                operations_seen[operation] = true;

                match operation {
                    0 => {
                        let expected_error = parents.contains_key(&parent_id)
                            || parents
                                .iter()
                                .any(|(id, existing)| id != &parent_id && existing == &slug);
                        let result = session
                            .execute(
                                "INSERT INTO corruption_fuzz_parent (id, slug) VALUES ($1, $2)",
                                &[Value::Text(parent_id.clone()), Value::Text(slug.clone())],
                            )
                            .await;
                        assert_constraint_result(
                            result,
                            expected_error,
                            LixError::CODE_UNIQUE,
                            &label,
                        );
                        if !expected_error {
                            parents.insert(parent_id, slug);
                        }
                    }
                    1 => {
                        let expected_error = parents.contains_key(&parent_id)
                            && parents
                                .iter()
                                .any(|(id, existing)| id != &parent_id && existing == &slug);
                        let result = session
                            .execute(
                                "UPDATE corruption_fuzz_parent SET slug = $2 WHERE id = $1",
                                &[Value::Text(parent_id.clone()), Value::Text(slug.clone())],
                            )
                            .await;
                        assert_constraint_result(
                            result,
                            expected_error,
                            LixError::CODE_UNIQUE,
                            &label,
                        );
                        if !expected_error && let Some(current) = parents.get_mut(&parent_id) {
                            *current = slug;
                        }
                    }
                    2 => {
                        let expected_error = children.values().any(|parent| parent == &parent_id);
                        let result = session
                            .execute(
                                "DELETE FROM corruption_fuzz_parent WHERE id = $1",
                                &[Value::Text(parent_id.clone())],
                            )
                            .await;
                        assert_constraint_result(
                            result,
                            expected_error,
                            LixError::CODE_FOREIGN_KEY,
                            &label,
                        );
                        if !expected_error {
                            parents.remove(&parent_id);
                        }
                    }
                    3 => {
                        // A child insert can violate both constraints at once
                        // (the id already exists *and* the referenced parent
                        // does not). The engine validates referential integrity
                        // first and reports the foreign-key violation, so the
                        // model must not claim UNIQUE whenever the id exists.
                        let expected_error =
                            children.contains_key(&child_id) || !parents.contains_key(&parent_id);
                        let expected_code = if parents.contains_key(&parent_id) {
                            LixError::CODE_UNIQUE
                        } else {
                            LixError::CODE_FOREIGN_KEY
                        };
                        let result = session
                            .execute(
                                "INSERT INTO corruption_fuzz_child (id, parent_id) VALUES ($1, $2)",
                                &[
                                    Value::Text(child_id.clone()),
                                    Value::Text(parent_id.clone()),
                                ],
                            )
                            .await;
                        assert_constraint_result(result, expected_error, expected_code, &label);
                        if !expected_error {
                            children.insert(child_id, parent_id);
                        }
                    }
                    4 => {
                        let expected_error =
                            children.contains_key(&child_id) && !parents.contains_key(&parent_id);
                        let result = session
                            .execute(
                                "UPDATE corruption_fuzz_child SET parent_id = $2 WHERE id = $1",
                                &[
                                    Value::Text(child_id.clone()),
                                    Value::Text(parent_id.clone()),
                                ],
                            )
                            .await;
                        assert_constraint_result(
                            result,
                            expected_error,
                            LixError::CODE_FOREIGN_KEY,
                            &label,
                        );
                        if !expected_error && let Some(current) = children.get_mut(&child_id) {
                            *current = parent_id;
                        }
                    }
                    5 => {
                        session
                            .execute(
                                "DELETE FROM corruption_fuzz_child WHERE id = $1",
                                &[Value::Text(child_id.clone())],
                            )
                            .await
                            .unwrap_or_else(|error| {
                                panic!("{label}: child delete failed: {error:?}")
                            });
                        children.remove(&child_id);
                    }
                    6 => {
                        exercise_partially_invalid_parent_batch(
                            &session, &prefix, step, &parents, &label,
                        )
                        .await;
                    }
                    _ => {
                        exercise_restricted_parent_delete_batch(
                            &session, &parents, &children, &label,
                        )
                        .await;
                    }
                }

                assert_constraint_state(&session, &prefix, &parents, &children, &label).await;
                if step % 12 == 11 {
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
                    assert_constraint_state(&reopened, &prefix, &parents, &children, &label).await;
                }
            }
        }

        assert!(
            operations_seen.into_iter().all(|seen| seen),
            "fixed fuzz corpus must exercise every constraint operation: {operations_seen:?}"
        );
    }
);

async fn register_constraint_schemas(
    session: &crate::support::simulation_test::engine::SimSession,
) {
    session
        .execute(
            r#"INSERT INTO lix_registered_schema (value) VALUES
               (lix_json('{"x-lix-key":"corruption_fuzz_parent","x-lix-primary-key":["/id"],"x-lix-unique":[["/slug"]],"type":"object","properties":{"id":{"type":"string"},"slug":{"type":"string"}},"required":["id","slug"],"additionalProperties":false}')),
               (lix_json('{"x-lix-key":"corruption_fuzz_child","x-lix-primary-key":["/id"],"x-lix-foreign-keys":[{"properties":["/parent_id"],"references":{"schemaKey":"corruption_fuzz_parent","properties":["/id"]}}],"type":"object","properties":{"id":{"type":"string"},"parent_id":{"type":"string"}},"required":["id","parent_id"],"additionalProperties":false}'))"#,
            &[],
        )
        .await
        .expect("constraint schemas should register atomically");
}

async fn seed_constraint_state(
    session: &crate::support::simulation_test::engine::SimSession,
    prefix: &str,
    parents: &mut BTreeMap<String, String>,
    children: &mut BTreeMap<String, String>,
) {
    let referenced_parent = format!("{prefix}seed-referenced-parent");
    let referenced_slug = format!("{prefix}seed-referenced-slug");
    let unreferenced_parent = format!("{prefix}seed-unreferenced-parent");
    let unreferenced_slug = format!("{prefix}seed-unreferenced-slug");
    let child = format!("{prefix}seed-child");
    session
        .execute(
            "INSERT INTO corruption_fuzz_parent (id, slug) VALUES ($1, $2), ($3, $4)",
            &[
                Value::Text(referenced_parent.clone()),
                Value::Text(referenced_slug.clone()),
                Value::Text(unreferenced_parent.clone()),
                Value::Text(unreferenced_slug.clone()),
            ],
        )
        .await
        .expect("deterministic parent setup should succeed");
    session
        .execute(
            "INSERT INTO corruption_fuzz_child (id, parent_id) VALUES ($1, $2)",
            &[
                Value::Text(child.clone()),
                Value::Text(referenced_parent.clone()),
            ],
        )
        .await
        .expect("deterministic child setup should succeed");
    parents.insert(referenced_parent.clone(), referenced_slug);
    parents.insert(unreferenced_parent, unreferenced_slug);
    children.insert(child, referenced_parent);
}

fn assert_constraint_result(
    result: Result<lix::ExecuteResult, LixError>,
    expected_error: bool,
    expected_code: &str,
    label: &str,
) {
    match (expected_error, result) {
        (true, Err(error)) => {
            assert_eq!(error.code, expected_code, "{label}: wrong constraint error");
        }
        (true, Ok(result)) => {
            panic!("{label}: invalid write unexpectedly succeeded: {result:?}");
        }
        (false, Ok(_)) => {}
        (false, Err(error)) => panic!("{label}: valid write failed: {error:?}"),
    }
}

async fn exercise_partially_invalid_parent_batch(
    session: &crate::support::simulation_test::engine::SimSession,
    prefix: &str,
    step: usize,
    parents: &BTreeMap<String, String>,
    label: &str,
) {
    let fresh_id = format!("{prefix}batch-fresh-{step}");
    let fresh_slug = format!("{prefix}batch-fresh-slug-{step}");
    let (conflict_id, conflict_slug) = parents.first_key_value().map_or_else(
        || (fresh_id.clone(), fresh_slug.clone()),
        |(id, slug)| (format!("{id}-conflict"), slug.clone()),
    );
    let error = session
        .execute(
            "INSERT INTO corruption_fuzz_parent (id, slug) VALUES ($1, $2), ($3, $4)",
            &[
                Value::Text(fresh_id),
                Value::Text(fresh_slug),
                Value::Text(conflict_id),
                Value::Text(conflict_slug),
            ],
        )
        .await
        .expect_err("mixed-validity parent batch must be rejected atomically");
    assert_eq!(error.code, LixError::CODE_UNIQUE, "{label}");
}

async fn exercise_restricted_parent_delete_batch(
    session: &crate::support::simulation_test::engine::SimSession,
    parents: &BTreeMap<String, String>,
    children: &BTreeMap<String, String>,
    label: &str,
) {
    let Some(restricted) = children
        .values()
        .find(|parent| parents.contains_key(*parent))
    else {
        return;
    };
    let Some(unrestricted) = parents
        .keys()
        .find(|candidate| children.values().all(|parent| parent != *candidate))
    else {
        return;
    };
    let error = session
        .execute(
            "DELETE FROM corruption_fuzz_parent WHERE id IN ($1, $2)",
            &[
                Value::Text(unrestricted.clone()),
                Value::Text(restricted.clone()),
            ],
        )
        .await
        .expect_err("batch containing a referenced parent must be rejected atomically");
    assert_eq!(error.code, LixError::CODE_FOREIGN_KEY, "{label}");
}

async fn assert_constraint_state(
    session: &crate::support::simulation_test::engine::SimSession,
    prefix: &str,
    parents: &BTreeMap<String, String>,
    children: &BTreeMap<String, String>,
    label: &str,
) {
    let parent_rows = session
        .execute(
            "SELECT id, slug FROM corruption_fuzz_parent WHERE id LIKE $1 ORDER BY id",
            &[Value::Text(format!("{prefix}%"))],
        )
        .await
        .unwrap_or_else(|error| panic!("{label}: parent state read failed: {error:?}"));
    let expected_parents = parents
        .iter()
        .map(|(id, slug)| vec![Value::Text(id.clone()), Value::Text(slug.clone())])
        .collect::<Vec<_>>();
    assert_eq!(
        parent_rows
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        expected_parents,
        "{label}: parent state diverged"
    );

    let child_rows = session
        .execute(
            "SELECT id, parent_id FROM corruption_fuzz_child WHERE id LIKE $1 ORDER BY id",
            &[Value::Text(format!("{prefix}%"))],
        )
        .await
        .unwrap_or_else(|error| panic!("{label}: child state read failed: {error:?}"));
    let expected_children = children
        .iter()
        .map(|(id, parent_id)| vec![Value::Text(id.clone()), Value::Text(parent_id.clone())])
        .collect::<Vec<_>>();
    assert_eq!(
        child_rows
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        expected_children,
        "{label}: child state diverged"
    );
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

    fn usize(&mut self, upper: usize) -> usize {
        // Use the better-mixed upper half; the low bits of an LCG cycle under
        // consecutive power-of-two modulo draws.
        ((self.next() >> 32) as usize) % upper
    }
}
