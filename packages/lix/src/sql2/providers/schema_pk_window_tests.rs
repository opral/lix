//! Primary-key interval reads against every live-state layer.
//!
//! The reference answer is always the unrestricted scan filtered in Rust: it
//! reads every layer without any primary-key access path, so it is an
//! independent oracle for the interval routes. Fixtures commit enough rows to
//! span many packed mutation parts, and then layer HOT updates, deletes,
//! file-scoped duplicates, untracked rows and global-branch rows on top.

use std::collections::BTreeSet;

use crate::{Lix, Memory, Value};

pub(super) const SCHEMA: &str = "pkw_item";
pub(super) const FILE_A: &str = "01920000-0000-7000-8000-00000000f0a1";
pub(super) const FILE_B: &str = "01920000-0000-7000-8000-00000000f0b2";

/// `(id, value, file_id, global, untracked)` as SQL returns it.
pub(super) type Row = (String, String, Option<String>, bool, bool);

pub(super) const SELECT_COLUMNS: &str =
    "id, value, lixcol_file_id, lixcol_global, lixcol_untracked";

/// A small deterministic generator; tests must replay exactly.
pub(super) struct Lcg(u64);

impl Lcg {
    pub(super) fn new(seed: u64) -> Self {
        Self(seed ^ 0x9e37_79b9_7f4a_7c15)
    }

    pub(super) fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }

    pub(super) fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

pub(super) fn id(index: u64) -> String {
    format!("k{index:05}")
}

pub(super) async fn open_fixture() -> Lix<Memory> {
    let lix = crate::open_lix()
        .with_storage(Memory::new())
        .await
        .unwrap();
    for global in ["FALSE", "TRUE"] {
    lix.execute(
        &format!(
            "INSERT INTO lix_registered_schema (value, lixcol_global) VALUES (CAST('{{\"$schema\":\"https://lix.dev/schema-v1.json\",\
             \"key\":\"{SCHEMA}\",\"columns\":[{{\"name\":\"id\",\"type\":\"text\",\"nullable\":false}},\
             {{\"name\":\"value\",\"type\":\"text\",\"nullable\":false}}],\"primary_key\":[\"id\"]}}' AS JSONB), {global})"
        ),
        &[],
    )
    .await
    .unwrap();
    }
    lix.execute(
        &format!(
            "INSERT INTO lix_file(id,path) VALUES ('{FILE_A}','/pkw-a'), ('{FILE_B}','/pkw-b')"
        ),
        &[],
    )
    .await
    .unwrap();
    lix.execute(
        &format!("INSERT INTO lix_file(id,path,lixcol_global) VALUES ('{FILE_B}','/pkw-global-b',TRUE)"),
        &[],
    )
    .await
    .unwrap();
    lix
}

fn file_sql(file_id: Option<&str>) -> String {
    file_id.map_or_else(|| "NULL".to_owned(), |file_id| format!("'{file_id}'"))
}

/// Inserts `(id, value, file)` rows in multi-row statements inside one
/// explicit transaction, so the commit lands as one packed base with many
/// mutation parts.
pub(super) async fn bulk_insert(lix: &Lix<Memory>, rows: &[(String, String, Option<&str>)]) {
    let mut transaction = lix.begin_transaction().await.unwrap();
    for chunk in rows.chunks(400) {
        let values = chunk
            .iter()
            .map(|(id, value, file_id)| format!("('{id}','{value}',{})", file_sql(*file_id)))
            .collect::<Vec<_>>()
            .join(", ");
        transaction
            .execute(
                &format!("INSERT INTO {SCHEMA} (id, value, lixcol_file_id) VALUES {values}"),
                &[],
            )
            .await
            .unwrap();
    }
    transaction.commit().await.unwrap();
}

pub(super) fn decode_rows(result: &crate::ExecuteResult) -> Vec<Row> {
    result
        .rows()
        .iter()
        .map(|row| {
            let text = |column: &str| match row.get::<Value>(column).unwrap() {
                Value::Text(value) => value,
                value => panic!("unexpected {column}: {value:?}"),
            };
            let file_id = match row.get::<Value>("lixcol_file_id").unwrap() {
                Value::Null => None,
                Value::Text(file_id) => Some(file_id),
                value => panic!("unexpected file id: {value:?}"),
            };
            let flag = |column: &str| match row.get::<Value>(column).unwrap() {
                Value::Boolean(flag) => flag,
                value => panic!("unexpected {column}: {value:?}"),
            };
            (
                text("id"),
                text("value"),
                file_id,
                flag("lixcol_global"),
                flag("lixcol_untracked"),
            )
        })
        .collect()
}

pub(super) async fn all_rows(lix: &Lix<Memory>) -> Vec<Row> {
    let result = lix
        .execute(&format!("SELECT {SELECT_COLUMNS} FROM {SCHEMA}"), &[])
        .await
        .unwrap();
    let mut rows = decode_rows(&result);
    rows.sort();
    rows
}

/// The physical layers a fixture places rows in before its HOT mutations.
#[derive(Debug, Clone, Copy)]
pub(super) struct FixtureShape {
    /// Publish a checkpoint after the bulk load, so later reads resolve the
    /// bulk rows through a durable root current base.
    pub(super) checkpoint: bool,
    /// Continue on a fresh branch created from the loaded head, whose serving
    /// generation starts from that head as its root current base.
    pub(super) branch: bool,
}

/// Two bulk unfiled commits (packed current bases with many mutation parts
/// and interleaved identities), file-scoped duplicates of those identities,
/// then updates, deletes, inserts, untracked and global rows in later commits
/// so the HOT overlay shadows the bases below and above every probed bound.
pub(super) async fn layered_fixture(seed: u64, shape: FixtureShape) -> Lix<Memory> {
    let lix = open_fixture().await;
    let mut rng = Lcg::new(seed);
    bulk_insert(
        &lix,
        &(0..1600)
            .map(|index| (id(index * 2), format!("base-{index}"), None))
            .collect::<Vec<_>>(),
    )
    .await;
    bulk_insert(
        &lix,
        &(0..900)
            .map(|index| (id(index * 4 + 1), format!("odd-{index}"), None))
            .collect::<Vec<_>>(),
    )
    .await;
    for file in [FILE_A, FILE_B] {
        let stride = if file == FILE_A { 9 } else { 7 };
        bulk_insert(
            &lix,
            &(0..300)
                .map(|index| (id(index * stride + 1), format!("{file}-{index}"), Some(file)))
                .collect::<Vec<_>>(),
        )
        .await;
    }
    if shape.checkpoint {
        lix.create_checkpoint().await.unwrap();
    }
    if shape.branch {
        let branch = lix
            .create_branch(crate::CreateBranchOptions {
                id: None,
                name: format!("pkw-{seed}"),
                from_commit_id: None,
            })
            .await
            .unwrap();
        lix.switch_branch(crate::SwitchBranchOptions {
            branch_id: branch.id,
        })
        .await
        .unwrap();
    }

    for round in 0..6 {
        let mut statements = Vec::new();
        for _ in 0..40 {
            let index = rng.below(4800);
            let file = match rng.below(3) {
                0 => None,
                1 => Some(FILE_A),
                _ => Some(FILE_B),
            };
            let file_filter = file.map_or_else(
                || "lixcol_file_id IS NULL".to_owned(),
                |file_id| format!("lixcol_file_id = '{file_id}'"),
            );
            match rng.below(4) {
                0 => statements.push(format!(
                    "UPDATE {SCHEMA} SET value = 'u{round}-{index}' WHERE id = '{}' AND {file_filter}",
                    id(index)
                )),
                1 => statements.push(format!(
                    "DELETE FROM {SCHEMA} WHERE id = '{}' AND {file_filter}",
                    id(index)
                )),
                2 => statements.push(format!(
                    "INSERT INTO {SCHEMA} (id, value, lixcol_file_id) VALUES ('{}-n{round}-{}', 'n{round}-{index}', {})",
                    id(index),
                    statements.len(),
                    file_sql(file)
                )),
                _ => statements.push(format!(
                    "DELETE FROM {SCHEMA} WHERE id = '{}' AND {file_filter}",
                    id(index * 2 % 4800)
                )),
            }
        }
        for statement in statements {
            lix.execute(&statement, &[]).await.unwrap();
        }
    }
    for index in 0..30 {
        let row = rng.below(4800);
        lix.execute(
            &format!(
                "INSERT INTO {SCHEMA} (id, value, lixcol_untracked) VALUES ('{}-u{index}', 'untracked-{index}', TRUE)",
                id(row)
            ),
            &[],
        )
        .await
        .unwrap();
    }
    // Distinct identities (113 is coprime with 4800), half of them colliding
    // with local rows so the branch-local winner must hide the global row.
    for index in 0..40_u64 {
        let row = index * 113 % 4800;
        let file = if index % 4 == 0 { Some(FILE_B) } else { None };
        lix.execute(
            &format!(
                "INSERT INTO {SCHEMA} (id, value, lixcol_file_id, lixcol_global) VALUES ('{}', 'global-{index}', {}, TRUE)",
                id(row),
                file_sql(file)
            ),
            &[],
        )
        .await
        .unwrap();
    }
    lix
}

fn in_window(row: &Row, lower: Option<(&str, bool)>, upper: Option<(&str, bool)>) -> bool {
    lower.is_none_or(|(bound, inclusive)| {
        row.0.as_str() > bound || (inclusive && row.0.as_str() == bound)
    }) && upper.is_none_or(|(bound, inclusive)| {
        row.0.as_str() < bound || (inclusive && row.0.as_str() == bound)
    })
}

async fn assert_windows_match_unrestricted_scan(shape: FixtureShape) {
    let lix = layered_fixture(7, shape).await;
    let reference = all_rows(&lix).await;
    assert!(reference.iter().any(|row| row.2.is_some()));
    assert!(reference.iter().any(|row| row.3));
    assert!(reference.iter().any(|row| row.4));
    let probes = [
        (Some(("k00100", false)), Some(("k00180", true))),
        (Some(("k00100", true)), Some(("k00180", false))),
        (Some(("k02399", false)), Some(("k02990", true))),
        (Some(("k04700", false)), None),
        (None, Some(("k00042", true))),
        (Some(("k01000", false)), Some(("k01000", true))),
        (Some(("k01001", true)), Some(("k01001", true))),
        (Some(("k09999", false)), None),
    ];
    for (lower, upper) in probes {
        let mut predicates = Vec::new();
        let mut params = Vec::new();
        if let Some((bound, inclusive)) = lower {
            params.push(Value::Text(bound.to_owned()));
            predicates.push(format!("id {} ${}", if inclusive { ">=" } else { ">" }, params.len()));
        }
        if let Some((bound, inclusive)) = upper {
            params.push(Value::Text(bound.to_owned()));
            predicates.push(format!("id {} ${}", if inclusive { "<=" } else { "<" }, params.len()));
        }
        let result = lix
            .execute(
                &format!(
                    "SELECT {SELECT_COLUMNS} FROM {SCHEMA} WHERE {}",
                    predicates.join(" AND ")
                ),
                &params,
            )
            .await
            .unwrap();
        let mut actual = decode_rows(&result);
        actual.sort();
        let expected = reference
            .iter()
            .filter(|row| in_window(row, lower, upper))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "window {lower:?}..{upper:?}");
        assert_eq!(
            actual.iter().collect::<BTreeSet<_>>().len(),
            actual.len(),
            "window {lower:?}..{upper:?} duplicated a row"
        );
    }
    lix.close().await.unwrap();
}

#[tokio::test]
async fn primary_key_windows_match_the_unrestricted_scan_over_packed_bases() {
    assert_windows_match_unrestricted_scan(FixtureShape {
        checkpoint: false,
        branch: false,
    })
    .await;
}

#[tokio::test]
async fn primary_key_windows_match_the_unrestricted_scan_over_a_checkpoint_root() {
    assert_windows_match_unrestricted_scan(FixtureShape {
        checkpoint: true,
        branch: false,
    })
    .await;
}

#[tokio::test]
async fn primary_key_windows_match_the_unrestricted_scan_on_a_branch_root() {
    assert_windows_match_unrestricted_scan(FixtureShape {
        checkpoint: false,
        branch: true,
    })
    .await;
}

/// Orders rows the way `ORDER BY id` must: by primary key. Rows sharing an
/// id (one per file scope or branch) have no defined relative order.
fn id_sequence(rows: &[Row]) -> Vec<&str> {
    rows.iter().map(|row| row.0.as_str()).collect()
}

/// Paginates `ORDER BY id LIMIT page` with `id > $1` keysets and checks every
/// page against the reference: the id sequence must be exactly the next
/// `page` ids, and every row must be a distinct current row.
pub(super) async fn assert_keyset_pages_match(
    execute: &mut (impl AsyncFnMut(&str, Vec<Value>) -> crate::ExecuteResult + ?Sized),
    reference: &[Row],
    page: usize,
) {
    let mut sorted = reference.to_vec();
    sorted.sort();
    let reference_set = sorted.iter().collect::<BTreeSet<_>>();
    let mut last: Option<String> = None;
    let mut pages = 0;
    loop {
        let result = match last.as_ref() {
            Some(last) => {
                execute(
                    &format!(
                        "SELECT {SELECT_COLUMNS} FROM {SCHEMA} WHERE id > $1 ORDER BY id LIMIT {page}"
                    ),
                    vec![Value::Text(last.clone())],
                )
                .await
            }
            None => {
                execute(
                    &format!(
                        "SELECT {SELECT_COLUMNS} FROM {SCHEMA} WHERE id >= $1 ORDER BY id LIMIT {page}"
                    ),
                    vec![Value::Text(String::new())],
                )
                .await
            }
        };
        let rows = decode_rows(&result);
        let expected = sorted
            .iter()
            .filter(|row| last.as_ref().is_none_or(|last| &row.0 > last))
            .take(page)
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            id_sequence(&rows),
            id_sequence(&expected),
            "page {pages} after {last:?}"
        );
        assert_eq!(
            rows.iter().collect::<BTreeSet<_>>().len(),
            rows.len(),
            "page {pages} after {last:?} duplicated a row"
        );
        for row in &rows {
            assert!(
                reference_set.contains(row),
                "page {pages} after {last:?} returned a row that is not current: {row:?}"
            );
        }
        pages += 1;
        if rows.len() < page {
            break;
        }
        last = rows.last().map(|row| row.0.clone());
        assert!(pages <= reference.len() + 1, "keyset pagination did not advance");
    }
}

async fn assert_ordered_routes_match(lix: &Lix<Memory>) {
    let reference = all_rows(lix).await;
    for page in [1, 7, 64, 500] {
        let mut execute = async |sql: &str, params: Vec<Value>| {
            lix.execute(sql, &params).await.unwrap()
        };
        assert_keyset_pages_match(&mut execute, &reference, page).await;
    }
    // Without ORDER BY a fetch may return any qualifying rows, but only
    // qualifying, current and distinct ones, and as many as exist.
    for (bound, limit) in [("k00500", 25_usize), ("k04790", 50), ("k01200", 4000)] {
        let result = lix
            .execute(
                &format!("SELECT {SELECT_COLUMNS} FROM {SCHEMA} WHERE id > $1 LIMIT {limit}"),
                &[Value::Text(bound.to_owned())],
            )
            .await
            .unwrap();
        let rows = decode_rows(&result);
        let qualifying = reference
            .iter()
            .filter(|row| row.0.as_str() > bound)
            .collect::<BTreeSet<_>>();
        assert_eq!(rows.len(), limit.min(qualifying.len()), "bound {bound}");
        assert_eq!(rows.iter().collect::<BTreeSet<_>>().len(), rows.len());
        assert!(rows.iter().all(|row| qualifying.contains(row)), "bound {bound}");
    }
    // A closed interval in primary-key order, with no fetch.
    let result = lix
        .execute(
            &format!(
                "SELECT {SELECT_COLUMNS} FROM {SCHEMA} WHERE id >= $1 AND id < $2 ORDER BY id"
            ),
            &[Value::Text("k00777".to_owned()), Value::Text("k03333".to_owned())],
        )
        .await
        .unwrap();
    let rows = decode_rows(&result);
    let mut expected = reference
        .iter()
        .filter(|row| row.0.as_str() >= "k00777" && row.0.as_str() < "k03333")
        .cloned()
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(id_sequence(&rows), id_sequence(&expected));
    let mut sorted_rows = rows.clone();
    sorted_rows.sort();
    assert_eq!(sorted_rows, expected);
}

#[tokio::test]
async fn keyset_pages_match_the_reference_over_packed_bases() {
    let lix = layered_fixture(11, FixtureShape {
        checkpoint: false,
        branch: false,
    })
    .await;
    assert_ordered_routes_match(&lix).await;
    lix.close().await.unwrap();
}

#[tokio::test]
async fn keyset_pages_match_the_reference_on_a_branch_root() {
    let lix = layered_fixture(13, FixtureShape {
        checkpoint: false,
        branch: true,
    })
    .await;
    assert_ordered_routes_match(&lix).await;
    lix.close().await.unwrap();
}

/// Randomized equivalence: after every round of random inserts, updates and
/// deletes across file scopes, untracked and global rows, keyset pagination
/// must reproduce the unrestricted scan in primary-key order.
#[tokio::test]
async fn keyset_pagination_matches_the_unrestricted_scan_across_random_mutations() {
    for seed in [3_u64, 29] {
        let lix = layered_fixture(seed, FixtureShape {
            checkpoint: false,
            branch: seed % 2 == 1,
        })
        .await;
        let mut rng = Lcg::new(seed.wrapping_mul(7919));
        for round in 0..5 {
            for step in 0..30 {
                let index = rng.below(4800);
                let file = match rng.below(4) {
                    0 | 1 => None,
                    2 => Some(FILE_A),
                    _ => Some(FILE_B),
                };
                let file_filter = file.map_or_else(
                    || "lixcol_file_id IS NULL".to_owned(),
                    |file_id| format!("lixcol_file_id = '{file_id}'"),
                );
                let statement = match rng.below(5) {
                    0 | 1 => format!(
                        "DELETE FROM {SCHEMA} WHERE id = '{}' AND {file_filter}",
                        id(index)
                    ),
                    2 => format!(
                        "UPDATE {SCHEMA} SET value = 'r{round}s{step}' WHERE id = '{}' AND {file_filter}",
                        id(index)
                    ),
                    3 => format!(
                        "INSERT INTO {SCHEMA} (id, value, lixcol_file_id) VALUES ('{}-r{round}s{step}', 'v', {})",
                        id(index),
                        file_sql(file)
                    ),
                    _ => format!(
                        "DELETE FROM {SCHEMA} WHERE id >= '{}' AND id < '{}' AND {file_filter}",
                        id(index),
                        id(index + 3)
                    ),
                };
                lix.execute(&statement, &[]).await.unwrap();
            }
            let reference = all_rows(&lix).await;
            for page in [5, 97] {
                let mut execute = async |sql: &str, params: Vec<Value>| {
                    lix.execute(sql, &params).await.unwrap()
                };
                assert_keyset_pages_match(&mut execute, &reference, page).await;
            }
        }
        lix.close().await.unwrap();
    }
}

/// A transaction reads its own staged writes; the ordered route must either
/// see them through the staged reader or not run at all.
#[tokio::test]
async fn keyset_pages_read_transaction_staged_rows() {
    let lix = layered_fixture(17, FixtureShape {
        checkpoint: false,
        branch: false,
    })
    .await;
    let mut transaction = lix.begin_transaction().await.unwrap();
    for statement in [
        format!("DELETE FROM {SCHEMA} WHERE id >= 'k00100' AND id < 'k00140'"),
        format!(
            "UPDATE {SCHEMA} SET value = 'staged' WHERE id = 'k01000' AND lixcol_file_id IS NULL"
        ),
        format!(
            "UPDATE {SCHEMA} SET value = 'staged-a' WHERE id = 'k01009' AND lixcol_file_id = '{FILE_A}'"
        ),
        format!(
            "INSERT INTO {SCHEMA} (id, value, lixcol_file_id) VALUES \
             ('k00100-staged', 'staged', NULL), ('k02000-staged', 'staged', '{FILE_A}')"
        ),
    ] {
        transaction
            .execute(&statement, &[])
            .await
            .unwrap_or_else(|error| panic!("{statement}: {error:?}"));
    }
    let result = transaction
        .execute(&format!("SELECT {SELECT_COLUMNS} FROM {SCHEMA}"), &[])
        .await
        .unwrap();
    let mut reference = decode_rows(&result);
    reference.sort();
    assert!(reference.iter().any(|row| row.0 == "k02000-staged"));
    for page in [9, 300] {
        let mut execute = async |sql: &str, params: Vec<Value>| {
            transaction.execute(sql, &params).await.unwrap()
        };
        assert_keyset_pages_match(&mut execute, &reference, page).await;
    }
    transaction.rollback().await.unwrap();
    lix.close().await.unwrap();
}

async fn physical_plan(lix: &Lix<Memory>, sql: &str, params: &[Value]) -> String {
    let result = lix.execute(&format!("EXPLAIN {sql}"), params).await.unwrap();
    result
        .rows()
        .iter()
        .flat_map(|row| row.values().to_vec())
        .filter_map(|value| match value {
            Value::Text(text) => Some(text),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test]
async fn primary_key_ranges_plan_as_ordered_fetching_scans() {
    let lix = open_fixture().await;
    let bound = [Value::Text("k00100".to_owned())];
    let plan = physical_plan(
        &lix,
        &format!("SELECT id, value FROM {SCHEMA} WHERE id > $1 ORDER BY id LIMIT 10"),
        &bound,
    )
    .await;
    let physical = plan.split("physical_plan").nth(1).expect("physical plan");
    assert!(!physical.contains("SortExec"), "{plan}");
    assert!(!physical.contains("FilterExec"), "{plan}");
    assert!(physical.contains(&format!("SpecScanExec({SCHEMA})")), "{plan}");
    assert!(physical.contains("fetch=10"), "{plan}");
    assert!(physical.contains("output_ordering=[id@0 ASC"), "{plan}");

    let plan = physical_plan(
        &lix,
        &format!("SELECT value FROM {SCHEMA} WHERE id > $1 LIMIT 7"),
        &bound,
    )
    .await;
    let physical = plan.split("physical_plan").nth(1).expect("physical plan");
    assert!(!physical.contains("FilterExec"), "{plan}");
    assert!(physical.contains("fetch=7"), "{plan}");

    // A residual predicate keeps its filter but no sort.
    let plan = physical_plan(
        &lix,
        &format!(
            "SELECT id, value FROM {SCHEMA} WHERE id > $1 AND value <> 'x' ORDER BY id LIMIT 10"
        ),
        &bound,
    )
    .await;
    let physical = plan.split("physical_plan").nth(1).expect("physical plan");
    assert!(!physical.contains("SortExec"), "{plan}");
    lix.close().await.unwrap();
}

/// A branch serves its inherited rows from a root current base, which the
/// tracked tree orders `(schema, file, row_pk)`. HOT overlays and packed
/// bases are ordered `(schema, row_pk, file)`. A full scan must still let
/// every overlay update and delete shadow its root row in every file scope.
#[tokio::test]
async fn full_scans_shadow_multi_scope_root_rows_with_hot_overlays() {
    let lix = open_fixture().await;
    lix.execute(
        &format!(
            "INSERT INTO {SCHEMA} (id, value, lixcol_file_id) VALUES \
             ('k1', 'null-1', NULL), ('k3', 'null-3', NULL), \
             ('k1', 'a-1', '{FILE_A}'), ('k2', 'a-2', '{FILE_A}'), \
             ('k2', 'b-2', '{FILE_B}'), ('k4', 'b-4', '{FILE_B}')"
        ),
        &[],
    )
    .await
    .unwrap();
    let branch = lix
        .create_branch(crate::CreateBranchOptions {
            id: None,
            name: "root-order".to_owned(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    lix.switch_branch(crate::SwitchBranchOptions {
        branch_id: branch.id,
    })
    .await
    .unwrap();
    for statement in [
        format!("UPDATE {SCHEMA} SET value = 'a-2-updated' WHERE id = 'k2' AND lixcol_file_id = '{FILE_A}'"),
        format!("DELETE FROM {SCHEMA} WHERE id = 'k1' AND lixcol_file_id = '{FILE_A}'"),
        format!("UPDATE {SCHEMA} SET value = 'null-3-updated' WHERE id = 'k3' AND lixcol_file_id IS NULL"),
    ] {
        lix.execute(&statement, &[]).await.unwrap();
    }
    let rows = all_rows(&lix).await;
    let row = |id: &str, value: &str, file: Option<&str>| -> Row {
        (id.to_owned(), value.to_owned(), file.map(str::to_owned), false, false)
    };
    let mut expected = vec![
        row("k1", "null-1", None),
        row("k2", "a-2-updated", Some(FILE_A)),
        row("k2", "b-2", Some(FILE_B)),
        row("k3", "null-3-updated", None),
        row("k4", "b-4", Some(FILE_B)),
    ];
    expected.sort();
    assert_eq!(rows, expected);
    lix.close().await.unwrap();
}
