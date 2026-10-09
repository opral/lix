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
