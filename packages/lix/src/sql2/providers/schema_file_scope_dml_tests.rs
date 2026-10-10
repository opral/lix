//! UPDATE and DELETE over primary keys that exist once per file scope.
//!
//! A schema row's identity is `(file scope, primary key)` within a branch:
//! the same primary key legitimately exists once unfiled and once in every
//! file. Every write path that carries rows from its source scan to the staged
//! write must therefore keep the file scope in the identity it keys rows by.
//! The fixture gives every identity a scope-specific `note`, so a write that
//! takes the source snapshot of the wrong scope is visible as a changed note,
//! not only as an error.

use crate::{Lix, Memory, Value};

const SCHEMA: &str = "fsd_item";
const FILE_A: &str = "01920000-0000-7000-8000-00000000fa01";
const FILE_B: &str = "01920000-0000-7000-8000-00000000fb02";

/// `(id, value, note, file_id, global, untracked)` as SQL returns it.
type Row = (String, String, String, Option<String>, bool, bool);

const SELECT_COLUMNS: &str = "id, value, note, lixcol_file_id, lixcol_global, lixcol_untracked";

const SCOPES: [Option<&str>; 3] = [None, Some(FILE_A), Some(FILE_B)];

fn scope_name(file_id: Option<&str>) -> &'static str {
    match file_id {
        None => "unfiled",
        Some(FILE_A) => "a",
        Some(FILE_B) => "b",
        Some(other) => panic!("unexpected file scope {other}"),
    }
}

fn file_sql(file_id: Option<&str>) -> String {
    file_id.map_or_else(|| "NULL".to_owned(), |file_id| format!("'{file_id}'"))
}

fn file_filter(file_id: Option<&str>) -> String {
    file_id.map_or_else(
        || "lixcol_file_id IS NULL".to_owned(),
        |file_id| format!("lixcol_file_id = '{file_id}'"),
    )
}

fn id(index: u64) -> String {
    format!("k{index:05}")
}

/// Ids `k01000..k01019` in every file scope, committed in packed bases, plus
/// later HOT updates, so the source scan merges several layers per scope.
async fn open_fixture(branch: bool) -> Lix<Memory> {
    let lix = crate::open_lix().with_storage(Memory::new()).await.unwrap();
    for global in ["FALSE", "TRUE"] {
        lix.execute(
            &format!(
                "INSERT INTO lix_registered_schema (value, lixcol_global) VALUES (CAST('{{\"$schema\":\"https://lix.dev/schema-v1.json\",\
                 \"key\":\"{SCHEMA}\",\"columns\":[{{\"name\":\"id\",\"type\":\"text\",\"nullable\":false}},\
                 {{\"name\":\"value\",\"type\":\"text\",\"nullable\":false}},\
                 {{\"name\":\"note\",\"type\":\"text\",\"nullable\":false}}],\"primary_key\":[\"id\"]}}' AS JSONB), {global})"
            ),
            &[],
        )
        .await
        .unwrap();
    }
    lix.execute(
        &format!(
            "INSERT INTO lix_file(id,path) VALUES ('{FILE_A}','/fsd-a'), ('{FILE_B}','/fsd-b')"
        ),
        &[],
    )
    .await
    .unwrap();
    for file_id in SCOPES {
        let values = (1000..1020)
            .map(|index| {
                format!(
                    "('{}', 'v', 'note-{}-{index}', {})",
                    id(index),
                    scope_name(file_id),
                    file_sql(file_id)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        lix.execute(
            &format!("INSERT INTO {SCHEMA} (id, value, note, lixcol_file_id) VALUES {values}"),
            &[],
        )
        .await
        .unwrap();
    }
    if branch {
        let branch = lix
            .create_branch(crate::CreateBranchOptions {
                id: None,
                name: "file-scope-dml".to_owned(),
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
    // A HOT layer above the bases in every scope.
    for file_id in SCOPES {
        lix.execute(
            &format!(
                "UPDATE {SCHEMA} SET value = 'hot' WHERE id = '{}' AND {}",
                id(1003),
                file_filter(file_id)
            ),
            &[],
        )
        .await
        .unwrap();
    }
    lix
}

fn decode_rows(result: &crate::ExecuteResult) -> Vec<Row> {
    let mut rows = result
        .rows()
        .iter()
        .map(|row| {
            let text = |column: &str| match row.get::<Value>(column).unwrap() {
                Value::Text(value) => value,
                value => panic!("unexpected {column}: {value:?}"),
            };
            let flag = |column: &str| match row.get::<Value>(column).unwrap() {
                Value::Boolean(flag) => flag,
                value => panic!("unexpected {column}: {value:?}"),
            };
            let file_id = match row.get::<Value>("lixcol_file_id").unwrap() {
                Value::Null => None,
                Value::Text(file_id) => Some(file_id),
                value => panic!("unexpected file id: {value:?}"),
            };
            (
                text("id"),
                text("value"),
                text("note"),
                file_id,
                flag("lixcol_global"),
                flag("lixcol_untracked"),
            )
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

/// The rows the fixture holds, with `value` replaced by `updated` for every
/// `(index, scope)` selected by `touched`, and the selected rows removed when
/// `updated` is `None`.
fn expected_rows(touched: impl Fn(u64, Option<&str>) -> bool, updated: Option<&str>) -> Vec<Row> {
    let mut rows = Vec::new();
    for file_id in SCOPES {
        for index in 1000..1020 {
            let value = if touched(index, file_id) {
                match updated {
                    Some(updated) => updated.to_owned(),
                    None => continue,
                }
            } else if index == 1003 {
                "hot".to_owned()
            } else {
                "v".to_owned()
            };
            rows.push((
                id(index),
                value,
                format!("note-{}-{index}", scope_name(file_id)),
                file_id.map(str::to_owned),
                false,
                false,
            ));
        }
    }
    rows.sort();
    rows
}

async fn all_rows(lix: &Lix<Memory>) -> Vec<Row> {
    decode_rows(
        &lix.execute(&format!("SELECT {SELECT_COLUMNS} FROM {SCHEMA}"), &[])
            .await
            .unwrap(),
    )
}

fn in_range(index: u64) -> bool {
    (1002..1008).contains(&index)
}

const RANGE: &str = "id >= 'k01002' AND id < 'k01008'";

/// The statements under test, each with the identities it must touch.
fn update_cases() -> Vec<(String, Box<dyn Fn(u64, Option<&str>) -> bool>)> {
    let mut cases: Vec<(String, Box<dyn Fn(u64, Option<&str>) -> bool>)> = Vec::new();
    for scope in SCOPES {
        cases.push((
            format!("{RANGE} AND {}", file_filter(scope)),
            Box::new(move |index, file_id| in_range(index) && file_id == scope),
        ));
        cases.push((
            format!("id IN ('k01003', 'k01011') AND {}", file_filter(scope)),
            Box::new(move |index, file_id| (index == 1003 || index == 1011) && file_id == scope),
        ));
    }
    cases.push((RANGE.to_owned(), Box::new(|index, _| in_range(index))));
    cases.push((
        "id IN ('k01003', 'k01011')".to_owned(),
        Box::new(|index, _| index == 1003 || index == 1011),
    ));
    cases.push((
        format!("lixcol_file_id IS NOT NULL AND {RANGE}"),
        Box::new(|index, file_id| in_range(index) && file_id.is_some()),
    ));
    cases.push((
        format!("note LIKE 'note-a-%' AND {RANGE}"),
        Box::new(|index, file_id| in_range(index) && file_id == Some(FILE_A)),
    ));
    cases
}

async fn assert_updates_keep_file_scopes(branch: bool, in_transaction: bool) {
    for (predicate, touched) in update_cases() {
        let lix = open_fixture(branch).await;
        let statement = format!("UPDATE {SCHEMA} SET value = 'updated' WHERE {predicate}");
        let expected = expected_rows(&touched, Some("updated"));
        let expected_count = expected.iter().filter(|row| row.1 == "updated").count();
        if in_transaction {
            let mut transaction = lix.begin_transaction().await.unwrap();
            let result = transaction
                .execute(&statement, &[])
                .await
                .unwrap_or_else(|error| panic!("{statement}: {error:?}"));
            assert_eq!(result.rows_affected(), expected_count as u64, "{statement}");
            let staged = decode_rows(
                &transaction
                    .execute(&format!("SELECT {SELECT_COLUMNS} FROM {SCHEMA}"), &[])
                    .await
                    .unwrap(),
            );
            assert_eq!(staged, expected, "staged rows after {statement}");
            transaction.commit().await.unwrap();
        } else {
            let result = lix
                .execute(&statement, &[])
                .await
                .unwrap_or_else(|error| panic!("{statement}: {error:?}"));
            assert_eq!(result.rows_affected(), expected_count as u64, "{statement}");
        }
        assert_eq!(all_rows(&lix).await, expected, "rows after {statement}");
        lix.close().await.unwrap();
    }
}

async fn assert_deletes_keep_file_scopes(branch: bool) {
    for (predicate, touched) in update_cases() {
        let lix = open_fixture(branch).await;
        let statement = format!("DELETE FROM {SCHEMA} WHERE {predicate}");
        let expected = expected_rows(&touched, None);
        let result = lix
            .execute(&statement, &[])
            .await
            .unwrap_or_else(|error| panic!("{statement}: {error:?}"));
        assert_eq!(
            result.rows_affected(),
            (60 - expected.len()) as u64,
            "{statement}"
        );
        assert_eq!(all_rows(&lix).await, expected, "rows after {statement}");
        lix.close().await.unwrap();
    }
}

#[tokio::test]
async fn updates_touch_each_file_scope_with_its_own_source_row() {
    assert_updates_keep_file_scopes(false, false).await;
}

#[tokio::test]
async fn updates_touch_each_file_scope_with_its_own_source_row_on_a_branch() {
    assert_updates_keep_file_scopes(true, false).await;
}

#[tokio::test]
async fn staged_updates_touch_each_file_scope_with_its_own_source_row() {
    assert_updates_keep_file_scopes(false, true).await;
}

#[tokio::test]
async fn deletes_touch_each_file_scope_exactly() {
    assert_deletes_keep_file_scopes(false).await;
}

#[tokio::test]
async fn deletes_touch_each_file_scope_exactly_on_a_branch() {
    assert_deletes_keep_file_scopes(true).await;
}

/// `UPDATE ... RETURNING` reads its post-image back by identity, so it must
/// return one row per updated `(file scope, primary key)`, each with its own
/// scope's columns, and nothing from scopes the statement did not touch.
#[tokio::test]
async fn update_returning_reports_each_updated_file_scope() {
    for (predicate, touched) in update_cases() {
        let lix = open_fixture(false).await;
        let statement = format!(
            "UPDATE {SCHEMA} SET value = 'updated' WHERE {predicate} RETURNING {SELECT_COLUMNS}"
        );
        let result = lix
            .execute(&statement, &[])
            .await
            .unwrap_or_else(|error| panic!("{statement}: {error:?}"));
        let expected = expected_rows(&touched, Some("updated"))
            .into_iter()
            .filter(|row| row.1 == "updated")
            .collect::<Vec<_>>();
        assert_eq!(decode_rows(&result), expected, "{statement}");
        lix.close().await.unwrap();
    }
}

/// An untracked or global row shares its primary key with tracked rows of
/// other file scopes. An UPDATE over all of them must write each visible row
/// back into its own scope and lane, and a DELETE must remove every one.
#[tokio::test]
async fn updates_keep_untracked_and_global_rows_in_their_file_scopes() {
    let lix = open_fixture(false).await;
    for (index, file_id, untracked, global) in [
        (1030, None, true, false),
        (1030, Some(FILE_A), false, false),
        (1030, Some(FILE_B), false, false),
        (1031, None, false, true),
        (1031, Some(FILE_A), false, false),
    ] {
        lix.execute(
            &format!(
                "INSERT INTO {SCHEMA} (id, value, note, lixcol_file_id, lixcol_untracked, lixcol_global) \
                 VALUES ('{}', 'v', 'note-{}-{index}', {}, {untracked}, {global})",
                id(index),
                scope_name(file_id),
                file_sql(file_id)
            ),
            &[],
        )
        .await
        .unwrap();
    }
    let result = lix
        .execute(
            &format!("UPDATE {SCHEMA} SET value = 'updated' WHERE id >= 'k01030'"),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(result.rows_affected(), 5);
    let rows = decode_rows(
        &lix.execute(
            &format!("SELECT {SELECT_COLUMNS} FROM {SCHEMA} WHERE id >= 'k01030'"),
            &[],
        )
        .await
        .unwrap(),
    );
    let row = |index: u64, file_id: Option<&str>, global: bool, untracked: bool| -> Row {
        (
            id(index),
            "updated".to_owned(),
            format!("note-{}-{index}", scope_name(file_id)),
            file_id.map(str::to_owned),
            global,
            untracked,
        )
    };
    let mut expected = vec![
        row(1030, None, false, true),
        row(1030, Some(FILE_A), false, false),
        row(1030, Some(FILE_B), false, false),
        row(1031, None, true, false),
        row(1031, Some(FILE_A), false, false),
    ];
    expected.sort();
    assert_eq!(rows, expected);
    let result = lix
        .execute(&format!("DELETE FROM {SCHEMA} WHERE id >= 'k01030'"), &[])
        .await
        .unwrap();
    assert_eq!(result.rows_affected(), 5);
    assert_eq!(all_rows(&lix).await, expected_rows(|_, _| false, None));
    lix.close().await.unwrap();
}
