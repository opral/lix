//! Full scans read as ascending primary-key windows.
//!
//! The oracle is the same statement read as one window: with the window
//! target above the collection size the page horizon declines and the scan
//! is a single exact read of the whole collection, i.e. the former
//! full-materialization path. Every comparison runs the windowed read with
//! small window targets so window boundaries land between, and on top of,
//! identities that live in several file scopes, HOT overlays, packed and root
//! bases, untracked and global rows.

use std::collections::BTreeSet;

use super::pk_window_tests::{
    FILE_A, FILE_B, FixtureShape, Lcg, Row, SCHEMA, SELECT_COLUMNS, bulk_insert, decode_rows, id,
    layered_fixture, open_fixture,
};
use super::{primary_key_windows_read, set_primary_key_window_rows};
use crate::{Lix, Memory};

/// A window target no fixture reaches: the scan is one full read.
const ONE_WINDOW: usize = usize::MAX;

/// Every row of `sql` as debug text, in result order.
async fn rows_of(lix: &Lix<Memory>, sql: &str, window_rows: usize) -> Vec<String> {
    let _window = set_primary_key_window_rows(window_rows);
    let result = lix
        .execute(sql, &[])
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    result
        .rows()
        .iter()
        .map(|row| format!("{:?}", row.values()))
        .collect()
}

fn sorted(mut rows: Vec<String>) -> Vec<String> {
    rows.sort();
    rows
}

/// Drains `sql` through `query_stream` with `page_bytes` pages. Returns the
/// decoded rows in stream order and the windows read before the first page.
async fn stream_rows(
    lix: &Lix<Memory>,
    sql: &str,
    page_bytes: usize,
    window_rows: usize,
) -> (Vec<Row>, usize) {
    let _window = set_primary_key_window_rows(window_rows);
    let windows_before = primary_key_windows_read();
    let mut stream = lix
        .query_stream(sql, &[])
        .with_page_bytes(page_bytes)
        .await
        .unwrap();
    let mut rows = Vec::new();
    let mut windows_at_first_page = None;
    while let Some(page) = stream.next_page().await.unwrap() {
        windows_at_first_page.get_or_insert(primary_key_windows_read() - windows_before);
        rows.extend(decode_rows(&page));
    }
    (rows, windows_at_first_page.unwrap_or_default())
}

fn assert_id_order(rows: &[Row], context: &str) {
    assert!(
        rows.windows(2).all(|pair| pair[0].0 <= pair[1].0),
        "{context}: windows must be emitted in primary-key order"
    );
}

async fn reference_rows(lix: &Lix<Memory>) -> Vec<Row> {
    let _window = set_primary_key_window_rows(ONE_WINDOW);
    let windows_before = primary_key_windows_read();
    let result = lix
        .execute(&format!("SELECT {SELECT_COLUMNS} FROM {SCHEMA}"), &[])
        .await
        .unwrap();
    assert_eq!(
        primary_key_windows_read() - windows_before,
        1,
        "the reference is one read of the whole collection"
    );
    let mut rows = decode_rows(&result);
    rows.sort();
    rows
}

async fn assert_full_scan_streams_window_by_window(shape: FixtureShape) {
    let lix = layered_fixture(31, shape).await;
    let reference = reference_rows(&lix).await;
    assert!(reference.iter().any(|row| row.2.is_some()));
    assert!(reference.iter().any(|row| row.3));
    assert!(reference.iter().any(|row| row.4));
    let sql = format!("SELECT {SELECT_COLUMNS} FROM {SCHEMA}");

    // Buffered execute reads the same windows and returns the same rows.
    let windows_before = primary_key_windows_read();
    let buffered = {
        let _window = set_primary_key_window_rows(41);
        decode_rows(&lix.execute(&sql, &[]).await.unwrap())
    };
    let buffered_windows = primary_key_windows_read() - windows_before;
    // Windows end on stored span boundaries (packed parts, tree children),
    // so they are at least one span wide; the collection still spans many.
    assert!(
        buffered_windows >= 8,
        "{shape:?}: {buffered_windows} windows for {} rows",
        reference.len()
    );
    assert_id_order(&buffered, &format!("{shape:?} buffered"));
    let mut buffered_sorted = buffered;
    buffered_sorted.sort();
    assert_eq!(buffered_sorted, reference, "{shape:?} buffered");

    // A stream hands out its first page after reading at most the window
    // it is cutting and the one the producer reads ahead.
    // A page larger than the fixture is cut only at the end of the stream.
    for page_bytes in [1, 300, 1 << 20] {
        let (rows, windows_at_first_page) = stream_rows(&lix, &sql, page_bytes, 41).await;
        assert!(
            page_bytes == 1 << 20 || (1..=2).contains(&windows_at_first_page),
            "{shape:?} {page_bytes}: first page after {windows_at_first_page} windows"
        );
        assert_id_order(&rows, &format!("{shape:?} stream {page_bytes}"));
        let mut rows = rows;
        rows.sort();
        assert_eq!(rows, reference, "{shape:?} stream {page_bytes}");
    }
    lix.close().await.unwrap();
}

#[tokio::test]
async fn full_scans_stream_window_by_window_over_packed_bases() {
    assert_full_scan_streams_window_by_window(FixtureShape {
        checkpoint: false,
        branch: false,
    })
    .await;
}

#[tokio::test]
async fn full_scans_stream_window_by_window_over_a_checkpoint_root() {
    assert_full_scan_streams_window_by_window(FixtureShape {
        checkpoint: true,
        branch: false,
    })
    .await;
}

#[tokio::test]
async fn full_scans_stream_window_by_window_on_a_branch_root() {
    assert_full_scan_streams_window_by_window(FixtureShape {
        checkpoint: false,
        branch: true,
    })
    .await;
}

/// Every window is read from the stream's pinned snapshot: writes committed
/// between two pulls — to rows in windows not read yet, to rows already
/// streamed, new identities in every file scope, untracked and global rows —
/// are invisible to the open stream and visible to the next read.
#[tokio::test]
async fn stream_windows_read_the_pinned_snapshot_across_writes_between_pulls() {
    for shape in [
        FixtureShape {
            checkpoint: false,
            branch: false,
        },
        FixtureShape {
            checkpoint: false,
            branch: true,
        },
    ] {
        let lix = layered_fixture(37, shape).await;
        let before = reference_rows(&lix).await;
        let _window = set_primary_key_window_rows(29);
        let mut stream = lix
            .query_stream(&format!("SELECT {SELECT_COLUMNS} FROM {SCHEMA}"), &[])
            .with_page_bytes(512)
            .await
            .unwrap();
        let mut streamed = decode_rows(&stream.next_page().await.unwrap().unwrap());
        let first_page_end = streamed.last().unwrap().0.clone();
        let streamed_scope = streamed[0].2.as_ref().map_or_else(
            || "lixcol_file_id IS NULL".to_owned(),
            |file_id| format!("lixcol_file_id = '{file_id}'"),
        );
        assert!(
            first_page_end.as_str() < "k01000",
            "{shape:?}: {first_page_end}"
        );
        for statement in [
            format!("DELETE FROM {SCHEMA} WHERE id >= 'k03000' AND id < 'k03400'"),
            format!(
                "UPDATE {SCHEMA} SET value = 'after' WHERE id = 'k02000' AND lixcol_file_id IS NULL"
            ),
            format!(
                "UPDATE {SCHEMA} SET value = 'after' WHERE id = 'k02098' AND lixcol_file_id IS NULL"
            ),
            format!(
                "UPDATE {SCHEMA} SET value = 'after' WHERE id = '{}' AND {streamed_scope}",
                streamed[0].0
            ),
            format!(
                "INSERT INTO {SCHEMA} (id, value, lixcol_file_id) VALUES \
                 ('k04000-after', 'after', NULL), ('k04000-after', 'after', '{FILE_A}'), \
                 ('k00000-after', 'after', '{FILE_B}')"
            ),
            format!(
                "INSERT INTO {SCHEMA} (id, value, lixcol_untracked) VALUES ('k01500-after', 'after', TRUE)"
            ),
            format!(
                "INSERT INTO {SCHEMA} (id, value, lixcol_global) VALUES ('k04500-after', 'after', TRUE)"
            ),
        ] {
            lix.execute(&statement, &[])
                .await
                .unwrap_or_else(|error| panic!("{statement}: {error:?}"));
        }
        while let Some(page) = stream.next_page().await.unwrap() {
            streamed.extend(decode_rows(&page));
        }
        assert_id_order(&streamed, &format!("{shape:?}"));
        streamed.sort();
        assert_eq!(streamed, before, "{shape:?}: the stream saw a later write");
        drop(_window);
        let after = reference_rows(&lix).await;
        assert_ne!(after, before);
        assert!(after.iter().any(|row| row.0 == "k04000-after"));
        assert!(after.iter().all(|row| row.1 != "base-1500"));
        lix.close().await.unwrap();
    }
}

/// One identity in the unfiled scope, two file scopes and the global branch:
/// window targets of one and two stored rows put window boundaries on every
/// such identity, and one-byte pages put a page boundary between each of its
/// rows. Each `(id, file)` row is streamed exactly once.
#[tokio::test]
async fn window_and_page_boundaries_split_one_identity_across_file_scopes() {
    let lix = open_fixture().await;
    let mut rows = Vec::new();
    for index in 0..120_u64 {
        rows.push((id(index), format!("unfiled-{index}"), None));
        if index % 2 == 0 {
            rows.push((id(index), format!("a-{index}"), Some(FILE_A)));
        }
        if index % 3 == 0 {
            rows.push((id(index), format!("b-{index}"), Some(FILE_B)));
        }
    }
    bulk_insert(&lix, &rows).await;
    for index in [6_u64, 7, 60, 119] {
        lix.execute(
            &format!(
                "INSERT INTO {SCHEMA} (id, value, lixcol_file_id, lixcol_global) VALUES ('{}', 'global', '{FILE_B}', TRUE)",
                id(index)
            ),
            &[],
        )
        .await
        .unwrap();
    }
    for statement in [
        format!(
            "UPDATE {SCHEMA} SET value = 'hot' WHERE id = '{}' AND lixcol_file_id = '{FILE_A}'",
            id(30)
        ),
        format!(
            "DELETE FROM {SCHEMA} WHERE id = '{}' AND lixcol_file_id IS NULL",
            id(30)
        ),
        format!(
            "DELETE FROM {SCHEMA} WHERE id = '{}' AND lixcol_file_id = '{FILE_B}'",
            id(33)
        ),
    ] {
        lix.execute(&statement, &[]).await.unwrap();
    }
    let reference = reference_rows(&lix).await;
    let sql = format!("SELECT {SELECT_COLUMNS} FROM {SCHEMA}");
    for window_rows in [1, 2, 3] {
        for page_bytes in [1, 64] {
            let (streamed, _) = stream_rows(&lix, &sql, page_bytes, window_rows).await;
            assert_id_order(&streamed, &format!("window {window_rows}"));
            let identities = streamed
                .iter()
                .map(|row| (row.0.clone(), row.2.clone()))
                .collect::<BTreeSet<_>>();
            assert_eq!(identities.len(), streamed.len(), "window {window_rows}");
            let mut streamed = streamed;
            streamed.sort();
            assert_eq!(
                streamed, reference,
                "window {window_rows} pages {page_bytes}"
            );
        }
    }
    lix.close().await.unwrap();
}

/// Randomized equivalence with the one-window read: projections, residual
/// predicates, aggregates and joins over windowed scans, after every round
/// of random mutations across file scopes, untracked and global rows.
#[tokio::test]
async fn windowed_reads_match_one_full_read_across_random_mutations() {
    let statements = [
        format!("SELECT {SELECT_COLUMNS} FROM {SCHEMA}"),
        format!("SELECT id FROM {SCHEMA}"),
        format!("SELECT value, lixcol_file_id FROM {SCHEMA} WHERE value LIKE 'base-1%'"),
        format!("SELECT COUNT(*), MIN(id), MAX(value) FROM {SCHEMA}"),
        format!("SELECT lixcol_file_id, COUNT(*) FROM {SCHEMA} GROUP BY lixcol_file_id"),
        format!(
            "SELECT a.id, a.value, b.value FROM {SCHEMA} a JOIN {SCHEMA} b \
             ON a.id = b.id AND a.lixcol_file_id IS NULL AND b.lixcol_file_id IS NOT NULL"
        ),
        format!("SELECT id, value FROM {SCHEMA} ORDER BY id DESC LIMIT 17"),
    ];
    for seed in [5_u64, 41] {
        let lix = layered_fixture(
            seed,
            FixtureShape {
                checkpoint: seed % 2 == 0,
                branch: seed % 2 == 1,
            },
        )
        .await;
        let mut rng = Lcg::new(seed.wrapping_mul(104_729));
        for round in 0..3 {
            for step in 0..25 {
                let index = rng.below(4800);
                let file_filter = match rng.below(3) {
                    0 => "lixcol_file_id IS NULL".to_owned(),
                    1 => format!("lixcol_file_id = '{FILE_A}'"),
                    _ => format!("lixcol_file_id = '{FILE_B}'"),
                };
                let statement = match rng.below(4) {
                    0 => format!(
                        "DELETE FROM {SCHEMA} WHERE id = '{}' AND {file_filter}",
                        id(index)
                    ),
                    1 => format!(
                        "UPDATE {SCHEMA} SET value = 'r{round}s{step}' WHERE id = '{}' AND {file_filter}",
                        id(index)
                    ),
                    2 => format!(
                        "INSERT INTO {SCHEMA} (id, value, lixcol_file_id) VALUES ('{}-r{round}s{step}', 'v', '{FILE_A}')",
                        id(index)
                    ),
                    _ => format!(
                        "DELETE FROM {SCHEMA} WHERE id >= '{}' AND id < '{}'",
                        id(index),
                        id(index + 5)
                    ),
                };
                lix.execute(&statement, &[]).await.unwrap();
            }
            let window_rows = [1, 7, 64, 1000][usize::try_from(rng.below(4)).unwrap()];
            for sql in &statements {
                let expected = sorted(rows_of(&lix, sql, ONE_WINDOW).await);
                let actual = sorted(rows_of(&lix, sql, window_rows).await);
                assert_eq!(
                    actual, expected,
                    "seed {seed} round {round} window {window_rows}: {sql}"
                );
            }
            let (streamed, _) = stream_rows(&lix, &statements[0], 128, window_rows).await;
            let mut streamed = streamed;
            streamed.sort();
            assert_eq!(
                streamed,
                reference_rows(&lix).await,
                "seed {seed} round {round}"
            );
        }
        lix.close().await.unwrap();
    }
}

/// Windows exist for every primary-key type, not only the string keys whose
/// order SQL can declare: integer and composite keys stream window by window
/// with the same rows as one full read.
#[tokio::test]
async fn non_string_primary_keys_are_read_in_windows() {
    let lix = crate::open_lix().with_storage(Memory::new()).await.unwrap();
    for (key, columns, primary_key) in [
        (
            "pkw_int",
            r#"{"name":"n","type":"int8","nullable":false},{"name":"value","type":"text","nullable":false}"#,
            r#"["n"]"#,
        ),
        (
            "pkw_pair",
            r#"{"name":"g","type":"text","nullable":false},{"name":"n","type":"int8","nullable":false},{"name":"value","type":"text","nullable":false}"#,
            r#"["g","n"]"#,
        ),
    ] {
        lix.execute(
            &format!(
                "INSERT INTO lix_registered_schema (value) VALUES (CAST('{{\"$schema\":\"https://lix.dev/schema-v1.json\",\
                 \"key\":\"{key}\",\"columns\":[{columns}],\"primary_key\":{primary_key}}}' AS JSONB))"
            ),
            &[],
        )
        .await
        .unwrap();
    }
    let mut transaction = lix.begin_transaction().await.unwrap();
    for chunk in (0..900_i64).collect::<Vec<_>>().chunks(300) {
        let ints = chunk
            .iter()
            .map(|n| format!("({}, 'v{n}')", n * 7 - 3000))
            .collect::<Vec<_>>()
            .join(", ");
        transaction
            .execute(
                &format!("INSERT INTO pkw_int (n, value) VALUES {ints}"),
                &[],
            )
            .await
            .unwrap();
        let pairs = chunk
            .iter()
            .map(|n| format!("('g{}', {}, 'v{n}')", n % 13, n / 13 - 20))
            .collect::<Vec<_>>()
            .join(", ");
        transaction
            .execute(
                &format!("INSERT INTO pkw_pair (g, n, value) VALUES {pairs}"),
                &[],
            )
            .await
            .unwrap();
    }
    transaction.commit().await.unwrap();
    for statement in [
        "DELETE FROM pkw_int WHERE n > 100 AND n < 400",
        "UPDATE pkw_int SET value = 'hot' WHERE n < -2900",
        "INSERT INTO pkw_int (n, value) VALUES (5, 'new'), (100000, 'new')",
        "DELETE FROM pkw_pair WHERE g = 'g3'",
        "UPDATE pkw_pair SET value = 'hot' WHERE n = 0",
    ] {
        lix.execute(statement, &[]).await.unwrap();
    }
    for sql in [
        "SELECT n, value FROM pkw_int",
        "SELECT g, n, value FROM pkw_pair",
        "SELECT COUNT(*), SUM(n) FROM pkw_int",
    ] {
        let expected = sorted(rows_of(&lix, sql, ONE_WINDOW).await);
        let windows_before = primary_key_windows_read();
        let actual = sorted(rows_of(&lix, sql, 13).await);
        assert!(primary_key_windows_read() - windows_before > 10, "{sql}");
        assert_eq!(actual, expected, "{sql}");
    }
    lix.close().await.unwrap();
}

/// DML sources are one read of the matched rows, never a windowed scan: an
/// UPDATE or DELETE whose matched rows span many primary-key windows (with
/// windows forced small) changes exactly the rows one full read selects, in
/// every file scope it names.
#[tokio::test]
async fn dml_over_many_windows_changes_exactly_the_matched_rows() {
    let lix = layered_fixture(
        43,
        FixtureShape {
            checkpoint: false,
            branch: true,
        },
    )
    .await;
    let before = reference_rows(&lix).await;
    let _window = set_primary_key_window_rows(3);
    let in_range = |row: &Row| row.0.as_str() >= "k00100" && row.0.as_str() < "k01500";
    let updated = lix
        .execute(
            &format!(
                "UPDATE {SCHEMA} SET value = 'windowed' WHERE id >= 'k00100' AND id < 'k01500' \
                 AND lixcol_file_id = '{FILE_A}'"
            ),
            &[],
        )
        .await
        .unwrap();
    let expected_updates = before
        .iter()
        .filter(|row| in_range(row) && row.2.as_deref() == Some(FILE_A))
        .count();
    assert!(expected_updates > 20);
    assert_eq!(updated.rows_affected(), expected_updates as u64);
    let deleted = lix
        .execute(
            &format!(
                "DELETE FROM {SCHEMA} WHERE id >= 'k02000' AND id < 'k03000' AND lixcol_file_id IS NULL"
            ),
            &[],
        )
        .await
        .unwrap();
    let in_delete =
        |row: &Row| row.0.as_str() >= "k02000" && row.0.as_str() < "k03000" && row.2.is_none();
    assert_eq!(
        deleted.rows_affected(),
        before.iter().filter(|row| in_delete(row)).count() as u64
    );
    drop(_window);
    let after = reference_rows(&lix).await;
    let mut expected = before
        .iter()
        .filter(|row| !in_delete(row))
        .cloned()
        .map(|mut row| {
            if in_range(&row) && row.2.as_deref() == Some(FILE_A) {
                row.1 = "windowed".to_owned();
            }
            row
        })
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(after.len(), expected.len());
    assert_eq!(after, expected);
    lix.close().await.unwrap();
}

/// Payload-free scans (`count(*)`, `SELECT 1`, system-column projections)
/// read the packed identity plane. Each window must visit only the mutation
/// parts that can hold its keys: the identity rows visited across all windows
/// stay proportional to the collection, never windows x collection.
#[tokio::test]
async fn payload_free_windowed_scans_visit_each_packed_identity_about_once() {
    let lix = crate::open_lix().with_storage(Memory::new()).await.unwrap();
    lix.execute(
        &format!(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST('{{\"$schema\":\"https://lix.dev/schema-v1.json\",\
             \"key\":\"{SCHEMA}\",\"columns\":[{{\"name\":\"id\",\"type\":\"text\",\"nullable\":false}},\
             {{\"name\":\"value\",\"type\":\"text\",\"nullable\":false}}],\"primary_key\":[\"id\"]}}' AS JSONB))"
        ),
        &[],
    )
    .await
    .unwrap();
    const ROWS: u64 = 4000;
    bulk_insert(
        &lix,
        &(0..ROWS)
            .map(|index| (id(index), format!("v{index}"), None))
            .collect::<Vec<_>>(),
    )
    .await;
    for (sql, expected) in [
        (
            format!("SELECT count(*) FROM {SCHEMA}"),
            vec![format!("{:?}", [crate::Value::Integer(ROWS as i64)])],
        ),
        (
            format!("SELECT count(lixcol_change_id) FROM {SCHEMA}"),
            vec![format!("{:?}", [crate::Value::Integer(ROWS as i64)])],
        ),
    ] {
        let windows_before = primary_key_windows_read();
        crate::hot_state::take_packed_identity_rows_visited_for_test();
        let actual = rows_of(&lix, &sql, 64).await;
        let visited = crate::hot_state::take_packed_identity_rows_visited_for_test();
        let windows = primary_key_windows_read() - windows_before;
        assert_eq!(actual, expected, "{sql}");
        assert!(windows >= 5, "{sql}: {windows} windows");
        assert!(
            visited <= 2 * ROWS as usize,
            "{sql}: {windows} windows visited {visited} packed identities for {ROWS} rows"
        );
    }
    for sql in [
        format!("SELECT 1 FROM {SCHEMA}"),
        format!("SELECT lixcol_file_id, lixcol_change_id FROM {SCHEMA}"),
    ] {
        let expected = sorted(rows_of(&lix, &sql, ONE_WINDOW).await);
        crate::hot_state::take_packed_identity_rows_visited_for_test();
        let actual = sorted(rows_of(&lix, &sql, 64).await);
        let visited = crate::hot_state::take_packed_identity_rows_visited_for_test();
        assert_eq!(actual.len(), ROWS as usize, "{sql}");
        assert_eq!(actual, expected, "{sql}");
        assert!(visited <= 2 * ROWS as usize, "{sql}: visited {visited}");
    }
    lix.close().await.unwrap();
}
