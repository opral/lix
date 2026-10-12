//! The query surface: buffered reads, single-row reads, streams, snapshots
//! and transaction reads.

use futures_util::StreamExt;

use crate::{Lix, LixError, Value, open_lix};

async fn seeded(rows: usize) -> Lix {
    let lix = open_lix().await.expect("open lix");
    lix.execute("INSERT INTO lix_registered_schema (value) VALUES (CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\
         \"key\":\"api_item\",\"columns\":[{\"name\":\"id\",\"type\":\"int8\",\"nullable\":false},\
         {\"name\":\"label\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB))")
    .await
    .expect("register schema");
    insert(&lix, 0..rows).await;
    lix
}

async fn insert(lix: &Lix, range: std::ops::Range<usize>) {
    let ids = range.collect::<Vec<_>>();
    for chunk in ids.chunks(500) {
        let values = chunk
            .iter()
            .map(|id| format!("({id}, 'label-{id}')"))
            .collect::<Vec<_>>()
            .join(", ");
        lix.execute(&format!("INSERT INTO api_item (id, label) VALUES {values}"))
            .await
            .expect("insert rows");
    }
}

async fn count(lix: &Lix) -> i64 {
    lix.query("SELECT COUNT(*) AS n FROM api_item")
        .one()
        .await
        .unwrap()
        .get::<i64>("n")
        .unwrap()
}

#[tokio::test]
async fn buffered_reads_bind_parameters_and_cap_bytes() {
    let lix = seeded(10).await;
    let rows = lix
        .query("SELECT id, label FROM api_item WHERE id >= $1 AND label <> $2 ORDER BY id")
        .bind(3_i64)
        .bind("label-4")
        .all()
        .await
        .unwrap();
    assert_eq!(rows.columns(), ["id", "label"]);
    assert_eq!(
        rows.iter()
            .map(|row| row.get("id").unwrap().clone())
            .collect::<Vec<_>>(),
        [3, 5, 6, 7, 8, 9].map(Value::Integer)
    );
    let first = &rows.rows()[0];
    assert_eq!(first.get::<i64>(0).unwrap(), 3);
    assert_eq!(first.get::<String>("label").unwrap(), "label-3");
    assert_eq!(first[1], Value::Text("label-3".into()));

    let params = [Value::Integer(1), Value::Integer(2)];
    let some = lix
        .query("SELECT id FROM api_item WHERE id IN ($1, $2) ORDER BY id")
        .params(&params)
        .all()
        .await
        .unwrap();
    assert_eq!(some.len(), 2);

    let error = lix
        .query("SELECT id, label FROM api_item")
        .max_bytes(64)
        .all()
        .await
        .unwrap_err();
    assert_eq!(error.code, LixError::CODE_RESULT_TOO_LARGE);
    assert!(error.hint.unwrap().contains("stream()"));
    assert_eq!(
        lix.query("SELECT id, label FROM api_item")
            .max_bytes(usize::MAX)
            .all()
            .await
            .unwrap()
            .len(),
        10
    );
}

#[tokio::test]
async fn narrow_and_null_columns_are_charged_for_their_rows() {
    let lix = seeded(1).await;
    for sql in [
        "SELECT true AS flag FROM generate_series(1, 100000)",
        "SELECT NULL AS nothing FROM generate_series(1, 100000)",
    ] {
        let error = lix
            .query(sql)
            .max_bytes(1024 * 1024)
            .all()
            .await
            .unwrap_err();
        assert_eq!(error.code, LixError::CODE_RESULT_TOO_LARGE, "{sql}");
    }
}

#[tokio::test]
async fn single_row_reads_report_absent_and_ambiguous_results() {
    let lix = seeded(3).await;
    let row = lix
        .query("SELECT label FROM api_item WHERE id = $1")
        .bind(2_i64)
        .one()
        .await
        .unwrap();
    assert_eq!(row.get::<String>("label").unwrap(), "label-2");
    assert!(
        lix.query("SELECT label FROM api_item WHERE id = 99")
            .optional()
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        lix.query("SELECT label FROM api_item WHERE id = 99")
            .one()
            .await
            .unwrap_err()
            .code,
        LixError::CODE_NO_ROWS
    );
    for result in [
        lix.query("SELECT id FROM api_item").one().await.map(|_| ()),
        lix.query("SELECT id FROM api_item")
            .optional()
            .await
            .map(|_| ()),
    ] {
        assert_eq!(result.unwrap_err().code, LixError::CODE_NOT_UNIQUE);
    }
    // Single-row reads are not byte-capped.
    let big = lix
        .query("SELECT repeat('x', 70000000) AS big")
        .one()
        .await
        .unwrap();
    assert_eq!(big.get::<String>("big").unwrap().len(), 70_000_000);
}

#[tokio::test]
async fn duplicate_column_names_fail_by_name_and_read_by_position() {
    let lix = seeded(1).await;
    let row = lix
        .query("SELECT a.id, b.id FROM api_item a JOIN api_item b ON a.id = b.id")
        .one()
        .await
        .unwrap();
    assert_eq!(
        row.get::<i64>("id").unwrap_err().code,
        LixError::CODE_AMBIGUOUS_COLUMN
    );
    assert_eq!(row.columns(), ["id", "id"]);
    assert_eq!(row.get::<i64>(0).unwrap(), 0);
    assert_eq!(row.get::<i64>(1).unwrap(), 0);
}

#[tokio::test]
async fn reads_reject_writes_and_durable_functions() {
    let lix = seeded(1).await;
    for sql in [
        "INSERT INTO api_item (id, label) VALUES (5, 'x')",
        "DELETE FROM api_item",
        "SELECT commit_id FROM lix_create_checkpoint(NULL, NULL)",
    ] {
        assert_eq!(
            lix.query(sql).all().await.unwrap_err().code,
            LixError::CODE_READ_ONLY,
            "{sql}"
        );
        assert_eq!(
            lix.query(sql).stream().await.unwrap_err().code,
            LixError::CODE_READ_ONLY,
            "{sql}"
        );
    }
    assert_eq!(count(&lix).await, 1);
}

#[tokio::test]
async fn runtime_functions_are_reads() {
    let lix = seeded(1).await;
    let sql = "SELECT uuidv7() AS id, current_timestamp AS now";
    let first = lix.query(sql).one().await.unwrap();
    let second = lix.query(sql).one().await.unwrap();
    assert_ne!(first.get::<String>("id"), second.get::<String>("id"));
    let mut stream = lix.query(sql).stream().await.unwrap();
    assert!(stream.try_next().await.unwrap().is_some());
    let snapshot = lix.snapshot().await.unwrap();
    assert!(snapshot.query(sql).optional().await.unwrap().is_some());
    let transaction = lix.begin_transaction().await.unwrap();
    assert!(transaction.query(sql).optional().await.unwrap().is_some());
    transaction.rollback().await.unwrap();
    // execute() also accepts them.
    assert_eq!(lix.execute(sql).await.unwrap().len(), 1);

    // Deterministic mode advances a persisted sequence: the live handle
    // advances it, a pinned snapshot refuses rather than reusing values.
    lix.execute(
        "INSERT INTO lix_key_value (key, value, lixcol_global, lixcol_untracked) \
         VALUES ('lix_deterministic_mode', CAST('{\"enabled\":true}' AS JSONB), true, true)",
    )
    .await
    .unwrap();
    let first = lix.query("SELECT uuidv7() AS id").one().await.unwrap();
    let second = lix.query("SELECT uuidv7() AS id").one().await.unwrap();
    assert_ne!(first.get::<String>("id"), second.get::<String>("id"));
    let snapshot = lix.snapshot().await.unwrap();
    let error = snapshot
        .query("SELECT uuidv7() AS id")
        .all()
        .await
        .unwrap_err();
    assert_eq!(error.code, LixError::CODE_READ_ONLY);
    assert!(error.hint.unwrap().contains("Lix::query()"));
}

#[tokio::test]
async fn streams_deliver_the_first_page_on_open_then_rows_and_pages() {
    let lix = seeded(5_000).await;
    let mut stream = lix
        .query("SELECT id FROM api_item ORDER BY id")
        .stream()
        .await
        .unwrap();
    assert_eq!(stream.columns(), ["id"]);
    let first = stream.next_page().await.unwrap().unwrap();
    assert_eq!(first.len(), 64, "the first page is small");
    let second = stream.next_page().await.unwrap().unwrap();
    assert_eq!(second.len(), 4096);
    let next = stream.try_next().await.unwrap().unwrap();
    assert_eq!(next.get::<i64>("id").unwrap(), 64 + 4096);
    let rest = (&mut stream)
        .map(|row| row.unwrap().get::<i64>(0).unwrap())
        .collect::<Vec<_>>()
        .await;
    assert_eq!(rest.len(), 5_000 - 64 - 4096 - 1);
    assert_eq!(*rest.last().unwrap(), 4_999);
    assert!(stream.try_next().await.unwrap().is_none());
    assert!(stream.next_page().await.unwrap().is_none());

    // Planning errors fail the open.
    let error = lix
        .query("SELECT missing FROM api_item")
        .stream()
        .await
        .unwrap_err();
    assert_eq!(error.code, LixError::CODE_COLUMN_NOT_FOUND);
    // An empty result opens and ends.
    let mut empty = lix
        .query("SELECT id FROM api_item WHERE id < 0")
        .stream()
        .await
        .unwrap();
    assert!(empty.try_next().await.unwrap().is_none());
    // Closing a partly read stream stops it.
    let stream = lix.query("SELECT id FROM api_item").stream().await.unwrap();
    stream.close().await.unwrap();
    assert_eq!(lix.running_query_stream_count_for_test(), 0);
}

#[tokio::test]
async fn snapshots_read_one_state_across_reads_and_concurrent_streams() {
    let lix = seeded(300).await;
    let snapshot = lix.snapshot().await.unwrap();
    let head = lix
        .query("SELECT lix_active_branch_commit_id() AS id")
        .one()
        .await
        .unwrap()
        .get::<String>("id")
        .unwrap();
    assert_eq!(snapshot.commit_id(), head);
    assert_eq!(
        snapshot.active_branch_id(),
        lix.active_branch_id().await.unwrap()
    );

    let mut left = snapshot
        .query("SELECT id FROM api_item ORDER BY id")
        .stream()
        .await
        .unwrap();
    insert(&lix, 300..400).await;
    lix.execute("DELETE FROM api_item WHERE id < 50")
        .await
        .unwrap();
    let mut right = snapshot
        .query("SELECT id FROM api_item ORDER BY id DESC")
        .stream()
        .await
        .unwrap();
    assert_eq!(count(&lix).await, 350);
    let snapshot_count = snapshot
        .query("SELECT COUNT(*) AS n FROM api_item")
        .one()
        .await
        .unwrap()
        .get::<i64>("n")
        .unwrap();
    assert_eq!(snapshot_count, 300);

    // Interleave the two streams.
    let (mut ascending, mut descending) = (Vec::new(), Vec::new());
    loop {
        let a = left.try_next().await.unwrap();
        let b = right.try_next().await.unwrap();
        if a.is_none() && b.is_none() {
            break;
        }
        ascending.extend(a.map(|row| row.get::<i64>(0).unwrap()));
        descending.extend(b.map(|row| row.get::<i64>(0).unwrap()));
    }
    assert_eq!(ascending, (0..300).collect::<Vec<_>>());
    assert_eq!(descending, (0..300).rev().collect::<Vec<_>>());

    // A stream keeps reading after its snapshot handle is dropped.
    let mut orphan = snapshot
        .query("SELECT id FROM api_item ORDER BY id")
        .stream()
        .await
        .unwrap();
    drop(snapshot);
    let mut seen = 0;
    while orphan.try_next().await.unwrap().is_some() {
        seen += 1;
    }
    assert_eq!(seen, 300);
}

#[tokio::test]
async fn closing_a_snapshot_stops_its_streams_and_later_reads() {
    let lix = seeded(500).await;
    let snapshot = lix.snapshot().await.unwrap();
    let clone = snapshot.clone();
    let mut stream = snapshot
        .query("SELECT id FROM api_item")
        .stream()
        .await
        .unwrap();
    assert_eq!(lix.running_query_stream_count_for_test(), 1);
    clone.close().await.unwrap();
    assert_eq!(lix.running_query_stream_count_for_test(), 0);
    // The next pull fails although the first page was already buffered.
    assert_eq!(
        stream.try_next().await.unwrap_err().code,
        LixError::CODE_SNAPSHOT_CLOSED
    );
    for result in [
        snapshot.query("SELECT 1").all().await.map(|_| ()),
        snapshot.query("SELECT 1").stream().await.map(|_| ()),
    ] {
        assert_eq!(result.unwrap_err().code, LixError::CODE_SNAPSHOT_CLOSED);
    }
    snapshot.close().await.unwrap();

    // Closing the handle closes its snapshots.
    let open = lix.snapshot().await.unwrap();
    lix.close().await.unwrap();
    assert_eq!(
        open.query("SELECT 1").all().await.unwrap_err().code,
        LixError::CODE_CLOSED
    );
}

#[tokio::test]
async fn snapshot_reads_and_streams_reject_writes() {
    let lix = seeded(1).await;
    let snapshot = lix.snapshot().await.unwrap();
    for sql in [
        "INSERT INTO api_item (id, label) VALUES (7, 'x')",
        "UPDATE api_item SET label = 'y'",
    ] {
        assert_eq!(
            snapshot.query(sql).all().await.unwrap_err().code,
            LixError::CODE_READ_ONLY
        );
    }
    assert_eq!(count(&lix).await, 1);
}

#[tokio::test]
async fn transaction_reads_and_streams_see_their_own_writes() {
    let lix = seeded(3).await;
    let transaction = lix.begin_transaction().await.unwrap();
    transaction
        .execute("INSERT INTO api_item (id, label) VALUES (10, 'staged')")
        .await
        .unwrap();
    let staged = transaction
        .query("SELECT label FROM api_item WHERE id = 10")
        .one()
        .await
        .unwrap();
    assert_eq!(staged.get::<String>("label").unwrap(), "staged");
    let mut stream = transaction
        .query("SELECT id FROM api_item ORDER BY id")
        .stream()
        .await
        .unwrap();
    let mut ids = Vec::new();
    while let Some(row) = stream.try_next().await.unwrap() {
        ids.push(row.get::<i64>(0).unwrap());
    }
    assert_eq!(ids, [0, 1, 2, 10]);
    assert_eq!(count(&lix).await, 3, "outside reads do not see staged rows");
    assert_eq!(
        transaction
            .query("DELETE FROM api_item")
            .all()
            .await
            .unwrap_err()
            .code,
        LixError::CODE_READ_ONLY
    );
    transaction.commit().await.unwrap();
    assert_eq!(count(&lix).await, 4);
}

#[test]
fn query_types_are_send_and_static() {
    fn assert_send_static<T: Send + 'static>() {}
    fn assert_sync<T: Sync>() {}
    assert_send_static::<crate::Query>();
    assert_send_static::<crate::Execute>();
    assert_send_static::<crate::ExecuteMany>();
    assert_send_static::<crate::RowStream>();
    assert_send_static::<crate::Snapshot>();
    assert_sync::<crate::Snapshot>();
    assert_send_static::<crate::LixTransaction>();
    assert_sync::<crate::LixTransaction>();
    fn assert_stream<T: futures_core::stream::FusedStream + Unpin>() {}
    assert_stream::<crate::RowStream>();
}

#[tokio::test]
async fn transaction_execute_many_is_all_or_nothing() {
    let lix = seeded(3).await;
    let transaction = lix.begin_transaction().await.unwrap();
    transaction
        .execute("INSERT INTO api_item (id, label) VALUES (100, 'kept')")
        .await
        .unwrap();
    let error = transaction
        .execute_many(
            "INSERT INTO api_item (id, label) VALUES ($1, $2)",
            [
                [Value::Integer(200), Value::from("first")],
                [Value::from("not a number"), Value::from("bad")],
            ],
        )
        .await
        .unwrap_err();
    assert_ne!(error.code, LixError::CODE_INTERNAL_ERROR);
    // The failed call staged nothing; earlier writes in the transaction stay.
    assert!(
        transaction
            .query("SELECT id FROM api_item WHERE id = 200")
            .optional()
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        transaction
            .execute_many(
                "INSERT INTO api_item (id, label) VALUES ($1, $2)",
                [[Value::Integer(201), Value::from("ok")]],
            )
            .await
            .unwrap(),
        1
    );
    // The statement is checked even without parameter sets.
    assert_eq!(
        transaction
            .execute_many("SELECT $1", Vec::<[Value; 1]>::new())
            .await
            .unwrap_err()
            .code,
        LixError::CODE_INVALID_PARAM
    );
    assert_eq!(
        transaction
            .execute_many(
                "INSERT INTO api_item (id, label) VALUES ($1, 'r') RETURNING id",
                [[Value::Integer(300)]],
            )
            .await
            .unwrap_err()
            .code,
        LixError::CODE_INVALID_PARAM
    );
    transaction.commit().await.unwrap();
    let ids = lix
        .query("SELECT id FROM api_item WHERE id >= 100 ORDER BY id")
        .all()
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<i64>(0).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(ids, [100, 201]);
    assert_eq!(
        lix.execute_many("SELECT $1", Vec::<[Value; 1]>::new())
            .await
            .unwrap_err()
            .code,
        LixError::CODE_INVALID_PARAM
    );
}

#[tokio::test]
async fn execute_runs_reads_under_the_byte_cap() {
    let lix = seeded(100).await;
    let result = lix
        .execute("SELECT id, label FROM api_item WHERE id < $1 ORDER BY id")
        .bind(3_i64)
        .await
        .unwrap();
    assert_eq!(result.rows_affected(), 0);
    assert_eq!(
        result
            .rows()
            .iter()
            .map(|row| row.get::<i64>("id").unwrap())
            .collect::<Vec<_>>(),
        [0, 1, 2]
    );
    assert_eq!(
        lix.execute("SELECT id, label FROM api_item")
            .max_bytes(128)
            .await
            .unwrap_err()
            .code,
        LixError::CODE_RESULT_TOO_LARGE
    );

    let transaction = lix.begin_transaction().await.unwrap();
    transaction
        .execute("INSERT INTO api_item (id, label) VALUES (1000, 'staged')")
        .await
        .unwrap();
    let staged = transaction
        .execute("SELECT label FROM api_item WHERE id = 1000")
        .await
        .unwrap();
    assert_eq!(staged.rows()[0].get::<String>("label").unwrap(), "staged");
    assert_eq!(
        transaction
            .execute("SELECT id, label FROM api_item")
            .max_bytes(128)
            .await
            .unwrap_err()
            .code,
        LixError::CODE_RESULT_TOO_LARGE
    );
    transaction.commit().await.unwrap();
    assert_eq!(count(&lix).await, 101);
}

#[tokio::test]
async fn returning_rows_over_the_cap_fail_without_committing() {
    let lix = seeded(50).await;
    let error = lix
        .execute("UPDATE api_item SET label = 'capped' RETURNING id, label")
        .max_bytes(256)
        .await
        .unwrap_err();
    assert_eq!(error.code, LixError::CODE_RESULT_TOO_LARGE);
    assert_eq!(
        lix.query("SELECT count(*) AS n FROM api_item WHERE label = 'capped'")
            .one()
            .await
            .unwrap()
            .get::<i64>("n")
            .unwrap(),
        0,
        "an auto-commit write over the cap must not commit"
    );

    let transaction = lix.begin_transaction().await.unwrap();
    transaction
        .execute("INSERT INTO api_item (id, label) VALUES (500, 'kept')")
        .await
        .unwrap();
    let error = transaction
        .execute("UPDATE api_item SET label = 'capped' RETURNING id, label")
        .max_bytes(256)
        .await
        .unwrap_err();
    assert_eq!(error.code, LixError::CODE_RESULT_TOO_LARGE);
    transaction.commit().await.unwrap();
    assert_eq!(
        count(&lix).await,
        51,
        "the earlier transaction write commits"
    );
    assert_eq!(
        lix.query("SELECT count(*) AS n FROM api_item WHERE label = 'capped'")
            .one()
            .await
            .unwrap()
            .get::<i64>("n")
            .unwrap(),
        0,
        "the capped statement was rolled back inside the transaction"
    );
    // Within the cap, RETURNING rows come back.
    let returned = lix
        .execute("UPDATE api_item SET label = 'small' WHERE id = 1 RETURNING id")
        .await
        .unwrap();
    assert_eq!(returned.len(), 1);
}

#[tokio::test]
async fn snapshot_and_transaction_reads_are_capped() {
    let lix = seeded(100).await;
    let snapshot = lix.snapshot().await.unwrap();
    assert_eq!(
        snapshot
            .query("SELECT id, label FROM api_item")
            .max_bytes(128)
            .all()
            .await
            .unwrap_err()
            .code,
        LixError::CODE_RESULT_TOO_LARGE
    );
    assert_eq!(
        snapshot
            .query("SELECT id, label FROM api_item")
            .max_bytes(usize::MAX)
            .all()
            .await
            .unwrap()
            .len(),
        100
    );
    let transaction = lix.begin_transaction().await.unwrap();
    assert_eq!(
        transaction
            .query("SELECT id, label FROM api_item")
            .max_bytes(128)
            .all()
            .await
            .unwrap_err()
            .code,
        LixError::CODE_RESULT_TOO_LARGE
    );
    transaction.rollback().await.unwrap();
}

#[tokio::test]
async fn buffered_streams_use_the_query_byte_cap() {
    let lix = seeded(1).await;
    for index in 0..3 {
        lix.execute("INSERT INTO lix_file (path, content) VALUES ($1, $2)")
            .bind(format!("/big-{index}.bin"))
            .bind(vec![7_u8; 10_000])
            .await
            .unwrap();
    }
    let sql = "SELECT path, content FROM lix_file ORDER BY path";
    let error = lix.query(sql).max_bytes(4096).stream().await.unwrap_err();
    assert_eq!(error.code, LixError::CODE_RESULT_TOO_LARGE);
    assert!(error.message.contains("buffered"), "{}", error.message);
    for stream in [
        lix.query(sql).stream().await.unwrap(),
        lix.snapshot()
            .await
            .unwrap()
            .query(sql)
            .max_bytes(usize::MAX)
            .stream()
            .await
            .unwrap(),
    ] {
        let mut stream = stream;
        let mut rows = 0;
        while stream.try_next().await.unwrap().is_some() {
            rows += 1;
        }
        assert!(rows >= 3);
    }
}

#[tokio::test]
async fn execute_many_rejects_every_row_returning_statement_and_validates_binding() {
    let lix = seeded(1).await;
    for sql in [
        "SELECT uuidv7()",
        "SELECT current_timestamp",
        "SELECT commit_id FROM lix_undo()",
        "SELECT $1",
        "INSERT INTO api_item (id, label) VALUES (900, 'x') RETURNING id",
    ] {
        let error = lix
            .execute_many(sql, Vec::<[Value; 0]>::new())
            .await
            .unwrap_err();
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM, "{sql}");
    }
    // Without parameter sets nothing runs, but the statement still binds.
    assert!(
        lix.execute_many("INSERT INTO nope (a) VALUES ($1)", Vec::<[Value; 1]>::new())
            .await
            .is_err()
    );
    assert_eq!(
        lix.execute_many(
            "INSERT INTO api_item (id, label) VALUES ($1, $2)",
            Vec::<[Value; 2]>::new()
        )
        .await
        .unwrap(),
        0
    );
    let transaction = lix.begin_transaction().await.unwrap();
    assert!(
        transaction
            .execute_many("INSERT INTO nope (a) VALUES ($1)", Vec::<[Value; 1]>::new())
            .await
            .is_err()
    );
    assert_eq!(
        transaction
            .execute_many("SELECT uuidv7()", Vec::<[Value; 0]>::new())
            .await
            .unwrap_err()
            .code,
        LixError::CODE_INVALID_PARAM
    );
    transaction.rollback().await.unwrap();
    assert_eq!(count(&lix).await, 1);
}
/// A read over data that is all in memory never waits on I/O, so without
/// cooperative yielding one poll would run the whole aggregate and nothing
/// (a cancel, a timeout, the browser's event loop) could interrupt it.
#[tokio::test]
async fn a_long_read_yields_so_dropping_it_cancels_promptly() {
    let lix = seeded(1).await;
    let started = std::time::Instant::now();
    let read = lix
        .query("SELECT count(*) AS n FROM generate_series(1, 4000000000)")
        .one();
    let outcome = tokio::time::timeout(std::time::Duration::from_millis(200), read).await;
    assert!(outcome.is_err(), "the read must still be running");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "the timeout must fire while the read runs, not after: {:?}",
        started.elapsed()
    );
    // Dropping the read released the session: a write proceeds at once.
    lix.execute("INSERT INTO api_item (id, label) VALUES (700, 'after cancel')")
        .await
        .unwrap();
    assert_eq!(count(&lix).await, 2);
}

#[tokio::test]
async fn a_long_snapshot_read_yields_so_dropping_it_cancels_promptly() {
    let lix = seeded(1).await;
    let snapshot = lix.snapshot().await.unwrap();
    let started = std::time::Instant::now();
    let read = snapshot
        .query("SELECT count(*) AS n FROM generate_series(1, 4000000000)")
        .one();
    let outcome = tokio::time::timeout(std::time::Duration::from_millis(200), read).await;
    assert!(outcome.is_err());
    assert!(started.elapsed() < std::time::Duration::from_secs(2));
    snapshot.close().await.unwrap();
}


/// Outside Tokio's cooperative budget (the browser's executor has none), the
/// engine still yields: no single poll of a long read runs past a few time
/// slices, so cancels and other work interleave with it.
#[tokio::test]
async fn a_long_read_yields_without_an_executor_budget() {
    let lix = seeded(1).await;
    let read = lix
        .query("SELECT count(*) AS n FROM generate_series(1, 4000000000)")
        .one();
    let mut read = std::pin::pin!(tokio::task::coop::unconstrained(read));
    let waker = futures_util::task::noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    let started = std::time::Instant::now();
    let mut longest = std::time::Duration::ZERO;
    while started.elapsed() < std::time::Duration::from_millis(300) {
        let poll_started = std::time::Instant::now();
        let poll = read.as_mut().poll(&mut cx);
        longest = longest.max(poll_started.elapsed());
        assert!(poll.is_pending(), "the read cannot finish this soon");
        // Let other tasks (admission, storage) make progress between polls.
        tokio::task::yield_now().await;
    }
    assert!(
        longest < std::time::Duration::from_millis(150),
        "one poll ran for {longest:?}"
    );
}

/// A failed `execute_many` in a transaction is rolled back as a unit,
/// including the deterministic sequence values its successful sets drew: the
/// next value is the one a transaction without it would get.
#[tokio::test]
async fn a_failed_execute_many_rewinds_the_deterministic_sequence() {
    async fn next_uuid_after(failed_execute_many: bool) -> String {
        let lix = open_lix().await.unwrap();
        lix.execute(
            "INSERT INTO lix_key_value (key, value, lixcol_global, lixcol_untracked) \
             VALUES ('lix_deterministic_mode', CAST('{\"enabled\":true}' AS JSONB), true, true)",
        )
        .await
        .unwrap();
        let transaction = lix.begin_transaction().await.unwrap();
        if failed_execute_many {
            // Each set draws a file id from the sequence; the second
            // repeats the path and fails.
            let error = transaction
                .execute_many("INSERT INTO lix_file (path, content) VALUES ($1, $2)", [
                    [Value::from("/a.txt"), Value::Blob(b"a".to_vec().into())],
                    [Value::from("/a.txt"), Value::Blob(b"b".to_vec().into())],
                ])
                .await
                .unwrap_err();
            assert_ne!(error.code, LixError::CODE_INTERNAL_ERROR);
        }
        let id = transaction
            .query("SELECT uuidv7() AS id")
            .one()
            .await
            .unwrap()
            .get::<String>("id")
            .unwrap();
        transaction.rollback().await.unwrap();
        id
    }
    assert_eq!(next_uuid_after(true).await, next_uuid_after(false).await);
}

/// A transaction read that is cancelled while it flushes staged writes
/// (after it took them from the journal) must not let the transaction commit
/// without those writes.
#[tokio::test]
async fn a_read_cancelled_mid_flush_poisons_its_transaction() {
    let lix = open_lix().await.unwrap();
    lix.execute(
        "INSERT INTO lix_registered_schema (value) VALUES (CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\
         \"key\":\"flush_probe\",\"columns\":[{\"name\":\"path\",\"type\":\"text\",\"nullable\":false},\
         {\"name\":\"value\",\"type\":\"jsonb\",\"nullable\":false}],\"primary_key\":[\"path\"]}' AS JSONB))",
    )
    .await
    .unwrap();
    lix.execute("INSERT INTO flush_probe (path, value) VALUES ('/a', '1'::jsonb), ('/b', '1'::jsonb)")
        .await
        .unwrap();
    let transaction = lix.begin_transaction().await.unwrap();
    // Exact-key path-value updates are staged through the mutation journal.
    for path in ["/a", "/b"] {
        transaction
            .execute("UPDATE flush_probe SET value = CAST($1 AS JSONB) WHERE path = $2")
            .params([Value::Integer(2), Value::from(path)])
            .await
            .unwrap();
    }
    let release = std::sync::Arc::new(tokio::sync::Notify::new());
    let reached = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    *crate::transaction::FLUSH_PAUSE.lock().unwrap() = Some((release.clone(), reached.clone()));
    let read = transaction
        .query("SELECT count(*) AS n FROM flush_probe")
        .one();
    let outcome = tokio::time::timeout(std::time::Duration::from_millis(300), read).await;
    *crate::transaction::FLUSH_PAUSE.lock().unwrap() = None;
    release.notify_waiters();
    assert!(outcome.is_err(), "the read must be dropped mid-flush");
    assert!(
        reached.load(std::sync::atomic::Ordering::SeqCst),
        "the read reached the journal flush"
    );
    let error = transaction.commit().await.unwrap_err();
    assert_eq!(error.code, LixError::CODE_INVALID_TRANSACTION_STATE);
    let values = lix
        .query("SELECT CAST(value AS TEXT) AS value FROM flush_probe ORDER BY path")
        .all()
        .await
        .unwrap();
    for row in values.rows() {
        assert_eq!(row.get::<String>("value").unwrap(), "1");
    }
}

/// `SELECT id, id` is ambiguous at planning. A write whose RETURNING repeats
/// a name runs and is read by position, unless the caller keys rows by name
/// (`unique_column_names`, as the JS SDK does): then it is rejected before
/// anything is written.
#[tokio::test]
async fn duplicate_output_columns_are_rejected_before_running_when_keyed_by_name() {
    let lix = seeded(1).await;
    let error = lix
        .query("SELECT id, id FROM api_item")
        .all()
        .await
        .unwrap_err();
    assert_eq!(error.code, LixError::CODE_AMBIGUOUS_COLUMN);
    let error = lix
        .execute("INSERT INTO api_item (id, label) VALUES (9, 'nine') RETURNING id, id")
        .unique_column_names()
        .await
        .unwrap_err();
    assert_eq!(error.code, LixError::CODE_AMBIGUOUS_COLUMN);
    let transaction = lix.begin_transaction().await.unwrap();
    let error = transaction
        .execute("UPDATE api_item SET label = 'x' RETURNING OLD.label, NEW.label")
        .unique_column_names()
        .await
        .unwrap_err();
    assert_eq!(error.code, LixError::CODE_AMBIGUOUS_COLUMN);
    transaction.commit().await.unwrap();
    assert_eq!(count(&lix).await, 1, "the insert did not run");
    assert_eq!(
        lix.query("SELECT label FROM api_item")
            .one()
            .await
            .unwrap()
            .get::<String>("label")
            .unwrap(),
        "label-0",
        "the update did not run"
    );
    // Without the option the write runs and its rows read by position.
    let result = lix
        .execute("UPDATE api_item SET label = 'y' RETURNING OLD.label, NEW.label")
        .await
        .unwrap();
    assert_eq!(result.rows_affected(), 1);
}

#[tokio::test]
async fn a_recursive_cte_streams_like_it_reads() {
    let lix = seeded(0).await;
    let sql = "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 5) \
               SELECT n FROM t ORDER BY n";
    let buffered = lix.query(sql).all().await.unwrap();
    let mut stream = lix.query(sql).stream().await.unwrap();
    let mut streamed = Vec::new();
    while let Some(page) = stream.next_page().await.unwrap() {
        streamed.extend(page.into_iter().map(|row| row.get::<i64>("n").unwrap()));
    }
    let expected = buffered
        .rows()
        .iter()
        .map(|row| row.get::<i64>("n").unwrap())
        .collect::<Vec<_>>();
    assert_eq!(expected, [1, 2, 3, 4, 5]);
    assert_eq!(streamed, expected);
}
