//! The query surface: buffered reads, single-row reads, streams, snapshots
//! and transaction reads.

use futures_util::StreamExt;

use crate::{Lix, LixError, Value, open_lix};

async fn seeded(rows: usize) -> Lix {
    let lix = open_lix().await.expect("open lix");
    lix.execute(
        "INSERT INTO lix_registered_schema (value) VALUES (CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\
         \"key\":\"api_item\",\"columns\":[{\"name\":\"id\",\"type\":\"int8\",\"nullable\":false},\
         {\"name\":\"label\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB))",
        &[],
    )
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
        lix.execute(&format!("INSERT INTO api_item (id, label) VALUES {values}"), &[])
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
            .map(|row| row.get("id").cloned())
            .collect::<Vec<_>>(),
        [3, 5, 6, 7, 8, 9].map(|id| Some(Value::Integer(id)))
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
        "SELECT uuidv7() AS id",
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
    lix.execute("DELETE FROM api_item WHERE id < 50", &[])
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
    // The buffered first page is gone with the stream.
    let error = loop {
        match stream.try_next().await {
            Ok(Some(_)) => {}
            Ok(None) => panic!("a closed snapshot's stream must not end normally"),
            Err(error) => break error,
        }
    };
    assert_eq!(error.code, LixError::CODE_SNAPSHOT_CLOSED);
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
        "SELECT uuidv7() AS id",
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
    let mut transaction = lix.begin_transaction().await.unwrap();
    transaction
        .execute("INSERT INTO api_item (id, label) VALUES (10, 'staged')", &[])
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
