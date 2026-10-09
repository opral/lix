//! Behavior of `Lix::query_stream` against the buffered `execute` path.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::common::public_row_bytes;
use crate::storage::{
    BeginScanOptions, GetManyRequest, GetManyResult, KeyRange, MemoryRead, MemoryWrite,
    ReadOptions, ScanCursor, Storage, StorageError, StorageRead, StorageSessionToken, StorageSpace,
    WriteOptions,
};
use crate::{ExecuteResult, Lix, LixError, Memory, QueryStream, Value, open_lix};

/// Memory storage that counts live read views, so tests can prove a stream
/// releases its pinned snapshot.
#[derive(Clone, Debug, Default)]
struct CountingStorage {
    inner: Memory,
    live_reads: Arc<AtomicUsize>,
    opened_reads: Arc<AtomicUsize>,
    /// Point and scan requests fail with `ReadExpired` while this is nonzero;
    /// each failure consumes one.
    expiring_requests: Arc<AtomicUsize>,
}

struct CountingRead {
    inner: MemoryRead,
    live_reads: Arc<AtomicUsize>,
    expiring_requests: Arc<AtomicUsize>,
}

impl CountingRead {
    fn expire(&self) -> Result<(), StorageError> {
        let expired = self
            .expiring_requests
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok();
        if expired {
            return Err(StorageError::ReadExpired);
        }
        Ok(())
    }
}

impl Drop for CountingRead {
    fn drop(&mut self) {
        self.live_reads.fetch_sub(1, Ordering::SeqCst);
    }
}

impl StorageRead for CountingRead {
    fn snapshot_cache_key(&self) -> Option<u128> {
        self.inner.snapshot_cache_key()
    }

    async fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> Result<GetManyResult, StorageError> {
        self.expire()?;
        self.inner.get_many(requests).await
    }

    async fn get_many_bounded(
        &self,
        requests: &[GetManyRequest<'_>],
        budget: crate::storage::ReadBudget,
    ) -> Result<GetManyResult, StorageError> {
        self.inner.get_many_bounded(requests, budget).await
    }

    async fn get_many_bounded_prefix(
        &self,
        requests: &[GetManyRequest<'_>],
        offset: usize,
        max_slots: usize,
        budget: crate::storage::ReadBudget,
    ) -> Result<crate::storage::GetManyPrefixResult, StorageError> {
        self.inner
            .get_many_bounded_prefix(requests, offset, max_slots, budget)
            .await
    }

    async fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> Result<ScanCursor<'_>, StorageError> {
        self.expire()?;
        self.inner.begin_scan(space, range, opts).await
    }
}

impl Storage for CountingStorage {
    type Read<'a>
        = CountingRead
    where
        Self: 'a;
    type Write<'a>
        = MemoryWrite
    where
        Self: 'a;

    async fn acquire_session(&self) -> Result<StorageSessionToken, StorageError> {
        self.inner.acquire_session().await
    }

    async fn acquire_partial_replica_owner(
        &self,
        token: StorageSessionToken,
    ) -> Result<crate::storage::StorageOwnerLease, StorageError> {
        self.inner.acquire_partial_replica_owner(token).await
    }

    async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        let inner = self.inner.begin_read(opts).await?;
        self.live_reads.fetch_add(1, Ordering::SeqCst);
        self.opened_reads.fetch_add(1, Ordering::SeqCst);
        Ok(CountingRead {
            inner,
            live_reads: Arc::clone(&self.live_reads),
            expiring_requests: Arc::clone(&self.expiring_requests),
        })
    }

    async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.inner.begin_write(opts).await
    }
}

async fn register_schema(
    lix: &Lix<impl Storage + Clone + Send + Sync + 'static>,
    key: &str,
    columns: &str,
) {
    lix.execute(
        &format!(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST('{{\"$schema\":\"https://lix.dev/schema-v1.json\",\
             \"key\":\"{key}\",\"columns\":[{columns}],\"primary_key\":[\"id\"]}}' AS JSONB))"
        ),
        &[],
    )
    .await
    .unwrap_or_else(|error| panic!("register {key}: {error:?}"));
}

const ITEMS: usize = 1_500;
const TAGS: usize = 900;

/// `item(id, grp, label, payload)` and `tag(id, item_id, name)`.
async fn seeded<S>(storage: S) -> Lix<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let lix = open_lix().with_storage(storage).await.expect("open lix");
    register_schema(
        &lix,
        "stream_item",
        "{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},\
         {\"name\":\"grp\",\"type\":\"int8\",\"nullable\":false},\
         {\"name\":\"label\",\"type\":\"text\",\"nullable\":true},\
         {\"name\":\"payload\",\"type\":\"jsonb\",\"nullable\":false}",
    )
    .await;
    register_schema(
        &lix,
        "stream_tag",
        "{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},\
         {\"name\":\"item_id\",\"type\":\"text\",\"nullable\":false},\
         {\"name\":\"name\",\"type\":\"text\",\"nullable\":false}",
    )
    .await;
    insert_items(&lix, 0..ITEMS).await;
    let tags = (0..TAGS)
        .map(|index| {
            vec![
                Value::Text(format!("tag-{index:05}")),
                Value::Text(format!("item-{:05}", (index * 7) % ITEMS)),
                Value::Text(format!("name-{}", index % 13)),
            ]
        })
        .collect::<Vec<_>>();
    insert_rows(&lix, "stream_tag", &["id", "item_id", "name"], tags).await;
    lix
}

async fn insert_items<S>(lix: &Lix<S>, range: std::ops::Range<usize>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let rows = range
        .map(|index| {
            vec![
                Value::Text(format!("item-{index:05}")),
                Value::Integer((index % 17) as i64),
                if index % 11 == 0 {
                    Value::Null
                } else {
                    Value::Text(format!("label {index} {}", "x".repeat(index % 40)))
                },
                Value::Jsonb(
                    crate::Json::parse(&format!("{{\"n\":{index},\"tags\":[\"a\",\"b\"]}}"))
                        .unwrap(),
                ),
            ]
        })
        .collect::<Vec<_>>();
    insert_rows(lix, "stream_item", &["id", "grp", "label", "payload"], rows).await;
}

async fn insert_rows<S>(lix: &Lix<S>, table: &str, columns: &[&str], rows: Vec<Vec<Value>>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    for chunk in rows.chunks(250) {
        let mut sql = format!("INSERT INTO {table} ({}) VALUES ", columns.join(", "));
        let mut parameter = 1;
        for (row_index, _) in chunk.iter().enumerate() {
            if row_index > 0 {
                sql.push_str(", ");
            }
            let placeholders = (0..columns.len())
                .map(|_| {
                    let placeholder = format!("${parameter}");
                    parameter += 1;
                    placeholder
                })
                .collect::<Vec<_>>();
            sql.push_str(&format!("({})", placeholders.join(", ")));
        }
        let params = chunk.iter().flatten().cloned().collect::<Vec<_>>();
        lix.execute(&sql, &params)
            .await
            .unwrap_or_else(|error| panic!("insert into {table}: {error:?}"));
    }
}

async fn drain(stream: &mut QueryStream) -> Result<Vec<ExecuteResult>, LixError> {
    let mut pages = Vec::new();
    while let Some(page) = stream.next_page().await? {
        pages.push(page);
    }
    Ok(pages)
}

fn page_rows(pages: &[ExecuteResult]) -> Vec<Vec<Value>> {
    pages
        .iter()
        .flat_map(|page| page.rows().iter().map(|row| row.values().to_vec()))
        .collect()
}

async fn with_timeout<T>(label: &str, future: impl IntoFuture<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(30), future.into_future())
        .await
        .unwrap_or_else(|_| panic!("{label} did not finish within 30s"))
}

#[tokio::test]
async fn streamed_pages_match_buffered_execute_across_query_shapes() {
    let lix = seeded(Memory::new()).await;
    let shapes: &[(&str, Vec<Value>)] = &[
        (
            "SELECT id, grp, label, payload FROM stream_item ORDER BY id",
            vec![],
        ),
        ("SELECT label, id FROM stream_item", vec![]),
        (
            "SELECT id, grp FROM stream_item WHERE grp = $1 AND label IS NOT NULL",
            vec![Value::Integer(3)],
        ),
        (
            "SELECT id FROM stream_item WHERE id = $1",
            vec![Value::Text("item-00042".into())],
        ),
        (
            "SELECT id, payload ->> 'n' AS n FROM stream_item ORDER BY grp DESC, id",
            vec![],
        ),
        (
            "SELECT i.id, i.grp, t.name FROM stream_item i JOIN stream_tag t ON t.item_id = i.id ORDER BY t.id",
            vec![],
        ),
        (
            "SELECT i.id, t.id AS tag_id FROM stream_item i LEFT JOIN stream_tag t ON t.item_id = i.id ORDER BY i.id, t.id",
            vec![],
        ),
        (
            "SELECT grp, COUNT(*) AS n, MAX(id) AS last FROM stream_item GROUP BY grp ORDER BY grp",
            vec![],
        ),
        ("SELECT COUNT(*) FROM stream_item", vec![]),
        (
            "SELECT id FROM stream_item ORDER BY id DESC LIMIT 37",
            vec![],
        ),
        (
            "SELECT id FROM stream_item ORDER BY id LIMIT 25 OFFSET 1000",
            vec![],
        ),
        ("SELECT DISTINCT name FROM stream_tag ORDER BY name", vec![]),
        (
            "SELECT id FROM stream_item WHERE grp < 2 UNION ALL SELECT id FROM stream_tag WHERE name = 'name-1' ORDER BY id",
            vec![],
        ),
        (
            "WITH busy AS (SELECT item_id FROM stream_tag GROUP BY item_id HAVING COUNT(*) > 0) \
             SELECT id FROM stream_item WHERE id IN (SELECT item_id FROM busy) ORDER BY id",
            vec![],
        ),
        ("SELECT id FROM stream_item WHERE grp = 999", vec![]),
    ];
    for (sql, params) in shapes {
        let buffered = lix.execute(sql, params).await.expect(sql);
        let mut stream = lix
            .query_stream(sql, params)
            .with_page_bytes(4 * 1024)
            .await
            .expect(sql);
        assert_eq!(stream.columns(), buffered.columns(), "{sql}");
        assert_eq!(stream.column_types(), buffered.column_types(), "{sql}");
        let pages = drain(&mut stream).await.expect(sql);
        assert!(pages.iter().all(|page| !page.is_empty()), "{sql}");
        assert!(
            pages
                .iter()
                .all(|page| page.columns() == buffered.columns()),
            "{sql}"
        );
        let mut streamed = page_rows(&pages);
        let mut expected = buffered
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>();
        if !sql.contains("ORDER BY") {
            let key = |row: &Vec<Value>| format!("{row:?}");
            streamed.sort_by_key(key);
            expected.sort_by_key(key);
        }
        assert_eq!(streamed, expected, "{sql}");
        if expected.len() == ITEMS {
            assert!(pages.len() > 10, "{sql} should span many pages");
        }
    }
}

#[tokio::test]
async fn pages_stay_within_the_page_byte_bound() {
    let lix = seeded(Memory::new()).await;
    for page_bytes in [1, 700, 16 * 1024] {
        let mut stream = lix
            .query_stream("SELECT id, grp, label FROM stream_item ORDER BY id", &[])
            .with_page_bytes(page_bytes)
            .await
            .unwrap();
        let pages = drain(&mut stream).await.unwrap();
        assert_eq!(page_rows(&pages).len(), ITEMS);
        for page in &pages {
            let bytes = page
                .rows()
                .iter()
                .map(|row| public_row_bytes(row.values()))
                .sum::<usize>();
            assert!(
                bytes <= page_bytes || page.len() == 1,
                "{page_bytes}-byte page held {} rows / {bytes} bytes",
                page.len()
            );
        }
        if page_bytes == 1 {
            assert_eq!(pages.len(), ITEMS, "every row is its own page");
        }
    }
    for page_bytes in [0, crate::MAX_QUERY_STREAM_PAGE_BYTES + 1] {
        let error = lix
            .query_stream("SELECT id FROM stream_item", &[])
            .with_page_bytes(page_bytes)
            .await
            .unwrap_err();
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
    }
}

#[tokio::test]
async fn cancel_drop_and_exhaustion_release_the_pinned_snapshot() {
    let storage = CountingStorage::default();
    let lix = seeded(storage.clone()).await;
    let idle = storage.live_reads.load(Ordering::SeqCst);
    let sql = "SELECT id, label FROM stream_item ORDER BY id";

    let mut stream = lix
        .query_stream(sql, &[])
        .with_page_bytes(512)
        .await
        .unwrap();
    assert!(stream.next_page().await.unwrap().is_some());
    assert_eq!(storage.live_reads.load(Ordering::SeqCst), idle + 1);
    assert_eq!(lix.running_query_stream_count_for_test(), 1);
    stream.cancel();
    assert_eq!(storage.live_reads.load(Ordering::SeqCst), idle);
    assert_eq!(lix.running_query_stream_count_for_test(), 0);
    assert!(stream.next_page().await.unwrap().is_none());

    let mut stream = lix
        .query_stream(sql, &[])
        .with_page_bytes(512)
        .await
        .unwrap();
    assert!(stream.next_page().await.unwrap().is_some());
    assert_eq!(storage.live_reads.load(Ordering::SeqCst), idle + 1);
    drop(stream);
    assert_eq!(storage.live_reads.load(Ordering::SeqCst), idle);

    // Opening alone pins the snapshot; dropping before the first page frees it.
    let stream = lix.query_stream(sql, &[]).await.unwrap();
    assert_eq!(storage.live_reads.load(Ordering::SeqCst), idle + 1);
    drop(stream);
    assert_eq!(storage.live_reads.load(Ordering::SeqCst), idle);

    let mut stream = lix
        .query_stream(sql, &[])
        .with_page_bytes(512)
        .await
        .unwrap();
    drain(&mut stream).await.unwrap();
    assert_eq!(storage.live_reads.load(Ordering::SeqCst), idle);
    assert_eq!(lix.running_query_stream_count_for_test(), 0);
}

#[tokio::test]
async fn idle_stream_does_not_block_writes_and_keeps_its_snapshot() {
    let lix = seeded(Memory::new()).await;
    let other = lix.open_another_session().await.unwrap();
    let mut stream = lix
        .query_stream("SELECT id, grp, label FROM stream_item ORDER BY id", &[])
        .with_page_bytes(1024)
        .await
        .unwrap();
    let first = stream.next_page().await.unwrap().expect("first page");
    let snapshot = lix
        .execute("SELECT id, grp, label FROM stream_item ORDER BY id", &[])
        .await
        .unwrap();

    // Same handle and an independent session both commit while the stream
    // is idle between pulls.
    with_timeout("same-handle insert", insert_items(&lix, ITEMS..ITEMS + 300)).await;
    with_timeout(
        "same-handle update",
        lix.execute(
            "UPDATE stream_item SET label = 'changed' WHERE grp = 1",
            &[],
        ),
    )
    .await
    .unwrap();
    with_timeout(
        "other-session delete",
        other.execute("DELETE FROM stream_item WHERE grp = 2", &[]),
    )
    .await
    .unwrap();

    let mut rows = page_rows(std::slice::from_ref(&first));
    rows.extend(page_rows(&drain(&mut stream).await.unwrap()));
    let expected = snapshot
        .rows()
        .iter()
        .map(|row| row.values().to_vec())
        .collect::<Vec<_>>();
    assert_eq!(rows, expected, "the stream reads only its opening snapshot");

    let now = lix
        .execute("SELECT COUNT(*) FROM stream_item", &[])
        .await
        .unwrap();
    assert_ne!(now.rows()[0].values(), &[Value::Integer(ITEMS as i64)]);
    other.close().await.unwrap();
}

#[tokio::test]
async fn closing_the_handle_cancels_open_streams() {
    let storage = CountingStorage::default();
    let lix = seeded(storage.clone()).await;
    let idle = storage.live_reads.load(Ordering::SeqCst);
    let mut started = lix
        .query_stream("SELECT id FROM stream_item ORDER BY id", &[])
        .with_page_bytes(256)
        .await
        .unwrap();
    assert!(started.next_page().await.unwrap().is_some());
    let mut unstarted = lix
        .query_stream("SELECT id FROM stream_tag", &[])
        .await
        .unwrap();
    assert_eq!(storage.live_reads.load(Ordering::SeqCst), idle + 2);

    with_timeout("close", lix.close()).await.unwrap();
    assert_eq!(storage.live_reads.load(Ordering::SeqCst), idle);
    for stream in [&mut started, &mut unstarted] {
        let error = stream.next_page().await.unwrap_err();
        assert_eq!(error.code, LixError::CODE_CLOSED);
        assert!(stream.next_page().await.unwrap().is_none());
    }
    let error = lix
        .query_stream("SELECT id FROM stream_item", &[])
        .await
        .unwrap_err();
    assert_eq!(error.code, LixError::CODE_CLOSED);
}

#[tokio::test]
async fn streams_are_read_only() {
    let lix = seeded(Memory::new()).await;
    for sql in [
        "INSERT INTO stream_tag (id, item_id, name) VALUES ('x', 'item-00001', 'x')",
        "UPDATE stream_item SET grp = 0",
        "DELETE FROM stream_tag",
        "SELECT uuidv7() AS id",
        "SELECT current_timestamp() AS now",
    ] {
        let error = lix.query_stream(sql, &[]).await.unwrap_err();
        assert_eq!(error.code, LixError::CODE_READ_ONLY, "{sql}");
    }
    let count = lix
        .execute("SELECT COUNT(*) FROM stream_tag", &[])
        .await
        .unwrap();
    assert_eq!(count.rows()[0].values(), &[Value::Integer(TAGS as i64)]);
    let error = lix
        .query_stream("SELECT nope FROM stream_item", &[])
        .await
        .unwrap_err();
    assert_eq!(error.code, LixError::CODE_COLUMN_NOT_FOUND);
}

#[tokio::test]
async fn file_content_reads_are_paged_from_a_buffered_result() {
    let lix = open_lix().with_storage(Memory::new()).await.unwrap();
    for index in 0..12 {
        lix.execute(
            "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
            &[
                Value::Text(format!("/docs/file-{index:02}.txt")),
                Value::Blob(vec![b'a' + index as u8; 3_000].into()),
            ],
        )
        .await
        .unwrap();
    }
    for (sql, params) in [
        ("SELECT path, content FROM lix_file ORDER BY path", vec![]),
        (
            "SELECT path, content FROM lix_file WHERE path = $1",
            vec![Value::Text("/docs/file-03.txt".into())],
        ),
        ("SELECT path FROM lix_file ORDER BY path", vec![]),
    ] {
        let buffered = lix.execute(sql, &params).await.unwrap();
        let mut stream = lix
            .query_stream(sql, &params)
            .with_page_bytes(4_000)
            .await
            .unwrap();
        assert_eq!(stream.columns(), buffered.columns());
        let pages = drain(&mut stream).await.unwrap();
        let expected = buffered
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>();
        assert_eq!(page_rows(&pages), expected, "{sql}");
        if sql.contains("content") && expected.len() > 1 {
            assert_eq!(pages.len(), expected.len(), "one 3 KB file per 4 KB page");
        }
    }
}

#[tokio::test]
async fn streams_exceed_the_buffered_read_budget() {
    let lix = seeded(Memory::new()).await;
    let rows = crate::common::MAX_READ_RESULT_ROWS + 1_000;
    let sql = format!("SELECT value FROM generate_series(1, {rows})");
    let error = lix.execute(&sql, &[]).await.unwrap_err();
    assert_eq!(error.code, "LIX_READ_RESOURCE_EXHAUSTED");
    let mut stream = lix.query_stream(&sql, &[]).await.unwrap();
    let mut seen = 0usize;
    let mut pages = 0usize;
    let mut last = 0i64;
    while let Some(page) = stream.next_page().await.unwrap() {
        pages += 1;
        for row in page.rows() {
            let Value::Integer(value) = row.values()[0] else {
                panic!("unexpected value {:?}", row.values());
            };
            assert_eq!(value, last + 1);
            last = value;
            seen += 1;
        }
    }
    assert_eq!(seen, rows);
    assert!(pages > 1);

    // 70 rows of ~1 MiB text exceed the 64 MiB byte budget.
    let big = "SELECT id, repeat('x', 1048576) AS blob FROM stream_item ORDER BY id LIMIT 70";
    let error = lix.execute(big, &[]).await.unwrap_err();
    assert_eq!(error.code, "LIX_READ_RESOURCE_EXHAUSTED");
    let mut stream = lix.query_stream(big, &[]).await.unwrap();
    let pages = drain(&mut stream).await.unwrap();
    let rows = page_rows(&pages);
    assert_eq!(rows.len(), 70);
    assert!(pages.len() >= 70, "each 1 MiB row needs its own 1 MiB page");
}

#[tokio::test]
async fn expiry_before_the_first_page_retries_and_after_it_surfaces() {
    let storage = CountingStorage::default();
    let lix = seeded(storage.clone()).await;
    let sql = "SELECT id FROM stream_item UNION ALL SELECT id FROM stream_tag";
    let buffered = lix.execute(sql, &[]).await.unwrap();

    // The first attempt expires before any page exists: the stream restarts
    // from a fresh snapshot and still returns the complete result.
    storage.expiring_requests.store(1, Ordering::SeqCst);
    let opened = storage.opened_reads.load(Ordering::SeqCst);
    let mut stream = lix
        .query_stream(sql, &[])
        .with_page_bytes(2048)
        .await
        .unwrap();
    let pages = drain(&mut stream).await.unwrap();
    assert_eq!(storage.expiring_requests.load(Ordering::SeqCst), 0);
    assert!(
        storage.opened_reads.load(Ordering::SeqCst) >= opened + 2,
        "retried on a new read"
    );
    let mut streamed = page_rows(&pages);
    let mut expected = buffered
        .rows()
        .iter()
        .map(|row| row.values().to_vec())
        .collect::<Vec<_>>();
    streamed.sort_by_key(|row| format!("{row:?}"));
    expected.sort_by_key(|row| format!("{row:?}"));
    assert_eq!(streamed, expected);

    // Once a page was handed out, expiry ends the stream instead of silently
    // restarting it (which could duplicate rows the caller already holds).
    let mut stream = lix
        .query_stream(sql, &[])
        .with_page_bytes(256)
        .await
        .unwrap();
    assert!(stream.next_page().await.unwrap().is_some());
    let opened = storage.opened_reads.load(Ordering::SeqCst);
    storage
        .expiring_requests
        .store(usize::MAX, Ordering::SeqCst);
    let error = loop {
        match stream.next_page().await {
            Ok(Some(_)) => {}
            Ok(None) => panic!("the second table must be read after the first page"),
            Err(error) => break error,
        }
    };
    storage.expiring_requests.store(0, Ordering::SeqCst);
    assert_eq!(error.code, LixError::CODE_STORAGE_READ_EXPIRED);
    assert_eq!(
        storage.opened_reads.load(Ordering::SeqCst),
        opened,
        "no retry after a page"
    );
    assert!(stream.next_page().await.unwrap().is_none());
    assert_eq!(lix.running_query_stream_count_for_test(), 0);
}

#[tokio::test]
async fn only_statements_that_need_the_whole_result_are_paged_from_a_buffer() {
    let lix = seeded(Memory::new()).await;
    for (sql, materialized) in [
        ("SELECT id, label FROM stream_item", false),
        ("SELECT id FROM stream_item WHERE id = 'item-00001'", false),
        ("SELECT path FROM lix_file", false),
        ("SELECT path, content FROM lix_file", true),
        ("SELECT content FROM lix_file WHERE path = '/a.txt'", true),
    ] {
        let route = lix.query_stream_route_for_test(sql, &[]).unwrap();
        assert_eq!(
            route == crate::session::QueryStreamRoute::Materialized,
            materialized,
            "{sql}"
        );
    }
}
