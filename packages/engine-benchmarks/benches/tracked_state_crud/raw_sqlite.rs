use rusqlite::{Connection, Rows, params, params_from_iter};
use tempfile::TempDir;

use crate::workload::WorkloadRow;
use lix_engine::{ExecuteResult, Value};

pub(crate) struct RawSqliteFixture {
    connection: Connection,
    rows: Vec<WorkloadRow>,
    read_many_by_pk_count: usize,
    read_many_by_pk_sql: String,
    _dir: TempDir,
}

pub(crate) fn empty_fixture(rows: &[WorkloadRow]) -> RawSqliteFixture {
    let dir = tempfile::tempdir().expect("create raw sqlite benchmark directory");
    let path = dir.path().join("raw.sqlite");
    let connection = Connection::open(path).expect("open raw sqlite benchmark database");
    connection.set_prepared_statement_cache_capacity(32);
    connection
        .execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA temp_store = MEMORY;
             PRAGMA cache_size = -20000;
             PRAGMA mmap_size = 268435456;
             PRAGMA wal_autocheckpoint = 10000;
             CREATE TABLE json_pointer (
                 path TEXT PRIMARY KEY NOT NULL,
                 value TEXT NOT NULL
             ) WITHOUT ROWID;",
        )
        .expect("initialize raw sqlite benchmark database");
    let read_many_by_pk_count = rows.len().min(crate::READ_MANY_PK_COUNT);
    RawSqliteFixture {
        connection,
        rows: rows.to_vec(),
        read_many_by_pk_count,
        read_many_by_pk_sql: select_many_by_pk_sql(read_many_by_pk_count),
        _dir: dir,
    }
}

pub(crate) fn seeded_fixture(rows: &[WorkloadRow]) -> RawSqliteFixture {
    let mut fixture = empty_fixture(rows);
    fixture.insert_all();
    fixture
}

impl RawSqliteFixture {
    pub(crate) fn insert_all(&mut self) -> usize {
        let transaction = self
            .connection
            .transaction()
            .expect("begin raw sqlite insert transaction");
        let mut affected = 0;
        {
            let mut statement = transaction
                .prepare_cached("INSERT INTO json_pointer (path, value) VALUES (?1, ?2)")
                .expect("prepare raw sqlite insert");
            for row in &self.rows {
                affected += statement
                    .execute(params![row.path, row.value_json])
                    .expect("insert raw sqlite row");
            }
        }
        transaction
            .commit()
            .expect("commit raw sqlite insert transaction");
        assert_eq!(affected, self.rows.len());
        affected
    }

    pub(crate) fn read_all(&self) -> usize {
        let mut statement = self
            .connection
            .prepare_cached("SELECT path, value FROM json_pointer ORDER BY path")
            .expect("prepare raw sqlite read all");
        let mut query = statement.query([]).expect("query all raw sqlite rows");
        let mut count = 0;
        while let Some(row) = query.next().expect("read next raw sqlite row") {
            let _: &str = row
                .get_ref(0)
                .expect("read raw sqlite path")
                .as_str()
                .expect("raw sqlite path must be text");
            let _: &str = row
                .get_ref(1)
                .expect("read raw sqlite value")
                .as_str()
                .expect("raw sqlite value must be text");
            count += 1;
        }
        assert_eq!(count, self.rows.len());
        count
    }

    /// Reads through standalone SQLite, then constructs the same public
    /// `ExecuteResult` value shape that the Lix SQL session returns.
    ///
    /// Keep this separate from [`Self::read_all`]. The latter intentionally
    /// borrows SQLite text values, which is the storage-engine lower bound;
    /// this control attributes the unavoidable owned `Value`/JSON-DOM result
    /// construction independently from the Lix SQL and storage layers.
    pub(crate) fn read_all_public_result(&self) -> ExecuteResult {
        let mut statement = self
            .connection
            .prepare_cached("SELECT path, value FROM json_pointer ORDER BY path")
            .expect("prepare raw sqlite public-result read all");
        let query = statement
            .query([])
            .expect("query all raw sqlite public-result rows");
        Self::public_result_from_query(query, self.rows.len())
    }

    pub(crate) fn read_one_by_pk(&self) -> usize {
        let row = &self.rows[self.rows.len() / 2];
        let mut statement = self
            .connection
            .prepare_cached("SELECT path, value FROM json_pointer WHERE path = ?1")
            .expect("prepare raw sqlite point read");
        let mut query = statement
            .query(params![row.path])
            .expect("query raw sqlite point row");
        let result = query
            .next()
            .expect("read raw sqlite point row")
            .expect("raw sqlite point row must exist");
        let _: &str = result
            .get_ref(0)
            .expect("read raw sqlite point path")
            .as_str()
            .expect("raw sqlite point path must be text");
        let _: &str = result
            .get_ref(1)
            .expect("read raw sqlite point value")
            .as_str()
            .expect("raw sqlite point value must be text");
        assert!(
            query
                .next()
                .expect("finish raw sqlite point query")
                .is_none()
        );
        1
    }

    pub(crate) fn read_one_by_pk_public_result(&self) -> ExecuteResult {
        let row = &self.rows[self.rows.len() / 2];
        let mut statement = self
            .connection
            .prepare_cached("SELECT path, value FROM json_pointer WHERE path = ?1")
            .expect("prepare raw sqlite public-result point read");
        let query = statement
            .query(params![row.path])
            .expect("query raw sqlite public-result point row");
        Self::public_result_from_query(query, 1)
    }

    pub(crate) fn read_many_by_pk(&self, count: usize) -> usize {
        let count = count.min(self.rows.len());
        assert_eq!(
            count, self.read_many_by_pk_count,
            "read-many benchmark must use the fixture's setup-excluded query shape"
        );
        let mut statement = self
            .connection
            .prepare_cached(&self.read_many_by_pk_sql)
            .expect("prepare raw sqlite multi-point read");
        let mut query = statement
            .query(params_from_iter(
                self.rows[..count].iter().map(|row| row.path.as_str()),
            ))
            .expect("query raw sqlite multi-point rows");
        let mut found = 0;
        while let Some(result) = query.next().expect("read next raw sqlite multi-point row") {
            let _: &str = result
                .get_ref(0)
                .expect("read raw sqlite multi-point path")
                .as_str()
                .expect("raw sqlite multi-point path must be text");
            let _: &str = result
                .get_ref(1)
                .expect("read raw sqlite multi-point value")
                .as_str()
                .expect("raw sqlite multi-point value must be text");
            found += 1;
        }
        assert_eq!(found, count);
        found
    }

    pub(crate) fn read_many_by_pk_public_result(&self, count: usize) -> ExecuteResult {
        let count = count.min(self.rows.len());
        assert_eq!(
            count, self.read_many_by_pk_count,
            "read-many benchmark must use the fixture's setup-excluded query shape"
        );
        let mut statement = self
            .connection
            .prepare_cached(&self.read_many_by_pk_sql)
            .expect("prepare raw sqlite public-result multi-point read");
        let query = statement
            .query(params_from_iter(
                self.rows[..count].iter().map(|row| row.path.as_str()),
            ))
            .expect("query raw sqlite public-result multi-point rows");
        Self::public_result_from_query(query, count)
    }

    pub(crate) fn update_all(&mut self) -> usize {
        let transaction = self
            .connection
            .transaction()
            .expect("begin raw sqlite update transaction");
        let mut affected = 0;
        {
            let mut statement = transaction
                .prepare_cached("UPDATE json_pointer SET value = ?1 WHERE path = ?2")
                .expect("prepare raw sqlite update");
            for row in &self.rows {
                affected += statement
                    .execute(params![row.updated_value_json, row.path])
                    .expect("update raw sqlite row");
            }
        }
        transaction
            .commit()
            .expect("commit raw sqlite update transaction");
        assert_eq!(affected, self.rows.len());
        affected
    }

    pub(crate) fn update_one_by_pk(&self) -> usize {
        let row = &self.rows[self.rows.len() / 2];
        let affected = self
            .connection
            .execute(
                "UPDATE json_pointer SET value = ?1 WHERE path = ?2",
                params![row.updated_value_json, row.path],
            )
            .expect("update one raw sqlite row");
        assert_eq!(affected, 1);
        affected
    }

    pub(crate) fn delete_all(&self) -> usize {
        let affected = self
            .connection
            .execute("DELETE FROM json_pointer", [])
            .expect("delete all raw sqlite rows");
        assert_eq!(affected, self.rows.len());
        affected
    }

    pub(crate) fn delete_one_by_pk(&self) -> usize {
        let row = &self.rows[self.rows.len() / 2];
        let affected = self
            .connection
            .execute(
                "DELETE FROM json_pointer WHERE path = ?1",
                params![row.path],
            )
            .expect("delete one raw sqlite row");
        assert_eq!(affected, 1);
        affected
    }

    fn public_result_from_query(mut query: Rows<'_>, expected_rows: usize) -> ExecuteResult {
        let mut rows = Vec::with_capacity(expected_rows);
        while let Some(row) = query
            .next()
            .expect("read next raw sqlite public-result row")
        {
            // `String` ownership and the JSON parse intentionally mirror
            // `query_result_from_batches` -> `string_scalar_to_lix_value`.
            let path = row
                .get::<_, String>(0)
                .expect("read raw sqlite public-result path");
            let value_json = row
                .get::<_, String>(1)
                .expect("read raw sqlite public-result value");
            let value =
                serde_json::from_str(&value_json).expect("raw sqlite fixture JSON must be valid");
            // Entity projection represents JSON `null` as an Arrow null, so
            // the public Lix result is `Value::Null` rather than
            // `Value::Json(JsonValue::Null)`.
            let value = match value {
                serde_json::Value::Null => Value::Null,
                value => Value::Json(value),
            };
            rows.push(vec![Value::Text(path), value]);
        }
        assert_eq!(rows.len(), expected_rows);
        ExecuteResult::from_rows(vec!["path".to_string(), "value".to_string()], rows)
    }
}

fn select_many_by_pk_sql(count: usize) -> String {
    assert!(count > 0, "read-many benchmark requires at least one row");
    let placeholders = (1..=count)
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(",");
    format!("SELECT path, value FROM json_pointer WHERE path IN ({placeholders}) ORDER BY path")
}
