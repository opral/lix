use postgres::{Client, NoTls, Statement};

use crate::workload::WorkloadRow;

const URL_ENV: &str = "LIX_TRACKED_STATE_CRUD_POSTGRES_URL";

pub(crate) struct RawPostgresFixture {
    client: Client,
    rows: Vec<WorkloadRow>,
    read_many_by_pk_count: usize,
    insert: Statement,
    read_all_statement: Statement,
    read_one_statement: Statement,
    read_many_statement: Statement,
    update: Statement,
    delete_all_statement: Statement,
    delete_one_statement: Statement,
}

pub(crate) fn configured_url() -> Option<String> {
    std::env::var(URL_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

pub(crate) fn empty_fixture(url: &str, rows: &[WorkloadRow]) -> RawPostgresFixture {
    empty_fixture_with_read_many_pk_count(url, rows, crate::READ_MANY_PK_COUNT)
}

fn empty_fixture_with_read_many_pk_count(
    url: &str,
    rows: &[WorkloadRow],
    read_many_by_pk_count: usize,
) -> RawPostgresFixture {
    assert!(
        (1..=rows.len()).contains(&read_many_by_pk_count),
        "read-many primary-key count must be between 1 and {}, got {read_many_by_pk_count}",
        rows.len()
    );
    let mut client = Client::connect(url, NoTls)
        .unwrap_or_else(|error| panic!("connect to PostgreSQL from {URL_ENV}: {error}"));
    client
        .batch_execute(
            "CREATE TEMP TABLE json_pointer (
                 path TEXT PRIMARY KEY NOT NULL,
                 value TEXT NOT NULL
             );",
        )
        .expect("initialize raw PostgreSQL benchmark table");

    let insert = client
        .prepare("INSERT INTO json_pointer (path, value) VALUES ($1, $2)")
        .expect("prepare raw PostgreSQL insert");
    let read_all_statement = client
        .prepare("SELECT path, value FROM json_pointer ORDER BY path")
        .expect("prepare raw PostgreSQL read all");
    let read_one_statement = client
        .prepare("SELECT path, value FROM json_pointer WHERE path = $1")
        .expect("prepare raw PostgreSQL point read");
    let read_many_statement = client
        .prepare(&select_many_by_pk_sql(read_many_by_pk_count))
        .expect("prepare raw PostgreSQL multi-point read");
    let update = client
        .prepare("UPDATE json_pointer SET value = $1 WHERE path = $2")
        .expect("prepare raw PostgreSQL update");
    let delete_all_statement = client
        .prepare("DELETE FROM json_pointer")
        .expect("prepare raw PostgreSQL delete all");
    let delete_one_statement = client
        .prepare("DELETE FROM json_pointer WHERE path = $1")
        .expect("prepare raw PostgreSQL point delete");

    RawPostgresFixture {
        client,
        rows: rows.to_vec(),
        read_many_by_pk_count,
        insert,
        read_all_statement,
        read_one_statement,
        read_many_statement,
        update,
        delete_all_statement,
        delete_one_statement,
    }
}

pub(crate) fn seeded_fixture(url: &str, rows: &[WorkloadRow]) -> RawPostgresFixture {
    let mut fixture = empty_fixture(url, rows);
    fixture.insert_all();
    fixture
}

impl RawPostgresFixture {
    pub(crate) fn insert_all(&mut self) -> usize {
        let statement = self.insert.clone();
        let mut transaction = self
            .client
            .transaction()
            .expect("begin raw PostgreSQL insert transaction");
        let mut affected = 0_u64;
        for row in &self.rows {
            affected += transaction
                .execute(&statement, &[&row.path, &row.value_json])
                .expect("insert raw PostgreSQL row");
        }
        transaction
            .commit()
            .expect("commit raw PostgreSQL insert transaction");
        assert_eq!(affected as usize, self.rows.len());
        affected as usize
    }

    pub(crate) fn read_all(&mut self) -> usize {
        let results = self
            .client
            .query(&self.read_all_statement, &[])
            .expect("query all raw PostgreSQL rows");
        for result in &results {
            let _: &str = result.get(0);
            let _: &str = result.get(1);
        }
        assert_eq!(results.len(), self.rows.len());
        results.len()
    }

    pub(crate) fn read_one_by_pk(&mut self) -> usize {
        let row = &self.rows[self.rows.len() / 2];
        let results = self
            .client
            .query(&self.read_one_statement, &[&row.path])
            .expect("query raw PostgreSQL point row");
        assert_eq!(results.len(), 1);
        let _: &str = results[0].get(0);
        let _: &str = results[0].get(1);
        results.len()
    }

    pub(crate) fn read_many_by_pk(&mut self, count: usize) -> usize {
        assert_eq!(
            count, self.read_many_by_pk_count,
            "read-many benchmark must use the fixture's setup-excluded query shape"
        );
        let parameters = self.rows[..count]
            .iter()
            .map(|row| {
                let parameter: &(dyn postgres::types::ToSql + Sync) = &row.path;
                parameter
            })
            .collect::<Vec<_>>();
        let results = self
            .client
            .query(&self.read_many_statement, &parameters)
            .expect("query raw PostgreSQL multi-point rows");
        for result in &results {
            let _: &str = result.get(0);
            let _: &str = result.get(1);
        }
        assert_eq!(results.len(), count);
        results.len()
    }

    pub(crate) fn update_all(&mut self) -> usize {
        let statement = self.update.clone();
        let mut transaction = self
            .client
            .transaction()
            .expect("begin raw PostgreSQL update transaction");
        let mut affected = 0_u64;
        for row in &self.rows {
            affected += transaction
                .execute(&statement, &[&row.updated_value_json, &row.path])
                .expect("update raw PostgreSQL row");
        }
        transaction
            .commit()
            .expect("commit raw PostgreSQL update transaction");
        assert_eq!(affected as usize, self.rows.len());
        affected as usize
    }

    pub(crate) fn update_one_by_pk(&mut self) -> usize {
        let row = &self.rows[self.rows.len() / 2];
        let affected = self
            .client
            .execute(&self.update, &[&row.updated_value_json, &row.path])
            .expect("update one raw PostgreSQL row");
        assert_eq!(affected, 1);
        affected as usize
    }

    pub(crate) fn delete_all(&mut self) -> usize {
        let affected = self
            .client
            .execute(&self.delete_all_statement, &[])
            .expect("delete all raw PostgreSQL rows");
        assert_eq!(affected as usize, self.rows.len());
        affected as usize
    }

    pub(crate) fn delete_one_by_pk(&mut self) -> usize {
        let row = &self.rows[self.rows.len() / 2];
        let affected = self
            .client
            .execute(&self.delete_one_statement, &[&row.path])
            .expect("delete one raw PostgreSQL row");
        assert_eq!(affected, 1);
        affected as usize
    }
}

fn select_many_by_pk_sql(count: usize) -> String {
    assert!(count > 0, "read-many benchmark requires at least one row");
    let placeholders = (1..=count)
        .map(|index| format!("${index}"))
        .collect::<Vec<_>>()
        .join(",");
    format!("SELECT path, value FROM json_pointer WHERE path IN ({placeholders}) ORDER BY path")
}
