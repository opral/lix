use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

type AnyResult<T> = Result<T, Box<dyn Error>>;

const SQLITE_EXPECTED: &str = "3.46.0";
const DOMAIN: &str = "lix.sqlite-oltp.baseline.0e543716.v1\0";

#[derive(Default)]
struct Counters {
    sql_statements: u64,
    read_queries: u64,
    write_statements: u64,
    returned_rows: u64,
    logical_ops: u64,
    transactions: u64,
    commits: u64,
    rollbacks: u64,
    savepoints: u64,
    sqlite_changes: u64,
}

struct DigestState(Sha256);

impl DigestState {
    fn new() -> Self {
        let mut hasher = Sha256::new();
        hasher.update(DOMAIN.as_bytes());
        Self(hasher)
    }

    fn event(&mut self, label: &str, fields: &[&str]) {
        self.0.update((label.len() as u64).to_le_bytes());
        self.0.update(label.as_bytes());
        for field in fields {
            self.0.update((field.len() as u64).to_le_bytes());
            self.0.update(field.as_bytes());
        }
    }

    fn finish(self) -> String {
        self.0
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }
}

#[derive(Debug)]
struct RowData {
    id: String,
    value: String,
    version: i64,
    nullable: Option<String>,
}

struct FileRowSpec<'a> {
    file_id: &'a str,
    snapshot_id: &'a str,
    kind: &'a str,
    descriptor_id: Option<&'a str>,
    blob_ref: Option<&'a [u8]>,
    content: Option<&'a [u8]>,
    deleted: bool,
}

type StoredFileRow = (
    String,
    Option<String>,
    Option<Vec<u8>>,
    Option<Vec<u8>>,
    bool,
);

fn row_data(row: &rusqlite::Row<'_>) -> rusqlite::Result<RowData> {
    Ok(RowData {
        id: row.get(0)?,
        value: row.get(1)?,
        version: row.get(2)?,
        nullable: row.get(3)?,
    })
}

fn record_row(digest: &mut DigestState, label: &str, row: &RowData) {
    digest.event(
        label,
        &[
            &row.id,
            &row.value,
            &row.version.to_string(),
            row.nullable.as_deref().unwrap_or("<NULL>"),
        ],
    );
}

fn configure(conn: &Connection) -> AnyResult<()> {
    conn.execute_batch(
        "PRAGMA journal_mode = DELETE;
         PRAGMA synchronous = FULL;
         PRAGMA foreign_keys = ON;
         PRAGMA recursive_triggers = OFF;
         PRAGMA temp_store = MEMORY;
         PRAGMA locking_mode = NORMAL;
         PRAGMA busy_timeout = 0;
         CREATE TABLE IF NOT EXISTS rows (
             id TEXT PRIMARY KEY NOT NULL,
             value TEXT NOT NULL,
             version INTEGER NOT NULL,
             nullable TEXT
         ) WITHOUT ROWID;
         CREATE TABLE IF NOT EXISTS file_rows (
             file_id TEXT NOT NULL,
             snapshot_id TEXT NOT NULL,
             kind TEXT NOT NULL CHECK(kind IN ('file', 'directory', 'plugin')),
             descriptor_id TEXT,
             blob_ref BLOB,
             snapshot_content BLOB,
             deleted INTEGER NOT NULL CHECK(deleted IN (0, 1)),
             PRIMARY KEY(file_id, snapshot_id)
         ) WITHOUT ROWID;",
    )?;
    let version: String = conn.query_row("SELECT sqlite_version()", [], |row| row.get(0))?;
    if version != SQLITE_EXPECTED {
        return Err(format!("expected SQLite {SQLITE_EXPECTED}, got {version}").into());
    }
    Ok(())
}

fn seed_rows(conn: &mut Connection, rows: usize) -> AnyResult<()> {
    let tx = conn.transaction()?;
    {
        let mut insert =
            tx.prepare("INSERT INTO rows(id, value, version, nullable) VALUES (?1, ?2, ?3, ?4)")?;
        for index in 0..rows {
            let id = format!("r{index:06}");
            let value = format!("value:{id}");
            let nullable = (index % 11 != 0).then(|| format!("nullable:{id}"));
            insert.execute(params![id, value, index as i64, nullable])?;
        }
    }
    tx.commit()?;
    Ok(())
}

fn digest_all_rows(conn: &Connection, counters: &mut Counters) -> AnyResult<String> {
    let mut digest = DigestState::new();
    let mut statement =
        conn.prepare("SELECT id, value, version, nullable FROM rows ORDER BY id COLLATE BINARY")?;
    counters.sql_statements += 1;
    counters.read_queries += 1;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let row = row_data(row)?;
        counters.returned_rows += 1;
        record_row(&mut digest, "row", &row);
    }
    Ok(digest.finish())
}

fn point_reads(conn: &Connection, counters: &mut Counters, rows: usize) -> AnyResult<String> {
    let mut digest = DigestState::new();
    let mut statement =
        conn.prepare("SELECT id, value, version, nullable FROM rows WHERE id = ?1")?;
    counters.sql_statements += 1;
    for index in 0..rows {
        let probe = (index * 37 + 11) % rows;
        let id = format!("r{probe:06}");
        let row = statement.query_row(params![id], row_data)?;
        counters.read_queries += 1;
        counters.logical_ops += 1;
        counters.returned_rows += 1;
        record_row(&mut digest, "point", &row);
    }
    Ok(digest.finish())
}

fn returning_one(
    conn: &Connection,
    counters: &mut Counters,
    digest: &mut DigestState,
    label: &str,
    sql: &str,
    parameters: &[&dyn rusqlite::ToSql],
) -> AnyResult<()> {
    let row = conn.query_row(sql, parameters, row_data)?;
    counters.sql_statements += 1;
    counters.write_statements += 1;
    counters.logical_ops += 1;
    counters.returned_rows += 1;
    counters.sqlite_changes += conn.changes();
    record_row(digest, label, &row);
    Ok(())
}

fn crud(conn: &Connection, counters: &mut Counters) -> AnyResult<String> {
    let mut digest = DigestState::new();
    conn.execute_batch("BEGIN IMMEDIATE")?;
    counters.sql_statements += 1;
    counters.transactions += 1;
    for index in 1000..1064 {
        let id = format!("r{index:06}");
        let value = format!("inserted:{id}");
        let version = index as i64;
        returning_one(
            conn,
            counters,
            &mut digest,
            "insert-returning",
            "INSERT INTO rows(id, value, version, nullable) VALUES (?1, ?2, ?3, NULL) RETURNING id, value, version, nullable",
            &[&id, &value, &version],
        )?;
    }
    for index in 0..64 {
        let id = format!("r{index:06}");
        let value = format!("updated:{id}");
        let version = (index + 10000) as i64;
        returning_one(
            conn,
            counters,
            &mut digest,
            "update-returning",
            "UPDATE rows SET value = ?1, version = ?2 WHERE id = ?3 RETURNING id, value, version, nullable",
            &[&value, &version, &id],
        )?;
    }
    for index in 64..96 {
        let id = format!("r{index:06}");
        returning_one(
            conn,
            counters,
            &mut digest,
            "delete-returning",
            "DELETE FROM rows WHERE id = ?1 RETURNING id, value, version, nullable",
            &[&id],
        )?;
    }
    conn.execute_batch("COMMIT")?;
    counters.sql_statements += 1;
    counters.commits += 1;
    Ok(digest.finish())
}

fn savepoint_transaction(conn: &Connection, counters: &mut Counters) -> AnyResult<String> {
    let mut digest = DigestState::new();
    conn.execute_batch("BEGIN IMMEDIATE; SAVEPOINT rollback_probe")?;
    counters.sql_statements += 2;
    counters.transactions += 1;
    counters.savepoints += 1;
    for index in 0..8 {
        let id = format!("rollback-{index:03}");
        let value = format!("discarded:{index}");
        conn.execute(
            "INSERT INTO rows(id, value, version, nullable) VALUES (?1, ?2, ?3, NULL)",
            params![id, value, index as i64],
        )?;
        counters.sql_statements += 1;
        counters.write_statements += 1;
        counters.logical_ops += 1;
        counters.sqlite_changes += conn.changes();
    }
    conn.execute_batch("ROLLBACK TO rollback_probe; RELEASE rollback_probe")?;
    counters.sql_statements += 2;
    counters.rollbacks += 1;
    for index in 0..8 {
        let id = format!("txn-{index:03}");
        let value = format!("committed:{index}");
        returning_one(
            conn,
            counters,
            &mut digest,
            "transaction-returning",
            "INSERT INTO rows(id, value, version, nullable) VALUES (?1, ?2, ?3, NULL) RETURNING id, value, version, nullable",
            &[&id, &value, &(index as i64)],
        )?;
    }
    conn.execute_batch("COMMIT")?;
    counters.sql_statements += 1;
    counters.commits += 1;
    Ok(digest.finish())
}

fn conflicts(conn: &Connection, counters: &mut Counters) -> AnyResult<String> {
    let mut digest = DigestState::new();
    conn.execute_batch("BEGIN IMMEDIATE")?;
    counters.sql_statements += 1;
    counters.transactions += 1;
    let id = "r000000";
    let value = "conflict-update";
    let version = 70000_i64;
    returning_one(
        conn,
        counters,
        &mut digest,
        "upsert-update-returning",
        "INSERT INTO rows(id, value, version, nullable) VALUES (?1, ?2, ?3, NULL) ON CONFLICT(id) DO UPDATE SET value = excluded.value, version = excluded.version RETURNING id, value, version, nullable",
        &[&id, &value, &version],
    )?;
    let new_id = "r002000";
    let new_value = "conflict-insert";
    let new_version = 2000_i64;
    returning_one(
        conn,
        counters,
        &mut digest,
        "upsert-insert-returning",
        "INSERT INTO rows(id, value, version, nullable) VALUES (?1, ?2, ?3, NULL) ON CONFLICT(id) DO NOTHING RETURNING id, value, version, nullable",
        &[&new_id, &new_value, &new_version],
    )?;
    let ignored_id = "r000001";
    let ignored_value = "must-not-write";
    let ignored_version = 90000_i64;
    let ignored: Option<RowData> = conn
        .query_row(
            "INSERT INTO rows(id, value, version, nullable) VALUES (?1, ?2, ?3, NULL) ON CONFLICT(id) DO NOTHING RETURNING id, value, version, nullable",
            params![ignored_id, ignored_value, ignored_version],
            row_data,
        )
        .optional()?;
    counters.sql_statements += 1;
    counters.write_statements += 1;
    counters.logical_ops += 1;
    if let Some(row) = ignored {
        counters.returned_rows += 1;
        record_row(&mut digest, "unexpected-do-nothing-row", &row);
        return Err("ON CONFLICT DO NOTHING unexpectedly returned a row".into());
    }
    digest.event("do-nothing", &[ignored_id, "no-row"]);
    conn.execute_batch("COMMIT")?;
    counters.sql_statements += 1;
    counters.commits += 1;
    Ok(digest.finish())
}

fn insert_file_row(
    conn: &Connection,
    counters: &mut Counters,
    spec: FileRowSpec<'_>,
) -> AnyResult<()> {
    conn.execute(
        "INSERT INTO file_rows(file_id, snapshot_id, kind, descriptor_id, blob_ref, snapshot_content, deleted) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![spec.file_id, spec.snapshot_id, spec.kind, spec.descriptor_id, spec.blob_ref, spec.content, spec.deleted as i64],
    )?;
    counters.sql_statements += 1;
    counters.write_statements += 1;
    counters.logical_ops += 1;
    counters.sqlite_changes += conn.changes();
    Ok(())
}

fn validate_file_row(conn: &Connection, file_id: &str, snapshot_id: &str) -> Result<(), String> {
    let row: StoredFileRow = conn
        .query_row(
            "SELECT kind, descriptor_id, blob_ref, snapshot_content, deleted FROM file_rows WHERE file_id = ?1 AND snapshot_id = ?2",
            params![file_id, snapshot_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .map_err(|error| error.to_string())?;
    let (kind, descriptor_id, blob_ref, content, deleted) = row;
    if kind == "directory" {
        if descriptor_id.is_some() || blob_ref.is_some() || content.is_some() {
            return Err("directory-owned payload or descriptor".into());
        }
        return Ok(());
    }
    if deleted {
        if blob_ref.is_some() || content.is_some() {
            return Err("tombstone carries payload".into());
        }
        return Ok(());
    }
    if descriptor_id.as_deref() != Some(file_id) {
        return Err("live file descriptor identity mismatch".into());
    }
    if blob_ref.is_none() || content.is_none() {
        return Err("live file missing required BlobRef/content".into());
    }
    Ok(())
}

fn file_rows(conn: &Connection, counters: &mut Counters) -> AnyResult<String> {
    let mut digest = DigestState::new();
    conn.execute_batch("BEGIN IMMEDIATE")?;
    counters.sql_statements += 1;
    counters.transactions += 1;
    insert_file_row(
        conn,
        counters,
        FileRowSpec {
            file_id: "file-001",
            snapshot_id: "snap-001",
            kind: "file",
            descriptor_id: Some("file-001"),
            blob_ref: Some(b"blob-a"),
            content: Some(b"content-a"),
            deleted: false,
        },
    )?;
    validate_file_row(conn, "file-001", "snap-001")
        .map_err(|error| -> Box<dyn Error> { error.into() })?;
    digest.event("file-live", &["file-001", "blob-a"]);
    conn.execute(
        "UPDATE file_rows SET blob_ref = ?1, snapshot_content = ?2 WHERE file_id = ?3 AND snapshot_id = ?4",
        params![b"blob-b".as_slice(), b"content-b".as_slice(), "file-001", "snap-001"],
    )?;
    counters.sql_statements += 1;
    counters.write_statements += 1;
    counters.logical_ops += 1;
    counters.sqlite_changes += conn.changes();
    validate_file_row(conn, "file-001", "snap-001")
        .map_err(|error| -> Box<dyn Error> { error.into() })?;
    digest.event("file-update", &["file-001", "blob-b"]);
    conn.execute(
        "UPDATE file_rows SET blob_ref = NULL, snapshot_content = NULL, deleted = 1 WHERE file_id = ?1 AND snapshot_id = ?2",
        params!["file-001", "snap-001"],
    )?;
    counters.sql_statements += 1;
    counters.write_statements += 1;
    counters.logical_ops += 1;
    counters.sqlite_changes += conn.changes();
    validate_file_row(conn, "file-001", "snap-001")
        .map_err(|error| -> Box<dyn Error> { error.into() })?;
    digest.event("file-tombstone", &["file-001", "absent"]);
    insert_file_row(
        conn,
        counters,
        FileRowSpec {
            file_id: "directory-001",
            snapshot_id: "snap-001",
            kind: "directory",
            descriptor_id: None,
            blob_ref: None,
            content: None,
            deleted: false,
        },
    )?;
    validate_file_row(conn, "directory-001", "snap-001")
        .map_err(|error| -> Box<dyn Error> { error.into() })?;
    digest.event("directory", &["directory-001", "no-payload"]);
    conn.execute_batch("COMMIT")?;
    counters.sql_statements += 1;
    counters.commits += 1;
    Ok(digest.finish())
}

fn corruption(conn: &Connection, counters: &mut Counters) -> AnyResult<String> {
    let mut digest = DigestState::new();
    conn.execute_batch("BEGIN IMMEDIATE")?;
    counters.sql_statements += 1;
    counters.transactions += 1;
    insert_file_row(
        conn,
        counters,
        FileRowSpec {
            file_id: "empty-file",
            snapshot_id: "snap-001",
            kind: "file",
            descriptor_id: Some("empty-file"),
            blob_ref: Some(b""),
            content: Some(b""),
            deleted: false,
        },
    )?;
    let valid_empty = validate_file_row(conn, "empty-file", "snap-001").is_ok();
    digest.event(
        "explicit-empty",
        &[if valid_empty { "valid" } else { "invalid" }],
    );

    insert_file_row(
        conn,
        counters,
        FileRowSpec {
            file_id: "missing-ref",
            snapshot_id: "snap-001",
            kind: "file",
            descriptor_id: Some("missing-ref"),
            blob_ref: None,
            content: Some(b"content"),
            deleted: false,
        },
    )?;
    let missing_ref_rejected = validate_file_row(conn, "missing-ref", "snap-001").is_err();
    digest.event(
        "missing-blob-ref",
        &[if missing_ref_rejected {
            "rejected"
        } else {
            "accepted"
        }],
    );

    insert_file_row(
        conn,
        counters,
        FileRowSpec {
            file_id: "payload-tombstone",
            snapshot_id: "snap-001",
            kind: "file",
            descriptor_id: Some("payload-tombstone"),
            blob_ref: Some(b"stale-blob"),
            content: Some(b"stale-content"),
            deleted: true,
        },
    )?;
    let payload_tombstone_rejected =
        validate_file_row(conn, "payload-tombstone", "snap-001").is_err();
    digest.event(
        "tombstone-payload",
        &[if payload_tombstone_rejected {
            "rejected"
        } else {
            "accepted"
        }],
    );

    insert_file_row(
        conn,
        counters,
        FileRowSpec {
            file_id: "identity-mismatch",
            snapshot_id: "snap-001",
            kind: "file",
            descriptor_id: Some("other-file"),
            blob_ref: Some(b"blob"),
            content: Some(b"content"),
            deleted: false,
        },
    )?;
    let identity_rejected = validate_file_row(conn, "identity-mismatch", "snap-001").is_err();
    digest.event(
        "descriptor-identity",
        &[if identity_rejected {
            "rejected"
        } else {
            "accepted"
        }],
    );
    if !(valid_empty && missing_ref_rejected && payload_tombstone_rejected && identity_rejected) {
        return Err("SQLite file-row corruption controls did not discriminate".into());
    }
    conn.execute_batch("COMMIT")?;
    counters.sql_statements += 1;
    counters.commits += 1;
    Ok(digest.finish())
}

fn page_stats(conn: &Connection) -> AnyResult<(i64, i64)> {
    let pages: i64 = conn.query_row("PRAGMA page_count", [], |row| row.get(0))?;
    let free: i64 = conn.query_row("PRAGMA freelist_count", [], |row| row.get(0))?;
    Ok((pages, free))
}

fn run_cell(root: &Path, cell: &str, rows: usize) -> AnyResult<()> {
    let path = root.join(format!("{cell}.sqlite"));
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut conn = Connection::open(&path)?;
    let setup_start = Instant::now();
    configure(&conn)?;
    if !matches!(cell, "file-row" | "corruption") {
        seed_rows(&mut conn, rows)?;
    }
    let setup_ns = setup_start.elapsed().as_nanos();
    let mut counters = Counters::default();
    let operation_start = Instant::now();
    let warm_digest = match cell {
        "point-1000" => point_reads(&conn, &mut counters, rows)?,
        "crud" => crud(&conn, &mut counters)?,
        "transaction-savepoint" => savepoint_transaction(&conn, &mut counters)?,
        "conflict" => conflicts(&conn, &mut counters)?,
        "reopen" => digest_all_rows(&conn, &mut counters)?,
        "file-row" => file_rows(&conn, &mut counters)?,
        "corruption" => corruption(&conn, &mut counters)?,
        other => return Err(format!("unknown cell {other}").into()),
    };
    let operation_ns = operation_start.elapsed().as_nanos();
    let (cold_digest, page_count, freelist_count) = if cell == "reopen" {
        drop(conn);
        let reopened = Connection::open(&path)?;
        configure(&reopened)?;
        let cold = digest_all_rows(&reopened, &mut counters)?;
        let (pages, free) = page_stats(&reopened)?;
        (Some(cold), pages, free)
    } else {
        let (pages, free) = page_stats(&conn)?;
        (None, pages, free)
    };
    let file_bytes = fs::metadata(&path)?.len();
    let verified =
        cold_digest.as_ref().is_none_or(|cold| cold == &warm_digest) && !warm_digest.is_empty();
    println!(
        "cell={cell},sqlite_version={SQLITE_EXPECTED},seed_rows={},logical_ops={},returned_rows={},result_sha256={warm_digest},cold_result_sha256={},cold_reopen={},sql_statements={},read_queries={},write_statements={},transactions={},savepoints={},commits={},rollbacks={},sqlite_changes={},page_count={page_count},freelist_pages={freelist_count},file_bytes={file_bytes},setup_ns={setup_ns},operation_ns={operation_ns},verified={verified}",
        if matches!(cell, "file-row" | "corruption") {
            0
        } else {
            rows
        },
        counters.logical_ops,
        counters.returned_rows,
        cold_digest.as_deref().unwrap_or(""),
        cold_digest.is_some(),
        counters.sql_statements,
        counters.read_queries,
        counters.write_statements,
        counters.transactions,
        counters.savepoints,
        counters.commits,
        counters.rollbacks,
        counters.sqlite_changes,
    );
    if !verified {
        return Err(format!("digest verification failed for {cell}").into());
    }
    Ok(())
}

fn main() -> AnyResult<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let root = PathBuf::from(
        args.get(1)
            .ok_or("usage: sqlite_oltp_baseline_0e543716 <fresh-dir> <cell> [rows]")?,
    );
    let cell = args.get(2).map(String::as_str).unwrap_or("smoke");
    let rows = args
        .get(3)
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(1000);
    fs::create_dir_all(&root)?;
    if cell == "smoke" {
        for cell in [
            "point-1000",
            "crud",
            "transaction-savepoint",
            "conflict",
            "reopen",
            "file-row",
            "corruption",
        ] {
            run_cell(&root, cell, rows)?;
        }
    } else {
        run_cell(&root, cell, rows)?;
    }
    Ok(())
}
