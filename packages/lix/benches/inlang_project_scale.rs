//! Replays the write and read shape of an inlang project load + Paraglide compile.
//!
//! `@inlang/sdk` registers three row schemas (bundle → message → variant, each
//! child with a foreign key to its parent), inserts every message and variant
//! of a project inside one explicit transaction using multi-row parameterised
//! `INSERT`s, commits, and Paraglide then reads everything back with a flat
//! three-way `LEFT JOIN`. Large projects (40+ locales × 10k messages) exceed
//! the commit deadline and the buffered read budget, so this probe times each
//! phase separately:
//!
//! ```text
//! LIX_INLANG_LOCALES=30 LIX_INLANG_MESSAGES=5000 \
//!   cargo bench -p lix --bench inlang_project_scale
//! ```
//!
//! `LIX_INLANG_PHASES` is a comma-separated subset of
//! `commit,flat,join,keyset,stream` (default: all). `stream` reads the same
//! flat and joined shapes through `Lix::query_stream` with
//! `LIX_INLANG_STREAM_PAGE_BYTES` pages (default 1 MiB). Every read phase
//! reports `peak_rss_mib`, the process high-water mark reset at phase start
//! (Linux only). Profile the built binary with `samply record` or
//! `perf record -g` using the same environment.

use std::future::Future;
use std::hint::black_box;
use std::time::Instant;

use lix::storage::Memory;
use lix::{Json, Lix, Value, open_lix};

const BATCH_ROWS: usize = 500;

/// Paraglide's compile read: every bundle with its messages and variants.
const JOIN_NESTED_SQL: &str = "SELECT b.id AS bundle_id, b.declarations, m.id AS message_id, m.locale, m.selectors, \
     v.id AS variant_id, v.matches, v.pattern \
     FROM inlang_bundle b LEFT JOIN inlang_message m ON m.bundle_id = b.id \
     LEFT JOIN inlang_variant v ON v.message_id = m.id \
     ORDER BY b.id, m.id, v.id";

fn schema_sql(key: &str, columns: &str, foreign_key: Option<(&str, &str)>) -> String {
    let fk = foreign_key
        .map(|(column, parent)| {
            format!(
                ",\"foreign_keys\":[{{\"columns\":[\"{column}\"],\"references\":{{\"schema_key\":\"{parent}\",\"columns\":[\"id\"]}}}}]"
            )
        })
        .unwrap_or_default();
    format!(
        "INSERT INTO lix_registered_schema (value) VALUES (CAST('{{\"$schema\":\"https://lix.dev/schema-v1.json\",\
         \"key\":\"{key}\",\"columns\":[{columns}],\"primary_key\":[\"id\"]{fk}}}' AS JSONB))"
    )
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .parse()
                .unwrap_or_else(|_| panic!("{name} must be a positive integer"))
        })
        .unwrap_or(default)
}

fn jsonb(text: &str) -> Value {
    Value::Jsonb(Json::parse(text).expect("benchmark JSON is valid"))
}

fn multi_row_insert(table: &str, columns: &[&str], rows: usize) -> String {
    let mut sql = format!("INSERT INTO {table} ({}) VALUES ", columns.join(", "));
    let mut parameter = 1;
    for row in 0..rows {
        if row > 0 {
            sql.push_str(", ");
        }
        sql.push('(');
        for column in 0..columns.len() {
            if column > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&format!("${parameter}"));
            parameter += 1;
        }
        sql.push(')');
    }
    sql
}

async fn timed<T>(label: &str, rows: usize, operation: impl Future<Output = T>) -> T {
    let start_rss = reset_peak_rss();
    let started = Instant::now();
    let result = operation.await;
    let elapsed = started.elapsed();
    println!(
        "inlang_project_scale phase={label} rows={rows} ms={} us_per_row={:.1} start_rss_mib={start_rss} peak_rss_mib={}",
        elapsed.as_millis(),
        elapsed.as_secs_f64() * 1e6 / rows.max(1) as f64,
        peak_rss_mib()
    );
    result
}

/// Resets the kernel's resident-set high-water mark so the next
/// `peak_rss_mib()` covers only the phase that follows. Returns the resident
/// set at the reset, i.e. the phase's starting point.
fn reset_peak_rss() -> String {
    let _ = std::fs::write("/proc/self/clear_refs", "5");
    status_mib("VmRSS:")
}

fn peak_rss_mib() -> String {
    status_mib("VmHWM:")
}

fn status_mib(field: &str) -> String {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find_map(|line| line.strip_prefix(field))
                .and_then(|value| {
                    value
                        .trim()
                        .trim_end_matches("kB")
                        .trim()
                        .parse::<u64>()
                        .ok()
                })
        })
        .map_or_else(|| "n/a".to_owned(), |kib| (kib / 1024).to_string())
}

/// Pulls every page of `sql`, touching each row the way a consumer would.
async fn stream_phase(session: &Lix<Memory>, label: &str, sql: &str, page_bytes: usize) {
    let start_rss = reset_peak_rss();
    let started = Instant::now();
    let mut first_page_ms = None;
    let mut rows = 0usize;
    let mut pages = 0usize;
    let outcome = async {
        let mut stream = session
            .query_stream(sql, &[])
            .with_page_bytes(page_bytes)
            .await?;
        while let Some(page) = stream.next_page().await? {
            first_page_ms.get_or_insert_with(|| started.elapsed().as_millis());
            pages += 1;
            rows += black_box(page.rows()).len();
        }
        Ok::<_, lix::LixError>(())
    }
    .await;
    let elapsed = started.elapsed();
    match outcome {
        Ok(()) => println!(
            "inlang_project_scale phase={label} rows={rows} pages={pages} page_bytes={page_bytes} \
             ms={} first_page_ms={} us_per_row={:.1} start_rss_mib={start_rss} peak_rss_mib={}",
            elapsed.as_millis(),
            first_page_ms.unwrap_or_default(),
            elapsed.as_secs_f64() * 1e6 / rows.max(1) as f64,
            peak_rss_mib()
        ),
        Err(error) => println!(
            "inlang_project_scale phase={label} error={} ms={}",
            error.code,
            elapsed.as_millis()
        ),
    }
}

async fn insert_all(
    transaction: &mut lix::LixTransaction<Memory>,
    table: &str,
    columns: &[&str],
    rows: Vec<Vec<Value>>,
) {
    for chunk in rows.chunks(BATCH_ROWS) {
        let sql = multi_row_insert(table, columns, chunk.len());
        let params: Vec<Value> = chunk.iter().flatten().cloned().collect();
        transaction
            .execute(&sql, &params)
            .await
            .unwrap_or_else(|error| panic!("insert into {table} failed: {error:?}"));
    }
}

async fn run() {
    let locales = env_usize("LIX_INLANG_LOCALES", 30);
    let messages = env_usize("LIX_INLANG_MESSAGES", 5_000);
    let phases = std::env::var("LIX_INLANG_PHASES")
        .unwrap_or_else(|_| "commit,flat,join,keyset,stream".into());
    let rows = locales * messages;
    println!(
        "inlang_project_scale locales={locales} messages={messages} message_rows={rows} variant_rows={rows}"
    );

    let session: Lix<Memory> = open_lix()
        .with_storage(Memory::new())
        .await
        .expect("open inlang benchmark lix");
    for sql in [
        schema_sql(
            "inlang_bundle",
            "{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"declarations\",\"type\":\"jsonb\",\"nullable\":false,\"default_value\":[]}",
            None,
        ),
        schema_sql(
            "inlang_message",
            "{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"bundle_id\",\"type\":\"text\",\"nullable\":false},\
             {\"name\":\"locale\",\"type\":\"text\",\"nullable\":false},{\"name\":\"selectors\",\"type\":\"jsonb\",\"nullable\":false,\"default_value\":[]}",
            Some(("bundle_id", "inlang_bundle")),
        ),
        schema_sql(
            "inlang_variant",
            "{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"message_id\",\"type\":\"text\",\"nullable\":false},\
             {\"name\":\"matches\",\"type\":\"jsonb\",\"nullable\":false,\"default_value\":[]},{\"name\":\"pattern\",\"type\":\"jsonb\",\"nullable\":false,\"default_value\":[]}",
            Some(("message_id", "inlang_message")),
        ),
    ] {
        session
            .execute(&sql, &[])
            .await
            .expect("register inlang schema");
    }

    // Same value shapes the message-format plugin produces.
    let bundles: Vec<Vec<Value>> = (0..messages)
        .map(|i| {
            vec![
                Value::Text(format!("section{}.key_{i}", i % 50)),
                jsonb("[{\"type\":\"input-variable\",\"name\":\"name\"}]"),
            ]
        })
        .collect();
    let mut message_rows = Vec::with_capacity(rows);
    let mut variant_rows = Vec::with_capacity(rows);
    for locale in 0..locales {
        for i in 0..messages {
            let message_id = format!("m-{locale}-{i}");
            message_rows.push(vec![
                Value::Text(format!("section{}.key_{i}", i % 50)),
                jsonb("[]"),
                Value::Text(format!("l{locale}")),
                Value::Text(message_id.clone()),
            ]);
            variant_rows.push(vec![
                Value::Text(format!("v-{locale}-{i}")),
                Value::Text(message_id),
                jsonb("[]"),
                jsonb(&format!(
                    "[{{\"type\":\"text\",\"value\":\"l{locale} message number {i} with \"}},\
                     {{\"type\":\"expression\",\"arg\":{{\"type\":\"variable-reference\",\"name\":\"name\"}}}},\
                     {{\"type\":\"text\",\"value\":\" and some text\"}}]"
                )),
            ]);
        }
    }

    let mut transaction = session
        .begin_transaction()
        .await
        .expect("begin inlang import transaction");
    timed(
        "insert_bundles",
        messages,
        insert_all(
            &mut transaction,
            "inlang_bundle",
            &["id", "declarations"],
            bundles,
        ),
    )
    .await;
    timed(
        "insert_messages",
        rows,
        insert_all(
            &mut transaction,
            "inlang_message",
            &["bundle_id", "selectors", "locale", "id"],
            message_rows,
        ),
    )
    .await;
    timed(
        "insert_variants",
        rows,
        insert_all(
            &mut transaction,
            "inlang_variant",
            &["id", "message_id", "matches", "pattern"],
            variant_rows,
        ),
    )
    .await;
    timed("commit", rows * 2 + messages, async {
        transaction.commit().await.expect("commit inlang import")
    })
    .await;

    if phases.contains("flat") {
        let result = timed("flat_variants", rows, async {
            session
                .execute(
                    "SELECT id, message_id, matches, pattern FROM inlang_variant",
                    &[],
                )
                .await
        })
        .await;
        match result {
            Ok(result) => {
                black_box(result.len());
            }
            Err(error) => println!(
                "inlang_project_scale phase=flat_variants error={}",
                error.code
            ),
        }
        let result = timed("flat_messages", rows, async {
            session
                .execute(
                    "SELECT id, bundle_id, locale, selectors FROM inlang_message",
                    &[],
                )
                .await
        })
        .await;
        if let Err(error) = result {
            println!(
                "inlang_project_scale phase=flat_messages error={}",
                error.code
            );
        }
    }
    if phases.contains("keyset") {
        let page = 10_000.min(rows);
        let mut last = String::new();
        let mut pages = 0;
        let started = Instant::now();
        loop {
            let result = session
                .execute(
                    "SELECT id, message_id, matches, pattern FROM inlang_variant WHERE id > $1 ORDER BY id LIMIT 10000",
                    &[Value::Text(last.clone())],
                )
                .await
                .expect("keyset page");
            pages += 1;
            if result.len() < page {
                break;
            }
            last = match result.rows().last().and_then(|row| row.get_index(0)) {
                Some(Value::Text(id)) => id.clone(),
                other => panic!("unexpected keyset id {other:?}"),
            };
        }
        let elapsed = started.elapsed();
        println!(
            "inlang_project_scale phase=keyset_scan pages={pages} ms={} ms_per_page={:.1}",
            elapsed.as_millis(),
            elapsed.as_millis() as f64 / pages as f64
        );
    }
    // Ad-hoc probes: LIX_INLANG_SQL="SELECT ...;;SELECT ..." (or LIX_INLANG_SQL_FILE for long
    // statements); $1 binds a mid-table variant id.
    let probes = std::env::var("LIX_INLANG_SQL").ok().or_else(|| {
        std::env::var("LIX_INLANG_SQL_FILE")
            .ok()
            .map(|path| std::fs::read_to_string(path).expect("read LIX_INLANG_SQL_FILE"))
    });
    if let Some(probes) = probes {
        let mid = Value::Text(format!("v-{}-{}", locales / 2, messages / 2));
        for sql in probes
            .split(";;")
            .map(str::trim)
            .filter(|sql| !sql.is_empty())
        {
            let params: Vec<Value> = if sql.contains("$1") {
                vec![mid.clone()]
            } else {
                vec![]
            };
            let started = Instant::now();
            let result = session.execute(sql, &params).await;
            let elapsed = started.elapsed();
            match result {
                Ok(result) => {
                    println!(
                        "inlang_project_scale probe ms={} rows={} sql={sql}",
                        elapsed.as_millis(),
                        result.len()
                    );
                    if sql.starts_with("EXPLAIN") {
                        for row in result.rows() {
                            for value in row.values() {
                                if let Value::Text(text) = value {
                                    println!("{text}");
                                }
                            }
                        }
                    }
                }
                Err(error) => println!(
                    "inlang_project_scale probe ms={} error={} sql={sql}",
                    elapsed.as_millis(),
                    error.code
                ),
            }
        }
    }
    if phases.contains("stream") {
        let page_bytes = env_usize(
            "LIX_INLANG_STREAM_PAGE_BYTES",
            lix::DEFAULT_QUERY_STREAM_PAGE_BYTES,
        );
        stream_phase(
            &session,
            "stream_variants",
            "SELECT id, message_id, matches, pattern FROM inlang_variant",
            page_bytes,
        )
        .await;
        stream_phase(
            &session,
            "stream_messages",
            "SELECT id, bundle_id, locale, selectors FROM inlang_message",
            page_bytes,
        )
        .await;
        stream_phase(&session, "stream_join_nested", JOIN_NESTED_SQL, page_bytes).await;
    }
    if phases.contains("join") {
        let result = timed("join_nested", rows, async {
            session.execute(JOIN_NESTED_SQL, &[]).await
        })
        .await;
        match result {
            Ok(result) => {
                black_box(result.len());
            }
            Err(error) => println!(
                "inlang_project_scale phase=join_nested error={}",
                error.code
            ),
        }
    }
}

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build benchmark runtime");
    runtime.block_on(run());
}
