//! E26: where lix's bytes-on-disk overhead against git+LFS actually goes.
//!
//! claim-2 measured lix storing 1.54-1.87x the bytes of an equivalent git+LFS
//! repository. A ratio is not actionable; an attribution is. This example
//! imports a corpus, runs N agent commits, and reports **per storage space**:
//! bytes after the import, bytes after N commits, and the per-commit delta.
//!
//! That split is the whole point. Bytes that appear at import and never grow
//! are *structural* — they are the queryable row-level state that git does not
//! store at all, which is the product rather than overhead. Bytes that grow per
//! commit are what a release post has to justify, and are where a cut could
//! live.
//!
//! `layout_accounting` reports logical key/value bytes and is adapter-neutral
//! (byte counts agree to within a byte across backends), so the attribution
//! holds for both shipping adapters even though this runs on RocksDB — the same
//! adapter claim-2 measured the ratio on. The on-disk figure is the real
//! RocksDB directory size after a clean close, which is the number comparable
//! to `.git`.
//!
//! Usage:
//!   e26_disk_attribution <db_dir> <rounds> [corpus_dir]
//!
//! With `corpus_dir` the corpus is every regular file under that directory.
//! Without it, a synthetic media corpus is generated from
//! `E26_ASSET_KIB` (default 10240) x `E26_FILES` (default 64) of incompressible
//! bytes, matching the claim-2 `bigmedia` shape.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use lix::storage_adapter::{StorageAdapter, StorageReadOptions};
use lix::storage_bench::layout_accounting;
use lix::{Lix, Value, open_lix};
use lix_storage_rocksdb::RocksDB;

#[derive(Clone, Copy, Default)]
struct Usage {
    rows: u64,
    key_bytes: u64,
    value_bytes: u64,
}

impl Usage {
    fn bytes(self) -> u64 {
        self.key_bytes + self.value_bytes
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn fill_pseudo_random(buffer: &mut [u8], seed: u64) {
    let mut state = seed | 1;
    for chunk in buffer.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let bytes = state.to_le_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
}

struct CorpusFile {
    path: String,
    bytes: Vec<u8>,
}

fn synthetic_corpus(asset_bytes: usize, files: usize) -> Vec<CorpusFile> {
    (0..files)
        .map(|index| {
            let mut bytes = vec![0_u8; asset_bytes];
            fill_pseudo_random(&mut bytes, 0x9E37_79B9_7F4A_7C15 ^ (index as u64 + 1));
            CorpusFile {
                path: format!("/assets/{index:05}.bin"),
                bytes,
            }
        })
        .collect()
}

fn directory_corpus(root: &Path) -> Vec<CorpusFile> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                if path.file_name().is_some_and(|name| name == ".git") {
                    continue;
                }
                stack.push(path);
            } else if meta.is_file()
                && let Ok(bytes) = std::fs::read(&path)
                && let Ok(rel) = path.strip_prefix(root)
            {
                out.push(CorpusFile {
                    path: format!("/{}", rel.to_string_lossy()),
                    bytes,
                });
            }
        }
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    out
}

async fn usage(storage: &RocksDB) -> BTreeMap<(u32, &'static str), Usage> {
    let adapter = StorageAdapter::new(storage.clone());
    let read = adapter
        .begin_read(StorageReadOptions::default())
        .await
        .expect("open storage snapshot");
    let out = layout_accounting(&read)
        .await
        .into_iter()
        .map(|entry| {
            (
                (entry.space_id, entry.space),
                Usage {
                    rows: entry.rows,
                    key_bytes: entry.key_bytes,
                    value_bytes: entry.value_bytes,
                },
            )
        })
        .collect();
    drop(read);
    out
}

fn directory_bytes(path: &Path) -> u64 {
    let mut total = 0;
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                stack.push(entry.path());
            } else {
                total += meta.len();
            }
        }
    }
    total
}

fn extension_bytes(path: &Path, extension: &str) -> u64 {
    let mut total = 0;
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                stack.push(entry.path());
            } else if entry
                .path()
                .extension()
                .is_some_and(|ext| ext == extension)
            {
                total += meta.len();
            }
        }
    }
    total
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let db_dir = PathBuf::from(args.next().expect("usage: e26_disk_attribution <db_dir> <rounds>"));
    let rounds = args
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(20);
    let corpus_dir = args.next().map(PathBuf::from);

    let corpus = match &corpus_dir {
        Some(dir) => directory_corpus(dir),
        None => synthetic_corpus(env_usize("E26_ASSET_KIB", 10240) * 1024, env_usize("E26_FILES", 64)),
    };
    assert!(corpus.len() > 3, "corpus too small: {} files", corpus.len());
    let raw_bytes: u64 = corpus.iter().map(|f| f.bytes.len() as u64).sum();
    println!(
        "e26 corpus source={} files={} raw_bytes={raw_bytes} rounds={rounds}",
        corpus_dir
            .as_ref()
            .map(|d| d.display().to_string())
            .unwrap_or_else(|| "synthetic".to_string()),
        corpus.len()
    );

    std::fs::create_dir_all(&db_dir).expect("create store directory");
    let storage = RocksDB::open(&db_dir).expect("open RocksDB storage");
    let lix: Lix<RocksDB> = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open lix workspace");

    // Import, batched by payload bytes so one statement stays bounded.
    const MAX_BATCH_BYTES: usize = 32 * 1024 * 1024;
    let mut cursor = 0_usize;
    while cursor < corpus.len() {
        let mut end = cursor;
        let mut batch_bytes = 0_usize;
        while end < corpus.len()
            && (end == cursor || batch_bytes + corpus[end].bytes.len() <= MAX_BATCH_BYTES)
        {
            batch_bytes += corpus[end].bytes.len();
            end += 1;
        }
        let mut sql = String::from("INSERT INTO lix_file (path, content) VALUES ");
        let mut params: Vec<Value> = Vec::new();
        for index in cursor..end {
            if index != cursor {
                sql.push(',');
            }
            let slot = (index - cursor) * 2;
            sql.push_str(&format!("(${},${})", slot + 1, slot + 2));
            params.push(Value::Text(corpus[index].path.clone()));
            params.push(Value::Blob(corpus[index].bytes.clone().into()));
        }
        sql.push_str(" ON CONFLICT (path) DO UPDATE SET content = excluded.content");
        lix.execute(&sql, &params).await.expect("import batch");
        cursor = end;
    }

    let after_import = usage(&storage).await;

    // N agent commits: three files each, one byte flipped near the middle.
    for round in 0..rounds {
        let mut sql = String::from("INSERT INTO lix_file (path, content) VALUES ");
        let mut params: Vec<Value> = Vec::new();
        for slot in 0..3 {
            let index = (round * 3 + slot) % corpus.len();
            let mut bytes = corpus[index].bytes.clone();
            if bytes.is_empty() {
                continue;
            }
            let at = (bytes.len() / 2 + round * 7) % bytes.len();
            bytes[at] = bytes[at].wrapping_add(1 + (round as u8 % 7));
            if !params.is_empty() {
                sql.push(',');
            }
            let s = params.len();
            sql.push_str(&format!("(${},${})", s + 1, s + 2));
            params.push(Value::Text(corpus[index].path.clone()));
            params.push(Value::Blob(bytes.into()));
        }
        sql.push_str(" ON CONFLICT (path) DO UPDATE SET content = excluded.content");
        lix.execute(&sql, &params).await.expect("agent commit");
    }

    let after_rounds = usage(&storage).await;

    // Read cost of the same corpus, on the same store, before anything is
    // dropped: the checkout shape (`SELECT path, content FROM lix_file`). Chunk
    // compression is paid back here, so bytes and read latency have to be
    // reported from one run or the trade cannot be judged.
    let read_reps = env_usize("E26_READ_REPS", 5);
    let mut read_ms: Vec<f64> = Vec::new();
    let mut read_bytes = 0_u64;
    for _ in 0..read_reps {
        let started = std::time::Instant::now();
        let result = lix
            .execute("SELECT path, content FROM lix_file", &[])
            .await
            .expect("read every file");
        let mut bytes = 0_u64;
        for row in result.rows() {
            match &row.values()[1] {
                Value::Blob(blob) => bytes += blob.len() as u64,
                other => panic!("content should be a blob, got {other:?}"),
            }
        }
        read_bytes = bytes;
        read_ms.push(started.elapsed().as_secs_f64() * 1000.0);
    }
    read_ms.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
    let read_median = read_ms[read_ms.len() / 2];
    println!(
        "e26 read reps={read_reps} bytes={read_bytes} median_ms={read_median:.3} \
         min_ms={:.3} max_ms={:.3} mib_per_s={:.1} raw=[{}]",
        read_ms.first().copied().unwrap_or(0.0),
        read_ms.last().copied().unwrap_or(0.0),
        (read_bytes as f64 / (1024.0 * 1024.0)) / (read_median / 1000.0),
        read_ms
            .iter()
            .map(|ms| format!("{ms:.3}"))
            .collect::<Vec<_>>()
            .join(" ")
    );

    drop(lix);

    // Three on-disk figures, because they answer different questions and
    // claim-2 quoted only the first. A "clean close" leaves every memtable
    // resident, so the WAL still holds a complete second copy of everything not
    // yet flushed — that is a durability artifact of when the measurement was
    // taken, not a property of the layout.
    let on_disk_closed = directory_bytes(&db_dir);
    let wal_closed = extension_bytes(&db_dir, "log");
    storage.flush().expect("flush column families");
    let on_disk_flushed = directory_bytes(&db_dir);
    let wal_flushed = extension_bytes(&db_dir, "log");
    drop(storage);
    let on_disk = on_disk_flushed;

    let mut spaces: Vec<(u32, &'static str)> = after_import.keys().copied().collect();
    spaces.extend(after_rounds.keys().copied());
    spaces.sort_unstable();
    spaces.dedup();

    let logical_import: u64 = after_import.values().map(|u| u.bytes()).sum();
    let logical_rounds: u64 = after_rounds.values().map(|u| u.bytes()).sum();

    println!(
        "\n{:<10} {:<50} {:>10} {:>14} {:>14} {:>13} {:>8} {:>8}",
        "space_id", "space", "rows_n", "bytes_import", "bytes_after_n", "per_commit", "%import", "%total"
    );
    let mut rows: Vec<_> = spaces
        .iter()
        .map(|key| {
            let one = after_import.get(key).copied().unwrap_or_default();
            let many = after_rounds.get(key).copied().unwrap_or_default();
            (*key, one, many)
        })
        .filter(|(_, one, many)| one.bytes() > 0 || many.bytes() > 0)
        .collect();
    rows.sort_by_key(|(_, _, many)| std::cmp::Reverse(many.bytes()));
    for ((id, name), one, many) in &rows {
        let per_commit = if rounds > 0 {
            (many.bytes().saturating_sub(one.bytes())) as f64 / rounds as f64
        } else {
            0.0
        };
        println!(
            "0x{id:08x} {name:<50} {:>10} {:>14} {:>14} {per_commit:>13.1} {:>7.2}% {:>7.2}%",
            many.rows,
            one.bytes(),
            many.bytes(),
            one.bytes() as f64 / logical_import.max(1) as f64 * 100.0,
            many.bytes() as f64 / logical_rounds.max(1) as f64 * 100.0,
        );
    }

    println!(
        "\ne26 totals raw_corpus_bytes={raw_bytes} logical_after_import={logical_import} \
         logical_after_{rounds}_rounds={logical_rounds} on_disk_bytes={on_disk}"
    );
    println!(
        "e26 ondisk closed={on_disk_closed} wal_closed={wal_closed} \
         flushed={on_disk_flushed} wal_flushed={wal_flushed} \
         closed_over_raw={:.3} flushed_over_raw={:.3} wal_share_of_closed={:.1}%",
        on_disk_closed as f64 / raw_bytes.max(1) as f64,
        on_disk_flushed as f64 / raw_bytes.max(1) as f64,
        wal_closed as f64 / on_disk_closed.max(1) as f64 * 100.0,
    );
    println!(
        "e26 ratios logical_import_over_raw={:.3} logical_after_over_raw={:.3} \
         on_disk_over_raw={:.3} growth_per_commit_bytes={:.1}",
        logical_import as f64 / raw_bytes.max(1) as f64,
        logical_rounds as f64 / raw_bytes.max(1) as f64,
        on_disk as f64 / raw_bytes.max(1) as f64,
        (logical_rounds.saturating_sub(logical_import)) as f64 / rounds.max(1) as f64,
    );
}
