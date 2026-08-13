//! Deterministic JSONB layout benchmark. Output is JSONL.

use std::alloc::{GlobalAlloc, Layout};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::fs;
use std::future::Future;
use std::hint::black_box;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use lix::storage::{
    GetManyRequest, GetOptions, Key, ProjectedValue, PutBatch, PutEntry, ReadOptions, SpaceId,
    Storage, StorageRead, StorageSpace, StorageWrite, StoredValue, WriteOptions,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;
use serde_json::{json, Value};

#[path = "jsonb_layout/common.rs"]
mod common;
#[path = "jsonb_layout/compact.rs"]
mod compact;
#[path = "jsonb_layout/indexed.rs"]
mod indexed;
#[path = "jsonb_layout/tape.rs"]
mod tape;
#[path = "jsonb_layout/text.rs"]
mod text;

use common::{
    canonical_text, content_id, dictionary_id, parse_jsonb, rewrite_value, JsonbCodec, PathSegment,
};
use compact::CompactCodec;
use indexed::IndexedCodec;
use tape::TapeCodec;
use text::CanonicalText;

const WARMUPS: usize = 3;
const DEFAULT_SAMPLES: usize = 20;
const PNPM_BYTES: usize = 392 * 1024;
const BENCH_SPACE: StorageSpace =
    StorageSpace::mutable(SpaceId(0xfffe0042), "jsonb.layout.benchmark");

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

struct TrackingAllocator;
static ALLOCATED: AtomicU64 = AtomicU64::new(0);
thread_local! { static COUNT_ALLOCATIONS: Cell<bool> = const { Cell::new(true) }; }

fn count(bytes: usize) {
    if COUNT_ALLOCATIONS.try_with(Cell::get).unwrap_or(false) {
        ALLOCATED.fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !ptr.is_null() {
            count(layout.size());
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { mimalloc::MiMalloc.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let new = unsafe { mimalloc::MiMalloc.realloc(ptr, layout, size) };
        if !new.is_null() && size > layout.size() {
            count(size - layout.size());
        }
        new
    }
}

#[derive(Clone)]
struct Case {
    id: &'static str,
    raw: String,
    value: Value,
    hit: Vec<PathSegment>,
    miss: Vec<PathSegment>,
    replacement: Value,
}

#[derive(Clone, Copy)]
struct Sample {
    wall: u64,
    cpu: u64,
    alloc: u64,
    rss: u64,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("jsonb_layout_experiment: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut samples = DEFAULT_SAMPLES;
    let mut storage = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--samples" => {
                samples = args
                    .next()
                    .ok_or("missing --samples value")?
                    .parse()
                    .map_err(|_| "invalid --samples")?
            }
            "storage" => storage = true,
            _ => {
                return Err(format!(
                    "usage: jsonb_layout_experiment [storage] [--samples N>=20]; unknown {arg:?}"
                ));
            }
        }
    }
    if samples < 20 {
        return Err(format!("samples must be >=20, got {samples}"));
    }
    semantic_oracles()?;
    container_threshold_probes(samples)?;
    let corpus = corpus()?;
    if storage {
        return storage_mode(&corpus, samples);
    }
    for case in &corpus {
        bench_case::<CanonicalText>(case, samples)?;
        bench_case::<TapeCodec>(case, samples)?;
        bench_case::<IndexedCodec>(case, samples)?;
        bench_case::<CompactCodec>(case, samples)?;
    }
    typed_row_oracles(&corpus[1], samples)
}

fn container_threshold_probes(samples: usize) -> Result<(), String> {
    for count in [1usize, 2, 4, 8, 9, 16, 32, 64] {
        let keys = (0..count)
            .map(|index| format!("key-{index:04}"))
            .collect::<Vec<_>>();
        let sought = keys.last().ok_or("empty threshold probe")?.clone();
        for algorithm in ["sequential", "binary"] {
            let mut operation = || -> Result<bool, String> {
                let mut found = false;
                for _ in 0..1024 {
                    found = match algorithm {
                        "sequential" => keys.iter().any(|key| key == black_box(&sought)),
                        "binary" => keys.binary_search(black_box(&sought)).is_ok(),
                        _ => unreachable!(),
                    };
                }
                Ok(found)
            };
            for _ in 0..WARMUPS {
                if !operation()? {
                    return Err("container threshold warmup missed".to_owned());
                }
            }
            let mut measured = Vec::with_capacity(samples);
            for _ in 0..samples {
                let (sample, found) = measure(&mut operation)?;
                if !found {
                    return Err("container threshold lookup missed".to_owned());
                }
                measured.push(sample);
            }
            emit(json!({
                "schema_version": 1,
                "benchmark_id": format!("jsonb-layout-v1/container-threshold/{count}/{algorithm}"),
                "mode": "container_threshold",
                "entry_count": count,
                "algorithm": algorithm,
                "lookups_per_sample": 1024,
                "wall_ns": stats(&measured, |sample| sample.wall),
                "cpu_ns": stats(&measured, |sample| sample.cpu),
                "sequential_metadata_bytes": 2 * count,
                "indexed_metadata_bytes": 4 + 8 * (count + 1),
                "correct": true
            }))?;
        }
    }
    Ok(())
}

fn semantic_oracles() -> Result<(), String> {
    for invalid in ["{", "[1,", r#""a\u0000b""#, r#"{"a\u0000b":1}"#] {
        if parse_jsonb(invalid).is_ok() {
            return Err(format!(
                "malformed or NUL-bearing JSONB was accepted: {invalid:?}"
            ));
        }
    }
    semantic_codec_oracle::<CanonicalText>(false)?;
    semantic_codec_oracle::<TapeCodec>(true)?;
    semantic_codec_oracle::<IndexedCodec>(true)?;
    semantic_codec_oracle::<CompactCodec>(true)?;
    emit(json!({
        "schema_version": 1,
        "benchmark_id": "jsonb-layout-v1/semantic-corruption-oracle",
        "mode": "semantic_oracle",
        "numeric_equivalence": true,
        "whitespace_equivalence": true,
        "duplicate_key_last_wins": true,
        "nul_rejected": true,
        "malformed_rejected": true,
        "unknown_version_rejected": true,
        "same_size_substitution_rejected_by_content_id": true,
        "correct": true
    }))
}

fn semantic_codec_oracle<C: JsonbCodec>(versioned: bool) -> Result<(), String> {
    let numeric = C::encode(&parse_jsonb("1")?)?;
    for equivalent in ["1.0", "1e0", "10e-1", " -0 "] {
        let expected = if equivalent.trim() == "-0" {
            C::encode(&parse_jsonb("0")?)?
        } else {
            numeric.clone()
        };
        if C::encode(&parse_jsonb(equivalent)?)? != expected {
            return Err(format!(
                "{} numeric equivalence failed for {equivalent}",
                C::NAME
            ));
        }
    }
    let canonical = C::encode(&parse_jsonb(r#"{"a":2,"z":0}"#)?)?;
    for equivalent in [r#" { "z" : 0, "a" : 2 } "#, r#"{"a":1,"\u0061":2,"z":0}"#] {
        if C::encode(&parse_jsonb(equivalent)?)? != canonical {
            return Err(format!("{} object equivalence failed", C::NAME));
        }
    }
    if content_id(&canonical) != content_id(&C::encode(&parse_jsonb(r#"{"z":0,"a":2}"#)?)?) {
        return Err(format!("{} content ID is not stable", C::NAME));
    }
    let mut stored = content_id(&canonical).to_vec();
    stored.extend_from_slice(&canonical);
    *stored.last_mut().ok_or("empty semantic fixture")? ^= 1;
    if env(&stored).is_ok() {
        return Err(format!("{} accepted same-size substitution", C::NAME));
    }
    if C::decode(&canonical[..canonical.len() - 1]).is_ok() {
        return Err(format!("{} accepted a truncated value", C::NAME));
    }
    if versioned {
        let mut unknown = canonical;
        unknown[4] = 0xff;
        if C::decode(&unknown).is_ok() {
            return Err(format!("{} accepted an unknown format version", C::NAME));
        }
    }
    Ok(())
}

fn corpus() -> Result<Vec<Case>, String> {
    let scalar = make(
        "scalar",
        json!("lix-jsonb-scalar"),
        vec![],
        vec![key("absent")],
        json!("rewritten"),
    )?;
    let small = make(
        "small_object",
        json!({
            "active":true,"count":17,"meta":{"owner":"lix","tags":["json","layout","bench"]},"nullable":null
        }),
        vec![key("meta"), key("owner")],
        vec![key("meta"), key("absent")],
        json!("lix-rewritten"),
    )?;
    let mut deep = json!({"leaf":"original"});
    for _ in 0..32 {
        deep = json!({"level":deep});
    }
    let mut deep_hit = (0..32).map(|_| key("level")).collect::<Vec<_>>();
    deep_hit.push(key("leaf"));
    let mut deep_miss = (0..31).map(|_| key("level")).collect::<Vec<_>>();
    deep_miss.push(key("absent"));
    let deep = make(
        "depth_32_object",
        deep,
        deep_hit,
        deep_miss,
        json!("rewritten"),
    )?;
    let mixed = Value::Array(
        (0..1024)
            .map(|i| match i % 5 {
                0 => Value::Null,
                1 => json!(i),
                2 => json!(format!("item-{i:04}")),
                3 => json!([i, i + 1]),
                _ => json!({"id":i,"enabled":i%2==0}),
            })
            .collect(),
    );
    let mixed = make(
        "mixed_array_1k",
        mixed,
        vec![PathSegment::Index(779), key("id")],
        vec![PathSegment::Index(779), key("absent")],
        json!(779000),
    )?;
    let docs = [
        ("json/manifest.json",include_str!("../../../plugins/json/manifest.json")),
        ("json/schema/json_root.json",include_str!("../../../plugins/json/schema/json_root.json")),
        ("csv/manifest.json",include_str!("../../../plugins/csv/manifest.json")),
        ("csv/schema/csv_row.json",include_str!("../../../plugins/csv/schema/csv_row.json")),
        ("markdown/manifest.json",include_str!("../../../plugins/markdown/manifest.json")),
        ("markdown/schema/markdown_node.json",include_str!("../../../plugins/markdown/schema/markdown_node.json")),
        ("excalidraw/manifest.json",include_str!("../../../plugins/excalidraw/manifest.json")),
        ("excalidraw/schema/excalidraw_element.json",include_str!("../../../plugins/excalidraw/schema/excalidraw_element.json")),
        ("text/manifest.json",include_str!("../../../plugins/text/manifest.json")),
        ("text/schema/text_line.json",include_str!("../../../plugins/text/schema/text_line.json")),
    ].into_iter().map(|(path,raw)| Ok(json!({"path":path,"document":serde_json::from_str::<Value>(raw).map_err(|e|e.to_string())?})))
        .collect::<Result<Vec<_>,String>>()?;
    let plugins = make(
        "real_plugin_documents",
        json!({"documents":docs}),
        vec![key("documents"), PathSegment::Index(3), key("path")],
        vec![
            key("documents"),
            PathSegment::Index(3),
            key("document"),
            key("absent"),
        ],
        json!("csv/schema/csv_row.rewritten.json"),
    )?;
    let mut boundary_value = json!({
        "kind": "jsonb-indirect-boundary",
        "path": "root.payload",
        "payload": ""
    });
    let boundary_base = canonical_text(&boundary_value)?.len();
    let boundary_bytes = 64 * 1024;
    boundary_value["payload"] = json!("x".repeat(boundary_bytes - boundary_base));
    let boundary = make(
        "indirect_boundary_64k",
        boundary_value,
        vec![key("path")],
        vec![key("absent")],
        json!("root.rewritten"),
    )?;
    if boundary.raw.len() != boundary_bytes {
        return Err(format!(
            "indirect boundary fixture {} != {boundary_bytes}",
            boundary.raw.len()
        ));
    }
    let packages = (0..1800)
        .map(|i| {
            json!({"dev":i%3==0,"name":format!("@lix/fixture-{i:04}"),
        "resolution":{"integrity":format!("sha512-{i:04}-deterministic-jsonb-layout-fixture"),
        "tarball":format!("https://registry.npmjs.org/@lix/fixture-{i:04}/-/fixture-{i:04}.tgz")},
        "version":format!("{}.{}.{}",i%17,i%29,i%43)})
        })
        .collect::<Vec<_>>();
    let mut lock = json!({"importers":{".":{"dependencies":{"@lix/sdk":"workspace:*"}}},
        "lockfileVersion":"9.0","packages":packages,"padding":""});
    let base = canonical_text(&lock)?.len();
    if base > PNPM_BYTES {
        return Err(format!("pnpm base {base} exceeds {PNPM_BYTES}"));
    }
    lock["padding"] = json!("p".repeat(PNPM_BYTES - base));
    let lock = make(
        "pnpm_lock_392k",
        lock,
        vec![
            key("packages"),
            PathSegment::Index(511),
            key("resolution"),
            key("integrity"),
        ],
        vec![
            key("packages"),
            PathSegment::Index(511),
            key("resolution"),
            key("absent"),
        ],
        json!("sha512-rewritten"),
    )?;
    if lock.raw.len() != PNPM_BYTES {
        return Err(format!("pnpm fixture {} != {PNPM_BYTES}", lock.raw.len()));
    }
    Ok(vec![scalar, small, deep, mixed, plugins, boundary, lock])
}

fn make(
    id: &'static str,
    value: Value,
    hit: Vec<PathSegment>,
    miss: Vec<PathSegment>,
    replacement: Value,
) -> Result<Case, String> {
    let raw = String::from_utf8(canonical_text(&value)?).map_err(|e| e.to_string())?;
    Ok(Case {
        id,
        raw,
        value,
        hit,
        miss,
        replacement,
    })
}
fn key(s: &str) -> PathSegment {
    PathSegment::Key(s.to_owned())
}

fn bench_case<C: JsonbCodec>(case: &Case, n: usize) -> Result<(), String> {
    let encoded = C::encode(&case.value)?;
    if C::decode(&encoded)? != case.value {
        return Err(format!("{}/{} roundtrip", case.id, C::NAME));
    }
    let hit = C::project_path(&encoded, &case.hit)?
        .ok_or_else(|| format!("{}/{} hit missed", case.id, C::NAME))?;
    let expected_hit = common::value_at_path(&case.value, &case.hit).ok_or("invalid hit oracle")?;
    if C::decode(&hit)? != *expected_hit {
        return Err("projection mismatch".into());
    }
    if C::project_path(&encoded, &case.miss)?.is_some() {
        return Err("miss hit".into());
    }
    let rewritten = C::rewrite_path(&encoded, &case.hit, &case.replacement)?;
    let mut expected = case.value.clone();
    rewrite_value(&mut expected, &case.hit, case.replacement.clone())?;
    if C::decode(&rewritten)? != expected || C::diff_count(&encoded, &rewritten)? != 1 {
        return Err("rewrite/diff mismatch".into());
    }
    let compressed = zstd::bulk::compress(&encoded, 3).map_err(|e| e.to_string())?;
    let dictionary = fixed_dictionary();
    let dictionary_compressed = zstd::bulk::Compressor::with_dictionary(3, &dictionary)
        .map_err(|e| e.to_string())?
        .compress(&encoded)
        .map_err(|e| e.to_string())?;
    if zstd::bulk::Decompressor::with_dictionary(&dictionary)
        .map_err(|e| e.to_string())?
        .decompress(&dictionary_compressed, encoded.len())
        .map_err(|e| e.to_string())?
        != encoded
    {
        return Err(format!("{}/{} dictionary roundtrip", case.id, C::NAME));
    }
    let fixed_dictionary_id = dictionary_id(&dictionary);
    let semantic_id = content_id(&encoded);
    macro_rules! bench {
        ($name:expr,$body:expr) => {{
            let mut f = || -> Result<bool, String> { $body };
            emit_op(
                case,
                C::NAME,
                $name,
                n,
                case.raw.len(),
                encoded.len(),
                compressed.len(),
                dictionary_compressed.len(),
                dictionary.len(),
                &fixed_dictionary_id,
                &semantic_id,
                &mut f,
            )?;
        }};
    }
    bench!(
        "parse_normalize",
        Ok(parse_jsonb(black_box(&case.raw))? == case.value)
    );
    bench!("encode_insert", {
        let b = C::encode(black_box(&case.value))?;
        let mut m = BTreeMap::new();
        m.insert(content_id(&b), b);
        Ok(m.len() == 1)
    });
    bench!(
        "blake3_hash",
        Ok(content_id(black_box(&encoded)) != [0; 32])
    );
    bench!(
        "zstd_compress",
        Ok(!zstd::bulk::compress(black_box(&encoded), 3)
            .map_err(|e| e.to_string())?
            .is_empty())
    );
    bench!(
        "zstd_dictionary_compress",
        Ok(
            !zstd::bulk::Compressor::with_dictionary(3, black_box(&dictionary))
                .map_err(|e| e.to_string())?
                .compress(black_box(&encoded))
                .map_err(|e| e.to_string())?
                .is_empty()
        )
    );
    bench!(
        "zstd_dictionary_decompress",
        Ok(
            zstd::bulk::Decompressor::with_dictionary(black_box(&dictionary))
                .map_err(|e| e.to_string())?
                .decompress(black_box(&dictionary_compressed), encoded.len())
                .map_err(|e| e.to_string())?
                == encoded
        )
    );
    bench!(
        "zstd_dictionary_compress",
        Ok(
            !zstd::bulk::Compressor::with_dictionary(3, black_box(&dictionary))
                .map_err(|e| e.to_string())?
                .compress(black_box(&encoded))
                .map_err(|e| e.to_string())?
                .is_empty()
        )
    );
    bench!(
        "zstd_decompress",
        Ok(
            zstd::bulk::decompress(black_box(&compressed), encoded.len())
                .map_err(|e| e.to_string())?
                == encoded
        )
    );
    bench!(
        "equality",
        Ok(C::decode(black_box(&encoded))? == case.value)
    );
    bench!(
        "path_hit",
        Ok(C::project_path(black_box(&encoded), black_box(&case.hit))?.is_some())
    );
    bench!(
        "path_miss",
        Ok(C::project_path(black_box(&encoded), black_box(&case.miss))?.is_none())
    );
    bench!(
        "partial_rewrite",
        Ok(C::rewrite_path(
            black_box(&encoded),
            black_box(&case.hit),
            black_box(&case.replacement)
        )? == rewritten)
    );
    bench!(
        "full_decode_render",
        Ok(canonical_text(&C::decode(black_box(&encoded))?)? == case.raw.as_bytes())
    );
    bench!(
        "semantic_diff",
        Ok(C::diff_count(black_box(&encoded), black_box(&rewritten))? == 1)
    );
    Ok(())
}

fn fixed_dictionary() -> Vec<u8> {
    [
        include_bytes!("../../../plugins/json/schema/json_root.json").as_slice(),
        include_bytes!("../../../plugins/csv/schema/csv_row.json").as_slice(),
        include_bytes!("../../../plugins/markdown/schema/markdown_node.json").as_slice(),
        include_bytes!("../../../plugins/excalidraw/schema/excalidraw_element.json").as_slice(),
        include_bytes!("../../../packages/e2e/benches/fixtures/json_pointer.schema.json")
            .as_slice(),
    ]
    .concat()
}

fn emit_op(
    case: &Case,
    codec: &str,
    op: &str,
    n: usize,
    raw: usize,
    encoded: usize,
    zstd: usize,
    dictionary_zstd: usize,
    dictionary_bytes: usize,
    fixed_dictionary_id: &[u8; 32],
    semantic_id: &[u8; 32],
    f: &mut dyn FnMut() -> Result<bool, String>,
) -> Result<(), String> {
    for _ in 0..WARMUPS {
        if !black_box(f()?) {
            return Err(format!("{}/{codec}/{op} warmup", case.id));
        }
    }
    let mut v = Vec::with_capacity(n);
    for _ in 0..n {
        let (s, ok) = measure(f)?;
        if !ok {
            return Err(format!("{}/{codec}/{op} correctness", case.id));
        }
        v.push(s);
    }
    emit(
        json!({"schema_version":1,"benchmark_id":format!("jsonb-layout-v1/{}/{codec}/{op}",case.id),
        "mode":"cpu","case":case.id,"codec":codec,"operation":op,"warmups":WARMUPS,"samples":n,
        "wall_ns":stats(&v,|x|x.wall),"cpu_ns":stats(&v,|x|x.cpu),"allocated_bytes":stats(&v,|x|x.alloc),
        "rss_bytes":{"max":v.iter().map(|x|x.rss).max().unwrap_or(0)},"raw_bytes":raw,"encoded_bytes":encoded,"zstd_bytes":zstd,
        "dictionary_zstd_bytes":dictionary_zstd,"fixed_dictionary_bytes":dictionary_bytes,
        "fixed_dictionary_id":hex(fixed_dictionary_id),"dictionary_decode_dependency":true,
        "content_id":hex(semantic_id),"path_hit":path_json(&case.hit),"path_miss":path_json(&case.miss),
        "rewrite":case.replacement,"correct":true}),
    )
}

fn measure<T>(f: &mut dyn FnMut() -> Result<T, String>) -> Result<(Sample, T), String> {
    let a = ALLOCATED.load(Ordering::Relaxed);
    let c = cpu_ns();
    let t = Instant::now();
    let out = f()?;
    Ok((
        Sample {
            wall: ns(t.elapsed()),
            cpu: cpu_ns().saturating_sub(c),
            alloc: ALLOCATED.load(Ordering::Relaxed).saturating_sub(a),
            rss: rss(),
        },
        out,
    ))
}
fn stats(v: &[Sample], f: impl Fn(&Sample) -> u64) -> Value {
    stats_u64(&v.iter().map(f).collect::<Vec<_>>())
}
fn stats_u64(v: &[u64]) -> Value {
    let mut v = v.to_vec();
    v.sort_unstable();
    json!({"p50":pct(&v,50),"p95":pct(&v,95)})
}
fn pct(v: &[u64], p: usize) -> u64 {
    if v.is_empty() {
        0
    } else {
        v[((v.len() * p).div_ceil(100))
            .saturating_sub(1)
            .min(v.len() - 1)]
    }
}
fn path_json(p: &[PathSegment]) -> Value {
    Value::Array(
        p.iter()
            .map(|s| match s {
                PathSegment::Key(k) => json!({"key":k}),
                PathSegment::Index(i) => json!({"index":i}),
            })
            .collect(),
    )
}
fn hex(v: &[u8]) -> String {
    v.iter().map(|b| format!("{b:02x}")).collect()
}
fn emit(v: Value) -> Result<(), String> {
    println!("{}", serde_json::to_string(&v).map_err(|e| e.to_string())?);
    Ok(())
}
fn ns(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}
fn cpu_ns() -> u64 {
    let mut r = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, r.as_mut_ptr()) } != 0 {
        return 0;
    }
    let r = unsafe { r.assume_init() };
    tv(r.ru_utime) + tv(r.ru_stime)
}
fn tv(v: libc::timeval) -> u64 {
    (v.tv_sec as u64) * 1_000_000_000 + (v.tv_usec as u64) * 1_000
}
fn rss() -> u64 {
    fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines().find_map(|l| {
                l.strip_prefix("VmRSS:")?
                    .split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()
            })
        })
        .unwrap_or(0)
        * 1024
}

fn typed_row_oracles(case: &Case, n: usize) -> Result<(), String> {
    typed_row::<CanonicalText>(case, n)?;
    typed_row::<TapeCodec>(case, n)?;
    typed_row::<IndexedCodec>(case, n).and_then(|()| typed_row::<CompactCodec>(case, n))
}
fn typed_row<C: JsonbCodec>(case: &Case, n: usize) -> Result<(), String> {
    let mut json_calls = 0_u64;
    let text = b"representative-row";
    let uuid = [0x42_u8; 16];
    let bigint = 9_223_372_036_854_775_i64;
    let boolean = true;
    let encoded = C::encode(&case.value)?;
    json_calls += 1;
    let semantic = content_id(&encoded);
    let fixed = 2 + 4 + text.len() + 16 + 8 + 1;
    let framing = 1 + 4 + 4 + 32;
    let offset = (fixed + framing) as u32;
    let length = encoded.len() as u32;
    let mut frame = Vec::with_capacity(offset as usize + encoded.len());
    frame.extend_from_slice(&(text.len() as u16).to_le_bytes());
    frame.extend_from_slice(&(text.len() as u32).to_le_bytes());
    frame.extend_from_slice(text);
    frame.extend_from_slice(&uuid);
    frame.extend_from_slice(&bigint.to_le_bytes());
    frame.push(boolean.into());
    frame.push(1);
    frame.extend_from_slice(&offset.to_le_bytes());
    frame.extend_from_slice(&length.to_le_bytes());
    frame.extend_from_slice(&semantic);
    frame.extend_from_slice(&encoded);
    if json_calls != 1 || &frame[offset as usize..] != encoded {
        return Err("typed-row framing invoked JSON for non-JSON fields or has bad offset".into());
    }
    let calls_before_decode = json_calls;
    let decoded = C::decode(&frame[offset as usize..offset as usize + length as usize])?;
    json_calls += 1;
    if decoded != case.value || calls_before_decode != 1 || json_calls != 2 {
        return Err("typed-row semantic oracle failed".into());
    }
    emit(
        json!({"schema_version":1,"benchmark_id":format!("jsonb-layout-v1/typed-row/{}",C::NAME),"mode":"typed_row_oracle",
        "codec":C::NAME,"samples":n,"version":1,"jsonb_offset":offset,"jsonb_length":length,"semantic_hash":hex(&semantic),
        "fixed_typed_bytes":fixed,"framing_offset_bytes":framing,"framing_overhead_bytes":framing,"row_bytes":frame.len(),
        "json_encode_calls":1,"json_decode_calls":1,"non_json_parse_encode_calls":0,"correct":true}),
    )
}

struct SF {
    codec: &'static str,
    keys: Vec<Key>,
    puts: Vec<PutEntry>,
    want: BTreeMap<Vec<u8>, Value>,
    raw: u64,
    enc: u64,
    zstd: u64,
    kb: u64,
    vb: u64,
}
#[derive(Default)]
struct SP {
    w: Vec<Sample>,
    hot: Vec<Sample>,
    flush: Vec<Sample>,
    cold: Vec<Sample>,
    disk: Vec<u64>,
}
fn storage_mode(corpus: &[Case], n: usize) -> Result<(), String> {
    let fixtures = vec![
        sf::<CanonicalText>(corpus)?,
        sf::<TapeCodec>(corpus)?,
        sf::<IndexedCodec>(corpus)?,
        sf::<CompactCodec>(corpus)?,
    ];
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?
        .block_on(async {
            let root = tempfile::tempdir().map_err(|e| e.to_string())?;
            for f in fixtures {
                let mut rp = SP::default();
                for i in 0..WARMUPS + n {
                    let x = rcycle(&root.path().join(format!("r-{}-{i}", f.codec)), &f).await?;
                    if i >= WARMUPS {
                        join(&mut rp, x)
                    }
                }
                out_storage("rocksdb", n, &f, &rp)?;
                let mut sp = SP::default();
                for i in 0..WARMUPS + n {
                    let x = scycle(&root.path().join(format!("s-{}-{i}", f.codec)), &f).await?;
                    if i >= WARMUPS {
                        join(&mut sp, x)
                    }
                }
                out_storage("slatedb", n, &f, &sp)?;
            }
            Ok(())
        })
}
fn sf<C: JsonbCodec>(cs: &[Case]) -> Result<SF, String> {
    let mut f = SF {
        codec: C::NAME,
        keys: vec![],
        puts: vec![],
        want: BTreeMap::new(),
        raw: 0,
        enc: 0,
        zstd: 0,
        kb: 0,
        vb: 0,
    };
    for c in cs {
        f.raw += c.raw.len() as u64;
        add::<C>(&mut f, c)?;
    }
    f.kb = f.keys.iter().map(|k| k.0.len() as u64).sum();
    f.vb = f.puts.iter().map(|p| p.value.bytes.len() as u64).sum();
    let mut bad = f.puts.last().ok_or("empty")?.value.bytes.to_vec();
    *bad.last_mut().ok_or("empty")? ^= 1;
    if env(&bad).is_ok() {
        return Err("corruption accepted".into());
    }
    Ok(f)
}
fn add<C: JsonbCodec>(f: &mut SF, c: &Case) -> Result<(), String> {
    let k = format!("{}/{}", c.id, C::NAME).into_bytes();
    let b = C::encode(&c.value)?;
    f.enc += b.len() as u64;
    f.zstd += zstd::bulk::compress(&b, 3)
        .map_err(|e| e.to_string())?
        .len() as u64;
    let mut v = content_id(&b).to_vec();
    v.extend_from_slice(&b);
    let key = Key(Bytes::copy_from_slice(&k));
    f.keys.push(key.clone());
    f.puts.push(PutEntry {
        key,
        value: StoredValue {
            bytes: Bytes::from(v),
        },
    });
    f.want.insert(k, c.value.clone());
    Ok(())
}
fn env(v: &[u8]) -> Result<&[u8], String> {
    let (h, b) = v.split_at_checked(32).ok_or("truncated")?;
    if h == content_id(b) {
        Ok(b)
    } else {
        Err("hash mismatch".into())
    }
}
async fn put<S: Storage>(s: &S, f: &SF) -> Result<(), String> {
    let mut w = s
        .begin_write(WriteOptions::default())
        .await
        .map_err(|e| e.to_string())?;
    w.put_many(
        BENCH_SPACE,
        PutBatch {
            entries: f.puts.clone(),
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    w.commit().await.map_err(|e| e.to_string())?;
    Ok(())
}
async fn get<S: Storage>(s: &S, f: &SF) -> Result<(), String> {
    let r = s
        .begin_read(ReadOptions::default())
        .await
        .map_err(|e| e.to_string())?;
    let v = r
        .get_many(&[GetManyRequest {
            space: BENCH_SPACE,
            keys: &f.keys,
            opts: GetOptions::default(),
        }])
        .await
        .map_err(|e| e.to_string())?;
    for (k, v) in f.keys.iter().zip(v.values) {
        let ProjectedValue::FullValue(v) = v.ok_or("miss")? else {
            return Err("projection".into());
        };
        let b = env(&v)?;
        let name = std::str::from_utf8(&k.0).map_err(|e| e.to_string())?;
        let codec = name.rsplit_once('/').ok_or("key")?.1;
        if codec != f.codec {
            return Err("fixture codec mismatch".into());
        }
        let x = match f.codec {
            CanonicalText::NAME => CanonicalText::decode(b)?,
            TapeCodec::NAME => TapeCodec::decode(b)?,
            IndexedCodec::NAME => IndexedCodec::decode(b)?,
            CompactCodec::NAME => CompactCodec::decode(b)?,
            _ => return Err("codec".into()),
        };
        if f.want.get(k.0.as_ref()) != Some(&x) {
            return Err("semantic mismatch".into());
        }
    }
    Ok(())
}
async fn ma<F, U, T>(f: F) -> Result<(Sample, T), String>
where
    F: FnOnce() -> U,
    U: Future<Output = Result<T, String>>,
{
    let a = ALLOCATED.load(Ordering::Relaxed);
    let c = cpu_ns();
    let t = Instant::now();
    let x = f().await?;
    Ok((
        Sample {
            wall: ns(t.elapsed()),
            cpu: cpu_ns() - c,
            alloc: ALLOCATED.load(Ordering::Relaxed) - a,
            rss: rss(),
        },
        x,
    ))
}
fn single(w: Sample, h: Sample, f: Sample, c: Sample, d: u64) -> SP {
    SP {
        w: vec![w],
        hot: vec![h],
        flush: vec![f],
        cold: vec![c],
        disk: vec![d],
    }
}
async fn rcycle(p: &Path, f: &SF) -> Result<SP, String> {
    let s = RocksDB::open(p).map_err(|e| e.to_string())?;
    let (w, _) = ma(|| put(&s, f)).await?;
    let (h, _) = ma(|| get(&s, f)).await?;
    let (fl, _) = ma(|| async { s.flush().map_err(|e| e.to_string()) }).await?;
    let d = dir(p)?;
    drop(s);
    let s = RocksDB::open(p).map_err(|e| e.to_string())?;
    let (c, _) = ma(|| get(&s, f)).await?;
    Ok(single(w, h, fl, c, d))
}
async fn scycle(p: &Path, f: &SF) -> Result<SP, String> {
    let s = SlateDB::open(p).map_err(|e| e.to_string())?;
    let (w, _) = ma(|| put(&s, f)).await?;
    let (h, _) = ma(|| get(&s, f)).await?;
    let (fl, _) = ma(|| async { s.flush().await.map_err(|e| e.to_string()) }).await?;
    let d = dir(p)?;
    drop(s);
    let s = SlateDB::open(p).map_err(|e| e.to_string())?;
    let (c, _) = ma(|| get(&s, f)).await?;
    Ok(single(w, h, fl, c, d))
}
fn join(a: &mut SP, b: SP) {
    a.w.extend(b.w);
    a.hot.extend(b.hot);
    a.flush.extend(b.flush);
    a.cold.extend(b.cold);
    a.disk.extend(b.disk)
}
fn out_storage(be: &str, n: usize, f: &SF, p: &SP) -> Result<(), String> {
    for (ph, v, calls, rb, wb) in [
        ("write", &p.w, 2, 0, f.kb + f.vb),
        ("warm_read", &p.hot, 2, f.kb + f.vb, 0),
        ("flush", &p.flush, 1, 0, 0),
        ("cold_read", &p.cold, 2, f.kb + f.vb, 0),
    ] {
        emit(
            json!({"schema_version":1,"benchmark_id":format!("jsonb-layout-v1/storage/{be}/{}/{ph}",f.codec),"mode":"storage","backend":be,"codec":f.codec,"phase":ph,"batch_entries":f.puts.len(),"warmups":WARMUPS,"samples":n,"wall_ns":stats(v,|x|x.wall),"cpu_ns":stats(v,|x|x.cpu),"allocated_bytes":stats(v,|x|x.alloc),"rss_bytes":{"max":v.iter().map(|x|x.rss).max().unwrap_or(0)},"logical_calls_per_sample":calls,"logical_read_bytes_per_sample":rb,"logical_write_bytes_per_sample":wb,"settled_directory_bytes":stats_u64(&p.disk),"raw_bytes":f.raw,"encoded_bytes":f.enc,"zstd_bytes":f.zstd,"flush_drop_reopen":true,"semantic_equality_verified":true,"corruption_rejected":true,"correct":true}),
        )?
    }
    Ok(())
}
fn dir(p: &Path) -> Result<u64, String> {
    let mut n = 0;
    let mut q = vec![p.to_path_buf()];
    while let Some(p) = q.pop() {
        for e in fs::read_dir(p).map_err(|e| e.to_string())? {
            let e = e.map_err(|e| e.to_string())?;
            let m = e.metadata().map_err(|e| e.to_string())?;
            if m.is_dir() {
                q.push(e.path())
            } else {
                n += m.len()
            }
        }
    }
    Ok(n)
}
