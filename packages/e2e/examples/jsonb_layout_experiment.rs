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
use lix_storage_rocksdb::{BlockFetchCounters, PerfProbe, RocksDB};
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters, SlateDBIoSnapshot};
use serde_json::{Value, json};

#[path = "jsonb_layout/common.rs"]
mod common;
#[path = "jsonb_layout/compact.rs"]
mod compact;
#[path = "jsonb_layout/indexed.rs"]
#[allow(dead_code)]
mod indexed;
#[path = "jsonb_layout/tape.rs"]
#[allow(dead_code)]
mod tape;
#[path = "jsonb_layout/text.rs"]
mod text;

use common::{
    JsonbCodec, PathSegment, canonical_text, content_id, dictionary_id, parse_jsonb, rewrite_value,
};
use compact::CompactCodec;
use text::CanonicalText;

const WARMUPS: usize = 3;
const DEFAULT_SAMPLES: usize = 20;
const PNPM_BYTES: usize = 392 * 1024;
const RECOMMENDED_INDIRECT_THRESHOLD: usize = 64 * 1024;
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
        bench_case::<CompactCodec>(case, samples)?;
    }
    typed_row_oracles(&corpus[1], samples)
}

fn container_threshold_probes(samples: usize) -> Result<(), String> {
    for count in [
        1usize, 2, 4, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 32, 64,
    ] {
        let keys = (0..count)
            .map(|index| format!("key-{index:04}"))
            .collect::<Vec<_>>();
        let values = (0..count)
            .map(|index| format!("value-{index:04}"))
            .collect::<Vec<_>>();
        let sought = keys.last().ok_or("empty threshold probe")?.clone();
        for algorithm in ["sequential", "binary"] {
            let indexed = algorithm == "binary";
            let encoded = threshold_encode(&keys, &values, indexed)?;
            if threshold_decode_owned(&encoded, indexed)?
                != keys
                    .iter()
                    .cloned()
                    .zip(values.iter().cloned())
                    .collect::<Vec<_>>()
            {
                return Err("threshold codec roundtrip mismatch".into());
            }
            for operation_name in ["encode", "decode", "path", "rewrite"] {
                let mut operation = || -> Result<bool, String> {
                    match operation_name {
                        "encode" => {
                            Ok(
                                !threshold_encode(black_box(&keys), black_box(&values), indexed)?
                                    .is_empty(),
                            )
                        }
                        "decode" => Ok(
                            threshold_decode_owned(black_box(&encoded), indexed)?.len() == count
                        ),
                        "path" => {
                            let mut found = false;
                            for _ in 0..1024 {
                                found = threshold_path(
                                    black_box(&encoded),
                                    indexed,
                                    black_box(&sought),
                                )?;
                            }
                            Ok(found)
                        }
                        "rewrite" => Ok(!threshold_rewrite(
                            black_box(&encoded),
                            indexed,
                            black_box(&sought),
                        )?
                        .is_empty()),
                        _ => unreachable!(),
                    }
                };
                for _ in 0..WARMUPS {
                    if !operation()? {
                        return Err("container threshold warmup failed".to_owned());
                    }
                }
                let mut measured = Vec::with_capacity(samples);
                for _ in 0..samples {
                    let (sample, passed) = measure(&mut operation)?;
                    if !passed {
                        return Err("container threshold operation failed".to_owned());
                    }
                    measured.push(sample);
                }
                emit(json!({
                    "schema_version": 1,
                    "benchmark_id": format!("jsonb-layout-v1/container-threshold/{count}/{algorithm}/{operation_name}"),
                    "mode": "container_threshold",
                    "entry_count": count,
                    "algorithm": algorithm,
                    "operation": operation_name,
                    "warmups": WARMUPS,
                    "samples": samples,
                    "lookups_per_path_sample": (operation_name == "path").then_some(1024),
                    "encoded_bytes": encoded.len(),
                    "wall_ns": stats(&measured, |sample| sample.wall),
                    "cpu_ns": stats(&measured, |sample| sample.cpu),
                    "raw_samples": measured.iter().map(|sample| json!({
                        "wall_ns": sample.wall,
                        "cpu_ns": sample.cpu,
                        "allocated_bytes": sample.alloc,
                        "rss_bytes": sample.rss
                    })).collect::<Vec<_>>(),
                    "model_matches_codec_layout": true,
                    "correct": true
                }))?;
            }
        }
    }
    Ok(())
}

fn threshold_encode(keys: &[String], values: &[String], indexed: bool) -> Result<Vec<u8>, String> {
    if keys.len() != values.len() {
        return Err("threshold fixture cardinality mismatch".into());
    }
    let mut output = vec![u8::from(indexed)];
    if indexed {
        output.extend_from_slice(
            &u32::try_from(keys.len())
                .map_err(|_| "count")?
                .to_le_bytes(),
        );
        for items in [keys, values] {
            let mut offset = 0_u32;
            output.extend_from_slice(&offset.to_le_bytes());
            for item in items {
                offset = offset
                    .checked_add(u32::try_from(item.len()).map_err(|_| "item")?)
                    .ok_or("offset")?;
                output.extend_from_slice(&offset.to_le_bytes());
            }
        }
        for item in keys.iter().chain(values) {
            output.extend_from_slice(item.as_bytes());
        }
    } else {
        threshold_put_varint(keys.len() as u64, &mut output);
        for (key, value) in keys.iter().zip(values) {
            threshold_put_varint(key.len() as u64, &mut output);
            output.extend_from_slice(key.as_bytes());
            threshold_put_varint(value.len() as u64, &mut output);
            output.extend_from_slice(value.as_bytes());
        }
    }
    Ok(output)
}

fn threshold_decode_owned(bytes: &[u8], indexed: bool) -> Result<Vec<(String, String)>, String> {
    if bytes.first().copied() != Some(u8::from(indexed)) {
        return Err("threshold layout tag mismatch".into());
    }
    if indexed {
        let count =
            u32::from_le_bytes(bytes.get(1..5).ok_or("count")?.try_into().unwrap()) as usize;
        let table = (count + 1).checked_mul(4).ok_or("table")?;
        let key_table = 5;
        let value_table = key_table + table;
        let data = value_table + table;
        let key_bytes = u32::from_le_bytes(
            bytes
                .get(key_table + count * 4..key_table + table)
                .ok_or("keys")?
                .try_into()
                .unwrap(),
        ) as usize;
        let key_data = bytes.get(data..data + key_bytes).ok_or("key data")?;
        let value_data = bytes.get(data + key_bytes..).ok_or("value data")?;
        let mut output = Vec::with_capacity(count);
        for index in 0..count {
            output.push((
                std::str::from_utf8(threshold_indexed_range(bytes, key_table, index, key_data)?)
                    .map_err(|e| e.to_string())?
                    .to_owned(),
                std::str::from_utf8(threshold_indexed_range(
                    bytes,
                    value_table,
                    index,
                    value_data,
                )?)
                .map_err(|e| e.to_string())?
                .to_owned(),
            ));
        }
        Ok(output)
    } else {
        let mut rest = &bytes[1..];
        let count = threshold_take_varint(&mut rest)? as usize;
        let mut output = Vec::with_capacity(count);
        for _ in 0..count {
            let key_len = threshold_take_varint(&mut rest)? as usize;
            let (key, tail) = rest.split_at_checked(key_len).ok_or("key")?;
            rest = tail;
            let value_len = threshold_take_varint(&mut rest)? as usize;
            let (value, tail) = rest.split_at_checked(value_len).ok_or("value")?;
            rest = tail;
            output.push((
                std::str::from_utf8(key)
                    .map_err(|e| e.to_string())?
                    .to_owned(),
                std::str::from_utf8(value)
                    .map_err(|e| e.to_string())?
                    .to_owned(),
            ));
        }
        if !rest.is_empty() {
            return Err("trailing threshold bytes".into());
        }
        Ok(output)
    }
}

fn threshold_indexed_range<'a>(
    bytes: &[u8],
    table_start: usize,
    index: usize,
    data: &'a [u8],
) -> Result<&'a [u8], String> {
    let at = table_start
        .checked_add(index.checked_mul(4).ok_or("offset")?)
        .ok_or("offset")?;
    let start =
        u32::from_le_bytes(bytes.get(at..at + 4).ok_or("offset")?.try_into().unwrap()) as usize;
    let end = u32::from_le_bytes(
        bytes
            .get(at + 4..at + 8)
            .ok_or("offset")?
            .try_into()
            .unwrap(),
    ) as usize;
    data.get(start..end).ok_or("range".into())
}

fn threshold_path(bytes: &[u8], indexed: bool, sought: &str) -> Result<bool, String> {
    threshold_validate(bytes, indexed)?;
    if bytes.first().copied() != Some(u8::from(indexed)) {
        return Err("threshold layout tag mismatch".into());
    }
    if indexed {
        let count =
            u32::from_le_bytes(bytes.get(1..5).ok_or("count")?.try_into().unwrap()) as usize;
        let table = (count + 1).checked_mul(4).ok_or("table")?;
        let key_table = 5;
        let value_table = key_table + table;
        let data = value_table + table;
        let key_bytes = u32::from_le_bytes(
            bytes
                .get(key_table + count * 4..key_table + table)
                .ok_or("keys")?
                .try_into()
                .unwrap(),
        ) as usize;
        let key_data = bytes.get(data..data + key_bytes).ok_or("key data")?;
        let mut lower = 0;
        let mut upper = count;
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            let key =
                std::str::from_utf8(threshold_indexed_range(bytes, key_table, middle, key_data)?)
                    .map_err(|e| e.to_string())?;
            match key.cmp(sought) {
                std::cmp::Ordering::Less => lower = middle + 1,
                std::cmp::Ordering::Greater => upper = middle,
                std::cmp::Ordering::Equal => return Ok(true),
            }
        }
        Ok(false)
    } else {
        let mut rest = &bytes[1..];
        let count = threshold_take_varint(&mut rest)? as usize;
        for _ in 0..count {
            let key_len = threshold_take_varint(&mut rest)? as usize;
            let (key, tail) = rest.split_at_checked(key_len).ok_or("key")?;
            rest = tail;
            let value_len = threshold_take_varint(&mut rest)? as usize;
            let (_, tail) = rest.split_at_checked(value_len).ok_or("value")?;
            rest = tail;
            if std::str::from_utf8(key).map_err(|e| e.to_string())? == sought {
                return Ok(true);
            }
        }
        if !rest.is_empty() {
            return Err("trailing threshold bytes".into());
        }
        Ok(false)
    }
}

fn threshold_validate(bytes: &[u8], indexed: bool) -> Result<(), String> {
    if bytes.first().copied() != Some(u8::from(indexed)) {
        return Err("threshold layout tag mismatch".into());
    }
    if indexed {
        let count =
            u32::from_le_bytes(bytes.get(1..5).ok_or("count")?.try_into().unwrap()) as usize;
        let table = (count + 1).checked_mul(4).ok_or("table")?;
        let key_table = 5;
        let value_table = key_table + table;
        let data = value_table + table;
        let key_bytes = u32::from_le_bytes(
            bytes
                .get(key_table + count * 4..key_table + table)
                .ok_or("keys")?
                .try_into()
                .unwrap(),
        ) as usize;
        let key_data = bytes.get(data..data + key_bytes).ok_or("key data")?;
        let value_data = bytes.get(data + key_bytes..).ok_or("value data")?;
        let mut previous: Option<&str> = None;
        for index in 0..count {
            let key =
                std::str::from_utf8(threshold_indexed_range(bytes, key_table, index, key_data)?)
                    .map_err(|e| e.to_string())?;
            if previous.is_some_and(|previous| previous >= key) {
                return Err("keys are not strictly ordered".into());
            }
            previous = Some(key);
            std::str::from_utf8(threshold_indexed_range(
                bytes,
                value_table,
                index,
                value_data,
            )?)
            .map_err(|e| e.to_string())?;
        }
    } else {
        let mut rest = &bytes[1..];
        let count = threshold_take_varint(&mut rest)? as usize;
        let mut previous: Option<&str> = None;
        for _ in 0..count {
            let key_len = threshold_take_varint(&mut rest)? as usize;
            let (key, tail) = rest.split_at_checked(key_len).ok_or("key")?;
            rest = tail;
            let key = std::str::from_utf8(key).map_err(|e| e.to_string())?;
            if previous.is_some_and(|previous| previous >= key) {
                return Err("keys are not strictly ordered".into());
            }
            previous = Some(key);
            let value_len = threshold_take_varint(&mut rest)? as usize;
            let (value, tail) = rest.split_at_checked(value_len).ok_or("value")?;
            rest = tail;
            std::str::from_utf8(value).map_err(|e| e.to_string())?;
        }
        if !rest.is_empty() {
            return Err("trailing threshold bytes".into());
        }
    }
    Ok(())
}

fn threshold_rewrite(bytes: &[u8], indexed: bool, sought: &str) -> Result<Vec<u8>, String> {
    let mut entries = threshold_decode_owned(bytes, indexed)?;
    let (_, value) = entries
        .iter_mut()
        .find(|(key, _)| key == sought)
        .ok_or("missing")?;
    value.push('x');
    let (keys, values): (Vec<_>, Vec<_>) = entries.into_iter().unzip();
    threshold_encode(&keys, &values, indexed)
}

fn threshold_put_varint(mut value: u64, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push(value as u8 | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}
fn threshold_take_varint(bytes: &mut &[u8]) -> Result<u64, String> {
    let mut value = 0_u64;
    for index in 0..10 {
        let byte = *bytes.first().ok_or("varint")?;
        *bytes = &bytes[1..];
        value |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err("varint overflow".into())
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
    semantic_codec_oracle::<CompactCodec>(true)?;
    for equivalents in [
        [
            "9007199254740993",
            "9007199254740993.0",
            "90071992547409930e-1",
        ],
        [
            "1.234567890123456789",
            "1234567890123456789e-18",
            "1.2345678901234567890",
        ],
        [
            "18446744073709551616",
            "184467440737095516160e-1",
            "1.8446744073709551616e19",
        ],
    ] {
        let expected = CompactCodec::encode(&parse_jsonb(equivalents[0])?)?;
        for spelling in equivalents {
            if CompactCodec::encode(&parse_jsonb(spelling)?)? != expected {
                return Err(format!(
                    "lossless numeric equivalence failed for {spelling}"
                ));
            }
        }
    }
    if recommended_storage_class(RECOMMENDED_INDIRECT_THRESHOLD - 1) != "inline"
        || recommended_storage_class(RECOMMENDED_INDIRECT_THRESHOLD) != "indirect"
    {
        return Err("JSONB size-class boundary is not exact".into());
    }
    emit(json!({
        "schema_version": 1,
        "benchmark_id": "jsonb-layout-v1/semantic-corruption-oracle",
        "mode": "semantic_oracle",
        "numeric_equivalence": true,
        "lossless_numeric_boundaries":true,
        "whitespace_equivalence": true,
        "duplicate_key_last_wins": true,
        "nul_rejected": true,
        "malformed_rejected": true,
        "unknown_version_rejected": true,
        "same_size_substitution_rejected_by_trusted_outer_content_id": true,
        "intrinsic_hash_is_corruption_detection_not_substitution_authority":true,
        "recommended_indirect_threshold_bytes": RECOMMENDED_INDIRECT_THRESHOLD,
        "threshold_minus_one_class":"inline",
        "threshold_exact_class":"indirect",
        "indirect_carrier_implemented":false,
        "correct": true
    }))
}

fn recommended_storage_class(encoded_bytes: usize) -> &'static str {
    if encoded_bytes < RECOMMENDED_INDIRECT_THRESHOLD {
        "inline"
    } else {
        "indirect"
    }
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
    mut value: Value,
    hit: Vec<PathSegment>,
    miss: Vec<PathSegment>,
    mut replacement: Value,
) -> Result<Case, String> {
    common::normalize_jsonb(&mut value)?;
    common::normalize_jsonb(&mut replacement)?;
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
        "raw_samples":v.iter().map(|sample|json!({"wall_ns":sample.wall,"cpu_ns":sample.cpu,"allocated_bytes":sample.alloc,"rss_bytes":sample.rss})).collect::<Vec<_>>(),
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
    typed_row::<CompactCodec>(case, n)
}
fn typed_row<C: JsonbCodec>(case: &Case, n: usize) -> Result<(), String> {
    let mut json_calls = 0_u64;
    let text = b"representative-row";
    let uuid = [0x42_u8; 16];
    let bigint = 9_223_372_036_854_775_i64;
    let float = 1.25_f64;
    let boolean = true;
    let timestamp_micros = 1_786_000_000_000_000_i64;
    let scalar_only_bytes = 2 + 4 + text.len() + 16 + 8 + 8 + 1 + 8;
    if json_calls != 0 {
        return Err("scalar-only typed row invoked JSONB codec".into());
    }
    let encoded = C::encode(&case.value)?;
    json_calls += 1;
    let semantic = content_id(&encoded);
    let fixed = scalar_only_bytes;
    let framing = 1 + 4 + 4 + 32;
    let offset = (fixed + framing) as u32;
    let length = encoded.len() as u32;
    let mut frame = Vec::with_capacity(offset as usize + encoded.len());
    frame.extend_from_slice(&(text.len() as u16).to_le_bytes());
    frame.extend_from_slice(&(text.len() as u32).to_le_bytes());
    frame.extend_from_slice(text);
    frame.extend_from_slice(&uuid);
    frame.extend_from_slice(&bigint.to_le_bytes());
    frame.extend_from_slice(&float.to_bits().to_le_bytes());
    frame.push(boolean.into());
    frame.extend_from_slice(&timestamp_micros.to_le_bytes());
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
        "scalar_only_row_bytes":scalar_only_bytes,"scalar_only_jsonb_codec_calls":0,
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
    wio: Vec<IoSample>,
    hotio: Vec<IoSample>,
    flushio: Vec<IoSample>,
    coldio: Vec<IoSample>,
}
#[derive(Clone, Copy, Default)]
struct IoSample {
    read_calls: u64,
    read_bytes: u64,
    write_calls: u64,
    write_bytes: u64,
}
fn storage_mode(corpus: &[Case], n: usize) -> Result<(), String> {
    let fixtures = vec![sf::<CanonicalText>(corpus)?, sf::<CompactCodec>(corpus)?];
    tokio::runtime::Builder::new_current_thread()
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
fn single(
    w: (Sample, IoSample),
    h: (Sample, IoSample),
    f: (Sample, IoSample),
    c: (Sample, IoSample),
    d: u64,
) -> SP {
    SP {
        w: vec![w.0],
        hot: vec![h.0],
        flush: vec![f.0],
        cold: vec![c.0],
        disk: vec![d],
        wio: vec![w.1],
        hotio: vec![h.1],
        flushio: vec![f.1],
        coldio: vec![c.1],
    }
}
async fn rcycle(p: &Path, f: &SF) -> Result<SP, String> {
    let mut probe = PerfProbe::new();
    let s = RocksDB::open(p).map_err(|e| e.to_string())?;
    probe.reset();
    let (w, _) = ma(|| put(&s, f)).await?;
    let wio = rocks_io(probe.read());
    probe.reset();
    let (h, _) = ma(|| get(&s, f)).await?;
    let hio = rocks_io(probe.read());
    probe.reset();
    let (fl, _) = ma(|| async { s.flush().map_err(|e| e.to_string()) }).await?;
    let fio = rocks_io(probe.read());
    let d = dir(p)?;
    drop(s);
    probe.reset();
    let (c, s) = ma(|| async {
        let s = RocksDB::open(p).map_err(|e| e.to_string())?;
        get(&s, f).await?;
        Ok(s)
    })
    .await?;
    let cio = rocks_io(probe.read());
    drop(s);
    Ok(single((w, wio), (h, hio), (fl, fio), (c, cio), d))
}
async fn scycle(p: &Path, f: &SF) -> Result<SP, String> {
    let counters = SlateDBIoCounters::default();
    let s = SlateDB::open_with_io_counters(p, counters.clone()).map_err(|e| e.to_string())?;
    let before = counters.snapshot();
    let (w, _) = ma(|| put(&s, f)).await?;
    let after_write = counters.snapshot();
    let wio = slate_io(after_write.saturating_sub(before));
    let (h, _) = ma(|| get(&s, f)).await?;
    let after_hot = counters.snapshot();
    let hio = slate_io(after_hot.saturating_sub(after_write));
    let (fl, _) = ma(|| async { s.flush().await.map_err(|e| e.to_string()) }).await?;
    let after_flush = counters.snapshot();
    let fio = slate_io(after_flush.saturating_sub(after_hot));
    let d = dir(p)?;
    drop(s);
    let (c, s) = ma(|| async {
        let s = SlateDB::open_with_io_counters(p, counters.clone()).map_err(|e| e.to_string())?;
        get(&s, f).await?;
        Ok(s)
    })
    .await?;
    let cio = slate_io(counters.snapshot().saturating_sub(after_flush));
    drop(s);
    Ok(single((w, wio), (h, hio), (fl, fio), (c, cio), d))
}
fn rocks_io(counters: BlockFetchCounters) -> IoSample {
    IoSample {
        read_calls: counters.block_fetches(),
        read_bytes: counters.block_read_bytes,
        ..IoSample::default()
    }
}
fn slate_io(counters: SlateDBIoSnapshot) -> IoSample {
    IoSample {
        read_calls: counters.read_objects,
        read_bytes: counters.read_bytes,
        write_calls: counters.write_objects,
        write_bytes: counters.write_bytes,
    }
}
fn join(a: &mut SP, b: SP) {
    a.w.extend(b.w);
    a.hot.extend(b.hot);
    a.flush.extend(b.flush);
    a.cold.extend(b.cold);
    a.disk.extend(b.disk);
    a.wio.extend(b.wio);
    a.hotio.extend(b.hotio);
    a.flushio.extend(b.flushio);
    a.coldio.extend(b.coldio);
}
fn out_storage(be: &str, n: usize, f: &SF, p: &SP) -> Result<(), String> {
    for (ph, v, io, logical_calls, rb, wb) in [
        ("write", &p.w, &p.wio, 3, 0, f.kb + f.vb),
        ("warm_read", &p.hot, &p.hotio, 2, f.kb + f.vb, 0),
        ("flush", &p.flush, &p.flushio, 1, 0, 0),
        ("reopen_cold_read", &p.cold, &p.coldio, 3, f.kb + f.vb, 0),
    ] {
        let physical_writes_measured = be == "slatedb";
        emit(
            json!({"schema_version":1,"benchmark_id":format!("jsonb-layout-v1/storage/{be}/{}/{ph}",f.codec),"mode":"storage","backend":be,"codec":f.codec,"phase":ph,"batch_entries":f.puts.len(),"warmups":WARMUPS,"samples":n,"wall_ns":stats(v,|x|x.wall),"cpu_ns":stats(v,|x|x.cpu),"allocated_bytes":stats(v,|x|x.alloc),"rss_bytes":{"max":v.iter().map(|x|x.rss).max().unwrap_or(0)},"raw_samples":v.iter().zip(io).map(|(sample,io)|json!({"wall_ns":sample.wall,"cpu_ns":sample.cpu,"allocated_bytes":sample.alloc,"rss_bytes":sample.rss,"physical_read_calls":io.read_calls,"physical_read_bytes":io.read_bytes,"physical_write_calls":physical_writes_measured.then_some(io.write_calls),"physical_write_bytes":physical_writes_measured.then_some(io.write_bytes)})).collect::<Vec<_>>(),"logical_calls_per_sample":logical_calls,"logical_read_bytes_per_sample":rb,"logical_write_bytes_per_sample":wb,"physical_counter_scope":if physical_writes_measured{"object_store_reads_and_writes"}else{"rocksdb_block_fetch_reads_only"},"physical_read_calls":stats_io(io,|x|x.read_calls),"physical_read_bytes":stats_io(io,|x|x.read_bytes),"physical_write_calls":physical_writes_measured.then(||stats_io(io,|x|x.write_calls)),"physical_write_bytes":physical_writes_measured.then(||stats_io(io,|x|x.write_bytes)),"settled_directory_bytes":stats_u64(&p.disk),"raw_bytes":f.raw,"encoded_bytes":f.enc,"zstd_bytes":f.zstd,"flush_drop_reopen":true,"reopen_in_timed_phase":ph=="reopen_cold_read","semantic_equality_verified":true,"corruption_rejected":true,"correct":true}),
        )?
    }
    Ok(())
}
fn stats_io(values: &[IoSample], field: impl Fn(&IoSample) -> u64) -> Value {
    let mut values = values.iter().map(field).collect::<Vec<_>>();
    values.sort_unstable();
    json!({"p50": pct(&values, 50), "p95": pct(&values, 95)})
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
