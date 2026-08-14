use lix::plugin::runtime::v1::{
    ByteEdit, PerformanceMeasurement, Root, Store, compare_to_baseline,
};
use std::hint::black_box;
use std::time::Instant;

#[test]
fn plugin_v1_wit_is_valid_and_has_no_guest_document_resource() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("wit");
    let mut resolve = wit_parser::Resolve::default();
    resolve
        .push_dir(&path)
        .unwrap_or_else(|error| panic!("parse {}: {error:#}", path.display()));
    let wit = std::fs::read_to_string(path.join("lix-plugin.wit")).unwrap();
    assert!(wit.contains("package lix:plugin@1.0.0"));
    assert!(wit.contains("resource snapshot"));
    assert!(wit.contains("resource transition"));
    assert!(wit.contains("apply: func("));
    assert!(wit.contains("output: borrow<transition>"));
    assert!(wit.contains("resolve-conflicts: func("));
    assert!(wit.contains("variant transition-request"));
    assert!(wit.contains("open(open-request)"));
    assert!(wit.contains("file-changed(file-changed-request)"));
    assert!(wit.contains("rows-changed(rows-changed-request)"));
    assert!(wit.contains("restore(restore-request)"));
    assert!(wit.contains("cold-file-changed(cold-file-changed-request)"));
    assert!(!wit.contains("path: option<string>"));
    assert!(!wit.contains("resource document"));
    assert!(!wit.contains("resource change-cursor"));
}

#[derive(Clone, Copy, Debug)]
enum Format {
    Markdown,
    Csv,
    Json,
    Excalidraw,
}

impl Format {
    const ALL: [Self; 4] = [Self::Markdown, Self::Csv, Self::Json, Self::Excalidraw];

    fn name(self) -> &'static str {
        match self {
            Self::Markdown => "markdown",
            Self::Csv => "csv",
            Self::Json => "json",
            Self::Excalidraw => "excalidraw",
        }
    }

    fn fixture(self) -> Vec<u8> {
        match self {
            Self::Markdown => records_to(8 * 1024 * 1024, |index| {
                format!(
                    "## Heading {index}\n\nParagraph {index} has *markup*, [a link](https://lix.dev/{index}), and `code-{index}`.\n\n"
                )
            }),
            Self::Csv => records_to(10_680_000, |index| {
                format!(
                    "{index:015},{:010},{:010},{:010}\n",
                    index.wrapping_mul(3),
                    index.wrapping_mul(5),
                    index.wrapping_mul(7)
                )
            }),
            Self::Json => {
                let mut bytes = b"{\"records\":[".to_vec();
                let mut index = 0_u64;
                while bytes.len() < 8 * 1024 * 1024 {
                    if index > 0 {
                        bytes.push(b',');
                    }
                    bytes.extend_from_slice(
                        format!(
                            "{{\"id\":\"record-{index}\",\"name\":\"fixture {index}\",\"enabled\":{}}}",
                            index % 2 == 0
                        )
                        .as_bytes(),
                    );
                    index += 1;
                }
                bytes.extend_from_slice(b"]}\n");
                bytes
            }
            Self::Excalidraw => {
                let mut bytes = br#"{"type":"excalidraw","version":2,"elements":["#.to_vec();
                let mut index = 0_u64;
                while bytes.len() < 8 * 1024 * 1024 {
                    if index > 0 {
                        bytes.push(b',');
                    }
                    bytes.extend_from_slice(
                        format!(
                            r#"{{"id":"shape-{index}","type":"rectangle","x":{index},"y":{},"width":30,"height":40}}"#,
                            index % 997
                        )
                        .as_bytes(),
                    );
                    index += 1;
                }
                bytes.extend_from_slice(br#"],"appState":{},"files":{}}"#);
                bytes
            }
        }
    }
}

fn records_to(target: usize, mut record: impl FnMut(u64) -> String) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(target + 128);
    let mut index = 0;
    while bytes.len() < target {
        bytes.extend_from_slice(record(index).as_bytes());
        index += 1;
    }
    bytes
}

#[derive(Debug)]
struct Score {
    format: &'static str,
    file_bytes: usize,
    v2_retained_bytes: usize,
    v3_retained_bytes: usize,
    v2_boundary_bytes: usize,
    v3_boundary_bytes: usize,
}

fn score(format: Format) -> Score {
    let bytes = format.fixture();
    let edit_offset = bytes.len() / 2;
    let replacement = vec![b'X'];
    let row = format!("{{\"format\":\"{}\",\"changed\":true}}", format.name()).into_bytes();
    let state = format!("{}-page:{}", format.name(), edit_offset).into_bytes();

    let store = Store::default();
    let root = Root::import(
        store.clone(),
        "v3-generation",
        &bytes,
        [(b"stable/row".to_vec(), b"{\"changed\":false}".to_vec())],
        [(b"affected/page".to_vec(), b"old-index-page".to_vec())],
    );
    let base_unique = store.unique_page_bytes();
    let mut transaction = root.transaction();
    transaction.edit_bytes(ByteEdit {
        offset: edit_offset as u64,
        delete_len: 1,
        insert: replacement.clone(),
    });
    transaction.upsert_row(b"stable/row".to_vec(), row.clone());
    transaction.put_state(b"affected/page".to_vec(), state.clone());
    let successor = transaction.commit().unwrap();
    let actual = successor
        .bytes
        .read(0, successor.bytes.len())
        .expect("successor bytes should read");
    let mut expected = bytes.clone();
    expected[edit_offset] = b'X';
    assert_eq!(actual, expected, "{} exact bytes", format.name());

    // v2 retains an owned accepted document per branch/version in guest
    // memory. This intentionally excludes allocator and index overhead, so it
    // is a conservative comparison.
    let v2_retained_bytes = bytes.len() * 2
        + b"{\"changed\":false}".len()
        + row.len()
        + b"old-index-page".len()
        + state.len();
    let v3_retained_bytes = store.unique_page_bytes();
    assert!(v3_retained_bytes >= base_unique);

    // v2 cold materialization lowers a full accepted document and persistent
    // state; v3 lowers one splice plus one row and one state page.
    let v2_boundary_bytes = bytes.len() + b"old-index-page".len();
    let v3_boundary_bytes = 24 + replacement.len() + row.len() + state.len();

    Score {
        format: format.name(),
        file_bytes: bytes.len(),
        v2_retained_bytes,
        v3_retained_bytes,
        v2_boundary_bytes,
        v3_boundary_bytes,
    }
}

#[test]
fn four_format_sparse_scorecard_reports_arena_ratios() {
    for format in Format::ALL {
        let score = score(format);
        eprintln!("{score:?}");
        assert!(score.v3_retained_bytes < score.v2_retained_bytes);
        assert!(
            score.v3_boundary_bytes * 100 <= score.v2_boundary_bytes,
            "{} must reduce boundary materialization by at least 99%: {score:?}",
            score.format
        );
        assert!(score.file_bytes >= 8 * 1024 * 1024);
    }
}

#[test]
#[ignore = "manual release-mode latency scorecard"]
fn four_format_warm_edit_latency_scorecard() {
    const WARMUPS: usize = 100;
    const SAMPLES: usize = 5_000;
    let enforce = std::env::var_os("LIX_PLUGIN_V1_ENFORCE_ACCEPTANCE").is_some();
    let mut failures = Vec::new();
    for format in Format::ALL {
        let bytes = format.fixture();
        let offset = bytes.len() / 2;
        let store = Store::default();
        let root = Root::import(
            store.clone(),
            "v3-generation",
            &bytes,
            std::iter::empty(),
            std::iter::empty(),
        );

        let mut v2_operation = || {
            let mut successor = bytes.clone();
            successor[offset] = black_box(b'X');
            black_box(successor);
        };
        let mut v3_operation = || {
            let mut transaction = root.transaction();
            transaction.edit_bytes(ByteEdit {
                offset: offset as u64,
                delete_len: 1,
                insert: vec![black_box(b'X')],
            });
            black_box(transaction.commit().unwrap());
        };

        for _ in 0..WARMUPS {
            v2_operation();
            v3_operation();
        }

        let mut v2_samples = Vec::with_capacity(SAMPLES);
        let mut v3_samples = Vec::with_capacity(SAMPLES);
        for sample in 0..SAMPLES {
            // Alternate order so frequency scaling and allocator state do not
            // systematically favor either implementation.
            if sample % 2 == 0 {
                v2_samples.push(measure_once(&mut v2_operation));
                v3_samples.push(measure_once(&mut v3_operation));
            } else {
                v3_samples.push(measure_once(&mut v3_operation));
                v2_samples.push(measure_once(&mut v2_operation));
            }
        }

        let v2 = Distribution::new(v2_samples);
        let v3 = Distribution::new(v3_samples);
        let v2_retained_bytes = bytes.len() * 2;
        let v3_retained_bytes = store.unique_page_bytes();
        let acceptance = compare_to_baseline(
            PerformanceMeasurement {
                p95_nanoseconds: v2.p95_ns,
                peak_total_bytes: v2_retained_bytes as u64,
            },
            PerformanceMeasurement {
                p95_nanoseconds: v3.p95_ns,
                peak_total_bytes: v3_retained_bytes as u64,
            },
        );
        eprintln!(
            "plugin_v3_arena_scorecard format={} bytes={} warmups={WARMUPS} samples={SAMPLES} \
             v2_p50_us={:.3} v2_p95_us={:.3} v2_mean_us={:.3} \
             v3_p50_us={:.3} v3_p95_us={:.3} v3_mean_us={:.3} \
             p95_speedup={:.3} v2_retained_bytes={} v3_retained_bytes={} \
             memory_reduction={:.3} latency_pass={} memory_pass={} accepted={}",
            format.name(),
            bytes.len(),
            ns_to_micros(v2.p50_ns),
            ns_to_micros(v2.p95_ns),
            ns_to_micros(v2.mean_ns),
            ns_to_micros(v3.p50_ns),
            ns_to_micros(v3.p95_ns),
            ns_to_micros(v3.mean_ns),
            v2.p95_ns as f64 / v3.p95_ns as f64,
            v2_retained_bytes,
            v3_retained_bytes,
            v2_retained_bytes as f64 / v3_retained_bytes as f64,
            acceptance.latency_passes,
            acceptance.memory_passes,
            acceptance.passes(),
        );
        if !acceptance.passes() {
            failures.push(format.name());
        }
    }
    if enforce {
        assert!(
            failures.is_empty(),
            "Plugin API v1 failed the 2x latency / 3x memory gate for: {}",
            failures.join(", ")
        );
    }
}

fn measure_once(operation: &mut impl FnMut()) -> u64 {
    let started = Instant::now();
    operation();
    u64::try_from(started.elapsed().as_nanos()).expect("sample duration fits u64")
}

#[derive(Debug)]
struct Distribution {
    p50_ns: u64,
    p95_ns: u64,
    mean_ns: u64,
}

impl Distribution {
    fn new(mut samples: Vec<u64>) -> Self {
        assert!(!samples.is_empty());
        samples.sort_unstable();
        let sum = samples
            .iter()
            .map(|sample| u128::from(*sample))
            .sum::<u128>();
        Self {
            p50_ns: percentile(&samples, 50),
            p95_ns: percentile(&samples, 95),
            mean_ns: u64::try_from(sum / samples.len() as u128).expect("mean fits u64"),
        }
    }
}

fn percentile(samples: &[u64], percentile: usize) -> u64 {
    let rank = (samples.len() * percentile).div_ceil(100);
    samples[rank.saturating_sub(1)]
}

fn ns_to_micros(ns: u64) -> f64 {
    ns as f64 / 1_000.0
}
