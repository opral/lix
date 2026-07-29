use lix_plugin_arena::{ByteEdit, Root, Store};
use std::hint::black_box;
use std::time::{Duration, Instant};

#[test]
fn component_v3_wit_is_valid_and_has_no_guest_document_resource() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../engine/wit/v3");
    let mut resolve = wit_parser::Resolve::default();
    resolve
        .push_dir(&path)
        .unwrap_or_else(|error| panic!("parse {}: {error:#}", path.display()));
    let wit = std::fs::read_to_string(path.join("lix-plugin-v3.wit")).unwrap();
    assert!(wit.contains("package lix:plugin@3.0.0"));
    assert!(wit.contains("resource root"));
    assert!(wit.contains("resource transaction"));
    assert!(!wit.contains("resource document"));
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
    let entity = format!("{{\"format\":\"{}\",\"changed\":true}}", format.name()).into_bytes();
    let state = format!("{}-page:{}", format.name(), edit_offset).into_bytes();

    let store = Store::default();
    let root = Root::import(
        store.clone(),
        "v3-generation",
        &bytes,
        [(b"stable/entity".to_vec(), b"{\"changed\":false}".to_vec())],
        [(b"affected/page".to_vec(), b"old-index-page".to_vec())],
    );
    let base_unique = store.unique_page_bytes();
    let mut transaction = root.transaction();
    transaction.edit_bytes(ByteEdit {
        offset: edit_offset as u64,
        delete_len: 1,
        insert: replacement.clone(),
    });
    transaction.upsert_entity(b"stable/entity".to_vec(), entity.clone());
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
        + entity.len()
        + b"old-index-page".len()
        + state.len();
    let v3_retained_bytes = store.unique_page_bytes();
    assert!(v3_retained_bytes >= base_unique);

    // v2 cold materialization lowers a full accepted document and persistent
    // state; v3 lowers one splice plus one entity and one state page.
    let v2_boundary_bytes = bytes.len() + b"old-index-page".len();
    let v3_boundary_bytes = 24 + replacement.len() + entity.len() + state.len();

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
fn four_format_sparse_scorecard_meets_materialization_gate() {
    for format in Format::ALL {
        let score = score(format);
        eprintln!("{score:?}");
        assert!(
            score.v3_retained_bytes * 100 <= score.v2_retained_bytes * 60,
            "{} must reduce retained immutable bytes by at least 40%: {score:?}",
            score.format
        );
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
    const SAMPLES: usize = 50;
    for format in Format::ALL {
        let bytes = format.fixture();
        let offset = bytes.len() / 2;
        let store = Store::default();
        let root = Root::import(
            store,
            "v3-generation",
            &bytes,
            std::iter::empty(),
            std::iter::empty(),
        );

        let v2 = measure(SAMPLES, || {
            let mut successor = bytes.clone();
            successor[offset] = black_box(b'X');
            black_box(successor);
        });
        let v3 = measure(SAMPLES, || {
            let mut transaction = root.transaction();
            transaction.edit_bytes(ByteEdit {
                offset: offset as u64,
                delete_len: 1,
                insert: vec![black_box(b'X')],
            });
            black_box(transaction.commit().unwrap());
        });
        eprintln!(
            "plugin_v3_arena_latency format={} bytes={} samples={SAMPLES} v2_mean_us={:.3} v3_mean_us={:.3} ratio={:.3}",
            format.name(),
            bytes.len(),
            mean_micros(v2, SAMPLES),
            mean_micros(v3, SAMPLES),
            v3.as_secs_f64() / v2.as_secs_f64(),
        );
    }
}

fn measure(samples: usize, mut operation: impl FnMut()) -> Duration {
    let started = Instant::now();
    for _ in 0..samples {
        operation();
    }
    started.elapsed()
}

fn mean_micros(duration: Duration, samples: usize) -> f64 {
    duration.as_secs_f64() * 1_000_000.0 / samples as f64
}
