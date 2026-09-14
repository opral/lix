//! Reproducible native CSV-core scaling probe: cargo run -p plugin_csv --release --example qa_scale.
#![allow(dead_code)]
#[path = "../src/core.rs"]
mod core;
use core::*;
use std::{hint::black_box, time::Instant};
fn timed<T>(label: &str, f: impl FnOnce() -> T) -> T {
    let start = Instant::now();
    let result = f();
    println!("{label}: {:.3} ms", start.elapsed().as_secs_f64() * 1000.0);
    result
}
fn main() {
    for count in [100_000, 1_000_000] {
        println!("rows={count}");
        let bytes = b"0123456789,abcdefghij,0123456789,abcdefghij\r\n".repeat(count);
        let ns = IdNamespace::from_halves(1, 2);
        let (document, _) = timed("parse/index", || {
            Document::open_file(bytes.clone(), None, ns).unwrap()
        });
        println!(
            "source_bytes={} retained_bytes={}",
            bytes.len(),
            document.retained_bytes_estimate()
        );
        assert!(document.bytes_equal(&bytes));
        let mut import = timed("cold parse", || {
            ColdInitialImport::open(bytes.clone(), None).unwrap()
        });
        timed("emit all typed rows", || {
            for i in 0..count {
                black_box(import.next_row(ns.encode(i as u64)).unwrap().unwrap());
            }
        });
        let (_, changes) = document
            .file_changed(
                &[FileEdit {
                    offset: (bytes.len() / 2 + 11) as u64,
                    delete_len: 1,
                    insert: b"z",
                }],
                ns,
            )
            .unwrap();
        assert_eq!(changes.len(), 1);
        timed("single file edit x100", || {
            for _ in 0..100 {
                black_box(
                    document
                        .file_changed(
                            &[FileEdit {
                                offset: (bytes.len() / 2 + 11) as u64,
                                delete_len: 1,
                                insert: b"z",
                            }],
                            ns,
                        )
                        .unwrap(),
                );
            }
        });
        timed("single SQL row core edit x100", || {
            for _ in 0..100 {
                black_box(document.rows_changed(&changes).unwrap());
            }
        });
        let checkpoint = timed("identity checkpoint", || document.identity_checkpoint());
        let reopened = timed("reopen identity checkpoint", || {
            Document::open_file_with_identities(bytes.clone(), checkpoint.0, ns, &checkpoint.1)
                .unwrap()
        });
        assert!(reopened.bytes_equal(&bytes));
        drop(reopened);
        let (_, other_changes) = document
            .file_changed(
                &[FileEdit {
                    offset: 11,
                    delete_len: 1,
                    insert: b"z",
                }],
                ns,
            )
            .unwrap();
        let two = [changes[0].clone(), other_changes[0].clone()];
        let (updated, edits) = timed("two SQL row core edits", || {
            document.rows_changed(&two).unwrap()
        });
        println!(
            "two_sql_edit_count={} replaced_bytes={}",
            edits.len(),
            edits.iter().map(|e| e.delete_len).sum::<u64>()
        );
        assert_eq!(updated.row_count(), count);
    }
}
