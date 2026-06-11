//! Two-phase profiling harness for the raw merge_10k operation.
//!
//! `setup <dir>` builds the fixture: opens a lix at <dir>/bench.lix, installs the
//! CSV plugin, writes the initial 10k-row CSV, and closes. `merge <dir>` reopens
//! the prepared file, warms the plugin outside the measured region, then runs the
//! merge inside `profile_merge_phase` so samply samples can be filtered to that
//! frame. The post-merge sqlite file is left on disk for inspection.

use lix_sdk::{FsWriteOptions, OpenLixOptions, SqliteBackend, Value, open_lix};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::io::{Cursor, Write as _};
use std::path::Path;
use std::time::Instant;

const INITIAL_ROW_COUNT: usize = 10_000;
const NEW_ROW_COUNT: usize = 10_000;
const CSV_PATH: &str = "/large-merge.csv";
const CSV_PLUGIN_WARMUP_PATH: &str = "/.csv-plugin-warmup.csv";

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (mode, dir) = match args.as_slice() {
        [_, mode, dir] => (mode.as_str(), dir.clone()),
        _ => {
            eprintln!("usage: profile_merge_10k <setup|merge> <dir>");
            std::process::exit(2);
        }
    };
    let lix_path = Path::new(&dir).join("bench.lix");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let initial_rows = random_csv_rows("initial", INITIAL_ROW_COUNT, 0x8ae7_b4b1_9f4c_d215);
    let new_rows = random_csv_rows("new", NEW_ROW_COUNT, 0xf3bb_91d4_6a8c_2e73);

    match mode {
        "setup" => runtime.block_on(async {
            let plugin = build_csv_plugin();
            let lix = open_lix(OpenLixOptions::new(
                SqliteBackend::open(&lix_path).expect("open sqlite backend"),
            ))
            .await
            .unwrap();
            lix.install_plugin_archive(&plugin).await.unwrap();
            let initial_csv = csv_bytes_from_rows(&initial_rows);
            let start = Instant::now();
            lix.write_file(CSV_PATH, initial_csv, FsWriteOptions::default())
                .await
                .unwrap();
            eprintln!("setup insert took {:?}", start.elapsed());
            lix.close().await.unwrap();
        }),
        "merge" => runtime.block_on(async {
            let updated_csv = csv_bytes_from_rows(&randomly_merge_csv_rows(
                &initial_rows,
                &new_rows,
                0x6449_2c6f_179d_31b5,
            ));
            let lix = open_lix(OpenLixOptions::new(
                SqliteBackend::open(&lix_path).expect("open sqlite backend"),
            ))
            .await
            .unwrap();
            // Warm: compiles the wasm component and primes caches outside the
            // measured region, mirroring warm_lix_csv_plugin in the bench.
            lix.write_file(
                CSV_PLUGIN_WARMUP_PATH,
                Vec::new(),
                FsWriteOptions::default(),
            )
            .await
            .unwrap();
            lix.execute(
                "DELETE FROM lix_file WHERE path = $1",
                &[Value::Text(CSV_PLUGIN_WARMUP_PATH.to_string())],
            )
            .await
            .unwrap();

            let start = Instant::now();
            profile_merge_phase(&lix, updated_csv).await;
            eprintln!("merge took {:?}", start.elapsed());
            lix.close().await.unwrap();
        }),
        other => {
            eprintln!("unknown mode {other}");
            std::process::exit(2);
        }
    }
}

#[inline(never)]
async fn profile_merge_phase(lix: &lix_sdk::Lix<SqliteBackend>, updated_csv: Vec<u8>) {
    lix.write_file(CSV_PATH, updated_csv, FsWriteOptions::default())
        .await
        .unwrap();
}

fn random_csv_rows(prefix: &str, count: usize, seed: u64) -> Vec<String> {
    let mut rng = SmallRng::seed_from_u64(seed);
    (0..count)
        .map(|offset| {
            format!(
                "{prefix}-{offset:05},{:016x},{:016x}",
                rng.random::<u64>(),
                rng.random::<u64>()
            )
        })
        .collect()
}

fn randomly_merge_csv_rows(initial_rows: &[String], new_rows: &[String], seed: u64) -> Vec<String> {
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut merged = Vec::with_capacity(initial_rows.len() + new_rows.len());
    let mut initial_index = 0usize;
    let mut new_index = 0usize;

    while initial_index < initial_rows.len() || new_index < new_rows.len() {
        let take_initial = if initial_index == initial_rows.len() {
            false
        } else if new_index == new_rows.len() {
            true
        } else {
            let remaining_initial = initial_rows.len() - initial_index;
            let remaining_new = new_rows.len() - new_index;
            rng.random_range(0..(remaining_initial + remaining_new)) < remaining_initial
        };

        if take_initial {
            merged.push(initial_rows[initial_index].clone());
            initial_index += 1;
        } else {
            merged.push(new_rows[new_index].clone());
            new_index += 1;
        }
    }

    merged
}

fn csv_bytes_from_rows(rows: &[String]) -> Vec<u8> {
    let mut csv = String::with_capacity(rows.iter().map(|row| row.len() + 1).sum());
    for row in rows {
        csv.push_str(row);
        csv.push('\n');
    }
    csv.into_bytes()
}

fn build_csv_plugin() -> Vec<u8> {
    let wasm = std::fs::read(Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_plugin_csv")))
        .expect("read bindep-built CSV plugin wasm");
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        (
            "manifest.json",
            include_str!("../../../plugins/csv/manifest.json").as_bytes(),
        ),
        (
            "schema/csv_table.json",
            include_str!("../../../plugins/csv/schema/csv_table.json").as_bytes(),
        ),
        (
            "schema/csv_row.json",
            include_str!("../../../plugins/csv/schema/csv_row.json").as_bytes(),
        ),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        writer.start_file(path, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}
