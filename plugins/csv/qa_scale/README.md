# CSV scaling probes

The native core probe measures parsing, typed-row emission, warm file/row edits,
identity checkpoint creation/reopening, and two-row updates at 100,000 and
1,000,000 rows:

```sh
cargo run -p plugin_csv --release --example qa_scale
```

The separate SQL probe imports the compiled plugin into an in-memory Lix instance,
imports the same 45-byte/four-cell rows, performs two successive cell edits, a
two-row edit, quoted multiline and shorter replacements through SQL, and a file
edit after SQL. It asserts exact CSV bytes after each variable-size edit and at
the end. Component compilation is timed separately using an empty warmup CSV. This includes the
engine, Wasm boundary, serialization, and storage work that core timings omit.
Build the component and probe from the repository root:

```sh
cargo build -p plugin_csv --target wasm32-wasip2 --release
cargo build --manifest-path plugins/csv/qa_scale/Cargo.toml --target-dir target
CSV_QA_ROWS=100000,1000000 target/debug/csv-qa-scale-sql target/wasm32-wasip2/release/plugin_csv.wasm
```

The default SQL build is a development build. Report its timings as development
measurements, not release performance. Set `CSV_QA_ROWS=100` for a smoke test.

Run the optional structural qualification to include row deletion, moving a row
to the end, appending a row through SQL, and updating 4,097 rows in one statement:

```sh
CSV_QA_ROWS=100000,1000000 CSV_QA_STRUCTURAL=1 target/debug/csv-qa-scale-sql target/wasm32-wasip2/release/plugin_csv.wasm
```

The probe verifies bytes after every operation, then closes the engine and reopens
the same in-memory storage to verify again. Structural errors are reported before
a final failing assertion, allowing other operations to run when the engine can
recover. Set `CSV_QA_SKIP_SPARSE=1` to isolate structural work during debugging.

Set `CSV_QA_BULK_STRIDE=2` to update 4,097 alternating rows and exercise
disjoint splices as well as contiguous batches.
