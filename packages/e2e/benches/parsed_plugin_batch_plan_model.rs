//! Perfect-elimination ceiling for the operation-scoped parsed-plugin plan.
//!
//! This deliberately gives the proposed plan every plausible saving: all
//! selection work outside required guest hydration/rendering, actor
//! instantiation, exact materialization reads, blob lookup, and native-row
//! preparation. Guest `open_rows` and render remain because public file bytes
//! and plugin semantics require them.

#[derive(Clone, Copy)]
struct Cell {
    backend: &'static str,
    rows_per_file: u64,
    files: u64,
    total_ms: f64,
    selection_ms: f64,
    cold_open_ms: f64,
    render_ms: f64,
    begin_read_ms: f64,
    materialization_ref_ms: f64,
    instantiate_ms: f64,
    semantic_rows_ms: f64,
    blob_ms: f64,
}

impl Cell {
    fn optimistic_removable_ms(self) -> f64 {
        let selection_exclusive =
            (self.selection_ms - self.cold_open_ms - self.render_ms).max(0.0);
        selection_exclusive
            + self.begin_read_ms
            + self.materialization_ref_ms
            + self.instantiate_ms
            + self.semantic_rows_ms
            + self.blob_ms
    }

    fn optimistic_improvement(self) -> f64 {
        self.optimistic_removable_ms() / self.total_ms
    }
}

fn main() {
    let cells = [
        Cell {
            backend: "rocksdb",
            rows_per_file: 100,
            files: 17,
            total_ms: 27.104_754,
            selection_ms: 19.576_895,
            cold_open_ms: 7.778_278,
            render_ms: 4.075_338,
            begin_read_ms: 0.004_410,
            materialization_ref_ms: 0.190_599,
            instantiate_ms: 0.773_137,
            semantic_rows_ms: 2.007_069,
            blob_ms: 0.153_790,
        },
        Cell {
            backend: "slatedb",
            rows_per_file: 100,
            files: 17,
            total_ms: 27.037_344,
            selection_ms: 16.373_373,
            cold_open_ms: 7.965_847,
            render_ms: 4.064_297,
            begin_read_ms: 0.005_040,
            materialization_ref_ms: 0.112_819,
            instantiate_ms: 0.677_296,
            semantic_rows_ms: 2.287_427,
            blob_ms: 0.271_329,
        },
        Cell {
            backend: "rocksdb",
            rows_per_file: 1_000,
            files: 17,
            total_ms: 104.923_674,
            selection_ms: 94.286_041,
            cold_open_ms: 51.420_723,
            render_ms: 34.823_343,
            begin_read_ms: 0.005_000,
            materialization_ref_ms: 0.264_612,
            instantiate_ms: 1.001_514,
            semantic_rows_ms: 13.627_957,
            blob_ms: 0.443_336,
        },
        Cell {
            backend: "slatedb",
            rows_per_file: 1_000,
            files: 17,
            total_ms: 105.970_418,
            selection_ms: 90.694_271,
            cold_open_ms: 51.463_602,
            render_ms: 36.078_506,
            begin_read_ms: 0.007_570,
            materialization_ref_ms: 0.167_969,
            instantiate_ms: 0.918_135,
            semantic_rows_ms: 13.196_659,
            blob_ms: 0.569_577,
        },
    ];

    for cell in cells {
        println!(
            "backend={} rows_per_file={} files={} total_ms={:.3} optimistic_removable_ms={:.3} optimistic_improvement_pct={:.2}",
            cell.backend,
            cell.rows_per_file,
            cell.files,
            cell.total_ms,
            cell.optimistic_removable_ms(),
            cell.optimistic_improvement() * 100.0,
        );
    }

    let slate_large = cells[3].optimistic_improvement();
    assert!(
        slate_large < 0.20,
        "the plan unexpectedly clears the required cross-adapter 20% gate"
    );
}
