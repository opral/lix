use std::cell::RefCell;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PluginBenchStats {
    pub(crate) reconciliation_calls: usize,
    pub(crate) candidate_file_writes: usize,
    pub(crate) filesystem_scans: usize,
    pub(crate) filesystem_rows_scanned: usize,
    pub(crate) plugin_discovery_calls: usize,
    pub(crate) discovery_file_entries_examined: usize,
    pub(crate) file_id_entries_examined: usize,
    pub(crate) plugin_archives_loaded: usize,
    pub(crate) plugin_archive_bytes_loaded: usize,
    pub(crate) archive_entries_inflated: usize,
    pub(crate) archive_uncompressed_bytes: usize,
    pub(crate) manifest_parses: usize,
    pub(crate) schema_parses: usize,
    pub(crate) glob_match_attempts: usize,
    pub(crate) glob_compiles: usize,
    pub(crate) plugin_state_reads: usize,
    pub(crate) plugin_state_rows_loaded: usize,
    pub(crate) wasm_component_initializations: usize,
    pub(crate) wasm_detect_invocations: usize,
    pub(crate) wasm_render_invocations: usize,
}

thread_local! {
    // The probe runs on an explicit current-thread runtime so unrelated tests
    // cannot contribute work to its measurements.
    static PLUGIN_BENCH_STATS: RefCell<PluginBenchStats> = RefCell::default();
}

pub(crate) fn reset_plugin_bench_stats() {
    PLUGIN_BENCH_STATS.with_borrow_mut(|stats| *stats = PluginBenchStats::default());
}

pub(crate) fn plugin_bench_stats() -> PluginBenchStats {
    PLUGIN_BENCH_STATS.with_borrow(|stats| *stats)
}

fn update_plugin_bench_stats(update: impl FnOnce(&mut PluginBenchStats)) {
    PLUGIN_BENCH_STATS.with_borrow_mut(update);
}

pub(crate) fn record_reconciliation(candidate_count: usize) {
    update_plugin_bench_stats(|stats| {
        stats.reconciliation_calls += 1;
        stats.candidate_file_writes += candidate_count;
    });
}

pub(crate) fn record_filesystem_scan(row_count: usize) {
    update_plugin_bench_stats(|stats| {
        stats.filesystem_scans += 1;
        stats.filesystem_rows_scanned += row_count;
    });
}

pub(crate) fn record_plugin_discovery(file_entry_count: usize) {
    update_plugin_bench_stats(|stats| {
        stats.plugin_discovery_calls += 1;
        stats.discovery_file_entries_examined += file_entry_count;
    });
}

pub(crate) fn record_file_id_entries_examined(entry_count: usize) {
    update_plugin_bench_stats(|stats| stats.file_id_entries_examined += entry_count);
}

pub(crate) fn record_plugin_archive_load(byte_count: usize) {
    update_plugin_bench_stats(|stats| {
        stats.plugin_archives_loaded += 1;
        stats.plugin_archive_bytes_loaded += byte_count;
    });
}

pub(crate) fn record_archive_entry_inflated(byte_count: usize) {
    update_plugin_bench_stats(|stats| {
        stats.archive_entries_inflated += 1;
        stats.archive_uncompressed_bytes += byte_count;
    });
}

pub(crate) fn record_manifest_parse() {
    update_plugin_bench_stats(|stats| stats.manifest_parses += 1);
}

pub(crate) fn record_schema_parse() {
    update_plugin_bench_stats(|stats| stats.schema_parses += 1);
}

pub(crate) fn record_glob_match_attempt() {
    update_plugin_bench_stats(|stats| stats.glob_match_attempts += 1);
}

pub(crate) fn record_glob_compile() {
    update_plugin_bench_stats(|stats| stats.glob_compiles += 1);
}

pub(crate) fn record_plugin_state_read(row_count: usize) {
    update_plugin_bench_stats(|stats| {
        stats.plugin_state_reads += 1;
        stats.plugin_state_rows_loaded += row_count;
    });
}

pub(crate) fn record_wasm_component_initialization() {
    update_plugin_bench_stats(|stats| stats.wasm_component_initializations += 1);
}

pub(crate) fn record_wasm_detect_invocation() {
    update_plugin_bench_stats(|stats| stats.wasm_detect_invocations += 1);
}

pub(crate) fn record_wasm_render_invocation() {
    update_plugin_bench_stats(|stats| stats.wasm_render_invocations += 1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn benchmark_stats_are_isolated_by_thread() {
        reset_plugin_bench_stats();
        record_manifest_parse();

        let other_thread_stats = std::thread::spawn(|| {
            reset_plugin_bench_stats();
            record_manifest_parse();
            record_manifest_parse();
            plugin_bench_stats()
        })
        .join()
        .expect("counter test thread should join");

        assert_eq!(other_thread_stats.manifest_parses, 2);
        assert_eq!(plugin_bench_stats().manifest_parses, 1);
    }
}
