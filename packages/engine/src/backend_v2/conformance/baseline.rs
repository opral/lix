use crate::backend_v2::conformance::ConformanceReport;

pub(crate) fn register(report: &mut ConformanceReport) {
    report.add("baseline::get_many_preserves_caller_order_duplicates_and_missing");
    report.add("baseline::scan_range_returns_forward_row_bounded_pages");
    report.add("baseline::scan_prefix_matches_equivalent_range");
    report.add("baseline::commit_is_atomic");
    report.add("baseline::rollback_discards_staged_mutations");
    report.add("baseline::begin_read_pins_coherent_view");
    report.add("baseline::write_reads_its_own_writes");
    report.add("baseline::spaces_are_isolated");
    report.add("baseline::full_value_and_key_only_are_core");
}
