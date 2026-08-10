#[path = "../benches/tracked_state_crud/sql_session.rs"]
#[allow(dead_code)]
mod sql_session;
#[path = "../benches/tracked_state_crud/storage.rs"]
#[allow(dead_code)]
mod storage;
#[path = "../benches/tracked_state_crud/workload.rs"]
#[allow(dead_code)]
mod workload;

const READ_MANY_PK_COUNT: usize = 10;

fn main() {
    let mut args = std::env::args().skip(1);
    let backend = args.next().expect("backend");
    let rows = args.next().expect("rows").parse::<usize>().expect("rows");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(run(backend, rows));
}

async fn run(backend: String, rows: usize) {
    let profile = match backend.as_str() {
        "rocksdb" => storage::StorageProfile::RocksDB,
        #[cfg(feature = "slatedb")]
        "slatedb" => storage::StorageProfile::SlateDB,
        other => panic!("unknown backend {other}"),
    };
    let workload = workload::fixture_rows(rows);
    let fixture =
        sql_session::seeded_fixture_with_read_many_pk_count(profile, &workload, READ_MANY_PK_COUNT)
            .await;
    let sql = "SELECT path, value FROM json_pointer ORDER BY path";
    for mode in ["full", "count_only", "stream", "live"] {
        let profile = fixture.profile_stream(sql, mode).await;
        println!(
            "backend={backend} rows={rows} mode={mode} total_us={} logical_us={} physical_us={} arrow_us={} materialize_us={} scan_us={} scan_rows={} scan_batches={} scan_arrow_bytes={} count_rows={} count_batches={} consumed={} materialized={} retained={} checksum={} branch_ranges={} untracked_overlays={} view_acquisitions={} page_reads={} page_entries={} page_bytes={} member_resolutions={} source_closure_gets={} member_closure_hits={} merge_rows={} tombstone_drops={} key_clones={}",
            profile.total.as_micros(),
            profile.logical_planning.as_micros(),
            profile.physical_planning.as_micros(),
            profile.arrow_execution.as_micros(),
            profile.public_result_materialization.as_micros(),
            profile.scan_elapsed.as_micros(),
            profile.scan_rows,
            profile.scan_batches,
            profile.scan_arrow_bytes,
            profile.result_count_only_rows,
            profile.result_count_only_batches,
            profile.result_rows_consumed,
            profile.result_rows_materialized,
            profile.result_rows_retained,
            profile.result_checksum,
            profile.scan_owner_branch_range_calls,
            profile.scan_owner_untracked_overlay_calls,
            profile.scan_owner_view_acquisitions,
            profile.scan_owner_page_reads,
            profile.scan_owner_page_entries,
            profile.scan_owner_page_bytes,
            profile.scan_owner_member_resolutions,
            profile.scan_owner_source_closure_gets,
            profile.scan_owner_member_closure_hits,
            profile.scan_owner_merge_rows,
            profile.scan_owner_tombstone_drops,
            profile.scan_owner_key_clones,
        );
    }
}
