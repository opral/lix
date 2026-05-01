use crate::binary_cas::{BinaryBlobWrite, BinaryCasContext};
use crate::changelog::{CanonicalChange, ChangelogContext, ChangelogScanRequest};
use crate::entity_identity::EntityIdentity;
use crate::tracked_state::{
    TrackedStateContext, TrackedStateDiffRequest, TrackedStateFilter, TrackedStateRow,
    TrackedStateRowRequest, TrackedStateScanRequest,
};
use crate::untracked_state::{
    UntrackedStateContext, UntrackedStateFilter, UntrackedStateRow, UntrackedStateRowRequest,
    UntrackedStateScanRequest,
};
use crate::{KvScanRange, LixBackend, LixError, NullableKeyFilter, TransactionBeginMode};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
pub struct StorageBenchConfig {
    pub rows: usize,
    pub blob_bytes: usize,
    pub state_payload_bytes: usize,
    pub key_pattern: StorageBenchKeyPattern,
    pub selectivity: StorageBenchSelectivity,
    pub update_fraction: StorageBenchUpdateFraction,
}

impl StorageBenchConfig {
    pub fn with_rows(mut self, rows: usize) -> Self {
        self.rows = rows;
        self
    }

    pub fn with_blob_bytes(mut self, blob_bytes: usize) -> Self {
        self.blob_bytes = blob_bytes;
        self
    }

    pub fn with_state_payload_bytes(mut self, state_payload_bytes: usize) -> Self {
        self.state_payload_bytes = state_payload_bytes;
        self
    }

    pub fn with_key_pattern(mut self, key_pattern: StorageBenchKeyPattern) -> Self {
        self.key_pattern = key_pattern;
        self
    }

    pub fn with_selectivity(mut self, selectivity: StorageBenchSelectivity) -> Self {
        self.selectivity = selectivity;
        self
    }

    pub fn with_update_fraction(mut self, update_fraction: StorageBenchUpdateFraction) -> Self {
        self.update_fraction = update_fraction;
        self
    }
}

#[derive(Debug, Clone, Copy)]
pub enum StorageBenchKeyPattern {
    Sequential,
    Random,
}

#[derive(Debug, Clone, Copy)]
pub enum StorageBenchSelectivity {
    Percent1,
    Percent10,
    Percent100,
}

impl StorageBenchSelectivity {
    fn matches(self, index: usize) -> bool {
        match self {
            Self::Percent1 => index % 100 == 0,
            Self::Percent10 => index % 10 == 0,
            Self::Percent100 => true,
        }
    }

    fn expected_rows(self, rows: usize) -> usize {
        (0..rows).filter(|index| self.matches(*index)).count()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum StorageBenchUpdateFraction {
    Percent10,
    Percent100,
}

impl StorageBenchUpdateFraction {
    fn rows(self, total_rows: usize) -> usize {
        match self {
            Self::Percent10 => total_rows.div_ceil(10),
            Self::Percent100 => total_rows,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StorageBenchReport {
    pub measured_rows: usize,
    pub verified_rows: usize,
    pub elapsed: Duration,
}

pub struct TrackedStateWriteRootFixture {
    context: TrackedStateContext,
    rows: Vec<TrackedStateRow>,
}

pub struct TrackedStateReadFixture {
    context: TrackedStateContext,
    rows: usize,
    commit_id: String,
    key_pattern: StorageBenchKeyPattern,
    selectivity: StorageBenchSelectivity,
}

pub struct TrackedStateUpdateFixture {
    context: TrackedStateContext,
    rows: Vec<TrackedStateRow>,
}

pub struct TrackedStateDiffFixture {
    context: TrackedStateContext,
    left_commit_id: String,
    right_commit_id: String,
    expected_entries: usize,
}

pub struct UntrackedStateWriteFixture {
    context: UntrackedStateContext,
    rows: Vec<UntrackedStateRow>,
}

pub struct UntrackedStateReadFixture {
    context: UntrackedStateContext,
    rows: usize,
    key_pattern: StorageBenchKeyPattern,
    selectivity: StorageBenchSelectivity,
}

pub struct ChangelogAppendFixture {
    context: ChangelogContext,
    changes: Vec<CanonicalChange>,
}

pub struct ChangelogReadFixture {
    context: ChangelogContext,
    rows: usize,
}

pub struct BinaryCasWriteFixture {
    context: BinaryCasContext,
    file_ids: Vec<String>,
    payloads: Vec<Vec<u8>>,
}

pub struct BinaryCasReadFixture {
    context: BinaryCasContext,
    rows: usize,
    hashes: Vec<String>,
}

pub async fn prepare_tracked_state_write_root(
    config: StorageBenchConfig,
) -> Result<TrackedStateWriteRootFixture, LixError> {
    Ok(TrackedStateWriteRootFixture {
        context: TrackedStateContext::new(),
        rows: tracked_rows(config, "bench-tracked-commit"),
    })
}

pub async fn tracked_state_write_root_prepared(
    backend: &(dyn LixBackend + Send + Sync),
    fixture: &TrackedStateWriteRootFixture,
) -> Result<StorageBenchReport, LixError> {
    write_tracked_root(
        backend,
        &fixture.context,
        "bench-tracked-commit",
        None,
        &fixture.rows,
    )
    .await?;
    Ok(report(
        fixture.rows.len(),
        fixture.rows.len(),
        Duration::ZERO,
    ))
}

pub async fn prepare_tracked_state_read(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<TrackedStateReadFixture, LixError> {
    let context = TrackedStateContext::new();
    let rows = tracked_rows(config, "bench-tracked-commit");
    write_tracked_root(backend, &context, "bench-tracked-commit", None, &rows).await?;
    Ok(TrackedStateReadFixture {
        context,
        rows: config.rows,
        commit_id: "bench-tracked-commit".to_string(),
        key_pattern: config.key_pattern,
        selectivity: config.selectivity,
    })
}

pub async fn tracked_state_read_point_hit_prepared(
    backend: &(dyn LixBackend + Send + Sync),
    fixture: &TrackedStateReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut verified_rows = 0;
    let mut reader = fixture.context.reader(backend);
    for index in 0..fixture.rows {
        if reader
            .load_row_at_commit(
                &fixture.commit_id,
                &TrackedStateRowRequest {
                    schema_key: tracked_schema_key(index, StorageBenchSelectivity::Percent100),
                    entity_id: EntityIdentity::single(entity_id(
                        "tracked",
                        index,
                        fixture.key_pattern,
                    )),
                    file_id: NullableKeyFilter::Value("bench.json".to_string()),
                },
            )
            .await?
            .is_some()
        {
            verified_rows += 1;
        }
    }
    Ok(report(fixture.rows, verified_rows, Duration::ZERO))
}

pub async fn tracked_state_read_point_hit_constant_prepared(
    backend: &(dyn LixBackend + Send + Sync),
    fixture: &TrackedStateReadFixture,
    measured_reads: usize,
) -> Result<StorageBenchReport, LixError> {
    let mut verified_rows = 0;
    let mut reader = fixture.context.reader(backend);
    for index in 0..measured_reads.min(fixture.rows) {
        if reader
            .load_row_at_commit(
                &fixture.commit_id,
                &TrackedStateRowRequest {
                    schema_key: tracked_schema_key(index, StorageBenchSelectivity::Percent100),
                    entity_id: EntityIdentity::single(entity_id(
                        "tracked",
                        index,
                        fixture.key_pattern,
                    )),
                    file_id: NullableKeyFilter::Value("bench.json".to_string()),
                },
            )
            .await?
            .is_some()
        {
            verified_rows += 1;
        }
    }
    Ok(report(
        measured_reads.min(fixture.rows),
        verified_rows,
        Duration::ZERO,
    ))
}

pub async fn tracked_state_read_point_miss_prepared(
    backend: &(dyn LixBackend + Send + Sync),
    fixture: &TrackedStateReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut misses = 0;
    let mut reader = fixture.context.reader(backend);
    for index in 0..fixture.rows {
        if reader
            .load_row_at_commit(
                &fixture.commit_id,
                &TrackedStateRowRequest {
                    schema_key: "bench_tracked_entity".to_string(),
                    entity_id: EntityIdentity::single(format!("missing-{index}")),
                    file_id: NullableKeyFilter::Value("bench.json".to_string()),
                },
            )
            .await?
            .is_none()
        {
            misses += 1;
        }
    }
    Ok(report(fixture.rows, misses, Duration::ZERO))
}

pub async fn tracked_state_scan_all_prepared(
    backend: &(dyn LixBackend + Send + Sync),
    fixture: &TrackedStateReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let verified_rows = scan_tracked(backend, &fixture.context, &fixture.commit_id)
        .await?
        .len();
    Ok(report(fixture.rows, verified_rows, Duration::ZERO))
}

pub async fn tracked_state_scan_schema_prepared(
    backend: &(dyn LixBackend + Send + Sync),
    fixture: &TrackedStateReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut reader = fixture.context.reader(backend);
    let verified_rows = reader
        .scan_rows_at_commit(
            &fixture.commit_id,
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    schema_keys: vec![tracked_schema_key(0, StorageBenchSelectivity::Percent100)],
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await?
        .len();
    Ok(report(fixture.rows, verified_rows, Duration::ZERO))
}

pub async fn tracked_state_scan_schema_selective_prepared(
    backend: &(dyn LixBackend + Send + Sync),
    fixture: &TrackedStateReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut reader = fixture.context.reader(backend);
    let verified_rows = reader
        .scan_rows_at_commit(
            &fixture.commit_id,
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    schema_keys: vec![TRACKED_MATCH_SCHEMA_KEY.to_string()],
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await?
        .len();
    Ok(report(
        fixture.selectivity.expected_rows(fixture.rows),
        verified_rows,
        Duration::ZERO,
    ))
}

pub async fn tracked_state_scan_file_prepared(
    backend: &(dyn LixBackend + Send + Sync),
    fixture: &TrackedStateReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut reader = fixture.context.reader(backend);
    let verified_rows = reader
        .scan_rows_at_commit(
            &fixture.commit_id,
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    file_ids: vec![NullableKeyFilter::Value("bench.json".to_string())],
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await?
        .len();
    Ok(report(fixture.rows, verified_rows, Duration::ZERO))
}

pub async fn prepare_tracked_state_update(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<TrackedStateUpdateFixture, LixError> {
    prepare_tracked_state_update_rows(backend, config, config.update_fraction.rows(config.rows))
        .await
}

pub async fn prepare_tracked_state_update_rows(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
    updated_rows: usize,
) -> Result<TrackedStateUpdateFixture, LixError> {
    let context = TrackedStateContext::new();
    let rows = tracked_rows(config, "bench-tracked-parent");
    write_tracked_root(backend, &context, "bench-tracked-parent", None, &rows).await?;
    let mut updated_rows = tracked_rows(
        config.with_rows(updated_rows.min(config.rows)),
        "bench-tracked-child",
    );
    for (index, row) in updated_rows.iter_mut().enumerate() {
        row.snapshot_content = Some(updated_snapshot_content(index, config.state_payload_bytes));
    }
    Ok(TrackedStateUpdateFixture {
        context,
        rows: updated_rows,
    })
}

pub async fn prepare_tracked_state_partial_snapshot_update_rows(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
    updated_rows: usize,
) -> Result<TrackedStateUpdateFixture, LixError> {
    let context = TrackedStateContext::new();
    let rows = tracked_rows(config, "bench-tracked-parent");
    write_tracked_root(backend, &context, "bench-tracked-parent", None, &rows).await?;
    let mut updated_rows = tracked_rows(
        config.with_rows(updated_rows.min(config.rows)),
        "bench-tracked-child",
    );
    for (index, row) in updated_rows.iter_mut().enumerate() {
        row.snapshot_content = Some(partial_updated_snapshot_content(
            index,
            config.state_payload_bytes,
        ));
    }
    Ok(TrackedStateUpdateFixture {
        context,
        rows: updated_rows,
    })
}

pub async fn prepare_tracked_state_append_child(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<TrackedStateUpdateFixture, LixError> {
    prepare_tracked_state_append_child_rows(backend, config, config.rows).await
}

pub async fn prepare_tracked_state_append_child_rows(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
    appended_rows: usize,
) -> Result<TrackedStateUpdateFixture, LixError> {
    let context = TrackedStateContext::new();
    let rows = tracked_rows(config, "bench-tracked-parent");
    write_tracked_root(backend, &context, "bench-tracked-parent", None, &rows).await?;
    let mut appended_rows = tracked_rows(
        config.with_rows(appended_rows.min(config.rows)),
        "bench-tracked-child",
    );
    for (index, row) in appended_rows.iter_mut().enumerate() {
        row.entity_id = EntityIdentity::single(entity_id("tracked-new", index, config.key_pattern));
        row.change_id = format!("tracked-new-change-{index}");
    }
    Ok(TrackedStateUpdateFixture {
        context,
        rows: appended_rows,
    })
}

pub async fn prepare_tracked_state_tombstone_rows(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
    tombstone_rows: usize,
) -> Result<TrackedStateUpdateFixture, LixError> {
    let context = TrackedStateContext::new();
    let rows = tracked_rows(config, "bench-tracked-parent");
    write_tracked_root(backend, &context, "bench-tracked-parent", None, &rows).await?;
    let mut tombstones = tracked_rows(
        config.with_rows(tombstone_rows.min(config.rows)),
        "bench-tracked-child",
    );
    for row in &mut tombstones {
        row.snapshot_content = None;
    }
    Ok(TrackedStateUpdateFixture {
        context,
        rows: tombstones,
    })
}

pub async fn tracked_state_update_existing_prepared(
    backend: &(dyn LixBackend + Send + Sync),
    fixture: &TrackedStateUpdateFixture,
) -> Result<StorageBenchReport, LixError> {
    write_tracked_root(
        backend,
        &fixture.context,
        "bench-tracked-child",
        Some("bench-tracked-parent"),
        &fixture.rows,
    )
    .await?;
    Ok(report(
        fixture.rows.len(),
        fixture.rows.len(),
        Duration::ZERO,
    ))
}

pub async fn prepare_tracked_state_diff_update_rows(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
    updated_rows: usize,
) -> Result<TrackedStateDiffFixture, LixError> {
    let fixture = prepare_tracked_state_update_rows(backend, config, updated_rows).await?;
    tracked_state_update_existing_prepared(backend, &fixture).await?;
    Ok(TrackedStateDiffFixture {
        context: fixture.context,
        left_commit_id: "bench-tracked-parent".to_string(),
        right_commit_id: "bench-tracked-child".to_string(),
        expected_entries: fixture.rows.len(),
    })
}

pub async fn prepare_tracked_state_diff_tombstone_rows(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
    tombstone_rows: usize,
) -> Result<TrackedStateDiffFixture, LixError> {
    let fixture = prepare_tracked_state_tombstone_rows(backend, config, tombstone_rows).await?;
    tracked_state_update_existing_prepared(backend, &fixture).await?;
    Ok(TrackedStateDiffFixture {
        context: fixture.context,
        left_commit_id: "bench-tracked-parent".to_string(),
        right_commit_id: "bench-tracked-child".to_string(),
        expected_entries: fixture.rows.len(),
    })
}

pub async fn prepare_tracked_state_diff_equal(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<TrackedStateDiffFixture, LixError> {
    let context = TrackedStateContext::new();
    let rows = tracked_rows(config, "bench-tracked-parent");
    write_tracked_root(backend, &context, "bench-tracked-parent", None, &rows).await?;
    Ok(TrackedStateDiffFixture {
        context,
        left_commit_id: "bench-tracked-parent".to_string(),
        right_commit_id: "bench-tracked-parent".to_string(),
        expected_entries: 0,
    })
}

pub async fn tracked_state_diff_commits_prepared(
    backend: &(dyn LixBackend + Send + Sync),
    fixture: &TrackedStateDiffFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut reader = fixture.context.reader(backend);
    let diff = reader
        .diff_commits(
            &fixture.left_commit_id,
            &fixture.right_commit_id,
            &TrackedStateDiffRequest::default(),
        )
        .await?;
    Ok(report(
        fixture.expected_entries,
        diff.entries.len(),
        Duration::ZERO,
    ))
}

pub async fn prepare_untracked_state_write_rows(
    config: StorageBenchConfig,
) -> Result<UntrackedStateWriteFixture, LixError> {
    Ok(UntrackedStateWriteFixture {
        context: UntrackedStateContext::new(),
        rows: untracked_rows(config),
    })
}

pub async fn untracked_state_write_rows_prepared(
    backend: &(dyn LixBackend + Send + Sync),
    fixture: &UntrackedStateWriteFixture,
) -> Result<StorageBenchReport, LixError> {
    write_untracked_rows(backend, &fixture.context, &fixture.rows).await?;
    let verified_rows = scan_untracked(
        backend,
        &fixture.context,
        UntrackedStateScanRequest::default(),
    )
    .await?
    .len();
    Ok(report(fixture.rows.len(), verified_rows, Duration::ZERO))
}

pub async fn prepare_untracked_state_read(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<UntrackedStateReadFixture, LixError> {
    let context = UntrackedStateContext::new();
    let rows = untracked_rows(config);
    write_untracked_rows(backend, &context, &rows).await?;
    Ok(UntrackedStateReadFixture {
        context,
        rows: config.rows,
        key_pattern: config.key_pattern,
        selectivity: config.selectivity,
    })
}

pub async fn untracked_state_read_point_hit_prepared(
    backend: &(dyn LixBackend + Send + Sync),
    fixture: &UntrackedStateReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut verified_rows = 0;
    let mut reader = fixture.context.reader(backend);
    for index in 0..fixture.rows {
        if reader
            .load_row(&UntrackedStateRowRequest {
                schema_key: untracked_schema_key(index, StorageBenchSelectivity::Percent100),
                version_id: "bench-version".to_string(),
                entity_id: EntityIdentity::single(entity_id(
                    "untracked",
                    index,
                    fixture.key_pattern,
                )),
                file_id: NullableKeyFilter::Value("bench.json".to_string()),
            })
            .await?
            .is_some()
        {
            verified_rows += 1;
        }
    }
    Ok(report(fixture.rows, verified_rows, Duration::ZERO))
}

pub async fn untracked_state_read_point_hit_constant_prepared(
    backend: &(dyn LixBackend + Send + Sync),
    fixture: &UntrackedStateReadFixture,
    measured_reads: usize,
) -> Result<StorageBenchReport, LixError> {
    let mut verified_rows = 0;
    let mut reader = fixture.context.reader(backend);
    for index in 0..measured_reads.min(fixture.rows) {
        if reader
            .load_row(&UntrackedStateRowRequest {
                schema_key: untracked_schema_key(index, StorageBenchSelectivity::Percent100),
                version_id: "bench-version".to_string(),
                entity_id: EntityIdentity::single(entity_id(
                    "untracked",
                    index,
                    fixture.key_pattern,
                )),
                file_id: NullableKeyFilter::Value("bench.json".to_string()),
            })
            .await?
            .is_some()
        {
            verified_rows += 1;
        }
    }
    Ok(report(
        measured_reads.min(fixture.rows),
        verified_rows,
        Duration::ZERO,
    ))
}

pub async fn untracked_state_read_point_miss_prepared(
    backend: &(dyn LixBackend + Send + Sync),
    fixture: &UntrackedStateReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut misses = 0;
    let mut reader = fixture.context.reader(backend);
    for index in 0..fixture.rows {
        if reader
            .load_row(&UntrackedStateRowRequest {
                schema_key: "bench_untracked_entity".to_string(),
                version_id: "bench-version".to_string(),
                entity_id: EntityIdentity::single(format!("missing-{index}")),
                file_id: NullableKeyFilter::Value("bench.json".to_string()),
            })
            .await?
            .is_none()
        {
            misses += 1;
        }
    }
    Ok(report(fixture.rows, misses, Duration::ZERO))
}

pub async fn untracked_state_scan_all_prepared(
    backend: &(dyn LixBackend + Send + Sync),
    fixture: &UntrackedStateReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let verified_rows = scan_untracked(
        backend,
        &fixture.context,
        UntrackedStateScanRequest::default(),
    )
    .await?
    .len();
    Ok(report(fixture.rows, verified_rows, Duration::ZERO))
}

pub async fn untracked_state_scan_version_prepared(
    backend: &(dyn LixBackend + Send + Sync),
    fixture: &UntrackedStateReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let verified_rows = scan_untracked(
        backend,
        &fixture.context,
        UntrackedStateScanRequest {
            filter: UntrackedStateFilter {
                version_ids: vec!["bench-version".to_string()],
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await?
    .len();
    Ok(report(fixture.rows, verified_rows, Duration::ZERO))
}

pub async fn untracked_state_scan_schema_prepared(
    backend: &(dyn LixBackend + Send + Sync),
    fixture: &UntrackedStateReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let verified_rows = scan_untracked(
        backend,
        &fixture.context,
        UntrackedStateScanRequest {
            filter: UntrackedStateFilter {
                schema_keys: vec![untracked_schema_key(0, StorageBenchSelectivity::Percent100)],
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await?
    .len();
    Ok(report(fixture.rows, verified_rows, Duration::ZERO))
}

pub async fn untracked_state_scan_schema_selective_prepared(
    backend: &(dyn LixBackend + Send + Sync),
    fixture: &UntrackedStateReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let verified_rows = scan_untracked(
        backend,
        &fixture.context,
        UntrackedStateScanRequest {
            filter: UntrackedStateFilter {
                schema_keys: vec![UNTRACKED_MATCH_SCHEMA_KEY.to_string()],
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await?
    .len();
    Ok(report(
        fixture.selectivity.expected_rows(fixture.rows),
        verified_rows,
        Duration::ZERO,
    ))
}

pub async fn prepare_untracked_state_overwrite(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<UntrackedStateWriteFixture, LixError> {
    let context = UntrackedStateContext::new();
    let rows = untracked_rows(config);
    write_untracked_rows(backend, &context, &rows).await?;
    let mut updated_rows =
        untracked_rows(config.with_rows(config.update_fraction.rows(config.rows)));
    for (index, row) in updated_rows.iter_mut().enumerate() {
        row.snapshot_content = Some(updated_snapshot_content(index, config.state_payload_bytes));
    }
    Ok(UntrackedStateWriteFixture {
        context,
        rows: updated_rows,
    })
}

pub async fn prepare_untracked_state_insert_new_keys(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<UntrackedStateWriteFixture, LixError> {
    let context = UntrackedStateContext::new();
    let rows = untracked_rows(config);
    write_untracked_rows(backend, &context, &rows).await?;
    let mut new_rows = untracked_rows(config);
    for (index, row) in new_rows.iter_mut().enumerate() {
        row.entity_id =
            EntityIdentity::single(entity_id("untracked-new", index, config.key_pattern));
    }
    Ok(UntrackedStateWriteFixture {
        context,
        rows: new_rows,
    })
}

pub async fn untracked_state_overwrite_existing_prepared(
    backend: &(dyn LixBackend + Send + Sync),
    fixture: &UntrackedStateWriteFixture,
) -> Result<StorageBenchReport, LixError> {
    write_untracked_rows(backend, &fixture.context, &fixture.rows).await?;
    let verified_rows = scan_untracked(
        backend,
        &fixture.context,
        UntrackedStateScanRequest::default(),
    )
    .await?
    .len();
    Ok(report(fixture.rows.len(), verified_rows, Duration::ZERO))
}

pub async fn prepare_changelog_append_changes(
    config: StorageBenchConfig,
) -> Result<ChangelogAppendFixture, LixError> {
    Ok(ChangelogAppendFixture {
        context: ChangelogContext::new(),
        changes: changelog_changes(config),
    })
}

pub async fn changelog_append_changes_prepared(
    backend: &(dyn LixBackend + Send + Sync),
    fixture: &ChangelogAppendFixture,
) -> Result<StorageBenchReport, LixError> {
    append_changelog_changes(backend, &fixture.context, &fixture.changes).await?;
    let reader = fixture.context.reader(backend);
    let verified_rows = reader
        .scan_changes(&ChangelogScanRequest::default())
        .await?
        .len();
    Ok(report(fixture.changes.len(), verified_rows, Duration::ZERO))
}

pub async fn prepare_changelog_read(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<ChangelogReadFixture, LixError> {
    let context = ChangelogContext::new();
    let changes = changelog_changes(config);
    append_changelog_changes(backend, &context, &changes).await?;
    Ok(ChangelogReadFixture {
        context,
        rows: config.rows,
    })
}

pub async fn changelog_load_change_hit_prepared(
    backend: &(dyn LixBackend + Send + Sync),
    fixture: &ChangelogReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let reader = fixture.context.reader(backend);
    let mut verified_rows = 0;
    for index in 0..fixture.rows {
        if reader
            .load_change(&format!("bench-change-{index}"))
            .await?
            .is_some()
        {
            verified_rows += 1;
        }
    }
    Ok(report(fixture.rows, verified_rows, Duration::ZERO))
}

pub async fn changelog_load_change_miss_prepared(
    backend: &(dyn LixBackend + Send + Sync),
    fixture: &ChangelogReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let reader = fixture.context.reader(backend);
    let mut misses = 0;
    for index in 0..fixture.rows {
        if reader
            .load_change(&format!("missing-change-{index}"))
            .await?
            .is_none()
        {
            misses += 1;
        }
    }
    Ok(report(fixture.rows, misses, Duration::ZERO))
}

pub async fn changelog_scan_all_prepared(
    backend: &(dyn LixBackend + Send + Sync),
    fixture: &ChangelogReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let reader = fixture.context.reader(backend);
    let verified_rows = reader
        .scan_changes(&ChangelogScanRequest::default())
        .await?
        .len();
    Ok(report(fixture.rows, verified_rows, Duration::ZERO))
}

pub async fn changelog_scan_limit_100_prepared(
    backend: &(dyn LixBackend + Send + Sync),
    fixture: &ChangelogReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let reader = fixture.context.reader(backend);
    let expected = fixture.rows.min(100);
    let verified_rows = reader
        .scan_changes(&ChangelogScanRequest {
            limit: Some(expected),
        })
        .await?
        .len();
    Ok(report(expected, verified_rows, Duration::ZERO))
}

pub async fn prepare_binary_cas_write_blobs(
    config: StorageBenchConfig,
) -> Result<BinaryCasWriteFixture, LixError> {
    Ok(BinaryCasWriteFixture {
        context: BinaryCasContext::new(),
        file_ids: binary_file_ids(config.rows),
        payloads: binary_payloads(config.rows, config.blob_bytes),
    })
}

pub async fn prepare_binary_cas_write_duplicate_payload(
    config: StorageBenchConfig,
) -> Result<BinaryCasWriteFixture, LixError> {
    let payload = binary_payload(0, config.blob_bytes);
    Ok(BinaryCasWriteFixture {
        context: BinaryCasContext::new(),
        file_ids: binary_file_ids(config.rows),
        payloads: (0..config.rows).map(|_| payload.clone()).collect(),
    })
}

pub async fn prepare_binary_cas_write_half_duplicate_payload(
    config: StorageBenchConfig,
) -> Result<BinaryCasWriteFixture, LixError> {
    Ok(BinaryCasWriteFixture {
        context: BinaryCasContext::new(),
        file_ids: binary_file_ids(config.rows),
        payloads: binary_half_duplicate_payloads(config.rows, config.blob_bytes),
    })
}

pub async fn binary_cas_write_blobs_prepared(
    backend: &(dyn LixBackend + Send + Sync),
    fixture: &BinaryCasWriteFixture,
) -> Result<StorageBenchReport, LixError> {
    let writes = binary_blob_writes(&fixture.file_ids, &fixture.payloads);
    write_binary_blob_writes(backend, &fixture.context, &writes).await?;
    let verified_rows = count_binary_cas_manifests(backend).await?;
    Ok(report(writes.len(), verified_rows, Duration::ZERO))
}

pub async fn prepare_binary_cas_read(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<BinaryCasReadFixture, LixError> {
    let context = BinaryCasContext::new();
    let payloads = binary_payloads(config.rows, config.blob_bytes);
    let file_ids = binary_file_ids(config.rows);
    let writes = binary_blob_writes(&file_ids, &payloads);
    write_binary_blob_writes(backend, &context, &writes).await?;
    let hashes = payloads
        .iter()
        .map(|payload| crate::binary_cas::binary_blob_hash_hex(payload))
        .collect::<Vec<_>>();
    Ok(BinaryCasReadFixture {
        context,
        rows: config.rows,
        hashes,
    })
}

pub async fn binary_cas_read_blob_hit_prepared(
    backend: &(dyn LixBackend + Send + Sync),
    fixture: &BinaryCasReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut verified_rows = 0;
    let mut reader = fixture.context.reader(backend);
    for hash in &fixture.hashes {
        if reader.load_blob_data_by_hash(hash).await?.is_some() {
            verified_rows += 1;
        }
    }
    Ok(report(fixture.hashes.len(), verified_rows, Duration::ZERO))
}

pub async fn binary_cas_read_blob_miss_prepared(
    backend: &(dyn LixBackend + Send + Sync),
    fixture: &BinaryCasReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut misses = 0;
    let mut reader = fixture.context.reader(backend);
    for index in 0..fixture.rows {
        let missing_hash = format!("{index:064x}");
        if reader
            .load_blob_data_by_hash(&missing_hash)
            .await?
            .is_none()
        {
            misses += 1;
        }
    }
    Ok(report(fixture.rows, misses, Duration::ZERO))
}

pub async fn tracked_state_write_root(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let rows = tracked_rows(config, "bench-tracked-commit");
    let context = TrackedStateContext::new();
    let started = Instant::now();
    write_tracked_root(backend, &context, "bench-tracked-commit", None, &rows).await?;
    let elapsed = started.elapsed();
    let verified_rows = scan_tracked(backend, &context, "bench-tracked-commit")
        .await?
        .len();
    Ok(report(rows.len(), verified_rows, elapsed))
}

pub async fn tracked_state_read_point_hit(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = TrackedStateContext::new();
    let rows = tracked_rows(config, "bench-tracked-commit");
    write_tracked_root(backend, &context, "bench-tracked-commit", None, &rows).await?;

    let started = Instant::now();
    let mut verified_rows = 0;
    let mut reader = context.reader(backend);
    for index in 0..config.rows {
        if reader
            .load_row_at_commit(
                "bench-tracked-commit",
                &TrackedStateRowRequest {
                    schema_key: tracked_schema_key(index, StorageBenchSelectivity::Percent100),
                    entity_id: EntityIdentity::single(entity_id(
                        "tracked",
                        index,
                        config.key_pattern,
                    )),
                    file_id: NullableKeyFilter::Value("bench.json".to_string()),
                },
            )
            .await?
            .is_some()
        {
            verified_rows += 1;
        }
    }
    Ok(report(config.rows, verified_rows, started.elapsed()))
}

pub async fn tracked_state_read_point_miss(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = TrackedStateContext::new();
    let rows = tracked_rows(config, "bench-tracked-commit");
    write_tracked_root(backend, &context, "bench-tracked-commit", None, &rows).await?;

    let started = Instant::now();
    let mut misses = 0;
    let mut reader = context.reader(backend);
    for index in 0..config.rows {
        if reader
            .load_row_at_commit(
                "bench-tracked-commit",
                &TrackedStateRowRequest {
                    schema_key: tracked_schema_key(index, StorageBenchSelectivity::Percent100),
                    entity_id: EntityIdentity::single(format!("missing-{index}")),
                    file_id: NullableKeyFilter::Value("bench.json".to_string()),
                },
            )
            .await?
            .is_none()
        {
            misses += 1;
        }
    }
    Ok(report(config.rows, misses, started.elapsed()))
}

pub async fn tracked_state_scan_all(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = TrackedStateContext::new();
    let rows = tracked_rows(config, "bench-tracked-commit");
    write_tracked_root(backend, &context, "bench-tracked-commit", None, &rows).await?;

    let started = Instant::now();
    let verified_rows = scan_tracked(backend, &context, "bench-tracked-commit")
        .await?
        .len();
    Ok(report(config.rows, verified_rows, started.elapsed()))
}

pub async fn tracked_state_scan_schema(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = TrackedStateContext::new();
    let rows = tracked_rows(config, "bench-tracked-commit");
    write_tracked_root(backend, &context, "bench-tracked-commit", None, &rows).await?;

    let started = Instant::now();
    let mut reader = context.reader(backend);
    let verified_rows = reader
        .scan_rows_at_commit(
            "bench-tracked-commit",
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    schema_keys: vec![tracked_schema_key(0, StorageBenchSelectivity::Percent100)],
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await?
        .len();
    Ok(report(config.rows, verified_rows, started.elapsed()))
}

pub async fn tracked_state_scan_file(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = TrackedStateContext::new();
    let rows = tracked_rows(config, "bench-tracked-commit");
    write_tracked_root(backend, &context, "bench-tracked-commit", None, &rows).await?;

    let started = Instant::now();
    let mut reader = context.reader(backend);
    let verified_rows = reader
        .scan_rows_at_commit(
            "bench-tracked-commit",
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    file_ids: vec![NullableKeyFilter::Value("bench.json".to_string())],
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await?
        .len();
    Ok(report(config.rows, verified_rows, started.elapsed()))
}

pub async fn tracked_state_update_existing(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = TrackedStateContext::new();
    let rows = tracked_rows(config, "bench-tracked-parent");
    write_tracked_root(backend, &context, "bench-tracked-parent", None, &rows).await?;
    let mut updated_rows = tracked_rows(config, "bench-tracked-child");
    for (index, row) in updated_rows.iter_mut().enumerate() {
        row.snapshot_content = Some(updated_snapshot_content(index, config.state_payload_bytes));
    }

    let started = Instant::now();
    write_tracked_root(
        backend,
        &context,
        "bench-tracked-child",
        Some("bench-tracked-parent"),
        &updated_rows,
    )
    .await?;
    let elapsed = started.elapsed();
    let verified_rows = scan_tracked(backend, &context, "bench-tracked-child")
        .await?
        .len();
    Ok(report(updated_rows.len(), verified_rows, elapsed))
}

pub async fn untracked_state_write_rows(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let rows = untracked_rows(config);
    let context = UntrackedStateContext::new();
    let started = Instant::now();
    write_untracked_rows(backend, &context, &rows).await?;
    let elapsed = started.elapsed();
    let verified_rows = scan_untracked(backend, &context, UntrackedStateScanRequest::default())
        .await?
        .len();
    Ok(report(rows.len(), verified_rows, elapsed))
}

pub async fn untracked_state_read_point_hit(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = UntrackedStateContext::new();
    let rows = untracked_rows(config);
    write_untracked_rows(backend, &context, &rows).await?;

    let started = Instant::now();
    let mut verified_rows = 0;
    let mut reader = context.reader(backend);
    for index in 0..config.rows {
        if reader
            .load_row(&UntrackedStateRowRequest {
                schema_key: untracked_schema_key(index, StorageBenchSelectivity::Percent100),
                version_id: "bench-version".to_string(),
                entity_id: EntityIdentity::single(entity_id(
                    "untracked",
                    index,
                    config.key_pattern,
                )),
                file_id: NullableKeyFilter::Value("bench.json".to_string()),
            })
            .await?
            .is_some()
        {
            verified_rows += 1;
        }
    }
    Ok(report(config.rows, verified_rows, started.elapsed()))
}

pub async fn untracked_state_read_point_miss(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = UntrackedStateContext::new();
    let rows = untracked_rows(config);
    write_untracked_rows(backend, &context, &rows).await?;

    let started = Instant::now();
    let mut misses = 0;
    let mut reader = context.reader(backend);
    for index in 0..config.rows {
        if reader
            .load_row(&UntrackedStateRowRequest {
                schema_key: "bench_untracked_entity".to_string(),
                version_id: "bench-version".to_string(),
                entity_id: EntityIdentity::single(format!("missing-{index}")),
                file_id: NullableKeyFilter::Value("bench.json".to_string()),
            })
            .await?
            .is_none()
        {
            misses += 1;
        }
    }
    Ok(report(config.rows, misses, started.elapsed()))
}

pub async fn untracked_state_scan_all(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = UntrackedStateContext::new();
    let rows = untracked_rows(config);
    write_untracked_rows(backend, &context, &rows).await?;

    let started = Instant::now();
    let verified_rows = scan_untracked(backend, &context, UntrackedStateScanRequest::default())
        .await?
        .len();
    Ok(report(config.rows, verified_rows, started.elapsed()))
}

pub async fn untracked_state_scan_version(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = UntrackedStateContext::new();
    let rows = untracked_rows(config);
    write_untracked_rows(backend, &context, &rows).await?;

    let started = Instant::now();
    let verified_rows = scan_untracked(
        backend,
        &context,
        UntrackedStateScanRequest {
            filter: UntrackedStateFilter {
                version_ids: vec!["bench-version".to_string()],
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await?
    .len();
    Ok(report(config.rows, verified_rows, started.elapsed()))
}

pub async fn untracked_state_scan_schema(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = UntrackedStateContext::new();
    let rows = untracked_rows(config);
    write_untracked_rows(backend, &context, &rows).await?;

    let started = Instant::now();
    let verified_rows = scan_untracked(
        backend,
        &context,
        UntrackedStateScanRequest {
            filter: UntrackedStateFilter {
                schema_keys: vec![untracked_schema_key(0, StorageBenchSelectivity::Percent100)],
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await?
    .len();
    Ok(report(config.rows, verified_rows, started.elapsed()))
}

pub async fn untracked_state_overwrite_existing(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = UntrackedStateContext::new();
    let rows = untracked_rows(config);
    write_untracked_rows(backend, &context, &rows).await?;
    let mut updated_rows = untracked_rows(config);
    for (index, row) in updated_rows.iter_mut().enumerate() {
        row.snapshot_content = Some(updated_snapshot_content(index, config.state_payload_bytes));
    }

    let started = Instant::now();
    write_untracked_rows(backend, &context, &updated_rows).await?;
    let elapsed = started.elapsed();
    let verified_rows = scan_untracked(backend, &context, UntrackedStateScanRequest::default())
        .await?
        .len();
    Ok(report(updated_rows.len(), verified_rows, elapsed))
}

pub async fn changelog_append_changes(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let changes = changelog_changes(config);
    let context = ChangelogContext::new();
    let started = Instant::now();
    append_changelog_changes(backend, &context, &changes).await?;
    let elapsed = started.elapsed();
    let reader = context.reader(backend);
    let verified_rows = reader
        .scan_changes(&ChangelogScanRequest::default())
        .await?
        .len();
    Ok(report(changes.len(), verified_rows, elapsed))
}

pub async fn changelog_load_change_hit(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = ChangelogContext::new();
    let changes = changelog_changes(config);
    append_changelog_changes(backend, &context, &changes).await?;
    let reader = context.reader(backend);

    let started = Instant::now();
    let mut verified_rows = 0;
    for index in 0..config.rows {
        if reader
            .load_change(&format!("bench-change-{index}"))
            .await?
            .is_some()
        {
            verified_rows += 1;
        }
    }
    Ok(report(config.rows, verified_rows, started.elapsed()))
}

pub async fn changelog_load_change_miss(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = ChangelogContext::new();
    let changes = changelog_changes(config);
    append_changelog_changes(backend, &context, &changes).await?;
    let reader = context.reader(backend);

    let started = Instant::now();
    let mut misses = 0;
    for index in 0..config.rows {
        if reader
            .load_change(&format!("missing-change-{index}"))
            .await?
            .is_none()
        {
            misses += 1;
        }
    }
    Ok(report(config.rows, misses, started.elapsed()))
}

pub async fn changelog_scan_all(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = ChangelogContext::new();
    let changes = changelog_changes(config);
    append_changelog_changes(backend, &context, &changes).await?;
    let reader = context.reader(backend);

    let started = Instant::now();
    let verified_rows = reader
        .scan_changes(&ChangelogScanRequest::default())
        .await?
        .len();
    Ok(report(config.rows, verified_rows, started.elapsed()))
}

pub async fn changelog_scan_limit_100(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = ChangelogContext::new();
    let changes = changelog_changes(config);
    append_changelog_changes(backend, &context, &changes).await?;
    let reader = context.reader(backend);
    let expected = config.rows.min(100);

    let started = Instant::now();
    let verified_rows = reader
        .scan_changes(&ChangelogScanRequest {
            limit: Some(expected),
        })
        .await?
        .len();
    Ok(report(expected, verified_rows, started.elapsed()))
}

pub async fn binary_cas_write_blobs(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let payloads = binary_payloads(config.rows, config.blob_bytes);
    let file_ids = binary_file_ids(config.rows);
    let writes = binary_blob_writes(&file_ids, &payloads);
    let context = BinaryCasContext::new();

    let started = Instant::now();
    write_binary_blob_writes(backend, &context, &writes).await?;
    let elapsed = started.elapsed();
    let verified_rows = count_binary_cas_manifests(backend).await?;
    Ok(report(writes.len(), verified_rows, elapsed))
}

pub async fn binary_cas_read_blob_hit(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = BinaryCasContext::new();
    let payloads = binary_payloads(config.rows, config.blob_bytes);
    let file_ids = binary_file_ids(config.rows);
    let writes = binary_blob_writes(&file_ids, &payloads);
    write_binary_blob_writes(backend, &context, &writes).await?;
    let hashes = payloads
        .iter()
        .map(|payload| crate::binary_cas::binary_blob_hash_hex(payload))
        .collect::<Vec<_>>();

    let started = Instant::now();
    let mut verified_rows = 0;
    let mut reader = context.reader(backend);
    for hash in &hashes {
        if reader.load_blob_data_by_hash(hash).await?.is_some() {
            verified_rows += 1;
        }
    }
    Ok(report(hashes.len(), verified_rows, started.elapsed()))
}

pub async fn binary_cas_read_blob_miss(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = BinaryCasContext::new();
    let payloads = binary_payloads(config.rows, config.blob_bytes);
    let file_ids = binary_file_ids(config.rows);
    let writes = binary_blob_writes(&file_ids, &payloads);
    write_binary_blob_writes(backend, &context, &writes).await?;

    let started = Instant::now();
    let mut misses = 0;
    let mut reader = context.reader(backend);
    for index in 0..config.rows {
        let missing_hash = format!("{index:064x}");
        if reader
            .load_blob_data_by_hash(&missing_hash)
            .await?
            .is_none()
        {
            misses += 1;
        }
    }
    Ok(report(config.rows, misses, started.elapsed()))
}

pub async fn binary_cas_write_duplicate_payload(
    backend: &(dyn LixBackend + Send + Sync),
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let payload = binary_payload(0, config.blob_bytes);
    let payloads = (0..config.rows)
        .map(|_| payload.clone())
        .collect::<Vec<_>>();
    let file_ids = binary_file_ids(config.rows);
    let writes = binary_blob_writes(&file_ids, &payloads);
    let context = BinaryCasContext::new();

    let started = Instant::now();
    write_binary_blob_writes(backend, &context, &writes).await?;
    let elapsed = started.elapsed();
    let verified_rows = count_binary_cas_manifests(backend).await?;
    Ok(report(writes.len(), verified_rows, elapsed))
}

async fn write_tracked_root(
    backend: &(dyn LixBackend + Send + Sync),
    context: &TrackedStateContext,
    commit_id: &str,
    parent_commit_id: Option<&str>,
    rows: &[TrackedStateRow],
) -> Result<(), LixError> {
    let mut transaction = backend
        .begin_transaction(TransactionBeginMode::Write)
        .await?;
    {
        let mut writer = context.writer(transaction.as_mut());
        writer.write_root(commit_id, parent_commit_id, rows).await?;
    }
    transaction.commit().await
}

async fn scan_tracked(
    backend: &(dyn LixBackend + Send + Sync),
    context: &TrackedStateContext,
    commit_id: &str,
) -> Result<Vec<TrackedStateRow>, LixError> {
    let mut reader = context.reader(backend);
    reader
        .scan_rows_at_commit(commit_id, &TrackedStateScanRequest::default())
        .await
}

async fn write_untracked_rows(
    backend: &(dyn LixBackend + Send + Sync),
    context: &UntrackedStateContext,
    rows: &[UntrackedStateRow],
) -> Result<(), LixError> {
    let mut transaction = backend
        .begin_transaction(TransactionBeginMode::Write)
        .await?;
    {
        let mut writer = context.writer(transaction.as_mut());
        writer.write_rows(rows).await?;
    }
    transaction.commit().await
}

async fn scan_untracked(
    backend: &(dyn LixBackend + Send + Sync),
    context: &UntrackedStateContext,
    request: UntrackedStateScanRequest,
) -> Result<Vec<UntrackedStateRow>, LixError> {
    let mut reader = context.reader(backend);
    reader.scan_rows(&request).await
}

async fn append_changelog_changes(
    backend: &(dyn LixBackend + Send + Sync),
    context: &ChangelogContext,
    changes: &[CanonicalChange],
) -> Result<(), LixError> {
    let mut transaction = backend
        .begin_transaction(TransactionBeginMode::Write)
        .await?;
    {
        let mut writer = context.writer(transaction.as_mut());
        writer.append_changes(changes).await?;
    }
    transaction.commit().await
}

async fn write_binary_blob_writes(
    backend: &(dyn LixBackend + Send + Sync),
    context: &BinaryCasContext,
    writes: &[BinaryBlobWrite<'_>],
) -> Result<(), LixError> {
    let mut transaction = backend
        .begin_transaction(TransactionBeginMode::Write)
        .await?;
    {
        let mut writer = context.writer(transaction.as_mut());
        writer.put_blob_writes(writes).await?;
    }
    transaction.commit().await
}

async fn count_binary_cas_manifests(
    backend: &(dyn LixBackend + Send + Sync),
) -> Result<usize, LixError> {
    let context = BinaryCasContext::new();
    let mut reader = context.reader(backend);
    reader.count_blob_manifests().await
}

fn report(measured_rows: usize, verified_rows: usize, elapsed: Duration) -> StorageBenchReport {
    StorageBenchReport {
        measured_rows,
        verified_rows,
        elapsed,
    }
}

const TRACKED_MATCH_SCHEMA_KEY: &str = "bench_tracked_entity";
const TRACKED_OTHER_SCHEMA_KEY: &str = "bench_tracked_other_entity";
const UNTRACKED_MATCH_SCHEMA_KEY: &str = "bench_untracked_entity";
const UNTRACKED_OTHER_SCHEMA_KEY: &str = "bench_untracked_other_entity";

fn tracked_rows(config: StorageBenchConfig, commit_id: &str) -> Vec<TrackedStateRow> {
    (0..config.rows)
        .map(|index| TrackedStateRow {
            entity_id: EntityIdentity::single(entity_id("tracked", index, config.key_pattern)),
            schema_key: tracked_schema_key(index, config.selectivity),
            file_id: Some("bench.json".to_string()),
            snapshot_content: Some(snapshot_content(index, config.state_payload_bytes)),
            metadata: None,
            schema_version: "1".to_string(),
            created_at: timestamp(index),
            updated_at: timestamp(index),
            change_id: format!("tracked-change-{index}"),
            commit_id: commit_id.to_string(),
        })
        .collect()
}

fn untracked_rows(config: StorageBenchConfig) -> Vec<UntrackedStateRow> {
    (0..config.rows)
        .map(|index| UntrackedStateRow {
            entity_id: EntityIdentity::single(entity_id("untracked", index, config.key_pattern)),
            schema_key: untracked_schema_key(index, config.selectivity),
            file_id: Some("bench.json".to_string()),
            snapshot_content: Some(snapshot_content(index, config.state_payload_bytes)),
            metadata: None,
            schema_version: "1".to_string(),
            created_at: timestamp(index),
            updated_at: timestamp(index),
            global: false,
            version_id: "bench-version".to_string(),
        })
        .collect()
}

fn changelog_changes(config: StorageBenchConfig) -> Vec<CanonicalChange> {
    (0..config.rows)
        .map(|index| CanonicalChange {
            id: format!("bench-change-{index}"),
            entity_id: EntityIdentity::single(entity_id(
                "change-entity",
                index,
                config.key_pattern,
            )),
            schema_key: "bench_changelog_entity".to_string(),
            schema_version: "1".to_string(),
            file_id: Some("bench.json".to_string()),
            snapshot_content: Some(snapshot_content(index, config.state_payload_bytes)),
            metadata: None,
            created_at: timestamp(index),
        })
        .collect()
}

fn tracked_schema_key(index: usize, selectivity: StorageBenchSelectivity) -> String {
    if selectivity.matches(index) {
        TRACKED_MATCH_SCHEMA_KEY
    } else {
        TRACKED_OTHER_SCHEMA_KEY
    }
    .to_string()
}

fn untracked_schema_key(index: usize, selectivity: StorageBenchSelectivity) -> String {
    if selectivity.matches(index) {
        UNTRACKED_MATCH_SCHEMA_KEY
    } else {
        UNTRACKED_OTHER_SCHEMA_KEY
    }
    .to_string()
}

fn entity_id(prefix: &str, index: usize, key_pattern: StorageBenchKeyPattern) -> String {
    match key_pattern {
        StorageBenchKeyPattern::Sequential => format!("{prefix}-{index}"),
        StorageBenchKeyPattern::Random => format!("{prefix}-{:016x}", randomish_index(index)),
    }
}

fn randomish_index(index: usize) -> u64 {
    let mut value = index as u64;
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn binary_file_ids(rows: usize) -> Vec<String> {
    (0..rows)
        .map(|index| format!("bench-file-{index}"))
        .collect()
}

fn binary_payloads(rows: usize, blob_bytes: usize) -> Vec<Vec<u8>> {
    (0..rows)
        .map(|index| binary_payload(index, blob_bytes))
        .collect()
}

fn binary_half_duplicate_payloads(rows: usize, blob_bytes: usize) -> Vec<Vec<u8>> {
    (0..rows)
        .map(|index| {
            if index % 2 == 0 {
                binary_payload(0, blob_bytes)
            } else {
                binary_payload(index, blob_bytes)
            }
        })
        .collect()
}

fn binary_blob_writes<'a>(
    file_ids: &'a [String],
    payloads: &'a [Vec<u8>],
) -> Vec<BinaryBlobWrite<'a>> {
    payloads
        .iter()
        .enumerate()
        .map(|(index, payload)| BinaryBlobWrite {
            file_id: file_ids[index].as_str(),
            version_id: "bench-version",
            data: payload.as_slice(),
        })
        .collect()
}

fn snapshot_content(index: usize, target_bytes: usize) -> String {
    let mut value = serde_json::json!({
        "id": format!("entity-{index}"),
        "value": format!("value-{index}"),
        "index": index
    });
    pad_snapshot_content(&mut value, target_bytes);
    value.to_string()
}

fn updated_snapshot_content(index: usize, target_bytes: usize) -> String {
    let mut value = serde_json::json!({
        "id": format!("entity-{index}"),
        "value": format!("updated-{index}"),
        "index": index
    });
    pad_snapshot_content(&mut value, target_bytes);
    value.to_string()
}

fn partial_updated_snapshot_content(index: usize, target_bytes: usize) -> String {
    let mut value = serde_json::json!({
        "id": format!("entity-{index}"),
        "value": format!("value-{index}"),
        "index": index,
        "done": true
    });
    pad_snapshot_content(&mut value, target_bytes);
    value.to_string()
}

fn pad_snapshot_content(value: &mut serde_json::Value, target_bytes: usize) {
    let current = value.to_string().len();
    if target_bytes <= current {
        return;
    }
    value["padding"] = serde_json::Value::String("x".repeat(target_bytes - current));
}

fn timestamp(index: usize) -> String {
    format!(
        "2026-05-01T00:{:02}:{:02}.000Z",
        (index / 60) % 60,
        index % 60
    )
}

fn binary_payload(index: usize, len: usize) -> Vec<u8> {
    let mut payload = (0..len)
        .map(|offset| {
            ((index as u64)
                .wrapping_mul(31)
                .wrapping_add((offset as u64).wrapping_mul(17))
                & 0xff) as u8
        })
        .collect::<Vec<_>>();
    for (offset, byte) in (index as u64).to_le_bytes().into_iter().enumerate() {
        if offset < payload.len() {
            payload[offset] = byte;
        }
    }
    payload
}
