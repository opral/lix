//! Commit-time validation of the state an explicit transaction's decisions
//! depended on.
//!
//! Explicit SQL reads and `UPDATE`/`DELETE` predicates are decisions made
//! against the transaction's opening snapshot. Instead of refusing every
//! commit whose branch moved after the transaction opened, the transaction
//! records the logical live-state reads those decisions issued (its *read
//! set*) and, when the branch did move, re-issues each read against both the
//! opening snapshot and the current snapshot. The commit conflicts only when
//! one of those reads would now return something different — a changed,
//! inserted (phantom) or deleted row — or when a row the transaction writes
//! changed concurrently (its *write set*). Everything else rebases onto the
//! new head through the ordinary stale-commit reconciliation.
//!
//! Reads are recorded at the hot-state reader boundary, so any provider that
//! reads live state through it is covered without per-provider knowledge.
//! Sources that expose history or branch control state (for example
//! `lix_branch`, `lix_change` or `lix_history`) cannot be validated row by
//! row. Reading one of them marks the read set as unvalidated, which keeps the
//! previous conservative behavior: any concurrent branch change conflicts.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use serde_json::{Value as JsonValue, json};

use crate::LixError;
use crate::NullableKeyFilter;
use crate::filesystem::FilesystemPathIndexRequest;
use crate::hot_state::{
    HotStateExactBatchRequest, HotStateExactRowRequest, HotStateFilter, HotStateReader,
    HotStateScanRequest, MaterializedHotStateBatch, MaterializedHotStateRowRef,
};
use crate::row_pk::RowPk;

/// Maximum number of overlapping identities reported in a conflict error.
const MAX_REPORTED_OVERLAPS: usize = 8;

/// One logical live-state read issued on behalf of a protected statement.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SqlReadFootprint {
    Scan(HotStateScanRequest),
    Exact(HotStateExactBatchRequest),
    PathIndex(FilesystemPathIndexRequest),
}

/// The reads that protected SQL decisions in one transaction depended on.
#[derive(Debug, Default)]
pub(crate) struct SqlReadSet {
    inner: Mutex<SqlReadSetInner>,
}

#[derive(Debug, Default)]
struct SqlReadSetInner {
    entries: Vec<SqlReadFootprint>,
    unvalidated: Option<String>,
}

/// State before one SQL statement began recording its reads.
pub(crate) struct SqlReadSetCheckpoint {
    entry_count: usize,
    unvalidated: Option<String>,
}

impl SqlReadSet {
    pub(crate) fn checkpoint(&self) -> SqlReadSetCheckpoint {
        let inner = self.lock();
        SqlReadSetCheckpoint {
            entry_count: inner.entries.len(),
            unvalidated: inner.unvalidated.clone(),
        }
    }

    pub(crate) fn restore(&self, checkpoint: SqlReadSetCheckpoint) {
        let mut inner = self.lock();
        inner.entries.truncate(checkpoint.entry_count);
        inner.unvalidated = checkpoint.unvalidated;
    }

    pub(crate) fn record_scan(&self, request: &HotStateScanRequest) {
        self.record(SqlReadFootprint::Scan(request.clone()));
    }

    pub(crate) fn record_exact(&self, request: &HotStateExactBatchRequest) {
        if request.rows.is_empty() {
            return;
        }
        self.record(SqlReadFootprint::Exact(request.clone()));
    }

    pub(crate) fn record_path_index(&self, request: &FilesystemPathIndexRequest) {
        if request
            .file_ids
            .as_ref()
            .is_some_and(Vec::is_empty)
        {
            return;
        }
        self.record(SqlReadFootprint::PathIndex(request.clone()));
    }

    /// Records that a protected statement read state which has no row-level
    /// validation, such as branch heads or change history.
    pub(crate) fn mark_unvalidated(&self, source: &str) {
        let mut inner = self.lock();
        if inner.unvalidated.is_none() {
            inner.unvalidated = Some(source.to_owned());
        }
    }

    pub(crate) fn unvalidated_source(&self) -> Option<String> {
        self.lock().unvalidated.clone()
    }

    pub(crate) fn entries(&self) -> Vec<SqlReadFootprint> {
        self.lock().entries.clone()
    }

    /// Branches explicitly named by recorded reads. Reads of another branch
    /// (for example through `*_by_branch` views) must be validated when that
    /// branch moves, even if the transaction's own branch did not.
    pub(crate) fn branch_ids(&self) -> BTreeSet<String> {
        let inner = self.lock();
        let mut branch_ids = BTreeSet::new();
        for entry in &inner.entries {
            match entry {
                SqlReadFootprint::Scan(request) => {
                    branch_ids.extend(request.filter.branch_ids.iter().cloned());
                }
                SqlReadFootprint::Exact(request) => {
                    branch_ids.extend(request.rows.iter().map(|row| row.branch_id.clone()));
                }
                SqlReadFootprint::PathIndex(request) => {
                    branch_ids.extend(request.branch_ids.iter().cloned());
                }
            }
        }
        branch_ids
    }

    fn record(&self, footprint: SqlReadFootprint) {
        let mut inner = self.lock();
        // Statements commonly repeat the same read back to back (for example
        // a scan followed by its overlay probe). Folding only adjacent
        // duplicates keeps recording O(1) on the statement hot path; any
        // remaining duplicates merely repeat a read during stale validation.
        if inner.entries.last() != Some(&footprint) {
            inner.entries.push(footprint);
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SqlReadSetInner> {
        // A poisoned recorder only means another statement panicked while
        // recording; the recorded entries remain a valid (conservative) set.
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Which part of the transaction observed a concurrent change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotOverlapKind {
    /// A row returned by (or matching) a protected read changed.
    Read,
    /// A row, or plugin-owned file, that the transaction writes changed.
    Write,
}

impl SnapshotOverlapKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

/// Identity of one live-state row that changed between the opening snapshot
/// and the commit snapshot.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SnapshotOverlap {
    pub(crate) branch_id: String,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) row_pk: Option<RowPk>,
}

impl SnapshotOverlap {
    fn to_json(&self) -> JsonValue {
        json!({
            "branchId": self.branch_id,
            "schemaKey": self.schema_key,
            "fileId": self.file_id,
            "rowPk": self
                .row_pk
                .as_ref()
                .and_then(|row_pk| row_pk.as_json_array_value().ok()),
        })
    }

    fn describe(&self) -> String {
        let mut text = format!("'{}'", self.schema_key);
        if let Some(row_pk) = self
            .row_pk
            .as_ref()
            .and_then(|row_pk| row_pk.as_json_array_text().ok())
        {
            text.push_str(&format!(" row {row_pk}"));
        }
        if let Some(file_id) = &self.file_id {
            text.push_str(&format!(" in file '{file_id}'"));
        }
        text
    }
}

/// Builds the public conflict error for rows that changed under a protected
/// read or write.
pub(crate) fn snapshot_overlap_conflict(
    kind: SnapshotOverlapKind,
    overlaps: &[SnapshotOverlap],
) -> LixError {
    let subject = match kind {
        SnapshotOverlapKind::Read => "a row this transaction read",
        SnapshotOverlapKind::Write => "a row this transaction writes",
    };
    let described = overlaps
        .first()
        .map(|overlap| format!(": {}", overlap.describe()))
        .unwrap_or_default();
    LixError::new(
        LixError::CODE_TRANSACTION_CONFLICT,
        format!("transaction conflict: {subject} was changed by a concurrent commit{described}"),
    )
    .with_hint("Retry the transaction against the latest committed state.")
    .with_details(json!({
        "retryable": true,
        "reason": match kind {
            SnapshotOverlapKind::Read => "readSetChanged",
            SnapshotOverlapKind::Write => "writeSetChanged",
        },
        "overlapKind": kind.as_str(),
        "overlaps": overlaps
            .iter()
            .take(MAX_REPORTED_OVERLAPS)
            .map(SnapshotOverlap::to_json)
            .collect::<Vec<_>>(),
        "overlapCount": overlaps.len(),
    }))
}

/// Builds the public conflict error for a protected read of state that has no
/// row-level validation.
pub(crate) fn unvalidated_read_conflict(source: &str) -> LixError {
    LixError::new(
        LixError::CODE_TRANSACTION_CONFLICT,
        format!(
            "transaction conflict: the branch changed after this transaction read '{source}', which cannot be validated row by row"
        ),
    )
    .with_hint("Retry the transaction against the latest committed state.")
    .with_details(json!({
        "retryable": true,
        "reason": "unvalidatedReadChanged",
        "source": source,
    }))
}

/// Re-issues each footprint against the opening and current snapshots and
/// returns the identities whose visible state differs.
pub(crate) async fn changed_footprint_rows(
    opening: &dyn HotStateReader,
    current: &dyn HotStateReader,
    footprints: &[SqlReadFootprint],
) -> Result<Vec<SnapshotOverlap>, LixError> {
    let mut overlaps = Vec::new();
    for footprint in footprints {
        let (before, after) = match footprint {
            SqlReadFootprint::Scan(request) => (
                fingerprint_batch(&opening.scan_batch(request).await?)?,
                fingerprint_batch(&current.scan_batch(request).await?)?,
            ),
            SqlReadFootprint::Exact(request) => {
                let before = opening.load_exact_batch(request).await?;
                let after = current.load_exact_batch(request).await?;
                let mut before_rows = BTreeMap::new();
                let mut after_rows = BTreeMap::new();
                for (slot, requested) in request.rows.iter().enumerate() {
                    let identity = exact_identity(requested);
                    before_rows.insert(
                        identity.clone(),
                        before.row(slot).map(fingerprint_row).transpose()?,
                    );
                    after_rows.insert(identity, after.row(slot).map(fingerprint_row).transpose()?);
                }
                (
                    flatten_optional(before_rows),
                    flatten_optional(after_rows),
                )
            }
            SqlReadFootprint::PathIndex(request) => (
                fingerprint_batch(&crate::filesystem::read_path_index_rows(opening, request).await?)?,
                fingerprint_batch(&crate::filesystem::read_path_index_rows(current, request).await?)?,
            ),
        };
        diff_fingerprints(&before, &after, &mut overlaps);
    }
    overlaps.sort();
    overlaps.dedup();
    Ok(overlaps)
}

/// Footprints covering every row a protected transaction writes.
#[derive(Debug, Default)]
pub(crate) struct WriteSetFootprints {
    /// Exact identities of the written rows.
    pub(crate) rows: Vec<SqlReadFootprint>,
    /// One scan per file that received plugin-owned rows. A plugin re-derives
    /// all of a file's rows from its content, so any concurrent change to the
    /// same file's plugin-owned rows invalidates the derivation even when the
    /// concrete row identities differ.
    pub(crate) files: Vec<SqlReadFootprint>,
}

/// Builds [`WriteSetFootprints`] for the written rows. `file_scoped` says
/// which schemas are derived per file.
pub(crate) fn write_set_footprints<'a>(
    rows: impl IntoIterator<Item = WrittenRowIdentity<'a>>,
    file_scoped: impl Fn(&str) -> bool,
) -> WriteSetFootprints {
    let mut exact = Vec::new();
    let mut seen = BTreeSet::new();
    let mut files = BTreeSet::<(String, String)>::new();
    for row in rows {
        let request = HotStateExactRowRequest {
            schema_key: row.schema_key.to_owned(),
            branch_id: row.branch_id.to_owned(),
            row_pk: row.row_pk.clone(),
            file_id: row.file_id.map(ToOwned::to_owned),
        };
        if seen.insert(request.clone()) {
            exact.push(request);
        }
        if let Some(file_id) = row.file_id
            && file_scoped(row.schema_key)
        {
            files.insert((row.branch_id.to_owned(), file_id.to_owned()));
        }
    }
    let mut footprints = WriteSetFootprints::default();
    if !exact.is_empty() {
        footprints
            .rows
            .push(SqlReadFootprint::Exact(HotStateExactBatchRequest {
                rows: exact,
                include_tombstones: true,
                ..HotStateExactBatchRequest::default()
            }));
    }
    for (branch_id, file_id) in files {
        footprints
            .files
            .push(SqlReadFootprint::Scan(HotStateScanRequest {
                filter: HotStateFilter {
                    branch_ids: vec![branch_id],
                    file_ids: vec![NullableKeyFilter::Value(file_id)],
                    ..HotStateFilter::default()
                },
                ..HotStateScanRequest::default()
            }));
    }
    footprints
}

/// Returns the changed rows of `footprints`, then of the per-file scans in
/// `write_set.files` restricted to `file_scoped` schemas. Rows of other
/// schemas that merely live in the same file (for example annotations with a
/// file scope) are not inputs of the plugin derivation.
pub(crate) async fn changed_write_set_rows(
    opening: &dyn HotStateReader,
    current: &dyn HotStateReader,
    write_set: &WriteSetFootprints,
    file_scoped: impl Fn(&str) -> bool,
) -> Result<Vec<SnapshotOverlap>, LixError> {
    let overlaps = changed_footprint_rows(opening, current, &write_set.rows).await?;
    if !overlaps.is_empty() {
        return Ok(overlaps);
    }
    for file in &write_set.files {
        let overlaps = changed_footprint_rows(opening, current, std::slice::from_ref(file))
            .await?
            .into_iter()
            .filter(|overlap| file_scoped(&overlap.schema_key))
            .collect::<Vec<_>>();
        if !overlaps.is_empty() {
            return Ok(overlaps);
        }
    }
    Ok(Vec::new())
}

/// Borrowed identity of one row in a transaction's write set.
#[derive(Debug, Clone, Copy)]
pub(crate) struct WrittenRowIdentity<'a> {
    pub(crate) branch_id: &'a str,
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) row_pk: &'a RowPk,
}

/// Visible state of one row that a decision could have depended on.
#[derive(Debug, Clone, PartialEq)]
struct RowFingerprint {
    deleted: bool,
    untracked: bool,
    global: bool,
    change_id: Option<crate::changelog::ChangeId>,
    snapshot: Option<JsonValue>,
    metadata: Option<String>,
}

type Fingerprints = BTreeMap<SnapshotOverlap, Vec<RowFingerprint>>;

fn fingerprint_batch(batch: &MaterializedHotStateBatch) -> Result<Fingerprints, LixError> {
    let mut rows = Fingerprints::new();
    for row in batch.iter() {
        rows.entry(row_identity(row))
            .or_default()
            .push(fingerprint_row(row)?);
    }
    Ok(rows)
}

fn fingerprint_row(row: MaterializedHotStateRowRef<'_>) -> Result<RowFingerprint, LixError> {
    Ok(RowFingerprint {
        deleted: row.deleted(),
        untracked: row.untracked(),
        global: row.global(),
        change_id: row.change_id(),
        snapshot: row.snapshot_json_value()?,
        metadata: row.metadata().map(ToString::to_string),
    })
}

fn row_identity(row: MaterializedHotStateRowRef<'_>) -> SnapshotOverlap {
    SnapshotOverlap {
        branch_id: row.branch_id().to_owned(),
        schema_key: row.schema_key().to_owned(),
        file_id: row.file_id().map(ToOwned::to_owned),
        row_pk: Some(row.row_pk().clone()),
    }
}

fn exact_identity(request: &HotStateExactRowRequest) -> SnapshotOverlap {
    SnapshotOverlap {
        branch_id: request.branch_id.clone(),
        schema_key: request.schema_key.clone(),
        file_id: request.file_id.clone(),
        row_pk: Some(request.row_pk.clone()),
    }
}

fn flatten_optional(rows: BTreeMap<SnapshotOverlap, Option<RowFingerprint>>) -> Fingerprints {
    rows.into_iter()
        .map(|(identity, row)| (identity, row.into_iter().collect()))
        .collect()
}

fn diff_fingerprints(before: &Fingerprints, after: &Fingerprints, out: &mut Vec<SnapshotOverlap>) {
    for (identity, rows) in before {
        if after.get(identity) != Some(rows) {
            out.push(identity.clone());
        }
    }
    for identity in after.keys() {
        if !before.contains_key(identity) {
            out.push(identity.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_set_folds_repeated_footprints() {
        let set = SqlReadSet::default();
        let request = HotStateScanRequest::default();
        set.record_scan(&request);
        set.record_scan(&request);
        set.record_exact(&HotStateExactBatchRequest::default());
        assert_eq!(set.entries(), vec![SqlReadFootprint::Scan(request)]);
        assert_eq!(set.unvalidated_source(), None);
        set.mark_unvalidated("lix_branch");
        set.mark_unvalidated("lix_change");
        assert_eq!(set.unvalidated_source().as_deref(), Some("lix_branch"));
    }

    #[test]
    fn restoring_failed_statement_reads_keeps_prior_reads() {
        let set = SqlReadSet::default();
        let prior = HotStateScanRequest::default();
        set.record_scan(&prior);
        let checkpoint = set.checkpoint();
        let mut failed = HotStateScanRequest::default();
        failed.filter.schema_keys.push("failed".to_owned());
        set.record_scan(&failed);
        set.mark_unvalidated("failed statement");
        set.restore(checkpoint);
        assert_eq!(set.entries(), vec![SqlReadFootprint::Scan(prior)]);
        assert_eq!(set.unvalidated_source(), None);
    }

    #[test]
    fn write_set_checks_plugin_rows_at_file_granularity() {
        let pk = RowPk::single("a");
        let footprints = write_set_footprints(
            [
                WrittenRowIdentity {
                    branch_id: "main",
                    schema_key: "plugin_row",
                    file_id: Some("file"),
                    row_pk: &pk,
                },
                WrittenRowIdentity {
                    branch_id: "main",
                    schema_key: "plain_row",
                    file_id: Some("other"),
                    row_pk: &pk,
                },
            ],
            |schema_key| schema_key == "plugin_row",
        );
        assert_eq!(footprints.rows.len(), 1);
        assert_eq!(footprints.files.len(), 1);
        let SqlReadFootprint::Scan(scan) = &footprints.files[0] else {
            panic!("expected file-scoped scan");
        };
        assert_eq!(
            scan.filter.file_ids,
            vec![NullableKeyFilter::Value("file".to_owned())]
        );
    }

    #[test]
    fn diff_reports_changed_inserted_and_deleted_rows() {
        let identity = |pk: &str| SnapshotOverlap {
            branch_id: "main".to_owned(),
            schema_key: "s".to_owned(),
            file_id: None,
            row_pk: Some(RowPk::single(pk)),
        };
        let row = |value: i64| RowFingerprint {
            deleted: false,
            untracked: false,
            global: false,
            change_id: None,
            snapshot: Some(json!({ "v": value })),
            metadata: None,
        };
        let before = Fingerprints::from([
            (identity("same"), vec![row(1)]),
            (identity("changed"), vec![row(1)]),
            (identity("deleted"), vec![row(1)]),
        ]);
        let after = Fingerprints::from([
            (identity("same"), vec![row(1)]),
            (identity("changed"), vec![row(2)]),
            (identity("inserted"), vec![row(1)]),
        ]);
        let mut overlaps = Vec::new();
        diff_fingerprints(&before, &after, &mut overlaps);
        overlaps.sort();
        assert_eq!(
            overlaps,
            vec![identity("changed"), identity("deleted"), identity("inserted")]
        );
    }
}
