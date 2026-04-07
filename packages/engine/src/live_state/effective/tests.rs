use std::collections::{BTreeMap, BTreeSet};

use crate::schema::annotations::writer_key::WorkspaceWriterKeyReadView;
use crate::contracts::traits::{
    LiveReadContext as ReadContext, TrackedReadView, TrackedTombstoneView, UntrackedReadView,
};
use crate::live_state::constraints::{Bound, ScanConstraint, ScanField, ScanOperator};
use crate::live_state::effective::{
    EffectiveRowRequest, EffectiveRowState, EffectiveRowsRequest, OverlayLane,
};
use crate::live_state::shared::identity::RowIdentity;
use crate::live_state::tracked::{
    BatchTrackedRowRequest, TrackedRow, TrackedScanRequest, TrackedTombstoneMarker,
};
use crate::live_state::untracked::{BatchUntrackedRowRequest, UntrackedRow, UntrackedScanRequest};
use crate::{LixError, Value};
use async_trait::async_trait;

use super::resolve::{overlay_lanes_for_version, resolve_effective_row, resolve_effective_rows};

#[derive(Default)]
struct MockTrackedView {
    rows: Vec<TrackedRow>,
}

#[derive(Default)]
struct MockUntrackedView {
    rows: Vec<UntrackedRow>,
}

#[derive(Default)]
struct MockTrackedTombstones {
    rows: Vec<TrackedTombstoneMarker>,
}

#[derive(Default)]
struct MockWorkspaceWriterKeys {
    annotations: BTreeMap<RowIdentity, Option<String>>,
}

#[async_trait(?Send)]
impl TrackedReadView for MockTrackedView {
    async fn load_exact_rows(
        &self,
        request: &BatchTrackedRowRequest,
    ) -> Result<Vec<TrackedRow>, LixError> {
        Ok(self
            .rows
            .iter()
            .filter(|row| {
                row.schema_key == request.schema_key
                    && row.version_id == request.version_id
                    && request.entity_ids.contains(&row.entity_id)
                    && request
                        .file_id
                        .as_ref()
                        .is_none_or(|file_id| row.file_id == *file_id)
            })
            .cloned()
            .collect())
    }

    async fn scan_rows(&self, request: &TrackedScanRequest) -> Result<Vec<TrackedRow>, LixError> {
        Ok(self
            .rows
            .iter()
            .filter(|row| tracked_row_matches_scan(row, request))
            .cloned()
            .collect())
    }
}

#[async_trait(?Send)]
impl UntrackedReadView for MockUntrackedView {
    async fn load_exact_rows(
        &self,
        request: &BatchUntrackedRowRequest,
    ) -> Result<Vec<UntrackedRow>, LixError> {
        Ok(self
            .rows
            .iter()
            .filter(|row| {
                row.schema_key == request.schema_key
                    && row.version_id == request.version_id
                    && request.entity_ids.contains(&row.entity_id)
                    && request
                        .file_id
                        .as_ref()
                        .is_none_or(|file_id| row.file_id == *file_id)
            })
            .cloned()
            .collect())
    }

    async fn scan_rows(
        &self,
        request: &UntrackedScanRequest,
    ) -> Result<Vec<UntrackedRow>, LixError> {
        Ok(self
            .rows
            .iter()
            .filter(|row| untracked_row_matches_scan(row, request))
            .cloned()
            .collect())
    }
}

#[async_trait(?Send)]
impl TrackedTombstoneView for MockTrackedTombstones {
    async fn scan_tombstones(
        &self,
        request: &TrackedScanRequest,
    ) -> Result<Vec<TrackedTombstoneMarker>, LixError> {
        Ok(self
            .rows
            .iter()
            .filter(|row| tombstone_matches_scan(row, request))
            .cloned()
            .collect())
    }
}

#[async_trait(?Send)]
impl WorkspaceWriterKeyReadView for MockWorkspaceWriterKeys {
    async fn load_annotation(
        &self,
        row_identity: &RowIdentity,
    ) -> Result<Option<String>, LixError> {
        Ok(self.annotations.get(row_identity).cloned().unwrap_or(None))
    }

    async fn load_annotations(
        &self,
        row_identities: &BTreeSet<RowIdentity>,
    ) -> Result<BTreeMap<RowIdentity, Option<String>>, LixError> {
        Ok(row_identities
            .iter()
            .cloned()
            .map(|row_identity| {
                let annotation = self.annotations.get(&row_identity).cloned().unwrap_or(None);
                (row_identity, annotation)
            })
            .collect())
    }
}

fn tracked_row(entity_id: &str, version_id: &str, global: bool, child_id: &str) -> TrackedRow {
    TrackedRow {
        entity_id: entity_id.to_string(),
        schema_key: "lix_commit_edge".to_string(),
        schema_version: "1".to_string(),
        file_id: "lix".to_string(),
        version_id: version_id.to_string(),
        global,
        plugin_key: "lix".to_string(),
        metadata: Some("{\"kind\":\"tracked\"}".to_string()),
        change_id: Some(format!("chg-{entity_id}-{version_id}")),
        writer_key: Some("writer-a".to_string()),
        created_at: "2026-03-24T00:00:00Z".to_string(),
        updated_at: "2026-03-24T00:00:00Z".to_string(),
        values: BTreeMap::from([
            ("child_id".to_string(), Value::Text(child_id.to_string())),
            (
                "parent_id".to_string(),
                Value::Text(format!("parent-{entity_id}")),
            ),
        ]),
    }
}

fn untracked_row(entity_id: &str, version_id: &str, global: bool, child_id: &str) -> UntrackedRow {
    UntrackedRow {
        entity_id: entity_id.to_string(),
        schema_key: "lix_commit_edge".to_string(),
        schema_version: "1".to_string(),
        file_id: "lix".to_string(),
        version_id: version_id.to_string(),
        global,
        plugin_key: "lix".to_string(),
        metadata: Some("{\"kind\":\"untracked\"}".to_string()),
        writer_key: Some("writer-b".to_string()),
        created_at: "2026-03-24T00:00:00Z".to_string(),
        updated_at: "2026-03-24T00:00:00Z".to_string(),
        values: BTreeMap::from([
            ("child_id".to_string(), Value::Text(child_id.to_string())),
            (
                "parent_id".to_string(),
                Value::Text(format!("parent-{entity_id}")),
            ),
        ]),
    }
}

fn tombstone(entity_id: &str, version_id: &str, global: bool) -> TrackedTombstoneMarker {
    TrackedTombstoneMarker {
        entity_id: entity_id.to_string(),
        schema_key: "lix_commit_edge".to_string(),
        file_id: "lix".to_string(),
        version_id: version_id.to_string(),
        global,
        schema_version: Some("1".to_string()),
        plugin_key: Some("lix".to_string()),
        metadata: Some("{\"kind\":\"tombstone\"}".to_string()),
        writer_key: Some("writer-a".to_string()),
        created_at: Some("2026-03-24T00:05:00Z".to_string()),
        updated_at: Some("2026-03-24T00:05:00Z".to_string()),
        change_id: Some(format!("tomb-{entity_id}-{version_id}")),
    }
}

fn workspace_writer_keys(rows: &[TrackedRow]) -> MockWorkspaceWriterKeys {
    MockWorkspaceWriterKeys {
        annotations: rows
            .iter()
            .map(|row| (RowIdentity::from_tracked_row(row), row.writer_key.clone()))
            .collect(),
    }
}

#[tokio::test]
async fn effective_state_exact_prefers_local_untracked_first() {
    let tracked = MockTrackedView {
        rows: vec![
            tracked_row("edge-1", "main", false, "tracked-local"),
            tracked_row("edge-1", "global", true, "tracked-global"),
        ],
    };
    let untracked = MockUntrackedView {
        rows: vec![
            untracked_row("edge-1", "main", false, "untracked-local"),
            untracked_row("edge-1", "global", true, "untracked-global"),
        ],
    };
    let writer_keys = workspace_writer_keys(&tracked.rows);

    let resolved = resolve_effective_row(
        &EffectiveRowRequest {
            schema_key: "lix_commit_edge".to_string(),
            version_id: "main".to_string(),
            entity_id: "edge-1".to_string(),
            file_id: Some("lix".to_string()),
            include_global: true,
            include_untracked: true,
        },
        &ReadContext::new(&tracked, &untracked, &writer_keys),
    )
    .await
    .expect("effective exact lookup should succeed")
    .expect("winner should exist");

    assert_eq!(resolved.overlay_lane, OverlayLane::LocalUntracked);
    assert_eq!(
        resolved.property_text("child_id").as_deref(),
        Some("untracked-local")
    );
    assert!(resolved.untracked);
    assert!(!resolved.global);
}

#[tokio::test]
async fn effective_state_exact_tombstone_hides_global_fallback() {
    let tracked = MockTrackedView {
        rows: vec![tracked_row("edge-1", "global", true, "tracked-global")],
    };
    let untracked = MockUntrackedView::default();
    let tombstones = MockTrackedTombstones {
        rows: vec![tombstone("edge-1", "main", false)],
    };
    let writer_keys = workspace_writer_keys(&tracked.rows);

    let resolved = resolve_effective_row(
        &EffectiveRowRequest {
            schema_key: "lix_commit_edge".to_string(),
            version_id: "main".to_string(),
            entity_id: "edge-1".to_string(),
            file_id: Some("lix".to_string()),
            include_global: true,
            include_untracked: true,
        },
        &ReadContext::new(&tracked, &untracked, &writer_keys).with_tracked_tombstones(&tombstones),
    )
    .await
    .expect("effective exact lookup should succeed");

    assert!(resolved.is_none());
}

#[tokio::test]
async fn effective_state_scan_merges_lanes_and_projects_global_versions() {
    let tracked = MockTrackedView {
        rows: vec![
            tracked_row("edge-a", "main", false, "tracked-local"),
            tracked_row("edge-b", "global", true, "tracked-global"),
        ],
    };
    let untracked = MockUntrackedView {
        rows: vec![
            untracked_row("edge-a", "main", false, "untracked-local"),
            untracked_row("edge-c", "global", true, "untracked-global"),
        ],
    };
    let writer_keys = workspace_writer_keys(&tracked.rows);

    let resolved = resolve_effective_rows(
        &EffectiveRowsRequest {
            schema_key: "lix_commit_edge".to_string(),
            version_id: "main".to_string(),
            constraints: vec![ScanConstraint {
                field: ScanField::SchemaVersion,
                operator: ScanOperator::Range {
                    lower: Some(Bound {
                        value: Value::Text("1".to_string()),
                        inclusive: true,
                    }),
                    upper: Some(Bound {
                        value: Value::Text("1".to_string()),
                        inclusive: true,
                    }),
                },
            }],
            required_columns: vec!["child_id".to_string()],
            include_global: true,
            include_untracked: true,
            include_tombstones: false,
        },
        &ReadContext::new(&tracked, &untracked, &writer_keys),
    )
    .await
    .expect("effective scan should succeed");

    assert_eq!(resolved.rows.len(), 3);
    assert_eq!(resolved.rows[0].overlay_lane, OverlayLane::LocalUntracked);
    assert_eq!(
        resolved.rows[0].property_text("child_id").as_deref(),
        Some("untracked-local")
    );
    assert_eq!(resolved.rows[1].overlay_lane, OverlayLane::GlobalTracked);
    assert_eq!(resolved.rows[1].version_id, "main");
    assert_eq!(resolved.rows[1].source_version_id, "global");
    assert_eq!(resolved.rows[2].overlay_lane, OverlayLane::GlobalUntracked);
    assert_eq!(resolved.rows[2].version_id, "main");
    assert_eq!(resolved.rows[2].source_version_id, "global");
}

#[tokio::test]
async fn effective_state_scan_can_return_tombstones_when_requested() {
    let tracked = MockTrackedView {
        rows: vec![tracked_row("edge-b", "global", true, "tracked-global")],
    };
    let untracked = MockUntrackedView::default();
    let tombstones = MockTrackedTombstones {
        rows: vec![tombstone("edge-a", "main", false)],
    };
    let writer_keys = workspace_writer_keys(&tracked.rows);

    let resolved = resolve_effective_rows(
        &EffectiveRowsRequest {
            schema_key: "lix_commit_edge".to_string(),
            version_id: "main".to_string(),
            constraints: Vec::new(),
            required_columns: vec!["child_id".to_string()],
            include_global: true,
            include_untracked: true,
            include_tombstones: true,
        },
        &ReadContext::new(&tracked, &untracked, &writer_keys).with_tracked_tombstones(&tombstones),
    )
    .await
    .expect("effective scan with tombstones should succeed");

    assert_eq!(resolved.rows.len(), 2);
    assert_eq!(resolved.rows[0].entity_id, "edge-a");
    assert_eq!(resolved.rows[0].state, EffectiveRowState::Tombstone);
    assert_eq!(resolved.rows[0].overlay_lane, OverlayLane::LocalTracked);
    assert_eq!(resolved.rows[1].entity_id, "edge-b");
    assert_eq!(resolved.rows[1].state, EffectiveRowState::Visible);
}

#[test]
fn effective_state_global_version_skips_duplicate_global_lanes() {
    assert_eq!(
        overlay_lanes_for_version("global", true, true),
        vec![OverlayLane::LocalUntracked, OverlayLane::LocalTracked]
    );
}

#[tokio::test]
async fn effective_state_exact_overlays_workspace_writer_key_annotation() {
    let mut row = tracked_row("edge-1", "main", false, "tracked-local");
    row.writer_key = Some("raw-stale".to_string());
    let tracked = MockTrackedView { rows: vec![row] };
    let untracked = MockUntrackedView::default();
    let writer_keys = MockWorkspaceWriterKeys {
        annotations: BTreeMap::from([(
            RowIdentity::from_tracked_row(&tracked.rows[0]),
            Some("writer-overlay".to_string()),
        )]),
    };

    let resolved = resolve_effective_row(
        &EffectiveRowRequest {
            schema_key: "lix_commit_edge".to_string(),
            version_id: "main".to_string(),
            entity_id: "edge-1".to_string(),
            file_id: Some("lix".to_string()),
            include_global: false,
            include_untracked: false,
        },
        &ReadContext::new(&tracked, &untracked, &writer_keys),
    )
    .await
    .expect("effective exact lookup should succeed")
    .expect("winner should exist");

    assert_eq!(resolved.writer_key.as_deref(), Some("writer-overlay"));
}

fn tracked_row_matches_scan(row: &TrackedRow, request: &TrackedScanRequest) -> bool {
    row.schema_key == request.schema_key
        && row.version_id == request.version_id
        && constraints_match(
            &request.constraints,
            &row.entity_id,
            &row.file_id,
            &row.plugin_key,
            &row.schema_version,
        )
}

fn untracked_row_matches_scan(row: &UntrackedRow, request: &UntrackedScanRequest) -> bool {
    row.schema_key == request.schema_key
        && row.version_id == request.version_id
        && constraints_match(
            &request.constraints,
            &row.entity_id,
            &row.file_id,
            &row.plugin_key,
            &row.schema_version,
        )
}

fn tombstone_matches_scan(row: &TrackedTombstoneMarker, request: &TrackedScanRequest) -> bool {
    row.schema_key == request.schema_key
        && row.version_id == request.version_id
        && constraints_match(
            &request.constraints,
            &row.entity_id,
            &row.file_id,
            row.plugin_key.as_deref().unwrap_or(""),
            row.schema_version.as_deref().unwrap_or(""),
        )
}

fn constraints_match(
    constraints: &[ScanConstraint],
    entity_id: &str,
    file_id: &str,
    plugin_key: &str,
    schema_version: &str,
) -> bool {
    constraints.iter().all(|constraint| {
        let value = match constraint.field {
            ScanField::EntityId => Value::Text(entity_id.to_string()),
            ScanField::FileId => Value::Text(file_id.to_string()),
            ScanField::PluginKey => Value::Text(plugin_key.to_string()),
            ScanField::SchemaVersion => Value::Text(schema_version.to_string()),
        };
        operator_matches(&constraint.operator, &value)
    })
}

fn operator_matches(operator: &ScanOperator, value: &Value) -> bool {
    match operator {
        ScanOperator::Eq(expected) => expected == value,
        ScanOperator::In(values) => values.iter().any(|candidate| candidate == value),
        ScanOperator::Range { lower, upper } => {
            lower
                .as_ref()
                .is_none_or(|bound| compare_bound(value, &bound.value, bound.inclusive, true))
                && upper
                    .as_ref()
                    .is_none_or(|bound| compare_bound(value, &bound.value, bound.inclusive, false))
        }
    }
}

fn compare_bound(value: &Value, bound: &Value, inclusive: bool, lower: bool) -> bool {
    match (value, bound) {
        (Value::Text(value), Value::Text(bound)) => {
            if lower {
                if inclusive {
                    value >= bound
                } else {
                    value > bound
                }
            } else if inclusive {
                value <= bound
            } else {
                value < bound
            }
        }
        _ => false,
    }
}
