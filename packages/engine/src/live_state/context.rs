use async_trait::async_trait;
use std::collections::BTreeSet;
use std::sync::Arc;

use crate::live_state::store::LiveStateBackendRef;
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackend, LixError, NullableKeyFilter, Value};

use super::{
    load_current_committed_version_frontier_with_backend, scan_live_rows, ExactRowRequest, LiveRow,
    LiveRowQuery, LiveRowSource, ScanConstraint, ScanField, ScanOperator,
};

/// Execution-facing live-state boundary consumed by `sql2`.
///
/// This API stays intentionally small and DataFusion-shaped:
/// providers/execution code should ask for scans and exact-row loads with
/// pushdown-like hints, rather than reaching into backend SQL or transaction
/// internals directly.
///
/// Overlay-aware transaction contexts can implement the same trait later
/// without changing the `sql2` call sites.
#[async_trait]
#[allow(dead_code)]
pub(crate) trait LiveStateContext: Send + Sync {
    async fn scan(&self, request: &LiveStateScanRequest) -> Result<Vec<LiveRow>, LixError>;

    async fn load_exact(&self, request: &ExactRowRequest) -> Result<Option<LiveRow>, LixError>;
}

/// Identity-centered filter for visible live entities.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct LiveStateFilter {
    #[serde(default)]
    pub schema_keys: Vec<String>,
    #[serde(default)]
    pub entity_ids: Vec<String>,
    #[serde(default)]
    pub version_ids: Vec<String>,
    #[serde(default)]
    pub file_ids: Vec<NullableKeyFilter<String>>,
    #[serde(default)]
    pub plugin_keys: Vec<NullableKeyFilter<String>>,
    #[serde(default)]
    pub constraints: Vec<ScanConstraint>,
    #[serde(default)]
    pub include_tombstones: bool,
}

/// Requested property set for a live-state scan.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct LiveStateProjection {
    #[serde(default)]
    pub columns: Vec<String>,
}

/// First-principles scan request for `sql2`-owned reads.
///
/// This is centered on visible entity identity and version scope rather than
/// SQL relation names. DataFusion providers above this boundary can translate
/// relation-specific pushdown into this request shape.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct LiveStateScanRequest {
    #[serde(default)]
    pub filter: LiveStateFilter,
    #[serde(default)]
    pub projection: LiveStateProjection,
    #[serde(default)]
    pub limit: Option<usize>,
}

impl LiveStateScanRequest {
    fn base_constraints(&self) -> Result<Vec<ScanConstraint>, LixError> {
        let mut constraints = self.filter.constraints.clone();
        push_text_identity_constraints(
            &mut constraints,
            ScanField::EntityId,
            &self.filter.entity_ids,
        );
        push_nullable_key_constraints(
            &mut constraints,
            ScanField::FileId,
            &self.filter.file_ids,
            "file_id",
        )?;
        push_nullable_key_constraints(
            &mut constraints,
            ScanField::PluginKey,
            &self.filter.plugin_keys,
            "plugin_key",
        )?;

        Ok(constraints)
    }
}

fn push_text_identity_constraints(
    constraints: &mut Vec<ScanConstraint>,
    field: ScanField,
    values: &[String],
) {
    match values {
        [] => {}
        [value] => constraints.push(ScanConstraint {
            field,
            operator: ScanOperator::Eq(Value::Text(value.clone())),
        }),
        many => constraints.push(ScanConstraint {
            field,
            operator: ScanOperator::In(many.iter().cloned().map(Value::Text).collect::<Vec<_>>()),
        }),
    }
}

fn push_nullable_key_constraints(
    constraints: &mut Vec<ScanConstraint>,
    field: ScanField,
    filters: &[NullableKeyFilter<String>],
    label: &str,
) -> Result<(), LixError> {
    match filters {
        [] => Ok(()),
        [NullableKeyFilter::Any] => Ok(()),
        [NullableKeyFilter::Null] => {
            constraints.push(ScanConstraint {
                field,
                operator: ScanOperator::Eq(Value::Null),
            });
            Ok(())
        }
        [NullableKeyFilter::Value(value)] => {
            constraints.push(ScanConstraint {
                field,
                operator: ScanOperator::Eq(Value::Text(value.clone())),
            });
            Ok(())
        }
        _ => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("CommittedLiveStateContext does not yet support multiple {label} filters"),
        )),
    }
}

/// Committed-state implementation used for normal engine reads.
#[allow(dead_code)]
pub(crate) struct CommittedLiveStateContext {
    backend: Arc<dyn LixBackend + Send + Sync>,
}

#[allow(dead_code)]
impl CommittedLiveStateContext {
    pub(crate) fn new(backend: Arc<dyn LixBackend + Send + Sync>) -> Self {
        Self { backend }
    }

    pub(crate) fn backend(&self) -> LiveStateBackendRef<'_> {
        self.backend.as_ref()
    }
}

#[async_trait]
impl LiveStateContext for CommittedLiveStateContext {
    async fn scan(&self, request: &LiveStateScanRequest) -> Result<Vec<LiveRow>, LixError> {
        let backend = self.backend();
        let version_ids = resolve_target_version_ids(backend, request).await?;
        let schema_keys = resolve_target_schema_keys(backend, request).await?;
        let constraints = request.base_constraints()?;
        let mut rows = Vec::new();

        for version_id in &version_ids {
            for schema_key in &schema_keys {
                let mut scanned = scan_live_rows(
                    backend,
                    &LiveRowQuery {
                        schema_key: schema_key.clone(),
                        version_id: version_id.clone(),
                        source: LiveRowSource::Effective,
                        constraints: constraints.clone(),
                        include_tombstones: request.filter.include_tombstones,
                    },
                )
                .await?;
                rows.append(&mut scanned);
                if let Some(limit) = request.limit {
                    if rows.len() >= limit {
                        rows.truncate(limit);
                        return Ok(rows);
                    }
                }
            }
        }

        hydrate_commit_ids(backend, &mut rows).await?;

        if let Some(limit) = request.limit {
            rows.truncate(limit);
        }

        Ok(rows)
    }

    async fn load_exact(&self, request: &ExactRowRequest) -> Result<Option<LiveRow>, LixError> {
        let query = super::ExactLiveRowQuery {
            source: LiveRowSource::Effective,
            schema_key: request.schema_key.clone(),
            version_id: request.version_id.clone(),
            entity_id: request.entity_id.clone(),
            file_id: request.file_id.clone(),
            schema_version: None,
            plugin_key: crate::NullableKeyFilter::Any,
            global: None,
            untracked: None,
            include_tombstones: false,
            include_global_overlay: true,
            include_untracked_overlay: true,
        };
        let backend = self.backend();
        let mut row = super::load_exact_live_row(backend, &query).await?;
        if let Some(row) = row.as_mut() {
            hydrate_commit_ids(backend, std::slice::from_mut(row)).await?;
        }
        Ok(row)
    }
}

async fn resolve_target_version_ids(
    backend: LiveStateBackendRef<'_>,
    request: &LiveStateScanRequest,
) -> Result<Vec<String>, LixError> {
    if !request.filter.version_ids.is_empty() {
        return Ok(request.filter.version_ids.clone());
    }

    let mut version_ids = load_current_committed_version_frontier_with_backend(backend)
        .await?
        .version_heads
        .into_keys()
        .collect::<Vec<_>>();
    version_ids.push(GLOBAL_VERSION_ID.to_string());
    version_ids.sort();
    version_ids.dedup();
    Ok(version_ids)
}

async fn resolve_target_schema_keys(
    backend: LiveStateBackendRef<'_>,
    request: &LiveStateScanRequest,
) -> Result<Vec<String>, LixError> {
    if !request.filter.schema_keys.is_empty() {
        return Ok(request.filter.schema_keys.clone());
    }

    let mut schema_keys = super::storage::load_live_storage_schema_keys(backend)
        .await?
        .into_iter()
        .collect::<Vec<_>>();
    schema_keys.sort();
    Ok(schema_keys)
}

async fn hydrate_commit_ids(
    backend: LiveStateBackendRef<'_>,
    rows: &mut [LiveRow],
) -> Result<(), LixError> {
    let change_ids = rows
        .iter()
        .filter(|row| row.commit_id.is_none())
        .filter_map(|row| row.change_id.as_ref())
        .filter(|change_id| !change_id.trim().is_empty())
        .cloned()
        .collect::<BTreeSet<_>>();

    if change_ids.is_empty() {
        return Ok(());
    }

    let commit_ids = super::storage::load_change_commit_id_map(backend, &change_ids).await?;
    for row in rows {
        if row.commit_id.is_some() {
            continue;
        }
        row.commit_id = row
            .change_id
            .as_ref()
            .and_then(|change_id| commit_ids.get(change_id).cloned());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{LiveStateFilter, LiveStateProjection, LiveStateScanRequest};

    #[test]
    fn live_state_scan_request_defaults_cleanly() {
        let request = LiveStateScanRequest::default();

        assert_eq!(request.filter, LiveStateFilter::default());
        assert_eq!(request.projection, LiveStateProjection::default());
        assert_eq!(request.limit, None);
    }
}
