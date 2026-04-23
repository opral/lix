use async_trait::async_trait;

use crate::live_state::store::LiveStateBackendRef;
use crate::{LixError, NullableKeyFilter, Value};

use super::{
    scan_live_rows, ExactRowRequest, LiveRow, LiveRowQuery, LiveRowSource, ScanConstraint,
    ScanField, ScanOperator,
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
#[async_trait(?Send)]
#[allow(dead_code)]
pub(crate) trait LiveStateContext {
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
    fn try_into_live_row_query(&self) -> Result<LiveRowQuery, LixError> {
        let version_id = match self.filter.version_ids.as_slice() {
            [version_id] => version_id.clone(),
            [] => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "CommittedLiveStateContext currently requires exactly one version_id",
                ))
            }
            _ => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "CommittedLiveStateContext does not yet support multi-version scans",
                ))
            }
        };

        let schema_key = match self.filter.schema_keys.as_slice() {
            [schema_key] => schema_key.clone(),
            [] => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "CommittedLiveStateContext currently requires exactly one schema_key",
                ))
            }
            _ => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "CommittedLiveStateContext does not yet support multi-schema scans",
                ))
            }
        };

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

        Ok(LiveRowQuery {
            schema_key,
            version_id,
            source: LiveRowSource::Effective,
            constraints,
            include_tombstones: self.filter.include_tombstones,
        })
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
            operator: ScanOperator::In(
                many.iter().cloned().map(Value::Text).collect::<Vec<_>>(),
            ),
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
            format!(
                "CommittedLiveStateContext does not yet support multiple {label} filters"
            ),
        )),
    }
}

/// Committed-state implementation used for normal engine reads.
#[allow(dead_code)]
pub(crate) struct CommittedLiveStateContext<'a> {
    backend: LiveStateBackendRef<'a>,
}

#[allow(dead_code)]
impl<'a> CommittedLiveStateContext<'a> {
    pub(crate) fn new(backend: LiveStateBackendRef<'a>) -> Self {
        Self { backend }
    }

    pub(crate) fn backend(&self) -> LiveStateBackendRef<'a> {
        self.backend
    }
}

#[async_trait(?Send)]
impl LiveStateContext for CommittedLiveStateContext<'_> {
    async fn scan(&self, request: &LiveStateScanRequest) -> Result<Vec<LiveRow>, LixError> {
        let query = request.try_into_live_row_query()?;
        let mut rows = scan_live_rows(self.backend, &query).await?;
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
        super::load_exact_live_row(self.backend, &query).await
    }
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
