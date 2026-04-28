use std::collections::BTreeSet;
use std::sync::Arc;

use crate::engine2::live_state::{LiveStateFilter, LiveStateReader, LiveStateScanRequest};
use crate::version::GLOBAL_VERSION_ID;
use crate::LixError;

/// Version scope requested by a SQL surface.
///
/// Active surfaces read through one session version. By-version surfaces either
/// read explicitly filtered versions or, without a version predicate, enumerate
/// every visible version scope before handing the request to live_state.
pub(crate) enum SqlVersionScope {
    Active(String),
    Explicit(Vec<String>),
    AllVisible,
}

impl SqlVersionScope {
    pub(crate) fn from_provider(
        default_version_id: Option<&str>,
        requested_version_ids: Vec<String>,
    ) -> Self {
        match default_version_id {
            Some(version_id) => Self::Active(version_id.to_string()),
            None if requested_version_ids.is_empty() => Self::AllVisible,
            None => Self::Explicit(requested_version_ids),
        }
    }
}

pub(crate) async fn resolve_sql_version_scope(
    live_state: Arc<dyn LiveStateReader>,
    scope: SqlVersionScope,
) -> Result<Vec<String>, LixError> {
    match scope {
        SqlVersionScope::Active(version_id) => Ok(vec![version_id]),
        SqlVersionScope::Explicit(version_ids) => Ok(version_ids),
        SqlVersionScope::AllVisible => visible_version_ids(live_state).await,
    }
}

pub(crate) async fn resolve_provider_version_ids(
    live_state: Arc<dyn LiveStateReader>,
    default_version_id: Option<&str>,
    requested_version_ids: Vec<String>,
) -> Result<Vec<String>, LixError> {
    resolve_sql_version_scope(
        live_state,
        SqlVersionScope::from_provider(default_version_id, requested_version_ids),
    )
    .await
}

async fn visible_version_ids(
    live_state: Arc<dyn LiveStateReader>,
) -> Result<Vec<String>, LixError> {
    let rows = live_state
        .scan_rows(&LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec!["lix_version_ref".to_string()],
                version_ids: vec![GLOBAL_VERSION_ID.to_string()],
                ..LiveStateFilter::default()
            },
            ..LiveStateScanRequest::default()
        })
        .await?;

    let mut version_ids = rows
        .into_iter()
        .map(|row| row.entity_id)
        .collect::<BTreeSet<_>>();
    version_ids.insert(GLOBAL_VERSION_ID.to_string());
    Ok(version_ids.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;
    use crate::engine2::live_state::{LiveStateRow, LiveStateRowRequest};

    #[tokio::test]
    async fn active_scope_uses_session_version() {
        let ids = resolve_provider_version_ids(
            Arc::new(RowsLiveStateReader::new(Vec::new())),
            Some("main"),
            Vec::new(),
        )
        .await
        .expect("scope should resolve");

        assert_eq!(ids, vec!["main".to_string()]);
    }

    #[tokio::test]
    async fn explicit_scope_keeps_requested_versions() {
        let ids = resolve_provider_version_ids(
            Arc::new(RowsLiveStateReader::new(Vec::new())),
            None,
            vec!["version-a".to_string(), "global".to_string()],
        )
        .await
        .expect("scope should resolve");

        assert_eq!(ids, vec!["version-a".to_string(), "global".to_string()]);
    }

    #[tokio::test]
    async fn all_visible_scope_loads_version_refs_and_global() {
        let ids = resolve_provider_version_ids(
            Arc::new(RowsLiveStateReader::new(vec![
                version_ref_row("version-b"),
                version_ref_row("version-a"),
            ])),
            None,
            Vec::new(),
        )
        .await
        .expect("scope should resolve");

        assert_eq!(
            ids,
            vec![
                "global".to_string(),
                "version-a".to_string(),
                "version-b".to_string(),
            ]
        );
    }

    struct RowsLiveStateReader {
        rows: Vec<LiveStateRow>,
    }

    impl RowsLiveStateReader {
        fn new(rows: Vec<LiveStateRow>) -> Self {
            Self { rows }
        }
    }

    #[async_trait]
    impl LiveStateReader for RowsLiveStateReader {
        async fn scan_rows(
            &self,
            _request: &LiveStateScanRequest,
        ) -> Result<Vec<LiveStateRow>, LixError> {
            Ok(self.rows.clone())
        }

        async fn load_row(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<LiveStateRow>, LixError> {
            Ok(None)
        }
    }

    fn version_ref_row(version_id: &str) -> LiveStateRow {
        LiveStateRow {
            entity_id: version_id.to_string(),
            schema_key: "lix_version_ref".to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some(format!(
                "{{\"id\":\"{version_id}\",\"commit_id\":\"commit-{version_id}\"}}"
            )),
            metadata: None,
            schema_version: "1".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global: true,
            change_id: None,
            commit_id: None,
            untracked: true,
            version_id: GLOBAL_VERSION_ID.to_string(),
        }
    }
}
