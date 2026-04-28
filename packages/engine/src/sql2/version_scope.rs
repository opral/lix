use std::collections::BTreeSet;

use crate::engine2::version_ref::VersionRefReader;
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
    version_ref: &dyn VersionRefReader,
    scope: SqlVersionScope,
) -> Result<Vec<String>, LixError> {
    match scope {
        SqlVersionScope::Active(version_id) => Ok(vec![version_id]),
        SqlVersionScope::Explicit(version_ids) => Ok(version_ids),
        SqlVersionScope::AllVisible => visible_version_ids(version_ref).await,
    }
}

pub(crate) async fn resolve_provider_version_ids(
    version_ref: &dyn VersionRefReader,
    default_version_id: Option<&str>,
    requested_version_ids: Vec<String>,
) -> Result<Vec<String>, LixError> {
    resolve_sql_version_scope(
        version_ref,
        SqlVersionScope::from_provider(default_version_id, requested_version_ids),
    )
    .await
}

async fn visible_version_ids(version_ref: &dyn VersionRefReader) -> Result<Vec<String>, LixError> {
    let mut version_ids = version_ref
        .scan_heads()
        .await?
        .into_iter()
        .map(|head| head.version_id)
        .collect::<BTreeSet<_>>();
    version_ids.insert(GLOBAL_VERSION_ID.to_string());
    Ok(version_ids.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;
    use crate::engine2::version_ref::VersionHead;

    #[tokio::test]
    async fn active_scope_uses_session_version() {
        let version_ref = RowsVersionRefReader::new(Vec::new());
        let ids = resolve_provider_version_ids(&version_ref, Some("main"), Vec::new())
            .await
            .expect("scope should resolve");

        assert_eq!(ids, vec!["main".to_string()]);
    }

    #[tokio::test]
    async fn explicit_scope_keeps_requested_versions() {
        let version_ref = RowsVersionRefReader::new(Vec::new());
        let ids = resolve_provider_version_ids(
            &version_ref,
            None,
            vec!["version-a".to_string(), "global".to_string()],
        )
        .await
        .expect("scope should resolve");

        assert_eq!(ids, vec!["version-a".to_string(), "global".to_string()]);
    }

    #[tokio::test]
    async fn all_visible_scope_loads_version_refs_and_global() {
        let version_ref = RowsVersionRefReader::new(vec![
            VersionHead {
                version_id: "version-b".to_string(),
                commit_id: "commit-version-b".to_string(),
            },
            VersionHead {
                version_id: "version-a".to_string(),
                commit_id: "commit-version-a".to_string(),
            },
        ]);
        let ids = resolve_provider_version_ids(&version_ref, None, Vec::new())
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

    struct RowsVersionRefReader {
        heads: Vec<VersionHead>,
    }

    impl RowsVersionRefReader {
        fn new(heads: Vec<VersionHead>) -> Self {
            Self { heads }
        }
    }

    #[async_trait]
    impl VersionRefReader for RowsVersionRefReader {
        async fn load_head(&self, version_id: &str) -> Result<Option<VersionHead>, LixError> {
            Ok(self
                .heads
                .iter()
                .find(|head| head.version_id == version_id)
                .cloned())
        }

        async fn scan_heads(&self) -> Result<Vec<VersionHead>, LixError> {
            Ok(self.heads.clone())
        }
    }
}
