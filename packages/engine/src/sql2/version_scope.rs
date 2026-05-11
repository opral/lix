use std::collections::BTreeSet;

use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;

use crate::version::VersionRefReader;
use crate::LixError;
use crate::GLOBAL_VERSION_ID;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum VersionBinding {
    Active { version_id: String },
    Explicit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WriteVersionScope {
    pub(crate) version_id: String,
    pub(crate) global: bool,
}

impl VersionBinding {
    pub(crate) fn active(version_id: impl Into<String>) -> Self {
        Self::Active {
            version_id: version_id.into(),
        }
    }

    pub(crate) fn explicit() -> Self {
        Self::Explicit
    }

    pub(crate) fn active_version_id(&self) -> Option<&str> {
        match self {
            Self::Active { version_id } => Some(version_id),
            Self::Explicit => None,
        }
    }

    pub(crate) fn require_active_version_id(&self, action: &str) -> Result<String, LixError> {
        match self {
            Self::Active { version_id } => Ok(version_id.clone()),
            Self::Explicit => Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("{action} is only supported for active-version SQL surfaces"),
            )),
        }
    }
}

pub(crate) fn resolve_write_version_scope(
    explicit_global: Option<bool>,
    explicit_version_id: Option<String>,
    fallback_version_id: Option<&str>,
    action: &str,
    surface: &str,
) -> Result<WriteVersionScope, DataFusionError> {
    if explicit_global == Some(true) {
        if explicit_version_id
            .as_deref()
            .is_some_and(|version_id| version_id != GLOBAL_VERSION_ID)
        {
            return Err(DataFusionError::Execution(format!(
                "{surface} cannot set lixcol_global=true with non-global lixcol_version_id"
            )));
        }
        return Ok(WriteVersionScope {
            version_id: GLOBAL_VERSION_ID.to_string(),
            global: true,
        });
    }

    let version_id = explicit_version_id
        .or_else(|| fallback_version_id.map(ToOwned::to_owned))
        .ok_or_else(|| {
            DataFusionError::Execution(format!("{action} requires lixcol_version_id"))
        })?;
    if explicit_global == Some(false) && version_id == GLOBAL_VERSION_ID {
        return Err(DataFusionError::Execution(format!(
            "{surface} cannot set lixcol_global=false with global lixcol_version_id"
        )));
    }
    Ok(WriteVersionScope {
        global: explicit_global.unwrap_or(version_id == GLOBAL_VERSION_ID),
        version_id,
    })
}

impl SqlVersionScope {
    pub(crate) fn from_provider(
        binding: &VersionBinding,
        requested_version_ids: Vec<String>,
    ) -> Self {
        match binding {
            VersionBinding::Active { version_id } => Self::Active(version_id.clone()),
            VersionBinding::Explicit if requested_version_ids.is_empty() => Self::AllVisible,
            VersionBinding::Explicit => Self::Explicit(requested_version_ids),
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
    binding: &VersionBinding,
    requested_version_ids: Vec<String>,
) -> Result<Vec<String>, LixError> {
    resolve_sql_version_scope(
        version_ref,
        SqlVersionScope::from_provider(binding, requested_version_ids),
    )
    .await
}

pub(crate) fn explicit_version_ids_from_dml_filters(filters: &[Expr]) -> Vec<String> {
    filters
        .iter()
        .flat_map(version_ids_from_filter)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn version_ids_from_filter(expr: &Expr) -> Vec<String> {
    match expr {
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::And => {
            let mut values = version_ids_from_filter(&binary_expr.left);
            values.extend(version_ids_from_filter(&binary_expr.right));
            values
        }
        Expr::BinaryExpr(binary_expr) => version_id_from_binary_filter(binary_expr)
            .map(|value| vec![value])
            .unwrap_or_default(),
        Expr::InList(in_list) => version_ids_from_in_list_filter(in_list).unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn version_id_from_binary_filter(binary_expr: &BinaryExpr) -> Option<String> {
    if binary_expr.op != Operator::Eq {
        return None;
    }

    version_id_from_column_literal_filter(&binary_expr.left, &binary_expr.right)
        .or_else(|| version_id_from_column_literal_filter(&binary_expr.right, &binary_expr.left))
}

fn version_ids_from_in_list_filter(in_list: &InList) -> Option<Vec<String>> {
    if in_list.negated {
        return None;
    }
    let Expr::Column(column) = in_list.expr.as_ref() else {
        return None;
    };
    if column.name != "lixcol_version_id" {
        return None;
    }

    let values = in_list
        .list
        .iter()
        .map(string_expr_literal)
        .collect::<Option<Vec<_>>>()?;
    if values.is_empty() {
        return None;
    }
    Some(values)
}

fn version_id_from_column_literal_filter(
    column_expr: &Expr,
    literal_expr: &Expr,
) -> Option<String> {
    let Expr::Column(column) = column_expr else {
        return None;
    };
    if column.name != "lixcol_version_id" {
        return None;
    }
    string_expr_literal(literal_expr)
}

fn string_expr_literal(expr: &Expr) -> Option<String> {
    let Expr::Literal(literal, _) = expr else {
        return None;
    };
    match literal {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Some(value.clone()),
        _ => None,
    }
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
    use crate::version::VersionHead;

    #[tokio::test]
    async fn active_scope_uses_session_version() {
        let version_ref = RowsVersionRefReader::new(Vec::new());
        let ids =
            resolve_provider_version_ids(&version_ref, &VersionBinding::active("main"), Vec::new())
                .await
                .expect("scope should resolve");

        assert_eq!(ids, vec!["main".to_string()]);
    }

    #[tokio::test]
    async fn explicit_scope_keeps_requested_versions() {
        let version_ref = RowsVersionRefReader::new(Vec::new());
        let ids = resolve_provider_version_ids(
            &version_ref,
            &VersionBinding::explicit(),
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
        let ids =
            resolve_provider_version_ids(&version_ref, &VersionBinding::explicit(), Vec::new())
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

    #[test]
    fn write_scope_uses_fallback_version_when_version_is_implicit() {
        let scope = resolve_write_version_scope(
            None,
            None,
            Some("active-version"),
            "INSERT into surface",
            "surface",
        )
        .expect("scope should resolve");

        assert_eq!(
            scope,
            WriteVersionScope {
                version_id: "active-version".to_string(),
                global: false,
            }
        );
    }

    #[test]
    fn write_scope_requires_version_without_fallback() {
        let error = resolve_write_version_scope(None, None, None, "INSERT into surface", "surface")
            .expect_err("missing version should be rejected");

        assert!(error
            .to_string()
            .contains("INSERT into surface requires lixcol_version_id"));
    }

    #[test]
    fn write_scope_derives_global_from_global_version_id() {
        let scope = resolve_write_version_scope(
            None,
            Some(GLOBAL_VERSION_ID.to_string()),
            None,
            "INSERT into surface",
            "surface",
        )
        .expect("scope should resolve");

        assert_eq!(
            scope,
            WriteVersionScope {
                version_id: GLOBAL_VERSION_ID.to_string(),
                global: true,
            }
        );
    }

    #[test]
    fn write_scope_rejects_non_global_with_global_version_id() {
        let error = resolve_write_version_scope(
            Some(false),
            Some(GLOBAL_VERSION_ID.to_string()),
            None,
            "INSERT into surface",
            "surface",
        )
        .expect_err("conflicting global/version scope should be rejected");

        assert!(error
            .to_string()
            .contains("surface cannot set lixcol_global=false with global lixcol_version_id"));
    }

    #[test]
    fn write_scope_rejects_global_with_non_global_version_id() {
        let error = resolve_write_version_scope(
            Some(true),
            Some("version-a".to_string()),
            None,
            "INSERT into surface",
            "surface",
        )
        .expect_err("conflicting global/version scope should be rejected");

        assert!(error
            .to_string()
            .contains("surface cannot set lixcol_global=true with non-global lixcol_version_id"));
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
