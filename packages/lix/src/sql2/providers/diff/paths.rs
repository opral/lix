//! Safe candidate routing for predicates over the two immutable path endpoints.
//! The original expression remains a residual filter (including SQL NULL rules).
use super::*;
use crate::filesystem::{FilesystemPathIndex, FilesystemPathKind, HistoricalPathIndexCache};

#[cfg(test)]
thread_local! { pub(super) static INDEX_BUILDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

#[derive(Clone, Debug)]
pub(super) enum PathRoute {
    Endpoint { after: bool, paths: Vec<String> },
    Union(Box<Self>, Box<Self>),
    Intersection(Box<Self>, Box<Self>),
}

impl PathRoute {
    pub(super) fn from_filters(filters: &[Expr]) -> Option<Self> {
        filters
            .iter()
            .filter_map(Self::expression)
            .reduce(|a, b| Self::Intersection(Box::new(a), Box::new(b)))
    }

    fn expression(expr: &Expr) -> Option<Self> {
        if let Expr::BinaryExpr(binary) = expr
            && matches!(binary.op, Operator::And | Operator::Or)
        {
            let left = Self::expression(&binary.left);
            let right = Self::expression(&binary.right);
            return match (binary.op, left, right) {
                (Operator::Or, Some(a), Some(b)) => Some(Self::Union(Box::new(a), Box::new(b))),
                (Operator::And, Some(a), Some(b)) => {
                    Some(Self::Intersection(Box::new(a), Box::new(b)))
                }
                // A bounded conjunct is a safe superset. An unbounded OR arm is not.
                (Operator::And, a, b) => a.or(b),
                _ => None,
            };
        }
        for (column, after) in [("from_path", false), ("to_path", true)] {
            if let Some(paths) = optional_values(std::slice::from_ref(expr), column) {
                return Some(Self::Endpoint { after, paths });
            }
        }
        None
    }

    fn needs(&self, after: bool) -> bool {
        match self {
            Self::Endpoint { after: side, paths } => *side == after && !paths.is_empty(),
            Self::Union(a, b) | Self::Intersection(a, b) => a.needs(after) || b.needs(after),
        }
    }

    fn candidates(
        &self,
        before: Option<&FilesystemPathIndex>,
        after_index: Option<&FilesystemPathIndex>,
    ) -> BTreeSet<String> {
        match self {
            Self::Endpoint { after, paths } => {
                let index = if *after { after_index } else { before };
                index
                    .into_iter()
                    .flat_map(|index| paths.iter().flat_map(|path| index.exact_entries(path)))
                    .filter(|entry| entry.kind == FilesystemPathKind::File)
                    .map(|entry| entry.id().to_owned())
                    .collect()
            }
            Self::Union(a, b) => {
                let mut ids = a.candidates(before, after_index);
                ids.extend(b.candidates(before, after_index));
                ids
            }
            Self::Intersection(a, b) => {
                let a = a.candidates(before, after_index);
                let b = b.candidates(before, after_index);
                a.intersection(&b).cloned().collect()
            }
        }
    }

    pub(super) async fn resolve<S: StorageAdapterRead + Clone>(
        &self,
        store: S,
        before: &str,
        after: &str,
        branch: &str,
        cache: Option<&HistoricalPathIndexCache>,
    ) -> Result<BTreeSet<String>> {
        async fn endpoint<S: StorageAdapterRead + Clone>(
            store: S,
            commit: &str,
            branch: &str,
            cache: Option<&HistoricalPathIndexCache>,
            needed: bool,
        ) -> Result<Option<Arc<FilesystemPathIndex>>> {
            if !needed {
                return Ok(None);
            }
            if let Some(index) = cache.and_then(|cache| cache.get(commit, branch)) {
                return Ok(Some(index));
            }
            #[cfg(test)]
            INDEX_BUILDS.with(|n| n.set(n.get() + 1));
            let index =
                super::super::state_at::historical_path_index(store, commit, branch).await?;
            if let Some(cache) = cache {
                cache.insert(commit, branch, index.clone());
            }
            Ok(Some(index))
        }
        let before = endpoint(store.clone(), before, branch, cache, self.needs(false)).await?;
        let after = endpoint(store, after, branch, cache, self.needs(true)).await?;
        Ok(self.candidates(before.as_deref(), after.as_deref()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit};
    #[test]
    fn unbounded_or_is_not_routed_but_bounded_conjunct_is() {
        let path = col("from_path").eq(lit("/old"));
        assert!(
            PathRoute::from_filters(&[path.clone().or(col("diff_type").eq(lit("added")))])
                .is_none()
        );
        assert!(
            PathRoute::from_filters(&[path.clone().and(col("diff_type").eq(lit("modified")))])
                .is_some()
        );
        let route = PathRoute::from_filters(&[path.or(col("to_path").eq(lit("/new")))]).unwrap();
        assert!(route.needs(false) && route.needs(true));
    }
}
