use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::LixError;
use crate::branch::{BranchHead, BranchRefReader};

/// Statement-scoped branch-ref reader that avoids resolving the same branch
/// head through the backend more than once.
pub(super) struct CachingBranchRefReader {
    inner: Arc<dyn BranchRefReader>,
    heads: Mutex<HashMap<String, Option<BranchHead>>>,
}

impl CachingBranchRefReader {
    pub(super) fn new(inner: Arc<dyn BranchRefReader>) -> Self {
        Self {
            inner,
            heads: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl BranchRefReader for CachingBranchRefReader {
    async fn load_head(&self, branch_id: &str) -> Result<Option<BranchHead>, LixError> {
        let mut heads = self.heads.lock().await;
        if let Some(head) = heads.get(branch_id) {
            return Ok(head.clone());
        }

        let head = self.inner.load_head(branch_id).await?;
        heads.insert(branch_id.to_string(), head.clone());
        Ok(head)
    }

    async fn scan_heads(&self) -> Result<Vec<BranchHead>, LixError> {
        let mut cache = self.heads.lock().await;
        let heads = self.inner.scan_heads().await?;
        for head in &heads {
            cache.insert(head.branch_id.clone(), Some(head.clone()));
        }
        Ok(heads)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::changelog::CommitId;

    struct CountingBranchRefReader {
        heads: Vec<BranchHead>,
        load_count: AtomicUsize,
        scan_count: AtomicUsize,
    }

    impl CountingBranchRefReader {
        fn new(heads: Vec<BranchHead>) -> Self {
            Self {
                heads,
                load_count: AtomicUsize::new(0),
                scan_count: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl BranchRefReader for CountingBranchRefReader {
        async fn load_head(&self, branch_id: &str) -> Result<Option<BranchHead>, LixError> {
            self.load_count.fetch_add(1, Ordering::Relaxed);
            Ok(self
                .heads
                .iter()
                .find(|head| head.branch_id == branch_id)
                .cloned())
        }

        async fn scan_heads(&self) -> Result<Vec<BranchHead>, LixError> {
            self.scan_count.fetch_add(1, Ordering::Relaxed);
            Ok(self.heads.clone())
        }
    }

    fn head(branch_id: &str, commit_id: &str) -> BranchHead {
        BranchHead {
            branch_id: branch_id.to_string(),
            commit_id: CommitId::for_test_label(commit_id),
        }
    }

    #[tokio::test]
    async fn repeated_load_head_uses_underlying_reader_once() {
        let inner = Arc::new(CountingBranchRefReader::new(vec![head(
            "branch-a", "commit-a",
        )]));
        let cached = CachingBranchRefReader::new(inner.clone());

        assert_eq!(
            cached.load_head("branch-a").await.unwrap(),
            Some(head("branch-a", "commit-a"))
        );
        assert_eq!(
            cached.load_head("branch-a").await.unwrap(),
            Some(head("branch-a", "commit-a"))
        );
        assert_eq!(inner.load_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn missing_load_head_is_cached_as_none() {
        let inner = Arc::new(CountingBranchRefReader::new(Vec::new()));
        let cached = CachingBranchRefReader::new(inner.clone());

        assert_eq!(cached.load_head("missing").await.unwrap(), None);
        assert_eq!(cached.load_head("missing").await.unwrap(), None);
        assert_eq!(inner.load_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn scan_heads_seeds_load_head_cache() {
        let inner = Arc::new(CountingBranchRefReader::new(vec![head(
            "branch-a", "commit-a",
        )]));
        let cached = CachingBranchRefReader::new(inner.clone());

        assert_eq!(
            cached.scan_heads().await.unwrap(),
            vec![head("branch-a", "commit-a")]
        );
        assert_eq!(
            cached.load_head("branch-a").await.unwrap(),
            Some(head("branch-a", "commit-a"))
        );
        assert_eq!(inner.scan_count.load(Ordering::Relaxed), 1);
        assert_eq!(inner.load_count.load(Ordering::Relaxed), 0);
    }
}
