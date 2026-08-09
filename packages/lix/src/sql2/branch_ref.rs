use std::sync::Arc;

use async_trait::async_trait;

use crate::LixError;
use crate::branch::{BranchHead, BranchRefMetadata, BranchRefReader};

/// Statement-scoped binding for a caller-provided prepared branch head.
///
/// The prepared head is an explicit input to a read-at-head operation, not a
/// cache. Every other branch lookup delegates to the caller-owned
/// ForkTree-backed reader, so this type never creates a second selector
/// authority or retains negative branch results.
pub(super) struct PreparedBranchRefReader {
    inner: Arc<dyn BranchRefReader>,
    prepared: BranchHead,
}

impl PreparedBranchRefReader {
    pub(super) fn new(inner: Arc<dyn BranchRefReader>, prepared: BranchHead) -> Self {
        Self { inner, prepared }
    }
}

#[async_trait]
impl BranchRefReader for PreparedBranchRefReader {
    async fn load_head(&self, branch_id: &str) -> Result<Option<BranchHead>, LixError> {
        let actual = self.inner.load_head(branch_id).await?;
        if branch_id != self.prepared.branch_id {
            return Ok(actual);
        }
        match actual {
            Some(actual) if actual == self.prepared => Ok(Some(actual)),
            Some(_) => Err(LixError::new(
                LixError::CODE_TRANSACTION_CONFLICT,
                "prepared SQL read head no longer matches the authenticated branch selector",
            )),
            None => Ok(None),
        }
    }

    async fn scan_heads(&self) -> Result<Vec<BranchHead>, LixError> {
        self.inner.scan_heads().await
    }

    async fn load_head_change_id(
        &self,
        branch_id: &str,
    ) -> Result<Option<crate::changelog::ChangeId>, LixError> {
        self.inner.load_head_change_id(branch_id).await
    }

    async fn load_head_metadata(
        &self,
        branch_id: &str,
    ) -> Result<Option<BranchRefMetadata>, LixError> {
        self.inner.load_head_metadata(branch_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::CommitId;

    struct CountingBranchRefReader {
        heads: Vec<BranchHead>,
    }

    impl CountingBranchRefReader {
        fn new(heads: Vec<BranchHead>) -> Self {
            Self { heads }
        }
    }

    #[async_trait]
    impl BranchRefReader for CountingBranchRefReader {
        async fn load_head(&self, branch_id: &str) -> Result<Option<BranchHead>, LixError> {
            Ok(self
                .heads
                .iter()
                .find(|head| head.branch_id == branch_id)
                .cloned())
        }

        async fn scan_heads(&self) -> Result<Vec<BranchHead>, LixError> {
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
    async fn prepared_head_requires_the_authenticated_selector() {
        let inner = Arc::new(CountingBranchRefReader::new(vec![head(
            "01920000-0000-7000-8000-0000000000a1",
            "commit-a",
        )]));
        let prepared = PreparedBranchRefReader::new(
            inner,
            head("01920000-0000-7000-8000-0000000000a1", "commit-prepared"),
        );

        let error = prepared
            .load_head("01920000-0000-7000-8000-0000000000a1")
            .await
            .unwrap_err();
        assert_eq!(error.code, LixError::CODE_TRANSACTION_CONFLICT);
    }

    #[tokio::test]
    async fn missing_prepared_selector_is_not_resurrected() {
        let inner = Arc::new(CountingBranchRefReader::new(Vec::new()));
        let prepared = PreparedBranchRefReader::new(
            inner,
            head("01920000-0000-7000-8000-0000000000a1", "commit-prepared"),
        );

        assert_eq!(
            prepared
                .load_head("01920000-0000-7000-8000-0000000000a1")
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn non_prepared_heads_delegate_to_the_same_reader() {
        let inner = Arc::new(CountingBranchRefReader::new(vec![head(
            "01920000-0000-7000-8000-0000000000a1",
            "commit-a",
        )]));
        let prepared = PreparedBranchRefReader::new(
            inner,
            head("01920000-0000-7000-8000-0000000000a1", "commit-prepared"),
        );

        assert_eq!(
            prepared.scan_heads().await.unwrap(),
            vec![head("01920000-0000-7000-8000-0000000000a1", "commit-a")]
        );
        assert_eq!(prepared.load_head("missing").await.unwrap(), None);
    }

    #[tokio::test]
    async fn prepared_head_does_not_override_other_branches() {
        let inner = Arc::new(CountingBranchRefReader::new(vec![head(
            "01920000-0000-7000-8000-0000000000b2",
            "commit-other",
        )]));
        let prepared = PreparedBranchRefReader::new(
            inner,
            head("01920000-0000-7000-8000-0000000000a1", "commit-prepared"),
        );

        assert_eq!(
            prepared
                .load_head("01920000-0000-7000-8000-0000000000b2")
                .await
                .unwrap(),
            Some(head("01920000-0000-7000-8000-0000000000b2", "commit-other",))
        );
    }
}
