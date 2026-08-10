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

    async fn scan_head_metadata(&self) -> Result<Vec<(BranchHead, BranchRefMetadata)>, LixError> {
        self.inner.scan_head_metadata().await
    }

    async fn load_head_metadata_batch(
        &self,
        branch_ids: &[String],
    ) -> Result<Vec<(BranchHead, BranchRefMetadata)>, LixError> {
        let rows = self.inner.load_head_metadata_batch(branch_ids).await?;
        if branch_ids
            .iter()
            .any(|branch_id| branch_id == &self.prepared.branch_id)
        {
            match rows
                .iter()
                .find(|(head, _metadata)| head.branch_id == self.prepared.branch_id)
            {
                Some((head, _metadata)) if head == &self.prepared => {}
                Some(_) => {
                    return Err(LixError::new(
                        LixError::CODE_TRANSACTION_CONFLICT,
                        "prepared SQL metadata batch head no longer matches the authenticated branch selector",
                    ));
                }
                None => {
                    return Err(LixError::new(
                        LixError::CODE_TRANSACTION_CONFLICT,
                        "prepared SQL metadata batch branch selector is absent",
                    ));
                }
            }
        }
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::LixTimestamp;

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

        async fn load_head_metadata_batch(
            &self,
            branch_ids: &[String],
        ) -> Result<Vec<(BranchHead, BranchRefMetadata)>, LixError> {
            Ok(branch_ids
                .iter()
                .filter_map(|branch_id| {
                    self.heads
                        .iter()
                        .find(|head| &head.branch_id == branch_id)
                        .cloned()
                        .map(|head| {
                            (
                                head,
                                BranchRefMetadata {
                                    change_id: ChangeId::for_test_label("change"),
                                    updated_at: LixTimestamp::from_unix_millis_utc_lossy(0),
                                },
                            )
                        })
                })
                .collect())
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

    #[tokio::test]
    async fn prepared_metadata_batch_requires_the_authenticated_prepared_head() {
        let branch_id = "01920000-0000-7000-8000-0000000000a1";
        let inner = Arc::new(CountingBranchRefReader::new(vec![head(
            branch_id, "commit-a",
        )]));
        let prepared = PreparedBranchRefReader::new(inner, head(branch_id, "commit-prepared"));

        let error = prepared
            .load_head_metadata_batch(&[branch_id.to_owned()])
            .await
            .expect_err("a changed prepared head must fail closed");
        assert_eq!(error.code, LixError::CODE_TRANSACTION_CONFLICT);
    }

    #[tokio::test]
    async fn prepared_metadata_batch_rejects_missing_prepared_branch() {
        let branch_id = "01920000-0000-7000-8000-0000000000a1";
        let inner = Arc::new(CountingBranchRefReader::new(Vec::new()));
        let prepared = PreparedBranchRefReader::new(inner, head(branch_id, "commit-prepared"));

        let error = prepared
            .load_head_metadata_batch(&[branch_id.to_owned()])
            .await
            .expect_err("an absent prepared branch must fail closed");
        assert_eq!(error.code, LixError::CODE_TRANSACTION_CONFLICT);
    }

    #[tokio::test]
    async fn prepared_metadata_batch_accepts_the_exact_prepared_head() {
        let branch_id = "01920000-0000-7000-8000-0000000000a1";
        let prepared_head = head(branch_id, "commit-prepared");
        let inner = Arc::new(CountingBranchRefReader::new(vec![prepared_head.clone()]));
        let prepared = PreparedBranchRefReader::new(inner, prepared_head.clone());

        let rows = prepared
            .load_head_metadata_batch(&[branch_id.to_owned()])
            .await
            .expect("the exact prepared head must remain readable");
        assert_eq!(rows[0].0, prepared_head);
    }
}
