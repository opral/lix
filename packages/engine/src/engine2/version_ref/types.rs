/// Current changelog head for a version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VersionHead {
    pub(crate) version_id: String,
    pub(crate) commit_id: String,
}

/// Typed reader for moving version heads.
#[async_trait::async_trait]
pub(crate) trait VersionRefReader: Send + Sync {
    async fn load_head(&self, version_id: &str) -> Result<Option<VersionHead>, crate::LixError>;

    async fn load_head_commit_id(
        &self,
        version_id: &str,
    ) -> Result<Option<String>, crate::LixError> {
        Ok(self.load_head(version_id).await?.map(|head| head.commit_id))
    }

    async fn scan_heads(&self) -> Result<Vec<VersionHead>, crate::LixError>;
}
