//! Explicit current-state admission, independent of cache residency/registry.
#[derive(Clone, Debug)]
pub(crate) struct PartialReadScopePolicy {
    branches: std::sync::Arc<[String]>,
    preparation_epoch: Option<std::sync::Arc<str>>,
}
impl PartialReadScopePolicy {
    pub(crate) fn new(selected: &str, global: &str) -> Self {
        let mut branches = vec![selected.to_owned(), global.to_owned()];
        branches.sort();
        branches.dedup();
        Self {
            branches: branches.into(),
            preparation_epoch: None,
        }
    }
    // Only a live engine admitted against this immutable physical epoch sets
    // this token. Unpublished candidate readers intentionally leave it absent.
    pub(crate) fn set_preparation_epoch(&mut self, epoch: &str) {
        self.preparation_epoch = Some(epoch.into());
    }
    pub(crate) fn preparation_epoch(&self) -> Option<&str> {
        self.preparation_epoch.as_deref()
    }
    pub(crate) fn validate(&self, requested: &[String]) -> Result<(), crate::LixError> {
        if requested.is_empty() || requested.iter().any(|id| !self.branches.contains(id)) {
            return Err(crate::LixError::new(
                "LIX_PARTIAL_SCOPE_PREPARATION_REQUIRED",
                "requested branch is outside this partial replica's admitted selected/global branch scope",
            ));
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_branch_is_unknown_and_inventory_is_not_complete() {
        let policy = PartialReadScopePolicy::new("selected", "global");
        assert!(
            policy
                .validate(&["selected".into(), "global".into()])
                .is_ok()
        );
        assert!(policy.validate(&[]).is_err());
        assert!(policy.validate(&["missing".into()]).is_err());
        assert!(
            policy
                .validate(&["selected".into(), "missing".into()])
                .is_err()
        );
    }
}

/// An owner-injected durable admission decoder. The serving layer does not own
/// the receipt schema: it only reads this exact coordinate from its pinned view.
#[derive(Clone)]
pub(crate) struct PartialReadScopeSource {
    space: crate::storage_adapter::StorageSpace,
    key: crate::storage_adapter::StorageKey,
    max_bytes: usize,
    decode: std::sync::Arc<
        dyn Fn(&[u8]) -> Result<PartialReadScopePolicy, crate::LixError> + Send + Sync,
    >,
}
impl PartialReadScopeSource {
    pub(crate) fn new(
        space: crate::storage_adapter::StorageSpace,
        key: crate::storage_adapter::StorageKey,
        max_bytes: usize,
        decode: impl Fn(&[u8]) -> Result<PartialReadScopePolicy, crate::LixError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            space,
            key,
            max_bytes,
            decode: std::sync::Arc::new(decode),
        }
    }

    pub(crate) async fn load(
        &self,
        read: &(impl crate::storage_adapter::StorageAdapterRead + ?Sized),
    ) -> Result<PartialReadScopePolicy, crate::LixError> {
        let values =
            crate::storage_adapter::PointReadPlan::new(self.space, std::slice::from_ref(&self.key))
                .materialize(read, Default::default())
                .await?;
        let Some(crate::storage_adapter::StorageProjectedValue::FullValue(bytes)) =
            values.value.into_iter().next().flatten()
        else {
            return Err(crate::LixError::new(
                "LIX_PARTIAL_REPLICA_STATE_INVALID",
                "partial read admission is absent from the pinned storage view",
            ));
        };
        if bytes.len() > self.max_bytes {
            return Err(crate::LixError::new(
                "LIX_PARTIAL_REPLICA_STATE_INVALID",
                "partial read admission exceeds its metadata bound",
            ));
        }
        (self.decode)(&bytes)
    }
}
