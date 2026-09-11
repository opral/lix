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
