use std::cmp::Ordering;

/// Replica-local replay boundary for derived-state catch-up.
///
/// This cursor only describes how far a particular engine instance has replayed
/// canonical storage into derived projections. It is not canonical meaning.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReplayCursor {
    pub change_id: String,
    pub created_at: String,
}

impl ReplayCursor {
    pub fn new(change_id: impl Into<String>, created_at: impl Into<String>) -> Self {
        Self {
            change_id: change_id.into(),
            created_at: created_at.into(),
        }
    }

    pub fn is_newer_than(&self, other: &Self) -> bool {
        self.cmp(other).is_gt()
    }
}

impl Ord for ReplayCursor {
    fn cmp(&self, other: &Self) -> Ordering {
        self.created_at
            .cmp(&other.created_at)
            .then_with(|| self.change_id.cmp(&other.change_id))
    }
}

impl PartialOrd for ReplayCursor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
