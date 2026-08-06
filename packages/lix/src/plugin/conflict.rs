use crate::changelog::ChangeId;
use crate::common::LixTimestamp;

/// Host-owned ordering key for the two non-base sides of a semantic conflict.
///
/// Branch merges and stale transaction reconciliation must present identical
/// changes to plugins in the same `a`/`b` order. Keeping that contract in one
/// typed key prevents either path from silently inventing a different tie
/// breaker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ConflictRank {
    updated_at: LixTimestamp,
    change_id: ChangeId,
}

impl ConflictRank {
    pub(crate) const fn new(updated_at: LixTimestamp, change_id: ChangeId) -> Self {
        Self {
            updated_at,
            change_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ConflictRank;
    use crate::changelog::ChangeId;
    use crate::common::LixTimestamp;

    #[test]
    fn timestamp_precedes_change_id_in_durable_conflict_order() {
        let early = ConflictRank::new(
            LixTimestamp::from_unix_millis_utc_lossy(1),
            ChangeId::for_test_label("later-id"),
        );
        let late = ConflictRank::new(
            LixTimestamp::from_unix_millis_utc_lossy(2),
            ChangeId::for_test_label("earlier-id"),
        );
        assert!(early < late);
    }
}
