use crate::changelog::ChangeId;
use crate::common::LixTimestamp;

/// Host-owned ordering key for the two non-base sides of a semantic conflict.
///
/// Branch merges and stale transaction reconciliation must present identical
/// changes to plugins in the same `a`/`b` order. Keeping that contract in one
/// typed key prevents either path from silently inventing a different tie
/// breaker. The durable change ID is primary because wall-clock timestamps may
/// be non-monotonic; the timestamp is only a defensive tie-breaker for corrupt
/// histories that reuse one change ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ConflictRank {
    change_id: ChangeId,
    updated_at: LixTimestamp,
}

impl ConflictRank {
    pub(crate) const fn new(updated_at: LixTimestamp, change_id: ChangeId) -> Self {
        Self {
            change_id,
            updated_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ConflictRank;
    use crate::changelog::ChangeId;
    use crate::common::LixTimestamp;

    #[test]
    fn change_id_precedes_non_monotonic_timestamp_in_durable_conflict_order() {
        let early = ConflictRank::new(
            LixTimestamp::from_unix_millis_utc_lossy(2),
            ChangeId::new(uuid::Uuid::from_u128(1)),
        );
        let late = ConflictRank::new(
            LixTimestamp::from_unix_millis_utc_lossy(1),
            ChangeId::new(uuid::Uuid::from_u128(2)),
        );
        assert!(early < late);
    }
}
