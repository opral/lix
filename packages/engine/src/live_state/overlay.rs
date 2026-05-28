use std::collections::BTreeMap;

use crate::live_state::{LiveStateRowIdentity, MaterializedLiveStateRow};

/// Applies the local untracked overlay to tracked live-state rows.
///
/// The visible live-state contract is "latest local untracked row wins" for
/// the same branch/schema/entity/file identity. This keeps SQL providers from
/// knowing whether a visible row came from tracked changelog projection or from
/// local untracked state.
pub(crate) fn overlay_untracked_rows(
    tracked_rows: Vec<MaterializedLiveStateRow>,
    untracked_rows: Vec<MaterializedLiveStateRow>,
) -> Vec<MaterializedLiveStateRow> {
    let mut rows_by_identity = BTreeMap::new();

    for row in tracked_rows {
        rows_by_identity.insert(LiveStateRowIdentity::from_row(&row), row);
    }
    for row in untracked_rows {
        rows_by_identity.insert(LiveStateRowIdentity::from_row(&row), row);
    }

    rows_by_identity.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::ChangeId;

    #[test]
    fn untracked_row_wins_for_same_identity() {
        let tracked = live_row("tracked", false, Some("change-tracked"));
        let untracked = live_row("untracked", true, None);

        let rows = overlay_untracked_rows(vec![tracked], vec![untracked]);

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"untracked\"}")
        );
        assert!(rows[0].untracked);
        assert_eq!(rows[0].change_id, None);
    }

    #[test]
    fn different_identities_are_preserved() {
        let tracked = live_row("tracked", false, Some("change-tracked"));
        let mut untracked = live_row("untracked", true, None);
        untracked.entity_pk = crate::entity_pk::EntityPk::single("other");

        let rows = overlay_untracked_rows(vec![tracked], vec![untracked]);

        assert_eq!(rows.len(), 2);
    }

    fn live_row(value: &str, untracked: bool, change_id: Option<&str>) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: crate::entity_pk::EntityPk::single("entity"),
            schema_key: "schema".to_string(),
            file_id: None,
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}")),
            metadata: None,
            deleted: false,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global: true,
            change_id: change_id.map(ChangeId::for_test_label),
            commit_id: None,
            untracked,
            branch_id: "global".to_string(),
        }
    }
}
