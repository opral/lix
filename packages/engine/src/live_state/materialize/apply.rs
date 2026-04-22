use std::collections::{BTreeMap, BTreeSet};

use super::types::{
    LiveStateApplyReport, LiveStateRebuildPlan, LiveStateRebuildScope, LiveStateWriteOp,
};
use crate::live_state::storage::quoted_live_table_name;
use crate::live_state::store::LiveStateTransactionRef;
use crate::live_state::LiveStateMode;
use crate::live_state::{
    mark_live_state_ready_at_latest_replay_cursor_in_transaction, register_schema_in_transaction,
};
use crate::LixError;

pub(crate) async fn apply_live_state_rebuild_plan_internal(
    transaction: LiveStateTransactionRef<'_>,
    plan: &LiveStateRebuildPlan,
) -> Result<LiveStateApplyReport, LixError> {
    crate::live_state::storage::set_live_state_mode_in_transaction(
        transaction,
        LiveStateMode::Rebuilding,
    )
    .await?;
    let (rows_deleted, tables_touched) =
        apply_live_state_scope_in_transaction(transaction, plan).await?;

    if matches!(plan.scope, LiveStateRebuildScope::Full) {
        mark_live_state_ready_at_latest_replay_cursor_in_transaction(transaction).await?;
    } else {
        crate::live_state::storage::set_live_state_mode_in_transaction(
            transaction,
            LiveStateMode::NeedsRebuild,
        )
        .await?;
    }

    Ok(LiveStateApplyReport {
        run_id: plan.run_id.clone(),
        rows_written: plan.writes.len(),
        rows_deleted,
        tables_touched: tables_touched.into_iter().collect(),
    })
}

pub(crate) async fn apply_live_state_scope_in_transaction(
    transaction: LiveStateTransactionRef<'_>,
    plan: &LiveStateRebuildPlan,
) -> Result<(usize, BTreeSet<String>), LixError> {
    let mut tables_touched = BTreeSet::new();

    let mut schema_lanes = BTreeMap::<String, BTreeSet<bool>>::new();
    for write in &plan.writes {
        schema_lanes
            .entry(write.schema_key.to_string())
            .or_default()
            .insert(write.untracked);
    }

    let rows_deleted =
        clear_scope_rows(transaction, &schema_lanes, &plan.scope, &mut tables_touched).await?;

    for write in &plan.writes {
        let table_name = quoted_live_table_name(&write.schema_key);
        tables_touched.insert(table_name.clone());

        if write.untracked && matches!(write.op, LiveStateWriteOp::Tombstone) {
            // Untracked live tables model deletion as absence, not as persisted
            // tombstone rows. Scope clearing has already removed the stale row.
            continue;
        }

        crate::live_state::storage::upsert_live_state_rebuild_row_in_transaction(
            transaction,
            write,
        )
        .await?;
    }

    Ok((rows_deleted, tables_touched))
}

async fn clear_scope_rows(
    transaction: LiveStateTransactionRef<'_>,
    schema_lanes: &BTreeMap<String, BTreeSet<bool>>,
    scope: &LiveStateRebuildScope,
    tables_touched: &mut BTreeSet<String>,
) -> Result<usize, LixError> {
    if schema_lanes.is_empty() {
        return Ok(0);
    }

    let version_filter = match scope {
        LiveStateRebuildScope::Full => None,
        LiveStateRebuildScope::Versions(versions) if versions.is_empty() => return Ok(0),
        LiveStateRebuildScope::Versions(versions) => Some(in_clause_values(versions)),
    };
    let mut rows_deleted = 0usize;

    for (schema_key, lanes) in schema_lanes {
        register_schema_in_transaction(transaction, schema_key.as_str()).await?;
        let table_name = quoted_live_table_name(schema_key);
        tables_touched.insert(table_name.clone());
        let lane_predicate = match (lanes.contains(&false), lanes.contains(&true)) {
            (true, true) => String::new(),
            (true, false) => " AND untracked = false".to_string(),
            (false, true) => " AND untracked = true".to_string(),
            (false, false) => continue,
        };

        rows_deleted += crate::live_state::storage::count_live_scope_rows_in_transaction(
            transaction,
            &table_name,
            version_filter.as_deref(),
            &lane_predicate,
        )
        .await?;
        crate::live_state::storage::delete_live_scope_rows_in_transaction(
            transaction,
            &table_name,
            version_filter.as_deref(),
            &lane_predicate,
        )
        .await?;
    }

    Ok(rows_deleted)
}

fn in_clause_values(values: &BTreeSet<String>) -> String {
    values
        .iter()
        .map(|value| {
            format!(
                "'{}'",
                crate::live_state::constraints::escape_sql_string(value)
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}
