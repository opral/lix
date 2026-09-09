//! Transaction-local branch commands and their SQL constraint/ledger projection.
//!
//! The projection is never inserted into the mutable row buffer. Head publication
//! consumes typed targets; row-shaped data exists only for generic SQL constraints
//! and immutable public change facts.

use super::staging::{PreparedInsertSelection, PreparedWriteSet};
use crate::LixError;
use crate::branch::{BRANCH_REF_SCHEMA_KEY, BranchHeadTarget, BranchHeadWrite};
use crate::changelog::{ChangeRecord, CommitId};
use crate::transaction_types::{PreparedStateBatch, TransactionWriteMode};
use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::BTreeSet;

#[derive(Clone, Default)]
pub(crate) struct PreparedBranchHeads {
    pub(crate) targets: BTreeMap<String, BranchHeadTarget>,
    projection: Option<Box<BranchHeadProjection>>,
}

#[derive(Clone)]
struct BranchHeadProjection {
    rows: PreparedStateBatch,
    inserts: PreparedInsertSelection,
}

impl PreparedBranchHeads {
    pub(crate) fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }

    pub(crate) fn from_commands(
        commands: &[BranchHeadWrite],
        projection: PreparedStateBatch,
        mode: TransactionWriteMode,
    ) -> Result<Self, LixError> {
        let mut result = Self::default();
        let mut inserts = PreparedInsertSelection::new();
        inserts.resize_rows(projection.len());
        for (index, command) in commands.iter().enumerate() {
            let row = projection.row(index);
            if mode == TransactionWriteMode::Insert {
                inserts.mark(index, row.origin, None);
            }
            result.insert_target(&command.branch_id, command.head_commit_id, row)?;
        }
        result.projection = Some(Box::new(BranchHeadProjection {
            rows: projection,
            inserts,
        }));
        Ok(result)
    }

    fn insert_target(
        &mut self,
        branch_id: &str,
        head_commit_id: Option<CommitId>,
        row: crate::transaction_types::PreparedStateRowRef<'_>,
    ) -> Result<(), LixError> {
        let ref_change_id = row.change_id.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("explicit branch intent for '{branch_id}' has no change identity"),
            )
        })?;
        if self
            .targets
            .insert(
                branch_id.to_owned(),
                BranchHeadTarget {
                    head_commit_id,
                    ref_change_id,
                    created_at: row.created_at,
                    updated_at: row.updated_at,
                },
            )
            .is_some()
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "transaction contains multiple explicit branch-ref publications for branch '{branch_id}'"
                ),
            ));
        }
        Ok(())
    }

    /// Adapts low-level row-shaped fixtures to the production control boundary.
    #[cfg(test)]
    pub(crate) fn extract(
        rows: &mut PreparedStateBatch,
        inserts: &mut PreparedInsertSelection,
    ) -> Result<Self, LixError> {
        let selected: Vec<_> = rows
            .iter()
            .enumerate()
            .filter(|(_, row)| row.untracked && row.schema_key == BRANCH_REF_SCHEMA_KEY)
            .map(|(index, _)| index)
            .collect();
        if selected.is_empty() {
            return Ok(Self::default());
        }
        let mut result = Self::default();
        for &index in &selected {
            let row = rows.row(index);
            let branch_id = row.row_pk.as_single_string_owned()?;
            let typed = row.materialize_decoded_snapshot()?;
            let head_commit_id = match typed
                .as_deref()
                .and_then(|typed| typed.row.get("commit_id"))
            {
                Some(lix_schema::Value::Uuid(id)) => Some(CommitId::new(*id)),
                Some(lix_schema::Value::Text(id)) => {
                    Some(CommitId::parse_lix(id, "branch intent target")?)
                }
                _ => None,
            };
            if row.has_payload() && head_commit_id.is_none() {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("branch ref for branch '{branch_id}' is missing commit_id"),
                ));
            }
            result.insert_target(&branch_id, head_commit_id, row)?;
        }
        let mut projection = BranchHeadProjection {
            rows: rows.clone(),
            inserts: inserts.clone(),
        };
        projection.rows.select_rows(&selected);
        projection.inserts.select_rows(&selected);
        result.projection = Some(Box::new(projection));
        let selected = selected.into_iter().collect::<BTreeSet<_>>();
        let retained = (0..rows.len())
            .filter(|index| !selected.contains(index))
            .collect::<Vec<_>>();
        rows.select_rows(&retained);
        inserts.select_rows(&retained);
        Ok(result)
    }

    pub(crate) fn append(&mut self, other: Self) {
        if self.is_empty() {
            *self = other;
            return;
        }
        let Some(incoming) = other.projection else {
            return;
        };
        let current = self
            .projection
            .as_mut()
            .expect("nonempty intents have a projection");
        let mut insert_metadata = BTreeMap::new();
        for insert in current
            .inserts
            .iter(&current.rows)
            .chain(incoming.inserts.iter(&incoming.rows))
        {
            insert_metadata.insert(
                insert.row.row_pk.clone(),
                (insert.origin.cloned(), insert.statement_index),
            );
        }
        self.targets.extend(other.targets);
        current.rows.append(incoming.rows);
        // Repeated statements replace a target, just as ordinary staged rows do.
        // An earlier INSERT still requires absence validation after replacement.
        let mut latest = BTreeMap::new();
        for (index, row) in current.rows.iter().enumerate() {
            latest.insert(row.row_pk.clone(), index);
        }
        current
            .rows
            .select_rows(&latest.into_values().collect::<Vec<_>>());
        current.inserts = PreparedInsertSelection::new();
        current.inserts.resize_rows(current.rows.len());
        for (index, row) in current.rows.iter().enumerate() {
            if let Some((origin, statement_index)) = insert_metadata.get(row.row_pk) {
                current
                    .inserts
                    .mark(index, origin.as_ref(), *statement_index);
            }
        }
    }

    pub(crate) fn validation_projection(&self, writes: &PreparedWriteSet) -> PreparedWriteSet {
        let mut projected = writes.clone();
        let Some(projection) = &self.projection else {
            return projected;
        };
        let offset = projected.state_rows.len();
        projected
            .insert_selection
            .resize_rows(offset + projection.rows.len());
        for insert in projection.inserts.iter(&projection.rows) {
            projected.insert_selection.mark(
                offset + insert.row_index,
                insert.origin,
                insert.statement_index,
            );
        }
        projected.state_rows.append(projection.rows.clone());
        projected.branch_heads = Self::default();
        projected
    }

    pub(crate) fn changes(&self, account_id: &str) -> Vec<ChangeRecord> {
        self.projection
            .iter()
            .flat_map(|projection| projection.rows.iter())
            .map(|row| ChangeRecord {
                format_version: 2,
                change_id: row
                    .change_id
                    .expect("validated branch intent change identity"),
                account_id: account_id.to_owned(),
                row_pk: row.row_pk.clone(),
                schema_key: BRANCH_REF_SCHEMA_KEY.to_owned(),
                file_id: None,
                metadata: row.metadata.cloned(),
                snapshot: row.snapshot.map(<[u8]>::to_vec),
                created_at: row.updated_at,
                origin_key: row.origin_key.map(ToString::to_string),
            })
            .collect()
    }
}
