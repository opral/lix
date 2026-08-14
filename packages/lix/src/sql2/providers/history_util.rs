use crate::LixError;
use crate::common::SharedStr;
use crate::forktree::{HistoricalStateRow, StateKey};
use crate::{NullableKeyFilter, row_pk::RowPk};

/// Native filter for historical ForkTree rows. This is local to the history
/// providers and carries no reader/request or materialized-batch vocabulary.
#[derive(Clone, Debug, Default)]
pub(super) struct StateFilter {
    pub(super) schema_keys: Vec<String>,
    pub(super) row_pks: Vec<RowPk>,
    pub(super) file_ids: Vec<NullableKeyFilter<String>>,
    pub(super) include_tombstones: bool,
}

/// Project a single-string history row pk as the canonical JSON array
/// text exposed by the `lixcol_row_pk` column.
pub(super) fn row_pk_json_array(row_pk: &str) -> Result<String, LixError> {
    serde_json::to_string(&[row_pk]).map_err(|error| {
        LixError::unknown(format!(
            "failed to encode history row pk as JSON: {error}"
        ))
    })
}

/// Compact address of one row retained by [`ObservedStateRows`].
///
/// File history can assemble one observed state from several point scans
/// (descriptors, ancestors, plugin rows). Keeping two ordinals per parsed
/// record avoids expanding each materialized batch into the legacy owned row
/// DTO while still allowing all source arenas to be released together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ObservedStateOrdinal {
    batch: u32,
    row: u32,
}

#[derive(Debug)]
struct ObservedStateBatch {
    observed_commit_id: SharedStr,
    terminal_snapshots: Vec<Option<SharedStr>>,
    rows: Vec<HistoricalStateRow>,
}

/// Owner for one or more exact historical scan batches.
///
/// The observed commit id is a shared buffer supplied once by the caller.
/// Every physical scan retains that same view, and parsed provider records
/// retain only compact ordinals into `batches`.
#[derive(Debug, Default)]
pub(super) struct ObservedStateRows {
    batches: Vec<ObservedStateBatch>,
    ordinals: Vec<ObservedStateOrdinal>,
}

impl ObservedStateRows {
    pub(super) fn from_rows(
        observed_commit_id: SharedStr,
        rows: Vec<HistoricalStateRow>,
    ) -> Result<Self, LixError> {
        let mut observed = Self::default();
        observed.push_batch(observed_commit_id, rows)?;
        Ok(observed)
    }

    pub(super) fn push_batch(
        &mut self,
        observed_commit_id: SharedStr,
        rows: Vec<HistoricalStateRow>,
    ) -> Result<(), LixError> {
        let batch = u32::try_from(self.batches.len()).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "historical SQL observed state exceeds u32 scan batches",
            )
        })?;
        let row_count = rows.len();
        let _: u32 = row_count.try_into().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "historical SQL observed state exceeds u32 rows in one scan batch",
            )
        })?;
        self.ordinals.reserve(row_count);
        self.ordinals
            .extend((0..row_count).map(|row| ObservedStateOrdinal {
                batch,
                row: u32::try_from(row).expect("historical row count was checked above"),
            }));
        let terminal_snapshots = rows
            .iter()
            .map(HistoricalStateRow::seed_snapshot_content)
            .collect::<Result<Vec<_>, _>>()?;
        self.batches.push(ObservedStateBatch {
            observed_commit_id,
            terminal_snapshots,
            rows,
        });
        Ok(())
    }

    pub(super) fn append(&mut self, other: Self) -> Result<(), LixError> {
        let batch_offset = u32::try_from(self.batches.len()).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "historical SQL observed state exceeds u32 scan batches",
            )
        })?;
        let final_batch_count = self
            .batches
            .len()
            .checked_add(other.batches.len())
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "historical SQL observed batch count overflow",
                )
            })?;
        if u32::try_from(final_batch_count).is_err() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "historical SQL observed state exceeds u32 scan batches",
            ));
        }
        self.ordinals.reserve(other.ordinals.len());
        self.ordinals
            .extend(other.ordinals.into_iter().map(|ordinal| {
                ObservedStateOrdinal {
                    batch: ordinal
                        .batch
                        .checked_add(batch_offset)
                        .expect("final observed batch count was checked above"),
                    row: ordinal.row,
                }
            }));
        self.batches.extend(other.batches);
        Ok(())
    }

    pub(super) fn iter(&self) -> impl ExactSizeIterator<Item = ObservedStateRowRef<'_>> {
        self.ordinals
            .iter()
            .copied()
            .map(|ordinal| self.row(ordinal))
    }

    pub(super) fn row(&self, ordinal: ObservedStateOrdinal) -> ObservedStateRowRef<'_> {
        let batch = self
            .batches
            .get(ordinal.batch as usize)
            .expect("historical SQL batch ordinal belongs to its owner");
        ObservedStateRowRef {
            observed_commit_id: batch.observed_commit_id.as_str(),
            snapshot_content: batch
                .terminal_snapshots
                .get(ordinal.row as usize)
                .and_then(Option::as_ref),
            row: batch
                .rows
                .get(ordinal.row as usize)
                .expect("historical row ordinal belongs to its owner"),
            ordinal,
        }
    }

    #[cfg(test)]
    pub(super) fn retained_batch_count(&self) -> usize {
        self.batches.len()
    }

    #[cfg(test)]
    pub(super) fn observed_commit_buffer_identitys(&self) -> Vec<(*const u8, usize)> {
        self.batches
            .iter()
            .map(|batch| batch.observed_commit_id.retained_buffer_identity())
            .collect()
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ObservedStateRowRef<'a> {
    observed_commit_id: &'a str,
    snapshot_content: Option<&'a SharedStr>,
    row: &'a HistoricalStateRow,
    ordinal: ObservedStateOrdinal,
}

impl<'a> ObservedStateRowRef<'a> {
    pub(super) fn observed_commit_id(self) -> &'a str {
        self.observed_commit_id
    }

    pub(super) fn row(self) -> HistoricalStateRowRef<'a> {
        HistoricalStateRowRef {
            row: self.row,
            snapshot_content: self.snapshot_content,
        }
    }

    pub(super) fn ordinal(self) -> ObservedStateOrdinal {
        self.ordinal
    }
}

#[derive(Clone, Copy)]
pub(super) struct HistoricalStateRowRef<'a> {
    row: &'a HistoricalStateRow,
    snapshot_content: Option<&'a SharedStr>,
}

impl<'a> HistoricalStateRowRef<'a> {
    pub(super) fn row_pk(self) -> &'a crate::row_pk::RowPk {
        &self.row.key.row_pk
    }

    pub(super) fn schema_key(self) -> &'a str {
        &self.row.key.schema_key
    }

    pub(super) fn file_id(self) -> Option<&'a str> {
        self.row.key.file_id.as_deref()
    }

    pub(super) fn snapshot_content(self) -> Option<&'a SharedStr> {
        self.snapshot_content
    }

    pub(super) fn deleted(self) -> bool {
        self.row.deleted
    }

    pub(super) fn key(self) -> &'a StateKey {
        &self.row.key
    }
}
