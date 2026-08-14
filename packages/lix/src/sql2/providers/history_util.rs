use crate::LixError;
use crate::common::SharedStr;
use crate::tracked_state::{MaterializedTrackedStateBatch, MaterializedTrackedStateRowRef};

/// Project a single-string history row pk as the canonical JSON array
/// text exposed by the `lixcol_row_pk` column.
pub(super) fn row_pk_json_array(row_pk: &str) -> Result<String, LixError> {
    serde_json::to_string(&[row_pk]).map_err(|error| {
        LixError::unknown(format!(
            "failed to encode history row pk as JSON: {error}"
        ))
    })
}

/// Compact address of one row retained by [`ObservedTrackedStateRows`].
///
/// File history can assemble one observed state from several point scans
/// (descriptors, ancestors, plugin rows). Keeping two ordinals per parsed
/// record avoids expanding each materialized batch into the legacy owned row
/// DTO while still allowing all source arenas to be released together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ObservedTrackedStateOrdinal {
    batch: u32,
    row: u32,
}

#[derive(Debug)]
struct ObservedTrackedStateBatch {
    observed_commit_id: SharedStr,
    rows: MaterializedTrackedStateBatch,
}

/// Owner for one or more exact historical scan batches.
///
/// The observed commit id is a shared buffer supplied once by the caller.
/// Every physical scan retains that same view, and parsed provider records
/// retain only compact ordinals into `batches`.
#[derive(Debug, Default)]
pub(super) struct ObservedTrackedStateRows {
    batches: Vec<ObservedTrackedStateBatch>,
    ordinals: Vec<ObservedTrackedStateOrdinal>,
}

impl ObservedTrackedStateRows {
    pub(super) fn from_batch(
        observed_commit_id: SharedStr,
        rows: MaterializedTrackedStateBatch,
    ) -> Result<Self, LixError> {
        let mut observed = Self::default();
        observed.push_batch(observed_commit_id, rows)?;
        Ok(observed)
    }

    pub(super) fn push_batch(
        &mut self,
        observed_commit_id: SharedStr,
        rows: MaterializedTrackedStateBatch,
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
            .extend((0..row_count).map(|row| ObservedTrackedStateOrdinal {
                batch,
                row: u32::try_from(row).expect("historical row count was checked above"),
            }));
        self.batches.push(ObservedTrackedStateBatch {
            observed_commit_id,
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
                ObservedTrackedStateOrdinal {
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

    pub(super) fn iter(&self) -> impl ExactSizeIterator<Item = ObservedTrackedStateRowRef<'_>> {
        self.ordinals
            .iter()
            .copied()
            .map(|ordinal| self.row(ordinal))
    }

    pub(super) fn row(
        &self,
        ordinal: ObservedTrackedStateOrdinal,
    ) -> ObservedTrackedStateRowRef<'_> {
        let batch = self
            .batches
            .get(ordinal.batch as usize)
            .expect("historical SQL batch ordinal belongs to its owner");
        ObservedTrackedStateRowRef {
            observed_commit_id: batch.observed_commit_id.as_str(),
            row: batch.rows.row(ordinal.row as usize),
            ordinal,
        }
    }

    #[cfg(test)]
    pub(super) fn retained_batch_count(&self) -> usize {
        self.batches.len()
    }

    #[cfg(test)]
    pub(super) fn observed_commit_buffer_identities(&self) -> Vec<(*const u8, usize)> {
        self.batches
            .iter()
            .map(|batch| batch.observed_commit_id.retained_buffer_identity())
            .collect()
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ObservedTrackedStateRowRef<'a> {
    observed_commit_id: &'a str,
    row: MaterializedTrackedStateRowRef<'a>,
    ordinal: ObservedTrackedStateOrdinal,
}

impl<'a> ObservedTrackedStateRowRef<'a> {
    pub(super) fn observed_commit_id(self) -> &'a str {
        self.observed_commit_id
    }

    pub(super) fn row(self) -> MaterializedTrackedStateRowRef<'a> {
        self.row
    }

    pub(super) fn ordinal(self) -> ObservedTrackedStateOrdinal {
        self.ordinal
    }
}
