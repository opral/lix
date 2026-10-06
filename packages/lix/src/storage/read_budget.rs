//! Call-wide admission for provider results. Limits count logical returned
//! bytes, including duplicate slots, before allocating the result payloads.
use super::{ProjectedValue, StorageError};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadBudget {
    pub max_result_bytes: usize,
    pub max_single_value_bytes: usize,
}
impl ReadBudget {
    pub const UNBOUNDED: Self = Self {
        max_result_bytes: usize::MAX,
        max_single_value_bytes: usize::MAX,
    };
    pub fn admit_value(
        self,
        bytes: usize,
        retained: usize,
        singleton: bool,
    ) -> Result<usize, StorageError> {
        if bytes > self.max_single_value_bytes {
            return Err(StorageError::ReadBudgetExceeded { singleton: true });
        }
        let total = retained
            .checked_add(bytes)
            .ok_or(StorageError::ReadBudgetExceeded { singleton: false })?;
        // A separately charged codec member may occupy a singleton result.
        if total > self.max_result_bytes && !(singleton && retained == 0) {
            return Err(StorageError::ReadBudgetExceeded { singleton: false });
        }
        Ok(total)
    }
    pub fn validate_result(self, values: &[Option<ProjectedValue>]) -> Result<(), StorageError> {
        let mut bytes = 0;
        for value in values.iter().flatten() {
            if let ProjectedValue::FullValue(value) = value {
                bytes = self.admit_value(value.len(), bytes, values.len() == 1)?;
            }
        }
        Ok(())
    }
}

/// One exact ordered prefix of point slots. `next_offset` indexes the original
/// flattened request list, preserving missing and duplicate slots.
#[derive(Debug)]
pub struct GetManyPrefixResult {
    pub values: Vec<Option<ProjectedValue>>,
    pub next_offset: Option<usize>,
}
impl GetManyPrefixResult {
    pub fn new(
        values: Vec<Option<ProjectedValue>>,
        offset: usize,
        total: usize,
    ) -> Result<Self, StorageError> {
        let next = offset
            .checked_add(values.len())
            .ok_or(StorageError::InvalidCursor)?;
        if next > total || (values.is_empty() && next < total) {
            return Err(StorageError::InvalidCursor);
        }
        Ok(Self {
            values,
            next_offset: (next < total).then_some(next),
        })
    }
}
/// A bounded coordinate window; this does not fetch or clone payloads.
pub fn bounded_prefix_requests<'a>(
    requests: &'a [super::GetManyRequest<'_>],
    offset: usize,
    max_slots: usize,
) -> Result<(Vec<super::GetManyRequest<'a>>, usize), StorageError> {
    let total = requests.iter().try_fold(0usize, |count, request| {
        count
            .checked_add(request.keys.len())
            .ok_or(StorageError::InvalidKey)
    })?;
    if offset > total || max_slots == 0 {
        return Err(StorageError::InvalidCursor);
    }
    let mut skip = offset;
    let mut left = max_slots.min(super::MAX_SCAN_PAGE_ROWS);
    let mut page = Vec::new();
    for request in requests {
        let begin = skip.min(request.keys.len());
        skip -= begin;
        let count = (request.keys.len() - begin).min(left);
        if count != 0 {
            page.push(super::GetManyRequest {
                space: request.space,
                keys: &request.keys[begin..begin + count],
                opts: request.opts,
            });
            left -= count;
        }
        if left == 0 {
            break;
        }
    }
    Ok((page, total))
}
impl ReadBudget {
    /// Greedy admission uses size metadata, never owned payload values.
    pub fn admitted_prefix(
        self,
        lengths: impl IntoIterator<Item = usize>,
    ) -> Result<usize, StorageError> {
        let mut slots = 0usize;
        let mut bytes = 0usize;
        for len in lengths {
            match self.admit_value(len, bytes, slots == 0) {
                Ok(next) => {
                    bytes = next;
                    slots += 1;
                }
                Err(error) if slots == 0 => return Err(error),
                Err(_) => break,
            }
            if bytes > self.max_result_bytes {
                break;
            }
        }
        Ok(slots)
    }
}
