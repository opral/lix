//! Shared work bounds for authority-side native diff recipe preparation.

use std::sync::{Arc, Mutex};

/// Bounds changed-identity work and encoded key bytes while a
/// native read recipe is being proven. Ordinary SQL readers do not install a
/// budget and retain their existing semantics.
#[derive(Clone)]
pub(crate) struct NativeDiffIdentityBudget {
    remaining: Arc<Mutex<Remaining>>,
}

struct Remaining {
    identities: usize,
    visits: usize,
    encoded_key_bytes: usize,
}

/// Stable code for deterministic native-diff identity, visit, encoded-key,
/// and bounded-replay refusals. Keep allocation and output-size failures on
/// the broader exact-operation work-bound code.
pub(crate) const NATIVE_DIFF_RECIPE_WORK_BOUND_CODE: &str = "LIX_NATIVE_DIFF_RECIPE_WORK_BOUND";

const MAX_ENCODED_KEY_BYTES: usize = 4 * 1024 * 1024;

impl NativeDiffIdentityBudget {
    pub(crate) fn new(max_identities: usize) -> Self {
        Self {
            remaining: Arc::new(Mutex::new(Remaining {
                identities: max_identities,
                visits: max_identities.saturating_mul(4),
                encoded_key_bytes: MAX_ENCODED_KEY_BYTES,
            })),
        }
    }

    /// Charges authority-side traversal work before filtering or retaining an
    /// identity. Tree node loads use zero key bytes; decoded keys charge their
    /// encoded length against the shared byte budget.
    pub(crate) fn charge_visit(&self, encoded_key_bytes: usize) -> Result<(), crate::LixError> {
        let mut remaining = self.remaining.lock().map_err(|_| poisoned_budget_error())?;
        if remaining.visits == 0 || encoded_key_bytes > remaining.encoded_key_bytes {
            return Err(work_bound_error());
        }
        remaining.visits -= 1;
        remaining.encoded_key_bytes -= encoded_key_bytes;
        Ok(())
    }

    pub(crate) fn charge(&self, encoded_key_bytes: usize) -> Result<(), crate::LixError> {
        let mut remaining = self.remaining.lock().map_err(|_| poisoned_budget_error())?;
        if remaining.identities == 0 || encoded_key_bytes > remaining.encoded_key_bytes {
            return Err(work_bound_error());
        }
        remaining.identities -= 1;
        remaining.encoded_key_bytes -= encoded_key_bytes;
        Ok(())
    }

    pub(crate) fn remaining_identities(&self) -> Result<usize, crate::LixError> {
        self.remaining
            .lock()
            .map(|remaining| remaining.identities)
            .map_err(|_| poisoned_budget_error())
    }

    pub(crate) fn validate_key_size(encoded_key_bytes: usize) -> Result<(), crate::LixError> {
        if encoded_key_bytes > MAX_ENCODED_KEY_BYTES {
            return Err(work_bound_error());
        }
        Ok(())
    }
}

fn work_bound_error() -> crate::LixError {
    crate::LixError::new(
        NATIVE_DIFF_RECIPE_WORK_BOUND_CODE,
        "native diff recipe changed-identity work limit exceeded",
    )
}

fn poisoned_budget_error() -> crate::LixError {
    crate::LixError::new(
        crate::LixError::CODE_INTERNAL_ERROR,
        "native diff work budget lock is poisoned",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clones_share_the_identity_and_encoded_key_budgets() {
        let budget = NativeDiffIdentityBudget::new(2);
        let shared = budget.clone();

        budget.charge(17).unwrap();
        assert_eq!(shared.remaining_identities().unwrap(), 1);
        shared.charge(23).unwrap();
        let error = budget
            .charge(1)
            .expect_err("all clones must consume one shared identity cap");
        assert_eq!(error.code, NATIVE_DIFF_RECIPE_WORK_BOUND_CODE);
    }

    #[test]
    fn clones_share_the_traversal_budget() {
        let budget = NativeDiffIdentityBudget::new(1);
        let shared = budget.clone();
        for _ in 0..4 {
            shared.charge_visit(0).unwrap();
        }
        let error = budget
            .charge_visit(0)
            .expect_err("visits share the four-times identity work cap");
        assert_eq!(error.code, NATIVE_DIFF_RECIPE_WORK_BOUND_CODE);
    }

    #[test]
    fn oversized_encoded_identity_key_fails_closed() {
        let error = NativeDiffIdentityBudget::validate_key_size(MAX_ENCODED_KEY_BYTES + 1)
            .expect_err("one oversized identity cannot bypass the aggregate byte cap");
        assert_eq!(error.code, NATIVE_DIFF_RECIPE_WORK_BOUND_CODE);
    }
}
