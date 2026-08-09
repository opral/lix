use crate::LixError;

/// Canonical identity check for a value returned by an exact-key read.
///
/// Implementations compare borrowed identity components. Construction of an
/// [`ExactBatch`] therefore adds no key clones, hashing, or index allocation.
pub(crate) trait ExactValue<K> {
    fn matches_exact_key(&self, key: &K) -> bool;
}

/// Opaque, identity-verified exact-read results in request order.
///
/// Missing values and duplicate requested keys remain distinct slots. The
/// request slice is borrowed, so the batch retains no second key allocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExactBatch<'keys, K, V> {
    requested: &'keys [K],
    values: Vec<Option<V>>,
}

impl<'keys, K, V> ExactBatch<'keys, K, V>
where
    V: ExactValue<K>,
{
    pub(crate) fn try_new(
        context: &str,
        requested: &'keys [K],
        values: Vec<Option<V>>,
    ) -> Result<Self, LixError> {
        if values.len() != requested.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "{context} exact read returned {} values for {} requested keys",
                    values.len(),
                    requested.len()
                ),
            ));
        }
        if let Some(index) = requested.iter().zip(&values).position(|(key, value)| {
            value
                .as_ref()
                .is_some_and(|value| !value.matches_exact_key(key))
        }) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "{context} exact read returned a value whose identity does not match requested key at slot {index}"
                ),
            ));
        }
        Ok(Self { requested, values })
    }
}

impl<'keys, K, V> IntoIterator for ExactBatch<'keys, K, V> {
    type Item = (&'keys K, Option<V>);
    type IntoIter = std::iter::Zip<std::slice::Iter<'keys, K>, std::vec::IntoIter<Option<V>>>;

    fn into_iter(self) -> Self::IntoIter {
        self.requested.iter().zip(self.values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct Value(u32);

    impl ExactValue<u32> for Value {
        fn matches_exact_key(&self, key: &u32) -> bool {
            self.0 == *key
        }
    }

    #[test]
    fn rejects_short_long_and_mismatched_results() {
        let keys = [1, 2];
        for values in [vec![Some(Value(1))], vec![Some(Value(1)), None, None]] {
            assert!(ExactBatch::try_new("test", &keys, values).is_err());
        }
        let error = ExactBatch::try_new("test", &keys, vec![Some(Value(2)), Some(Value(2))])
            .expect_err("mismatched identity must fail");
        assert!(
            error
                .to_string()
                .contains("does not match requested key at slot 0")
        );
    }

    #[test]
    fn preserves_duplicate_missing_and_requested_order() {
        let keys = [2, 1, 2, 3];
        let batch = ExactBatch::try_new(
            "test",
            &keys,
            vec![Some(Value(2)), Some(Value(1)), Some(Value(2)), None],
        )
        .expect("valid exact batch");
        assert_eq!(
            batch
                .into_iter()
                .map(|(key, value)| (*key, value.map(|value| value.0)))
                .collect::<Vec<_>>(),
            [(2, Some(2)), (1, Some(1)), (2, Some(2)), (3, None)]
        );
    }
}
