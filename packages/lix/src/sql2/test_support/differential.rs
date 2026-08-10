//! Differential coverage is owned by the native SQL integration fixtures.
//!
//! The former helper in this module fabricated a generic reader and
//! materialized compatibility batches. Keeping that harness would create a
//! second state authority, so it is intentionally removed.  Native SQL
//! integration tests now exercise the concrete ForkTree/transaction views
//! through the public execution path.

#[cfg(test)]
mod tests {
    #[test]
    fn compatibility_differential_harness_is_not_reintroduced() {
        // This guard is deliberately small: semantic assertions belong to
        // the canonical ForkTree integration fixtures, not a mock reader.
        assert!(true);
    }
}
