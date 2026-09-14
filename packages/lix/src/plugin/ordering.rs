//! Shared fractional ordering for plugin rows.
//!
//! Keys are canonical lowercase hexadecimal strings ordered lexicographically.
//! Use `ORDER BY order_key, id`: concurrent allocations in the same gap can
//! produce the same key, so UUID identity provides a deterministic tie break.
//! Allocation does not reserve a gap or serialize concurrent insertion.
//! Open bounds mean prepend/append; interior allocation may lengthen keys.

mod key;
pub use key::OrderKey;

/// Allocate one key between exclusive bounds. `None` represents an open end.
pub fn order_between(previous: Option<&str>, next: Option<&str>) -> Result<String, String> {
    Ok(order_between_batch(previous, next, 1)?.remove(0))
}

/// Allocate an ordered batch without repeatedly narrowing the same gap.
/// Invalid, equal, or reversed bounds return an error, including for empty batches.
pub fn order_between_batch(
    previous: Option<&str>,
    next: Option<&str>,
    count: usize,
) -> Result<Vec<String>, String> {
    let previous = previous.map(OrderKey::from_snapshot_string).transpose()?;
    let next = next.map(OrderKey::from_snapshot_string).transpose()?;
    Ok(
        OrderKey::evenly_between(previous.as_ref(), next.as_ref(), count)?
            .iter()
            .map(OrderKey::to_snapshot_string)
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_helpers_allocate_batches_and_validate_bounds() {
        let keys = order_between_batch(Some("80"), Some("c0"), 100).unwrap();
        assert_eq!(keys.len(), 100);
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(keys.first().unwrap().as_str() > "80");
        assert!(keys.last().unwrap().as_str() < "c0");
        assert_eq!(order_between(None, None).unwrap(), "80");
        assert!(order_between_batch(Some("80"), Some("80"), 0).is_err());
        assert!(order_between_batch(Some("c0"), Some("80"), 0).is_err());
        assert!(order_between(Some("FF"), None).is_err());
    }
}
