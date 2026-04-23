/// Execution-facing catalog boundary consumed by `sql2`.
///
/// This stays intentionally minimal while the engine separates relation
/// metadata ownership from read execution. The concrete catalog context can
/// grow later without forcing `sql2` to own another long-lived runtime object.
#[allow(dead_code)]
pub(crate) trait CatalogContext {}

impl<T> CatalogContext for T {}
