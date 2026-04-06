#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionMode {
    Read,
    Write,
    Deferred,
}
