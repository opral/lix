#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionBeginMode {
    Read,
    Write,
    Deferred,
}
