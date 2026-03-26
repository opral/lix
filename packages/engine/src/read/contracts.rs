#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommittedReadMode {
    PendingView,
    CommittedOnly,
    MaterializedState,
}
