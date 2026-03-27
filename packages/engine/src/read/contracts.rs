use crate::sql::public::runtime::PreparedPublicRead;
use crate::TransactionMode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommittedReadMode {
    CommittedOnly,
    MaterializedState,
}

impl CommittedReadMode {
    pub(crate) fn transaction_mode(self) -> TransactionMode {
        match self {
            Self::CommittedOnly => TransactionMode::Read,
            Self::MaterializedState => TransactionMode::Deferred,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublicReadExecutionMode {
    PendingView,
    Committed(CommittedReadMode),
}

pub(crate) fn committed_read_mode_from_prepared_public_read(
    public_read: &PreparedPublicRead,
) -> CommittedReadMode {
    if public_read.effective_state_request().is_none()
        && public_read.effective_state_plan().is_none()
    {
        return CommittedReadMode::CommittedOnly;
    }

    CommittedReadMode::MaterializedState
}
