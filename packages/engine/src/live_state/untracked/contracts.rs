use async_trait::async_trait;

pub use crate::contracts::artifacts::{
    BatchRowRequest as BatchUntrackedRowRequest, ExactRowRequest as ExactUntrackedRowRequest,
    ScanRequest as UntrackedScanRequest, UntrackedRow, UntrackedWriteBatch,
    UntrackedWriteOperation, UntrackedWriteRow,
};
#[cfg(test)]
pub(crate) use crate::contracts::traits::UntrackedReadView;
pub(crate) use crate::contracts::traits::UntrackedWriteParticipant;
use crate::{LixBackendTransaction, LixError};
#[cfg(test)]
use crate::LixBackend;

#[cfg(test)]
#[async_trait(?Send)]
impl<T> UntrackedReadView for T
where
    T: LixBackend,
{
    async fn load_exact_rows(
        &self,
        request: &BatchUntrackedRowRequest,
    ) -> Result<Vec<UntrackedRow>, LixError> {
        let mut executor = self;
        super::read::load_exact_rows_with_executor(&mut executor, request).await
    }

    #[cfg(test)]
    async fn scan_rows(
        &self,
        request: &UntrackedScanRequest,
    ) -> Result<Vec<UntrackedRow>, LixError> {
        let mut executor = self;
        super::read::scan_rows_with_executor(&mut executor, request).await
    }
}

#[async_trait(?Send)]
impl<T> UntrackedWriteParticipant for T
where
    T: LixBackendTransaction,
{
    async fn apply_untracked_write_batch(
        &mut self,
        batch: &[UntrackedWriteRow],
    ) -> Result<(), LixError> {
        super::write::apply_write_batch_in_transaction(self, batch).await
    }
}

#[async_trait(?Send)]
impl UntrackedWriteParticipant for dyn LixBackendTransaction + '_ {
    async fn apply_untracked_write_batch(
        &mut self,
        batch: &[UntrackedWriteRow],
    ) -> Result<(), LixError> {
        super::write::apply_write_batch_in_transaction(self, batch).await
    }
}
