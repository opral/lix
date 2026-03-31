use async_trait::async_trait;

pub use crate::contracts::artifacts::{
    BatchRowRequest as BatchTrackedRowRequest, ExactRowRequest as ExactTrackedRowRequest,
    ScanRequest as TrackedScanRequest, TrackedRow, TrackedTombstoneMarker, TrackedWriteBatch,
    TrackedWriteOperation, TrackedWriteRow,
};
pub(crate) use crate::contracts::traits::{
    TrackedReadView, TrackedTombstoneView, TrackedWriteParticipant,
};
use crate::{LixBackend, LixBackendTransaction, LixError};

#[async_trait(?Send)]
impl<T> TrackedReadView for T
where
    T: LixBackend,
{
    async fn load_exact_rows(
        &self,
        request: &BatchTrackedRowRequest,
    ) -> Result<Vec<TrackedRow>, LixError> {
        let mut executor = self;
        super::read::load_exact_rows_with_executor(&mut executor, request).await
    }

    async fn scan_rows(&self, request: &TrackedScanRequest) -> Result<Vec<TrackedRow>, LixError> {
        let mut executor = self;
        super::read::scan_rows_with_executor(&mut executor, request).await
    }
}

#[async_trait(?Send)]
impl<T> TrackedTombstoneView for T
where
    T: LixBackend,
{
    async fn scan_tombstones(
        &self,
        request: &TrackedScanRequest,
    ) -> Result<Vec<TrackedTombstoneMarker>, LixError> {
        let mut executor = self;
        super::read::scan_tombstones_with_executor(&mut executor, request).await
    }
}

#[async_trait(?Send)]
impl<T> TrackedWriteParticipant for T
where
    T: LixBackendTransaction,
{
    async fn apply_tracked_write_batch(
        &mut self,
        batch: &[TrackedWriteRow],
    ) -> Result<(), LixError> {
        super::write::apply_write_batch_in_transaction(self, batch).await
    }
}

#[async_trait(?Send)]
impl TrackedWriteParticipant for dyn LixBackendTransaction + '_ {
    async fn apply_tracked_write_batch(
        &mut self,
        batch: &[TrackedWriteRow],
    ) -> Result<(), LixError> {
        super::write::apply_write_batch_in_transaction(self, batch).await
    }
}
