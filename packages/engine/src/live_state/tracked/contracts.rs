#[cfg(test)]
use async_trait::async_trait;

#[cfg(test)]
pub use crate::contracts::artifacts::BatchRowRequest as BatchTrackedRowRequest;
pub use crate::contracts::artifacts::{
    ExactRowRequest as ExactTrackedRowRequest, ScanRequest as TrackedScanRequest, TrackedRow,
    TrackedTombstoneMarker, TrackedWriteOperation, TrackedWriteRow,
};
#[cfg(test)]
pub(crate) use crate::contracts::traits::TrackedReadView;
#[cfg(test)]
pub(crate) use crate::contracts::traits::TrackedTombstoneView;
#[cfg(test)]
use crate::LixBackend;
#[cfg(test)]
use crate::LixError;

#[cfg(test)]
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

    #[cfg(test)]
    async fn scan_rows(&self, request: &TrackedScanRequest) -> Result<Vec<TrackedRow>, LixError> {
        let mut executor = self;
        super::read::scan_rows_with_executor(&mut executor, request).await
    }
}

#[cfg(test)]
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
