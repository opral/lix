#[cfg(test)]
use async_trait::async_trait;

#[cfg(test)]
pub use crate::contracts::BatchRowRequest as BatchUntrackedRowRequest;
#[cfg(test)]
pub(crate) use crate::contracts::UntrackedReadView;
pub use crate::contracts::{
    ExactRowRequest as ExactUntrackedRowRequest, ScanRequest as UntrackedScanRequest, UntrackedRow,
    UntrackedWriteOperation, UntrackedWriteRow,
};
#[cfg(test)]
use crate::LixBackend;
#[cfg(test)]
use crate::LixError;

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
