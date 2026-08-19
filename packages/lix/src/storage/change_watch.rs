use std::future::Future;
use std::pin::Pin;

use crate::storage::StorageError;

/// Adapter-owned source for external storage change notifications.
#[doc(hidden)]
pub trait StorageChangeSource: Send {
    fn changed(&mut self) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + '_>>;
}

/// A storage-owned watch for changes to the readable committed view.
///
/// Notifications are invalidation hints rather than a change log: adapters may
/// coalesce changes or wake spuriously. After [`Self::changed`] resolves, the
/// caller opens a fresh read view to inspect the current state. Dropping the
/// watch releases its adapter resources.
#[expect(missing_debug_implementations)]
pub struct StorageChangeWatch {
    source: Box<dyn StorageChangeSource>,
}

impl StorageChangeWatch {
    #[doc(hidden)]
    pub fn from_source(source: impl StorageChangeSource + 'static) -> Self {
        Self {
            source: Box::new(source),
        }
    }

    /// Waits until the underlying readable committed storage may have changed.
    pub async fn changed(&mut self) -> Result<(), StorageError> {
        self.source.changed().await
    }
}
