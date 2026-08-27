use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

use crate::storage::{ReadOptions, Storage, StorageChangeWatch, StorageError, WriteOptions};

static NEXT_SESSION_TOKEN: AtomicU64 = AtomicU64::new(1);

/// Identifies the fenced generation currently allowed to access a storage.
///
/// Tokens are opaque capabilities. Callers obtain one from
/// [`Storage::acquire_session`] and should normally use [`StorageSession`] to
/// attach it to every storage operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StorageSessionToken(u64);

impl StorageSessionToken {
    fn next() -> Self {
        Self(NEXT_SESSION_TOKEN.fetch_add(1, Ordering::Relaxed))
    }

    /// Encodes this adapter-owned token without losing precision at a
    /// JavaScript boundary.
    ///
    /// Application code should acquire and carry tokens through
    /// [`StorageSession`]. This representation exists for storage adapters
    /// which must round-trip the capability through another runtime.
    pub fn to_decimal_string(self) -> String {
        self.0.to_string()
    }

    /// Decodes a token previously minted by a storage adapter.
    ///
    /// This is an adapter interoperability hook, not an application-level way
    /// to select or replace the active storage generation.
    pub fn from_decimal_string(value: &str) -> Option<Self> {
        let parsed = value.parse::<u64>().ok()?;
        (parsed.to_string() == value).then_some(Self(parsed))
    }
}

/// A storage wrapper which attaches one acquired session token to all reads
/// and writes.
#[derive(Clone, Debug)]
pub struct StorageSession<S> {
    storage: S,
    token: StorageSessionToken,
}

impl<S> StorageSession<S>
where
    S: Storage,
{
    pub async fn acquire(storage: S) -> Result<Self, StorageError> {
        let token = storage.acquire_session().await?;
        Ok(Self { storage, token })
    }

    pub fn token(&self) -> StorageSessionToken {
        self.token
    }
}

impl<S> Storage for StorageSession<S>
where
    S: Storage,
{
    type Read<'a>
        = S::Read<'a>
    where
        Self: 'a;

    type Write<'a>
        = S::Write<'a>
    where
        Self: 'a;

    async fn acquire_session(&self) -> Result<StorageSessionToken, StorageError> {
        Ok(self.token)
    }

    fn begin_read(
        &self,
        mut opts: ReadOptions,
    ) -> impl Future<Output = Result<Self::Read<'_>, StorageError>> + Send {
        opts.session_token = Some(self.token);
        self.storage.begin_read(opts)
    }

    fn begin_write(
        &self,
        mut opts: WriteOptions,
    ) -> impl Future<Output = Result<Self::Write<'_>, StorageError>> + Send {
        opts.session_token = Some(self.token);
        self.storage.begin_write(opts)
    }

    fn watch_for_changes(
        &self,
    ) -> impl Future<Output = Result<StorageChangeWatch, StorageError>> + Send {
        self.storage.watch_for_changes()
    }
}

/// Reusable in-process fencing state for storage adapters.
///
/// Before the first acquisition, tokenless access is accepted for bootstrap
/// and migration compatibility. The first acquisition permanently closes
/// that path and establishes one token. Later acquisitions share the same
/// token so independently opened Lix handles can safely share the generation.
#[derive(Debug, Default)]
pub struct StorageSessionGate {
    current: Mutex<Option<StorageSessionToken>>,
}

impl StorageSessionGate {
    pub fn acquire(&self) -> Result<StorageSessionToken, StorageError> {
        let mut current = self.lock()?;
        Ok(*current.get_or_insert_with(StorageSessionToken::next))
    }

    /// Validates an operation and holds acquisition behind a barrier until the
    /// returned permit is dropped.
    ///
    /// Commit implementations retain this permit for their complete atomic
    /// publication so an acquisition cannot race between validation and
    /// visibility.
    pub fn validate(
        &self,
        token: Option<StorageSessionToken>,
    ) -> Result<StorageSessionPermit<'_>, StorageError> {
        let current = self.lock()?;
        match (*current, token) {
            (None, None) | (Some(_), Some(_)) if *current == token => {
                Ok(StorageSessionPermit { _barrier: current })
            }
            _ => Err(StorageError::Fenced),
        }
    }

    fn lock(&self) -> Result<MutexGuard<'_, Option<StorageSessionToken>>, StorageError> {
        self.current
            .lock()
            .map_err(|_| StorageError::Io("storage session gate lock poisoned".to_string()))
    }
}

#[must_use = "dropping the permit releases the acquisition barrier"]
#[derive(Debug)]
pub struct StorageSessionPermit<'a> {
    _barrier: MutexGuard<'a, Option<StorageSessionToken>>,
}

#[cfg(test)]
mod tests {
    use super::StorageSessionToken;

    #[test]
    fn session_token_decimal_encoding_preserves_u64_precision() {
        let encoded = u64::MAX.to_string();
        let token = StorageSessionToken::from_decimal_string(&encoded).unwrap();

        assert_eq!(token.to_decimal_string(), encoded);
        assert!(StorageSessionToken::from_decimal_string("not-a-token").is_none());
        assert!(StorageSessionToken::from_decimal_string("01").is_none());
        assert!(StorageSessionToken::from_decimal_string("+1").is_none());
        assert!(StorageSessionToken::from_decimal_string("18446744073709551616").is_none());
    }
}
