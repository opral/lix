//! Active-account validation and its storage-snapshot identity token.
//!
//! Every commit must prove that the account it is attributed to still exists
//! and is enabled. That proof is a hot-state point read plus a JSON parse of
//! the account snapshot — measured at 13.8 µs, ~8% of `commit_prepared`, on a
//! single-row update whose account state had not changed in thousands of
//! commits.
//!
//! The token here is the same device the filesystem path index and the schema
//! catalog use: a uuid-v7 written into [`REVISION_SPACE`] **only** by a commit
//! that can change which account rows are visible. Equality is its only
//! meaningful operation, and equality is one-directional by construction — a
//! reader holding an older token can never match a newer view, so it misses
//! and re-reads. It cannot be served a newer view under an older token,
//! because a new view always carries a freshly generated token.

use std::collections::VecDeque;
use std::sync::{LazyLock, Mutex};

use bytes::Bytes;

use crate::LixError;
use crate::storage_adapter::{
    REVISION_KEY_ACCOUNT, REVISION_SPACE, StorageAdapterRead, StorageValue, StorageWriteSet,
    load_revision, revision_key,
};

/// Storage-snapshot identity for the visible `lix_account` rows.
pub(crate) async fn load_account_revision(
    store: &(impl StorageAdapterRead + ?Sized),
) -> Result<Option<Bytes>, LixError> {
    Ok(load_revision(store, REVISION_KEY_ACCOUNT).await?)
}

/// Rotates the account token. Called by every commit that writes an account
/// row or moves a branch ref, and once at repository initialization.
pub(crate) fn stage_account_revision(writes: &mut StorageWriteSet) {
    writes.put(
        REVISION_SPACE,
        revision_key(REVISION_KEY_ACCOUNT),
        StorageValue {
            bytes: Bytes::copy_from_slice(uuid::Uuid::now_v7().as_bytes()),
        },
    );
}

/// Bounded set of `(account token, account id)` pairs already proven active.
///
/// The token is a uuid-v7 minted by the write that produced the view, so a
/// pair is globally unique and permanently true: "under this exact account
/// view, this account was active". Two repositories cannot collide on one,
/// which is what makes a process-wide cache sound. A missing token (`None`)
/// never matches, so a repository that has never rotated one simply keeps
/// paying the full read.
///
/// The queue is capped, so a process cycling through unrelated repositories
/// or accounts evicts rather than grows.
const MAX_VALIDATED_ACCOUNTS: usize = 16;

static VALIDATED_ACCOUNTS: LazyLock<Mutex<VecDeque<(Bytes, String)>>> =
    LazyLock::new(|| Mutex::new(VecDeque::new()));

pub(crate) fn account_proven_active(revision: Option<&Bytes>, account_id: &str) -> bool {
    let Some(revision) = revision else {
        return false;
    };
    VALIDATED_ACCOUNTS
        .lock()
        .expect("validated account cache lock poisoned")
        .iter()
        .any(|(token, id)| token == revision && id == account_id)
}

pub(crate) fn record_account_proven_active(revision: Option<&Bytes>, account_id: &str) {
    let Some(revision) = revision else {
        return;
    };
    let mut cache = VALIDATED_ACCOUNTS
        .lock()
        .expect("validated account cache lock poisoned");
    if cache
        .iter()
        .any(|(token, id)| token == revision && id == account_id)
    {
        return;
    }
    cache.push_back((revision.clone(), account_id.to_string()));
    while cache.len() > MAX_VALIDATED_ACCOUNTS {
        cache.pop_front();
    }
}

#[cfg(test)]
pub(crate) fn clear_validated_accounts_for_test() {
    VALIDATED_ACCOUNTS
        .lock()
        .expect("validated account cache lock poisoned")
        .clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::storage::{Memory, Storage};
    use crate::storage_adapter::{StorageAdapter, StorageReadOptions};
    use crate::{Value, open_lix};

    /// The cache is one process-wide static, so the tests that assert on its
    /// contents take turns rather than racing each other.
    static CACHE_TESTS: Mutex<()> = Mutex::new(());

    fn token(byte: u8) -> Bytes {
        Bytes::from(vec![byte; 16])
    }

    async fn current_token<S: Storage>(adapter: &StorageAdapter<S>) -> Option<Bytes> {
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("token read should open");
        load_account_revision(&read)
            .await
            .expect("account token should load")
    }

    /// The whole win rests on this asymmetry: ordinary row commits must leave
    /// the token alone (or the cache never hits), and any commit that can
    /// change the visible account rows must rotate it (or a disabled account
    /// keeps committing off a warm cache).
    #[tokio::test]
    async fn ordinary_commits_keep_the_token_while_account_writes_rotate_it() {
        const AUTHOR_ID: &str = "01920000-0000-7000-8000-0000000006a1";
        let storage = Memory::new();
        let lix = open_lix()
            .with_storage(storage)
            .await
            .expect("repository should open");
        let adapter = lix.storage_adapter();

        let initialized = current_token(&adapter)
            .await
            .expect("initialization stages an account token");

        lix.ensure_account(AUTHOR_ID, "Ada", "human")
            .await
            .expect("provision author");
        let after_account_write = current_token(&adapter)
            .await
            .expect("account write keeps a token");
        assert_ne!(
            after_account_write, initialized,
            "writing an account row must rotate the token"
        );

        let session = lix
            .open_another_session()
            .with_account(AUTHOR_ID)
            .await
            .expect("attributed session should open");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('token-control', CAST('1' AS JSONB))",
                &[],
            )
            .await
            .expect("ordinary write should commit");
        assert_eq!(
            current_token(&adapter).await,
            Some(after_account_write.clone()),
            "ordinary CRUD must not rotate the account token"
        );
        session
            .execute(
                "UPDATE lix_key_value SET value = CAST('2' AS JSONB) WHERE key = 'token-control'",
                &[],
            )
            .await
            .expect("second ordinary write should commit");
        assert_eq!(
            current_token(&adapter).await,
            Some(after_account_write.clone()),
            "the token is stable across repeated ordinary commits"
        );

        let system = lix
            .open_another_session()
            .with_branch(crate::GLOBAL_BRANCH_ID)
            .with_account(crate::SYSTEM_ACCOUNT_ID)
            .await
            .expect("system session should open");
        system
            .execute(
                "UPDATE lix_account SET status = 'disabled' WHERE id = $1",
                &[Value::Text(AUTHOR_ID.to_string())],
            )
            .await
            .expect("disable author");
        let after_disable = current_token(&adapter)
            .await
            .expect("disabling keeps a token");
        assert_ne!(
            after_disable, after_account_write,
            "disabling an account must rotate the token"
        );

        // The warm session proved itself active moments ago; the rotated token
        // is what makes it re-read and fail.
        let error = session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('after-disable', CAST('1' AS JSONB))",
                &[],
            )
            .await
            .expect_err("a disabled account must not commit off a warm proof");
        assert_eq!(error.code, "LIX_ACCOUNT_DISABLED");
    }

    #[test]
    fn a_missing_token_never_matches() {
        let _guard = CACHE_TESTS.lock().expect("cache test guard");
        record_account_proven_active(None, "account-a");
        assert!(!account_proven_active(None, "account-a"));
    }

    #[test]
    fn a_different_token_misses() {
        let _guard = CACHE_TESTS.lock().expect("cache test guard");
        clear_validated_accounts_for_test();
        record_account_proven_active(Some(&token(1)), "account-a");
        assert!(account_proven_active(Some(&token(1)), "account-a"));
        assert!(
            !account_proven_active(Some(&token(2)), "account-a"),
            "a rotated token must miss, which is what makes a disabled account re-read"
        );
        assert!(
            !account_proven_active(Some(&token(1)), "account-b"),
            "another account under the same view must prove itself"
        );
    }

    #[test]
    fn the_cache_is_bounded() {
        let _guard = CACHE_TESTS.lock().expect("cache test guard");
        clear_validated_accounts_for_test();
        for index in 0..(MAX_VALIDATED_ACCOUNTS as u8 + 8) {
            record_account_proven_active(Some(&token(index)), "account-a");
        }
        let len = VALIDATED_ACCOUNTS
            .lock()
            .expect("validated account cache lock poisoned")
            .len();
        assert_eq!(len, MAX_VALIDATED_ACCOUNTS);
        assert!(
            !account_proven_active(Some(&token(0)), "account-a"),
            "the oldest entry must have been evicted"
        );
    }
}

/// A sealed system operation: it may create one authenticated principal and
/// cannot expose the reduced catalog to arbitrary SQL or transaction writes.
#[derive(Clone, Debug)]
pub(crate) struct AccountInsertion {
    id: String,
    params: [crate::Value; 3],
}
impl AccountInsertion {
    pub(crate) const SQL: &'static str = "INSERT INTO lix_account (id, name, kind, status, lixcol_global, lixcol_untracked) VALUES ($1, $2, $3, 'active', true, false) ON CONFLICT (id) DO NOTHING";
    pub(crate) fn new(id: &str, name: &str, kind: &str) -> Result<Self, LixError> {
        if crate::storage_codec::id_string::uuid_bytes_from_canonical(id).is_none() {
            return Err(LixError::new(
                "LIX_INVALID_ACCOUNT_ID",
                "account ID must be a canonical UUID",
            ));
        }
        Ok(Self {
            id: id.into(),
            params: [
                crate::Value::Text(id.into()),
                crate::Value::Text(name.into()),
                crate::Value::Text(kind.into()),
            ],
        })
    }
    pub(crate) fn params(&self) -> &[crate::Value] {
        &self.params
    }
    pub(crate) fn id(&self) -> &str {
        &self.id
    }
    pub(crate) fn reject() -> LixError {
        LixError::new(
            "LIX_ACCOUNT_INSERTION_SCOPE",
            "authenticated account preparation permits only its exact account insertion",
        )
    }
    pub(crate) fn ensure_sql(&self, sql: &str, params: &[crate::Value]) -> Result<(), LixError> {
        if sql != Self::SQL || params != self.params.as_slice() {
            return Err(Self::reject());
        }
        Ok(())
    }
    pub(crate) fn ensure_typed_snapshot(&self, row: &lix_schema::Row) -> Result<(), LixError> {
        let [
            crate::Value::Text(_),
            crate::Value::Text(name),
            crate::Value::Text(kind),
        ] = &self.params
        else {
            return Err(Self::reject());
        };
        let id = uuid::Uuid::parse_str(&self.id).map_err(|_| Self::reject())?;
        if row.len() != 5
            || row.get("id") != Some(&lix_schema::Value::Uuid(id))
            || row.get("name") != Some(&lix_schema::Value::Text(name.clone()))
            || row.get("kind") != Some(&lix_schema::Value::Text(kind.clone()))
            || row.get("status") != Some(&lix_schema::Value::Text("active".into()))
            || row.get("profile_uri") != Some(&lix_schema::Value::Null)
        {
            return Err(Self::reject().with_details(serde_json::json!({"stage":"typed_snapshot","columns":row.len(),"hasNullProfile":row.get("profile_uri")==Some(&lix_schema::Value::Null)})));
        }
        Ok(())
    }
    pub(crate) fn ensure_json_snapshot(&self, row: &serde_json::Value) -> Result<(), LixError> {
        let [
            crate::Value::Text(_),
            crate::Value::Text(name),
            crate::Value::Text(kind),
        ] = &self.params
        else {
            return Err(Self::reject());
        };
        let Some(row) = row.as_object() else {
            return Err(Self::reject());
        };
        if row.get("id").and_then(|v| v.as_str()) != Some(self.id.as_str())
            || row.get("name").and_then(|v| v.as_str()) != Some(name.as_str())
            || row.get("kind").and_then(|v| v.as_str()) != Some(kind.as_str())
            || row.get("status").and_then(|v| v.as_str()) != Some("active")
            || row.get("profile_uri").is_some_and(|v| !v.is_null())
            || row.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "id" | "name" | "kind" | "status" | "profile_uri"
                )
            })
        {
            return Err(Self::reject()
                .with_details(serde_json::json!({"stage":"json_snapshot","columns":row.len()})));
        }
        Ok(())
    }
    pub(crate) fn ensure_session(&self, branch: &str, account: &str) -> Result<(), LixError> {
        if branch != crate::GLOBAL_BRANCH_ID || account != crate::SYSTEM_ACCOUNT_ID {
            return Err(Self::reject());
        }
        Ok(())
    }
}
