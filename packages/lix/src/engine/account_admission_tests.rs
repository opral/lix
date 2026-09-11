//! Authenticated admission profiles use canonical Memory and count real I/O.
use crate::storage_adapter::*;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Default, serde::Serialize)]
struct Counts {
    reads: u64,
    calls: u64,
    keys: u64,
    bytes: u64,
    scans: u64,
    scan_rows: u64,
    writes: u64,
    scan_spaces: std::collections::BTreeMap<String, u64>,
    scan_rows_by_space: std::collections::BTreeMap<String, u64>,
}
#[derive(Clone)]
struct CountedStorage<S> {
    inner: S,
    counts: Arc<Mutex<Counts>>,
}
impl<S> CountedStorage<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            counts: Arc::new(Mutex::new(Counts::default())),
        }
    }
    fn reset(&self) {
        *self.counts.lock().unwrap() = Counts::default();
    }
    fn counts(&self) -> Counts {
        self.counts.lock().unwrap().clone()
    }
}
struct CountedRead<R> {
    inner: R,
    counts: Arc<Mutex<Counts>>,
}
impl<S: Storage> Storage for CountedStorage<S> {
    type Read<'a>
        = CountedRead<S::Read<'a>>
    where
        Self: 'a;
    type Write<'a>
        = S::Write<'a>
    where
        Self: 'a;
    async fn acquire_session(&self) -> Result<StorageSessionToken, StorageError> {
        self.inner.acquire_session().await
    }
    async fn acquire_partial_replica_owner(
        &self,
        token: StorageSessionToken,
    ) -> Result<StorageOwnerLease, StorageError> {
        self.inner.acquire_partial_replica_owner(token).await
    }
    async fn begin_read(&self, opts: StorageReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.counts.lock().unwrap().reads += 1;
        Ok(CountedRead {
            inner: self.inner.begin_read(opts).await?,
            counts: self.counts.clone(),
        })
    }
    async fn begin_write(
        &self,
        opts: StorageWriteOptions,
    ) -> Result<Self::Write<'_>, StorageError> {
        self.counts.lock().unwrap().writes += 1;
        self.inner.begin_write(opts).await
    }
}
impl<R: StorageRead> StorageRead for CountedRead<R> {
    // Disable snapshot-key caches so the profile exposes the read dependency.
    async fn get_many(
        &self,
        requests: &[StorageGetManyRequest<'_>],
    ) -> Result<StorageGetManyResult, StorageError> {
        {
            let mut counts = self.counts.lock().unwrap();
            counts.calls += 1;
            counts.keys += requests
                .iter()
                .map(|request| request.keys.len() as u64)
                .sum::<u64>();
        }
        let result = self.inner.get_many(requests).await?;
        self.counts.lock().unwrap().bytes += result
            .values
            .iter()
            .flatten()
            .map(|value| match value {
                StorageProjectedValue::FullValue(bytes) => bytes.len() as u64,
                StorageProjectedValue::KeyOnly => 0,
            })
            .sum::<u64>();
        Ok(result)
    }
    async fn begin_scan(
        &self,
        space: StorageSpace,
        range: StorageKeyRange,
        opts: StorageBeginScanOptions,
    ) -> Result<StorageScanCursor<'_>, StorageError> {
        {
            let mut counts = self.counts.lock().unwrap();
            counts.scans += 1;
            *counts.scan_spaces.entry(space.name.to_owned()).or_default() += 1;
        }
        let order = opts.order;
        let inner = self.inner.begin_scan(space, range.clone(), opts).await?;
        StorageScanCursor::from_source(
            range,
            order,
            CountedScan {
                space: space.name,
                inner,
                counts: self.counts.clone(),
            },
        )
    }
}
struct CountedScan<'a> {
    space: &'static str,
    inner: StorageScanCursor<'a>,
    counts: Arc<Mutex<Counts>>,
}
impl StorageScanSource for CountedScan<'_> {
    fn next_page(
        &mut self,
        limit: usize,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<StorageScanChunk, StorageError>> + Send + '_>>
    {
        Box::pin(async move {
            let (entries, more) = self.inner.next_page(limit).await?.into_parts();
            {
                let mut counts = self.counts.lock().unwrap();
                counts.scan_rows += entries.len() as u64;
                *counts
                    .scan_rows_by_space
                    .entry(self.space.to_owned())
                    .or_default() += entries.len() as u64;
                counts.bytes += entries
                    .iter()
                    .map(|entry| match &entry.value {
                        StorageProjectedValue::FullValue(bytes) => bytes.len() as u64,
                        StorageProjectedValue::KeyOnly => 0,
                    })
                    .sum::<u64>();
            }
            Ok(StorageScanChunk::new(entries, more))
        })
    }
}

#[tokio::test]
async fn existing_account_admission_is_read_only_and_disabled_accounts_fail() {
    let store = CountedStorage::new(Memory::new());
    let lix = crate::open_lix().with_storage(store.clone()).await.unwrap();
    let id = uuid::Uuid::now_v7().to_string();
    lix.ensure_account(&id, "Original name", "human")
        .await
        .unwrap();
    store.reset();
    lix.ensure_account(&id, "Must not rename", "human")
        .await
        .unwrap();
    let counts = store.counts();
    assert_eq!(counts.writes, 0);
    assert_eq!(counts.scans, 0);
    let rows = lix
        .execute(
            "SELECT name FROM lix_account WHERE id=$1",
            &[crate::Value::Text(id.clone())],
        )
        .await
        .unwrap();
    assert_eq!(
        rows.rows()[0].get::<String>("name").unwrap(),
        "Original name"
    );
    lix.execute(
        "UPDATE lix_account SET status='disabled' WHERE id=$1",
        &[crate::Value::Text(id.clone())],
    )
    .await
    .unwrap();
    store.reset();
    assert_eq!(
        lix.ensure_account(&id, "Name", "human")
            .await
            .unwrap_err()
            .code,
        "LIX_ACCOUNT_DISABLED"
    );
    assert_eq!(store.counts().writes, 0);
    assert_eq!(
        lix.ensure_account("invalid-id", "Name", "human")
            .await
            .unwrap_err()
            .code,
        "LIX_INVALID_ACCOUNT_ID"
    );
}

#[cfg(feature = "server-protocol")]
#[tokio::test]
#[ignore = "large authority account-admission I/O profile"]
async fn authenticated_account_admission_profile() {
    let mut reports = Vec::new();
    for dimension in [
        "selected_rows",
        "global_rows",
        "accounts",
        "global_schemas",
        "branches",
    ] {
        let widths: &[usize] = if dimension == "global_schemas" {
            &[16, 256, 1600]
        } else if dimension == "branches" {
            &[16, 128, 1600]
        } else {
            &[16, 1600, 16000]
        };
        for &width in widths {
            let store = CountedStorage::new(crate::sync::durable_memory_for_test(Memory::new()));
            let lix = crate::open_lix().with_storage(store.clone()).await.unwrap();
            // Branch creation is fixture work, outside both timers/I/O samples.
            if dimension == "branches" {
                for branch in 0..width {
                    lix.create_branch(crate::CreateBranchOptions {
                        id: None,
                        name: format!("account-profile-{branch}"),
                        from_commit_id: None,
                    })
                    .await
                    .unwrap();
                }
            }
            // Batch fixture construction, outside both timers and I/O samples.
            for start in (0..if dimension == "branches" { 0 } else { width }).step_by(400) {
                let end = (start + 400).min(width);
                let values = (start..end)
                    .map(|index| {
                        if dimension == "global_schemas" {
                            let schema = serde_json::json!({
                                "$schema": "https://lix.dev/schema-v1.json",
                                "key": format!("account_profile_schema_{index}"),
                                "columns": [{"name":"id","type":"text","nullable":false}],
                                "primary_key": ["id"],
                            });
                            format!("(CAST('{}' AS JSONB),true)", schema)
                        } else if dimension == "accounts" {
                            format!(
                                "('{}','account-{index}','human','active',true,false)",
                                uuid::Uuid::now_v7()
                            )
                        } else {
                            format!(
                                "('account-admission-{index}','value',{})",
                                dimension == "global_rows"
                            )
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                let sql = if dimension == "global_schemas" {
                    format!(
                        "INSERT INTO lix_registered_schema (value,lixcol_global) VALUES {values}"
                    )
                } else if dimension == "accounts" {
                    format!(
                        "INSERT INTO lix_account (id,name,kind,status,lixcol_global,lixcol_untracked) VALUES {values}"
                    )
                } else {
                    format!("INSERT INTO lix_key_value (key,value,lixcol_global) VALUES {values}")
                };
                lix.execute(&sql, &[]).await.unwrap();
            }
            // Match the authority role that authenticated server handshakes use.
            let adapter = lix.storage_adapter();
            let read = adapter.begin_read(Default::default()).await.unwrap();
            let revision = load_repository_mutation_revision(&read).await.unwrap();
            drop(read);
            crate::sync::admit_sync_authority_storage(&adapter, revision)
                .await
                .unwrap();
            lix.sync_mode_state()
                .set_role(crate::sync::SyncRole::Authority);
            let id = uuid::Uuid::now_v7().to_string();
            for existing in [false, true] {
                store.reset();
                let started = web_time::Instant::now();
                lix.ensure_account(&id, "New authenticated principal", "human")
                    .await
                    .unwrap();
                let micros = started.elapsed().as_micros();
                let counts = store.counts();
                if existing {
                    assert_eq!(counts.writes, 0);
                    assert_eq!(counts.scans, 0);
                }
                reports.push(serde_json::json!({"dimension":dimension,"width":width,"existing":existing,"microseconds":micros,"io":counts}));
            }
            lix.close().await.unwrap();
        }
    }
    eprintln!(
        "AUTHENTICATED_ACCOUNT_ADMISSION_PROFILE {}",
        serde_json::to_string(&reports).unwrap()
    );
}

#[tokio::test]
async fn first_account_creation_does_not_enumerate_custom_global_schemas() {
    let store = CountedStorage::new(Memory::new());
    let lix = crate::open_lix().with_storage(store.clone()).await.unwrap();
    let schemas = (0..32).map(|index| {
        let schema = serde_json::json!({"$schema":"https://lix.dev/schema-v1.json", "key":format!("account_scope_{index}"),
            "columns":[{"name":"id","type":"text","nullable":false}],"primary_key":["id"]});
        format!("(CAST('{}' AS JSONB),true)",schema)
    }).collect::<Vec<_>>().join(",");
    lix.execute(
        &format!("INSERT INTO lix_registered_schema (value,lixcol_global) VALUES {schemas}"),
        &[],
    )
    .await
    .unwrap();
    let global = lix
        .open_another_session()
        .with_branch(crate::GLOBAL_BRANCH_ID)
        .await
        .unwrap();
    global
        .execute(
            "INSERT INTO account_scope_31 (id) VALUES ('global-before')",
            &[],
        )
        .await
        .unwrap();
    lix.execute("INSERT INTO lix_registered_schema(value) VALUES(CAST($1 AS JSONB))",&[crate::Value::Text(serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"account_local_scope","columns":[{"name":"id","type":"text","nullable":false}],"primary_key":["id"]}).to_string())]).await.unwrap();
    lix.execute(
        "INSERT INTO account_local_scope(id) VALUES('local-before')",
        &[],
    )
    .await
    .unwrap();
    store.reset();
    let id = uuid::Uuid::now_v7().to_string();
    lix.ensure_account(&id, "bounded principal", "human")
        .await
        .unwrap();
    assert_eq!(
        store.counts().scan_rows,
        0,
        "private account insertion must not enumerate custom schema or plugin rows"
    );
    // A normal session still sees and validates the complete custom catalog.
    global
        .execute(
            "INSERT INTO account_scope_31 (id) VALUES ('custom-visible')",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        global
            .execute("SELECT id FROM account_scope_31", &[])
            .await
            .unwrap()
            .rows()
            .len(),
        2
    );
    lix.execute(
        "INSERT INTO account_local_scope(id) VALUES('local-after')",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        lix.execute("SELECT id FROM account_local_scope", &[])
            .await
            .unwrap()
            .rows()
            .len(),
        2
    );
    global.close().await.unwrap();
    lix.close().await.unwrap();
    let reopened = crate::open_lix().with_storage(store).await.unwrap();
    let global = reopened
        .open_another_session()
        .with_branch(crate::GLOBAL_BRANCH_ID)
        .await
        .unwrap();
    assert_eq!(
        reopened
            .execute("SELECT id FROM account_local_scope", &[])
            .await
            .unwrap()
            .rows()
            .len(),
        2
    );
    assert_eq!(
        global
            .execute("SELECT id FROM account_scope_31", &[])
            .await
            .unwrap()
            .rows()
            .len(),
        2
    );
    global.close().await.unwrap();
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn sealed_account_insertion_rejects_arbitrary_sql_and_native_writes() {
    // Canonical initialization and a directly owned engine session exercise
    // the private scope without exporting it through the public API.
    let adapter = StorageAdapter::new(Memory::new());
    crate::engine::Engine::initialize_with_adapter(adapter.clone(), None)
        .await
        .unwrap();
    let engine =
        crate::engine::Engine::new_with_adapter(adapter, crate::engine::EngineOptions::new())
            .await
            .unwrap();
    let id = uuid::Uuid::now_v7().to_string();
    let operation = crate::account::AccountInsertion::new(&id, "bounded", "human").unwrap();
    let system = engine
        .open_session_at_with_account(crate::GLOBAL_BRANCH_ID, crate::SYSTEM_ACCOUNT_ID)
        .await
        .unwrap()
        .with_account_insertion(operation.clone());
    assert_eq!(
        system
            .execute("DELETE FROM lix_account", &[])
            .await
            .unwrap_err()
            .code,
        "LIX_ACCOUNT_INSERTION_SCOPE"
    );
    let mut transaction = system.begin_transaction().await.unwrap();
    assert!(
        transaction
            .execute(
                "INSERT INTO lix_key_value (key,value) VALUES ('forbidden','value')",
                &[]
            )
            .await
            .is_err()
    );
    transaction.rollback().await.unwrap();
    // Explicit transaction execution bypasses the session SQL-string check;
    // native scope validation must still reject a same-ID payload substitution.
    for (column, value) in [
        ("name", "different"),
        ("kind", "agent"),
        ("status", "disabled"),
        ("profile_uri", "https://example.com/profile"),
    ] {
        let mut transaction = system.begin_transaction().await.unwrap();
        let mut fields = serde_json::json!({"name":"bounded","kind":"human","status":"active","profile_uri":null});
        fields[column] = serde_json::Value::String(value.into());
        let error = transaction.execute(
            "INSERT INTO lix_account (id,name,kind,status,profile_uri,lixcol_global,lixcol_untracked) VALUES ($1,$2,$3,$4,$5,true,false)",
            &[crate::Value::Text(id.clone()),crate::Value::Text(fields["name"].as_str().unwrap().into()),crate::Value::Text(fields["kind"].as_str().unwrap().into()),crate::Value::Text(fields["status"].as_str().unwrap().into()),fields["profile_uri"].as_str().map(|v|crate::Value::Text(v.into())).unwrap_or(crate::Value::Null)]
        ).await.unwrap_err();
        assert_eq!(
            error.code, "LIX_ACCOUNT_INSERTION_SCOPE",
            "changed {column}"
        );
        transaction.rollback().await.unwrap();
    }
    system
        .execute(crate::account::AccountInsertion::SQL, operation.params())
        .await
        .unwrap();
    let ordinary = engine.open_session().await.unwrap();
    assert!(
        ordinary
            .execute("SELECT key FROM lix_key_value WHERE key='forbidden'", &[])
            .await
            .unwrap()
            .rows()
            .is_empty()
    );
    assert_eq!(
        ordinary
            .execute(
                "SELECT id FROM lix_account WHERE id=$1",
                &[crate::Value::Text(id)]
            )
            .await
            .unwrap()
            .rows()
            .len(),
        1
    );
}

#[tokio::test]
async fn account_do_nothing_keeps_insert_validation_under_concurrent_creation() {
    let adapter = StorageAdapter::new(Memory::new());
    crate::engine::Engine::initialize_with_adapter(adapter.clone(), None)
        .await
        .unwrap();
    let engine =
        crate::engine::Engine::new_with_adapter(adapter, crate::engine::EngineOptions::new())
            .await
            .unwrap();
    let first = engine.open_session().await.unwrap();
    let second = engine.open_session().await.unwrap();
    let id = uuid::Uuid::now_v7().to_string();
    let sql = crate::account::AccountInsertion::SQL;
    let left = crate::account::AccountInsertion::new(&id, "first", "human").unwrap();
    let right = crate::account::AccountInsertion::new(&id, "second", "agent").unwrap();
    // Both statements see absence; publication of the second must never turn
    // DO NOTHING into replacement of the first committed principal.
    let mut a = first.begin_transaction().await.unwrap();
    let mut b = second.begin_transaction().await.unwrap();
    a.execute(sql, left.params()).await.unwrap();
    b.execute(sql, right.params()).await.unwrap();
    a.commit().await.unwrap();
    assert!(
        b.commit().await.is_err(),
        "stale insertion must conflict instead of replacing committed account"
    );
    assert_eq!(
        second
            .execute(sql, right.params())
            .await
            .unwrap()
            .rows_affected(),
        0
    );
    let result = first
        .execute(
            "SELECT name,kind FROM lix_account WHERE id=$1",
            &[crate::Value::Text(id)],
        )
        .await
        .unwrap();
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].get::<String>("name").unwrap(), "first");
    assert_eq!(result.rows()[0].get::<String>("kind").unwrap(), "human");
}

#[tokio::test]
async fn authenticated_account_creation_has_bounded_work_across_branch_inventory() {
    let mut samples = Vec::new();
    for width in [1, 32] {
        let store = CountedStorage::new(Memory::new());
        let lix = crate::open_lix().with_storage(store.clone()).await.unwrap();
        for branch in 0..width {
            lix.create_branch(crate::CreateBranchOptions {
                id: None,
                name: format!("account-width-{branch}"),
                from_commit_id: None,
            })
            .await
            .unwrap();
        }
        store.reset();
        lix.ensure_account(&uuid::Uuid::now_v7().to_string(), "bounded", "human")
            .await
            .unwrap();
        samples.push(store.counts());
        lix.close().await.unwrap();
    }
    // The optional singleton sync state may be present or absent; neither
    // case may enumerate branch rows.
    // Native exact paths can gain a node as the tree partitions change;
    // equality of point counts is not a complexity guarantee. This fixture's
    // budget permits that variation while rejecting branch enumeration.
    for sample in &samples {
        assert!(
            sample.scan_rows <= 1,
            "admission must not enumerate branch rows: {samples:?}"
        );
        assert!(
            sample.keys <= 128,
            "bounded fixture point-read budget exceeded: {samples:?}"
        );
    }
    assert_eq!(samples[0].scan_spaces, samples[1].scan_spaces);
}
