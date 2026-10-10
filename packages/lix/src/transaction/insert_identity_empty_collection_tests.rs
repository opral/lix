//! INSERT identity validation when the committed collection is empty.
//!
//! Validation skips the per-identity existence reads for a `(branch, schema)`
//! whose schema-wide committed live count is zero in the validation
//! snapshot. These tests pin that the shortcut is taken for a first import,
//! and that every way an identity can already exist — a committed row, an
//! untracked row, a row in another file scope, a row inherited from the
//! branch's root, a global row, a tombstone, a concurrent commit — is
//! detected or allowed exactly as the full check decides.
//!
//! Single-row tracked unfiled inserts into an unconstrained schema take the
//! certified row-local route, which has its own live-count proof. The tests
//! that assert this module's shortcut therefore write the foreign-keyed child
//! schema, or file-scoped rows, which take the full validation path.

use std::sync::{Arc, Mutex};

use futures_util::future::BoxFuture;

use super::{
    peek_committed_insert_identity_empty_collections, take_committed_insert_identity_stats,
};

/// Takes this repository's active-branch validation stats for the test
/// schemas: `(identities point-read, collections proven empty)`.
async fn stats<S: Storage + Clone + Send + Sync + 'static>(lix: &Lix<S>) -> (usize, usize) {
    take_committed_insert_identity_stats(&lix.active_branch_id().await.unwrap(), SCHEMAS)
}
use crate::storage::{
    CommitResult, Key, KeyRange, Memory, MemoryRead, MemoryWrite, PutBatch, ReadOptions, Storage,
    StorageError, StorageSessionToken, StorageSpace, StorageWrite, WriteOptions,
};
use crate::{Lix, LixError, Value};

const PARENT: &str = "eci_parent";
const CHILD: &str = "eci_child";
const ITEM: &str = "eci_item";
const SCHEMAS: &[&str] = &[PARENT, CHILD, ITEM];
const FILE_A: &str = "01920000-0000-7000-8000-0000000eca01";
const FILE_B: &str = "01920000-0000-7000-8000-0000000ecb02";

fn schema_sql(key: &str, columns: &str, foreign_key: Option<(&str, &str)>, global: bool) -> String {
    let foreign_key = foreign_key
        .map(|(column, parent)| {
            format!(
                ",\"foreign_keys\":[{{\"columns\":[\"{column}\"],\"references\":{{\"schema_key\":\"{parent}\",\"columns\":[\"id\"]}}}}]"
            )
        })
        .unwrap_or_default();
    format!(
        "INSERT INTO lix_registered_schema (value, lixcol_global) VALUES (CAST('{{\"$schema\":\"https://lix.dev/schema-v1.json\",\
         \"key\":\"{key}\",\"columns\":[{columns}],\"primary_key\":[\"id\"]{foreign_key}}}' AS JSONB), {global})"
    )
}

const ID_COLUMN: &str = "{\"name\":\"id\",\"type\":\"text\",\"nullable\":false}";
const VALUE_COLUMN: &str = "{\"name\":\"value\",\"type\":\"text\",\"nullable\":false}";

async fn register<S: Storage + Clone + Send + Sync + 'static>(lix: &Lix<S>) {
    let child_columns =
        format!("{ID_COLUMN},{{\"name\":\"parent_id\",\"type\":\"text\",\"nullable\":false}}");
    for global in [false, true] {
        for sql in [
            schema_sql(PARENT, ID_COLUMN, None, global),
            schema_sql(CHILD, &child_columns, Some(("parent_id", PARENT)), global),
            schema_sql(ITEM, &format!("{ID_COLUMN},{VALUE_COLUMN}"), None, global),
        ] {
            lix.execute(&sql, &[]).await.unwrap();
        }
    }
    lix.execute(
        &format!(
            "INSERT INTO lix_file(id,path) VALUES ('{FILE_A}','/eci-a'), ('{FILE_B}','/eci-b')"
        ),
        &[],
    )
    .await
    .unwrap();
}

async fn open() -> Lix<Memory> {
    let lix = crate::open_lix().with_storage(Memory::new()).await.unwrap();
    register(&lix).await;
    stats(&lix).await;
    lix
}

fn file_sql(file_id: Option<&str>) -> String {
    file_id.map_or_else(|| "NULL".to_owned(), |file_id| format!("'{file_id}'"))
}

async fn insert_item<S: Storage + Clone + Send + Sync + 'static>(
    lix: &Lix<S>,
    id: &str,
    value: &str,
    file_id: Option<&str>,
    untracked: bool,
    global: bool,
) -> Result<(), LixError> {
    lix.execute(
        &format!(
            "INSERT INTO {ITEM} (id, value, lixcol_file_id, lixcol_untracked, lixcol_global) \
             VALUES ('{id}', '{value}', {}, {untracked}, {global})",
            file_sql(file_id)
        ),
        &[],
    )
    .await
    .map(drop)
}

/// `(id, value, file_id, untracked, global)` of every visible item.
async fn items<S: Storage + Clone + Send + Sync + 'static>(
    lix: &Lix<S>,
) -> Vec<(String, String, Option<String>, bool, bool)> {
    let result = lix
        .execute(
            &format!(
                "SELECT id, value, lixcol_file_id, lixcol_untracked, lixcol_global FROM {ITEM} \
                 ORDER BY id, lixcol_file_id"
            ),
            &[],
        )
        .await
        .unwrap();
    result
        .rows()
        .iter()
        .map(|row| {
            let file_id = match row.get::<Value>("lixcol_file_id").unwrap() {
                Value::Null => None,
                Value::Text(file_id) => Some(file_id),
                value => panic!("unexpected file id {value:?}"),
            };
            (
                row.get::<String>("id").unwrap(),
                row.get::<String>("value").unwrap(),
                file_id,
                row.get::<bool>("lixcol_untracked").unwrap(),
                row.get::<bool>("lixcol_global").unwrap(),
            )
        })
        .collect()
}

fn assert_unique(result: Result<(), LixError>, message_part: &str) {
    let error = result.expect_err("the identity already exists");
    assert_eq!(error.code, LixError::CODE_UNIQUE, "{error:?}");
    assert!(error.message.contains(message_part), "{error:?}");
}

/// The bench shape: a parent and a child schema with a foreign key, both
/// imported in one transaction into empty collections. Validation proves
/// absence from the two collection counts and reads no identity; the second
/// import into the now non-empty collections reads every identity again.
#[tokio::test]
async fn first_import_into_empty_collections_skips_identity_reads() {
    let lix = open().await;
    let import = |offset: usize| {
        let parents = (offset..offset + 300)
            .map(|index| format!("('p{index:04}')"))
            .collect::<Vec<_>>()
            .join(", ");
        let children = (offset..offset + 300)
            .map(|index| format!("('c{index:04}', 'p{index:04}')"))
            .collect::<Vec<_>>()
            .join(", ");
        (
            format!("INSERT INTO {PARENT} (id) VALUES {parents}"),
            format!("INSERT INTO {CHILD} (id, parent_id) VALUES {children}"),
        )
    };
    for (round, offset) in [0, 300].into_iter().enumerate() {
        let (parents, children) = import(offset);
        stats(&lix).await;
        let mut transaction = lix.begin_transaction().await.unwrap();
        transaction.execute(&parents, &[]).await.unwrap();
        transaction.execute(&children, &[]).await.unwrap();
        transaction.commit().await.unwrap();
        let (probes, empty_collections) = stats(&lix).await;
        if round == 0 {
            assert_eq!((probes, empty_collections), (0, 2), "first import");
        } else {
            assert_eq!((probes, empty_collections), (600, 0), "second import");
        }
    }
    for (schema, expected) in [(PARENT, 600), (CHILD, 600)] {
        let result = lix
            .execute(&format!("SELECT COUNT(*) AS n FROM {schema}"), &[])
            .await
            .unwrap();
        assert_eq!(result.rows()[0].get::<i64>("n").unwrap(), expected);
    }
    // A duplicate in the same shape is still rejected by the full check.
    let mut transaction = lix.begin_transaction().await.unwrap();
    transaction
        .execute(&format!("INSERT INTO {PARENT} (id) VALUES ('p9999')"), &[])
        .await
        .unwrap();
    transaction
        .execute(
            &format!("INSERT INTO {CHILD} (id, parent_id) VALUES ('c0001', 'p9999')"),
            &[],
        )
        .await
        .unwrap();
    let error = transaction.commit().await.unwrap_err();
    assert_eq!(error.code, LixError::CODE_UNIQUE, "{error:?}");
}

#[tokio::test]
async fn duplicate_of_a_committed_row_still_conflicts() {
    let lix = open().await;
    insert_item(&lix, "k1", "first", None, false, false)
        .await
        .unwrap();
    stats(&lix).await;
    assert_unique(
        insert_item(&lix, "k1", "second", None, false, false).await,
        "",
    );
    assert_eq!(stats(&lix).await.1, 0);
    insert_item(&lix, "k2", "second", None, false, false)
        .await
        .unwrap();
    assert_eq!(
        items(&lix).await,
        vec![
            ("k1".into(), "first".into(), None, false, false),
            ("k2".into(), "second".into(), None, false, false),
        ]
    );
}

/// The only row with the identity is untracked: the collection count includes
/// it, and a tracked insert of the same identity is rejected (and vice versa).
#[tokio::test]
async fn identity_that_exists_only_untracked_still_conflicts() {
    let lix = open().await;
    insert_item(&lix, "k1", "untracked", None, true, false)
        .await
        .unwrap();
    stats(&lix).await;
    assert_unique(
        insert_item(&lix, "k1", "tracked", None, false, false).await,
        "a canonical untracked row with the same primary key already exists",
    );
    let (_, empty_collections) = stats(&lix).await;
    assert_eq!(empty_collections, 0);

    let lix = open().await;
    insert_item(&lix, "k1", "tracked", None, false, false)
        .await
        .unwrap();
    assert_unique(
        insert_item(&lix, "k1", "untracked", None, true, false).await,
        "a canonical tracked row with the same primary key already exists",
    );
    assert_eq!(
        items(&lix).await,
        vec![("k1".into(), "tracked".into(), None, false, false)]
    );
}

/// The same primary key in another file scope is a different identity: it
/// is allowed, but it makes the schema-wide collection non-empty, so the
/// shortcut is not taken. A duplicate within one file scope still conflicts.
#[tokio::test]
async fn identity_in_another_file_scope_is_still_allowed() {
    let lix = open().await;
    insert_item(&lix, "k1", "a", Some(FILE_A), false, false)
        .await
        .unwrap();
    stats(&lix).await;
    insert_item(&lix, "k1", "unfiled", None, false, false)
        .await
        .unwrap();
    insert_item(&lix, "k1", "b", Some(FILE_B), false, false)
        .await
        .unwrap();
    assert_eq!(stats(&lix).await.1, 0);
    assert_unique(
        insert_item(&lix, "k1", "a-again", Some(FILE_A), false, false).await,
        "",
    );
    assert_unique(
        insert_item(&lix, "k1", "unfiled-again", None, false, false).await,
        "",
    );
    let mut expected = vec![
        ("k1".to_owned(), "unfiled".to_owned(), None, false, false),
        (
            "k1".to_owned(),
            "a".to_owned(),
            Some(FILE_A.to_owned()),
            false,
            false,
        ),
        (
            "k1".to_owned(),
            "b".to_owned(),
            Some(FILE_B.to_owned()),
            false,
            false,
        ),
    ];
    expected.sort();
    let mut actual = items(&lix).await;
    actual.sort();
    assert_eq!(actual, expected);
}

/// A first file-scoped import into an empty schema takes the shortcut too.
#[tokio::test]
async fn first_file_scoped_import_into_an_empty_schema_skips_identity_reads() {
    let lix = open().await;
    let values = (0..50)
        .map(|index| format!("('f{index:03}', 'v', '{FILE_A}')"))
        .collect::<Vec<_>>()
        .join(", ");
    lix.execute(
        &format!("INSERT INTO {ITEM} (id, value, lixcol_file_id) VALUES {values}"),
        &[],
    )
    .await
    .unwrap();
    assert_eq!(stats(&lix).await, (0, 1));
    assert_unique(
        insert_item(&lix, "f000", "again", Some(FILE_A), false, false).await,
        "",
    );
    insert_item(&lix, "f000", "other scope", Some(FILE_B), false, false)
        .await
        .unwrap();
}

/// A branch created from a commit inherits its rows through a root current
/// base whose count is deferred, so the inherited identity is still read and
/// rejected. An identity committed only on another branch is allowed.
#[tokio::test]
async fn identity_inherited_from_the_parent_branch_still_conflicts() {
    let lix = open().await;
    let main = lix.active_branch_id().await.unwrap();
    insert_item(&lix, "k1", "main", None, false, false)
        .await
        .unwrap();
    let branch = lix
        .create_branch(crate::CreateBranchOptions {
            id: None,
            name: "eci-child".to_owned(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    lix.switch_branch(crate::SwitchBranchOptions {
        branch_id: branch.id.clone(),
    })
    .await
    .unwrap();
    stats(&lix).await;
    assert_unique(
        insert_item(&lix, "k1", "branch", None, false, false).await,
        "",
    );
    assert_eq!(stats(&lix).await.1, 0);
    insert_item(&lix, "k2", "branch-only", None, false, false)
        .await
        .unwrap();
    lix.switch_branch(crate::SwitchBranchOptions { branch_id: main })
        .await
        .unwrap();
    insert_item(&lix, "k2", "main", None, false, false)
        .await
        .unwrap();
    assert_eq!(
        items(&lix).await,
        vec![
            ("k1".into(), "main".into(), None, false, false),
            ("k2".into(), "main".into(), None, false, false),
        ]
    );
}

/// The full check never treats a global row as an existing branch-local
/// identity; the branch-local insert shadows it. The shortcut keeps that.
#[tokio::test]
async fn identity_that_exists_only_globally_is_allowed_locally() {
    let lix = open().await;
    insert_item(&lix, "k1", "global", None, false, true)
        .await
        .unwrap();
    stats(&lix).await;
    insert_item(&lix, "k1", "local", None, false, false)
        .await
        .unwrap();
    assert_eq!(
        items(&lix).await,
        vec![("k1".into(), "local".into(), None, false, false)]
    );
    assert_unique(
        insert_item(&lix, "k1", "global-again", None, false, true).await,
        "",
    );
}

async fn insert_child<S: Storage + Clone + Send + Sync + 'static>(
    lix: &Lix<S>,
    id: &str,
    parent_id: &str,
    untracked: bool,
) -> Result<(), LixError> {
    lix.execute(
        &format!(
            "INSERT INTO {CHILD} (id, parent_id, lixcol_untracked) \
             VALUES ('{id}', '{parent_id}', {untracked})"
        ),
        &[],
    )
    .await
    .map(drop)
}

/// `(id, parent_id, untracked)` of every visible child.
async fn children<S: Storage + Clone + Send + Sync + 'static>(
    lix: &Lix<S>,
) -> Vec<(String, String, bool)> {
    let result = lix
        .execute(
            &format!("SELECT id, parent_id, lixcol_untracked FROM {CHILD} ORDER BY id"),
            &[],
        )
        .await
        .unwrap();
    result
        .rows()
        .iter()
        .map(|row| {
            (
                row.get::<String>("id").unwrap(),
                row.get::<String>("parent_id").unwrap(),
                row.get::<bool>("lixcol_untracked").unwrap(),
            )
        })
        .collect()
}

/// Tombstones are not live members. After every row of a collection is
/// deleted row by row the count is zero again and re-inserting a deleted
/// identity takes the shortcut; after a whole-collection DELETE the count is
/// deferred and the full check runs. Both allow the insert, as before.
#[tokio::test]
async fn tombstoned_identities_can_be_inserted_again() {
    let lix = open().await;
    lix.execute(
        &format!("INSERT INTO {PARENT} (id) VALUES ('p1'), ('p2')"),
        &[],
    )
    .await
    .unwrap();
    for id in ["c1", "c2"] {
        insert_child(&lix, id, "p1", false).await.unwrap();
    }
    lix.execute(
        &format!("DELETE FROM {CHILD} WHERE id IN ('c1', 'c2')"),
        &[],
    )
    .await
    .unwrap();
    stats(&lix).await;
    insert_child(&lix, "c1", "p2", false).await.unwrap();
    assert_eq!(stats(&lix).await, (0, 1));
    let error = insert_child(&lix, "c1", "p1", false).await.unwrap_err();
    assert_eq!(error.code, LixError::CODE_UNIQUE, "{error:?}");
    assert_eq!(
        children(&lix).await,
        vec![("c1".into(), "p2".into(), false)]
    );

    // A whole-collection DELETE of file-scoped rows (full validation path).
    for id in ["f1", "f2"] {
        insert_item(&lix, id, "first", Some(FILE_A), false, false)
            .await
            .unwrap();
    }
    lix.execute(&format!("DELETE FROM {ITEM}"), &[])
        .await
        .unwrap();
    stats(&lix).await;
    insert_item(&lix, "f1", "second", Some(FILE_A), false, false)
        .await
        .unwrap();
    // The collection fence defers the count, so the full check runs.
    assert_eq!(stats(&lix).await, (1, 0));

    assert_unique(
        insert_item(&lix, "f1", "third", Some(FILE_A), false, false).await,
        "",
    );
    assert_eq!(
        items(&lix).await,
        vec![(
            "f1".into(),
            "second".into(),
            Some(FILE_A.to_owned()),
            false,
            false
        )]
    );
}

type Race = Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send>;

/// Runs one concurrent commit at the first storage commit that is
/// acknowledged after commit validation proved the child collection empty:
/// strictly between this transaction's validation snapshot and its
/// publication.
#[derive(Clone)]
struct RacingStorage {
    inner: Memory,
    branch_id: Arc<Mutex<String>>,
    race: Arc<Mutex<Option<Race>>>,
}

impl RacingStorage {
    fn take_race(&self) -> Option<Race> {
        let branch_id = self.branch_id.lock().unwrap().clone();
        if peek_committed_insert_identity_empty_collections(&branch_id, CHILD) == 0 {
            return None;
        }
        self.race.lock().unwrap().take()
    }
}

struct RacingWrite {
    inner: MemoryWrite,
    storage: RacingStorage,
}

impl StorageWrite for RacingWrite {
    async fn put_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        self.inner.put_many(space, entries).await
    }

    async fn replace_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        self.inner.replace_many(space, entries).await
    }

    async fn delete_many(&mut self, space: StorageSpace, keys: &[Key]) -> Result<(), StorageError> {
        self.inner.delete_many(space, keys).await
    }

    async fn delete_range(
        &mut self,
        space: StorageSpace,
        range: KeyRange,
    ) -> Result<(), StorageError> {
        self.inner.delete_range(space, range).await
    }

    async fn commit(self) -> Result<CommitResult, StorageError> {
        if let Some(race) = self.storage.take_race() {
            race().await;
        }
        self.inner.commit().await
    }

    async fn rollback(self) -> Result<(), StorageError> {
        self.inner.rollback().await
    }
}

impl Storage for RacingStorage {
    type Read<'a>
        = MemoryRead
    where
        Self: 'a;
    type Write<'a>
        = RacingWrite
    where
        Self: 'a;

    async fn acquire_session(&self) -> Result<StorageSessionToken, StorageError> {
        self.inner.acquire_session().await
    }

    async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.inner.begin_read(options).await
    }

    async fn begin_write(&self, options: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        Ok(RacingWrite {
            inner: self.inner.begin_write(options).await?,
            storage: self.clone(),
        })
    }
}

/// A concurrent writer commits between this transaction's validation, which
/// proved the child collection empty, and its publication. The publication
/// must not succeed on that stale proof: the identity is never stored twice,
/// and a disjoint concurrent insert does not lose this transaction's row.
#[tokio::test]
async fn concurrent_insert_racing_the_commit_is_not_overwritten() {
    for (racing_id, racing_untracked) in [("c1", false), ("c2", false), ("c1", true)] {
        let memory = Memory::new();
        let storage = RacingStorage {
            inner: memory.clone(),
            branch_id: Arc::default(),
            race: Arc::default(),
        };
        let lix = crate::open_lix()
            .with_storage(storage.clone())
            .await
            .unwrap();
        register(&lix).await;
        lix.execute(&format!("INSERT INTO {PARENT} (id) VALUES ('p1')"), &[])
            .await
            .unwrap();
        *storage.branch_id.lock().unwrap() = lix.active_branch_id().await.unwrap();
        stats(&lix).await;
        let racer = crate::open_lix().with_storage(memory).await.unwrap();

        let mut transaction = lix.begin_transaction().await.unwrap();
        transaction
            .execute(
                &format!("INSERT INTO {CHILD} (id, parent_id) VALUES ('c1', 'p1')"),
                &[],
            )
            .await
            .unwrap();
        let racer_id = racing_id.to_owned();
        let raced = Arc::new(Mutex::new(false));
        let raced_flag = Arc::clone(&raced);
        *storage.race.lock().unwrap() = Some(Box::new(move || {
            Box::pin(async move {
                insert_child(&racer, &racer_id, "p1", racing_untracked)
                    .await
                    .unwrap();
                *raced_flag.lock().unwrap() = true;
            })
        }));
        let outcome = transaction.commit().await;
        assert!(
            *raced.lock().unwrap(),
            "the racer must commit after validation proved the collection empty"
        );
        let rows = children(&lix).await;
        let outcome = outcome.map(drop);
        let label = format!("racer {racing_id} untracked={racing_untracked}: {rows:?} {outcome:?}");
        if racing_id == "c1" {
            // The racer took the identity first; this transaction must fail.
            let error = outcome.expect_err(&label);
            assert!(
                [LixError::CODE_TRANSACTION_CONFLICT, LixError::CODE_UNIQUE]
                    .contains(&error.code.as_str()),
                "{label}"
            );
            assert_eq!(
                rows,
                vec![("c1".to_owned(), "p1".to_owned(), racing_untracked)],
                "{label}"
            );
        } else {
            match outcome {
                Ok(()) => assert_eq!(
                    rows,
                    vec![
                        ("c1".to_owned(), "p1".to_owned(), false),
                        ("c2".to_owned(), "p1".to_owned(), false),
                    ],
                    "{label}"
                ),
                // Disjoint, but a commit raced the snapshot: the conservative
                // retryable conflict, and the racer's row alone is stored.
                Err(error) => {
                    assert_eq!(error.code, LixError::CODE_TRANSACTION_CONFLICT, "{label}");
                    assert_eq!(
                        rows,
                        vec![("c2".to_owned(), "p1".to_owned(), false)],
                        "{label}"
                    );
                }
            }
        }
    }
}
