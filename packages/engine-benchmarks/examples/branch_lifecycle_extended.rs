//! Benchmark-only branch lifecycle and selector baseline.
//!
//! Repository setup is excluded from surface timings. The harness measures
//! public branch listing/ref projections, derived commit projections, and the
//! supported create/switch/advance/delete lifecycle. It also drops and reopens
//! the adapter and verifies the surviving branch digests.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Instant;

use blake3::Hasher;
use lix::integration::Engine;
use lix::{CreateBranchOptions, ExecuteResult, SwitchBranchOptions, Value};
use uuid::Uuid;

#[path = "../benches/tracked_state_crud/storage.rs"]
mod storage;

trait ReopenableStorage: lix::storage::Storage + Clone + Send + Sync + 'static {
    fn reopen(path: PathBuf) -> Self;
}

impl ReopenableStorage for storage::RocksDB {
    fn reopen(path: PathBuf) -> Self {
        Self::open(path).expect("reopen RocksDB branch baseline")
    }
}

#[cfg(feature = "slatedb")]
impl ReopenableStorage for storage::SlateDB {
    fn reopen(path: PathBuf) -> Self {
        Self::open(path).expect("reopen SlateDB branch baseline")
    }
}

#[derive(Clone, Copy, Debug)]
enum Backend {
    RocksDb,
    #[cfg(feature = "slatedb")]
    SlateDb,
}

impl Backend {
    fn parse(value: &str) -> Self {
        match value {
            "rocksdb" => Self::RocksDb,
            #[cfg(feature = "slatedb")]
            "slatedb" => Self::SlateDb,
            other => panic!("unknown backend {other}; expected rocksdb or slatedb"),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::RocksDb => "rocksdb",
            #[cfg(feature = "slatedb")]
            Self::SlateDb => "slatedb",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Surface {
    BranchListing,
    BranchRefR1,
    BranchRefRMany,
    DerivedEmpty,
    DerivedExplicit,
    DerivedGlobal,
}

impl Surface {
    fn name(self) -> &'static str {
        match self {
            Self::BranchListing => "branch_listing",
            Self::BranchRefR1 => "branch_ref_r1",
            Self::BranchRefRMany => "branch_ref_rmany",
            Self::DerivedEmpty => "derived_commit_by_branch_empty",
            Self::DerivedExplicit => "derived_commit_by_branch_explicit",
            Self::DerivedGlobal => "derived_commit_by_branch_global",
        }
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let backend = Backend::parse(&args.next().unwrap_or_else(|| usage("missing backend")));
    let branch_count = args
        .next()
        .unwrap_or_else(|| usage("missing branch count"))
        .parse::<usize>()
        .unwrap_or_else(|error| usage(&format!("invalid branch count: {error}")));
    let samples = args
        .next()
        .unwrap_or_else(|| usage("missing sample count"))
        .parse::<usize>()
        .unwrap_or_else(|error| usage(&format!("invalid sample count: {error}")));
    if !matches!(branch_count, 1 | 32 | 128) {
        usage("branch count must be 1, 32, or 128");
    }
    assert!(samples > 0, "samples must be positive");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build branch lifecycle runtime");
    runtime.block_on(run(backend, branch_count, samples));
}

fn usage(message: &str) -> ! {
    eprintln!("{message}\nusage: branch_lifecycle_extended <rocksdb|slatedb> <1|32|128> <samples>");
    std::process::exit(2);
}

async fn run(backend: Backend, branch_count: usize, samples: usize) {
    println!(
        "branch_lifecycle_extended backend={} branches={} samples={} setup_excluded=true backend_counters=unavailable",
        backend.name(), branch_count, samples
    );

    match backend {
        Backend::RocksDb => {
            let profile = storage::StorageProfile::RocksDB.storage();
            let storage::ProfileStorage::RocksDB { storage, _dir } = profile else {
                unreachable!()
            };
            run_storage(
                storage,
                _dir.path().join("bench.rocksdb"),
                branch_count,
                samples,
            )
            .await;
        }
        #[cfg(feature = "slatedb")]
        Backend::SlateDb => {
            let profile = storage::StorageProfile::SlateDB.storage();
            let storage::ProfileStorage::SlateDB { storage, _dir } = profile else {
                unreachable!()
            };
            run_storage(
                storage,
                _dir.path().join("bench.slatedb"),
                branch_count,
                samples,
            )
            .await;
        }
    }
}

async fn run_storage<StorageImpl>(
    storage: StorageImpl,
    path: PathBuf,
    branch_count: usize,
    samples: usize,
) where
    StorageImpl: ReopenableStorage,
{
    let setup_started = Instant::now();
    let receipt = Engine::<StorageImpl>::initialize(storage.clone())
        .await
        .expect("initialize branch lifecycle storage");
    let engine = Engine::new(storage.clone())
        .await
        .expect("open branch lifecycle engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("open branch lifecycle session");

    let mut branch_ids = Vec::with_capacity(branch_count);
    for index in 0..branch_count {
        let branch_id = format!("01900000-0000-7000-8000-{index:012x}");
        session
            .create_branch(CreateBranchOptions {
                id: Some(branch_id.clone()),
                name: format!("extended-{index:03}"),
                from_commit_id: Some(receipt.initial_commit_id.clone()),
            })
            .await
            .expect("create setup branch");
        branch_ids.push(branch_id);
    }
    let setup_us = setup_started.elapsed().as_micros();
    let exact_branch_id = branch_ids[branch_count / 2].clone();
    let many_branch_ids = branch_ids.join("','");

    println!(
        "setup_us={} branches_created={} initial_commit={} exact_branch_id={} main_branch_id={}",
        setup_us, branch_count, receipt.initial_commit_id, exact_branch_id, receipt.main_branch_id
    );

    let queries = vec![
        (
            Surface::BranchListing,
            "SELECT id, name, hidden, commit_id FROM lix_branch ORDER BY id".to_string(),
        ),
        (
            Surface::BranchRefR1,
            format!(
                "SELECT id, commit_id FROM lix_branch_ref WHERE id = '{}' ORDER BY id",
                exact_branch_id
            ),
        ),
        (
            Surface::BranchRefRMany,
            format!(
                "SELECT id, commit_id FROM lix_branch_ref WHERE id IN ('{}') ORDER BY id",
                many_branch_ids
            ),
        ),
        (
            Surface::DerivedEmpty,
            "SELECT id, lixcol_branch_id, lixcol_global, lixcol_untracked \
             FROM lix_commit_by_branch ORDER BY id, lixcol_branch_id"
                .to_string(),
        ),
        (
            Surface::DerivedExplicit,
            format!(
                "SELECT id, lixcol_branch_id, lixcol_global, lixcol_untracked \
                 FROM lix_commit_by_branch WHERE lixcol_branch_id = '{}' \
                 ORDER BY id, lixcol_branch_id",
                exact_branch_id
            ),
        ),
        (
            Surface::DerivedGlobal,
            "SELECT id, lixcol_branch_id, lixcol_global, lixcol_untracked \
             FROM lix_commit_by_branch WHERE lixcol_global = true \
             ORDER BY id, lixcol_branch_id"
                .to_string(),
        ),
    ];
    let _baseline_digests = measure_queries(&session, &queries, samples).await;

    let lifecycle_id = "01900000-0000-7000-8000-ffffffffffff".to_string();
    let create_started = Instant::now();
    session
        .execute(
            &format!(
                "INSERT INTO lix_branch (id, name) VALUES ('{}', 'lifecycle-extra')",
                lifecycle_id
            ),
            &[],
        )
        .await
        .expect("lifecycle branch create");
    println!(
        "lifecycle=create elapsed_us={}",
        create_started.elapsed().as_micros()
    );

    let switch_started = Instant::now();
    session
        .switch_branch(SwitchBranchOptions {
            branch_id: lifecycle_id.clone(),
        })
        .await
        .expect("lifecycle branch switch");
    println!(
        "lifecycle=switch elapsed_us={}",
        switch_started.elapsed().as_micros()
    );

    let advance_started = Instant::now();
    session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('branch-lifecycle-advance', 'advanced')",
            &[],
        )
        .await
        .expect("lifecycle branch advance");
    println!(
        "lifecycle=advance elapsed_us={}",
        advance_started.elapsed().as_micros()
    );

    session
        .switch_branch(SwitchBranchOptions {
            branch_id: receipt.main_branch_id.clone(),
        })
        .await
        .expect("switch back to main before delete");
    let delete_started = Instant::now();
    session
        .execute(
            &format!("DELETE FROM lix_branch WHERE id = '{lifecycle_id}'"),
            &[],
        )
        .await
        .expect("lifecycle branch delete");
    println!(
        "lifecycle=delete elapsed_us={}",
        delete_started.elapsed().as_micros()
    );

    let listing_query = &queries[0].1;
    let refs_query = &queries[2].1;
    let post_delete_listing = session
        .execute(listing_query, &[])
        .await
        .expect("post-delete lifecycle listing");
    let post_delete_refs = session
        .execute(refs_query, &[])
        .await
        .expect("post-delete lifecycle refs");
    let post_delete_listing_digest = digest(&post_delete_listing);
    let post_delete_refs_digest = digest(&post_delete_refs);
    let post_delete_listing_semantic_digest = canonical_digest(&post_delete_listing);
    let post_delete_refs_semantic_digest = canonical_digest(&post_delete_refs);
    println!(
        "lifecycle=post_delete listing_digest={} refs_digest={} listing_semantic_digest={} refs_semantic_digest={} rows_listing={} rows_refs={}",
        post_delete_listing_digest,
        post_delete_refs_digest,
        post_delete_listing_semantic_digest,
        post_delete_refs_semantic_digest,
        post_delete_listing.rows().len(),
        post_delete_refs.rows().len()
    );

    drop(session);
    drop(engine);
    drop(storage);
    let reopen_started = Instant::now();
    let reopened_storage = StorageImpl::reopen(path);
    let reopened_engine = Engine::new(reopened_storage)
        .await
        .expect("cold reopen branch lifecycle engine");
    let reopened_session = reopened_engine
        .open_workspace_session()
        .await
        .expect("cold reopen branch lifecycle session");
    let reopened_listing = reopened_session
        .execute(listing_query, &[])
        .await
        .expect("cold reopen listing");
    let reopened_refs = reopened_session
        .execute(refs_query, &[])
        .await
        .expect("cold reopen refs");
    assert_eq!(post_delete_listing_digest, digest(&reopened_listing));
    assert_eq!(post_delete_refs_digest, digest(&reopened_refs));
    assert_eq!(
        post_delete_listing_semantic_digest,
        canonical_digest(&reopened_listing)
    );
    assert_eq!(
        post_delete_refs_semantic_digest,
        canonical_digest(&reopened_refs)
    );
    println!(
        "cold_reopen_us={} listing_digest={} refs_digest={} listing_semantic_digest={} refs_semantic_digest={} rows_listing={} rows_refs={}",
        reopen_started.elapsed().as_micros(),
        digest(&reopened_listing),
        digest(&reopened_refs),
        canonical_digest(&reopened_listing),
        canonical_digest(&reopened_refs),
        reopened_listing.rows().len(),
        reopened_refs.rows().len()
    );
}

async fn measure_queries<StorageImpl>(
    session: &lix::integration::SessionContext<StorageImpl>,
    queries: &[(Surface, String)],
    samples: usize,
) -> Vec<(Surface, String)>
where
    StorageImpl: lix::storage::Storage + Clone + Send + Sync + 'static,
{
    let mut baseline = Vec::new();
    for sample in 0..samples {
        for (surface, query) in queries {
            let started = Instant::now();
            let result = session
                .execute(query, &[])
                .await
                .unwrap_or_else(|error| panic!("{} failed: {error:?}", surface.name()));
            let elapsed_us = started.elapsed().as_micros();
            let result_digest = digest(&result);
            let semantic_digest = canonical_digest(&result);
            if sample == 0 {
                baseline.push((*surface, semantic_digest.clone()));
            } else {
                let expected = baseline
                    .iter()
                    .find(|(candidate, _)| candidate == surface)
                    .map(|(_, digest)| digest)
                    .expect("baseline query digest");
                assert_eq!(
                    expected,
                    &semantic_digest,
                    "digest changed for {}",
                    surface.name()
                );
            }
            println!(
                "sample={} surface={} elapsed_us={} rows={} digest={} semantic_digest={}",
                sample,
                surface.name(),
                elapsed_us,
                result.rows().len(),
                result_digest,
                semantic_digest
            );
        }
    }
    baseline
}

fn digest(result: &ExecuteResult) -> String {
    let mut hasher = Hasher::new();
    feed(&mut hasher, b"forktree-branch-lifecycle-extended-v1");
    feed(&mut hasher, &(result.columns().len() as u64).to_le_bytes());
    for column in result.columns() {
        feed(&mut hasher, column.as_bytes());
    }
    feed(&mut hasher, &(result.rows().len() as u64).to_le_bytes());
    for row in result.rows() {
        feed(&mut hasher, &(row.values().len() as u64).to_le_bytes());
        for value in row.values() {
            feed(
                &mut hasher,
                &serde_json::to_vec(value).expect("Value is serializable"),
            );
        }
    }
    hasher.finalize().to_hex().to_string()
}

/// Cross-run semantic fingerprint. Engine initialization intentionally creates
/// fresh UUID-v7 commit identities, so raw digests cannot be compared between
/// exact-main and a candidate. UUID-bearing values are replaced by canonical
/// ordinals; columns, row order, multiplicity, NULLs, and all other values
/// remain exact. This is an oracle normalization, not a production identity
/// rule.
fn canonical_digest(result: &ExecuteResult) -> String {
    let mut ids = BTreeSet::new();
    for row in result.rows() {
        for value in row.values() {
            collect_uuid_values(value, &mut ids);
        }
    }
    let ids = ids
        .into_iter()
        .enumerate()
        .map(|(index, id)| (id, format!("$uuid:{index}")))
        .collect::<BTreeMap<_, _>>();

    let mut hasher = Hasher::new();
    feed(&mut hasher, b"forktree-branch-lifecycle-semantic-v2");
    feed(&mut hasher, &(result.columns().len() as u64).to_le_bytes());
    for column in result.columns() {
        feed(&mut hasher, column.as_bytes());
    }
    feed(&mut hasher, &(result.rows().len() as u64).to_le_bytes());
    for row in result.rows() {
        feed(&mut hasher, &(row.values().len() as u64).to_le_bytes());
        for value in row.values() {
            let normalized = normalize_value(value, &ids);
            feed(
                &mut hasher,
                &serde_json::to_vec(&normalized).expect("normalized Value is serializable"),
            );
        }
    }
    hasher.finalize().to_hex().to_string()
}

fn collect_uuid_values(value: &Value, ids: &mut BTreeSet<String>) {
    if let Value::Text(text) = value {
        if Uuid::parse_str(text).is_ok() {
            ids.insert(text.clone());
        }
    }
}

fn normalize_value(value: &Value, ids: &BTreeMap<String, String>) -> serde_json::Value {
    match value {
        Value::Text(text) => ids
            .get(text)
            .cloned()
            .map(serde_json::Value::String)
            .unwrap_or_else(|| serde_json::Value::String(text.clone())),
        other => serde_json::to_value(other).expect("Value is serializable"),
    }
}

fn feed(hasher: &mut Hasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}
