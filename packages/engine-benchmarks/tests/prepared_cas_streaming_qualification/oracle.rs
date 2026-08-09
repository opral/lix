#![deny(warnings)]

//! Standalone contract for the private prepared-CAS streaming seam.
//!
//! This file has no production imports. It models the required ownership and
//! counters until a compatible engine implementation exposes them.

use std::collections::{BTreeMap, BTreeSet};

const CHUNK_BYTES: usize = 64 * 1024;
const RECEIPT_METADATA_BYTES: usize = 256;
const MANIFEST_METADATA_BYTES: usize = 192;
const CHUNK_METADATA_BYTES: usize = 96;
const FILES: usize = 65;
const PAYLOAD_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Adapter {
    Memory,
    RocksDb,
    SlateDb,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Fault {
    MissingReceipt,
    WrongOwner,
    ManifestSubstitution,
    ChunkSubstitution,
    SizeMismatch,
    DigestMismatch,
    CrossView,
    DuplicateReceipt,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Payload {
    file_id: String,
    owner: String,
    view_id: u64,
    size: usize,
    digest: u64,
}

impl Payload {
    fn new(index: usize) -> Self {
        let file_id = format!("file-{index:04}");
        Self {
            digest: digest(&format!("{file_id}:{PAYLOAD_BYTES}")),
            file_id,
            owner: "branch-main".to_owned(),
            view_id: 7,
            size: PAYLOAD_BYTES,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ChunkReceipt {
    file_id: String,
    owner: String,
    view_id: u64,
    ordinal: usize,
    size: usize,
    digest: u64,
    object_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ManifestReceipt {
    file_id: String,
    owner: String,
    view_id: u64,
    size: usize,
    digest: u64,
    chunk_ids: Vec<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PreparedReceipt {
    payload: Payload,
    manifest: ManifestReceipt,
    chunks: Vec<ChunkReceipt>,
}

impl PreparedReceipt {
    fn for_payload(payload: &Payload) -> Self {
        let chunk_count = payload.size.div_ceil(CHUNK_BYTES);
        let chunks = (0..chunk_count)
            .map(|ordinal| ChunkReceipt {
                file_id: payload.file_id.clone(),
                owner: payload.owner.clone(),
                view_id: payload.view_id,
                ordinal,
                size: (payload.size - ordinal * CHUNK_BYTES).min(CHUNK_BYTES),
                digest: digest(&format!("{}:{ordinal}", payload.digest)),
                object_id: digest(&format!("object:{}:{ordinal}", payload.digest)),
            })
            .collect::<Vec<_>>();
        let manifest = ManifestReceipt {
            file_id: payload.file_id.clone(),
            owner: payload.owner.clone(),
            view_id: payload.view_id,
            size: payload.size,
            digest: payload.digest,
            chunk_ids: chunks.iter().map(|chunk| chunk.object_id).collect(),
        };
        Self {
            payload: payload.clone(),
            manifest,
            chunks,
        }
    }

    fn metadata_bytes(&self) -> usize {
        RECEIPT_METADATA_BYTES + MANIFEST_METADATA_BYTES + self.chunks.len() * CHUNK_METADATA_BYTES
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VisibleRow {
    file_id: String,
    digest: u64,
    size: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct Counters {
    file_content_writes_payload_bytes: usize,
    file_content_writes_metadata_bytes: usize,
    peak_file_content_writes_bytes: usize,
    prepared_receipt_bytes: usize,
    peak_prepared_receipt_bytes: usize,
    prepared_object_payload_bytes: usize,
    peak_prepared_object_payload_bytes: usize,
    prepared_object_metadata_bytes: usize,
    peak_transaction_retained_payload_bytes: usize,
    semantic_markers: usize,
    commits: usize,
}

impl Counters {
    fn receipt_bound(page_size: usize) -> usize {
        let chunks_per_payload = PAYLOAD_BYTES.div_ceil(CHUNK_BYTES);
        page_size
            * (RECEIPT_METADATA_BYTES
                + MANIFEST_METADATA_BYTES
                + chunks_per_payload * CHUNK_METADATA_BYTES)
            + CHUNK_BYTES
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ModelStore {
    adapter: Adapter,
    rows: BTreeMap<String, VisibleRow>,
    selectors: BTreeSet<String>,
    marker: Option<String>,
    orphan_payload_bytes: usize,
    orphan_object_count: usize,
}

impl ModelStore {
    fn new(adapter: Adapter) -> Self {
        Self {
            adapter,
            rows: BTreeMap::new(),
            selectors: BTreeSet::new(),
            marker: None,
            orphan_payload_bytes: 0,
            orphan_object_count: 0,
        }
    }

    fn begin(&self, owner: &str, view_id: u64, page_size: usize) -> ModelTransaction {
        ModelTransaction {
            adapter: self.adapter,
            owner: owner.to_owned(),
            view_id,
            page_size,
            rows: BTreeMap::new(),
            receipts: BTreeMap::new(),
            prepared_objects: 0,
            marker: None,
            counters: Counters::default(),
        }
    }

    fn commit(&mut self, transaction: ModelTransaction) -> Result<Counters, String> {
        let marker = transaction
            .marker
            .as_ref()
            .ok_or_else(|| "semantic commit marker is missing".to_owned())?;
        if transaction.counters.semantic_markers != 1 {
            return Err("semantic commit marker count is not exactly one".to_owned());
        }
        if transaction.counters.commits != 0 {
            return Err("transaction was committed twice".to_owned());
        }
        self.rows = transaction.rows;
        self.selectors.insert(transaction.owner.clone());
        self.marker = Some(marker.clone());
        let mut counters = transaction.counters;
        counters.commits = 1;
        Ok(counters)
    }

    fn cold_reopen(&self) -> Self {
        Self {
            adapter: self.adapter,
            rows: self.rows.clone(),
            selectors: self.selectors.clone(),
            marker: self.marker.clone(),
            orphan_payload_bytes: self.orphan_payload_bytes,
            orphan_object_count: self.orphan_object_count,
        }
    }

    fn reclaim_orphans(&mut self) -> (usize, usize) {
        let reclaimed = (self.orphan_payload_bytes, self.orphan_object_count);
        self.orphan_payload_bytes = 0;
        self.orphan_object_count = 0;
        reclaimed
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ModelTransaction {
    adapter: Adapter,
    owner: String,
    view_id: u64,
    page_size: usize,
    rows: BTreeMap<String, VisibleRow>,
    receipts: BTreeMap<String, PreparedReceipt>,
    prepared_objects: usize,
    marker: Option<String>,
    counters: Counters,
}

impl ModelTransaction {
    fn stage_page(&mut self, page: &[PreparedReceipt]) -> Result<(), String> {
        if page.len() > self.page_size {
            return Err("receipt page exceeds configured page size".to_owned());
        }
        let page_metadata = page
            .iter()
            .map(PreparedReceipt::metadata_bytes)
            .sum::<usize>();
        self.counters.prepared_receipt_bytes += page_metadata;
        self.counters.peak_prepared_receipt_bytes =
            self.counters.peak_prepared_receipt_bytes.max(page_metadata);
        self.counters.file_content_writes_metadata_bytes += page_metadata;
        self.counters.peak_file_content_writes_bytes = self
            .counters
            .peak_file_content_writes_bytes
            .max(page_metadata);
        self.counters.peak_transaction_retained_payload_bytes = self
            .counters
            .peak_transaction_retained_payload_bytes
            .max(CHUNK_BYTES);

        for receipt in page {
            // Prepared CAS objects are already owned by this transaction's
            // rollback/reclamation scope before receipt authentication. A
            // malformed receipt therefore still has reclaimable orphan
            // objects, but it must never enter visible rows or selectors.
            self.prepared_objects += receipt.chunks.len() + 1;
            self.counters.prepared_object_payload_bytes += receipt.payload.size;
            self.counters.peak_prepared_object_payload_bytes = self
                .counters
                .peak_prepared_object_payload_bytes
                .max(CHUNK_BYTES);
            self.counters.prepared_object_metadata_bytes +=
                MANIFEST_METADATA_BYTES + receipt.chunks.len() * CHUNK_METADATA_BYTES;
            validate_receipt(receipt, &self.owner, self.view_id)?;
            if self
                .receipts
                .insert(receipt.payload.file_id.clone(), receipt.clone())
                .is_some()
            {
                return Err("duplicate file receipt".to_owned());
            }
            self.rows.insert(
                receipt.payload.file_id.clone(),
                VisibleRow {
                    file_id: receipt.payload.file_id.clone(),
                    digest: receipt.payload.digest,
                    size: receipt.payload.size,
                },
            );
        }
        Ok(())
    }

    fn stage_marker(&mut self, marker: &str) -> Result<(), String> {
        if self.marker.replace(marker.to_owned()).is_some() {
            return Err("duplicate semantic commit marker".to_owned());
        }
        self.counters.semantic_markers += 1;
        Ok(())
    }

    fn rollback(self, store: &mut ModelStore) {
        store.orphan_payload_bytes += self.counters.prepared_object_payload_bytes;
        store.orphan_object_count += self.prepared_objects;
    }
}

fn validate_receipt(receipt: &PreparedReceipt, owner: &str, view_id: u64) -> Result<(), String> {
    if receipt.payload.owner != owner
        || receipt.manifest.owner != owner
        || receipt.chunks.iter().any(|chunk| chunk.owner != owner)
    {
        return Err("prepared receipt owner mismatch".to_owned());
    }
    if receipt.payload.view_id != view_id
        || receipt.manifest.view_id != view_id
        || receipt.chunks.iter().any(|chunk| chunk.view_id != view_id)
    {
        return Err("prepared receipt cross-view mismatch".to_owned());
    }
    if receipt.manifest.file_id != receipt.payload.file_id
        || receipt.manifest.size != receipt.payload.size
        || receipt.manifest.digest != receipt.payload.digest
    {
        return Err("prepared manifest identity/size/digest mismatch".to_owned());
    }
    if receipt.manifest.chunk_ids
        != receipt
            .chunks
            .iter()
            .map(|chunk| chunk.object_id)
            .collect::<Vec<_>>()
    {
        return Err("prepared manifest chunk identity mismatch".to_owned());
    }
    let mut total = 0;
    let mut seen = BTreeSet::new();
    for chunk in &receipt.chunks {
        if chunk.file_id != receipt.payload.file_id || !seen.insert(chunk.ordinal) {
            return Err("prepared chunk identity/order mismatch".to_owned());
        }
        if chunk.size > CHUNK_BYTES || chunk.size == 0 {
            return Err("prepared chunk size mismatch".to_owned());
        }
        total += chunk.size;
        if chunk.digest != digest(&format!("{}:{}", receipt.payload.digest, chunk.ordinal)) {
            return Err("prepared chunk digest mismatch".to_owned());
        }
    }
    if total != receipt.payload.size {
        return Err("prepared chunk aggregate size mismatch".to_owned());
    }
    if receipt.chunks.is_empty() {
        return Err("prepared receipt has no chunks".to_owned());
    }
    Ok(())
}

fn digest(value: &str) -> u64 {
    value.bytes().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    })
}

fn fixture() -> Vec<Payload> {
    (0..FILES).rev().map(Payload::new).collect()
}

fn tree_digest(rows: &BTreeMap<String, VisibleRow>) -> u64 {
    rows.values().fold(0xcbf29ce484222325, |hash, row| {
        digest(&format!(
            "{hash}:{}:{}:{}",
            row.file_id, row.size, row.digest
        ))
    })
}

fn plugin_digest(rows: &BTreeMap<String, VisibleRow>) -> u64 {
    rows.values().fold(0xcbf29ce484222325, |hash, row| {
        digest(&format!("{hash}:{}:{}", row.file_id, row.digest))
    })
}

fn run_success(adapter: Adapter, page_size: usize) -> (ModelStore, Counters, u64, u64) {
    let payloads = fixture();
    let total_payload = payloads.iter().map(|payload| payload.size).sum::<usize>();
    let mut store = ModelStore::new(adapter);
    let mut transaction = store.begin("branch-main", 7, page_size);
    let mut receipts = payloads
        .iter()
        .map(PreparedReceipt::for_payload)
        .collect::<Vec<_>>();
    receipts.sort_by(|left, right| left.payload.file_id.cmp(&right.payload.file_id));
    for page in receipts.chunks(page_size) {
        transaction.stage_page(page).expect("valid receipt page");
        assert!(
            transaction.counters.peak_transaction_retained_payload_bytes < total_payload,
            "transaction payload must not grow with total payload"
        );
    }
    transaction
        .stage_marker("commit-65")
        .expect("one marker stages");
    let counters = store.commit(transaction).expect("one semantic commit");
    let reopened = store.cold_reopen();
    assert_eq!(tree_digest(&store.rows), tree_digest(&reopened.rows));
    assert_eq!(store.marker, Some("commit-65".to_owned()));
    assert_eq!(reopened.marker, store.marker);
    assert_eq!(store.rows.len(), FILES);
    assert_eq!(store.selectors.len(), 1);
    assert_eq!(counters.semantic_markers, 1);
    assert_eq!(counters.commits, 1);
    assert_eq!(counters.file_content_writes_payload_bytes, 0);
    assert!(counters.peak_file_content_writes_bytes <= Counters::receipt_bound(page_size));
    assert!(counters.peak_prepared_receipt_bytes <= Counters::receipt_bound(page_size));
    assert!(counters.peak_transaction_retained_payload_bytes <= Counters::receipt_bound(page_size));
    assert!(counters.prepared_object_payload_bytes >= total_payload);
    let tree = tree_digest(&store.rows);
    let plugin = plugin_digest(&store.rows);
    (reopened, counters, tree, plugin)
}

fn corrupt(receipt: &mut PreparedReceipt, fault: Fault) {
    match fault {
        Fault::MissingReceipt => {}
        Fault::WrongOwner => receipt.manifest.owner = "other-owner".to_owned(),
        Fault::ManifestSubstitution => receipt.manifest.file_id = "other-file".to_owned(),
        Fault::ChunkSubstitution => receipt.chunks[0].object_id ^= 1,
        Fault::SizeMismatch => receipt.manifest.size += 1,
        Fault::DigestMismatch => receipt.chunks[0].digest ^= 1,
        Fault::CrossView => receipt.chunks[0].view_id += 1,
        Fault::DuplicateReceipt => {}
    }
}

fn run_fault(fault: Fault) {
    let payload = Payload::new(0);
    let mut store = ModelStore::new(Adapter::Memory);
    let mut transaction = store.begin("branch-main", 7, 8);
    let mut receipt = PreparedReceipt::for_payload(&payload);
    if fault == Fault::MissingReceipt {
        transaction.rollback(&mut store);
    } else if fault == Fault::DuplicateReceipt {
        let valid = PreparedReceipt::for_payload(&payload);
        transaction
            .stage_page(std::slice::from_ref(&valid))
            .expect("first receipt");
        transaction
            .stage_page(std::slice::from_ref(&valid))
            .expect_err("duplicate receipt must fail closed");
        transaction.rollback(&mut store);
    } else {
        corrupt(&mut receipt, fault);
        transaction
            .stage_page(std::slice::from_ref(&receipt))
            .expect_err("corrupt receipt must fail closed");
        transaction.rollback(&mut store);
    }
    assert!(store.rows.is_empty());
    assert!(store.selectors.is_empty());
    assert!(store.marker.is_none());
    assert!(store.orphan_object_count > 0 || fault == Fault::MissingReceipt);
    let reclaimed = store.reclaim_orphans();
    assert_eq!(store.orphan_object_count, 0);
    assert_eq!(store.orphan_payload_bytes, 0);
    if fault != Fault::MissingReceipt {
        assert!(reclaimed.0 > 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_count_does_not_change_semantic_digests_or_order() {
        let expected_ids = (0..FILES)
            .map(|index| format!("file-{index:04}"))
            .collect::<Vec<_>>();
        let mut reference = None;
        for page_size in [1, 8, 32, 64] {
            let (store, counters, tree, plugin) = run_success(Adapter::Memory, page_size);
            assert_eq!(store.rows.keys().cloned().collect::<Vec<_>>(), expected_ids);
            assert!(counters.peak_transaction_retained_payload_bytes < PAYLOAD_BYTES * FILES);
            if let Some((expected_tree, expected_plugin)) = reference {
                assert_eq!(tree, expected_tree);
                assert_eq!(plugin, expected_plugin);
            } else {
                reference = Some((tree, plugin));
            }
        }
    }

    #[test]
    fn simulated_memory_rocks_and_slate_cold_reopen_are_identical() {
        let mut digests = None;
        for adapter in [Adapter::Memory, Adapter::RocksDb, Adapter::SlateDb] {
            let (store, counters, tree, plugin) = run_success(adapter, 8);
            assert_eq!(store.adapter, adapter);
            assert_eq!(counters.semantic_markers, 1);
            if let Some(expected) = digests {
                assert_eq!((tree, plugin), expected);
            } else {
                digests = Some((tree, plugin));
            }
        }
    }

    #[test]
    fn all_authenticated_receipt_corruption_fails_closed_and_reclaims_orphans() {
        for fault in [
            Fault::MissingReceipt,
            Fault::WrongOwner,
            Fault::ManifestSubstitution,
            Fault::ChunkSubstitution,
            Fault::SizeMismatch,
            Fault::DigestMismatch,
            Fault::CrossView,
            Fault::DuplicateReceipt,
        ] {
            run_fault(fault);
        }
    }

    #[test]
    fn rollback_has_no_marker_rows_or_selector_and_one_commit_is_atomic() {
        let payload = Payload::new(1);
        let mut store = ModelStore::new(Adapter::Memory);
        let mut transaction = store.begin("branch-main", 7, 1);
        let receipt = PreparedReceipt::for_payload(&payload);
        transaction
            .stage_page(std::slice::from_ref(&receipt))
            .unwrap();
        transaction.stage_marker("commit-rollback").unwrap();
        transaction.rollback(&mut store);
        assert!(store.rows.is_empty());
        assert!(store.selectors.is_empty());
        assert!(store.marker.is_none());
        assert!(store.reclaim_orphans().0 >= PAYLOAD_BYTES);
    }
}
