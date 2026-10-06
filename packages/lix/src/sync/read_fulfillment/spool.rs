//! Private operation payloads. Native authority spools never retain readers.
//! File lifetime follows the owned spool, including cancellation and failure.
use super::{LixError, MAX_INPUT_BYTES, MAX_PAYLOAD_BYTES, invalid};

use std::sync::atomic::{AtomicUsize, Ordering};
static LIVE_SPOOL_BYTES: AtomicUsize = AtomicUsize::new(0);
const MAX_LIVE_SPOOL_BYTES: usize = 1024 * 1024 * 1024;
struct ByteReservation(usize);
impl Drop for ByteReservation {
    fn drop(&mut self) {
        LIVE_SPOOL_BYTES.fetch_sub(self.0, Ordering::AcqRel);
    }
}
impl Drop for PayloadSpool {
    fn drop(&mut self) {
        LIVE_SPOOL_BYTES.fetch_sub(self.len as usize, Ordering::AcqRel);
    }
}

#[derive(Clone, Copy)]
pub(super) struct PayloadRef {
    pub(super) offset: u64,
    pub(super) len: usize,
    pub(super) digest: [u8; 32],
}

#[derive(Default)]
pub(super) struct PayloadSpool {
    #[cfg(not(target_family = "wasm"))]
    file: Option<std::fs::File>,
    #[cfg(target_family = "wasm")]
    bytes: Vec<u8>,
    len: u64,
}

impl PayloadSpool {
    pub(super) fn append(&mut self, bytes: &[u8]) -> Result<PayloadRef, LixError> {
        if bytes.len() > MAX_INPUT_BYTES
            || self.len.saturating_add(bytes.len() as u64) > (2 * MAX_PAYLOAD_BYTES) as u64
        {
            return Err(LixError::new(
                "LIX_NATIVE_RECIPE_WORK_BOUND",
                "operation spool quota exceeded",
            ));
        }
        LIVE_SPOOL_BYTES
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |bytes_in_use| {
                bytes_in_use
                    .checked_add(bytes.len())
                    .filter(|total| *total <= MAX_LIVE_SPOOL_BYTES)
            })
            .map_err(|_| {
                LixError::new(
                    "LIX_NATIVE_RECIPE_WORK_BOUND",
                    "global operation spool byte quota exceeded",
                )
            })?;
        let reservation = ByteReservation(bytes.len());
        let reference = PayloadRef {
            offset: self.len,
            len: bytes.len(),
            digest: *blake3::hash(bytes).as_bytes(),
        };
        #[cfg(not(target_family = "wasm"))]
        {
            use std::io::{Seek, SeekFrom, Write};
            if self.file.is_none() {
                self.file = Some(
                    tempfile::tempfile().map_err(|_| invalid("operation spool creation failed"))?,
                );
            }
            let file = self.file.as_mut().expect("spool initialized");
            file.seek(SeekFrom::Start(self.len))
                .map_err(|_| invalid("operation spool seek failed"))?;
            file.write_all(bytes)
                .map_err(|_| invalid("operation spool write failed"))?;
        }
        #[cfg(target_family = "wasm")]
        {
            // Browser replicas consume authority pages. A browser acting as an
            // authority must decline beyond this explicit in-memory budget.
            if self.len.saturating_add(bytes.len() as u64) > 8 * 1024 * 1024 {
                return Err(LixError::new(
                    "LIX_NATIVE_RECIPE_WORK_BOUND",
                    "browser authority spool requires external scratch",
                ));
            }
            self.bytes.extend_from_slice(bytes);
        }
        self.len += bytes.len() as u64;
        std::mem::forget(reservation);
        Ok(reference)
    }

    pub(super) fn read_range(
        &mut self,
        reference: PayloadRef,
        offset: usize,
        len: usize,
    ) -> Result<Vec<u8>, LixError> {
        if offset.saturating_add(len) > reference.len || len > PAGE_PAYLOAD_BYTES {
            return Err(invalid("invalid spool frame range"));
        }
        #[cfg(not(target_family = "wasm"))]
        {
            use std::io::{Read, Seek, SeekFrom};
            let file = self
                .file
                .as_mut()
                .ok_or_else(|| invalid("operation spool absent"))?;
            file.seek(SeekFrom::Start(reference.offset + offset as u64))
                .map_err(|_| invalid("spool frame seek failed"))?;
            let mut bytes = vec![0; len];
            file.read_exact(&mut bytes)
                .map_err(|_| invalid("spool frame read failed"))?;
            Ok(bytes)
        }
        #[cfg(target_family = "wasm")]
        {
            Ok(self.bytes
                [reference.offset as usize + offset..reference.offset as usize + offset + len]
                .to_vec())
        }
    }

    pub(super) fn read(&mut self, reference: PayloadRef) -> Result<Vec<u8>, LixError> {
        if reference.len > MAX_INPUT_BYTES
            || reference.offset.saturating_add(reference.len as u64) > self.len
        {
            return Err(invalid("operation spool reference exceeds sealed data"));
        }
        #[cfg(not(target_family = "wasm"))]
        let bytes = {
            use std::io::{Read, Seek, SeekFrom};
            let file = self
                .file
                .as_mut()
                .ok_or_else(|| invalid("operation spool is absent"))?;
            file.seek(SeekFrom::Start(reference.offset))
                .map_err(|_| invalid("operation spool seek failed"))?;
            let mut bytes = vec![0; reference.len];
            file.read_exact(&mut bytes)
                .map_err(|_| invalid("operation spool read failed"))?;
            bytes
        };
        #[cfg(target_family = "wasm")]
        let bytes = self.bytes
            [reference.offset as usize..reference.offset as usize + reference.len]
            .to_vec();
        if *blake3::hash(&bytes).as_bytes() != reference.digest {
            return Err(invalid("operation spool payload changed"));
        }
        Ok(bytes)
    }
}

use super::{
    BTreeMap, DiscoveryProfile, PAGE_PAYLOAD_BYTES, ReadContinuation, ReadFulfillmentOutcome,
    ReadFulfillmentRequest, ReadFulfillmentResponse, ReadInput, ReadInputAddress, StorageKey,
    StorageSpace,
};
use std::sync::{Arc, Mutex, OnceLock};

#[derive(Clone)]
pub(super) struct IndexedInput {
    pub(super) address: ReadInputAddress,
    pub(super) payload: PayloadRef,
}

pub(super) struct InputSpool {
    pub(super) payloads: Arc<Mutex<PayloadSpool>>,
    pub(super) inputs: Vec<IndexedInput>,
    coordinates: BTreeMap<(StorageSpace, StorageKey), usize>,
    pub(super) payload_bytes: usize,
    index_bytes: usize,
}
impl InputSpool {
    pub(super) fn new(payloads: Arc<Mutex<PayloadSpool>>) -> Self {
        Self {
            payloads,
            inputs: Vec::new(),
            coordinates: BTreeMap::new(),
            payload_bytes: 0,
            index_bytes: 0,
        }
    }
    pub(super) fn append(&mut self, input: ReadInput) -> Result<(), LixError> {
        input.address.validate(&input.bytes)?;
        let coordinate = input.address.coordinate()?;
        if let Some(&index) = self.coordinates.get(&coordinate) {
            let existing = &self.inputs[index];
            if existing.address != input.address
                || existing.payload.len != input.bytes.len()
                || existing.payload.digest != *blake3::hash(&input.bytes).as_bytes()
            {
                return Err(invalid(
                    "operation spool repeats a coordinate with different input",
                ));
            }
            return Ok(());
        }
        let index_bytes = serde_json::to_vec(&input.address)
            .map_err(|_| invalid("invalid spool address"))?
            .len()
            .saturating_add(coordinate.1.0.len())
            .saturating_add(128);
        if self.inputs.len() >= super::MAX_RECORDS
            || self.payload_bytes.saturating_add(input.bytes.len()) > MAX_PAYLOAD_BYTES
            || self.index_bytes.saturating_add(index_bytes) > 4 * 1024 * 1024
        {
            return Err(LixError::new(
                "LIX_NATIVE_RECIPE_WORK_BOUND",
                "operation spool input or index budget exceeded",
            ));
        }
        let payload = self
            .payloads
            .lock()
            .map_err(|_| invalid("operation spool poisoned"))?
            .append(&input.bytes)?;
        self.coordinates.insert(coordinate, self.inputs.len());
        self.payload_bytes += payload.len;
        self.index_bytes += index_bytes;
        self.inputs.push(IndexedInput {
            address: input.address,
            payload,
        });
        Ok(())
    }
    pub(super) fn read(&self, index: usize) -> Result<ReadInput, LixError> {
        let input = self
            .inputs
            .get(index)
            .ok_or_else(|| invalid("operation spool index is absent"))?;
        let bytes = self
            .payloads
            .lock()
            .map_err(|_| invalid("operation spool poisoned"))?
            .read(input.payload)?;
        Ok(ReadInput {
            address: input.address.clone(),
            bytes,
        })
    }
    pub(super) fn contains(&self, address: &ReadInputAddress) -> Result<bool, LixError> {
        Ok(self
            .coordinates
            .get(&address.coordinate()?)
            .is_some_and(|&index| self.inputs[index].address == *address))
    }
    pub(super) fn digest(&self, request: &ReadFulfillmentRequest) -> Result<String, LixError> {
        let mut digest = blake3::Hasher::new();
        digest.update(request.digest()?.as_bytes());
        for index in 0..self.inputs.len() {
            let input = self.read(index)?;
            let address =
                serde_json::to_vec(&input.address).map_err(|_| invalid("invalid spool address"))?;
            digest.update(&(address.len() as u64).to_be_bytes());
            digest.update(&address);
            digest.update(&(input.bytes.len() as u64).to_be_bytes());
            digest.update(&input.bytes);
        }
        Ok(digest.finalize().to_hex().to_string())
    }
}

static ADMISSIONS: OnceLock<Mutex<BTreeMap<(String, String), usize>>> = OnceLock::new();
pub(super) struct Admission {
    key: (String, String),
}
impl Admission {
    pub(super) fn reserve(repository: &str, account: &str) -> Result<Self, LixError> {
        let key = (repository.to_owned(), account.to_owned());
        let mut active = ADMISSIONS
            .get_or_init(Default::default)
            .lock()
            .map_err(|_| invalid("spool admission poisoned"))?;
        if active.values().sum::<usize>() >= 16 || active.get(&key).copied().unwrap_or(0) >= 4 {
            return Err(LixError::new(
                "LIX_NATIVE_RECIPE_WORK_BOUND",
                "in-flight read operation quota exceeded",
            ));
        }
        *active.entry(key.clone()).or_default() += 1;
        Ok(Self { key })
    }
}
impl Drop for Admission {
    fn drop(&mut self) {
        if let Some(active) = ADMISSIONS.get() {
            if let Ok(mut active) = active.lock() {
                if let Some(count) = active.get_mut(&self.key) {
                    *count -= 1;
                    if *count == 0 {
                        active.remove(&self.key);
                    }
                }
            }
        }
    }
}

struct SealedSpool {
    spool: InputSpool,
    _admission: Admission,
    egress_pages: AtomicUsize,
    egress_bytes: AtomicUsize,
    repository: String,
    account: String,
    lease: String,
    epoch: String,
    request_digest: String,
    closure_digest: String,
    expires_at_ms: u64,
    profile: DiscoveryProfile,
    page_starts: Vec<(usize, usize)>,
}
static SEALED: OnceLock<Mutex<BTreeMap<String, Arc<SealedSpool>>>> = OnceLock::new();
const MAX_SEALED_BYTES: usize = 1024 * 1024 * 1024;
const MAX_ACCOUNT_SPOOLS: usize = 4;

fn registry() -> &'static Mutex<BTreeMap<String, Arc<SealedSpool>>> {
    SEALED.get_or_init(|| {
        #[cfg(not(target_family = "wasm"))]
        std::thread::spawn(|| {
            loop {
                std::thread::sleep(std::time::Duration::from_secs(1));
                if let Some(registry) = SEALED.get() {
                    if let Ok(mut spools) = registry.lock() {
                        let now = crate::telemetry::unix_time_ms();
                        spools.retain(|_, spool| spool.expires_at_ms > now);
                    }
                }
            }
        });
        Mutex::new(BTreeMap::new())
    })
}

fn page(
    sealed: &SealedSpool,
    token: &str,
    start: (usize, usize),
) -> Result<ReadFulfillmentResponse, LixError> {
    let page_index = sealed
        .page_starts
        .iter()
        .position(|&value| value == start)
        .ok_or_else(|| invalid("read continuation does not name a sealed page boundary"))?;
    let next = sealed.page_starts.get(page_index + 1).copied();
    if sealed.egress_pages.fetch_add(1, Ordering::AcqRel) >= 3 * super::MAX_PAGES {
        return Err(LixError::new(
            "LIX_READ_FULFILLMENT_RESTART",
            "read operation replay page budget exceeded",
        ));
    }
    let entry = &sealed.spool.inputs[start.0];
    let payload_bytes = if entry.payload.len > PAGE_PAYLOAD_BYTES {
        (entry.payload.len - start.1).min(PAGE_PAYLOAD_BYTES)
    } else {
        let end = next.map_or(sealed.spool.inputs.len(), |position| position.0);
        sealed.spool.inputs[start.0..end]
            .iter()
            .map(|input| input.payload.len)
            .sum()
    };
    sealed
        .egress_bytes
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |bytes| {
            bytes
                .checked_add(payload_bytes)
                .filter(|total| *total <= 3 * MAX_PAYLOAD_BYTES)
        })
        .map_err(|_| {
            LixError::new(
                "LIX_READ_FULFILLMENT_RESTART",
                "read operation replay byte budget exceeded",
            )
        })?;
    let (inputs, frame) = if entry.payload.len > PAGE_PAYLOAD_BYTES {
        let len = (entry.payload.len - start.1).min(PAGE_PAYLOAD_BYTES);
        let bytes = sealed
            .spool
            .payloads
            .lock()
            .map_err(|_| invalid("operation spool poisoned"))?
            .read_range(entry.payload, start.1, len)?;
        (
            Vec::new(),
            Some(super::ReadInputFrame {
                address: entry.address.clone(),
                total_bytes: entry.payload.len,
                offset: start.1,
                digest: entry.payload.digest,
                bytes,
            }),
        )
    } else {
        let end = next.map_or(sealed.spool.inputs.len(), |position| position.0);
        (
            (start.0..end)
                .map(|index| sealed.spool.read(index))
                .collect::<Result<Vec<_>, _>>()?,
            None,
        )
    };
    Ok(ReadFulfillmentResponse {
        lix_id: sealed.repository.clone(),
        epoch_id: sealed.epoch.clone(),
        request_digest: sealed.request_digest.clone(),
        closure_digest: sealed.closure_digest.clone(),
        inputs,
        frame,
        profile: sealed.profile.clone(),
        outcome: ReadFulfillmentOutcome::Complete,
        continuation: next.map(|(next_input, next_offset)| ReadContinuation {
            next_input,
            next_offset,
            closure_digest: sealed.closure_digest.clone(),
            spool_id: token.to_owned(),
        }),
    })
}

pub(super) fn seal_and_page(
    spool: InputSpool,
    repository: &str,
    account: &str,
    lease: &str,
    lease_expires_at_ms: u64,
    request: &ReadFulfillmentRequest,
    profile: DiscoveryProfile,
    admission: Admission,
) -> Result<ReadFulfillmentResponse, LixError> {
    let mut page_starts = Vec::new();
    let mut bytes = 0usize;
    let mut encoded_bytes = 0usize;
    // Reserve fixed response fields independently from encoded addresses and
    // payloads. Base64 padding is charged per member, rather than per page.
    const RESPONSE_FIELDS_BYTES: usize = 64 * 1024;
    let encoded_limit = super::MAX_RESPONSE_BYTES - RESPONSE_FIELDS_BYTES;
    for (index, input) in spool.inputs.iter().enumerate() {
        let address_bytes = serde_json::to_vec(&input.address)
            .map_err(|_| invalid("invalid page address"))?
            .len();
        let encoded_member = address_bytes
            .saturating_add(input.payload.len.min(PAGE_PAYLOAD_BYTES).div_ceil(3) * 4)
            .saturating_add(1024);
        if encoded_member > encoded_limit {
            return Err(LixError::new(
                "LIX_NATIVE_RECIPE_WORK_BOUND",
                "read member address exceeds encoded page budget",
            ));
        }
        if input.payload.len > PAGE_PAYLOAD_BYTES {
            for offset in (0..input.payload.len).step_by(PAGE_PAYLOAD_BYTES) {
                page_starts.push((index, offset));
            }
            bytes = 0;
            encoded_bytes = 0;
        } else {
            if encoded_bytes == 0
                || bytes.saturating_add(input.payload.len) > PAGE_PAYLOAD_BYTES
                || encoded_bytes.saturating_add(encoded_member) > encoded_limit
            {
                page_starts.push((index, 0));
                bytes = 0;
                encoded_bytes = 0;
            }
            bytes += input.payload.len;
            encoded_bytes += encoded_member;
        }
    }
    if page_starts.len() > super::MAX_PAGES {
        return Err(LixError::new(
            "LIX_NATIVE_RECIPE_WORK_BOUND",
            "read operation encoded page count exceeds budget",
        ));
    }
    if spool.inputs.is_empty() {
        return Err(invalid("read operation has no required member"));
    }
    let now = crate::telemetry::unix_time_ms();
    let sealed = SealedSpool {
        closure_digest: spool.digest(request)?,
        spool,
        _admission: admission,
        egress_pages: AtomicUsize::new(0),
        egress_bytes: AtomicUsize::new(0),
        page_starts,
        repository: repository.into(),
        account: account.into(),
        lease: lease.into(),
        epoch: request.epoch_id.clone(),
        request_digest: request.digest()?,
        expires_at_ms: lease_expires_at_ms.min(now.saturating_add(120_000)),
        profile,
    };
    let token = uuid::Uuid::now_v7().to_string();
    let response = page(&sealed, &token, (0, 0))?;
    if response.continuation.is_some() {
        let mut registry = registry()
            .lock()
            .map_err(|_| invalid("sealed spool registry poisoned"))?;
        registry.retain(|_, spool| spool.expires_at_ms > now);
        let bytes = registry
            .values()
            .map(|spool| {
                spool
                    .spool
                    .payloads
                    .lock()
                    .map(|p| p.len as usize)
                    .unwrap_or(MAX_SEALED_BYTES)
            })
            .sum::<usize>();
        let owned_bytes = sealed
            .spool
            .payloads
            .lock()
            .map_err(|_| invalid("operation spool poisoned"))?
            .len as usize;
        if registry.len() >= 16
            || registry
                .values()
                .filter(|spool| spool.account == account && spool.repository == repository)
                .count()
                >= MAX_ACCOUNT_SPOOLS
            || bytes.saturating_add(owned_bytes) > MAX_SEALED_BYTES
        {
            return Err(LixError::new(
                "LIX_NATIVE_RECIPE_WORK_BOUND",
                "sealed spool admission quota exceeded",
            ));
        }
        registry.insert(token, Arc::new(sealed));
    }
    Ok(response)
}

pub(super) fn continuation_page(
    repository: &str,
    account: &str,
    lease: &str,
    request: &ReadFulfillmentRequest,
) -> Result<ReadFulfillmentResponse, LixError> {
    let cursor = request
        .continuation
        .as_ref()
        .ok_or_else(|| invalid("read continuation absent"))?;
    let now = crate::telemetry::unix_time_ms();
    let mut registry = registry()
        .lock()
        .map_err(|_| invalid("sealed spool registry poisoned"))?;
    registry.retain(|_, spool| spool.expires_at_ms > now);
    let sealed = registry.get(&cursor.spool_id).cloned().ok_or_else(|| {
        LixError::new(
            "LIX_READ_FULFILLMENT_RESTART",
            "sealed read operation expired or belongs to another authority process",
        )
    })?;
    drop(registry);
    if sealed.repository != repository
        || sealed.account != account
        || sealed.lease != lease
        || sealed.epoch != request.epoch_id
        || sealed.request_digest != request.digest()?
        || sealed.closure_digest != cursor.closure_digest
    {
        return Err(invalid("read continuation admission binding differs"));
    }
    page(
        &sealed,
        &cursor.spool_id,
        (cursor.next_input, cursor.next_offset),
    )
}

pub(super) fn release(
    repository: &str,
    account: &str,
    lease: &str,
    request: &ReadFulfillmentRequest,
) -> Result<(), LixError> {
    let cursor = request
        .continuation
        .as_ref()
        .ok_or_else(|| invalid("release lacks sealed operation"))?;
    let mut registry = registry()
        .lock()
        .map_err(|_| invalid("sealed spool registry poisoned"))?;
    if let Some(sealed) = registry.get(&cursor.spool_id) {
        if sealed.repository != repository
            || sealed.account != account
            || sealed.lease != lease
            || sealed.epoch != request.epoch_id
            || sealed.request_digest != request.digest()?
            || sealed.closure_digest != cursor.closure_digest
        {
            return Err(invalid("release admission binding differs"));
        }
        registry.remove(&cursor.spool_id);
    }
    Ok(())
}

#[cfg(all(test, not(target_family = "wasm")))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sealed_128_mib_closure_uses_32_pages_and_replays_without_discovery() {
        let authority = crate::open_lix().await.unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let repository = descriptor.lix_id.clone();
        let mut request = ReadFulfillmentRequest {
            release: false,
            epoch_id: uuid::Uuid::now_v7().to_string(),
            descriptor,
            interests: vec![crate::hot_state::LogicalReadInterest::FilesystemMetadata {
                directory: false,
                branch_ids: Vec::new(),
                file_ids: None,
                directory_ids: None,
                root_directory: false,
                path_predicate: crate::hot_state::FilePathInterest::All,
            }],
            required: Vec::new(),
            continuation: None,
        };
        request.interests = vec![crate::hot_state::LogicalReadInterest::FilesystemMetadata {
            directory: false,
            branch_ids: vec![request.descriptor.selected_branch.branch_id.clone()],
            file_ids: None,
            directory_ids: None,
            root_directory: false,
            path_predicate: crate::hot_state::FilePathInterest::All,
        }];
        let payloads = Arc::new(Mutex::new(PayloadSpool::default()));
        let mut spool = InputSpool::new(payloads.clone());
        for index in 0..128 {
            let mut bytes = vec![43u8; 1024 * 1024];
            bytes[0] = index;
            let address = ReadInputAddress::BlobChunk(*blake3::hash(&bytes).as_bytes());
            if index == 0 {
                request.required.push(address.clone());
            }
            spool.append(ReadInput { address, bytes }).unwrap();
        }
        assert_eq!(spool.payload_bytes, 128 * 1024 * 1024);
        assert_eq!(payloads.lock().unwrap().len, 128 * 1024 * 1024);
        assert!(spool.index_bytes < 128 * 512);
        super::super::validate_spooled_complete(&request, &spool).unwrap();
        let account = uuid::Uuid::now_v7().to_string();
        let lease = uuid::Uuid::now_v7().to_string();
        let mut page = seal_and_page(
            spool,
            &repository,
            &account,
            &lease,
            u64::MAX,
            &request,
            DiscoveryProfile::default(),
            Admission::reserve(&repository, &account).unwrap(),
        )
        .unwrap();
        let closure_digest = page.closure_digest.clone();
        let token = page.continuation.as_ref().unwrap().spool_id.clone();
        let mut digest = blake3::Hasher::new();
        digest.update(request.digest().unwrap().as_bytes());
        let mut pages = 0;
        let mut members = 0;
        loop {
            pages += 1;
            super::super::validate_response(&request, &page).unwrap();
            assert_eq!(page.inputs.len(), 4);
            assert_eq!(
                page.inputs
                    .iter()
                    .map(|input| input.bytes.len())
                    .sum::<usize>(),
                4 * 1024 * 1024
            );
            for input in &page.inputs {
                let address = serde_json::to_vec(&input.address).unwrap();
                digest.update(&(address.len() as u64).to_be_bytes());
                digest.update(&address);
                digest.update(&(input.bytes.len() as u64).to_be_bytes());
                digest.update(&input.bytes);
                members += 1;
            }
            let next = page.continuation.take();
            drop(page);
            let Some(cursor) = next else {
                break;
            };
            request.continuation = Some(cursor);
            let replay = continuation_page(&repository, &account, &lease, &request).unwrap();
            page = continuation_page(&repository, &account, &lease, &request).unwrap();
            assert_eq!(
                serde_json::to_vec(&replay).unwrap(),
                serde_json::to_vec(&page).unwrap()
            );
            drop(replay);
            assert!(continuation_page(&repository, "other-account", &lease, &request).is_err());
            assert!(continuation_page(&repository, &account, "other-lease", &request).is_err());
            assert!(continuation_page("other-repository", &account, &lease, &request).is_err());
        }
        assert_eq!(pages, 32);
        assert_eq!(members, 128);
        assert_eq!(digest.finalize().to_hex().to_string(), closure_digest);
        SEALED.get().unwrap().lock().unwrap().remove(&token);
        let restart = continuation_page(&repository, &account, &lease, &request).unwrap_err();
        assert_eq!(restart.code, "LIX_READ_FULFILLMENT_RESTART");
        authority.close().await.unwrap();
    }

    #[test]
    fn spool_rejects_duplicate_coordinate_with_different_bytes() {
        let payloads = Arc::new(Mutex::new(PayloadSpool::default()));
        let mut spool = InputSpool::new(payloads);
        let bytes = b"immutable input".to_vec();
        let input = ReadInput {
            address: ReadInputAddress::BlobChunk(*blake3::hash(&bytes).as_bytes()),
            bytes,
        };
        spool.append(input.clone()).unwrap();
        spool.append(input.clone()).unwrap();
        assert_eq!(spool.inputs.len(), 1);
        let mut corrupt = input;
        corrupt.bytes[0] ^= 1;
        assert!(spool.append(corrupt).is_err());
        assert_eq!(spool.inputs.len(), 1);
    }
}
