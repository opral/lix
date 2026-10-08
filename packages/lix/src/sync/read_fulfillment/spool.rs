//! Private operation payloads. Native authority spools never retain readers.
//! File lifetime follows the owned spool, including cancellation and failure.
use super::{LixError, MAX_INPUT_BYTES, MAX_PAYLOAD_BYTES, invalid};

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
static LIVE_SPOOL_BYTES: AtomicUsize = AtomicUsize::new(0);
const MAX_LIVE_SPOOL_BYTES: usize = 1024 * 1024 * 1024;
#[derive(Clone, Default)]
enum LiveSpoolCounter {
    #[default]
    Global,
    #[cfg(test)]
    Local(Arc<AtomicUsize>),
}
impl LiveSpoolCounter {
    fn atomic(&self) -> &AtomicUsize {
        match self {
            Self::Global => &LIVE_SPOOL_BYTES,
            #[cfg(test)]
            Self::Local(counter) => counter,
        }
    }

    fn same_budget(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Global, Self::Global) => true,
            #[cfg(test)]
            (Self::Local(left), Self::Local(right)) => Arc::ptr_eq(left, right),
            #[cfg(test)]
            _ => false,
        }
    }
}

struct ByteReservation {
    counter: LiveSpoolCounter,
    bytes: usize,
}
impl Drop for ByteReservation {
    fn drop(&mut self) {
        self.counter
            .atomic()
            .fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
impl Drop for PayloadSpool {
    fn drop(&mut self) {
        self.counter
            .atomic()
            .fetch_sub(self.len as usize, Ordering::AcqRel);
    }
}

#[derive(Clone, Copy)]
pub(super) struct PayloadRef {
    pub(super) offset: u64,
    pub(super) len: usize,
    pub(super) digest: [u8; 32],
}

pub(super) struct PayloadSpool {
    #[cfg(not(target_family = "wasm"))]
    file: Option<std::fs::File>,
    #[cfg(target_family = "wasm")]
    bytes: Vec<u8>,
    len: u64,
    pressure_key: Option<OperationKey>,
    counter: LiveSpoolCounter,
    live_limit: usize,
}

impl Default for PayloadSpool {
    fn default() -> Self {
        Self {
            #[cfg(not(target_family = "wasm"))]
            file: None,
            #[cfg(target_family = "wasm")]
            bytes: Vec::new(),
            len: 0,
            pressure_key: None,
            counter: LiveSpoolCounter::default(),
            live_limit: MAX_LIVE_SPOOL_BYTES,
        }
    }
}

impl PayloadSpool {
    fn bind_operation(&mut self, key: OperationKey) -> Result<(), LixError> {
        if self
            .pressure_key
            .as_ref()
            .is_some_and(|existing| *existing != key)
        {
            return Err(invalid("operation spool is already bound to another read"));
        }
        self.pressure_key = Some(key);
        Ok(())
    }

    pub(super) fn append(&mut self, bytes: &[u8]) -> Result<PayloadRef, LixError> {
        if bytes.len() > MAX_INPUT_BYTES
            || self.len.saturating_add(bytes.len() as u64) > (2 * MAX_PAYLOAD_BYTES) as u64
        {
            return Err(LixError::new(
                "LIX_NATIVE_RECIPE_WORK_BOUND",
                "operation spool quota exceeded",
            ));
        }
        let reservation = reserve_live_spool_bytes(
            bytes.len(),
            self.pressure_key.as_ref(),
            &self.counter,
            self.live_limit,
        )?;
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
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::watch;

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
        let ReadInput { address, bytes } = input;
        self.append_borrowed(address, &bytes)
    }
    /// Appends a validated input from a borrowed provider page without first
    /// copying its payload into a ReadInput allocation.
    pub(super) fn append_borrowed(
        &mut self,
        address: ReadInputAddress,
        bytes: &[u8],
    ) -> Result<(), LixError> {
        address.validate(bytes)?;
        let coordinate = address.coordinate()?;
        if let Some(&index) = self.coordinates.get(&coordinate) {
            let existing = &self.inputs[index];
            if existing.address != address
                || existing.payload.len != bytes.len()
                || existing.payload.digest != *blake3::hash(bytes).as_bytes()
            {
                return Err(invalid(
                    "operation spool repeats a coordinate with different input",
                ));
            }
            return Ok(());
        }
        let index_bytes = serde_json::to_vec(&address)
            .map_err(|_| invalid("invalid spool address"))?
            .len()
            .saturating_add(coordinate.1.0.len())
            .saturating_add(128);
        if self.inputs.len() >= super::MAX_RECORDS
            || self.payload_bytes.saturating_add(bytes.len()) > MAX_PAYLOAD_BYTES
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
            .append(bytes)?;
        self.coordinates.insert(coordinate, self.inputs.len());
        self.payload_bytes += payload.len;
        self.index_bytes += index_bytes;
        self.inputs.push(IndexedInput {
            address,
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
    fn usage(repository: &str, account: &str) -> Result<(usize, usize), LixError> {
        let active = ADMISSIONS
            .get_or_init(Default::default)
            .lock()
            .map_err(|_| invalid("spool admission poisoned"))?;
        let scoped = active
            .get(&(repository.to_owned(), account.to_owned()))
            .copied()
            .unwrap_or_default();
        Ok((active.values().sum(), scoped))
    }

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
    resident_bytes: usize,
    live_counter: LiveSpoolCounter,
    _admission: Option<Admission>,
    egress_pages: AtomicUsize,
    egress_bytes: AtomicUsize,
    token: String,
    repository: String,
    account: String,
    lease: String,
    epoch: String,
    request_digest: String,
    closure_digest: String,
    operation_expires_at_ms: u64,
    expires_at_ms: u64,
    profile: DiscoveryProfile,
    page_starts: Vec<(usize, usize)>,
}
const MAX_SEALED_BYTES: usize = 1024 * 1024 * 1024;
const MAX_ACCOUNT_SPOOLS: usize = 4;
const MAX_CANCELLED_OPERATIONS: usize = 256;
const MAX_ACCOUNT_CANCELLED_OPERATIONS: usize = 64;
const MAX_OPERATION_RECORDS: usize = 16 + MAX_CANCELLED_OPERATIONS;

type OperationKey = (String, String, String, String);

fn hash_parts(parts: &[&str]) -> [u8; 32] {
    let mut digest = blake3::Hasher::new();
    for part in parts {
        digest.update(&(part.len() as u64).to_be_bytes());
        digest.update(part.as_bytes());
    }
    *digest.finalize().as_bytes()
}

fn scope_fingerprint(repository: &str, account: &str, lease: &str) -> [u8; 16] {
    let full = hash_parts(&[repository, account, lease]);
    let mut scope = [0; 16];
    scope.copy_from_slice(&full[..16]);
    scope
}

fn operation_fingerprint(key: &OperationKey) -> [u8; 32] {
    hash_parts(&[&key.0, &key.1, &key.2, &key.3])
}

fn request_digest_fingerprint(request_digest: &str) -> [u8; 32] {
    *blake3::hash(request_digest.as_bytes()).as_bytes()
}

fn operation_state_busy() -> LixError {
    LixError::new(
        "LIX_NATIVE_RECIPE_WORK_BOUND",
        "read operation state is busy; retry the request",
    )
}

fn block_scope(registry: &mut OperationRegistry, scope: [u8; 16], expires_at_ms: u64) {
    if let Some(previous) = registry.blocked_scopes.get(&scope).copied() {
        if previous >= expires_at_ms {
            return;
        }
        registry.blocked_scope_expiry.remove(&(previous, scope));
    } else if registry.blocked_scopes.len() >= MAX_BLOCKED_SCOPES {
        // This is a last-resort fail-closed gate after the separately bounded
        // scope tombstone index itself is exhausted. Existing operation replay
        // is checked before this global gate.
        registry.reject_new_until_ms = registry.reject_new_until_ms.max(expires_at_ms);
        return;
    }
    registry.blocked_scopes.insert(scope, expires_at_ms);
    registry.blocked_scope_expiry.insert((expires_at_ms, scope));
}

fn record_completed(
    registry: &mut OperationRegistry,
    key: &OperationKey,
    request_digest: &str,
    operation_expires_at_ms: u64,
) {
    let scope = scope_fingerprint(&key.0, &key.1, &key.2);
    let fingerprint = operation_fingerprint(key);
    let scope_count = registry
        .completed_scope_counts
        .get(&scope)
        .copied()
        .unwrap_or_default();
    // Every caller retires a Sealed operation that reserved its completed
    // identity before discovery began. The registry mutex serializes that
    // reservation conversion, so do not rescan and lock unrelated operations
    // here; payload-pressure callers may hold a payload mutex while retiring.
    if registry
        .completed
        .len()
        .saturating_add(1)
        .saturating_mul(COMPLETED_FINGERPRINT_BYTES)
        > MAX_COMPLETED_FINGERPRINT_BYTES
        || registry.completed.len() >= MAX_COMPLETED_FINGERPRINTS
        || scope_count >= MAX_COMPLETED_FINGERPRINTS_PER_SCOPE
    {
        block_scope(registry, scope, operation_expires_at_ms);
        return;
    }
    let retired = CompletedFingerprint {
        scope,
        request_digest: request_digest_fingerprint(request_digest),
        expires_at_ms: operation_expires_at_ms,
    };
    if registry.completed.insert(fingerprint, retired).is_none() {
        registry
            .completed_expiry
            .insert((operation_expires_at_ms, fingerprint));
        *registry.completed_scope_counts.entry(scope).or_default() += 1;
    }
}

/// Pending and sealed operations each reserve one compact retirement marker.
/// The registry is bounded at 272 operation records, so counting the active
/// reservations here keeps the marker ledger's admission checks bounded without
/// scanning its 65,536-entry expiry index.
fn completion_reservations(
    registry: &OperationRegistry,
    scope: Option<[u8; 16]>,
) -> Result<usize, LixError> {
    let mut count = 0usize;
    for (key, operation) in &registry.operations {
        if scope.is_some_and(|scope| scope_fingerprint(&key.0, &key.1, &key.2) != scope) {
            continue;
        }
        match operation.state.try_lock() {
            Ok(state) => {
                if matches!(
                    *state,
                    OperationState::Pending(_) | OperationState::Sealed(_)
                ) {
                    count += 1;
                }
            }
            // A busy state may still own a completion reservation. Count it
            // conservatively instead of blocking while holding the registry.
            Err(std::sync::TryLockError::WouldBlock) => count += 1,
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(invalid("read operation state poisoned"));
            }
        }
    }
    Ok(count)
}

fn has_retirement_capacity(
    registry: &OperationRegistry,
    scope: [u8; 16],
    global_limit: usize,
    scope_limit: usize,
) -> Result<bool, LixError> {
    let global_reservations = completion_reservations(registry, None)?;
    let scoped_reservations = completion_reservations(registry, Some(scope))?;
    let scoped_completed = registry
        .completed_scope_counts
        .get(&scope)
        .copied()
        .unwrap_or_default();
    Ok(
        registry.completed.len().saturating_add(global_reservations) < global_limit
            && scoped_completed.saturating_add(scoped_reservations) < scope_limit,
    )
}

enum OperationState {
    Pending(Option<Admission>),
    Sealed(Arc<SealedSpool>),
    Cancelled(Option<Admission>),
    Complete,
}

struct Operation {
    request_digest: String,
    epoch: String,
    operation_expires_at_ms: u64,
    expires_at_ms: AtomicU64,
    state: Mutex<OperationState>,
    changed: watch::Sender<()>,
}

#[derive(Clone, Copy)]
struct CompletedFingerprint {
    scope: [u8; 16],
    request_digest: [u8; 32],
    expires_at_ms: u64,
}

// The compact completed-operation ledger holds at most 65,536 entries. Each
// entry stores a 32-byte operation fingerprint and reserves an 80-byte value
// allowance. The value contains a 16-byte scope fingerprint, 32-byte request
// digest, and 8-byte fixed expiry; the allowance is padded before map and
// expiry-index overhead. Each Pending or Sealed operation reserves one marker
// before work starts, so every retired spool has a stable restart identity.
// Entries expire at the client-captured server lease expiry; lease renewal
// cannot extend an old operation.
const MAX_COMPLETED_FINGERPRINTS: usize = 65_536;
const MAX_COMPLETED_FINGERPRINTS_PER_SCOPE: usize = 4_096;
const COMPLETED_FINGERPRINT_BYTES: usize = 32 + 80;
const MAX_COMPLETED_FINGERPRINT_BYTES: usize =
    MAX_COMPLETED_FINGERPRINTS * COMPLETED_FINGERPRINT_BYTES;
const MAX_BLOCKED_SCOPES: usize = 65_536;

#[derive(Default)]
struct OperationRegistry {
    operations: BTreeMap<OperationKey, Arc<Operation>>,
    completed: HashMap<[u8; 32], CompletedFingerprint>,
    completed_expiry: BTreeSet<(u64, [u8; 32])>,
    completed_scope_counts: HashMap<[u8; 16], usize>,
    blocked_scopes: HashMap<[u8; 16], u64>,
    blocked_scope_expiry: BTreeSet<(u64, [u8; 16])>,
    reject_new_until_ms: u64,
}

static OPERATIONS: OnceLock<Mutex<OperationRegistry>> = OnceLock::new();

fn operation_key(
    repository: &str,
    account: &str,
    lease: &str,
    request: &ReadFulfillmentRequest,
) -> OperationKey {
    (
        repository.to_owned(),
        account.to_owned(),
        lease.to_owned(),
        request.operation_id.clone(),
    )
}

fn operation_registry() -> &'static Mutex<OperationRegistry> {
    OPERATIONS.get_or_init(|| {
        #[cfg(not(target_family = "wasm"))]
        std::thread::spawn(|| {
            loop {
                std::thread::sleep(std::time::Duration::from_secs(1));
                if let Some(registry) = OPERATIONS.get()
                    && let Ok(mut registry) = registry.lock()
                {
                    sweep_expired(&mut registry, crate::telemetry::unix_time_ms());
                }
            }
        });
        Mutex::new(OperationRegistry {
            operations: BTreeMap::new(),
            completed: HashMap::new(),
            completed_expiry: BTreeSet::new(),
            completed_scope_counts: HashMap::new(),
            blocked_scopes: HashMap::new(),
            blocked_scope_expiry: BTreeSet::new(),
            reject_new_until_ms: 0,
        })
    })
}

fn sweep_expired(registry: &mut OperationRegistry, now_ms: u64) {
    let keys = registry.operations.keys().cloned().collect::<Vec<_>>();
    for key in keys {
        let Some(operation) = registry.operations.get(&key).cloned() else {
            continue;
        };
        if operation.expires_at_ms.load(Ordering::Acquire) > now_ms {
            continue;
        }
        let mut state = match operation.state.try_lock() {
            Ok(state) => state,
            // Expiry is opportunistic. A request which currently owns this
            // state can finish or publish its cancellation; a later sweep
            // will reap it without blocking every registry operation here.
            Err(std::sync::TryLockError::WouldBlock) => continue,
            Err(std::sync::TryLockError::Poisoned(_)) => {
                registry.operations.remove(&key);
                continue;
            }
        };
        match std::mem::replace(&mut *state, OperationState::Complete) {
            OperationState::Pending(admission) => {
                // Keep work admission until the owner actually stops, but
                // prevent it from publishing after its work deadline.
                *state = OperationState::Cancelled(admission);
                operation
                    .expires_at_ms
                    .store(operation.operation_expires_at_ms, Ordering::Release);
                operation.changed.send_replace(());
            }
            OperationState::Cancelled(Some(admission)) => {
                *state = OperationState::Cancelled(Some(admission));
            }
            OperationState::Sealed(_) => {
                registry.operations.remove(&key);
                record_completed(
                    registry,
                    &key,
                    &operation.request_digest,
                    operation.operation_expires_at_ms,
                );
                operation.changed.send_replace(());
            }
            OperationState::Cancelled(None) | OperationState::Complete => {
                registry.operations.remove(&key);
            }
        }
    }
    while let Some(&(expiry, fingerprint)) = registry.completed_expiry.first() {
        if expiry > now_ms {
            break;
        }
        registry.completed_expiry.pop_first();
        if let Some(retired) = registry.completed.remove(&fingerprint) {
            if let Some(count) = registry.completed_scope_counts.get_mut(&retired.scope) {
                *count -= 1;
                if *count == 0 {
                    registry.completed_scope_counts.remove(&retired.scope);
                }
            }
        }
    }
    while let Some(&(expiry, scope)) = registry.blocked_scope_expiry.first() {
        if expiry > now_ms {
            break;
        }
        registry.blocked_scope_expiry.pop_first();
        if registry
            .blocked_scopes
            .get(&scope)
            .is_some_and(|current| *current <= now_ms)
        {
            registry.blocked_scopes.remove(&scope);
        }
    }
    if registry.reject_new_until_ms <= now_ms {
        registry.reject_new_until_ms = 0;
    }
}

const MAX_PRESSURE_RETIRE_ATTEMPTS: usize = 16;

#[derive(Clone, Copy, PartialEq, Eq)]
enum EvictionNeed {
    Slot,
    SealedBytes,
    LiveBytes,
}

#[derive(Clone)]
struct SealedCandidate {
    key: OperationKey,
    admission_count: usize,
    tenant_spools: usize,
    tenant_bytes: usize,
    egress_bytes: usize,
    resident_bytes: usize,
    operation_expires_at_ms: u64,
}

/// Retire one inactive sealed spool to make room for a bounded operation.
/// The registry lock is held by the caller, preserving registry -> operation
/// lock order. Candidate payload mutexes are never acquired here: append may
/// call this while holding its own shared payload mutex.
fn retire_one_sealed_for_pressure(
    registry: &mut OperationRegistry,
    protected_key: Option<&OperationKey>,
    tenant_only: Option<(&str, &str)>,
    need: EvictionNeed,
    live_counter: Option<&LiveSpoolCounter>,
) -> Result<bool, LixError> {
    let active_admissions = ADMISSIONS
        .get_or_init(Default::default)
        .lock()
        .map_err(|_| invalid("spool admission poisoned"))?
        .clone();

    let mut tenant_usage = BTreeMap::<(String, String), (usize, usize)>::new();
    let mut candidates = Vec::new();
    for (key, operation) in &registry.operations {
        if protected_key.is_some_and(|protected| protected == key) {
            continue;
        }
        if Arc::strong_count(operation) != 1 {
            continue;
        }
        let state = match operation.state.try_lock() {
            Ok(state) => state,
            Err(std::sync::TryLockError::WouldBlock) => continue,
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(invalid("read operation state poisoned"));
            }
        };
        let OperationState::Sealed(sealed) = &*state else {
            continue;
        };
        let tenant = (key.0.clone(), key.1.clone());
        let usage = tenant_usage.entry(tenant).or_default();
        usage.0 = usage.0.saturating_add(1);
        usage.1 = usage.1.saturating_add(sealed.resident_bytes);

        if tenant_only.is_some_and(|(repository, account)| key.0 != repository || key.1 != account)
            || Arc::strong_count(operation) != 1
            || Arc::strong_count(sealed) != 1
            || sealed._admission.is_none()
            || (need != EvictionNeed::Slot && sealed.resident_bytes == 0)
            || (need == EvictionNeed::LiveBytes && Arc::strong_count(&sealed.spool.payloads) != 1)
            || (need == EvictionNeed::LiveBytes
                && !live_counter.is_some_and(|counter| sealed.live_counter.same_budget(counter)))
        {
            continue;
        }
        candidates.push(SealedCandidate {
            key: key.clone(),
            admission_count: active_admissions
                .get(&(key.0.clone(), key.1.clone()))
                .copied()
                .unwrap_or_default(),
            tenant_spools: 0,
            tenant_bytes: 0,
            egress_bytes: sealed.egress_bytes.load(Ordering::Acquire),
            resident_bytes: sealed.resident_bytes,
            operation_expires_at_ms: operation.operation_expires_at_ms,
        });
    }
    for candidate in &mut candidates {
        if let Some((spools, bytes)) =
            tenant_usage.get(&(candidate.key.0.clone(), candidate.key.1.clone()))
        {
            candidate.tenant_spools = *spools;
            candidate.tenant_bytes = *bytes;
        }
    }
    // Global pressure is shared fairly: retire from the tenant with the
    // largest active reservation first, then prefer its larger sealed cache,
    // least-served egress, oldest fixed expiry, and stable key order.
    candidates.sort_by(|left, right| {
        right
            .admission_count
            .cmp(&left.admission_count)
            .then_with(|| right.tenant_spools.cmp(&left.tenant_spools))
            .then_with(|| right.tenant_bytes.cmp(&left.tenant_bytes))
            .then_with(|| left.egress_bytes.cmp(&right.egress_bytes))
            .then_with(|| {
                left.operation_expires_at_ms
                    .cmp(&right.operation_expires_at_ms)
            })
            .then_with(|| left.key.cmp(&right.key))
    });

    for candidate in candidates.into_iter().take(MAX_PRESSURE_RETIRE_ATTEMPTS) {
        let Some(operation_in_registry) = registry.operations.get(&candidate.key) else {
            continue;
        };
        // The registry owns the only strong reference for an inactive spool.
        // Clone only after that check, then recheck the expected map+local
        // references before changing state.
        if Arc::strong_count(operation_in_registry) != 1 {
            continue;
        }
        let operation = operation_in_registry.clone();
        if Arc::strong_count(&operation) != 2 {
            drop(operation);
            continue;
        }
        let mut state = match operation.state.try_lock() {
            Ok(state) => state,
            Err(std::sync::TryLockError::WouldBlock) => continue,
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(invalid("read operation state poisoned"));
            }
        };
        let OperationState::Sealed(sealed) = &*state else {
            continue;
        };
        if Arc::strong_count(sealed) != 1
            || sealed._admission.is_none()
            || sealed.resident_bytes != candidate.resident_bytes
            || (need != EvictionNeed::Slot && sealed.resident_bytes == 0)
            || (need == EvictionNeed::LiveBytes && Arc::strong_count(&sealed.spool.payloads) != 1)
            || (need == EvictionNeed::LiveBytes
                && !live_counter.is_some_and(|counter| sealed.live_counter.same_budget(counter)))
        {
            continue;
        }
        *state = OperationState::Complete;
        drop(state);
        registry.operations.remove(&candidate.key);
        record_completed(
            registry,
            &candidate.key,
            &operation.request_digest,
            operation.operation_expires_at_ms,
        );
        operation.changed.send_replace(());
        return Ok(true);
    }
    Ok(false)
}

fn reclaimable_live_bytes(
    registry: &OperationRegistry,
    protected_key: Option<&OperationKey>,
    live_counter: &LiveSpoolCounter,
) -> Result<usize, LixError> {
    let mut reclaimable = 0usize;
    for (key, operation) in &registry.operations {
        if protected_key.is_some_and(|protected| protected == key)
            || Arc::strong_count(operation) != 1
        {
            continue;
        }
        let state = match operation.state.try_lock() {
            Ok(state) => state,
            Err(std::sync::TryLockError::WouldBlock) => continue,
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(invalid("read operation state poisoned"));
            }
        };
        let OperationState::Sealed(sealed) = &*state else {
            continue;
        };
        if Arc::strong_count(sealed) == 1
            && sealed._admission.is_some()
            && sealed.resident_bytes > 0
            && Arc::strong_count(&sealed.spool.payloads) == 1
            && sealed.live_counter.same_budget(live_counter)
        {
            reclaimable = reclaimable.saturating_add(sealed.resident_bytes);
        }
    }
    Ok(reclaimable)
}

fn reserve_live_spool_bytes(
    bytes: usize,
    current_key: Option<&OperationKey>,
    counter: &LiveSpoolCounter,
    limit: usize,
) -> Result<ByteReservation, LixError> {
    let quota_error = || {
        LixError::new(
            "LIX_NATIVE_RECIPE_WORK_BOUND",
            "global operation spool byte quota exceeded",
        )
    };
    for attempt in 0..=MAX_PRESSURE_RETIRE_ATTEMPTS {
        if counter
            .atomic()
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |bytes_in_use| {
                bytes_in_use
                    .checked_add(bytes)
                    .filter(|total| *total <= limit)
            })
            .is_ok()
        {
            return Ok(ByteReservation {
                counter: counter.clone(),
                bytes,
            });
        }
        if attempt == MAX_PRESSURE_RETIRE_ATTEMPTS {
            break;
        }
        let Some(key) = current_key else {
            break;
        };
        let mut registry = operation_registry()
            .lock()
            .map_err(|_| invalid("read operation registry poisoned"))?;
        let needed = counter
            .atomic()
            .load(Ordering::Acquire)
            .saturating_add(bytes)
            .saturating_sub(limit);
        if needed == 0 {
            // A concurrent payload drop freed the required space after our
            // failed reservation CAS. Retry the real atomic reservation.
            continue;
        }
        if reclaimable_live_bytes(&registry, Some(key), counter)? < needed {
            // No sealed spool can reclaim enough under this snapshot. Drop the
            // registry and do one final CAS so a concurrent drop between the
            // snapshot and this decision is not turned into a false quota
            // failure, without repeating a full registry scan.
            drop(registry);
            if counter
                .atomic()
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |bytes_in_use| {
                    bytes_in_use
                        .checked_add(bytes)
                        .filter(|total| *total <= limit)
                })
                .is_ok()
            {
                return Ok(ByteReservation {
                    counter: counter.clone(),
                    bytes,
                });
            }
            break;
        }
        if !retire_one_sealed_for_pressure(
            &mut registry,
            Some(key),
            None,
            EvictionNeed::LiveBytes,
            Some(counter),
        )? {
            break;
        }
    }
    Err(quota_error())
}

fn reserve_admission_with_pressure(
    registry: &mut OperationRegistry,
    repository: &str,
    account: &str,
) -> Result<Admission, LixError> {
    for attempt in 0..=MAX_PRESSURE_RETIRE_ATTEMPTS {
        match Admission::reserve(repository, account) {
            Ok(admission) => return Ok(admission),
            Err(error) if error.code != "LIX_NATIVE_RECIPE_WORK_BOUND" => return Err(error),
            Err(error) => {
                if attempt == MAX_PRESSURE_RETIRE_ATTEMPTS {
                    return Err(error);
                }
                let (global_count, scoped_count) = Admission::usage(repository, account)?;
                let tenant_only =
                    (scoped_count >= MAX_ACCOUNT_SPOOLS).then_some((repository, account));
                if global_count < 16 && scoped_count < MAX_ACCOUNT_SPOOLS {
                    return Err(error);
                }
                if !retire_one_sealed_for_pressure(
                    registry,
                    None,
                    tenant_only,
                    EvictionNeed::Slot,
                    None,
                )? {
                    return Err(error);
                }
            }
        }
    }
    Err(LixError::new(
        "LIX_NATIVE_RECIPE_WORK_BOUND",
        "in-flight read operation quota exceeded",
    ))
}

fn restart_error() -> LixError {
    LixError::new(
        "LIX_READ_FULFILLMENT_RESTART",
        "read operation was cancelled, expired, or is no longer available",
    )
}

pub(super) enum BeginOperation {
    Owner(PendingOperation),
    Replay(ReadFulfillmentResponse),
}

pub(super) struct PendingOperation {
    key: OperationKey,
    operation: Arc<Operation>,
    finished: bool,
}

impl PendingOperation {
    pub(super) fn bind_payload_spool(
        &self,
        payloads: &Arc<Mutex<PayloadSpool>>,
    ) -> Result<(), LixError> {
        payloads
            .lock()
            .map_err(|_| invalid("operation spool poisoned"))?
            .bind_operation(self.key.clone())
    }
}

impl Drop for PendingOperation {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let Ok(mut registry) = operation_registry().lock() else {
            return;
        };
        if !registry
            .operations
            .get(&self.key)
            .is_some_and(|operation| Arc::ptr_eq(operation, &self.operation))
        {
            return;
        }
        let Ok(mut state) = self.operation.state.lock() else {
            return;
        };
        match std::mem::replace(&mut *state, OperationState::Complete) {
            OperationState::Pending(admission) => {
                drop(admission);
                registry.operations.remove(&self.key);
            }
            OperationState::Cancelled(admission) => {
                drop(admission);
                let now = crate::telemetry::unix_time_ms();
                let scope = scope_fingerprint(&self.key.0, &self.key.1, &self.key.2);
                if registry.blocked_scopes.contains_key(&scope)
                    || self.operation.operation_expires_at_ms <= now
                {
                    registry.operations.remove(&self.key);
                } else {
                    *state = OperationState::Cancelled(None);
                    self.operation
                        .expires_at_ms
                        .store(self.operation.operation_expires_at_ms, Ordering::Release);
                }
            }
            other => *state = other,
        }
        self.operation.changed.send_replace(());
    }
}

pub(super) async fn begin(
    repository: &str,
    account: &str,
    lease: &str,
    lease_expires_at_ms: u64,
    request: &ReadFulfillmentRequest,
) -> Result<BeginOperation, LixError> {
    begin_inner(
        operation_registry(),
        repository,
        account,
        lease,
        lease_expires_at_ms,
        request,
        crate::telemetry::unix_time_ms,
    )
    .await
}

async fn begin_inner<Now>(
    registry_mutex: &Mutex<OperationRegistry>,
    repository: &str,
    account: &str,
    lease: &str,
    lease_expires_at_ms: u64,
    request: &ReadFulfillmentRequest,
    now: Now,
) -> Result<BeginOperation, LixError>
where
    Now: Fn() -> u64,
{
    let key = operation_key(repository, account, lease, request);
    let scope = scope_fingerprint(repository, account, lease);
    let now_ms = now();
    if request.operation_expires_at_ms <= now_ms {
        return Err(restart_error());
    }
    if request.operation_expires_at_ms > lease_expires_at_ms {
        return Err(invalid(
            "operation expiry exceeds authenticated lease expiry",
        ));
    }
    let request_digest = request.digest()?;
    loop {
        let now_ms = now();
        // A waiter can wake after its pending record has expired and been
        // swept. Re-check the immutable operation deadline before looking up
        // or admitting the ID again; a renewed lease must not revive it.
        if request.operation_expires_at_ms <= now_ms || lease_expires_at_ms <= now_ms {
            return Err(restart_error());
        }
        let existing = {
            let mut registry = registry_mutex
                .lock()
                .map_err(|_| invalid("read operation registry poisoned"))?;
            sweep_expired(&mut registry, now_ms);
            let fingerprint = operation_fingerprint(&key);
            if let Some(retired) = registry.completed.get(&fingerprint) {
                if retired.request_digest != request_digest_fingerprint(&request_digest)
                    || retired.expires_at_ms != request.operation_expires_at_ms
                {
                    return Err(invalid("completed operation identity binding differs"));
                }
                return Err(restart_error());
            }
            if let Some(operation) = registry.operations.get(&key).cloned() {
                if operation.request_digest != request_digest
                    || operation.epoch != request.epoch_id
                    || operation.operation_expires_at_ms != request.operation_expires_at_ms
                {
                    return Err(invalid("read operation identity binding differs"));
                }
                Some(operation)
            } else {
                if lease_expires_at_ms <= now_ms {
                    return Err(restart_error());
                }
                if registry.blocked_scopes.contains_key(&scope)
                    || registry.reject_new_until_ms > now_ms
                {
                    return Err(LixError::new(
                        "LIX_NATIVE_RECIPE_WORK_BOUND",
                        "read operation cancellation registry is saturated for this scope",
                    ));
                }
                if !has_retirement_capacity(
                    &registry,
                    scope,
                    MAX_COMPLETED_FINGERPRINTS,
                    MAX_COMPLETED_FINGERPRINTS_PER_SCOPE,
                )? {
                    return Err(LixError::new(
                        "LIX_NATIVE_RECIPE_WORK_BOUND",
                        "read operation retirement identity quota exceeded",
                    ));
                }
                if registry.operations.len() >= MAX_OPERATION_RECORDS {
                    let (_, scoped_count) = Admission::usage(repository, account)?;
                    let tenant_only =
                        (scoped_count >= MAX_ACCOUNT_SPOOLS).then_some((repository, account));
                    if !retire_one_sealed_for_pressure(
                        &mut registry,
                        None,
                        tenant_only,
                        EvictionNeed::Slot,
                        None,
                    )? {
                        return Err(LixError::new(
                            "LIX_NATIVE_RECIPE_WORK_BOUND",
                            "read operation registry quota exceeded",
                        ));
                    }
                }
                let admission =
                    reserve_admission_with_pressure(&mut registry, repository, account)?;
                let expiry = request
                    .operation_expires_at_ms
                    .min(now_ms.saturating_add(120_000));
                let (changed, _) = watch::channel(());
                let operation = Arc::new(Operation {
                    request_digest: request_digest.clone(),
                    epoch: request.epoch_id.clone(),
                    operation_expires_at_ms: request.operation_expires_at_ms,
                    expires_at_ms: AtomicU64::new(expiry),
                    state: Mutex::new(OperationState::Pending(Some(admission))),
                    changed,
                });
                registry.operations.insert(key.clone(), operation.clone());
                return Ok(BeginOperation::Owner(PendingOperation {
                    key,
                    operation,
                    finished: false,
                }));
            }
        };
        let operation = existing.expect("existing operation was selected");
        let mut changed = operation.changed.subscribe();
        let pending = {
            let state = operation
                .state
                .lock()
                .map_err(|_| invalid("read operation state poisoned"))?;
            match &*state {
                OperationState::Pending(_) => true,
                OperationState::Sealed(sealed) => {
                    if operation.expires_at_ms.load(Ordering::Acquire) <= now_ms
                        || sealed.expires_at_ms <= now_ms
                    {
                        return Err(restart_error());
                    }
                    return replay_first_page(sealed, repository, account, lease, request)
                        .map(BeginOperation::Replay);
                }
                OperationState::Cancelled(_) | OperationState::Complete => {
                    return Err(restart_error());
                }
            }
        };
        debug_assert!(pending);
        let now_ms = now();
        let expiry = operation.expires_at_ms.load(Ordering::Acquire);
        if expiry <= now_ms {
            return Err(restart_error());
        }
        let wait_ms = expiry.saturating_sub(now_ms);
        if tokio::time::timeout(std::time::Duration::from_millis(wait_ms), changed.changed())
            .await
            .is_err()
        {
            return Err(restart_error());
        }
    }
}

fn page(sealed: &SealedSpool, start: (usize, usize)) -> Result<ReadFulfillmentResponse, LixError> {
    if sealed.expires_at_ms <= crate::telemetry::unix_time_ms() {
        return Err(restart_error());
    }
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
    let response = ReadFulfillmentResponse {
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
            spool_id: sealed.token.clone(),
        }),
    };
    if sealed.expires_at_ms <= crate::telemetry::unix_time_ms() {
        return Err(restart_error());
    }
    Ok(response)
}

fn validate_sealed_binding(
    sealed: &SealedSpool,
    repository: &str,
    account: &str,
    lease: &str,
    request: &ReadFulfillmentRequest,
) -> Result<(), LixError> {
    if sealed.repository != repository
        || sealed.account != account
        || sealed.lease != lease
        || sealed.epoch != request.epoch_id
        || sealed.request_digest != request.digest()?
        || sealed.operation_expires_at_ms != request.operation_expires_at_ms
    {
        return Err(invalid("read operation admission binding differs"));
    }
    Ok(())
}

fn replay_first_page(
    sealed: &SealedSpool,
    repository: &str,
    account: &str,
    lease: &str,
    request: &ReadFulfillmentRequest,
) -> Result<ReadFulfillmentResponse, LixError> {
    validate_sealed_binding(sealed, repository, account, lease, request)?;
    page(sealed, (0, 0))
}

fn sealed_usage(
    registry: &OperationRegistry,
    current_key: &OperationKey,
) -> Result<(usize, usize, usize), LixError> {
    let mut count = 0usize;
    let mut scoped_count = 0usize;
    let mut bytes = 0usize;
    for (key, operation) in &registry.operations {
        if key == current_key {
            continue;
        }
        let state = match operation.state.try_lock() {
            Ok(state) => state,
            Err(std::sync::TryLockError::WouldBlock) => {
                return Err(operation_state_busy());
            }
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(invalid("read operation state poisoned"));
            }
        };
        let OperationState::Sealed(sealed) = &*state else {
            continue;
        };
        count += 1;
        if key.0 == current_key.0 && key.1 == current_key.1 {
            scoped_count += 1;
        }
        bytes = bytes.saturating_add(sealed.resident_bytes);
    }
    Ok((count, scoped_count, bytes))
}

fn cancellation_count(
    registry: &OperationRegistry,
    scoped: Option<[u8; 16]>,
) -> Result<usize, LixError> {
    let mut count = 0usize;
    for (key, operation) in &registry.operations {
        if scoped.is_some_and(|scope| scope_fingerprint(&key.0, &key.1, &key.2) != scope) {
            continue;
        }
        match operation.state.try_lock() {
            Ok(state) => {
                if matches!(*state, OperationState::Cancelled(_)) {
                    count += 1;
                }
            }
            // Saturation checks fail closed when another request is changing
            // the operation state, rather than waiting under the registry.
            Err(std::sync::TryLockError::WouldBlock) => count += 1,
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(invalid("read operation state poisoned"));
            }
        }
    }
    Ok(count)
}

fn cancelled_tombstone(
    request_digest: String,
    epoch: String,
    operation_expires_at_ms: u64,
) -> Arc<Operation> {
    let (changed, _) = watch::channel(());
    Arc::new(Operation {
        request_digest,
        epoch,
        operation_expires_at_ms,
        expires_at_ms: AtomicU64::new(operation_expires_at_ms),
        state: Mutex::new(OperationState::Cancelled(None)),
        changed,
    })
}

pub(super) fn seal_and_page(
    spool: InputSpool,
    repository: &str,
    account: &str,
    lease: &str,
    operation_expires_at_ms: u64,
    request: &ReadFulfillmentRequest,
    profile: DiscoveryProfile,
    pending: &mut PendingOperation,
) -> Result<ReadFulfillmentResponse, LixError> {
    if operation_expires_at_ms <= crate::telemetry::unix_time_ms()
        || operation_expires_at_ms != request.operation_expires_at_ms
        || pending.operation.operation_expires_at_ms != request.operation_expires_at_ms
    {
        return Err(restart_error());
    }
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
    let token = uuid::Uuid::now_v7().to_string();
    let expires_at_ms = pending.operation.expires_at_ms.load(Ordering::Acquire);
    let (resident_bytes, live_counter) = {
        let payloads = spool
            .payloads
            .lock()
            .map_err(|_| invalid("operation spool poisoned"))?;
        (payloads.len as usize, payloads.counter.clone())
    };
    let mut sealed = SealedSpool {
        closure_digest: spool.digest(request)?,
        spool,
        resident_bytes,
        live_counter,
        _admission: None,
        egress_pages: AtomicUsize::new(0),
        egress_bytes: AtomicUsize::new(0),
        token,
        page_starts,
        repository: repository.into(),
        account: account.into(),
        lease: lease.into(),
        epoch: request.epoch_id.clone(),
        request_digest: request.digest()?,
        operation_expires_at_ms,
        expires_at_ms,
        profile,
    };
    let response = page(&sealed, (0, 0))?;
    let key = pending.key.clone();
    let mut registry = operation_registry()
        .lock()
        .map_err(|_| invalid("read operation registry poisoned"))?;
    let commit_now = crate::telemetry::unix_time_ms();
    sweep_expired(&mut registry, commit_now);
    if !registry
        .operations
        .get(&key)
        .is_some_and(|operation| Arc::ptr_eq(operation, &pending.operation))
    {
        return Err(restart_error());
    }
    {
        let state = pending
            .operation
            .state
            .lock()
            .map_err(|_| invalid("read operation state poisoned"))?;
        match &*state {
            OperationState::Pending(_) => {}
            OperationState::Cancelled(_) | OperationState::Sealed(_) | OperationState::Complete => {
                return Err(restart_error());
            }
        }
        if operation_expires_at_ms <= commit_now
            || pending.operation.expires_at_ms.load(Ordering::Acquire) <= commit_now
        {
            return Err(restart_error());
        }
    }
    if response.continuation.is_some() {
        let mut admitted = false;
        for attempt in 0..=MAX_PRESSURE_RETIRE_ATTEMPTS {
            let (count, scoped_count, existing_bytes) = sealed_usage(&registry, &key)?;
            let needs_account_slot = scoped_count >= MAX_ACCOUNT_SPOOLS;
            let needs_global_slot = count >= 16;
            let needs_bytes = existing_bytes.saturating_add(resident_bytes) > MAX_SEALED_BYTES;
            if !needs_account_slot && !needs_global_slot && !needs_bytes {
                admitted = true;
                break;
            }
            if attempt == MAX_PRESSURE_RETIRE_ATTEMPTS {
                break;
            }
            let (tenant_only, need) = if needs_account_slot {
                (Some((repository, account)), EvictionNeed::Slot)
            } else if needs_global_slot {
                (None, EvictionNeed::Slot)
            } else {
                (None, EvictionNeed::SealedBytes)
            };
            if !retire_one_sealed_for_pressure(&mut registry, Some(&key), tenant_only, need, None)?
            {
                break;
            }
        }
        if !admitted {
            return Err(LixError::new(
                "LIX_NATIVE_RECIPE_WORK_BOUND",
                "sealed spool admission quota exceeded",
            ));
        }
    }
    let mut state = pending
        .operation
        .state
        .lock()
        .map_err(|_| invalid("read operation state poisoned"))?;
    let admission = match &mut *state {
        OperationState::Pending(admission) => admission,
        OperationState::Cancelled(_) | OperationState::Sealed(_) | OperationState::Complete => {
            return Err(restart_error());
        }
    };
    if operation_expires_at_ms <= crate::telemetry::unix_time_ms()
        || pending.operation.expires_at_ms.load(Ordering::Acquire)
            <= crate::telemetry::unix_time_ms()
    {
        return Err(restart_error());
    }
    if let Some(cursor) = &response.continuation {
        let Some(admission) = admission.take() else {
            return Err(invalid("read operation admission absent"));
        };
        sealed._admission = Some(admission);
        *state = OperationState::Sealed(Arc::new(sealed));
        pending.finished = true;
        pending.operation.changed.send_replace(());
        debug_assert_eq!(
            cursor.spool_id,
            match &*state {
                OperationState::Sealed(sealed) => sealed.token.as_str(),
                _ => "",
            }
        );
    } else {
        let Some(admission) = admission.take() else {
            return Err(invalid("read operation admission absent"));
        };
        // One-page closures retain no spool after this response is built, so
        // they release their reserved retirement slot here. Retrying a lost
        // one-page response safely re-runs this immutable bounded read.
        *state = OperationState::Complete;
        registry.operations.remove(&key);
        pending.finished = true;
        pending.operation.changed.send_replace(());
        drop(admission);
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
    let key = operation_key(repository, account, lease, request);
    let operation = {
        let mut registry = operation_registry()
            .lock()
            .map_err(|_| invalid("read operation registry poisoned"))?;
        sweep_expired(&mut registry, crate::telemetry::unix_time_ms());
        registry
            .operations
            .get(&key)
            .cloned()
            .ok_or_else(restart_error)?
    };
    if operation.request_digest != request.digest()? || operation.epoch != request.epoch_id {
        return Err(invalid("read continuation admission binding differs"));
    }
    let sealed = {
        let state = operation
            .state
            .lock()
            .map_err(|_| invalid("read operation state poisoned"))?;
        match &*state {
            OperationState::Sealed(sealed) => sealed.clone(),
            OperationState::Pending(_)
            | OperationState::Cancelled(_)
            | OperationState::Complete => {
                return Err(restart_error());
            }
        }
    };
    validate_sealed_binding(&sealed, repository, account, lease, request)?;
    if sealed.token != cursor.spool_id || sealed.closure_digest != cursor.closure_digest {
        return Err(invalid("read continuation admission binding differs"));
    }
    let response = page(&sealed, (cursor.next_input, cursor.next_offset))?;
    if response.continuation.is_none() {
        retire_terminal_page(repository, account, lease, request, &operation, &sealed)?;
    }
    Ok(response)
}

/// Retire the sealed spool before returning its terminal page. Rechecking the
/// operation and exact spool under registry -> operation lock order prevents a
/// late page from resurrecting work after cancellation, expiry, or another
/// duplicate terminal request.
fn retire_terminal_page(
    repository: &str,
    account: &str,
    lease: &str,
    request: &ReadFulfillmentRequest,
    operation: &Arc<Operation>,
    sealed: &Arc<SealedSpool>,
) -> Result<(), LixError> {
    let key = operation_key(repository, account, lease, request);
    let mut registry = operation_registry()
        .lock()
        .map_err(|_| invalid("read operation registry poisoned"))?;
    let now = crate::telemetry::unix_time_ms();
    sweep_expired(&mut registry, now);
    if request.operation_expires_at_ms <= now
        || !registry
            .operations
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, operation))
    {
        return Err(restart_error());
    }
    let mut state = operation
        .state
        .lock()
        .map_err(|_| invalid("read operation state poisoned"))?;
    let OperationState::Sealed(current) = &*state else {
        return Err(restart_error());
    };
    if !Arc::ptr_eq(current, sealed)
        || current.expires_at_ms <= now
        || current.operation_expires_at_ms != request.operation_expires_at_ms
        || current.request_digest != request.digest()?
    {
        return Err(restart_error());
    }
    *state = OperationState::Complete;
    registry.operations.remove(&key);
    drop(state);
    record_completed(
        &mut registry,
        &key,
        &operation.request_digest,
        operation.operation_expires_at_ms,
    );
    operation.changed.send_replace(());
    Ok(())
}

pub(super) fn release(
    repository: &str,
    account: &str,
    lease: &str,
    lease_expires_at_ms: u64,
    request: &ReadFulfillmentRequest,
) -> Result<(), LixError> {
    let now = crate::telemetry::unix_time_ms();
    if lease_expires_at_ms <= now {
        return Err(restart_error());
    }
    if request.operation_expires_at_ms <= now {
        return Err(restart_error());
    }
    if request.operation_expires_at_ms > lease_expires_at_ms {
        return Err(invalid(
            "operation expiry exceeds authenticated lease expiry",
        ));
    }
    let key = operation_key(repository, account, lease, request);
    let scope = scope_fingerprint(repository, account, lease);
    let fingerprint = operation_fingerprint(&key);
    let request_digest = request.digest()?;
    let request_digest_hash = request_digest_fingerprint(&request_digest);
    let mut registry = operation_registry()
        .lock()
        .map_err(|_| invalid("read operation registry poisoned"))?;
    sweep_expired(&mut registry, now);
    if let Some(retired) = registry.completed.get(&fingerprint) {
        if retired.request_digest != request_digest_hash
            || retired.expires_at_ms != request.operation_expires_at_ms
        {
            return Err(invalid("completed operation identity binding differs"));
        }
        return Err(restart_error());
    }
    if let Some(operation) = registry.operations.get(&key).cloned() {
        if operation.request_digest != request_digest
            || operation.epoch != request.epoch_id
            || operation.operation_expires_at_ms != request.operation_expires_at_ms
        {
            return Err(invalid("release admission binding differs"));
        }
        {
            let state = operation
                .state
                .lock()
                .map_err(|_| invalid("read operation state poisoned"))?;
            if matches!(&*state, OperationState::Cancelled(_)) {
                return Ok(());
            }
        }
        let cancelled_total = cancellation_count(&registry, None)?;
        let cancelled_scoped = cancellation_count(&registry, Some(scope))?;
        let over_quota = cancelled_total >= MAX_CANCELLED_OPERATIONS
            || cancelled_scoped >= MAX_ACCOUNT_CANCELLED_OPERATIONS
            || registry.operations.len() >= MAX_OPERATION_RECORDS;
        if over_quota {
            block_scope(&mut registry, scope, request.operation_expires_at_ms);
        }
        let mut state = operation
            .state
            .lock()
            .map_err(|_| invalid("read operation state poisoned"))?;
        match std::mem::replace(&mut *state, OperationState::Complete) {
            OperationState::Pending(admission) => {
                *state = OperationState::Cancelled(admission);
                operation
                    .expires_at_ms
                    .store(request.operation_expires_at_ms, Ordering::Release);
            }
            OperationState::Sealed(_) if over_quota => {
                registry.operations.remove(&key);
            }
            OperationState::Sealed(_) | OperationState::Cancelled(None) => {
                *state = OperationState::Cancelled(None);
                operation
                    .expires_at_ms
                    .store(request.operation_expires_at_ms, Ordering::Release);
            }
            OperationState::Complete => {
                registry.operations.remove(&key);
            }
            OperationState::Cancelled(Some(admission)) => {
                *state = OperationState::Cancelled(Some(admission));
            }
        }
        operation.changed.send_replace(());
        return Ok(());
    }

    let cancelled_total = cancellation_count(&registry, None)?;
    let cancelled_scoped = cancellation_count(&registry, Some(scope))?;
    if cancelled_total >= MAX_CANCELLED_OPERATIONS
        || cancelled_scoped >= MAX_ACCOUNT_CANCELLED_OPERATIONS
        || registry.operations.len() >= MAX_OPERATION_RECORDS
    {
        // Cancellation saturation fails closed only for this repository,
        // account, and lease; valid operations in other scopes remain usable.
        block_scope(&mut registry, scope, request.operation_expires_at_ms);
        return Ok(());
    }
    registry.operations.insert(
        key,
        cancelled_tombstone(
            request_digest,
            request.epoch_id.clone(),
            request.operation_expires_at_ms,
        ),
    );
    Ok(())
}

#[cfg(all(test, not(target_family = "wasm")))]
mod tests {
    use super::*;

    fn test_payload_spool_with_budget(
        counter: Arc<AtomicUsize>,
        live_limit: usize,
    ) -> PayloadSpool {
        PayloadSpool {
            #[cfg(not(target_family = "wasm"))]
            file: None,
            #[cfg(target_family = "wasm")]
            bytes: Vec::new(),
            len: 0,
            pressure_key: None,
            counter: LiveSpoolCounter::Local(counter),
            live_limit,
        }
    }

    fn insert_test_sealed(
        repository: &str,
        account: &str,
        lease: &str,
        request: &ReadFulfillmentRequest,
        spool: InputSpool,
        resident_bytes: usize,
    ) {
        let request_digest = request.digest().unwrap();
        let live_counter = spool.payloads.lock().unwrap().counter.clone();
        let key = operation_key(repository, account, lease, request);
        let (changed, _) = watch::channel(());
        let sealed = Arc::new(SealedSpool {
            spool,
            resident_bytes,
            live_counter,
            _admission: Some(Admission::reserve(repository, account).unwrap()),
            egress_pages: AtomicUsize::new(1),
            egress_bytes: AtomicUsize::new(0),
            token: uuid::Uuid::now_v7().to_string(),
            repository: repository.into(),
            account: account.into(),
            lease: lease.into(),
            epoch: request.epoch_id.clone(),
            request_digest: request_digest.clone(),
            closure_digest: blake3::hash(b"inactive pressure victim")
                .to_hex()
                .to_string(),
            operation_expires_at_ms: request.operation_expires_at_ms,
            expires_at_ms: request.operation_expires_at_ms,
            profile: DiscoveryProfile::default(),
            page_starts: vec![(0, 0), (1, 0)],
        });
        let operation = Arc::new(Operation {
            request_digest,
            epoch: request.epoch_id.clone(),
            operation_expires_at_ms: request.operation_expires_at_ms,
            expires_at_ms: AtomicU64::new(request.operation_expires_at_ms),
            state: Mutex::new(OperationState::Sealed(sealed)),
            changed,
        });
        assert!(
            operation_registry()
                .lock()
                .unwrap()
                .operations
                .insert(key, operation)
                .is_none()
        );
    }

    fn clear_test_scope(repository: &str, account: &str, lease: &str) {
        let scope = scope_fingerprint(repository, account, lease);
        let mut registry = operation_registry().lock().unwrap();
        registry
            .operations
            .retain(|key, _| scope_fingerprint(&key.0, &key.1, &key.2) != scope);
        let completed = registry
            .completed
            .iter()
            .filter_map(|(fingerprint, retired)| (retired.scope == scope).then_some(*fingerprint))
            .collect::<BTreeSet<_>>();
        for fingerprint in &completed {
            registry.completed.remove(fingerprint);
        }
        registry
            .completed_expiry
            .retain(|(_, fingerprint)| !completed.contains(fingerprint));
        registry.completed_scope_counts.remove(&scope);
        registry.blocked_scopes.remove(&scope);
        registry
            .blocked_scope_expiry
            .retain(|(_, blocked_scope)| *blocked_scope != scope);
    }

    #[tokio::test]
    async fn sealed_128_mib_closure_replays_after_scoped_saturation_and_retires() {
        let authority = crate::open_lix().await.unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let repository = descriptor.lix_id.clone();
        let mut request = ReadFulfillmentRequest {
            operation_id: uuid::Uuid::now_v7().to_string(),
            release: false,
            operation_expires_at_ms: u64::MAX,
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
        let mut pending = match begin(&repository, &account, &lease, u64::MAX, &request)
            .await
            .unwrap()
        {
            BeginOperation::Owner(pending) => pending,
            BeginOperation::Replay(_) => panic!("new operation unexpectedly replayed"),
        };
        let waiter_repository = repository.clone();
        let waiter_account = account.clone();
        let waiter_lease = lease.clone();
        let waiter_request = request.clone();
        let mut waiter = tokio::spawn(async move {
            begin(
                &waiter_repository,
                &waiter_account,
                &waiter_lease,
                u64::MAX,
                &waiter_request,
            )
            .await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut waiter)
                .await
                .is_err()
        );
        let mut page = seal_and_page(
            spool,
            &repository,
            &account,
            &lease,
            u64::MAX,
            &request,
            DiscoveryProfile::default(),
            &mut pending,
        )
        .unwrap();
        let closure_digest = page.closure_digest.clone();
        let initial = page.clone();
        for _ in 0..65 {
            let mut cancellation = request.clone();
            cancellation.operation_id = uuid::Uuid::now_v7().to_string();
            cancellation.release = true;
            cancellation.continuation = None;
            release(&repository, &account, &lease, u64::MAX, &cancellation).unwrap();
        }
        let other_account = uuid::Uuid::now_v7().to_string();
        let other_scope = match begin(&repository, &other_account, &lease, u64::MAX, &request)
            .await
            .unwrap()
        {
            BeginOperation::Owner(owner) => owner,
            BeginOperation::Replay(_) => panic!("another account inherited an operation"),
        };
        drop(other_scope);
        let blocked_new = ReadFulfillmentRequest {
            operation_id: uuid::Uuid::now_v7().to_string(),
            ..request.clone()
        };
        assert_eq!(
            begin(&repository, &account, &lease, u64::MAX, &blocked_new)
                .await
                .err()
                .unwrap()
                .code,
            "LIX_NATIVE_RECIPE_WORK_BOUND"
        );
        let replay = match begin(&repository, &account, &lease, u64::MAX, &request)
            .await
            .unwrap()
        {
            BeginOperation::Replay(replay) => replay,
            BeginOperation::Owner(_) => panic!("sealed operation was not replayed"),
        };
        let waiter_replay =
            match tokio::time::timeout(std::time::Duration::from_secs(1), &mut waiter)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
            {
                BeginOperation::Replay(replay) => replay,
                BeginOperation::Owner(_) => panic!("waiting operation was not replayed"),
            };
        assert_eq!(
            serde_json::to_vec(&initial).unwrap(),
            serde_json::to_vec(&replay).unwrap()
        );
        assert_eq!(
            serde_json::to_vec(&initial).unwrap(),
            serde_json::to_vec(&waiter_replay).unwrap()
        );
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
            let terminal_page = cursor.next_input == 124;
            request.continuation = Some(cursor);
            page = continuation_page(&repository, &account, &lease, &request).unwrap();
            if terminal_page {
                assert_eq!(
                    continuation_page(&repository, &account, &lease, &request)
                        .unwrap_err()
                        .code,
                    "LIX_READ_FULFILLMENT_RESTART"
                );
            } else {
                let replay = continuation_page(&repository, &account, &lease, &request).unwrap();
                assert_eq!(
                    serde_json::to_vec(&replay).unwrap(),
                    serde_json::to_vec(&page).unwrap()
                );
                drop(replay);
            }
            assert!(continuation_page(&repository, "other-account", &lease, &request).is_err());
            assert!(continuation_page(&repository, &account, "other-lease", &request).is_err());
            assert!(continuation_page("other-repository", &account, &lease, &request).is_err());
        }
        assert_eq!(pages, 32);
        assert_eq!(members, 128);
        assert_eq!(digest.finalize().to_hex().to_string(), closure_digest);
        assert_eq!(
            begin(&repository, &account, &lease, u64::MAX, &request)
                .await
                .err()
                .unwrap()
                .code,
            "LIX_READ_FULFILLMENT_RESTART"
        );
        let restart = continuation_page(&repository, &account, &lease, &request).unwrap_err();
        assert_eq!(restart.code, "LIX_READ_FULFILLMENT_RESTART");
        clear_test_scope(&repository, &account, &lease);
        clear_test_scope(&repository, &other_account, &lease);
        authority.close().await.unwrap();
    }

    #[tokio::test]
    async fn completed_registry_retires_more_than_64_paginated_operations() {
        use std::time::Instant;

        const COMPLETIONS: usize = 65;
        let authority = crate::open_lix().await.unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let repository = descriptor.lix_id.clone();
        let account = uuid::Uuid::now_v7().to_string();
        let lease = uuid::Uuid::now_v7().to_string();
        let operation_expires_at_ms = crate::telemetry::unix_time_ms() + 60_000;
        let started = Instant::now();

        clear_test_scope(&repository, &account, &lease);
        for _ in 0..COMPLETIONS {
            let request = ReadFulfillmentRequest {
                operation_id: uuid::Uuid::now_v7().to_string(),
                release: false,
                operation_expires_at_ms,
                epoch_id: uuid::Uuid::now_v7().to_string(),
                descriptor: descriptor.clone(),
                interests: Vec::new(),
                required: Vec::new(),
                continuation: None,
            };
            let request_digest = request.digest().unwrap();
            let token = uuid::Uuid::now_v7().to_string();
            let closure_digest = blake3::hash(b"registry retirement closure")
                .to_hex()
                .to_string();
            let key = operation_key(&repository, &account, &lease, &request);
            let (changed, _) = watch::channel(());
            let sealed = Arc::new(SealedSpool {
                spool: InputSpool::new(Arc::new(Mutex::new(PayloadSpool::default()))),
                resident_bytes: 0,
                live_counter: LiveSpoolCounter::default(),
                _admission: Some(Admission::reserve(&repository, &account).unwrap()),
                egress_pages: AtomicUsize::new(1),
                egress_bytes: AtomicUsize::new(0),
                token,
                repository: repository.clone(),
                account: account.clone(),
                lease: lease.clone(),
                epoch: request.epoch_id.clone(),
                request_digest: request_digest.clone(),
                closure_digest,
                operation_expires_at_ms,
                expires_at_ms: operation_expires_at_ms,
                profile: DiscoveryProfile::default(),
                page_starts: vec![(0, 0), (1, 0)],
            });
            let operation = Arc::new(Operation {
                request_digest,
                epoch: request.epoch_id.clone(),
                operation_expires_at_ms,
                expires_at_ms: AtomicU64::new(operation_expires_at_ms),
                state: Mutex::new(OperationState::Sealed(sealed.clone())),
                changed,
            });
            {
                let mut registry = operation_registry().lock().unwrap();
                assert!(registry.operations.insert(key, operation).is_none());
            }

            let key = operation_key(&repository, &account, &lease, &request);
            let operation = operation_registry()
                .lock()
                .unwrap()
                .operations
                .get(&key)
                .cloned()
                .unwrap();
            retire_terminal_page(&repository, &account, &lease, &request, &operation, &sealed)
                .unwrap();
            assert_eq!(
                begin(
                    &repository,
                    &account,
                    &lease,
                    operation_expires_at_ms,
                    &request,
                )
                .await
                .err()
                .unwrap()
                .code,
                "LIX_READ_FULFILLMENT_RESTART"
            );
            drop(operation);
            drop(sealed);
        }

        let scope = scope_fingerprint(&repository, &account, &lease);
        {
            let registry = operation_registry().lock().unwrap();
            assert_eq!(
                registry.completed_scope_counts.get(&scope),
                Some(&COMPLETIONS)
            );
            assert_eq!(cancellation_count(&registry, Some(scope)).unwrap(), 0);
        }

        // Completed operations are not counted against the cursorless
        // cancellation budget; a new paginated read in the same scope remains
        // admissible after more than 64 automatic retirements.
        let fresh = ReadFulfillmentRequest {
            operation_id: uuid::Uuid::now_v7().to_string(),
            release: false,
            operation_expires_at_ms,
            epoch_id: uuid::Uuid::now_v7().to_string(),
            descriptor,
            interests: Vec::new(),
            required: Vec::new(),
            continuation: None,
        };
        match begin(
            &repository,
            &account,
            &lease,
            operation_expires_at_ms,
            &fresh,
        )
        .await
        .unwrap()
        {
            BeginOperation::Owner(owner) => drop(owner),
            BeginOperation::Replay(_) => panic!("fresh operation unexpectedly replayed"),
        }

        eprintln!(
            "READ_OPERATION_REGISTRY_RETIREMENT_PROFILE_JSON={{\"completedPaginatedOperations\":{COMPLETIONS},\"completedMarkers\":{COMPLETIONS},\"cursorlessCancellations\":0,\"elapsedMs\":{}}}",
            started.elapsed().as_secs_f64() * 1000.0
        );
        clear_test_scope(&repository, &account, &lease);
        authority.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_new_read_retires_an_inactive_sealed_spool_at_account_capacity() {
        let authority = crate::open_lix().await.unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let repository = descriptor.lix_id.clone();
        let account = uuid::Uuid::now_v7().to_string();
        let lease = uuid::Uuid::now_v7().to_string();
        let operation_expires_at_ms = crate::telemetry::unix_time_ms() + 60_000;
        let new_request = || ReadFulfillmentRequest {
            operation_id: uuid::Uuid::now_v7().to_string(),
            release: false,
            operation_expires_at_ms,
            epoch_id: uuid::Uuid::now_v7().to_string(),
            descriptor: descriptor.clone(),
            interests: Vec::new(),
            required: Vec::new(),
            continuation: None,
        };

        clear_test_scope(&repository, &account, &lease);
        let mut retired_requests = Vec::new();
        for _ in 0..MAX_ACCOUNT_SPOOLS {
            let request = new_request();
            let request_digest = request.digest().unwrap();
            let key = operation_key(&repository, &account, &lease, &request);
            let (changed, _) = watch::channel(());
            let sealed = Arc::new(SealedSpool {
                spool: InputSpool::new(Arc::new(Mutex::new(PayloadSpool::default()))),
                resident_bytes: 0,
                live_counter: LiveSpoolCounter::default(),
                _admission: Some(Admission::reserve(&repository, &account).unwrap()),
                egress_pages: AtomicUsize::new(1),
                egress_bytes: AtomicUsize::new(0),
                token: uuid::Uuid::now_v7().to_string(),
                repository: repository.clone(),
                account: account.clone(),
                lease: lease.clone(),
                epoch: request.epoch_id.clone(),
                request_digest: request_digest.clone(),
                closure_digest: blake3::hash(b"inactive pressure victim")
                    .to_hex()
                    .to_string(),
                operation_expires_at_ms,
                expires_at_ms: operation_expires_at_ms,
                profile: DiscoveryProfile::default(),
                page_starts: vec![(0, 0), (1, 0)],
            });
            let operation = Arc::new(Operation {
                request_digest,
                epoch: request.epoch_id.clone(),
                operation_expires_at_ms,
                expires_at_ms: AtomicU64::new(operation_expires_at_ms),
                state: Mutex::new(OperationState::Sealed(sealed)),
                changed,
            });
            assert!(
                operation_registry()
                    .lock()
                    .unwrap()
                    .operations
                    .insert(key, operation)
                    .is_none()
            );
            retired_requests.push(request);
        }

        let fresh = new_request();
        let owner = match begin(
            &repository,
            &account,
            &lease,
            operation_expires_at_ms,
            &fresh,
        )
        .await
        .unwrap()
        {
            BeginOperation::Owner(owner) => owner,
            BeginOperation::Replay(_) => panic!("fresh operation unexpectedly replayed"),
        };
        let retired =
            retired_requests
                .iter()
                .find(|request| {
                    operation_registry().lock().unwrap().completed.contains_key(
                        &operation_fingerprint(&operation_key(
                            &repository,
                            &account,
                            &lease,
                            request,
                        )),
                    )
                })
                .expect("one sealed operation is retained as a restart marker")
                .clone();
        assert_eq!(
            begin(
                &repository,
                &account,
                &lease,
                operation_expires_at_ms,
                &retired
            )
            .await
            .err()
            .unwrap()
            .code,
            "LIX_READ_FULFILLMENT_RESTART"
        );
        let mut mismatched = retired;
        mismatched.epoch_id = uuid::Uuid::now_v7().to_string();
        assert_eq!(
            begin(
                &repository,
                &account,
                &lease,
                operation_expires_at_ms,
                &mismatched,
            )
            .await
            .err()
            .unwrap()
            .code,
            "LIX_READ_FULFILLMENT_INVALID"
        );
        drop(owner);
        clear_test_scope(&repository, &account, &lease);
        authority.close().await.unwrap();
    }

    #[tokio::test]
    async fn global_admission_pressure_retires_from_a_heavy_tenant_first() {
        let authority = crate::open_lix().await.unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let repository = descriptor.lix_id.clone();
        let lease = uuid::Uuid::now_v7().to_string();
        let operation_expires_at_ms = crate::telemetry::unix_time_ms() + 60_000;
        let accounts = (0..6)
            .map(|_| uuid::Uuid::now_v7().to_string())
            .collect::<Vec<_>>();
        let new_request = || ReadFulfillmentRequest {
            operation_id: uuid::Uuid::now_v7().to_string(),
            release: false,
            operation_expires_at_ms,
            epoch_id: uuid::Uuid::now_v7().to_string(),
            descriptor: descriptor.clone(),
            interests: Vec::new(),
            required: Vec::new(),
            continuation: None,
        };

        for account in &accounts {
            clear_test_scope(&repository, account, &lease);
        }
        let mut requests_by_account = BTreeMap::<String, Vec<ReadFulfillmentRequest>>::new();
        for (account_index, count) in [4, 4, 4, 3, 1].into_iter().enumerate() {
            let account = &accounts[account_index];
            for _ in 0..count {
                let request = new_request();
                insert_test_sealed(
                    &repository,
                    account,
                    &lease,
                    &request,
                    InputSpool::new(Arc::new(Mutex::new(PayloadSpool::default()))),
                    0,
                );
                requests_by_account
                    .entry(account.clone())
                    .or_default()
                    .push(request);
            }
        }
        assert_eq!(Admission::usage(&repository, &accounts[0]).unwrap().0, 16);

        let fresh = new_request();
        let fresh_owner = match begin(
            &repository,
            &accounts[5],
            &lease,
            operation_expires_at_ms,
            &fresh,
        )
        .await
        .unwrap()
        {
            BeginOperation::Owner(owner) => owner,
            BeginOperation::Replay(_) => panic!("fresh operation unexpectedly replayed"),
        };

        let retired = {
            let registry = operation_registry().lock().unwrap();
            let mut retired = Vec::new();
            for (account, requests) in &requests_by_account {
                for request in requests {
                    if registry
                        .completed
                        .contains_key(&operation_fingerprint(&operation_key(
                            &repository,
                            account,
                            &lease,
                            request,
                        )))
                    {
                        retired.push(account.clone());
                    }
                }
            }
            retired
        };
        assert_eq!(
            retired.len(),
            1,
            "one completed marker should replace one admission"
        );
        assert!(
            accounts[..3].contains(&retired[0]),
            "global eviction should select a heavy tenant, got account index {}",
            accounts
                .iter()
                .position(|account| account == &retired[0])
                .unwrap()
        );
        assert_eq!(Admission::usage(&repository, &accounts[0]).unwrap().0, 16);

        let retired_request =
            requests_by_account[&retired[0]]
                .iter()
                .find(|request| {
                    operation_registry().lock().unwrap().completed.contains_key(
                        &operation_fingerprint(&operation_key(
                            &repository,
                            &retired[0],
                            &lease,
                            request,
                        )),
                    )
                })
                .unwrap();
        assert_eq!(
            begin(
                &repository,
                &retired[0],
                &lease,
                operation_expires_at_ms,
                retired_request,
            )
            .await
            .err()
            .unwrap()
            .code,
            "LIX_READ_FULFILLMENT_RESTART"
        );

        drop(fresh_owner);
        for account in &accounts {
            clear_test_scope(&repository, account, &lease);
        }
        authority.close().await.unwrap();
    }

    #[tokio::test]
    async fn live_byte_pressure_retires_sealed_payload_before_retrying_append() {
        let authority = crate::open_lix().await.unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let repository = descriptor.lix_id.clone();
        let account = uuid::Uuid::now_v7().to_string();
        let lease = uuid::Uuid::now_v7().to_string();
        let operation_expires_at_ms = crate::telemetry::unix_time_ms() + 60_000;
        let new_request = || ReadFulfillmentRequest {
            operation_id: uuid::Uuid::now_v7().to_string(),
            release: false,
            operation_expires_at_ms,
            epoch_id: uuid::Uuid::now_v7().to_string(),
            descriptor: descriptor.clone(),
            interests: Vec::new(),
            required: Vec::new(),
            continuation: None,
        };
        clear_test_scope(&repository, &account, &lease);

        let counter = Arc::new(AtomicUsize::new(0));
        let retired = new_request();
        let retired_bytes = b"12345678".to_vec();
        let retired_address = ReadInputAddress::BlobChunk(*blake3::hash(&retired_bytes).as_bytes());
        let retired_payloads = Arc::new(Mutex::new(test_payload_spool_with_budget(
            counter.clone(),
            8,
        )));
        let mut retired_spool = InputSpool::new(retired_payloads.clone());
        retired_spool
            .append(ReadInput {
                address: retired_address,
                bytes: retired_bytes,
            })
            .unwrap();
        drop(retired_payloads);
        insert_test_sealed(&repository, &account, &lease, &retired, retired_spool, 8);
        assert_eq!(counter.load(Ordering::Acquire), 8);

        let current = new_request();
        let owner = match begin(
            &repository,
            &account,
            &lease,
            operation_expires_at_ms,
            &current,
        )
        .await
        .unwrap()
        {
            BeginOperation::Owner(owner) => owner,
            BeginOperation::Replay(_) => panic!("fresh operation unexpectedly replayed"),
        };
        let payloads = Arc::new(Mutex::new(test_payload_spool_with_budget(
            counter.clone(),
            8,
        )));
        owner.bind_payload_spool(&payloads).unwrap();
        let mut current_spool = InputSpool::new(payloads.clone());
        let current_bytes = b"data".to_vec();
        current_spool
            .append(ReadInput {
                address: ReadInputAddress::BlobChunk(*blake3::hash(&current_bytes).as_bytes()),
                bytes: current_bytes,
            })
            .unwrap();
        assert_eq!(counter.load(Ordering::Acquire), 4);
        assert_eq!(
            begin(
                &repository,
                &account,
                &lease,
                operation_expires_at_ms,
                &retired
            )
            .await
            .err()
            .unwrap()
            .code,
            "LIX_READ_FULFILLMENT_RESTART"
        );
        drop(current_spool);
        drop(payloads);
        drop(owner);
        assert_eq!(counter.load(Ordering::Acquire), 0);
        clear_test_scope(&repository, &account, &lease);
        authority.close().await.unwrap();
    }

    #[tokio::test]
    async fn append_pressure_skips_a_locked_active_spool_without_blocking() {
        let authority = crate::open_lix().await.unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let repository = descriptor.lix_id.clone();
        let account = uuid::Uuid::now_v7().to_string();
        let lease = uuid::Uuid::now_v7().to_string();
        let operation_expires_at_ms = crate::telemetry::unix_time_ms() + 60_000;
        let new_request = || ReadFulfillmentRequest {
            operation_id: uuid::Uuid::now_v7().to_string(),
            release: false,
            operation_expires_at_ms,
            epoch_id: uuid::Uuid::now_v7().to_string(),
            descriptor: descriptor.clone(),
            interests: Vec::new(),
            required: Vec::new(),
            continuation: None,
        };
        clear_test_scope(&repository, &account, &lease);

        let counter = Arc::new(AtomicUsize::new(0));
        let active = new_request();
        let inactive = new_request();
        for (request, bytes) in [(&active, b"live".to_vec()), (&inactive, b"free".to_vec())] {
            let address = ReadInputAddress::BlobChunk(*blake3::hash(&bytes).as_bytes());
            let payloads = Arc::new(Mutex::new(test_payload_spool_with_budget(
                counter.clone(),
                32,
            )));
            let mut spool = InputSpool::new(payloads.clone());
            spool.append(ReadInput { address, bytes }).unwrap();
            drop(payloads);
            insert_test_sealed(&repository, &account, &lease, request, spool, 4);
        }

        let active_key = operation_key(&repository, &account, &lease, &active);
        let active_operation = operation_registry()
            .lock()
            .unwrap()
            .operations
            .get(&active_key)
            .unwrap()
            .clone();
        let current = new_request();
        let owner = match begin(
            &repository,
            &account,
            &lease,
            operation_expires_at_ms,
            &current,
        )
        .await
        .unwrap()
        {
            BeginOperation::Owner(owner) => owner,
            BeginOperation::Replay(_) => panic!("fresh operation unexpectedly replayed"),
        };
        let active_state = active_operation.state.lock().unwrap();
        let payloads = Arc::new(Mutex::new(test_payload_spool_with_budget(
            counter.clone(),
            8,
        )));
        owner.bind_payload_spool(&payloads).unwrap();
        let append_payloads = payloads.clone();
        let bytes = b"data".to_vec();
        let address = ReadInputAddress::BlobChunk(*blake3::hash(&bytes).as_bytes());
        let (sent, received) = std::sync::mpsc::channel();
        let append = std::thread::spawn(move || {
            let mut spool = InputSpool::new(append_payloads);
            sent.send(spool.append(ReadInput { address, bytes }).map(|_| ()))
                .unwrap();
        });
        let append_result = received.recv_timeout(std::time::Duration::from_secs(1));
        drop(active_state);
        append.join().unwrap();
        append_result
            .expect("pressure retirement blocked on a state held by an active reader")
            .unwrap();
        assert_eq!(counter.load(Ordering::Acquire), 8);
        assert!(
            operation_registry()
                .lock()
                .unwrap()
                .operations
                .contains_key(&active_key)
        );
        assert_eq!(
            begin(
                &repository,
                &account,
                &lease,
                operation_expires_at_ms,
                &inactive
            )
            .await
            .err()
            .unwrap()
            .code,
            "LIX_READ_FULFILLMENT_RESTART"
        );
        drop(active_operation);
        drop(payloads);
        drop(owner);
        clear_test_scope(&repository, &account, &lease);
        authority.close().await.unwrap();
    }

    #[tokio::test]
    async fn sealed_pressure_never_retires_a_pending_owner() {
        let authority = crate::open_lix().await.unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let repository = descriptor.lix_id.clone();
        let account = uuid::Uuid::now_v7().to_string();
        let lease = uuid::Uuid::now_v7().to_string();
        let operation_expires_at_ms = crate::telemetry::unix_time_ms() + 60_000;
        let new_request = || ReadFulfillmentRequest {
            operation_id: uuid::Uuid::now_v7().to_string(),
            release: false,
            operation_expires_at_ms,
            epoch_id: uuid::Uuid::now_v7().to_string(),
            descriptor: descriptor.clone(),
            interests: Vec::new(),
            required: Vec::new(),
            continuation: None,
        };
        clear_test_scope(&repository, &account, &lease);

        let pending_request = new_request();
        let pending_owner = match begin(
            &repository,
            &account,
            &lease,
            operation_expires_at_ms,
            &pending_request,
        )
        .await
        .unwrap()
        {
            BeginOperation::Owner(owner) => owner,
            BeginOperation::Replay(_) => panic!("fresh operation unexpectedly replayed"),
        };
        for _ in 0..(MAX_ACCOUNT_SPOOLS - 1) {
            let request = new_request();
            insert_test_sealed(
                &repository,
                &account,
                &lease,
                &request,
                InputSpool::new(Arc::new(Mutex::new(PayloadSpool::default()))),
                0,
            );
        }

        let fresh = new_request();
        let fresh_owner = match begin(
            &repository,
            &account,
            &lease,
            operation_expires_at_ms,
            &fresh,
        )
        .await
        .unwrap()
        {
            BeginOperation::Owner(owner) => owner,
            BeginOperation::Replay(_) => panic!("fresh operation unexpectedly replayed"),
        };
        assert!(matches!(
            *pending_owner.operation.state.lock().unwrap(),
            OperationState::Pending(Some(_))
        ));
        assert!(
            operation_registry()
                .lock()
                .unwrap()
                .operations
                .get(&pending_owner.key)
                .is_some_and(|operation| Arc::ptr_eq(operation, &pending_owner.operation))
        );
        drop(fresh_owner);
        drop(pending_owner);
        clear_test_scope(&repository, &account, &lease);
        authority.close().await.unwrap();
    }

    #[tokio::test]
    async fn cursorless_cancel_tombstones_are_scoped_and_do_not_consume_read_admission() {
        let authority = crate::open_lix().await.unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let repository = descriptor.lix_id.clone();
        let account = uuid::Uuid::now_v7().to_string();
        let other_account = uuid::Uuid::now_v7().to_string();
        let lease = uuid::Uuid::now_v7().to_string();
        let lease_expires_at_ms = crate::telemetry::unix_time_ms() + 60_000;
        let bytes = b"cursorless operation binding".to_vec();
        let address = ReadInputAddress::BlobChunk(*blake3::hash(&bytes).as_bytes());
        let new_request = || ReadFulfillmentRequest {
            operation_id: uuid::Uuid::now_v7().to_string(),
            release: false,
            operation_expires_at_ms: lease_expires_at_ms,
            epoch_id: uuid::Uuid::now_v7().to_string(),
            descriptor: descriptor.clone(),
            interests: vec![crate::hot_state::LogicalReadInterest::FilesystemMetadata {
                directory: false,
                branch_ids: vec![descriptor.selected_branch.branch_id.clone()],
                file_ids: None,
                directory_ids: None,
                root_directory: false,
                path_predicate: crate::hot_state::FilePathInterest::All,
            }],
            required: vec![address.clone()],
            continuation: None,
        };

        let mut released_requests = vec![new_request()];
        released_requests.extend((1..4).map(|_| new_request()));
        let cancelled = released_requests[0].clone();
        for first in released_requests {
            let mut release_request = first.clone();
            release_request.release = true;
            release(
                &repository,
                &account,
                &lease,
                lease_expires_at_ms,
                &release_request,
            )
            .unwrap();
            assert_eq!(
                begin(&repository, &account, &lease, lease_expires_at_ms, &first)
                    .await
                    .err()
                    .unwrap()
                    .code,
                "LIX_READ_FULFILLMENT_RESTART"
            );
        }

        let cross_account = cancelled.clone();
        let other_scope = match begin(
            &repository,
            &other_account,
            &lease,
            lease_expires_at_ms,
            &cross_account,
        )
        .await
        .unwrap()
        {
            BeginOperation::Owner(operation) => operation,
            BeginOperation::Replay(_) => panic!("another account inherited an operation"),
        };
        drop(other_scope);

        let changed_digest = {
            let mut changed = cancelled.clone();
            changed.epoch_id = uuid::Uuid::now_v7().to_string();
            changed
        };
        let mut first_cancelled = cancelled;
        first_cancelled.release = true;
        release(
            &repository,
            &account,
            &lease,
            lease_expires_at_ms,
            &first_cancelled,
        )
        .unwrap();
        let mut mismatched = changed_digest;
        mismatched.operation_id = first_cancelled.operation_id.clone();
        assert_eq!(
            begin(
                &repository,
                &account,
                &lease,
                lease_expires_at_ms,
                &mismatched
            )
            .await
            .err()
            .unwrap()
            .code,
            "LIX_READ_FULFILLMENT_INVALID"
        );

        let healthy = new_request();
        let owner = match begin(&repository, &account, &lease, lease_expires_at_ms, &healthy)
            .await
            .unwrap()
        {
            BeginOperation::Owner(owner) => owner,
            BeginOperation::Replay(_) => panic!("fresh operation unexpectedly replayed"),
        };
        drop(owner);
        authority.close().await.unwrap();
    }

    #[tokio::test]
    async fn release_during_pending_discovery_prevents_sealing_and_releases_admission() {
        let authority = crate::open_lix().await.unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let repository = descriptor.lix_id.clone();
        let account = uuid::Uuid::now_v7().to_string();
        let lease = uuid::Uuid::now_v7().to_string();
        let lease_expires_at_ms = crate::telemetry::unix_time_ms() + 60_000;
        let bytes = b"cancel while pending".to_vec();
        let address = ReadInputAddress::BlobChunk(*blake3::hash(&bytes).as_bytes());
        let request = ReadFulfillmentRequest {
            operation_id: uuid::Uuid::now_v7().to_string(),
            release: false,
            operation_expires_at_ms: lease_expires_at_ms,
            epoch_id: uuid::Uuid::now_v7().to_string(),
            descriptor: descriptor.clone(),
            interests: vec![crate::hot_state::LogicalReadInterest::FilesystemMetadata {
                directory: false,
                branch_ids: vec![descriptor.selected_branch.branch_id.clone()],
                file_ids: None,
                directory_ids: None,
                root_directory: false,
                path_predicate: crate::hot_state::FilePathInterest::All,
            }],
            required: vec![address.clone()],
            continuation: None,
        };
        let mut owner = match begin(&repository, &account, &lease, lease_expires_at_ms, &request)
            .await
            .unwrap()
        {
            BeginOperation::Owner(owner) => owner,
            BeginOperation::Replay(_) => panic!("new operation unexpectedly replayed"),
        };
        let mut release_request = request.clone();
        release_request.release = true;
        release(
            &repository,
            &account,
            &lease,
            lease_expires_at_ms,
            &release_request,
        )
        .unwrap();
        let payloads = Arc::new(Mutex::new(PayloadSpool::default()));
        let mut input_spool = InputSpool::new(payloads);
        input_spool.append(ReadInput { address, bytes }).unwrap();
        assert_eq!(
            seal_and_page(
                input_spool,
                &repository,
                &account,
                &lease,
                lease_expires_at_ms,
                &request,
                DiscoveryProfile::default(),
                &mut owner,
            )
            .err()
            .unwrap()
            .code,
            "LIX_READ_FULFILLMENT_RESTART"
        );
        drop(owner);
        assert_eq!(
            begin(&repository, &account, &lease, lease_expires_at_ms, &request)
                .await
                .err()
                .unwrap()
                .code,
            "LIX_READ_FULFILLMENT_RESTART"
        );
        let healthy = ReadFulfillmentRequest {
            operation_id: uuid::Uuid::now_v7().to_string(),
            ..request
        };
        let admitted = match begin(&repository, &account, &lease, lease_expires_at_ms, &healthy)
            .await
            .unwrap()
        {
            BeginOperation::Owner(admitted) => admitted,
            BeginOperation::Replay(_) => panic!("fresh operation unexpectedly replayed"),
        };
        drop(admitted);
        authority.close().await.unwrap();
    }

    #[tokio::test]
    async fn waiter_cannot_readmit_an_operation_after_its_fixed_expiry() {
        use std::sync::atomic::AtomicU64;

        let authority = crate::open_lix().await.unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let repository = descriptor.lix_id.clone();
        let account = uuid::Uuid::now_v7().to_string();
        let lease = uuid::Uuid::now_v7().to_string();
        let now_ms = crate::telemetry::unix_time_ms();
        let operation_expires_at_ms = now_ms + 60_000;
        let lease_expires_at_ms = operation_expires_at_ms + 60_000;
        let bytes = b"fixed operation expiry".to_vec();
        let request = ReadFulfillmentRequest {
            operation_id: uuid::Uuid::now_v7().to_string(),
            release: false,
            operation_expires_at_ms,
            epoch_id: uuid::Uuid::now_v7().to_string(),
            descriptor,
            interests: Vec::new(),
            required: vec![ReadInputAddress::BlobChunk(
                *blake3::hash(&bytes).as_bytes(),
            )],
            continuation: None,
        };
        let registry_mutex = Mutex::new(OperationRegistry::default());
        let mut owner = match begin_inner(
            &registry_mutex,
            &repository,
            &account,
            &lease,
            lease_expires_at_ms,
            &request,
            || now_ms,
        )
        .await
        .unwrap()
        {
            BeginOperation::Owner(owner) => owner,
            BeginOperation::Replay(_) => panic!("fresh operation unexpectedly replayed"),
        };

        let fake_now = Arc::new(AtomicU64::new(now_ms));
        let waiter_now = fake_now.clone();
        let mut waiter = Box::pin(async {
            begin_inner(
                &registry_mutex,
                &repository,
                &account,
                &lease,
                lease_expires_at_ms,
                &request,
                || waiter_now.load(Ordering::Acquire),
            )
            .await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut waiter)
                .await
                .is_err()
        );

        // Wake the waiter only after the operation's immutable deadline. The
        // authenticated lease remains valid, modeling a renewed lease.
        fake_now.store(operation_expires_at_ms + 1, Ordering::Release);
        {
            let mut state = owner.operation.state.lock().unwrap();
            *state = OperationState::Cancelled(None);
        }
        owner.finished = true;
        owner.operation.changed.send_replace(());
        drop(owner);
        assert_eq!(
            waiter.await.err().unwrap().code,
            "LIX_READ_FULFILLMENT_RESTART"
        );
        assert_eq!(
            begin_inner(
                &registry_mutex,
                &repository,
                &account,
                &lease,
                lease_expires_at_ms,
                &request,
                || operation_expires_at_ms + 1,
            )
            .await
            .err()
            .unwrap()
            .code,
            "LIX_READ_FULFILLMENT_RESTART"
        );
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
