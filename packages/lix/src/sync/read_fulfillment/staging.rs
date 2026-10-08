//! Storage-native private pages for a client read operation. These coordinates
//! never belong to a normal native read, semantic snapshot, or coverage receipt.
use super::*;
mod lifecycle;
pub(crate) use lifecycle::{reap_abandoned, reap_expired};

pub(crate) const STAGING_SPACE: StorageSpace = StorageSpace::declare_private(
    StorageSpaceId(0x0007_0024),
    "sync.read_operation_scratch.v1",
    ValueSemantics::Mutable,
);
const FRAME_BYTES: usize = PAGE_PAYLOAD_BYTES;
const PROMOTION_ITEMS: usize = super::super::transfer::CONTENT_GROUP_ITEMS;
const PROMOTION_BYTES: usize = PAGE_PAYLOAD_BYTES / 2;

#[cfg(not(target_family = "wasm"))]
type RemoteOperationCleanup = Box<dyn FnOnce() -> futures_util::future::BoxFuture<'static, ()> + Send + Sync>;
#[cfg(target_family = "wasm")]
type RemoteOperationCleanup = Box<dyn FnOnce() -> futures_util::future::LocalBoxFuture<'static, ()>>;

#[derive(Clone)]
struct StagedInputRef {
    address: ReadInputAddress,
    len: usize,
    received: usize,
    digest: [u8; 32],
    frames: Vec<StorageKey>,
}

pub(crate) struct StagedClosure<S: Storage + Clone + Send + Sync + 'static> {
    storage: StorageAdapter<S>,
    state: PartialReplicaState,
    id: uuid::Uuid,
    inputs: Vec<StagedInputRef>,
    coordinates: BTreeSet<(StorageSpace, StorageKey)>,
    payload_bytes: usize,
    index_bytes: usize,
    header: ReadFulfillmentResponse,
    permit: Option<lifecycle::Permit>,
    validated: bool,
    released: bool,
    heartbeat: Option<crate::background_task::OwnedBackgroundTask>,
    remote_operation_cleanup: Option<RemoteOperationCleanup>,
    read_operation_owner: Option<super::super::http::ReadOperationOwner>,
}

/// The only authority to fold a scratch-owner reaping claim into a canonical
/// installation transaction. Instances are created by `promote` only after
/// the complete response has passed validation.
pub(super) struct ScratchOwnerFinalizeCapability {
    owner: uuid::Uuid,
}

pub(super) async fn stage_owner_reaping_fence<R: StorageAdapterRead>(
    read: &R,
    state: &PartialReplicaState,
    capability: &ScratchOwnerFinalizeCapability,
    writes: &mut StorageWriteSet,
    preconditions: &mut Vec<StoragePrecondition>,
) -> Result<(), LixError> {
    lifecycle::stage_reaping_fence(read, state, capability.owner, writes, preconditions).await
}

impl<S: Storage + Clone + Send + Sync + 'static> StagedClosure<S> {
    fn new(
        storage: &StorageAdapter<S>,
        state: &PartialReplicaState,
        mut header: ReadFulfillmentResponse,
        id: uuid::Uuid,
        permit: lifecycle::Permit,
        read_operation_owner: Option<super::super::http::ReadOperationOwner>,
    ) -> Self {
        header.inputs.clear();
        Self {
            storage: storage.clone(),
            state: state.clone(),
            id,
            inputs: Vec::new(),
            coordinates: BTreeSet::new(),
            payload_bytes: 0,
            index_bytes: 0,
            header,
            permit: Some(permit),
            validated: false,
            released: false,
            heartbeat: None,
            remote_operation_cleanup: None,
            read_operation_owner,
        }
    }

    pub(crate) fn outcome(&self) -> ReadFulfillmentOutcome {
        self.header.outcome
    }

    fn track_remote_operation_send<C>(
        &mut self,
        transport: &super::super::http::HttpSyncTransport<C>,
        request: &ReadFulfillmentRequest,
        event: super::super::http::ReadSendEvent,
        cancellation: &mut tokio::sync::oneshot::Receiver<()>,
    ) -> Result<(), LixError>
    where
        C: super::super::http::RawHttpClient + Clone + 'static,
    {
        match event {
            super::super::http::ReadSendEvent::RejectedBeforeExecution => {
                // The server proved this session rejected the request before
                // execution. Do not release through a stale session if
                // recovery fails; a replacement dispatch below installs its
                // own exact capability.
                self.remote_operation_cleanup.take();
            }
            super::super::http::ReadSendEvent::Dispatched(session_id) => {
                self.remote_operation_cleanup = Some(remote_operation_cleanup(
                    transport,
                    request,
                    session_id,
                ));
            }
        }
        if cancellation_requested(cancellation) {
            return Err(cancellation_error());
        }
        Ok(())
    }

    async fn release_scratch(&mut self) -> Result<(), LixError> {
        if self.released {
            return Ok(());
        }
        if let Some(cleanup) = self.remote_operation_cleanup.take() {
            cleanup().await;
        }
        self.release_local_scratch().await?;
        self.permit.take();
        self.read_operation_owner.take();
        Ok(())
    }

    async fn release_local_scratch(&mut self) -> Result<(), LixError> {
        if self.released {
            return Ok(());
        }
        if let Some(heartbeat) = &self.heartbeat {
            let _ = heartbeat.cancel_and_join();
        }
        self.heartbeat.take();
        lifecycle::release_until_acknowledged(self.storage.clone(), self.id).await;
        self.released = true;
        Ok(())
    }

    fn start_heartbeat(&mut self) -> Result<(), LixError> {
        let (storage, state, id) = (self.storage.clone(), self.state.clone(), self.id);
        self.heartbeat = Some(crate::background_task::spawn_owned(
            "read-operation-scratch-renewal",
            move || async move {
                loop {
                    super::super::platform::sleep(std::time::Duration::from_secs(60)).await;
                    if lifecycle::commit_frames(&storage, &state, id, &[])
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            },
        )?);
        Ok(())
    }

    async fn append_page(&mut self, page: Vec<ReadInput>) -> Result<(), LixError> {
        let page_bytes = page.iter().map(|input| input.bytes.len()).sum::<usize>();
        if page_bytes > PAGE_PAYLOAD_BYTES {
            return Err(invalid("scratch write page exceeds transport budget"));
        }
        let mut frames = Vec::new();
        let mut staged = Vec::new();
        for input in page {
            input.address.validate(&input.bytes)?;
            if !self.coordinates.insert(input.address.coordinate()?) {
                return Err(invalid("staged closure repeats an input coordinate"));
            }
            let index_bytes = serde_json::to_vec(&input.address)
                .map_err(|_| invalid("invalid staged address"))?
                .len()
                .saturating_mul(2)
                .saturating_add(256);
            if self.inputs.len() + staged.len() >= MAX_RECORDS
                || self.payload_bytes.saturating_add(input.bytes.len()) > MAX_PAYLOAD_BYTES
                || self.index_bytes.saturating_add(index_bytes) > 4 * 1024 * 1024
            {
                return Err(invalid("staged closure payload or index budget exceeded"));
            }
            let mut key = self.id.as_bytes().to_vec();
            key.extend_from_slice(&((self.inputs.len() + staged.len()) as u32).to_be_bytes());
            key.extend_from_slice(&0u32.to_be_bytes());
            let key = StorageKey(Bytes::from(key));
            let len = input.bytes.len();
            let digest = *blake3::hash(&input.bytes).as_bytes();
            self.payload_bytes += len;
            self.index_bytes += index_bytes;
            frames.push((key.clone(), Bytes::from(input.bytes)));
            staged.push(StagedInputRef {
                address: input.address,
                len,
                received: len,
                digest,
                frames: vec![key],
            });
        }
        lifecycle::commit_frames(&self.storage, &self.state, self.id, &frames).await?;
        self.inputs.extend(staged);
        Ok(())
    }

    async fn append_frame(&mut self, frame: ReadInputFrame) -> Result<(), LixError> {
        if frame.offset == 0 {
            if !self.coordinates.insert(frame.address.coordinate()?)
                || self.inputs.len() >= MAX_RECORDS
            {
                return Err(invalid("staged frame repeats an input"));
            }
            let index_bytes = serde_json::to_vec(&frame.address)
                .map_err(|_| invalid("invalid framed address"))?
                .len()
                .saturating_mul(2)
                .saturating_add(256 + frame.total_bytes.div_ceil(FRAME_BYTES) * 64);
            self.index_bytes = self.index_bytes.saturating_add(index_bytes);
            if self.index_bytes > 4 * 1024 * 1024 {
                return Err(invalid("framed scratch index exceeds quota"));
            }
            self.inputs.push(StagedInputRef {
                address: frame.address.clone(),
                len: frame.total_bytes,
                received: 0,
                digest: frame.digest,
                frames: Vec::new(),
            });
        }
        let index = self
            .inputs
            .len()
            .checked_sub(1)
            .ok_or_else(|| invalid("frame lacks initial member"))?;
        let input = &self.inputs[index];
        if input.address != frame.address
            || input.len != frame.total_bytes
            || input.digest != frame.digest
            || input.received != frame.offset
            || frame.bytes.len() > FRAME_BYTES
            || input.received.saturating_add(frame.bytes.len()) > input.len
        {
            return Err(invalid("framed scratch member changed or reordered"));
        }
        if self.payload_bytes.saturating_add(frame.bytes.len()) > MAX_PAYLOAD_BYTES {
            return Err(invalid("framed scratch payload exceeds quota"));
        }
        let mut key = self.id.as_bytes().to_vec();
        key.extend_from_slice(&(index as u32).to_be_bytes());
        key.extend_from_slice(&(input.frames.len() as u32).to_be_bytes());
        let key = StorageKey(Bytes::from(key));
        let len = frame.bytes.len();
        lifecycle::commit_frames(
            &self.storage,
            &self.state,
            self.id,
            &[(key.clone(), Bytes::from(frame.bytes))],
        )
        .await?;
        let input = &mut self.inputs[index];
        input.received += len;
        input.frames.push(key.clone());
        self.payload_bytes += len;
        Ok(())
    }

    async fn read_many(&self, indices: &[usize]) -> Result<Vec<ReadInput>, LixError> {
        let mut keys = Vec::new();
        let mut group_bytes = 0usize;
        for &index in indices {
            let input = self
                .inputs
                .get(index)
                .ok_or_else(|| invalid("staged input absent"))?;
            if input.received != input.len {
                return Err(invalid("staged member incomplete"));
            }
            group_bytes = group_bytes.saturating_add(input.len);
            keys.extend(input.frames.iter().cloned());
        }
        if group_bytes > PROMOTION_BYTES && (indices.len() != 1 && group_bytes > MAX_INPUT_BYTES) {
            return Err(invalid("staged codec group exceeds allocation budget"));
        }
        let read = self.storage.begin_read(Default::default()).await?;
        let values = read
            .get_many_bounded(
                &[StorageGetManyRequest {
                    space: STAGING_SPACE,
                    keys: &keys,
                    opts: Default::default(),
                }],
                ReadBudget {
                    max_result_bytes: group_bytes,
                    max_single_value_bytes: FRAME_BYTES,
                },
            )
            .await?
            .values;
        drop(read);
        let mut values = values.into_iter();
        let mut inputs = Vec::with_capacity(indices.len());
        for &index in indices {
            let input = &self.inputs[index];
            let mut bytes = Vec::with_capacity(input.len);
            for _ in &input.frames {
                let Some(Some(StorageProjectedValue::FullValue(frame))) = values.next() else {
                    return Err(invalid("staged frame absent"));
                };
                if frame.len() > FRAME_BYTES || bytes.len().saturating_add(frame.len()) > input.len
                {
                    return Err(invalid("staged frame exceeds declared length"));
                }
                bytes.extend_from_slice(&frame);
            }
            if bytes.len() != input.len || *blake3::hash(&bytes).as_bytes() != input.digest {
                return Err(invalid("staged member changed before validation"));
            }
            input.address.validate(&bytes)?;
            inputs.push(ReadInput {
                address: input.address.clone(),
                bytes,
            });
        }
        Ok(inputs)
    }
    fn bounded_groups(&self) -> Vec<Vec<usize>> {
        let mut groups = Vec::new();
        let mut pending = Vec::new();
        let mut bytes = 0usize;
        for (index, input) in self.inputs.iter().enumerate() {
            if !pending.is_empty()
                && (pending.len() == 32 || bytes.saturating_add(input.len) > PROMOTION_BYTES)
            {
                groups.push(std::mem::take(&mut pending));
                bytes = 0;
            }
            pending.push(index);
            bytes += input.len;
        }
        if !pending.is_empty() {
            groups.push(pending);
        }
        groups
    }

    async fn validate(&mut self, request: &ReadFulfillmentRequest) -> Result<(), LixError> {
        if self.header.lix_id != request.descriptor.lix_id
            || self.header.epoch_id != request.epoch_id
            || self.header.request_digest != request.digest()?
            || self.header.continuation.is_some()
        {
            return Err(invalid(
                "staged read closure admission or terminal page differs",
            ));
        }
        let mut digest = blake3::Hasher::new();
        digest.update(request.digest()?.as_bytes());
        let mut proof_inputs = Vec::new();
        let mut proof_bytes = 0usize;
        for group in self.bounded_groups() {
            for input in self.read_many(&group).await? {
                let address = serde_json::to_vec(&input.address)
                    .map_err(|_| invalid("invalid staged address"))?;
                digest.update(&(address.len() as u64).to_be_bytes());
                digest.update(&address);
                digest.update(&(input.bytes.len() as u64).to_be_bytes());
                digest.update(&input.bytes);
                if matches!(
                    &input.address,
                    ReadInputAddress::ChangeRecord { .. }
                        | ReadInputAddress::Metadata(NativeMetadataRef::ChangeLocator(_))
                ) {
                    let bytes = if proof_requires_payload(&input.address) {
                        input.bytes
                    } else {
                        Vec::new()
                    };
                    let (fact, resident_bytes) = proof_fact(input.address, bytes)?;
                    proof_bytes += resident_bytes;
                    if proof_bytes > 4 * 1024 * 1024 {
                        return Err(invalid("staged closure proof facts exceed byte budget"));
                    }
                    proof_inputs.push(fact);
                }
            }
        }
        if digest.finalize().to_hex().as_str() != self.header.closure_digest {
            return Err(invalid("staged read closure commitment differs"));
        }
        for required in &request.required {
            if !self.inputs.iter().any(|input| &input.address == required) {
                return Err(invalid("staged closure omitted a required input"));
            }
        }
        validate_payload_membership(request, &proof_inputs)?;
        self.validated = true;
        Ok(())
    }

    pub(crate) async fn promote(
        &mut self,
        request: &ReadFulfillmentRequest,
        immutable_only: bool,
    ) -> Result<super::super::runtime::HydratedInputs, LixError> {
        if request.epoch_id != self.state.epoch_id()
            || (!immutable_only && request.descriptor != *self.state.descriptor())
        {
            return Err(invalid("read fulfillment basis changed before promotion"));
        }
        if self.outcome() != ReadFulfillmentOutcome::Complete {
            return Err(invalid("fallback cannot publish read coverage"));
        }
        if !self.validated {
            self.validate(request).await?;
        }
        // Owner and selected-payload groups are whole validation units, even
        // when their records arrived on different transport pages.
        let mut owner_groups = BTreeMap::<String, Vec<usize>>::new();
        let mut ordinary = Vec::new();
        let selected = self
            .inputs
            .iter()
            .filter_map(|input| match &input.address {
                ReadInputAddress::ChangeRecord { change_id, .. } => Some(change_id.clone()),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        for (index, input) in self.inputs.iter().enumerate() {
            let group = match &input.address {
                ReadInputAddress::ChangeRecord { change_id, .. } => {
                    Some(format!("change:{change_id}"))
                }
                ReadInputAddress::Metadata(NativeMetadataRef::ChangeLocator(id))
                    if selected.contains(id) =>
                {
                    Some(format!("change:{id}"))
                }
                address => owner_commit_id(address).map(|id| format!("owner:{id}")),
            };
            if let Some(group) = group {
                owner_groups.entry(group).or_default().push(index);
            } else {
                ordinary.push(index);
            }
        }
        let mut groups = owner_groups.into_values().collect::<Vec<_>>();
        let mut page = Vec::new();
        let mut bytes = 0usize;
        for index in ordinary {
            let next = self.inputs[index].len;
            if !page.is_empty()
                && (bytes.saturating_add(next) > PROMOTION_BYTES || page.len() == 32)
            {
                groups.push(std::mem::take(&mut page));
                bytes = 0;
            }
            page.push(index);
            bytes += next;
        }
        if !page.is_empty() {
            groups.push(page);
        }
        // Refuse an oversized indivisible owner before publishing any group.
        validate_promotion_unit_sizes(&groups, &self.inputs)?;
        let groups = pack_promotion_groups(groups, &self.inputs)?;
        if immutable_only
            && self
                .inputs
                .iter()
                .any(|input| matches!(&input.address, ReadInputAddress::ChangeRecord { .. }))
        {
            return Err(invalid(
                "candidate immutable closure contains selected change records",
            ));
        }
        let mut hydrated = super::super::runtime::HydratedInputs::default();
        let mut groups = groups.into_iter().peekable();
        while let Some(group) = groups.next() {
            let group_bytes = group
                .iter()
                .map(|&index| self.inputs[index].len)
                .sum::<usize>();
            if group_bytes > MAX_INPUT_BYTES {
                return Err(invalid("native owner bundle exceeds its codec budget"));
            }
            let inputs = self.read_many(&group).await?;
            let mut response = self.header.clone();
            response.inputs = inputs;
            let finalize = groups.peek().is_none().then_some(
                ScratchOwnerFinalizeCapability { owner: self.id },
            );
            let installed = install_inputs(
                &self.storage,
                &self.state,
                request,
                &response,
                immutable_only,
                finalize.as_ref(),
            )
            .await?;
            hydrated.keys.extend(installed.keys);
            hydrated.blob_manifests.extend(installed.blob_manifests);
        }
        if self.inputs.is_empty() {
            lifecycle::finalize_empty(&self.storage, &self.state, self.id).await?;
        }
        // A fully promoted operation needs no foreground release request.
        // The server operation is already terminal; disarming here preserves
        // the existing one-pass success latency.
        self.remote_operation_cleanup.take();
        self.release_scratch().await?;
        Ok(hydrated)
    }
}

/// Co-pack independent validation units without splitting an owner bundle or
/// one of the existing ordinary codec pages. A unit that exceeds this normal
/// lane remains a standalone codec operation, up to the existing per-unit
/// allocation bound checked by `promote` above.
fn pack_promotion_groups(
    units: Vec<Vec<usize>>,
    inputs: &[StagedInputRef],
) -> Result<Vec<Vec<usize>>, LixError> {
    use super::super::transfer::TransferBatch;

    fn new_batch() -> TransferBatch<Vec<usize>> {
        TransferBatch::with_limits(PROMOTION_ITEMS, PROMOTION_BYTES, PROMOTION_BYTES)
    }

    fn flatten_units(units: Vec<Vec<usize>>) -> Vec<usize> {
        units.into_iter().flatten().collect()
    }

    let mut batches = Vec::new();
    let mut pending = new_batch();
    for unit in units {
        let (encoded, decoded) = promotion_unit_weights(&unit, inputs)?;
        let item_count = unit.len();
        let indivisible_oversize = item_count > PROMOTION_ITEMS
            || encoded.saturating_add(2) > PROMOTION_BYTES
            || decoded > PROMOTION_BYTES;
        if indivisible_oversize {
            if !pending.items.is_empty() {
                batches.push(flatten_units(std::mem::take(&mut pending.items)));
                pending = new_batch();
            }
            batches.push(unit);
            continue;
        }

        if let Some(unit) = pending.push_counted(unit, item_count, encoded, decoded)? {
            batches.push(flatten_units(std::mem::take(&mut pending.items)));
            pending = new_batch();
            if pending
                .push_counted(unit, item_count, encoded, decoded)?
                .is_some()
            {
                return Err(invalid("promotion unit did not fit an empty batch"));
            }
        }
    }
    if !pending.items.is_empty() {
        batches.push(flatten_units(pending.items));
    }
    Ok(batches)
}

fn promotion_unit_weights(
    unit: &[usize],
    inputs: &[StagedInputRef],
) -> Result<(usize, usize), LixError> {
    let mut encoded = 0usize;
    let mut decoded = 0usize;
    for &index in unit {
        let input = inputs
            .get(index)
            .ok_or_else(|| invalid("promotion unit references an absent input"))?;
        let address_bytes = serde_json::to_vec(&input.address)
            .map_err(|_| invalid("invalid promotion address"))?
            .len();
        // Read fulfillment uses JSON/base64 on the wire. Include its address
        // and small envelope overhead while bounding raw decoded bytes too.
        encoded = encoded
            .saturating_add(address_bytes)
            .saturating_add(input.len.div_ceil(3).saturating_mul(4))
            .saturating_add(32);
        decoded = decoded.saturating_add(input.len);
    }
    Ok((encoded, decoded))
}

fn validate_promotion_unit_sizes(
    units: &[Vec<usize>],
    inputs: &[StagedInputRef],
) -> Result<(), LixError> {
    if units.iter().any(|unit| {
        unit.iter()
            .filter_map(|&index| inputs.get(index))
            .map(|input| input.len)
            .sum::<usize>()
            > MAX_INPUT_BYTES
    }) {
        return Err(invalid(
            "native owner bundle exceeds codec allocation budget",
        ));
    }
    Ok(())
}

impl<S: Storage + Clone + Send + Sync + 'static> Drop for StagedClosure<S> {
    fn drop(&mut self) {
        self.heartbeat.take();
        if self.released {
            return;
        }
        let storage = self.storage.clone();
        let id = self.id;
        let permit = Arc::new(Mutex::new(self.permit.take()));
        let worker_permit = Arc::clone(&permit);
        let remote_operation_cleanup = self.remote_operation_cleanup.take();
        let read_operation_owner = self.read_operation_owner.take();
        let worker_read_operation_owner =
            Arc::new(Mutex::new(read_operation_owner));
        let deferred_read_operation_owner = Arc::clone(&worker_read_operation_owner);
        let queued = crate::background_task::spawn_runtime_compatible(
            "read-operation-scratch-cleanup",
            move || async move {
                if let Some(cleanup) = remote_operation_cleanup {
                    cleanup().await;
                }
                lifecycle::release_until_acknowledged(storage, id).await;
                worker_permit
                    .lock()
                    .expect("scratch cleanup permit lock is not poisoned")
                    .take();
                deferred_read_operation_owner
                    .lock()
                    .expect("read operation owner lock is not poisoned")
                    .take();
            },
        );
        if queued.is_err()
            && let Some(permit) = permit
                .lock()
                .expect("scratch cleanup permit lock is not poisoned")
                .take()
        {
            // Fail closed if no executor can own cleanup: do not advertise
            // process capacity while this owner's durable state is unknown.
            std::mem::forget(permit);
        }
        if queued.is_err()
            && let Some(owner) = worker_read_operation_owner
                .lock()
                .expect("read operation owner lock is not poisoned")
                .take()
        {
            // Session deletion must remain behind remote and durable cleanup.
            // Keep the capability alive if no executor can own that work.
            std::mem::forget(owner);
        }
    }
}

/// A lost sealed-operation cursor can restart discovery against the same
/// immutable leased basis. Scratch from the failed attempt is removed before
/// another admission; repeated expiry remains a bounded error.
pub(crate) async fn fetch_staged<S, C>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    transport: &super::super::http::HttpSyncTransport<C>,
    request: &ReadFulfillmentRequest,
) -> Result<StagedClosure<S>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: super::super::http::RawHttpClient + Clone + 'static,
{
    fetch_staged_with_permit(
        storage,
        state,
        transport,
        request,
        lifecycle::Permit::acquire()?,
    )
    .await
}

async fn fetch_staged_with_permit<S, C>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    transport: &super::super::http::HttpSyncTransport<C>,
    request: &ReadFulfillmentRequest,
    owner_permit: lifecycle::Permit,
) -> Result<StagedClosure<S>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: super::super::http::RawHttpClient + Clone + 'static,
{
    let (result_sender, result_receiver) = tokio::sync::oneshot::channel();
    let (cancel_sender, cancel_receiver) = tokio::sync::oneshot::channel();
    let mut cancellation = FetchCancellation(Some(cancel_sender));
    let owner_id = uuid::Uuid::now_v7();
    let read_operation_owner = transport.acquire_read_operation_owner()?;
    let storage = storage.clone();
    let state = state.clone();
    let transport = transport.clone();
    let request = request.clone();
    crate::background_task::spawn_runtime_compatible(
        "read-operation-staged-fetch",
        move || async move {
            let result = fetch_staged_owned(
                &storage,
                &state,
                &transport,
                &request,
                (owner_id, owner_permit),
                read_operation_owner,
                cancel_receiver,
            )
            .await;
            let _ = result_sender.send(result);
        },
    )?;

    match result_receiver.await {
        Ok(result) => {
            cancellation.disarm();
            result
        }
        Err(_) => {
            cancellation.disarm();
            Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "staged fetch owner ended before returning a result",
            ))
        }
    }
}

struct FetchCancellation(Option<tokio::sync::oneshot::Sender<()>>);

impl FetchCancellation {
    fn disarm(&mut self) {
        self.0.take();
    }
}

impl Drop for FetchCancellation {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

fn cancellation_requested(receiver: &mut tokio::sync::oneshot::Receiver<()>) -> bool {
    matches!(
        receiver.try_recv(),
        Ok(()) | Err(tokio::sync::oneshot::error::TryRecvError::Closed)
    )
}

fn cancellation_error() -> LixError {
    LixError::new("LIX_READ_FULFILLMENT_CANCELED", "staged fetch was canceled")
}

struct StagedFetchAttemptError {
    error: LixError,
    remote_operation_cleanup: Option<RemoteOperationCleanup>,
    permit: Option<lifecycle::Permit>,
}

impl<S: Storage + Clone + Send + Sync + 'static> StagedClosure<S> {
    async fn into_attempt_error(&mut self, error: LixError) -> StagedFetchAttemptError {
        // The scratch ledger owns only private client frames, so this attempt
        // may acknowledge their deletion before the driver releases the
        // remote operation. The returned process permit stays with the
        // driver's retry/error state, and its outer ReadOperationOwner stays
        // alive until that remote cleanup (or same-ID network retry) finishes.
        let remote_operation_cleanup = self.remote_operation_cleanup.take();
        let _ = self.release_local_scratch().await;
        StagedFetchAttemptError {
            error,
            remote_operation_cleanup,
            permit: self.permit.take(),
        }
    }
}

impl From<LixError> for StagedFetchAttemptError {
    fn from(error: LixError) -> Self {
        Self {
            error,
            remote_operation_cleanup: None,
            permit: None,
        }
    }
}

async fn fetch_staged_owned<S, C>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    transport: &super::super::http::HttpSyncTransport<C>,
    request: &ReadFulfillmentRequest,
    initial_owner: (uuid::Uuid, lifecycle::Permit),
    read_operation_owner: super::super::http::ReadOperationOwner,
    mut cancellation: tokio::sync::oneshot::Receiver<()>,
) -> Result<StagedClosure<S>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: super::super::http::RawHttpClient + Clone + 'static,
{
    let mut attempt_request = request.clone();
    let mut network_retry_used = false;
    let mut operation_restart_used = false;
    let mut reusable_permit = Some(initial_owner.1);
    let mut next_owner_id = initial_owner.0;
    let mut retry_remote_cleanup: Option<RemoteOperationCleanup> = None;
    for _ in 0..3 {
        if cancellation_requested(&mut cancellation) {
            if let Some(cleanup) = retry_remote_cleanup.take() {
                cleanup().await;
            }
            drop(reusable_permit.take());
            return Err(cancellation_error());
        }
        match fetch_staged_once(
            storage,
            state,
            transport,
            &attempt_request,
            reusable_permit
                .take()
                .map(|permit| (next_owner_id, permit)),
            read_operation_owner.clone(),
            &mut cancellation,
        )
        .await
        {
            Ok(mut stage) => {
                if cancellation_requested(&mut cancellation) {
                    let _ = stage.release_scratch().await;
                    if let Some(cleanup) = retry_remote_cleanup.take() {
                        cleanup().await;
                    }
                    return Err(cancellation_error());
                }
                // A prior same-ID network attempt may have reached the same
                // server operation. The successful attempt now owns its
                // terminal cleanup capability, so discard the older unpolled
                // duplicate without sending a release request.
                retry_remote_cleanup.take();
                return Ok(stage);
            }
            Err(mut failure)
                if failure.error.code == "LIX_TRANSPORT_NETWORK" && !network_retry_used =>
            {
                if cancellation_requested(&mut cancellation) {
                    if let Some(cleanup) = failure
                        .remote_operation_cleanup
                        .take()
                        .or_else(|| retry_remote_cleanup.take())
                    {
                        cleanup().await;
                    }
                    drop(failure.permit.take());
                    drop(retry_remote_cleanup.take());
                    drop(reusable_permit.take());
                    return Err(cancellation_error());
                }
                if let Some(cleanup) = failure.remote_operation_cleanup.take() {
                    retry_remote_cleanup = Some(cleanup);
                }
                reusable_permit = failure.permit.take();
                next_owner_id = uuid::Uuid::now_v7();
                network_retry_used = true;
            }
            Err(mut failure)
                if failure.error.code == "LIX_READ_FULFILLMENT_RESTART"
                    && !operation_restart_used =>
            {
                if cancellation_requested(&mut cancellation) {
                    if let Some(cleanup) = failure
                        .remote_operation_cleanup
                        .take()
                        .or_else(|| retry_remote_cleanup.take())
                    {
                        cleanup().await;
                    }
                    drop(failure.permit.take());
                    drop(retry_remote_cleanup.take());
                    drop(reusable_permit.take());
                    return Err(cancellation_error());
                }
                if let Some(cleanup) = failure
                    .remote_operation_cleanup
                    .take()
                    .or_else(|| retry_remote_cleanup.take())
                {
                    cleanup().await;
                }
                drop(retry_remote_cleanup.take());
                reusable_permit = failure.permit.take().or_else(|| reusable_permit.take());
                attempt_request.operation_id = uuid::Uuid::now_v7().to_string();
                next_owner_id = uuid::Uuid::now_v7();
                operation_restart_used = true;
            }
            Err(mut failure) => {
                if let Some(cleanup) = failure
                    .remote_operation_cleanup
                    .take()
                    .or_else(|| retry_remote_cleanup.take())
                {
                    cleanup().await;
                }
                drop(failure.permit.take());
                drop(retry_remote_cleanup.take());
                drop(reusable_permit.take());
                return Err(failure.error);
            }
        }
    }
    unreachable!("bounded staged fetch always returns on its third attempt")
}

fn remote_operation_cleanup<C>(
    transport: &super::super::http::HttpSyncTransport<C>,
    request: &ReadFulfillmentRequest,
    session_id: String,
) -> RemoteOperationCleanup
where
    C: super::super::http::RawHttpClient + Clone + 'static,
{
    let transport = transport.clone();
    let mut release = request.clone();
    release.release = true;
    release.continuation = None;
    Box::new(move || {
        Box::pin(async move {
            // Release is idempotent for the same operation identity. Retry one
            // ambiguous network failure so a request lost before authority
            // acceptance does not retain a sealed spool until lease expiry.
            for attempt in 0..2 {
                match transport.release_read_operation(&release, &session_id).await {
                    Ok(_) => return,
                    Err(error) if attempt == 0 && error.code == "LIX_TRANSPORT_NETWORK" => {}
                    Err(_) => return,
                }
            }
        })
    })
}

async fn fetch_staged_once<S, C>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    transport: &super::super::http::HttpSyncTransport<C>,
    request: &ReadFulfillmentRequest,
    owner: Option<(uuid::Uuid, lifecycle::Permit)>,
    read_operation_owner: super::super::http::ReadOperationOwner,
    cancellation: &mut tokio::sync::oneshot::Receiver<()>,
) -> Result<StagedClosure<S>, StagedFetchAttemptError>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: super::super::http::RawHttpClient + Clone + 'static,
{
    let header = ReadFulfillmentResponse {
        frame: None,
        lix_id: request.descriptor.lix_id.clone(),
        epoch_id: request.epoch_id.clone(),
        request_digest: request.digest()?,
        inputs: Vec::new(),
        profile: DiscoveryProfile::default(),
        closure_digest: input_digest(request, &[])?,
        continuation: None,
        outcome: ReadFulfillmentOutcome::Complete,
    };
    // The detached owner task keeps the reservation/commit future alive until
    // it receives a definite result, even if its caller drops the future.
    // Both process capacity and durable ownership still precede HTTP work.
    let (id, permit) = match owner {
        Some((id, permit)) => match lifecycle::reserve_owned(storage, state, id, permit).await {
            Ok(reserved) => reserved,
            Err((error, permit)) => {
                return Err(StagedFetchAttemptError {
                    error,
                    remote_operation_cleanup: None,
                    permit: Some(permit),
                });
            }
        },
        None => lifecycle::reserve(storage, state).await?,
    };
    let mut stage = StagedClosure::new(
        storage,
        state,
        header,
        id,
        permit,
        Some(read_operation_owner),
    );
    if cancellation_requested(cancellation) {
        return Err(stage.into_attempt_error(cancellation_error()).await);
    }
    if let Err(error) = stage.start_heartbeat() {
        return Err(stage.into_attempt_error(error).await);
    }
    let mut page_request = request.clone();
    let mut page = match transport
        .fulfill_read_tracked(&page_request, |event| {
            // The first raw dispatch makes cancellation authority necessary.
            // A canonical SESSION_GONE proves the authority rejected that
            // attempt before execution, so discard it before recovery can
            // fail or the replacement session can be closed.
            stage.track_remote_operation_send(transport, request, event, cancellation)
        })
        .await
    {
        Ok(page) => page,
        Err(error) => {
            return Err(stage.into_attempt_error(error).await);
        }
    };
    if cancellation_requested(cancellation) {
        return Err(stage.into_attempt_error(cancellation_error()).await);
    }
    stage.header = page.clone();
    stage.header.inputs.clear();
    stage.header.frame = None;
    if page.outcome != ReadFulfillmentOutcome::Complete {
        if let Err(error) = validate_complete(request, &page) {
            return Err(stage.into_attempt_error(error).await);
        }
        // A validated fallback response is terminal: the authority removed
        // its pending operation. Releasing it again would create a tombstone.
        stage.remote_operation_cleanup.take();
        stage.release_scratch().await?;
        return Ok(stage);
    }
    let result = async {
        for _ in 0..MAX_PAGES {
            if cancellation_requested(cancellation) {
                return Err(cancellation_error());
            }
            validate_response(&page_request, &page)?;
            if page.closure_digest != stage.header.closure_digest
                || page.outcome != ReadFulfillmentOutcome::Complete
            {
                return Err(invalid(
                    "staged page changed its operation commitment or outcome",
                ));
            }
            let next = page.continuation.take();
            if let Some(frame) = page.frame.take() {
                stage.append_frame(frame).await?;
            } else {
                stage.append_page(page.inputs).await?;
            }
            if cancellation_requested(cancellation) {
                return Err(cancellation_error());
            }
            let Some(next) = next else {
                stage.header.continuation = None;
                stage.validate(request).await?;
                if cancellation_requested(cancellation) {
                    return Err(cancellation_error());
                }
                return Ok(());
            };
            page_request.continuation = Some(next);
            page = transport
                .fulfill_read_tracked(&page_request, |event| {
                    stage.track_remote_operation_send(transport, request, event, cancellation)
                })
                .await?;
            if cancellation_requested(cancellation) {
                return Err(cancellation_error());
            }
        }
        Err(invalid("staged read continuation count exceeds bound"))
    }
    .await;
    if let Err(error) = result {
        return Err(stage.into_attempt_error(error).await);
    }
    Ok(stage)
}

#[cfg(test)]
mod tests;
