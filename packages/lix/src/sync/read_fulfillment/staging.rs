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
    /// Present only while the nonblocking transfer-wide RAM permit is held;
    /// entries align by index with `inputs` and own their decoded payloads.
    retained_inputs: Option<Vec<ReadInput>>,
    retained_payload_bytes: usize,
    retained_payload_permit: Option<super::super::transfer::RetainedPayloadPermit>,
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
        retained_payload_permit: Option<super::super::transfer::RetainedPayloadPermit>,
        read_operation_owner: Option<super::super::http::ReadOperationOwner>,
    ) -> Self {
        header.inputs.clear();
        Self {
            storage: storage.clone(),
            state: state.clone(),
            id,
            inputs: Vec::new(),
            retained_inputs: retained_payload_permit.as_ref().map(|_| Vec::new()),
            retained_payload_bytes: 0,
            retained_payload_permit,
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
        Ok(())
    }

    async fn release_scratch(&mut self) -> Result<(), LixError> {
        if self.released {
            return Ok(());
        }
        self.retained_inputs.take();
        self.retained_payload_bytes = 0;
        self.retained_payload_permit.take();
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
        let page_bytes = page
            .iter()
            .try_fold(0usize, |total, input| total.checked_add(input.bytes.len()))
            .ok_or_else(|| invalid("scratch write page size overflowed"))?;
        if page_bytes > PAGE_PAYLOAD_BYTES {
            return Err(invalid("scratch write page exceeds transport budget"));
        }

        let mut staged = Vec::with_capacity(page.len());
        let mut page_coordinates = BTreeSet::new();
        for input in &page {
            input.address.validate(&input.bytes)?;
            let coordinate = input.address.coordinate()?;
            if self.coordinates.contains(&coordinate) || !page_coordinates.insert(coordinate) {
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
            let len = input.bytes.len();
            self.payload_bytes += len;
            self.index_bytes += index_bytes;
            staged.push(StagedInputRef {
                address: input.address.clone(),
                len,
                received: len,
                // Retained inputs are validated against the terminal closure
                // commitment before promotion. Compute a per-input digest only
                // if they later spill to durable scratch.
                digest: [0; 32],
                frames: Vec::new(),
            });
        }

        self.coordinates.extend(page_coordinates);
        if self.retained_inputs.is_some()
            && self
                .retained_payload_bytes
                .checked_add(page_bytes)
                .is_some_and(|total| {
                    total <= super::super::transfer::RETAINED_READ_CLOSURE_BYTES
                })
        {
            self.retained_payload_bytes += page_bytes;
            self.inputs.extend(staged);
            self.retained_inputs
                .as_mut()
                .expect("retained payload mode has a payload vector")
                .extend(page);
            return Ok(());
        }

        // The retained closure is always at most one page, so it can be
        // atomically spilled through the same owner-fenced scratch writer.
        // Release the process permit only after that write is acknowledged.
        if self.retained_inputs.is_some() {
            self.spill_retained().await?;
        }
        self.append_page_to_scratch(page, staged).await
    }

    async fn append_page_to_scratch(
        &mut self,
        page: Vec<ReadInput>,
        mut staged: Vec<StagedInputRef>,
    ) -> Result<(), LixError> {
        let page_bytes = page.iter().map(|input| input.bytes.len()).sum::<usize>();
        if page_bytes > PAGE_PAYLOAD_BYTES || page.len() != staged.len() {
            return Err(invalid("scratch write page exceeds transport budget"));
        }
        let mut frames = Vec::with_capacity(page.len());
        for (offset, input) in page.into_iter().enumerate() {
            let index = self.inputs.len() + offset;
            if staged[offset].address != input.address
                || staged[offset].len != input.bytes.len()
            {
                return Err(invalid("scratch page metadata changed before staging"));
            }
            staged[offset].digest = *blake3::hash(&input.bytes).as_bytes();
            let mut key = self.id.as_bytes().to_vec();
            key.extend_from_slice(&(index as u32).to_be_bytes());
            key.extend_from_slice(&0u32.to_be_bytes());
            let key = StorageKey(Bytes::from(key));
            staged[offset].frames = vec![key.clone()];
            frames.push((key, Bytes::from(input.bytes)));
        }
        lifecycle::commit_frames(&self.storage, &self.state, self.id, &frames).await?;
        self.inputs.extend(staged);
        Ok(())
    }

    async fn spill_retained(&mut self) -> Result<(), LixError> {
        let Some(retained) = self.retained_inputs.as_ref() else {
            return Ok(());
        };
        self.validate_retained_metadata(retained)?;
        let inputs = self
            .retained_inputs
            .take()
            .expect("retained inputs were checked above");
        if inputs.is_empty() {
            self.retained_payload_bytes = 0;
            self.retained_payload_permit.take();
            return Ok(());
        }
        let staged = std::mem::take(&mut self.inputs);
        self.append_page_to_scratch(inputs, staged).await?;
        self.retained_payload_bytes = 0;
        self.retained_payload_permit.take();
        Ok(())
    }

    fn validate_retained_metadata(&self, inputs: &[ReadInput]) -> Result<(), LixError> {
        let mut payload_bytes = 0usize;
        if inputs.len() != self.inputs.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "retained read closure has the wrong input count",
            ));
        }
        for (input, staged) in inputs.iter().zip(&self.inputs) {
            payload_bytes = payload_bytes
                .checked_add(input.bytes.len())
                .ok_or_else(|| invalid("retained read closure size overflowed"))?;
            if input.address != staged.address
                || input.bytes.len() != staged.len
                || staged.received != staged.len
                || !staged.frames.is_empty()
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "retained read closure metadata is inconsistent",
                ));
            }
        }
        if payload_bytes != self.retained_payload_bytes
            || payload_bytes > super::super::transfer::RETAINED_READ_CLOSURE_BYTES
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "retained read closure exceeds its memory budget",
            ));
        }
        Ok(())
    }

    async fn append_frame(&mut self, frame: ReadInputFrame) -> Result<(), LixError> {
        if self.retained_inputs.is_some() {
            self.spill_retained().await?;
        }
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
        if let Some(inputs) = self.retained_inputs.as_ref() {
            self.validate_retained_metadata(inputs)?;
            let inputs = self
                .retained_inputs
                .take()
                .expect("retained inputs were checked above");
            self.header.inputs = inputs;
            let result = validate_complete(request, &self.header);
            self.retained_inputs = Some(std::mem::take(&mut self.header.inputs));
            result?;
            self.validated = true;
            return Ok(());
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
            let finalize = groups.peek().is_none().then_some(
                ScratchOwnerFinalizeCapability { owner: self.id },
            );
            let installed = if let Some(retained) = self.retained_inputs.as_ref() {
                let inputs = group
                    .iter()
                    .map(|&index| {
                        retained
                            .get(index)
                            .ok_or_else(|| invalid("retained staged input is absent"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                install_inputs_from_refs(
                    &self.storage,
                    &self.state,
                    request,
                    &inputs,
                    immutable_only,
                    finalize.as_ref(),
                )
                .await?
            } else {
                let owned = self.read_many(&group).await?;
                let inputs = owned.iter().collect::<Vec<_>>();
                install_inputs_from_refs(
                    &self.storage,
                    &self.state,
                    request,
                    &inputs,
                    immutable_only,
                    finalize.as_ref(),
                )
                .await?
            };
            hydrated.keys.extend(installed.keys);
            hydrated.blob_manifests.extend(installed.blob_manifests);
        }
        self.retained_inputs.take();
        self.retained_payload_bytes = 0;
        self.retained_payload_permit.take();
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
    // Production capacity is decided before any HTTP dispatch. Unit tests
    // inject the permit explicitly so concurrent tests stay deterministic.
    #[cfg(test)]
    let retained_payload_permit = None;
    #[cfg(not(test))]
    let retained_payload_permit =
        super::super::transfer::RetainedPayloadPermit::try_acquire();
    fetch_staged_with_permits(
        storage,
        state,
        transport,
        request,
        lifecycle::Permit::acquire()?,
        retained_payload_permit,
    )
    .await
}

#[cfg(test)]
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
    fetch_staged_with_permits(storage, state, transport, request, owner_permit, None).await
}

#[cfg(test)]
async fn fetch_staged_with_retained_payload_permit<S, C>(
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
    let retained = super::super::transfer::RetainedPayloadPermit::try_acquire()
        .expect("retained payload test owns the bounded slot");
    fetch_staged_with_permits(storage, state, transport, request, owner_permit, Some(retained))
        .await
}

async fn fetch_staged_with_permits<S, C>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    transport: &super::super::http::HttpSyncTransport<C>,
    request: &ReadFulfillmentRequest,
    owner_permit: lifecycle::Permit,
    retained_payload_permit: Option<super::super::transfer::RetainedPayloadPermit>,
) -> Result<StagedClosure<S>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: super::super::http::RawHttpClient + Clone + 'static,
{
    start_staged_fetch_with_permits(
        storage,
        state,
        transport,
        request,
        owner_permit,
        retained_payload_permit,
    )?
    .wait()
    .await
}

fn start_staged_fetch_with_permits<S, C>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    transport: &super::super::http::HttpSyncTransport<C>,
    request: &ReadFulfillmentRequest,
    owner_permit: lifecycle::Permit,
    retained_payload_permit: Option<super::super::transfer::RetainedPayloadPermit>,
) -> Result<StagedFetchHandle<S>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: super::super::http::RawHttpClient + Clone + 'static,
{
    let (result_sender, result_receiver) = tokio::sync::oneshot::channel();
    let (cancel_sender, cancel_receiver) = tokio::sync::oneshot::channel();
    let owner_id = uuid::Uuid::now_v7();
    let read_operation_owner = transport.acquire_read_operation_owner()?;
    let storage = storage.clone();
    let state = state.clone();
    let transport = transport.clone();
    let request = request.clone();
    if let Err(error) = crate::background_task::spawn_runtime_compatible(
        "read-operation-staged-fetch",
        move || async move {
            let result = fetch_staged_owned(
                &storage,
                &state,
                &transport,
                &request,
                (owner_id, owner_permit),
                retained_payload_permit,
                read_operation_owner,
                cancel_receiver,
            )
            .await;
            let _ = result_sender.send(result);
        },
    ) {
        return Err(error);
    }
    Ok(StagedFetchHandle {
        result: Some(result_receiver),
        cancellation: Some(cancel_sender),
        cleanup: None,
        finished: false,
    })
}

/// A staged network fetch whose owner can outlive an awaiting candidate pass.
/// Dropping a waiter does not cancel this handle; the one transfer owner is
/// canceled and joined only when the enclosing reconciliation target is
/// invalidated.
pub(crate) struct StagedFetchHandle<S: Storage + Clone + Send + Sync + 'static> {
    result: Option<tokio::sync::oneshot::Receiver<Result<StagedClosure<S>, LixError>>>,
    cancellation: Option<tokio::sync::oneshot::Sender<()>>,
    cleanup: Option<tokio::sync::oneshot::Receiver<Result<(), LixError>>>,
    finished: bool,
}

impl<S: Storage + Clone + Send + Sync + 'static> StagedFetchHandle<S> {
    async fn wait(&mut self) -> Result<StagedClosure<S>, LixError> {
        let result = self
            .result
            .as_mut()
            .expect("staged fetch result is consumed once")
            .await;
        self.result.take();
        self.cancellation.take();
        match result {
            Ok(result) => result,
            Err(_) => Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "staged fetch owner ended before returning a result",
            )),
        }
    }

    async fn cancel_and_wait(&mut self) -> Result<(), LixError> {
        if let Some(cancellation) = self.cancellation.take() {
            let _ = cancellation.send(());
        }
        if let Some(result) = self.result.as_mut() {
            let result = result.await;
            self.result.take();
            match result {
                Ok(Ok(staged)) => match start_staged_cleanup(staged) {
                    Ok(cleanup) => self.cleanup = Some(cleanup),
                    Err(error) => {
                        self.finished = true;
                        return Err(error);
                    }
                },
                Ok(Err(_)) | Err(_) => self.finished = true,
            }
        }
        if let Some(cleanup) = self.cleanup.as_mut() {
            let result = cleanup.await;
            self.cleanup.take();
            self.finished = true;
            return result.unwrap_or_else(|_| {
                Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "staged cleanup owner ended before acknowledging release",
                ))
            });
        }
        Ok(())
    }

    fn finished(&self) -> bool {
        self.finished
    }
}

impl<S: Storage + Clone + Send + Sync + 'static> Drop for StagedFetchHandle<S> {
    fn drop(&mut self) {
        if let Some(cancellation) = self.cancellation.take() {
            let _ = cancellation.send(());
        }
    }
}

struct CandidateTransferTarget {
    source: PartialReplicaState,
    descriptor: super::super::partial_replica::PartialReplicaDescriptor,
    lease: crate::gc::NativeBaselineLease,
    deadline: super::super::http::CandidateBaselineDeadline,
}

impl CandidateTransferTarget {
    fn matches(
        &self,
        source: &PartialReplicaState,
        target: &PartialReplicaState,
        deadline: &super::super::http::CandidateBaselineDeadline,
    ) -> bool {
        self.source == *source
            && self.descriptor == *target.descriptor()
            && self.lease == *target.baseline_lease()
            && self.deadline.same_window(deadline)
    }
}

pub(crate) enum CandidateTransferResult {
    Ready,
    Fallback(ReadFulfillmentOutcome),
}

/// One candidate's immutable transfer can outlive an interrupted candidate
/// evaluation, but only while the source admission and exact leased target
/// remain unchanged. This owns no candidate read scope or prepared writes.
pub(crate) struct CandidateReadTransfer<S: Storage + Clone + Send + Sync + 'static> {
    target: Option<CandidateTransferTarget>,
    request_digest: Option<String>,
    immutable_only: Option<bool>,
    fetch: Option<StagedFetchHandle<S>>,
    staged: Option<StagedClosure<S>>,
    cleanup: Option<tokio::sync::oneshot::Receiver<Result<(), LixError>>>,
}

impl<S: Storage + Clone + Send + Sync + 'static> Default for CandidateReadTransfer<S> {
    fn default() -> Self {
        Self {
            target: None,
            request_digest: None,
            immutable_only: None,
            fetch: None,
            staged: None,
            cleanup: None,
        }
    }
}

impl<S: Storage + Clone + Send + Sync + 'static> CandidateReadTransfer<S> {
    /// Return the exact target currently owned by this transfer. Nested merge
    /// reconciliation can replace the outer descriptor with a newer leased
    /// target; the worker must resume that target after demand preemption.
    pub(crate) fn retained_target(
        &self,
    ) -> Option<(
        PartialReplicaState,
        super::super::http::TimedLeasedPartialDescriptor,
    )> {
        if self.cleanup.is_some() {
            return None;
        }
        let target = self.target.as_ref()?;
        Some((
            target.source.clone(),
            super::super::http::TimedLeasedPartialDescriptor {
                wire: super::super::LeasedPartialReplicaDescriptor {
                    descriptor: target.descriptor.clone(),
                    lease: target.lease.clone(),
                },
                deadline: target.deadline.clone(),
            },
        ))
    }

    pub(crate) async fn ensure_target(
        &mut self,
        source: &PartialReplicaState,
        wrapper: &super::super::http::TimedLeasedPartialDescriptor,
    ) -> Result<(), LixError> {
        let target_matches = self.target.as_ref().is_none_or(|target| {
            target.source == *source
                && target.descriptor == wrapper.wire.descriptor
                && target.lease == wrapper.wire.lease
                && target.deadline.same_window(&wrapper.deadline)
        });
        if !target_matches || self.cleanup.is_some() {
            self.clear().await?;
        }
        Ok(())
    }

    /// Retire this target without making the sync worker wait for remote
    /// release retries or local scratch acknowledgement. The cleanup owner
    /// retains the existing process permit and read-operation owner until the
    /// same release/local-ledger sequence finishes. A later candidate must
    /// join `cleanup` before acquiring another transfer slot.
    pub(crate) fn retire(&mut self) -> Result<(), LixError> {
        if self.cleanup.is_some() {
            return Ok(());
        }
        if self.fetch.is_none() && self.staged.is_none() {
            self.target.take();
            self.request_digest.take();
            self.immutable_only.take();
            return Ok(());
        }

        let retiring = std::mem::take(self);
        let (done_sender, done_receiver) = tokio::sync::oneshot::channel();
        let owner = Arc::new(Mutex::new(Some(retiring)));
        let task_owner = Arc::clone(&owner);
        if let Err(error) = crate::background_task::spawn_runtime_compatible(
            "candidate-read-transfer-retirement",
            move || async move {
                let mut retiring = task_owner
                    .lock()
                    .expect("candidate transfer retirement lock is not poisoned")
                    .take()
                    .expect("candidate transfer retirement has one owner");
                let result = retiring.clear().await;
                let _ = done_sender.send(result);
            },
        ) {
            // Keep ownership if the shared executor cannot accept the task.
            // The caller can fall back to its existing awaited cleanup path.
            *self = owner
                .lock()
                .expect("candidate transfer retirement lock is not poisoned")
                .take()
                .expect("failed retirement returned its owner");
            return Err(error);
        }
        self.cleanup = Some(done_receiver);
        Ok(())
    }

    pub(crate) async fn fetch_or_wait<C>(
        &mut self,
        storage: &StorageAdapter<S>,
        source: &PartialReplicaState,
        target: &PartialReplicaState,
        deadline: &super::super::http::CandidateBaselineDeadline,
        transport: &super::super::http::HttpSyncTransport<C>,
        request: &ReadFulfillmentRequest,
        immutable_only: bool,
    ) -> Result<CandidateTransferResult, LixError>
    where
        C: super::super::http::RawHttpClient + Clone + 'static,
    {
        if let Err(error) = deadline.check(&target.baseline_lease().lease_id) {
            self.clear().await?;
            return Err(error);
        }
        let request_digest = request.digest()?;
        let target_matches = self
            .target
            .as_ref()
            .is_some_and(|current| current.matches(source, target, deadline));
        if !target_matches
            || self.request_digest.as_deref() != Some(&request_digest)
            || self.immutable_only != Some(immutable_only)
            || self.cleanup.is_some()
        {
            self.clear().await?;
            #[cfg(test)]
            let retained_payload_permit = None;
            #[cfg(not(test))]
            let retained_payload_permit = super::super::transfer::RetainedPayloadPermit::try_acquire();
            let fetch = match start_staged_fetch_with_permits(
                storage,
                source,
                transport,
                request,
                lifecycle::Permit::acquire()?,
                retained_payload_permit,
            ) {
                Ok(fetch) => fetch,
                Err(error) => return Err(error),
            };
            self.target = Some(CandidateTransferTarget {
                source: source.clone(),
                descriptor: target.descriptor().clone(),
                lease: target.baseline_lease().clone(),
                deadline: deadline.clone(),
            });
            self.request_digest = Some(request_digest);
            self.immutable_only = Some(immutable_only);
            self.fetch = Some(fetch);
        }
        if let Err(error) = deadline.check(&target.baseline_lease().lease_id) {
            self.clear().await?;
            return Err(error);
        }
        if self.staged.is_none() {
            let result = self
                .fetch
                .as_mut()
                .expect("matching candidate transfer has an owner")
                .wait()
                .await;
            self.fetch.take();
            match result {
                Ok(staged) => self.staged = Some(staged),
                Err(error) => {
                    self.target.take();
                    self.request_digest.take();
                    self.immutable_only.take();
                    return Err(error);
                }
            }
        }
        if let Err(error) = deadline.check(&target.baseline_lease().lease_id) {
            self.clear().await?;
            return Err(error);
        }
        let outcome = self
            .staged
            .as_ref()
            .expect("completed candidate transfer has staged closure")
            .outcome();
        if outcome != ReadFulfillmentOutcome::Complete {
            self.clear().await?;
            return Ok(CandidateTransferResult::Fallback(outcome));
        }
        Ok(CandidateTransferResult::Ready)
    }

    pub(crate) async fn promote(
        &mut self,
        source: &PartialReplicaState,
        target: &PartialReplicaState,
        deadline: &super::super::http::CandidateBaselineDeadline,
        request: &ReadFulfillmentRequest,
        immutable_only: bool,
    ) -> Result<super::super::runtime::HydratedInputs, LixError> {
        deadline.check(&target.baseline_lease().lease_id)?;
        let request_digest = request.digest()?;
        if !self.target.as_ref().is_some_and(|current| {
            current.matches(source, target, deadline)
        }) || self.request_digest.as_deref() != Some(&request_digest)
            || self.immutable_only != Some(immutable_only)
        {
            return Err(invalid("candidate transfer changed before promotion"));
        }
        let expected_request_descriptor = if immutable_only {
            target.descriptor()
        } else {
            source.descriptor()
        };
        if source.repository_id() != target.repository_id()
            || source.remote_id() != target.remote_id()
            || source.active_account_id() != target.active_account_id()
            || source.epoch_id() != target.epoch_id()
            || source.descriptor().selected_branch.branch_id
                != target.descriptor().selected_branch.branch_id
            || target.descriptor().cursor < source.descriptor().cursor
            || request.epoch_id != target.epoch_id()
            || request.descriptor != *expected_request_descriptor
        {
            return Err(invalid("candidate transfer crossed its admission basis"));
        }
        request.validate(target.repository_id())?;
        let staged = self
            .staged
            .as_mut()
            .ok_or_else(|| invalid("candidate transfer is not ready for promotion"))?;
        let promoted = staged.promote(request, immutable_only).await?;
        self.staged.take();
        self.target.take();
        self.request_digest.take();
        self.immutable_only.take();
        Ok(promoted)
    }

    pub(crate) async fn clear(&mut self) -> Result<(), LixError> {
        let mut first_error = None;
        if let Some(fetch) = self.fetch.as_mut() {
            if let Err(error) = fetch.cancel_and_wait().await {
                first_error = Some(error);
            }
            if fetch.finished() {
                self.fetch.take();
            }
        }
        if self.cleanup.is_none()
            && let Some(staged) = self.staged.take()
        {
            match start_staged_cleanup(staged) {
                Ok(cleanup) => self.cleanup = Some(cleanup),
                Err(error) => {
                    self.target.take();
                    self.request_digest.take();
                    self.immutable_only.take();
                    return Err(error);
                }
            }
        }
        if let Some(cleanup) = self.cleanup.as_mut() {
            let result = cleanup.await;
            self.cleanup.take();
            if let Err(error) = result.unwrap_or_else(|_| {
                Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "staged cleanup owner ended before acknowledging release",
                ))
            }) && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        if self.fetch.is_none() && self.staged.is_none() && self.cleanup.is_none() {
            self.target.take();
            self.request_digest.take();
            self.immutable_only.take();
        }
        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(())
        }
    }
}

fn start_staged_cleanup<S>(
    mut staged: StagedClosure<S>,
) -> Result<tokio::sync::oneshot::Receiver<Result<(), LixError>>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let (done_sender, done_receiver) = tokio::sync::oneshot::channel();
    crate::background_task::spawn_runtime_compatible(
        "candidate-read-transfer-cleanup",
        move || async move {
            let _ = done_sender.send(staged.release_scratch().await);
        },
    )?;
    Ok(done_receiver)
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

async fn fulfill_read_cancellable<S, C>(
    transport: &super::super::http::HttpSyncTransport<C>,
    operation_request: &ReadFulfillmentRequest,
    page_request: &ReadFulfillmentRequest,
    stage: &mut StagedClosure<S>,
    cancellation: &mut tokio::sync::oneshot::Receiver<()>,
) -> Result<ReadFulfillmentResponse, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: super::super::http::RawHttpClient + Clone + 'static,
{
    let response = transport.fulfill_read_tracked(page_request, |event| {
        stage.track_remote_operation_send(transport, operation_request, event)
    });
    // Poll the response first so an already-complete terminal fallback can
    // disarm remote cleanup before cancellation is observed. Once raw handoff
    // is reported, dropping the request future is safe: release is keyed by
    // this exact operation and the authority records a cancellation tombstone
    // if release reaches it before the original request.
    tokio::select! {
        biased;
        result = response => result,
        _ = cancellation => Err(cancellation_error()),
    }
}

fn response_header(response: &ReadFulfillmentResponse) -> ReadFulfillmentResponse {
    ReadFulfillmentResponse {
        lix_id: response.lix_id.clone(),
        epoch_id: response.epoch_id.clone(),
        request_digest: response.request_digest.clone(),
        inputs: Vec::new(),
        frame: None,
        profile: response.profile.clone(),
        closure_digest: response.closure_digest.clone(),
        continuation: response.continuation.clone(),
        outcome: response.outcome,
    }
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
        self.retained_inputs.take();
        self.retained_payload_bytes = 0;
        self.retained_payload_permit.take();
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
    retained_payload_permit: Option<super::super::transfer::RetainedPayloadPermit>,
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
    let mut first_retained_payload_permit = retained_payload_permit;
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
            first_retained_payload_permit.take(),
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
    retained_payload_permit: Option<super::super::transfer::RetainedPayloadPermit>,
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
        retained_payload_permit,
        Some(read_operation_owner),
    );
    if cancellation_requested(cancellation) {
        return Err(stage.into_attempt_error(cancellation_error()).await);
    }
    if let Err(error) = stage.start_heartbeat() {
        return Err(stage.into_attempt_error(error).await);
    }
    let mut page_request = request.clone();
    let mut page = match fulfill_read_cancellable(
        transport,
        request,
        &page_request,
        &mut stage,
        cancellation,
    )
    .await
    {
        Ok(page) => page,
        Err(error) => {
            return Err(stage.into_attempt_error(error).await);
        }
    };
    stage.header = response_header(&page);
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
    if cancellation_requested(cancellation) {
        return Err(stage.into_attempt_error(cancellation_error()).await);
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
            page = fulfill_read_cancellable(
                transport,
                request,
                &page_request,
                &mut stage,
                cancellation,
            )
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
