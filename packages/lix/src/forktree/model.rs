use std::collections::BTreeSet;

use bytes::Bytes;

use crate::binary_cas::BlobId;
use crate::common::LixTimestamp;
use crate::storage::StorageError;

use super::codec::{
    Decoder, Encoder, authenticated_body, corruption, encode_authenticated, keyed_hash,
};
use super::object::{
    ObjectDomain, ObjectId, decode_id, decode_object, decode_optional_id, encode_id, encode_object,
    encode_optional_id,
};

/// Maximum authenticated outgoing edges decoded from one immutable envelope.
/// Larger logical values must use a blocked tree rather than an unbounded
/// vector in one object.
pub(crate) const AUTHENTICATED_EDGE_PAGE_ENTRIES: usize = 256;
const COMMIT_MEMBER_PAGE_EDGE_BUDGET: usize = AUTHENTICATED_EDGE_PAGE_ENTRIES;
const COMMIT_CHANGE_PAGE_TARGET_BYTES: usize = 64 * 1024;
const COMMIT_CHANGE_PAGE_MAX_BYTES: usize = 4 * 1024 * 1024;

const GLOBAL_SELECTOR_MAGIC: &[u8; 8] = b"LIXFTG\0\x01";
const BRANCH_SELECTOR_MAGIC: &[u8; 8] = b"LIXFTB\0\x01";
const UPLOAD_SELECTOR_MAGIC: &[u8; 8] = b"LIXFTU\0\x01";
const SNAPSHOT_SELECTOR_MAGIC: &[u8; 8] = b"LIXFTS\0\x01";
const GC_SELECTOR_MAGIC: &[u8; 8] = b"LIXFTC\0\x02";
const GLOBAL_SELECTOR_DOMAIN: &str = "lix forktree global selector v1";
const BRANCH_SELECTOR_DOMAIN: &str = "lix forktree branch selector v1";
const UPLOAD_SELECTOR_DOMAIN: &str = "lix forktree upload selector v1";
const SNAPSHOT_SELECTOR_DOMAIN: &str = "lix forktree snapshot selector v1";
const GC_SELECTOR_DOMAIN: &str = "lix forktree gc-progress selector v2";
const UPLOAD_BINDING_DOMAIN: &str = "lix forktree upload binding v1";
pub(crate) const BLOB_MERKLE_CHUNK_BYTES: u64 = 1_048_576;
const MERKLE_BLOB_ID_DOMAIN: &str = "lix binary blob canonical merkle identity v1";
const MERKLE_BLOB_ID_MAGIC: &[u8; 8] = b"LIXBMRK\0";

macro_rules! raw_uuid_id {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub(crate) struct $name([u8; 16]);

        impl $name {
            pub(crate) const fn from_bytes(bytes: [u8; 16]) -> Self {
                Self(bytes)
            }

            pub(crate) const fn as_bytes(&self) -> &[u8; 16] {
                &self.0
            }
        }
    };
}

raw_uuid_id!(CommitId);
raw_uuid_id!(ChangeId);
raw_uuid_id!(CanonicalBranchId);
raw_uuid_id!(SnapshotSelectorId);

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct CanonicalUploadId(Vec<u8>);

impl CanonicalUploadId {
    pub(crate) fn new(value: impl AsRef<[u8]>) -> Result<Self, StorageError> {
        let value = value.as_ref();
        if value.is_empty() || value.len() > 200 || !value.is_ascii() {
            return Err(corruption(
                "upload id must contain 1-200 canonical ASCII bytes",
            ));
        }
        Ok(Self(value.to_vec()))
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RepositoryRootV1 {
    pub(crate) global_state_root: ObjectId,
    pub(crate) commit_catalog_root: ObjectId,
    pub(crate) change_catalog_root: ObjectId,
}

impl RepositoryRootV1 {
    pub(crate) fn encode(self) -> Result<(ObjectId, Bytes), StorageError> {
        validate_nonzero_ids(
            "repository root",
            &[
                self.global_state_root,
                self.commit_catalog_root,
                self.change_catalog_root,
            ],
        )?;
        encode_object(ObjectDomain::RepositoryRoot, |encoder| {
            encode_id(encoder, self.global_state_root);
            encode_id(encoder, self.commit_catalog_root);
            encode_id(encoder, self.change_catalog_root);
            Ok(())
        })
    }

    pub(crate) fn decode(id: ObjectId, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = decode_object(id, ObjectDomain::RepositoryRoot, bytes)?;
        let value = Self {
            global_state_root: decode_id(&mut decoder)?,
            commit_catalog_root: decode_id(&mut decoder)?,
            change_catalog_root: decode_id(&mut decoder)?,
        };
        decoder.finish()?;
        validate_nonzero_ids(
            "repository root",
            &[
                value.global_state_root,
                value.commit_catalog_root,
                value.change_catalog_root,
            ],
        )?;
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BranchSnapshotV1 {
    pub(crate) branch_id: CanonicalBranchId,
    pub(crate) local_state_root: ObjectId,
    pub(crate) semantic_head_commit_object_id: ObjectId,
    pub(crate) latest_ref_change_object_id: Option<ObjectId>,
    pub(crate) historical_global_state_root: ObjectId,
}

impl BranchSnapshotV1 {
    pub(crate) fn encode(self) -> Result<(ObjectId, Bytes), StorageError> {
        validate_nonzero_ids(
            "branch snapshot",
            &[
                self.local_state_root,
                self.semantic_head_commit_object_id,
                self.historical_global_state_root,
            ],
        )?;
        if self.latest_ref_change_object_id == Some(ObjectId::ZERO) {
            return Err(corruption("branch snapshot ref-change edge is zero"));
        }
        encode_object(ObjectDomain::BranchSnapshot, |encoder| {
            encoder.fixed(self.branch_id.as_bytes());
            encode_id(encoder, self.local_state_root);
            encode_id(encoder, self.semantic_head_commit_object_id);
            encode_optional_id(encoder, self.latest_ref_change_object_id);
            encode_id(encoder, self.historical_global_state_root);
            Ok(())
        })
    }

    pub(crate) fn decode(id: ObjectId, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = decode_object(id, ObjectDomain::BranchSnapshot, bytes)?;
        let value = Self {
            branch_id: CanonicalBranchId::from_bytes(decoder.fixed()?),
            local_state_root: decode_id(&mut decoder)?,
            semantic_head_commit_object_id: decode_id(&mut decoder)?,
            latest_ref_change_object_id: decode_optional_id(&mut decoder, "ref-change edge")?,
            historical_global_state_root: decode_id(&mut decoder)?,
        };
        decoder.finish()?;
        validate_nonzero_ids(
            "branch snapshot",
            &[
                value.local_state_root,
                value.semantic_head_commit_object_id,
                value.historical_global_state_root,
            ],
        )?;
        if value.latest_ref_change_object_id == Some(ObjectId::ZERO) {
            return Err(corruption("branch snapshot ref-change edge is zero"));
        }
        Ok(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CommitMemberV3 {
    Introduced {
        change_id: ChangeId,
        encoded_key: Vec<u8>,
        layout_id: [u8; 32],
        global: bool,
        owner_digest: [u8; 32],
        semantic_digest: [u8; 32],
        deleted: bool,
        account_id: String,
        created_at: LixTimestamp,
        updated_at: LixTimestamp,
        origin_key: Option<String>,
    },
    Selected {
        change_id: ChangeId,
        source_commit_object_id: ObjectId,
        source_ordinal: u32,
        /// Authenticated lifecycle timestamp projected by this selected
        /// membership. Checkpoint compaction may rebase an identity that was
        /// absent from its parent while retaining the canonical source member.
        created_at: LixTimestamp,
    },
}

impl CommitMemberV3 {
    pub(crate) fn introduced(
        change_id: ChangeId,
        encoded_key: Vec<u8>,
        layout_id: [u8; 32],
        global: bool,
        owner_digest: [u8; 32],
        semantic_digest: [u8; 32],
        deleted: bool,
        account_id: String,
        created_at: LixTimestamp,
        updated_at: LixTimestamp,
        origin_key: Option<String>,
    ) -> Self {
        Self::Introduced {
            change_id,
            encoded_key,
            layout_id,
            global,
            owner_digest,
            semantic_digest,
            deleted,
            account_id,
            created_at,
            updated_at,
            origin_key,
        }
    }

    pub(crate) fn selected(
        change_id: ChangeId,
        source_commit_object_id: ObjectId,
        source_ordinal: u32,
        created_at: LixTimestamp,
    ) -> Self {
        Self::Selected {
            change_id,
            source_commit_object_id,
            source_ordinal,
            created_at,
        }
    }

    pub(crate) fn with_selected_created_at(
        mut self,
        created_at: LixTimestamp,
    ) -> Result<Self, StorageError> {
        let Self::Selected {
            created_at: selected_created_at,
            ..
        } = &mut self
        else {
            return Err(corruption(
                "cannot project a selected timestamp onto an introduced member",
            ));
        };
        *selected_created_at = created_at;
        Ok(self)
    }

    pub(crate) fn selected_created_at(&self) -> Option<LixTimestamp> {
        match self {
            Self::Selected { created_at, .. } => Some(*created_at),
            Self::Introduced { .. } => None,
        }
    }

    pub(crate) fn change_id(&self) -> ChangeId {
        match self {
            Self::Introduced { change_id, .. } | Self::Selected { change_id, .. } => *change_id,
        }
    }

    pub(crate) fn source(&self) -> Option<(ObjectId, u32)> {
        match self {
            Self::Introduced { .. } => None,
            Self::Selected {
                source_commit_object_id,
                source_ordinal,
                ..
            } => Some((*source_commit_object_id, *source_ordinal)),
        }
    }

    pub(crate) fn introduced_identity(
        &self,
    ) -> Option<(&[u8], [u8; 32], bool, [u8; 32], [u8; 32], bool)> {
        match self {
            Self::Introduced {
                encoded_key,
                layout_id,
                global,
                owner_digest,
                semantic_digest,
                deleted,
                ..
            } => Some((
                encoded_key,
                *layout_id,
                *global,
                *owner_digest,
                *semantic_digest,
                *deleted,
            )),
            Self::Selected { .. } => None,
        }
    }

    fn encode(&self, encoder: &mut Encoder) -> Result<(), StorageError> {
        match self {
            Self::Introduced {
                change_id,
                encoded_key,
                layout_id,
                global,
                owner_digest,
                semantic_digest,
                deleted,
                account_id,
                created_at,
                updated_at,
                origin_key,
            } => {
                encoder.u8(0);
                encoder.fixed(change_id.as_bytes());
                encoder.bytes(encoded_key)?;
                encoder.fixed(layout_id);
                encoder.u8(u8::from(*global));
                encoder.fixed(owner_digest);
                encoder.fixed(semantic_digest);
                encoder.u8(u8::from(*deleted));
                encoder.bytes(account_id.as_bytes())?;
                encoder.u64(created_at.packed());
                encoder.u64(updated_at.packed());
                match origin_key {
                    None => encoder.u8(0),
                    Some(value) => {
                        encoder.u8(1);
                        encoder.bytes(value.as_bytes())?;
                    }
                }
            }
            Self::Selected {
                change_id,
                source_commit_object_id,
                source_ordinal,
                created_at,
            } => {
                encoder.u8(1);
                encoder.fixed(change_id.as_bytes());
                encode_id(encoder, *source_commit_object_id);
                encoder.u32(*source_ordinal);
                encoder.u64(created_at.packed());
            }
        }
        Ok(())
    }

    fn decode(decoder: &mut Decoder<'_>) -> Result<Self, StorageError> {
        let value = match decoder.u8()? {
            0 => Self::introduced(
                ChangeId::from_bytes(decoder.fixed()?),
                decoder.bytes("semantic change key")?,
                decoder.fixed()?,
                match decoder.u8()? {
                    0 => false,
                    1 => true,
                    value => {
                        return Err(corruption(format!(
                            "semantic change state-domain tag {value} is invalid"
                        )));
                    }
                },
                decoder.fixed()?,
                decoder.fixed()?,
                match decoder.u8()? {
                    0 => false,
                    1 => true,
                    value => {
                        return Err(corruption(format!(
                            "semantic change deletion tag {value} is invalid"
                        )));
                    }
                },
                String::from_utf8(decoder.bytes("semantic change account")?)
                    .map_err(|_| corruption("semantic change account is not UTF-8"))?,
                LixTimestamp::from_packed(decoder.u64()?).map_err(|error| {
                    corruption(format!("semantic change created_at is invalid: {error}"))
                })?,
                LixTimestamp::from_packed(decoder.u64()?).map_err(|error| {
                    corruption(format!("semantic change updated_at is invalid: {error}"))
                })?,
                match decoder.u8()? {
                    0 => None,
                    1 => Some(
                        String::from_utf8(decoder.bytes("semantic change origin key")?)
                            .map_err(|_| corruption("semantic change origin key is not UTF-8"))?,
                    ),
                    tag => {
                        return Err(corruption(format!(
                            "semantic change origin-key tag {tag} is invalid"
                        )));
                    }
                },
            ),
            1 => Self::selected(
                ChangeId::from_bytes(decoder.fixed()?),
                decode_id(decoder)?,
                decoder.u32()?,
                LixTimestamp::from_packed(decoder.u64()?).map_err(|error| {
                    corruption(format!("selected change created_at is invalid: {error}"))
                })?,
            ),
            tag => {
                return Err(corruption(format!(
                    "commit member has invalid membership tag {tag}"
                )));
            }
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), StorageError> {
        if self
            .source()
            .is_some_and(|(commit_object_id, _)| commit_object_id == ObjectId::ZERO)
        {
            return Err(corruption("commit member contains a zero object edge"));
        }
        if let Self::Introduced {
            encoded_key,
            layout_id,
            owner_digest,
            semantic_digest,
            deleted,
            account_id,
            ..
        } = self
        {
            if encoded_key.is_empty() {
                return Err(corruption("introduced commit member has an empty key"));
            }
            let any_zero_digest =
                *layout_id == [0; 32] || *owner_digest == [0; 32] || *semantic_digest == [0; 32];
            let all_zero_digests =
                *layout_id == [0; 32] && *owner_digest == [0; 32] && *semantic_digest == [0; 32];
            if *deleted && !all_zero_digests {
                return Err(corruption(
                    "introduced tombstone member carries a native tuple digest",
                ));
            }
            if !*deleted && any_zero_digest {
                return Err(corruption("introduced live member has a zero native digest"));
            }
            if account_id.is_empty() {
                return Err(corruption("introduced commit member has an empty account"));
            }
        }
        Ok(())
    }

    fn authenticated_edge_count(&self) -> usize {
        match self {
            Self::Introduced { .. } => 0,
            Self::Selected { .. } => 1,
        }
    }
}


/// One byte-bounded authenticated page in a commit's ordered semantic-change
/// closure. Introduced members carry only native-row identity, lifecycle, and
/// provenance; the body lives once in the authenticated current-state pack.
/// There is no per-row immutable SemanticChange payload or alternate lookup
/// path. The commit authenticates the complete ordered page-ID vector and
/// state leaves point back to the exact page/ordinal descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitChangePageV3 {
    pub(crate) commit_id: CommitId,
    pub(crate) start_ordinal: u32,
    pub(crate) members: Vec<CommitMemberV3>,
}

impl CommitChangePageV3 {
    pub(crate) fn encode(&self) -> Result<(ObjectId, Bytes), StorageError> {
        self.validate()?;
        let mut body = Encoder::default();
        body.fixed(self.commit_id.as_bytes());
        body.u32(self.start_ordinal);
        body.u32(
            u32::try_from(self.members.len())
                .map_err(|_| corruption("commit member page count exceeds u32"))?,
        );
        for member in &self.members {
            member.encode(&mut body)?;
        }
        let body = body.into_vec();
        if body.len() > COMMIT_CHANGE_PAGE_MAX_BYTES {
            return Err(corruption("commit change page exceeds its byte bound"));
        }
        let compressed = crate::compression::compress_zstd_level_1(&body).map_err(|error| {
            corruption(format!("commit change page compression failed: {error}"))
        })?;
        encode_object(ObjectDomain::CommitChangePageV3, |encoder| {
            encoder.u32(
                u32::try_from(body.len())
                    .map_err(|_| corruption("commit change page decoded length exceeds u32"))?,
            );
            encoder.bytes(&compressed)
        })
    }

    pub(crate) fn decode(id: ObjectId, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = decode_object(id, ObjectDomain::CommitChangePageV3, bytes)?;
        let decoded_len = decoder.usize("commit change page decoded length")?;
        if decoded_len == 0 || decoded_len > COMMIT_CHANGE_PAGE_MAX_BYTES {
            return Err(corruption(
                "commit change page decoded length exceeds its bound",
            ));
        }
        let compressed = decoder.bytes("compressed commit change page")?;
        decoder.finish()?;
        let body =
            crate::compression::decompress_zstd(&compressed, decoded_len).map_err(|error| {
                corruption(format!("commit change page decompression failed: {error}"))
            })?;
        if body.len() != decoded_len {
            return Err(corruption(
                "commit change page decoded length is inconsistent",
            ));
        }
        let mut body = Decoder::after_prefix(&body, &[])?;
        let commit_id = CommitId::from_bytes(body.fixed()?);
        let start_ordinal = body.u32()?;
        let count = body.usize("commit member page count")?;
        // Members are inline authenticated payload, not outgoing object edges.
        // Bound their count by the already byte-bounded body while keeping the
        // independent object-edge budget in `validate` below.
        if count == 0 || count > body.remaining() / 17 {
            return Err(corruption(
                "commit member page count exceeds its encoded body",
            ));
        }
        let mut members = Vec::with_capacity(count);
        for _ in 0..count {
            members.push(CommitMemberV3::decode(&mut body)?);
        }
        body.finish()?;
        let value = Self {
            commit_id,
            start_ordinal,
            members,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), StorageError> {
        if self.members.is_empty() {
            return Err(corruption("commit member page must not be empty"));
        }
        let member_edges = self
            .members
            .iter()
            .map(CommitMemberV3::authenticated_edge_count)
            .try_fold(0usize, |total, count| total.checked_add(count))
            .ok_or_else(|| corruption("commit member page edge count overflowed"))?;
        if member_edges > AUTHENTICATED_EDGE_PAGE_ENTRIES {
            return Err(corruption(
                "commit member page exceeds its authenticated edge bound",
            ));
        }
        self.start_ordinal
            .checked_add(
                u32::try_from(self.members.len())
                    .map_err(|_| corruption("commit member page ordinal exceeds u32"))?,
            )
            .ok_or_else(|| corruption("commit member page ordinal overflows u32"))?;
        let mut unique_changes = BTreeSet::new();
        for member in &self.members {
            member.validate()?;
            if !unique_changes.insert(member.change_id()) {
                return Err(corruption("commit change page repeats a ChangeId"));
            }
        }
        Ok(())
    }

    pub(crate) fn encode_pages(
        commit_id: CommitId,
        members: &[CommitMemberV3],
    ) -> Result<PreparedCommitChangePages, StorageError> {
        if members.is_empty() {
            // Empty/ref-only commits are represented by an empty authenticated
            // page vector. There is no member object to page, and the commit
            // envelope remains the sole authority for the empty membership.
            return Ok(PreparedCommitChangePages {
                objects: Vec::new(),
                member_counts: Vec::new(),
                member_locations: Vec::new(),
            });
        }
        let max_page_objects = AUTHENTICATED_EDGE_PAGE_ENTRIES.saturating_sub(2);
        let mut member_sizes = Vec::with_capacity(members.len());
        let mut total_member_bytes = 0usize;
        let mut max_member_bytes = 0usize;
        for member in members {
            member.validate()?;
            let member_edges = member.authenticated_edge_count();
            if member_edges > COMMIT_MEMBER_PAGE_EDGE_BUDGET {
                return Err(corruption("one commit member exceeds the page edge budget"));
            }
            let mut encoded_member = Encoder::default();
            member.encode(&mut encoded_member)?;
            let member_bytes = encoded_member.into_vec().len();
            if member_bytes > COMMIT_CHANGE_PAGE_MAX_BYTES {
                return Err(corruption("one commit member exceeds the page byte bound"));
            }
            total_member_bytes = total_member_bytes
                .checked_add(member_bytes)
                .ok_or_else(|| corruption("commit member byte count overflowed"))?;
            max_member_bytes = max_member_bytes.max(member_bytes);
            member_sizes.push(member_bytes);
        }
        // Keep ordinary commits at the sparse-read-friendly 64 KiB target.
        // Only a commit that would overflow the authenticated page-ID vector
        // widens pages to the minimum target needed to keep that vector bound.
        // Sequential packing may leave up to one largest member of slack per
        // page, so include that slack when deriving the bounded page target.
        let required_page_payload = total_member_bytes
            .div_ceil(max_page_objects)
            .saturating_add(max_member_bytes);
        let page_target_bytes = COMMIT_CHANGE_PAGE_TARGET_BYTES
            .max(required_page_payload.saturating_add(128))
            .min(COMMIT_CHANGE_PAGE_MAX_BYTES);

        let mut chunks = Vec::<(u32, Vec<CommitMemberV3>)>::new();
        let mut start = 0usize;
        let mut current = Vec::new();
        let mut current_edges = 0usize;
        let mut current_bytes = 0usize;
        for (member, member_bytes) in members.iter().cloned().zip(member_sizes) {
            let member_edges = member.authenticated_edge_count();
            if !current.is_empty()
                && (current_edges
                    .checked_add(member_edges)
                    .is_none_or(|edges| edges > COMMIT_MEMBER_PAGE_EDGE_BUDGET)
                    || current_bytes
                        .checked_add(member_bytes)
                        .is_none_or(|bytes| bytes > page_target_bytes.saturating_sub(128)))
            {
                chunks.push((
                    u32::try_from(start)
                        .map_err(|_| corruption("commit member page ordinal exceeds u32"))?,
                    std::mem::take(&mut current),
                ));
                start = start
                    .checked_add(chunks.last().expect("just pushed").1.len())
                    .ok_or_else(|| corruption("commit member page ordinal overflows usize"))?;
                current_edges = 0;
                current_bytes = 0;
            }
            current.push(member);
            current_edges = current_edges
                .checked_add(member_edges)
                .ok_or_else(|| corruption("commit member page edge count overflowed"))?;
            current_bytes = current_bytes
                .checked_add(member_bytes)
                .ok_or_else(|| corruption("commit member page byte count overflowed"))?;
        }
        if !current.is_empty() {
            chunks.push((
                u32::try_from(start)
                    .map_err(|_| corruption("commit member page ordinal exceeds u32"))?,
                current,
            ));
        }

        let mut encoded = Vec::with_capacity(chunks.len());
        let mut member_counts = Vec::with_capacity(chunks.len());
        let mut member_locations = Vec::with_capacity(members.len());
        for (start_ordinal, page_members) in chunks {
            let page = Self {
                commit_id,
                start_ordinal,
                members: page_members,
            };
            let (id, bytes) = page.encode()?;
            member_locations.extend((0..page.members.len()).map(|ordinal| StatePageLocation {
                page_object_id: id,
                page_ordinal:
                    u32::try_from(ordinal).expect("page member count is bounded below u32"),
            }));
            member_counts.push(
                u32::try_from(page.members.len())
                    .expect("page member count is bounded below u32"),
            );
            encoded.push((id, bytes));
        }
        if encoded.len() + 2 > AUTHENTICATED_EDGE_PAGE_ENTRIES {
            return Err(corruption(
                "commit change-page vector exceeds its edge bound",
            ));
        }
        Ok(PreparedCommitChangePages {
            objects: encoded,
            member_counts,
            member_locations,
        })
    }
}


#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StatePageLocation {
    pub(crate) page_object_id: ObjectId,
    pub(crate) page_ordinal: u32,
}

#[derive(Debug)]
pub(crate) struct PreparedCommitChangePages {
    pub(crate) objects: Vec<(ObjectId, Bytes)>,
    pub(crate) member_counts: Vec<u32>,
    pub(crate) member_locations: Vec<StatePageLocation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitObjectV1 {
    pub(crate) commit_id: CommitId,
    pub(crate) generation: u64,
    pub(crate) parent_commit_object_ids: Vec<ObjectId>,
    pub(crate) members: Vec<CommitMemberV3>,
    pub(crate) member_page_object_ids: Vec<ObjectId>,
    /// Canonical member cardinality aligned with `member_page_object_ids`.
    /// Together the vectors are the commit-bound routing directory: an exact
    /// current-pack page back-edge can prove its global ordinal without
    /// decoding every preceding page.
    pub(crate) member_page_member_counts: Vec<u32>,
    pub(crate) global_state_root: ObjectId,
    pub(crate) local_state_root: ObjectId,
    /// Authenticated, branch-bound first-parent checkpoint chronology.
    pub(crate) checkpoint_cursor: CheckpointCursorV1,
    pub(crate) metadata: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CheckpointCursorV1 {
    /// The repository root is the implicit checkpoint shared by every branch.
    Root,
    Ordinary {
        owner_branch_id: CanonicalBranchId,
        root_commit_object_id: ObjectId,
        distance_to_root: u32,
        latest_checkpoint_object_id: ObjectId,
        distance_to_latest: u32,
    },
    Checkpoint {
        owner_branch_id: CanonicalBranchId,
        root_commit_object_id: ObjectId,
        distance_to_root: u32,
        previous_checkpoint_object_id: ObjectId,
        distance_to_previous: u32,
    },
}

impl CheckpointCursorV1 {
    pub(crate) const fn root() -> Self {
        Self::Root
    }

    pub(crate) fn after_first_parent(
        parent_object_id: ObjectId,
        parent: &CommitObjectV1,
        owner_branch_id: CanonicalBranchId,
        is_checkpoint: bool,
    ) -> Result<Self, StorageError> {
        let (root_commit_object_id, distance_to_root) = match parent.checkpoint_cursor {
            Self::Root => (parent_object_id, 1),
            Self::Ordinary {
                root_commit_object_id,
                distance_to_root,
                ..
            }
            | Self::Checkpoint {
                root_commit_object_id,
                distance_to_root,
                ..
            } => (
                root_commit_object_id,
                distance_to_root
                    .checked_add(1)
                    .ok_or_else(|| corruption("checkpoint root distance overflowed"))?,
            ),
        };
        let (latest_checkpoint_object_id, distance_to_latest) = match parent.checkpoint_cursor {
            Self::Root => (parent_object_id, 1),
            Self::Ordinary {
                owner_branch_id: parent_owner,
                latest_checkpoint_object_id,
                distance_to_latest,
                ..
            } if parent_owner == owner_branch_id => (
                latest_checkpoint_object_id,
                distance_to_latest
                    .checked_add(1)
                    .ok_or_else(|| corruption("checkpoint latest distance overflowed"))?,
            ),
            Self::Checkpoint {
                owner_branch_id: parent_owner,
                ..
            } if parent_owner == owner_branch_id => (parent_object_id, 1),
            Self::Ordinary { .. } | Self::Checkpoint { .. } => {
                (root_commit_object_id, distance_to_root)
            }
        };
        let value = if is_checkpoint {
            Self::Checkpoint {
                owner_branch_id,
                root_commit_object_id,
                distance_to_root,
                previous_checkpoint_object_id: latest_checkpoint_object_id,
                distance_to_previous: distance_to_latest,
            }
        } else {
            Self::Ordinary {
                owner_branch_id,
                root_commit_object_id,
                distance_to_root,
                latest_checkpoint_object_id,
                distance_to_latest,
            }
        };
        value.validate(false)?;
        Ok(value)
    }

    pub(crate) fn root_edge(self, current_object_id: ObjectId) -> (ObjectId, u32) {
        match self {
            Self::Root => (current_object_id, 0),
            Self::Ordinary {
                root_commit_object_id,
                distance_to_root,
                ..
            }
            | Self::Checkpoint {
                root_commit_object_id,
                distance_to_root,
                ..
            } => (root_commit_object_id, distance_to_root),
        }
    }

    pub(crate) const fn owner_branch_id(self) -> Option<CanonicalBranchId> {
        match self {
            Self::Root => None,
            Self::Ordinary {
                owner_branch_id, ..
            }
            | Self::Checkpoint {
                owner_branch_id, ..
            } => Some(owner_branch_id),
        }
    }

    pub(crate) fn latest_for_branch(
        self,
        current_object_id: ObjectId,
        branch_id: CanonicalBranchId,
    ) -> (ObjectId, u32) {
        match self {
            Self::Root => (current_object_id, 0),
            Self::Checkpoint {
                owner_branch_id, ..
            } if owner_branch_id == branch_id => (current_object_id, 0),
            Self::Ordinary {
                owner_branch_id,
                latest_checkpoint_object_id,
                distance_to_latest,
                ..
            } if owner_branch_id == branch_id => (latest_checkpoint_object_id, distance_to_latest),
            value => value.root_edge(current_object_id),
        }
    }

    pub(crate) fn previous_checkpoint(self) -> Option<(ObjectId, u32)> {
        match self {
            Self::Checkpoint {
                previous_checkpoint_object_id,
                distance_to_previous,
                ..
            } => Some((previous_checkpoint_object_id, distance_to_previous)),
            Self::Root | Self::Ordinary { .. } => None,
        }
    }

    pub(crate) fn edges(self) -> impl Iterator<Item = ObjectId> {
        let edges = match self {
            Self::Root => [None, None],
            Self::Ordinary {
                root_commit_object_id,
                latest_checkpoint_object_id,
                ..
            } => [
                Some(root_commit_object_id),
                Some(latest_checkpoint_object_id),
            ],
            Self::Checkpoint {
                root_commit_object_id,
                previous_checkpoint_object_id,
                ..
            } => [
                Some(root_commit_object_id),
                Some(previous_checkpoint_object_id),
            ],
        };
        edges.into_iter().flatten()
    }

    fn checked_non_root_fields(
        root_commit_object_id: ObjectId,
        distance_to_root: u32,
        checkpoint_object_id: ObjectId,
        checkpoint_distance: u32,
    ) -> Result<(), StorageError> {
        if root_commit_object_id == ObjectId::ZERO
            || checkpoint_object_id == ObjectId::ZERO
            || distance_to_root == 0
            || checkpoint_distance == 0
            || checkpoint_distance > distance_to_root
        {
            return Err(corruption("checkpoint chronology cursor is invalid"));
        }
        Ok(())
    }

    fn validate(self, root: bool) -> Result<(), StorageError> {
        if root {
            if self != Self::Root {
                return Err(corruption(
                    "root commit must carry the implicit checkpoint cursor",
                ));
            }
            return Ok(());
        }
        match self {
            Self::Root => Err(corruption(
                "non-root commit cannot carry the implicit checkpoint cursor",
            )),
            Self::Ordinary {
                root_commit_object_id,
                distance_to_root,
                latest_checkpoint_object_id,
                distance_to_latest,
                ..
            } => Self::checked_non_root_fields(
                root_commit_object_id,
                distance_to_root,
                latest_checkpoint_object_id,
                distance_to_latest,
            ),
            Self::Checkpoint {
                root_commit_object_id,
                distance_to_root,
                previous_checkpoint_object_id,
                distance_to_previous,
                ..
            } => Self::checked_non_root_fields(
                root_commit_object_id,
                distance_to_root,
                previous_checkpoint_object_id,
                distance_to_previous,
            ),
        }
    }
}

impl CommitObjectV1 {
    pub(crate) fn encode(&self) -> Result<(ObjectId, Bytes), StorageError> {
        self.validate_edge_bound()?;
        validate_nonzero_ids("commit parent", &self.parent_commit_object_ids)?;
        for member in &self.members {
            member.validate()?;
        }
        validate_nonzero_ids(
            "commit state",
            &[self.global_state_root, self.local_state_root],
        )?;
        self.checkpoint_cursor
            .validate(self.parent_commit_object_ids.is_empty())?;
        let parent_count = u32::try_from(self.parent_commit_object_ids.len())
            .map_err(|_| corruption("commit has too many parents"))?;
        if !self.members.is_empty() && self.member_page_object_ids.is_empty() {
            return Err(corruption(
                "nonempty commit members have no authenticated pages",
            ));
        }
        encode_object(ObjectDomain::CommitV2, |encoder| {
            encoder.fixed(self.commit_id.as_bytes());
            encoder.u64(self.generation);
            encoder.u32(parent_count);
            for parent in &self.parent_commit_object_ids {
                encode_id(encoder, *parent);
            }
            encoder.u32(
                u32::try_from(self.member_page_object_ids.len())
                    .map_err(|_| corruption("commit change-page count exceeds u32"))?,
            );
            for (page_id, member_count) in self
                .member_page_object_ids
                .iter()
                .zip(&self.member_page_member_counts)
            {
                encode_id(encoder, *page_id);
                encoder.u32(*member_count);
            }
            encode_id(encoder, self.global_state_root);
            encode_id(encoder, self.local_state_root);
            match self.checkpoint_cursor {
                CheckpointCursorV1::Root => encoder.u8(0),
                CheckpointCursorV1::Ordinary {
                    owner_branch_id,
                    root_commit_object_id,
                    distance_to_root,
                    latest_checkpoint_object_id,
                    distance_to_latest,
                } => {
                    encoder.u8(1);
                    encoder.fixed(owner_branch_id.as_bytes());
                    encode_id(encoder, root_commit_object_id);
                    encoder.u32(distance_to_root);
                    encode_id(encoder, latest_checkpoint_object_id);
                    encoder.u32(distance_to_latest);
                }
                CheckpointCursorV1::Checkpoint {
                    owner_branch_id,
                    root_commit_object_id,
                    distance_to_root,
                    previous_checkpoint_object_id,
                    distance_to_previous,
                } => {
                    encoder.u8(2);
                    encoder.fixed(owner_branch_id.as_bytes());
                    encode_id(encoder, root_commit_object_id);
                    encoder.u32(distance_to_root);
                    encode_id(encoder, previous_checkpoint_object_id);
                    encoder.u32(distance_to_previous);
                }
            }
            encoder.bytes(&self.metadata)
        })
    }

    pub(crate) fn decode(id: ObjectId, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = decode_object(id, ObjectDomain::CommitV2, bytes)?;
        let commit_id = CommitId::from_bytes(decoder.fixed()?);
        let generation = decoder.u64()?;
        let parent_count = decoder.usize("commit parent count")?;
        validate_count(parent_count, decoder.remaining(), 32, "commit parent count")?;
        let mut parent_commit_object_ids = Vec::with_capacity(parent_count);
        for _ in 0..parent_count {
            parent_commit_object_ids.push(decode_id(&mut decoder)?);
        }
        let page_count = decoder.usize("commit change-page count")?;
        validate_count(
            page_count,
            decoder.remaining(),
            36,
            "commit change-page count",
        )?;
        let mut member_page_object_ids = Vec::with_capacity(page_count);
        let mut member_page_member_counts = Vec::with_capacity(page_count);
        for _ in 0..page_count {
            member_page_object_ids.push(decode_id(&mut decoder)?);
            member_page_member_counts.push(decoder.u32()?);
        }
        let value = Self {
            commit_id,
            generation,
            parent_commit_object_ids,
            members: Vec::new(),
            member_page_object_ids,
            member_page_member_counts,
            global_state_root: decode_id(&mut decoder)?,
            local_state_root: decode_id(&mut decoder)?,
            checkpoint_cursor: match decoder.u8()? {
                0 => CheckpointCursorV1::Root,
                tag @ (1 | 2) => {
                    let owner_branch_id = CanonicalBranchId::from_bytes(decoder.fixed()?);
                    let root_commit_object_id = decode_id(&mut decoder)?;
                    let distance_to_root = decoder.u32()?;
                    let checkpoint_object_id = decode_id(&mut decoder)?;
                    let checkpoint_distance = decoder.u32()?;
                    if tag == 1 {
                        CheckpointCursorV1::Ordinary {
                            owner_branch_id,
                            root_commit_object_id,
                            distance_to_root,
                            latest_checkpoint_object_id: checkpoint_object_id,
                            distance_to_latest: checkpoint_distance,
                        }
                    } else {
                        CheckpointCursorV1::Checkpoint {
                            owner_branch_id,
                            root_commit_object_id,
                            distance_to_root,
                            previous_checkpoint_object_id: checkpoint_object_id,
                            distance_to_previous: checkpoint_distance,
                        }
                    }
                }
                _ => return Err(corruption("checkpoint cursor tag is not canonical")),
            },
            metadata: decoder.bytes("commit metadata")?,
        };
        decoder.finish()?;
        validate_nonzero_ids("commit parent", &value.parent_commit_object_ids)?;
        for member in &value.members {
            member.validate()?;
        }
        validate_nonzero_ids(
            "commit state",
            &[value.global_state_root, value.local_state_root],
        )?;
        value
            .checkpoint_cursor
            .validate(value.parent_commit_object_ids.is_empty())?;
        value.validate_edge_bound()?;
        Ok(value)
    }

    fn validate_edge_bound(&self) -> Result<(), StorageError> {
        validate_nonzero_ids("commit change page", &self.member_page_object_ids)?;
        if self.member_page_object_ids.len() != self.member_page_member_counts.len()
            || self.member_page_member_counts.contains(&0)
            || self
                .member_page_member_counts
                .iter()
                .try_fold(0_u32, |total, count| total.checked_add(*count))
                .is_none()
        {
            return Err(corruption(
                "commit change-page routing directory is not canonical",
            ));
        }
        if self.member_page_object_ids.iter().copied().collect::<BTreeSet<_>>().len()
            != self.member_page_object_ids.len()
        {
            return Err(corruption("commit repeats one change-page object"));
        }
        if self
            .parent_commit_object_ids
            .len()
            .checked_add(self.member_page_object_ids.len())
            .and_then(|count| count.checked_add(2))
            .and_then(|count| count.checked_add(self.checkpoint_cursor.edges().count()))
            .is_none_or(|count| count > AUTHENTICATED_EDGE_PAGE_ENTRIES)
        {
            return Err(corruption(
                "commit edge list exceeds one authenticated edge page",
            ));
        }
        Ok(())
    }

    /// Prepare an oversized member closure as authenticated pages. The full
    /// member vector remains available to the writer-side validation path, but
    /// the persisted commit envelope carries only the authenticated page root.
    pub(crate) fn prepare_member_pages(&mut self) -> Result<Vec<(ObjectId, Bytes)>, StorageError> {
        if !self.member_page_object_ids.is_empty() && self.members.is_empty() {
            return Ok(Vec::new());
        }
        if self.members.is_empty() && self.member_page_object_ids.is_empty() {
            return Ok(Vec::new());
        }
        let pages = CommitChangePageV3::encode_pages(self.commit_id, &self.members)?;
        let page_ids = pages.objects.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        if !self.member_page_object_ids.is_empty() {
            if self.member_page_object_ids != page_ids
                || self.member_page_member_counts != pages.member_counts
            {
                return Err(corruption(
                    "commit change-page directory does not match its ordered member closure",
                ));
            }
            return Ok(pages.objects);
        }
        self.member_page_object_ids = page_ids;
        self.member_page_member_counts = pages.member_counts;
        Ok(pages.objects)
    }

    /// Resolve the complete ordered member closure, authenticating every page
    /// link and every page's position.  This is intentionally a caller-owned
    /// loader: page objects are validated with the same object-domain decoder
    /// as the commit envelope and are never treated as an alternate catalog.
    pub(crate) fn load_members_with(
        &self,
        mut load: impl FnMut(ObjectId) -> Result<Bytes, StorageError>,
    ) -> Result<Vec<CommitMemberV3>, StorageError> {
        if self.member_page_object_ids.is_empty() {
            return Ok(self.members.clone());
        }
        if !self.members.is_empty() {
            return Err(corruption("paged commit carries an inline member closure"));
        }
        let mut output = Vec::new();
        for (page_id, expected_count) in self
            .member_page_object_ids
            .iter()
            .zip(&self.member_page_member_counts)
        {
            let page = CommitChangePageV3::decode(*page_id, &load(*page_id)?)?;
            if page.commit_id != self.commit_id
                || page.start_ordinal
                    != u32::try_from(output.len()).map_err(|_| {
                        corruption("commit member page ordinal exceeds u32 while loading")
                    })?
                || usize::try_from(*expected_count).ok() != Some(page.members.len())
            {
                return Err(corruption(
                    "commit member page chain has a mismatched commit, ordinal, or count",
                ));
            }
            output.extend(page.members);
        }
        let mut unique_changes = BTreeSet::new();
        for member in &output {
            if !unique_changes.insert(member.change_id()) {
                return Err(corruption("commit change page chain repeats a ChangeId"));
            }
        }
        Ok(output)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ChangeObjectV1 {
    Semantic {
        change_id: ChangeId,
        payload: Vec<u8>,
        /// Authenticated edges for large JSON payloads referenced by the
        /// semantic change body. These are part of the Change object rather
        /// than a side-plane lookup, so reachability and corruption checks
        /// retain the payload with the change.
        json_payload_object_ids: Vec<ObjectId>,
    },
    BranchRef {
        change_id: ChangeId,
        updated_at: LixTimestamp,
        branch_id: CanonicalBranchId,
        before_semantic_head_commit_object_id: Option<ObjectId>,
        after_semantic_head_commit_object_id: Option<ObjectId>,
        previous_ref_change_object_id: Option<ObjectId>,
        payload: Vec<u8>,
        json_payload_object_ids: Vec<ObjectId>,
    },
}

impl ChangeObjectV1 {
    pub(crate) fn change_id(&self) -> ChangeId {
        match self {
            Self::Semantic { change_id, .. } | Self::BranchRef { change_id, .. } => *change_id,
        }
    }

    pub(crate) fn encode(&self) -> Result<(ObjectId, Bytes), StorageError> {
        let domain = match self {
            Self::Semantic { .. } => {
                return Err(corruption(
                    "semantic changes are embedded in authenticated commit change pages",
                ));
            }
            Self::BranchRef { .. } => ObjectDomain::BranchRefChange,
        };
        if let Self::BranchRef {
            before_semantic_head_commit_object_id,
            after_semantic_head_commit_object_id,
            previous_ref_change_object_id,
            ..
        } = self
        {
            validate_ref_transition(
                *before_semantic_head_commit_object_id,
                *after_semantic_head_commit_object_id,
            )?;
            for edge in [
                *before_semantic_head_commit_object_id,
                *after_semantic_head_commit_object_id,
                *previous_ref_change_object_id,
            ] {
                if edge == Some(ObjectId::ZERO) {
                    return Err(corruption("branch-ref change contains a zero edge"));
                }
            }
        }
        encode_object(domain, |encoder| {
            encoder.fixed(self.change_id().as_bytes());
            match self {
                Self::Semantic {
                    payload,
                    json_payload_object_ids,
                    ..
                } => {
                    encoder.bytes(payload)?;
                    encode_object_id_list(encoder, json_payload_object_ids)
                }
                Self::BranchRef {
                    updated_at,
                    branch_id,
                    before_semantic_head_commit_object_id,
                    after_semantic_head_commit_object_id,
                    previous_ref_change_object_id,
                    payload,
                    json_payload_object_ids,
                    ..
                } => {
                    encoder.u64(updated_at.packed());
                    encoder.fixed(branch_id.as_bytes());
                    encode_optional_id(encoder, *before_semantic_head_commit_object_id);
                    encode_optional_id(encoder, *after_semantic_head_commit_object_id);
                    encode_optional_id(encoder, *previous_ref_change_object_id);
                    encoder.bytes(payload)?;
                    encode_object_id_list(encoder, json_payload_object_ids)
                }
            }
        })
    }

    pub(crate) fn decode(id: ObjectId, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = decode_object(id, ObjectDomain::BranchRefChange, bytes)?;
        let value = Self::BranchRef {
            change_id: ChangeId::from_bytes(decoder.fixed()?),
            updated_at: LixTimestamp::from_packed(decoder.u64()?)
                .map_err(|error| corruption(format!("invalid branch-ref updated_at: {error}")))?,
            branch_id: CanonicalBranchId::from_bytes(decoder.fixed()?),
            before_semantic_head_commit_object_id: decode_optional_id(
                &mut decoder,
                "branch-ref before edge",
            )?,
            after_semantic_head_commit_object_id: decode_optional_id(
                &mut decoder,
                "branch-ref after edge",
            )?,
            previous_ref_change_object_id: decode_optional_id(
                &mut decoder,
                "branch-ref previous edge",
            )?,
            payload: decoder.bytes("branch-ref payload")?,
            json_payload_object_ids: decode_object_id_list(
                &mut decoder,
                "branch-ref JSON payload edges",
            )?,
        };
        decoder.finish()?;
        if let Self::BranchRef {
            before_semantic_head_commit_object_id,
            after_semantic_head_commit_object_id,
            previous_ref_change_object_id,
            ..
        } = &value
        {
            validate_ref_transition(
                *before_semantic_head_commit_object_id,
                *after_semantic_head_commit_object_id,
            )?;
            for edge in [
                *before_semantic_head_commit_object_id,
                *after_semantic_head_commit_object_id,
                *previous_ref_change_object_id,
            ] {
                if edge == Some(ObjectId::ZERO) {
                    return Err(corruption("branch-ref change contains a zero edge"));
                }
            }
        }
        Ok(value)
    }
}

fn encode_object_id_list(encoder: &mut Encoder, ids: &[ObjectId]) -> Result<(), StorageError> {
    let count = u32::try_from(ids.len())
        .map_err(|_| corruption("JSON payload object edge count exceeds u32"))?;
    encoder.u32(count);
    for id in ids {
        if *id == ObjectId::ZERO || ids.iter().filter(|candidate| **candidate == *id).count() != 1 {
            return Err(corruption(
                "JSON payload object edges contain a zero or duplicate object",
            ));
        }
        encode_id(encoder, *id);
    }
    Ok(())
}

fn decode_object_id_list(
    decoder: &mut Decoder<'_>,
    claim: &str,
) -> Result<Vec<ObjectId>, StorageError> {
    let count = decoder.u32()? as usize;
    if count > AUTHENTICATED_EDGE_PAGE_ENTRIES {
        return Err(corruption(format!(
            "{claim} exceeds authenticated edge bound"
        )));
    }
    let mut ids = Vec::with_capacity(count);
    for _ in 0..count {
        let id = decode_id(decoder)?;
        if id == ObjectId::ZERO || ids.contains(&id) {
            return Err(corruption(format!(
                "{claim} contains a zero or duplicate object"
            )));
        }
        ids.push(id);
    }
    Ok(ids)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CommitCatalogEntry {
    pub(crate) commit_object_id: ObjectId,
}

impl CommitCatalogEntry {
    pub(crate) fn encode(self) -> Result<Vec<u8>, StorageError> {
        if self.commit_object_id == ObjectId::ZERO {
            return Err(corruption("commit catalog entry contains a zero object id"));
        }
        Ok(self.commit_object_id.as_bytes().to_vec())
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let commit_object_id = ObjectId::from_bytes(
            bytes
                .try_into()
                .map_err(|_| corruption("commit catalog value is not 32 bytes"))?,
        );
        if commit_object_id == ObjectId::ZERO {
            return Err(corruption("commit catalog entry contains a zero object id"));
        }
        Ok(Self { commit_object_id })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ChangeCatalogOwner {
    CommitMember {
        commit_object_id: ObjectId,
        ordinal: u32,
    },
    BranchRef {
        ref_change_object_id: ObjectId,
        branch_id: CanonicalBranchId,
    },
    /// One catalog owner for a commit whose introduced semantic changes use
    /// the commit's reserved low-32-bit ordinal address space. The Commit
    /// object and its authenticated change pages remain the payload owner.
    PackedCommit {
        commit_object_id: ObjectId,
        member_count: u32,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ChangeCatalogEntry {
    pub(crate) owner: ChangeCatalogOwner,
}

impl ChangeCatalogEntry {
    pub(crate) fn encode(self) -> Result<Vec<u8>, StorageError> {
        let mut encoder = Encoder::default();
        match self.owner {
            ChangeCatalogOwner::CommitMember {
                commit_object_id,
                ordinal,
            } => {
                if commit_object_id == ObjectId::ZERO {
                    return Err(corruption("change owner contains a zero commit object id"));
                }
                encoder.u8(0);
                encode_id(&mut encoder, commit_object_id);
                encoder.u32(ordinal);
            }
            ChangeCatalogOwner::BranchRef {
                ref_change_object_id,
                branch_id,
            } => {
                if ref_change_object_id == ObjectId::ZERO {
                    return Err(corruption("branch-ref catalog owner is zero"));
                }
                encoder.u8(1);
                encode_id(&mut encoder, ref_change_object_id);
                encoder.fixed(branch_id.as_bytes());
            }
            ChangeCatalogOwner::PackedCommit {
                commit_object_id,
                member_count,
            } => {
                if commit_object_id == ObjectId::ZERO || member_count == 0 {
                    return Err(corruption("packed change owner is empty"));
                }
                encoder.u8(2);
                encode_id(&mut encoder, commit_object_id);
                encoder.u32(member_count);
            }
        }
        Ok(encoder.into_vec())
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = Decoder::after_prefix(bytes, &[])?;
        let owner = match decoder.u8()? {
            0 => ChangeCatalogOwner::CommitMember {
                commit_object_id: decode_id(&mut decoder)?,
                ordinal: decoder.u32()?,
            },
            1 => ChangeCatalogOwner::BranchRef {
                ref_change_object_id: decode_id(&mut decoder)?,
                branch_id: CanonicalBranchId::from_bytes(decoder.fixed()?),
            },
            2 => ChangeCatalogOwner::PackedCommit {
                commit_object_id: decode_id(&mut decoder)?,
                member_count: decoder.u32()?,
            },
            tag => return Err(corruption(format!("unknown change owner tag {tag}"))),
        };
        decoder.finish()?;
        let value = Self { owner };
        let _ = value.encode()?;
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct GlobalSelectorV1 {
    pub(crate) repository_root: ObjectId,
    pub(crate) epoch: u64,
    pub(crate) selector_generation: u64,
}

impl GlobalSelectorV1 {
    pub(crate) fn encode(self) -> Result<Bytes, StorageError> {
        validate_selector(self.repository_root, self.selector_generation, "global")?;
        encode_authenticated(GLOBAL_SELECTOR_DOMAIN, GLOBAL_SELECTOR_MAGIC, |encoder| {
            encode_id(encoder, self.repository_root);
            encoder.u64(self.epoch);
            encoder.u64(self.selector_generation);
            Ok(())
        })
        .map(Bytes::from)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = authenticated_body(GLOBAL_SELECTOR_DOMAIN, GLOBAL_SELECTOR_MAGIC, bytes)?;
        let value = Self {
            repository_root: decode_id(&mut decoder)?,
            epoch: decoder.u64()?,
            selector_generation: decoder.u64()?,
        };
        decoder.finish()?;
        validate_selector(value.repository_root, value.selector_generation, "global")?;
        Ok(value)
    }

    pub(crate) fn rotated(self) -> Result<Self, StorageError> {
        Ok(Self {
            repository_root: self.repository_root,
            epoch: self
                .epoch
                .checked_add(1)
                .ok_or_else(|| corruption("global epoch overflowed"))?,
            selector_generation: self
                .selector_generation
                .checked_add(1)
                .ok_or_else(|| corruption("global selector generation overflowed"))?,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BranchSelectorV1 {
    pub(crate) branch_id: CanonicalBranchId,
    pub(crate) branch_snapshot_object_id: ObjectId,
    pub(crate) selector_generation: u64,
}

impl BranchSelectorV1 {
    pub(crate) fn encode(self) -> Result<Bytes, StorageError> {
        validate_selector(
            self.branch_snapshot_object_id,
            self.selector_generation,
            "branch",
        )?;
        encode_authenticated(BRANCH_SELECTOR_DOMAIN, BRANCH_SELECTOR_MAGIC, |encoder| {
            encoder.fixed(self.branch_id.as_bytes());
            encode_id(encoder, self.branch_snapshot_object_id);
            encoder.u64(self.selector_generation);
            Ok(())
        })
        .map(Bytes::from)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = authenticated_body(BRANCH_SELECTOR_DOMAIN, BRANCH_SELECTOR_MAGIC, bytes)?;
        let value = Self {
            branch_id: CanonicalBranchId::from_bytes(decoder.fixed()?),
            branch_snapshot_object_id: decode_id(&mut decoder)?,
            selector_generation: decoder.u64()?,
        };
        decoder.finish()?;
        validate_selector(
            value.branch_snapshot_object_id,
            value.selector_generation,
            "branch",
        )?;
        Ok(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BlobChunkRefV1 {
    pub(crate) chunk_object_id: ObjectId,
    pub(crate) declared_len: u64,
}

/// One canonical authenticated Merkle leaf. The chunk object remains the
/// payload owner; this node binds its ordinal, length, and content digest into
/// the range-proof tree.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BlobMerkleLeafV1 {
    pub(crate) ordinal: u64,
    pub(crate) chunk_object_id: ObjectId,
    pub(crate) declared_len: u64,
    pub(crate) chunk_digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BlobMerkleNodeRefV1 {
    pub(crate) object_id: ObjectId,
    pub(crate) height: u32,
    pub(crate) first_ordinal: u64,
    pub(crate) leaf_count: u64,
    pub(crate) logical_bytes: u64,
}

/// One canonical internal node. Child summaries are encoded along with child
/// ObjectIds so a proof can validate adjacency and lengths without reading an
/// unrelated subtree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BlobMerkleInternalV1 {
    pub(crate) height: u32,
    pub(crate) first_ordinal: u64,
    pub(crate) leaf_count: u64,
    pub(crate) logical_bytes: u64,
    pub(crate) left: BlobMerkleNodeRefV1,
    pub(crate) right: BlobMerkleNodeRefV1,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BlobChunkV1 {
    pub(crate) bytes: Bytes,
}

impl BlobChunkV1 {
    pub(crate) fn encode(&self) -> Result<(ObjectId, Bytes), StorageError> {
        encode_object(ObjectDomain::BlobChunk, |encoder| {
            encoder.bytes(&self.bytes)
        })
    }

    pub(crate) fn decode(id: ObjectId, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = decode_object(id, ObjectDomain::BlobChunk, bytes)?;
        let value = Self {
            bytes: Bytes::from(decoder.bytes("blob chunk bytes")?),
        };
        decoder.finish()?;
        Ok(value)
    }

    pub(crate) fn decode_borrowed<'a>(
        id: ObjectId,
        bytes: &'a [u8],
    ) -> Result<&'a [u8], StorageError> {
        let mut decoder = decode_object(id, ObjectDomain::BlobChunk, bytes)?;
        let value = decoder.bytes_borrowed("blob chunk bytes")?;
        decoder.finish()?;
        Ok(value)
    }
}

/// The sole authenticated blob layout descriptor. The manifest points to the
/// canonical Merkle root; chunk references and flat whole-content digests are
/// deliberately not part of this durable authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BlobManifestV1 {
    pub(crate) logical_bytes: u64,
    pub(crate) leaf_count: u64,
    pub(crate) root_object_id: ObjectId,
    pub(crate) root_height: u32,
    pub(crate) chunk_bytes: u64,
    pub(crate) canonical_blob_id: BlobId,
}

impl BlobManifestV1 {
    pub(crate) fn from_merkle_root(
        logical_bytes: u64,
        leaf_count: u64,
        root_object_id: ObjectId,
        root_height: u32,
    ) -> Self {
        let canonical_blob_id = canonical_merkle_blob_id(
            root_object_id,
            logical_bytes,
            leaf_count,
            root_height,
            BLOB_MERKLE_CHUNK_BYTES,
        );
        Self {
            logical_bytes,
            leaf_count,
            root_object_id,
            root_height,
            chunk_bytes: BLOB_MERKLE_CHUNK_BYTES,
            canonical_blob_id,
        }
    }

    pub(crate) fn encode(&self) -> Result<(ObjectId, Bytes), StorageError> {
        self.validate()?;
        encode_object(ObjectDomain::BlobManifest, |encoder| {
            encoder.u64(self.logical_bytes);
            encoder.u64(self.leaf_count);
            encoder.u64(self.chunk_bytes);
            encoder.u32(self.root_height);
            encode_id(encoder, self.root_object_id);
            encoder.fixed(self.canonical_blob_id.as_bytes());
            Ok(())
        })
    }

    pub(crate) fn decode(id: ObjectId, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = decode_object(id, ObjectDomain::BlobManifest, bytes)?;
        let logical_bytes = decoder.u64()?;
        let value = Self {
            logical_bytes,
            leaf_count: decoder.u64()?,
            chunk_bytes: decoder.u64()?,
            root_height: decoder.u32()?,
            root_object_id: decode_id(&mut decoder)?,
            canonical_blob_id: BlobId::from_bytes(decoder.fixed()?),
        };
        decoder.finish()?;
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), StorageError> {
        if self.leaf_count == 0
            || self.root_object_id == ObjectId::ZERO
            || self.chunk_bytes != BLOB_MERKLE_CHUNK_BYTES
            || self.leaf_count != self.logical_bytes.div_ceil(self.chunk_bytes).max(1)
            || (self.leaf_count == 1) != (self.root_height == 0)
            || (self.leaf_count > 1 && self.root_height == 0)
        {
            return Err(corruption("blob manifest Merkle geometry is invalid"));
        }
        if self.canonical_blob_id
            != canonical_merkle_blob_id(
                self.root_object_id,
                self.logical_bytes,
                self.leaf_count,
                self.root_height,
                self.chunk_bytes,
            )
        {
            return Err(corruption(
                "blob manifest Merkle BlobId is not root-derived",
            ));
        }
        Ok(())
    }
}

pub(crate) fn canonical_merkle_blob_id(
    root_object_id: ObjectId,
    logical_bytes: u64,
    leaf_count: u64,
    root_height: u32,
    chunk_bytes: u64,
) -> BlobId {
    let mut encoder = Encoder::with_prefix(MERKLE_BLOB_ID_MAGIC);
    encoder.u64(logical_bytes);
    encoder.u64(leaf_count);
    encoder.u32(root_height);
    encoder.u64(chunk_bytes);
    encode_id(&mut encoder, root_object_id);
    BlobId::from_bytes(keyed_hash(MERKLE_BLOB_ID_DOMAIN, &encoder.into_vec()))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct UploadPartV1 {
    pub(crate) upload_id: CanonicalUploadId,
    pub(crate) part_number: u64,
    pub(crate) byte_offset: u64,
    pub(crate) declared_part_len: u64,
    pub(crate) ordered_chunks: Vec<BlobChunkRefV1>,
    pub(crate) part_digest: [u8; 32],
}

impl UploadPartV1 {
    pub(crate) fn encode(&self) -> Result<(ObjectId, Bytes), StorageError> {
        self.validate()?;
        let chunk_count = u32::try_from(self.ordered_chunks.len())
            .map_err(|_| corruption("upload part has too many chunks"))?;
        encode_object(ObjectDomain::UploadPart, |encoder| {
            encoder.bytes(self.upload_id.as_bytes())?;
            encoder.u64(self.part_number);
            encoder.u64(self.byte_offset);
            encoder.u64(self.declared_part_len);
            encoder.u32(chunk_count);
            for chunk in &self.ordered_chunks {
                encode_id(encoder, chunk.chunk_object_id);
                encoder.u64(chunk.declared_len);
            }
            encoder.fixed(&self.part_digest);
            Ok(())
        })
    }

    pub(crate) fn decode(id: ObjectId, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = decode_object(id, ObjectDomain::UploadPart, bytes)?;
        let upload_id = CanonicalUploadId::new(decoder.bytes("upload id")?)?;
        let part_number = decoder.u64()?;
        let byte_offset = decoder.u64()?;
        let declared_part_len = decoder.u64()?;
        let chunk_count = decoder.usize("upload part chunk count")?;
        validate_count(
            chunk_count,
            decoder.remaining(),
            40,
            "upload part chunk count",
        )?;
        let mut ordered_chunks = Vec::with_capacity(chunk_count);
        for _ in 0..chunk_count {
            ordered_chunks.push(BlobChunkRefV1 {
                chunk_object_id: decode_id(&mut decoder)?,
                declared_len: decoder.u64()?,
            });
        }
        let value = Self {
            upload_id,
            part_number,
            byte_offset,
            declared_part_len,
            ordered_chunks,
            part_digest: decoder.fixed()?,
        };
        decoder.finish()?;
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), StorageError> {
        if self.ordered_chunks.len() > AUTHENTICATED_EDGE_PAGE_ENTRIES {
            return Err(corruption(
                "upload part exceeds one authenticated edge page; split the part",
            ));
        }
        let mut encoded_len = 0_u64;
        for chunk in &self.ordered_chunks {
            if chunk.chunk_object_id == ObjectId::ZERO || chunk.declared_len == 0 {
                return Err(corruption(
                    "upload part contains an invalid chunk reference",
                ));
            }
            encoded_len = encoded_len
                .checked_add(chunk.declared_len)
                .ok_or_else(|| corruption("upload part chunk lengths overflow u64"))?;
        }
        if encoded_len != self.declared_part_len {
            return Err(corruption(
                "upload part chunk lengths do not equal declared part length",
            ));
        }
        self.byte_offset
            .checked_add(self.declared_part_len)
            .ok_or_else(|| corruption("upload part byte range overflows u64"))?;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct UploadProgressV1 {
    pub(crate) upload_id: CanonicalUploadId,
    pub(crate) binding_digest: [u8; 32],
    pub(crate) receipt_tree_root: ObjectId,
    pub(crate) completed_part_count: u64,
    pub(crate) received_bytes: u64,
    pub(crate) contiguous_prefix_bytes: u64,
}

impl UploadProgressV1 {
    pub(crate) fn encode(&self) -> Result<(ObjectId, Bytes), StorageError> {
        self.validate()?;
        encode_object(ObjectDomain::UploadProgress, |encoder| {
            encoder.bytes(self.upload_id.as_bytes())?;
            encoder.fixed(&self.binding_digest);
            encode_id(encoder, self.receipt_tree_root);
            encoder.u64(self.completed_part_count);
            encoder.u64(self.received_bytes);
            encoder.u64(self.contiguous_prefix_bytes);
            Ok(())
        })
    }

    pub(crate) fn decode(id: ObjectId, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = decode_object(id, ObjectDomain::UploadProgress, bytes)?;
        let value = Self {
            upload_id: CanonicalUploadId::new(decoder.bytes("upload id")?)?,
            binding_digest: decoder.fixed()?,
            receipt_tree_root: decode_id(&mut decoder)?,
            completed_part_count: decoder.u64()?,
            received_bytes: decoder.u64()?,
            contiguous_prefix_bytes: decoder.u64()?,
        };
        decoder.finish()?;
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), StorageError> {
        if self.receipt_tree_root == ObjectId::ZERO {
            return Err(corruption("upload progress contains a zero receipt root"));
        }
        if self.contiguous_prefix_bytes > self.received_bytes {
            return Err(corruption(
                "upload progress contiguous prefix exceeds received bytes",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct UploadSelectorV1 {
    pub(crate) upload_id: CanonicalUploadId,
    pub(crate) binding_digest: [u8; 32],
    pub(crate) progress_object_id: ObjectId,
    pub(crate) selector_generation: u64,
}

impl UploadSelectorV1 {
    pub(crate) fn encode(&self) -> Result<Bytes, StorageError> {
        validate_selector(self.progress_object_id, self.selector_generation, "upload")?;
        encode_authenticated(UPLOAD_SELECTOR_DOMAIN, UPLOAD_SELECTOR_MAGIC, |encoder| {
            encoder.bytes(self.upload_id.as_bytes())?;
            encoder.fixed(&self.binding_digest);
            encode_id(encoder, self.progress_object_id);
            encoder.u64(self.selector_generation);
            Ok(())
        })
        .map(Bytes::from)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = authenticated_body(UPLOAD_SELECTOR_DOMAIN, UPLOAD_SELECTOR_MAGIC, bytes)?;
        let value = Self {
            upload_id: CanonicalUploadId::new(decoder.bytes("upload id")?)?,
            binding_digest: decoder.fixed()?,
            progress_object_id: decode_id(&mut decoder)?,
            selector_generation: decoder.u64()?,
        };
        decoder.finish()?;
        validate_selector(
            value.progress_object_id,
            value.selector_generation,
            "upload",
        )?;
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum SnapshotRole {
    Checkpoint = 1,
    Recovery = 2,
}

impl SnapshotRole {
    pub(super) fn decode(value: u8) -> Result<Self, StorageError> {
        match value {
            1 => Ok(Self::Checkpoint),
            2 => Ok(Self::Recovery),
            _ => Err(corruption(format!(
                "unknown retained snapshot role {value}"
            ))),
        }
    }

    pub(super) fn key_prefix(self) -> &'static [u8] {
        match self {
            Self::Checkpoint => b"checkpoint/",
            Self::Recovery => b"recovery/",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SnapshotTargetV1 {
    pub(crate) role: SnapshotRole,
    pub(crate) selector_id: SnapshotSelectorId,
    pub(crate) branch_id: CanonicalBranchId,
    pub(crate) branch_snapshot_object_id: ObjectId,
    pub(crate) semantic_commit_object_id: ObjectId,
}

impl SnapshotTargetV1 {
    pub(crate) fn encode(self) -> Result<(ObjectId, Bytes), StorageError> {
        validate_nonzero_ids(
            "retained snapshot target",
            &[
                self.branch_snapshot_object_id,
                self.semantic_commit_object_id,
            ],
        )?;
        encode_object(ObjectDomain::SnapshotTarget, |encoder| {
            encoder.u8(self.role as u8);
            encoder.fixed(self.selector_id.as_bytes());
            encoder.fixed(self.branch_id.as_bytes());
            encode_id(encoder, self.branch_snapshot_object_id);
            encode_id(encoder, self.semantic_commit_object_id);
            Ok(())
        })
    }

    pub(crate) fn decode(id: ObjectId, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = decode_object(id, ObjectDomain::SnapshotTarget, bytes)?;
        let value = Self {
            role: SnapshotRole::decode(decoder.u8()?)?,
            selector_id: SnapshotSelectorId::from_bytes(decoder.fixed()?),
            branch_id: CanonicalBranchId::from_bytes(decoder.fixed()?),
            branch_snapshot_object_id: decode_id(&mut decoder)?,
            semantic_commit_object_id: decode_id(&mut decoder)?,
        };
        decoder.finish()?;
        validate_nonzero_ids(
            "retained snapshot target",
            &[
                value.branch_snapshot_object_id,
                value.semantic_commit_object_id,
            ],
        )?;
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SnapshotSelectorV1 {
    pub(crate) role: SnapshotRole,
    pub(crate) selector_id: SnapshotSelectorId,
    pub(crate) target_object_id: ObjectId,
    pub(crate) selector_generation: u64,
}

impl SnapshotSelectorV1 {
    pub(crate) fn encode(self) -> Result<Bytes, StorageError> {
        validate_selector(
            self.target_object_id,
            self.selector_generation,
            "retained snapshot",
        )?;
        encode_authenticated(
            SNAPSHOT_SELECTOR_DOMAIN,
            SNAPSHOT_SELECTOR_MAGIC,
            |encoder| {
                encoder.u8(self.role as u8);
                encoder.fixed(self.selector_id.as_bytes());
                encode_id(encoder, self.target_object_id);
                encoder.u64(self.selector_generation);
                Ok(())
            },
        )
        .map(Bytes::from)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder =
            authenticated_body(SNAPSHOT_SELECTOR_DOMAIN, SNAPSHOT_SELECTOR_MAGIC, bytes)?;
        let value = Self {
            role: SnapshotRole::decode(decoder.u8()?)?,
            selector_id: SnapshotSelectorId::from_bytes(decoder.fixed()?),
            target_object_id: decode_id(&mut decoder)?,
            selector_generation: decoder.u64()?,
        };
        decoder.finish()?;
        validate_selector(
            value.target_object_id,
            value.selector_generation,
            "retained snapshot",
        )?;
        Ok(value)
    }
}

pub(super) const GC_CURSOR_MAX_BYTES: usize = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum GcPhaseV2 {
    RootSelectors = 1,
    Traverse = 2,
    Sweep = 3,
    Cleanup = 4,
}

impl GcPhaseV2 {
    fn decode(value: u8) -> Result<Self, StorageError> {
        match value {
            1 => Ok(Self::RootSelectors),
            2 => Ok(Self::Traverse),
            3 => Ok(Self::Sweep),
            4 => Ok(Self::Cleanup),
            _ => Err(corruption(format!("unknown GC phase {value}"))),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(super) enum GcRadixKindV1 {
    LiveBranch = 1,
    Mark = 2,
    Queue = 3,
}

impl GcRadixKindV1 {
    fn decode(value: u8) -> Result<Self, StorageError> {
        match value {
            1 => Ok(Self::LiveBranch),
            2 => Ok(Self::Mark),
            3 => Ok(Self::Queue),
            _ => Err(corruption(format!("unknown GC radix kind {value}"))),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct GcMarkEntryV2 {
    pub(super) object_id: ObjectId,
    pub(super) expected_domain: u16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GcMarkPackV2 {
    pub(super) cycle_id: [u8; 16],
    pub(super) consumed_prefix: Vec<u8>,
    pub(super) entries: Vec<GcMarkEntryV2>,
}

impl GcMarkPackV2 {
    pub(super) const MAX_ENTRIES: usize = 4096;

    pub(super) fn encode(&self) -> Result<(ObjectId, Bytes), StorageError> {
        self.validate()?;
        encode_object(ObjectDomain::GcMarkPackV2, |encoder| {
            encoder.fixed(&self.cycle_id);
            encoder.bytes(&self.consumed_prefix)?;
            encoder.u32(self.entries.len() as u32);
            for entry in &self.entries {
                encode_id(encoder, entry.object_id);
                encoder.u32(u32::from(entry.expected_domain));
            }
            Ok(())
        })
    }

    pub(super) fn decode(id: ObjectId, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = decode_object(id, ObjectDomain::GcMarkPackV2, bytes)?;
        let cycle_id = decoder.fixed()?;
        let consumed_prefix = decoder.bytes("GC mark consumed prefix")?;
        let count = decoder.usize("GC mark entry count")?;
        if count == 0 || count > Self::MAX_ENTRIES || count > decoder.remaining() / 36 {
            return Err(corruption("GC mark pack count is invalid"));
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            entries.push(GcMarkEntryV2 {
                object_id: decode_id(&mut decoder)?,
                expected_domain: u16::try_from(decoder.u32()?)
                    .map_err(|_| corruption("GC mark domain exceeds u16"))?,
            });
        }
        decoder.finish()?;
        let value = Self {
            cycle_id,
            consumed_prefix,
            entries,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), StorageError> {
        validate_gc_cycle(self.cycle_id)?;
        if self.consumed_prefix.len() > 32
            || self.entries.is_empty()
            || self.entries.len() > Self::MAX_ENTRIES
        {
            return Err(corruption("GC mark pack exceeds its bounded contract"));
        }
        let mut previous = None;
        for entry in &self.entries {
            validate_gc_object_claim(entry.object_id, entry.expected_domain)?;
            if previous.is_some_and(|id| id >= entry.object_id) {
                return Err(corruption("GC mark entries are not strictly ordered"));
            }
            if !entry
                .object_id
                .as_bytes()
                .starts_with(&self.consumed_prefix)
            {
                return Err(corruption("GC mark entry is outside its consumed prefix"));
            }
            previous = Some(entry.object_id);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GcEdgeCursorV1 {
    pub(super) source_object_id: ObjectId,
    pub(super) source_domain: u16,
    pub(super) next_edge_ordinal: u64,
    pub(super) owner_cursor: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GcQueueEntryV1 {
    pub(super) sequence: u64,
    pub(super) object_id: ObjectId,
    pub(super) expected_domain: u16,
    pub(super) edge_cursor: Option<GcEdgeCursorV1>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GcQueuePackV1 {
    pub(super) cycle_id: [u8; 16],
    pub(super) entries: Vec<GcQueueEntryV1>,
}

impl GcQueuePackV1 {
    pub(super) const MAX_ENTRIES: usize = 1024;

    pub(super) fn encode(&self) -> Result<(ObjectId, Bytes), StorageError> {
        self.validate()?;
        encode_object(ObjectDomain::GcQueuePackV1, |encoder| {
            encoder.fixed(&self.cycle_id);
            encoder.u32(self.entries.len() as u32);
            for entry in &self.entries {
                encoder.u64(entry.sequence);
                encode_id(encoder, entry.object_id);
                encoder.u32(u32::from(entry.expected_domain));
                match &entry.edge_cursor {
                    None => encoder.u8(0),
                    Some(cursor) => {
                        encoder.u8(1);
                        encode_id(encoder, cursor.source_object_id);
                        encoder.u32(u32::from(cursor.source_domain));
                        encoder.u64(cursor.next_edge_ordinal);
                        encoder.bytes(&cursor.owner_cursor)?;
                    }
                }
            }
            Ok(())
        })
    }

    pub(super) fn decode(id: ObjectId, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = decode_object(id, ObjectDomain::GcQueuePackV1, bytes)?;
        let cycle_id = decoder.fixed()?;
        let count = decoder.usize("GC queue entry count")?;
        if count == 0 || count > Self::MAX_ENTRIES || count > decoder.remaining() / 44 {
            return Err(corruption("GC queue pack count is invalid"));
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let sequence = decoder.u64()?;
            let object_id = decode_id(&mut decoder)?;
            let expected_domain = u16::try_from(decoder.u32()?)
                .map_err(|_| corruption("GC queue domain exceeds u16"))?;
            let edge_cursor = match decoder.u8()? {
                0 => None,
                1 => Some(GcEdgeCursorV1 {
                    source_object_id: decode_id(&mut decoder)?,
                    source_domain: u16::try_from(decoder.u32()?)
                        .map_err(|_| corruption("GC edge cursor domain exceeds u16"))?,
                    next_edge_ordinal: decoder.u64()?,
                    owner_cursor: decoder.bytes("GC edge owner cursor")?,
                }),
                tag => return Err(corruption(format!("GC edge cursor tag {tag} is invalid"))),
            };
            entries.push(GcQueueEntryV1 {
                sequence,
                object_id,
                expected_domain,
                edge_cursor,
            });
        }
        decoder.finish()?;
        let value = Self { cycle_id, entries };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), StorageError> {
        validate_gc_cycle(self.cycle_id)?;
        if self.entries.is_empty() || self.entries.len() > Self::MAX_ENTRIES {
            return Err(corruption("GC queue pack exceeds its bounded contract"));
        }
        let mut previous = None;
        for entry in &self.entries {
            if previous.is_some_and(|sequence| sequence >= entry.sequence) {
                return Err(corruption("GC queue sequences are not strictly increasing"));
            }
            validate_gc_object_claim(entry.object_id, entry.expected_domain)?;
            if let Some(cursor) = &entry.edge_cursor {
                validate_gc_object_claim(cursor.source_object_id, cursor.source_domain)?;
                if cursor.source_object_id != entry.object_id
                    || cursor.source_domain != entry.expected_domain
                    || cursor.next_edge_ordinal == 0
                    || cursor.owner_cursor.len() > GC_CURSOR_MAX_BYTES
                {
                    return Err(corruption("GC edge cursor is inconsistent or unbounded"));
                }
            }
            previous = Some(entry.sequence);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct GcLiveBranchEntryV1 {
    pub(super) key_digest: [u8; 32],
    pub(super) branch_id: CanonicalBranchId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GcLiveBranchPackV1 {
    pub(super) cycle_id: [u8; 16],
    pub(super) entries: Vec<GcLiveBranchEntryV1>,
}

impl GcLiveBranchPackV1 {
    pub(super) const MAX_ENTRIES: usize = 4096;

    pub(super) fn encode(&self) -> Result<(ObjectId, Bytes), StorageError> {
        self.validate()?;
        encode_object(ObjectDomain::GcLiveBranchPackV1, |encoder| {
            encoder.fixed(&self.cycle_id);
            encoder.u32(self.entries.len() as u32);
            for entry in &self.entries {
                encoder.fixed(&entry.key_digest);
                encoder.fixed(entry.branch_id.as_bytes());
            }
            Ok(())
        })
    }

    pub(super) fn decode(id: ObjectId, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = decode_object(id, ObjectDomain::GcLiveBranchPackV1, bytes)?;
        let cycle_id = decoder.fixed()?;
        let count = decoder.usize("GC live branch count")?;
        if count == 0 || count > Self::MAX_ENTRIES || count > decoder.remaining() / 48 {
            return Err(corruption("GC live branch pack count is invalid"));
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            entries.push(GcLiveBranchEntryV1 {
                key_digest: decoder.fixed()?,
                branch_id: CanonicalBranchId::from_bytes(decoder.fixed()?),
            });
        }
        decoder.finish()?;
        let value = Self { cycle_id, entries };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), StorageError> {
        validate_gc_cycle(self.cycle_id)?;
        if self.entries.is_empty() || self.entries.len() > Self::MAX_ENTRIES {
            return Err(corruption(
                "GC live branch pack exceeds its bounded contract",
            ));
        }
        if self
            .entries
            .windows(2)
            .any(|pair| pair[0].key_digest >= pair[1].key_digest)
        {
            return Err(corruption(
                "GC live branch entries are not strictly ordered",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GcRadixNodeV1 {
    pub(super) cycle_id: [u8; 16],
    pub(super) kind: GcRadixKindV1,
    pub(super) consumed_prefix: Vec<u8>,
    pub(super) child_bitmap: [u8; 32],
    pub(super) child_object_ids: Vec<ObjectId>,
}

impl GcRadixNodeV1 {
    pub(super) fn encode(&self) -> Result<(ObjectId, Bytes), StorageError> {
        self.validate()?;
        encode_object(ObjectDomain::GcRadixNodeV1, |encoder| {
            encoder.fixed(&self.cycle_id);
            encoder.u8(self.kind as u8);
            encoder.u8(self.consumed_prefix.len() as u8);
            encoder.fixed(&self.consumed_prefix);
            encoder.fixed(&self.child_bitmap);
            for child in &self.child_object_ids {
                encode_id(encoder, *child);
            }
            Ok(())
        })
    }

    pub(super) fn decode(id: ObjectId, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = decode_object(id, ObjectDomain::GcRadixNodeV1, bytes)?;
        let cycle_id = decoder.fixed()?;
        let kind = GcRadixKindV1::decode(decoder.u8()?)?;
        let prefix_len = usize::from(decoder.u8()?);
        if prefix_len > 32 {
            return Err(corruption("GC radix prefix exceeds ObjectId width"));
        }
        let consumed_prefix = decoder.take(prefix_len)?.to_vec();
        let child_bitmap = decoder.fixed()?;
        let child_count = child_bitmap
            .iter()
            .map(|byte| byte.count_ones() as usize)
            .sum();
        if child_count == 0 || child_count > 256 || child_count > decoder.remaining() / 32 {
            return Err(corruption("GC radix child bitmap is invalid"));
        }
        let mut child_object_ids = Vec::with_capacity(child_count);
        for _ in 0..child_count {
            child_object_ids.push(decode_id(&mut decoder)?);
        }
        decoder.finish()?;
        let value = Self {
            cycle_id,
            kind,
            consumed_prefix,
            child_bitmap,
            child_object_ids,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), StorageError> {
        validate_gc_cycle(self.cycle_id)?;
        let child_count = self
            .child_bitmap
            .iter()
            .map(|byte| byte.count_ones() as usize)
            .sum::<usize>();
        if self.consumed_prefix.len() > 32
            || child_count == 0
            || child_count != self.child_object_ids.len()
            || self.child_object_ids.contains(&ObjectId::ZERO)
        {
            return Err(corruption("GC radix node violates its bounded shape"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GcProgressV2 {
    pub(super) cycle_id: [u8; 16],
    pub(super) phase: GcPhaseV2,
    pub(super) expected_global_digest: [u8; 32],
    pub(super) expected_global_epoch: u64,
    pub(super) selector_resume_after: Option<Vec<u8>>,
    pub(super) object_resume_after: Option<ObjectId>,
    pub(super) maintenance_resume_after: Option<ObjectId>,
    pub(super) saw_global_selector: bool,
    pub(super) live_branch_index_root: Option<ObjectId>,
    pub(super) mark_index_root: Option<ObjectId>,
    pub(super) queue_index_root: Option<ObjectId>,
    pub(super) queue_pop_sequence: u64,
    pub(super) queue_push_sequence: u64,
    pub(super) marked_count: u64,
    pub(super) validated_count: u64,
    pub(super) reclaimed_count: u64,
}

impl GcProgressV2 {
    pub(super) fn encode(&self) -> Result<(ObjectId, Bytes), StorageError> {
        self.validate()?;
        encode_object(ObjectDomain::GcProgressV2, |encoder| {
            encoder.fixed(&self.cycle_id);
            encoder.u8(self.phase as u8);
            encoder.fixed(&self.expected_global_digest);
            encoder.u64(self.expected_global_epoch);
            encode_optional_bounded_bytes(encoder, self.selector_resume_after.as_deref())?;
            encode_optional_id(encoder, self.object_resume_after);
            encode_optional_id(encoder, self.maintenance_resume_after);
            encoder.u8(u8::from(self.saw_global_selector));
            encode_optional_id(encoder, self.live_branch_index_root);
            encode_optional_id(encoder, self.mark_index_root);
            encode_optional_id(encoder, self.queue_index_root);
            encoder.u64(self.queue_pop_sequence);
            encoder.u64(self.queue_push_sequence);
            encoder.u64(self.marked_count);
            encoder.u64(self.validated_count);
            encoder.u64(self.reclaimed_count);
            Ok(())
        })
    }

    pub(super) fn decode(id: ObjectId, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = decode_object(id, ObjectDomain::GcProgressV2, bytes)?;
        let value = Self {
            cycle_id: decoder.fixed()?,
            phase: GcPhaseV2::decode(decoder.u8()?)?,
            expected_global_digest: decoder.fixed()?,
            expected_global_epoch: decoder.u64()?,
            selector_resume_after: decode_optional_bounded_bytes(
                &mut decoder,
                "GC selector cursor",
            )?,
            object_resume_after: decode_optional_id(&mut decoder, "GC object cursor")?,
            maintenance_resume_after: decode_optional_id(&mut decoder, "GC maintenance cursor")?,
            saw_global_selector: match decoder.u8()? {
                0 => false,
                1 => true,
                tag => return Err(corruption(format!("GC global-seen tag {tag} is invalid"))),
            },
            live_branch_index_root: decode_optional_id(&mut decoder, "GC live branch root")?,
            mark_index_root: decode_optional_id(&mut decoder, "GC mark root")?,
            queue_index_root: decode_optional_id(&mut decoder, "GC queue root")?,
            queue_pop_sequence: decoder.u64()?,
            queue_push_sequence: decoder.u64()?,
            marked_count: decoder.u64()?,
            validated_count: decoder.u64()?,
            reclaimed_count: decoder.u64()?,
        };
        decoder.finish()?;
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), StorageError> {
        validate_gc_cycle(self.cycle_id)?;
        if self.expected_global_digest == [0; 32]
            || self.expected_global_epoch == 0
            || self
                .selector_resume_after
                .as_ref()
                .is_some_and(|value| value.len() > GC_CURSOR_MAX_BYTES)
            || self.object_resume_after == Some(ObjectId::ZERO)
            || self.maintenance_resume_after == Some(ObjectId::ZERO)
            || self.live_branch_index_root == Some(ObjectId::ZERO)
            || self.mark_index_root == Some(ObjectId::ZERO)
            || self.queue_index_root == Some(ObjectId::ZERO)
            || self.queue_pop_sequence > self.queue_push_sequence
            || self.validated_count > self.marked_count
        {
            return Err(corruption(
                "GC progress violates its authenticated invariants",
            ));
        }
        let queue_nonempty = self.queue_pop_sequence < self.queue_push_sequence;
        if queue_nonempty != self.queue_index_root.is_some()
            || (self.phase != GcPhaseV2::Cleanup
                && (self.marked_count == 0) != self.mark_index_root.is_none())
        {
            return Err(corruption(
                "GC progress queue/mark roots disagree with their authenticated counts",
            ));
        }
        let phase_valid = match self.phase {
            GcPhaseV2::RootSelectors => {
                self.object_resume_after.is_none()
                    && self.maintenance_resume_after.is_none()
                    && self.validated_count == 0
            }
            GcPhaseV2::Traverse => {
                self.saw_global_selector
                    && self.selector_resume_after.is_none()
                    && self.object_resume_after.is_none()
                    && self.maintenance_resume_after.is_none()
                    && self.mark_index_root.is_some()
            }
            GcPhaseV2::Sweep => {
                self.saw_global_selector
                    && self.selector_resume_after.is_none()
                    && self.maintenance_resume_after.is_none()
                    && self.mark_index_root.is_some()
                    && self.queue_index_root.is_none()
                    && self.queue_pop_sequence == self.queue_push_sequence
                    && self.validated_count == self.marked_count
            }
            GcPhaseV2::Cleanup => {
                self.saw_global_selector
                    && self.selector_resume_after.is_none()
                    && self.object_resume_after.is_none()
                    && self.live_branch_index_root.is_none()
                    && self.mark_index_root.is_none()
                    && self.queue_index_root.is_none()
                    && self.queue_pop_sequence == self.queue_push_sequence
                    && self.validated_count == self.marked_count
            }
        };
        if !phase_valid {
            return Err(corruption(
                "GC progress phase disagrees with its authenticated cursors or roots",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct GcProgressSelectorV2 {
    pub(super) cycle_id: [u8; 16],
    pub(super) progress_object_id: ObjectId,
    pub(super) selector_generation: u64,
}

impl GcProgressSelectorV2 {
    pub(super) fn encode(self) -> Result<Bytes, StorageError> {
        validate_gc_cycle(self.cycle_id)?;
        validate_selector(
            self.progress_object_id,
            self.selector_generation,
            "GC progress",
        )?;
        encode_authenticated(GC_SELECTOR_DOMAIN, GC_SELECTOR_MAGIC, |encoder| {
            encoder.fixed(&self.cycle_id);
            encode_id(encoder, self.progress_object_id);
            encoder.u64(self.selector_generation);
            Ok(())
        })
        .map(Bytes::from)
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = authenticated_body(GC_SELECTOR_DOMAIN, GC_SELECTOR_MAGIC, bytes)?;
        let value = Self {
            cycle_id: decoder.fixed()?,
            progress_object_id: decode_id(&mut decoder)?,
            selector_generation: decoder.u64()?,
        };
        decoder.finish()?;
        validate_gc_cycle(value.cycle_id)?;
        validate_selector(
            value.progress_object_id,
            value.selector_generation,
            "GC progress",
        )?;
        Ok(value)
    }
}

fn validate_gc_cycle(cycle_id: [u8; 16]) -> Result<(), StorageError> {
    if cycle_id == [0; 16] {
        Err(corruption("GC cycle ID is zero"))
    } else {
        Ok(())
    }
}

fn validate_gc_object_claim(id: ObjectId, domain: u16) -> Result<(), StorageError> {
    if id == ObjectId::ZERO {
        return Err(corruption("GC object claim contains a zero ID"));
    }
    let _ = ObjectDomain::decode(domain)?;
    Ok(())
}

fn encode_optional_bounded_bytes(
    encoder: &mut Encoder,
    value: Option<&[u8]>,
) -> Result<(), StorageError> {
    match value {
        None => encoder.u8(0),
        Some(value) => {
            if value.len() > GC_CURSOR_MAX_BYTES {
                return Err(corruption("GC cursor exceeds its protocol maximum"));
            }
            encoder.u8(1);
            encoder.bytes(value)?;
        }
    }
    Ok(())
}

fn decode_optional_bounded_bytes(
    decoder: &mut Decoder<'_>,
    label: &str,
) -> Result<Option<Vec<u8>>, StorageError> {
    match decoder.u8()? {
        0 => Ok(None),
        1 => {
            let value = decoder.bytes(label)?;
            if value.len() > GC_CURSOR_MAX_BYTES {
                return Err(corruption(format!("{label} exceeds its protocol maximum")));
            }
            Ok(Some(value))
        }
        tag => Err(corruption(format!("{label} has invalid option tag {tag}"))),
    }
}

pub(crate) fn upload_binding_digest(
    repository_identity: &[u8],
    path: &[u8],
    payload_domain: &[u8],
    declared_total_size: u64,
    declared_final_hash: Option<[u8; 32]>,
) -> Result<[u8; 32], StorageError> {
    let mut encoder = Encoder::default();
    encoder.bytes(repository_identity)?;
    encoder.bytes(path)?;
    encoder.bytes(payload_domain)?;
    encoder.u64(declared_total_size);
    encoder.optional_fixed(declared_final_hash.as_ref().map(<[u8; 32]>::as_slice));
    Ok(keyed_hash(UPLOAD_BINDING_DOMAIN, &encoder.into_vec()))
}

fn validate_ref_transition(
    before: Option<ObjectId>,
    after: Option<ObjectId>,
) -> Result<(), StorageError> {
    if before.is_none() && after.is_none() {
        return Err(corruption(
            "branch-ref change cannot have absent before and after targets",
        ));
    }
    if before == after {
        return Err(corruption(
            "branch-ref change before and after targets must differ",
        ));
    }
    Ok(())
}

fn validate_selector(
    object_id: ObjectId,
    selector_generation: u64,
    role: &str,
) -> Result<(), StorageError> {
    if object_id == ObjectId::ZERO {
        return Err(corruption(format!(
            "{role} selector contains a zero object id"
        )));
    }
    if selector_generation == 0 {
        return Err(corruption(format!(
            "{role} selector generation must be nonzero"
        )));
    }
    Ok(())
}

fn validate_nonzero_ids(label: &str, ids: &[ObjectId]) -> Result<(), StorageError> {
    if ids.contains(&ObjectId::ZERO) {
        return Err(corruption(format!("{label} contains a zero object id")));
    }
    Ok(())
}

fn validate_count(
    count: usize,
    remaining: usize,
    minimum_bytes_per_item: usize,
    label: &str,
) -> Result<(), StorageError> {
    if count > remaining / minimum_bytes_per_item {
        return Err(corruption(format!("{label} exceeds encoded body")));
    }
    Ok(())
}

pub(super) fn global_selector_key() -> Bytes {
    Bytes::from_static(b"global")
}

pub(super) fn branch_selector_key(branch_id: CanonicalBranchId) -> Bytes {
    let mut key = Vec::with_capacity(7 + 16);
    key.extend_from_slice(b"branch/");
    key.extend_from_slice(branch_id.as_bytes());
    Bytes::from(key)
}

pub(super) fn upload_selector_key(upload_id: &CanonicalUploadId) -> Result<Bytes, StorageError> {
    let mut encoder = Encoder::with_prefix(b"upload/");
    encoder.bytes(upload_id.as_bytes())?;
    Ok(Bytes::from(encoder.into_vec()))
}

pub(crate) fn snapshot_selector_key(role: SnapshotRole, selector_id: SnapshotSelectorId) -> Bytes {
    let mut key = Vec::with_capacity(role.key_prefix().len() + 16);
    key.extend_from_slice(role.key_prefix());
    key.extend_from_slice(selector_id.as_bytes());
    Bytes::from(key)
}

pub(super) fn gc_progress_selector_key() -> Bytes {
    Bytes::from_static(b"gc-progress")
}

#[cfg(test)]
mod tests {
    use super::{ChangeCatalogEntry, ChangeCatalogOwner, CommitChangePageV3, CommitId, ObjectId};

    #[test]
    fn empty_commit_change_pages_are_a_valid_empty_closure() {
        let pages = CommitChangePageV3::encode_pages(CommitId::from_bytes([0x42; 16]), &[])
            .expect("empty commit page closure should be encodable");
        assert!(pages.member_locations.is_empty());
        assert!(pages.objects.is_empty());
    }

    #[test]
    fn packed_commit_catalog_owner_round_trips() {
        let entry = ChangeCatalogEntry {
            owner: ChangeCatalogOwner::PackedCommit {
                commit_object_id: ObjectId::from_bytes([0x24; 32]),
                member_count: 257,
            },
        };
        let encoded = entry.encode().expect("encode packed commit owner");
        assert_eq!(
            ChangeCatalogEntry::decode(&encoded).expect("decode packed commit owner"),
            entry
        );
    }
}
