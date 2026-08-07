use bytes::Bytes;

use crate::storage::StorageError;

use super::codec::{
    Decoder, Encoder, authenticated_body, corruption, encode_authenticated, keyed_hash,
};
use super::object::{
    ObjectDomain, ObjectId, decode_id, decode_object, decode_optional_id, encode_id, encode_object,
    encode_optional_id,
};

const GLOBAL_SELECTOR_MAGIC: &[u8; 8] = b"LIXFTG\0\x01";
const BRANCH_SELECTOR_MAGIC: &[u8; 8] = b"LIXFTB\0\x01";
const UPLOAD_SELECTOR_MAGIC: &[u8; 8] = b"LIXFTU\0\x01";
const GLOBAL_SELECTOR_DOMAIN: &str = "lix forktree global selector v1";
const BRANCH_SELECTOR_DOMAIN: &str = "lix forktree branch selector v1";
const UPLOAD_SELECTOR_DOMAIN: &str = "lix forktree upload selector v1";
const UPLOAD_BINDING_DOMAIN: &str = "lix forktree upload binding v1";

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

            pub(crate) const fn into_bytes(self) -> [u8; 16] {
                self.0
            }
        }
    };
}

raw_uuid_id!(CommitId);
raw_uuid_id!(ChangeId);
raw_uuid_id!(CanonicalBranchId);

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
    pub(crate) retention_policy_root: ObjectId,
}

impl RepositoryRootV1 {
    pub(crate) fn encode(self) -> Result<(ObjectId, Bytes), StorageError> {
        validate_nonzero_ids(
            "repository root",
            &[
                self.global_state_root,
                self.commit_catalog_root,
                self.change_catalog_root,
                self.retention_policy_root,
            ],
        )?;
        encode_object(ObjectDomain::RepositoryRoot, |encoder| {
            encode_id(encoder, self.global_state_root);
            encode_id(encoder, self.commit_catalog_root);
            encode_id(encoder, self.change_catalog_root);
            encode_id(encoder, self.retention_policy_root);
            Ok(())
        })
    }

    pub(crate) fn decode(id: ObjectId, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = decode_object(id, ObjectDomain::RepositoryRoot, bytes)?;
        let value = Self {
            global_state_root: decode_id(&mut decoder)?,
            commit_catalog_root: decode_id(&mut decoder)?,
            change_catalog_root: decode_id(&mut decoder)?,
            retention_policy_root: decode_id(&mut decoder)?,
        };
        decoder.finish()?;
        validate_nonzero_ids(
            "repository root",
            &[
                value.global_state_root,
                value.commit_catalog_root,
                value.change_catalog_root,
                value.retention_policy_root,
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
pub(crate) struct CommitObjectV1 {
    pub(crate) commit_id: CommitId,
    pub(crate) generation: u64,
    pub(crate) parent_commit_object_ids: Vec<ObjectId>,
    pub(crate) member_change_object_ids: Vec<ObjectId>,
    pub(crate) global_state_root: ObjectId,
    pub(crate) local_state_root: ObjectId,
    pub(crate) metadata: Vec<u8>,
}

impl CommitObjectV1 {
    pub(crate) fn encode(&self) -> Result<(ObjectId, Bytes), StorageError> {
        validate_nonzero_ids("commit parent", &self.parent_commit_object_ids)?;
        validate_nonzero_ids("commit member", &self.member_change_object_ids)?;
        validate_nonzero_ids(
            "commit state",
            &[self.global_state_root, self.local_state_root],
        )?;
        let parent_count = u32::try_from(self.parent_commit_object_ids.len())
            .map_err(|_| corruption("commit has too many parents"))?;
        let member_count = u32::try_from(self.member_change_object_ids.len())
            .map_err(|_| corruption("commit has too many member changes"))?;
        encode_object(ObjectDomain::Commit, |encoder| {
            encoder.fixed(self.commit_id.as_bytes());
            encoder.u64(self.generation);
            encoder.u32(parent_count);
            for parent in &self.parent_commit_object_ids {
                encode_id(encoder, *parent);
            }
            encoder.u32(member_count);
            for change in &self.member_change_object_ids {
                encode_id(encoder, *change);
            }
            encode_id(encoder, self.global_state_root);
            encode_id(encoder, self.local_state_root);
            encoder.bytes(&self.metadata)
        })
    }

    pub(crate) fn decode(id: ObjectId, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = decode_object(id, ObjectDomain::Commit, bytes)?;
        let commit_id = CommitId::from_bytes(decoder.fixed()?);
        let generation = decoder.u64()?;
        let parent_count = decoder.usize("commit parent count")?;
        validate_count(parent_count, decoder.remaining(), 32, "commit parent count")?;
        let mut parent_commit_object_ids = Vec::with_capacity(parent_count);
        for _ in 0..parent_count {
            parent_commit_object_ids.push(decode_id(&mut decoder)?);
        }
        let member_count = decoder.usize("commit member count")?;
        validate_count(member_count, decoder.remaining(), 32, "commit member count")?;
        let mut member_change_object_ids = Vec::with_capacity(member_count);
        for _ in 0..member_count {
            member_change_object_ids.push(decode_id(&mut decoder)?);
        }
        let value = Self {
            commit_id,
            generation,
            parent_commit_object_ids,
            member_change_object_ids,
            global_state_root: decode_id(&mut decoder)?,
            local_state_root: decode_id(&mut decoder)?,
            metadata: decoder.bytes("commit metadata")?,
        };
        decoder.finish()?;
        validate_nonzero_ids("commit parent", &value.parent_commit_object_ids)?;
        validate_nonzero_ids("commit member", &value.member_change_object_ids)?;
        validate_nonzero_ids(
            "commit state",
            &[value.global_state_root, value.local_state_root],
        )?;
        Ok(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ChangeObjectV1 {
    Semantic {
        change_id: ChangeId,
        payload: Vec<u8>,
    },
    BranchRef {
        change_id: ChangeId,
        branch_id: CanonicalBranchId,
        before_semantic_head_commit_object_id: Option<ObjectId>,
        after_semantic_head_commit_object_id: Option<ObjectId>,
        previous_ref_change_object_id: Option<ObjectId>,
        payload: Vec<u8>,
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
            Self::Semantic { .. } => ObjectDomain::SemanticChange,
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
                Self::Semantic { payload, .. } => encoder.bytes(payload),
                Self::BranchRef {
                    branch_id,
                    before_semantic_head_commit_object_id,
                    after_semantic_head_commit_object_id,
                    previous_ref_change_object_id,
                    payload,
                    ..
                } => {
                    encoder.fixed(branch_id.as_bytes());
                    encode_optional_id(encoder, *before_semantic_head_commit_object_id);
                    encode_optional_id(encoder, *after_semantic_head_commit_object_id);
                    encode_optional_id(encoder, *previous_ref_change_object_id);
                    encoder.bytes(payload)
                }
            }
        })
    }

    pub(crate) fn decode(id: ObjectId, bytes: &[u8]) -> Result<Self, StorageError> {
        let semantic = decode_object(id, ObjectDomain::SemanticChange, bytes);
        let value = match semantic {
            Ok(mut decoder) => {
                let value = Self::Semantic {
                    change_id: ChangeId::from_bytes(decoder.fixed()?),
                    payload: decoder.bytes("semantic change payload")?,
                };
                decoder.finish()?;
                value
            }
            Err(StorageError::Corruption(_)) => {
                let mut decoder = decode_object(id, ObjectDomain::BranchRefChange, bytes)?;
                let value = Self::BranchRef {
                    change_id: ChangeId::from_bytes(decoder.fixed()?),
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
                };
                decoder.finish()?;
                value
            }
            Err(error) => return Err(error),
        };
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ChangeCatalogEntry {
    pub(crate) change_object_id: ObjectId,
    pub(crate) owner: ChangeCatalogOwner,
}

impl ChangeCatalogEntry {
    pub(crate) fn encode(self) -> Result<Vec<u8>, StorageError> {
        if self.change_object_id == ObjectId::ZERO {
            return Err(corruption(
                "change catalog contains a zero change object id",
            ));
        }
        let mut encoder = Encoder::default();
        encode_id(&mut encoder, self.change_object_id);
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
                if ref_change_object_id == ObjectId::ZERO
                    || ref_change_object_id != self.change_object_id
                {
                    return Err(corruption(
                        "branch-ref catalog owner must equal the change object id",
                    ));
                }
                encoder.u8(1);
                encode_id(&mut encoder, ref_change_object_id);
                encoder.fixed(branch_id.as_bytes());
            }
        }
        Ok(encoder.into_vec())
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = Decoder::after_prefix(bytes, &[])?;
        let change_object_id = decode_id(&mut decoder)?;
        let owner = match decoder.u8()? {
            0 => ChangeCatalogOwner::CommitMember {
                commit_object_id: decode_id(&mut decoder)?,
                ordinal: decoder.u32()?,
            },
            1 => ChangeCatalogOwner::BranchRef {
                ref_change_object_id: decode_id(&mut decoder)?,
                branch_id: CanonicalBranchId::from_bytes(decoder.fixed()?),
            },
            tag => return Err(corruption(format!("unknown change owner tag {tag}"))),
        };
        decoder.finish()?;
        let value = Self {
            change_object_id,
            owner,
        };
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
