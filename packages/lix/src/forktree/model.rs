use std::collections::BTreeSet;

use bytes::Bytes;

use crate::binary_cas::BlobId;
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
const COMMIT_MEMBER_PAGE_MARKER: u32 = u32::MAX;
const COMMIT_MEMBER_PAGE_EDGE_BUDGET: usize = AUTHENTICATED_EDGE_PAGE_ENTRIES - 1;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommitMemberV1 {
    Introduced {
        change_object_id: ObjectId,
    },
    Selected {
        change_object_id: ObjectId,
        source_commit_object_id: ObjectId,
        source_ordinal: u32,
    },
}

impl CommitMemberV1 {
    pub(crate) fn introduced(change_object_id: ObjectId) -> Self {
        Self::Introduced { change_object_id }
    }

    pub(crate) fn selected(
        change_object_id: ObjectId,
        source_commit_object_id: ObjectId,
        source_ordinal: u32,
    ) -> Self {
        Self::Selected {
            change_object_id,
            source_commit_object_id,
            source_ordinal,
        }
    }

    pub(crate) fn change_object_id(self) -> ObjectId {
        match self {
            Self::Introduced { change_object_id }
            | Self::Selected {
                change_object_id, ..
            } => change_object_id,
        }
    }

    pub(crate) fn source(self) -> Option<(ObjectId, u32)> {
        match self {
            Self::Introduced { .. } => None,
            Self::Selected {
                source_commit_object_id,
                source_ordinal,
                ..
            } => Some((source_commit_object_id, source_ordinal)),
        }
    }

    fn encode(self, encoder: &mut Encoder) {
        match self {
            Self::Introduced { change_object_id } => {
                encoder.u8(0);
                encode_id(encoder, change_object_id);
            }
            Self::Selected {
                change_object_id,
                source_commit_object_id,
                source_ordinal,
            } => {
                encoder.u8(1);
                encode_id(encoder, change_object_id);
                encode_id(encoder, source_commit_object_id);
                encoder.u32(source_ordinal);
            }
        }
    }

    fn decode(decoder: &mut Decoder<'_>) -> Result<Self, StorageError> {
        let value = match decoder.u8()? {
            0 => Self::introduced(decode_id(decoder)?),
            1 => Self::selected(decode_id(decoder)?, decode_id(decoder)?, decoder.u32()?),
            tag => {
                return Err(corruption(format!(
                    "commit member has invalid membership tag {tag}"
                )));
            }
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(self) -> Result<(), StorageError> {
        if self.change_object_id() == ObjectId::ZERO
            || self
                .source()
                .is_some_and(|(commit_object_id, _)| commit_object_id == ObjectId::ZERO)
        {
            return Err(corruption("commit member contains a zero object edge"));
        }
        Ok(())
    }

    fn authenticated_edge_count(self) -> usize {
        1 + usize::from(self.source().is_some())
    }
}

/// One authenticated page in a commit's ordered member closure. The commit
/// object points to the first page; every page authenticates its commit ID,
/// ordinal range, member edges, and optional successor link.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitMemberPageV1 {
    pub(crate) commit_id: CommitId,
    pub(crate) start_ordinal: u32,
    pub(crate) members: Vec<CommitMemberV1>,
    pub(crate) next_page_object_id: Option<ObjectId>,
}

impl CommitMemberPageV1 {
    pub(crate) fn encode(&self) -> Result<(ObjectId, Bytes), StorageError> {
        self.validate()?;
        encode_object(ObjectDomain::CommitMemberPageV1, |encoder| {
            encoder.fixed(self.commit_id.as_bytes());
            encoder.u32(self.start_ordinal);
            encoder.u32(
                u32::try_from(self.members.len())
                    .map_err(|_| corruption("commit member page count exceeds u32"))?,
            );
            for member in &self.members {
                member.encode(encoder);
            }
            encode_optional_id(encoder, self.next_page_object_id);
            Ok(())
        })
    }

    pub(crate) fn decode(id: ObjectId, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = decode_object(id, ObjectDomain::CommitMemberPageV1, bytes)?;
        let commit_id = CommitId::from_bytes(decoder.fixed()?);
        let start_ordinal = decoder.u32()?;
        let count = decoder.usize("commit member page count")?;
        if count == 0 || count > decoder.remaining() / 33 {
            return Err(corruption(
                "commit member page count exceeds its encoded body",
            ));
        }
        let mut members = Vec::with_capacity(count);
        for _ in 0..count {
            members.push(CommitMemberV1::decode(&mut decoder)?);
        }
        let next_page_object_id = decode_optional_id(&mut decoder, "commit member page next")?;
        decoder.finish()?;
        let value = Self {
            commit_id,
            start_ordinal,
            members,
            next_page_object_id,
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
            .copied()
            .map(CommitMemberV1::authenticated_edge_count)
            .try_fold(0usize, |total, count| total.checked_add(count))
            .ok_or_else(|| corruption("commit member page edge count overflowed"))?;
        let edge_count = member_edges
            .checked_add(usize::from(self.next_page_object_id.is_some()))
            .ok_or_else(|| corruption("commit member page edge count overflowed"))?;
        if edge_count > AUTHENTICATED_EDGE_PAGE_ENTRIES {
            return Err(corruption(
                "commit member page exceeds its authenticated edge bound",
            ));
        }
        if self.next_page_object_id == Some(ObjectId::ZERO) {
            return Err(corruption("commit member page has a zero successor"));
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
            if !unique_changes.insert(member.change_object_id()) {
                return Err(corruption("commit member page repeats a change object"));
            }
        }
        Ok(())
    }

    pub(crate) fn encode_chain(
        commit_id: CommitId,
        members: &[CommitMemberV1],
    ) -> Result<(ObjectId, Vec<(ObjectId, Bytes)>), StorageError> {
        if members.is_empty() {
            return Err(corruption("cannot page an empty commit member set"));
        }
        let mut chunks = Vec::<(u32, Vec<CommitMemberV1>)>::new();
        let mut start = 0usize;
        let mut current = Vec::new();
        let mut current_edges = 0usize;
        for member in members.iter().copied() {
            member.validate()?;
            let member_edges = member.authenticated_edge_count();
            if member_edges > COMMIT_MEMBER_PAGE_EDGE_BUDGET {
                return Err(corruption("one commit member exceeds the page edge budget"));
            }
            if !current.is_empty()
                && current_edges
                    .checked_add(member_edges)
                    .is_none_or(|edges| edges > COMMIT_MEMBER_PAGE_EDGE_BUDGET)
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
            }
            current.push(member);
            current_edges = current_edges
                .checked_add(member_edges)
                .ok_or_else(|| corruption("commit member page edge count overflowed"))?;
        }
        if !current.is_empty() {
            chunks.push((
                u32::try_from(start)
                    .map_err(|_| corruption("commit member page ordinal exceeds u32"))?,
                current,
            ));
        }

        let mut next = None;
        let mut encoded = Vec::with_capacity(chunks.len());
        for (start_ordinal, page_members) in chunks.into_iter().rev() {
            let page = Self {
                commit_id,
                start_ordinal,
                members: page_members,
                next_page_object_id: next,
            };
            let (id, bytes) = page.encode()?;
            encoded.push((id, bytes));
            next = Some(id);
        }
        encoded.reverse();
        Ok((next.expect("nonempty member pages"), encoded))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitObjectV1 {
    pub(crate) commit_id: CommitId,
    pub(crate) generation: u64,
    pub(crate) parent_commit_object_ids: Vec<ObjectId>,
    pub(crate) members: Vec<CommitMemberV1>,
    pub(crate) member_page_root: Option<ObjectId>,
    pub(crate) global_state_root: ObjectId,
    pub(crate) local_state_root: ObjectId,
    pub(crate) metadata: Vec<u8>,
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
        let parent_count = u32::try_from(self.parent_commit_object_ids.len())
            .map_err(|_| corruption("commit has too many parents"))?;
        let member_count = if self.member_page_root.is_some() {
            COMMIT_MEMBER_PAGE_MARKER
        } else {
            u32::try_from(self.members.len())
                .map_err(|_| corruption("commit has too many member changes"))?
        };
        encode_object(ObjectDomain::Commit, |encoder| {
            encoder.fixed(self.commit_id.as_bytes());
            encoder.u64(self.generation);
            encoder.u32(parent_count);
            for parent in &self.parent_commit_object_ids {
                encode_id(encoder, *parent);
            }
            encoder.u32(member_count);
            if let Some(page_root) = self.member_page_root {
                encode_id(encoder, page_root);
            } else {
                for member in &self.members {
                    member.encode(encoder);
                }
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
        let member_count = decoder.u32()?;
        let (members, member_page_root) = if member_count == COMMIT_MEMBER_PAGE_MARKER {
            (Vec::new(), Some(decode_id(&mut decoder)?))
        } else {
            let member_count = member_count as usize;
            validate_count(member_count, decoder.remaining(), 33, "commit member count")?;
            let mut members = Vec::with_capacity(member_count);
            for _ in 0..member_count {
                members.push(CommitMemberV1::decode(&mut decoder)?);
            }
            (members, None)
        };
        let value = Self {
            commit_id,
            generation,
            parent_commit_object_ids,
            members,
            member_page_root,
            global_state_root: decode_id(&mut decoder)?,
            local_state_root: decode_id(&mut decoder)?,
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
        value.validate_edge_bound()?;
        Ok(value)
    }

    fn validate_edge_bound(&self) -> Result<(), StorageError> {
        if self.member_page_root == Some(ObjectId::ZERO) {
            return Err(corruption("commit member page root is zero"));
        }
        let member_edges = self.member_page_root.map_or_else(
            || {
                self.members
                    .iter()
                    .copied()
                    .map(CommitMemberV1::authenticated_edge_count)
                    .try_fold(0usize, |total, count| total.checked_add(count))
            },
            |_| Some(1),
        );
        if self
            .parent_commit_object_ids
            .len()
            .checked_add(member_edges.unwrap_or(usize::MAX))
            .and_then(|count| count.checked_add(2))
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
        if self.member_page_root.is_some() && self.members.is_empty() {
            return Ok(Vec::new());
        }
        let direct_edges = self
            .members
            .iter()
            .copied()
            .map(CommitMemberV1::authenticated_edge_count)
            .try_fold(self.parent_commit_object_ids.len(), |total, count| {
                total.checked_add(count)
            })
            .and_then(|count| count.checked_add(2));
        if self.member_page_root.is_none()
            && direct_edges.is_some_and(|count| count <= AUTHENTICATED_EDGE_PAGE_ENTRIES)
        {
            return Ok(Vec::new());
        }
        let (root, pages) = CommitMemberPageV1::encode_chain(self.commit_id, &self.members)?;
        if let Some(expected_root) = self.member_page_root {
            if expected_root != root {
                return Err(corruption(
                    "commit member page root does not match its ordered member closure",
                ));
            }
            return Ok(pages);
        }
        self.member_page_root = Some(root);
        Ok(pages)
    }

    /// Resolve the complete ordered member closure, authenticating every page
    /// link and every page's position.  This is intentionally a caller-owned
    /// loader: page objects are validated with the same object-domain decoder
    /// as the commit envelope and are never treated as an alternate catalog.
    pub(crate) fn load_members_with(
        &self,
        mut load: impl FnMut(ObjectId) -> Result<Bytes, StorageError>,
    ) -> Result<Vec<CommitMemberV1>, StorageError> {
        let Some(mut page_id) = self.member_page_root else {
            return Ok(self.members.clone());
        };
        if !self.members.is_empty() {
            return Err(corruption("paged commit carries an inline member closure"));
        }
        let mut output = Vec::new();
        let mut visited = BTreeSet::new();
        loop {
            if !visited.insert(page_id) {
                return Err(corruption("commit member page chain contains a cycle"));
            }
            let page = CommitMemberPageV1::decode(page_id, &load(page_id)?)?;
            if page.commit_id != self.commit_id
                || page.start_ordinal
                    != u32::try_from(output.len()).map_err(|_| {
                        corruption("commit member page ordinal exceeds u32 while loading")
                    })?
            {
                return Err(corruption(
                    "commit member page chain has a mismatched commit or ordinal",
                ));
            }
            output.extend(page.members);
            match page.next_page_object_id {
                Some(next) => page_id = next,
                None => break,
            }
        }
        let mut unique_changes = BTreeSet::new();
        for member in &output {
            if !unique_changes.insert(member.change_object_id()) {
                return Err(corruption(
                    "commit member page chain repeats a change object",
                ));
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
pub(crate) struct BlobManifestV1 {
    pub(crate) logical_bytes: u64,
    pub(crate) ordered_chunks: Vec<BlobChunkRefV1>,
    /// Integrity-bound copy of the canonical public identity. The selected
    /// state row remains the sole serving owner; this field is only compared
    /// with that row before payload access and is never independently keyed.
    pub(super) canonical_blob_id: BlobId,
    pub(crate) content_digest: [u8; 32],
}

impl BlobManifestV1 {
    /// Constructs the sole authenticated upload manifest representation from
    /// the complete ordered chunk closure. Callers cannot provide a detached
    /// manifest identity; encoding remains the owner validation boundary.
    pub(crate) fn from_authenticated_chunks(
        logical_bytes: u64,
        ordered_chunks: Vec<BlobChunkRefV1>,
        canonical_blob_id: BlobId,
        content_digest: [u8; 32],
    ) -> Self {
        Self {
            logical_bytes,
            ordered_chunks,
            canonical_blob_id,
            content_digest,
        }
    }

    pub(crate) fn encode(&self) -> Result<(ObjectId, Bytes), StorageError> {
        self.validate()?;
        let count = u32::try_from(self.ordered_chunks.len())
            .map_err(|_| corruption("blob manifest has too many chunks"))?;
        encode_object(ObjectDomain::BlobManifest, |encoder| {
            encoder.u64(self.logical_bytes);
            encoder.u32(count);
            for chunk in &self.ordered_chunks {
                encode_id(encoder, chunk.chunk_object_id);
                encoder.u64(chunk.declared_len);
            }
            encoder.fixed(self.canonical_blob_id.as_bytes());
            encoder.fixed(&self.content_digest);
            Ok(())
        })
    }

    pub(crate) fn decode(id: ObjectId, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = decode_object(id, ObjectDomain::BlobManifest, bytes)?;
        let logical_bytes = decoder.u64()?;
        let count = decoder.usize("blob manifest chunk count")?;
        validate_count(count, decoder.remaining(), 40, "blob manifest chunk count")?;
        let mut ordered_chunks = Vec::with_capacity(count);
        for _ in 0..count {
            ordered_chunks.push(BlobChunkRefV1 {
                chunk_object_id: decode_id(&mut decoder)?,
                declared_len: decoder.u64()?,
            });
        }
        let value = Self {
            logical_bytes,
            ordered_chunks,
            canonical_blob_id: BlobId::from_bytes(decoder.fixed()?),
            content_digest: decoder.fixed()?,
        };
        decoder.finish()?;
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), StorageError> {
        if self.ordered_chunks.len() > AUTHENTICATED_EDGE_PAGE_ENTRIES {
            return Err(corruption(
                "blob manifest exceeds one authenticated edge page; use a blocked manifest",
            ));
        }
        let mut total = 0_u64;
        for chunk in &self.ordered_chunks {
            if chunk.chunk_object_id == ObjectId::ZERO || chunk.declared_len == 0 {
                return Err(corruption("blob manifest has an invalid chunk reference"));
            }
            total = total
                .checked_add(chunk.declared_len)
                .ok_or_else(|| corruption("blob manifest chunk lengths overflow u64"))?;
        }
        if total != self.logical_bytes {
            return Err(corruption(
                "blob manifest chunk lengths do not equal its logical length",
            ));
        }
        if self.logical_bytes == 0 && !self.ordered_chunks.is_empty()
            || self.logical_bytes != 0 && self.ordered_chunks.is_empty()
        {
            return Err(corruption("blob manifest empty layout is inconsistent"));
        }
        Ok(())
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
    Undo = 3,
    Redo = 4,
    BranchTombstone = 5,
}

impl SnapshotRole {
    fn decode(value: u8) -> Result<Self, StorageError> {
        match value {
            1 => Ok(Self::Checkpoint),
            2 => Ok(Self::Recovery),
            3 => Ok(Self::Undo),
            4 => Ok(Self::Redo),
            5 => Ok(Self::BranchTombstone),
            _ => Err(corruption(format!(
                "unknown retained snapshot role {value}"
            ))),
        }
    }

    pub(super) fn key_prefix(self) -> &'static [u8] {
        match self {
            Self::Checkpoint => b"checkpoint/",
            Self::Recovery => b"recovery/",
            Self::Undo => b"undo/",
            Self::Redo => b"redo/",
            Self::BranchTombstone => b"branch-tombstone/",
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
    RootUntracked = 2,
    Traverse = 3,
    Sweep = 4,
    Cleanup = 5,
}

impl GcPhaseV2 {
    fn decode(value: u8) -> Result<Self, StorageError> {
        match value {
            1 => Ok(Self::RootSelectors),
            2 => Ok(Self::RootUntracked),
            3 => Ok(Self::Traverse),
            4 => Ok(Self::Sweep),
            5 => Ok(Self::Cleanup),
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
    pub(super) untracked_resume_after: Option<Vec<u8>>,
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
            encode_optional_bounded_bytes(encoder, self.untracked_resume_after.as_deref())?;
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
            untracked_resume_after: decode_optional_bounded_bytes(
                &mut decoder,
                "GC untracked cursor",
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
            || self
                .untracked_resume_after
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
                self.untracked_resume_after.is_none()
                    && self.object_resume_after.is_none()
                    && self.maintenance_resume_after.is_none()
                    && self.validated_count == 0
            }
            GcPhaseV2::RootUntracked => {
                self.saw_global_selector
                    && self.selector_resume_after.is_none()
                    && self.object_resume_after.is_none()
                    && self.maintenance_resume_after.is_none()
            }
            GcPhaseV2::Traverse => {
                self.saw_global_selector
                    && self.selector_resume_after.is_none()
                    && self.untracked_resume_after.is_none()
                    && self.object_resume_after.is_none()
                    && self.maintenance_resume_after.is_none()
                    && self.mark_index_root.is_some()
            }
            GcPhaseV2::Sweep => {
                self.saw_global_selector
                    && self.selector_resume_after.is_none()
                    && self.untracked_resume_after.is_none()
                    && self.maintenance_resume_after.is_none()
                    && self.mark_index_root.is_some()
                    && self.queue_index_root.is_none()
                    && self.queue_pop_sequence == self.queue_push_sequence
                    && self.validated_count == self.marked_count
            }
            GcPhaseV2::Cleanup => {
                self.saw_global_selector
                    && self.selector_resume_after.is_none()
                    && self.untracked_resume_after.is_none()
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

pub(super) fn snapshot_selector_key(role: SnapshotRole, selector_id: SnapshotSelectorId) -> Bytes {
    let mut key = Vec::with_capacity(role.key_prefix().len() + 16);
    key.extend_from_slice(role.key_prefix());
    key.extend_from_slice(selector_id.as_bytes());
    Bytes::from(key)
}

pub(super) fn gc_progress_selector_key() -> Bytes {
    Bytes::from_static(b"gc-progress")
}
