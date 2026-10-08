//! Lazy binary-CAS sync outside the live commit/ref cursor.

use base64::Engine as _;
use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use std::fmt;

use crate::binary_cas::{
    BlobChunkReceipt, BlobId, CanonicalBlobChunk, CanonicalBlobManifest, ChunkHash,
    chunk_presence_many, load_metadata_many, load_verified_chunk,
    stage_deferred_canonical_manifest, stage_transfer_publication_fence,
    stage_verified_canonical_manifest, stage_verified_inline_canonical_blob,
    stage_verified_raw_chunk,
};
use crate::common::BoundedString;
use crate::storage_adapter::{Storage, StorageReadOptions, StorageWriteOptions, StorageWriteSet};
use crate::{Lix, LixError};

use super::{SyncBlobChunk, SyncBlobManifest, SyncBlobRegistration};

const MAX_SYNC_BLOB_CHUNKS: usize = 16_384;
pub(super) const MAX_INLINE_SYNC_BLOB_BYTES: usize = 256 * 1024;
const MAX_SYNC_BLOB_ID_BYTES: usize = 64;
// The smallest server-produced inline manifest is the empty blob: a canonical
// 64-byte lowercase-hex id, sizeBytes=0, no chunk receipts, and empty inline
// base64.
// Count budgets below use this minimum only after the inline visitor enforces
// that exact known-field shape.
const MIN_INLINE_MANIFEST_JSON_BYTES: usize = 126;

/// Maximum JSON array size for `expected_count` manifests using the existing
/// 2 MiB per-manifest registration bound plus array punctuation. A full
/// 16-item response stays just over 32 MiB, below the shared 64 MiB cap.
pub(super) fn manifest_response_body_limit(expected_count: usize) -> Result<usize, LixError> {
    if expected_count == 0 || expected_count > super::MAX_SYNC_BLOB_BATCH_ITEMS {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync blob request exceeds its manifest response count bound",
        ));
    }
    super::transfer::MAX_MANIFEST_SINGLETON_ENCODED_BYTES
        .checked_mul(expected_count)
        .and_then(|bytes| {
            expected_count
                .checked_add(1)
                .and_then(|array_overhead| bytes.checked_add(array_overhead))
        })
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync blob manifest response byte bound overflowed",
            )
        })
}

/// Decodes an authority manifest response while enforcing the request and
/// schema cardinality bounds before allocating owned response rows. The HTTP
/// request caps the whole body with `manifest_response_body_limit`; serde_json
/// may still use parser scratch for escaped strings, bounded by that body cap.
pub(super) fn decode_manifest_response(
    bytes: &[u8],
    expected_count: usize,
) -> Result<Vec<SyncBlobManifest>, LixError> {
    manifest_response_body_limit(expected_count)?;

    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let manifests = ManifestResponseSeed { expected_count }
        .deserialize(&mut deserializer)
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!("decode bounded sync blob manifest response: {error}"),
            )
        })?;
    deserializer.end().map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("decode bounded sync blob manifest response: {error}"),
        )
    })?;
    Ok(manifests)
}

#[derive(serde::Deserialize)]
#[serde(field_identifier, rename_all = "camelCase")]
enum PullResponseField {
    Kind,
    Cursor,
    LixId,
    DefaultBranchId,
    Branches,
    Events,
    #[serde(other)]
    Other,
}

struct PullResponseSeed {
    max_events: usize,
    manifest_budget: usize,
}

impl<'de> DeserializeSeed<'de> for PullResponseSeed {
    type Value = super::SyncRepositoryPullResponse;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(PullResponseVisitor(self))
    }
}

struct PullResponseVisitor(PullResponseSeed);

impl<'de> Visitor<'de> for PullResponseVisitor {
    type Value = super::SyncRepositoryPullResponse;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded sync pull response")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut kind = None;
        let mut cursor = None;
        let mut lix_id = None;
        let mut default_branch_id = None;
        let mut branches = None;
        let mut events = None;
        while let Some(field) = map.next_key::<PullResponseField>()? {
            match field {
                PullResponseField::Kind => {
                    if kind.is_some() {
                        return Err(serde::de::Error::duplicate_field("kind"));
                    }
                    kind = Some(map.next_value::<BoundedString<16>>()?.0);
                }
                PullResponseField::Cursor => {
                    if cursor.is_some() {
                        return Err(serde::de::Error::duplicate_field("cursor"));
                    }
                    cursor = Some(map.next_value::<&serde_json::value::RawValue>()?);
                }
                PullResponseField::LixId => {
                    if lix_id.is_some() {
                        return Err(serde::de::Error::duplicate_field("lixId"));
                    }
                    lix_id = Some(map.next_value::<&serde_json::value::RawValue>()?);
                }
                PullResponseField::DefaultBranchId => {
                    if default_branch_id.is_some() {
                        return Err(serde::de::Error::duplicate_field("defaultBranchId"));
                    }
                    default_branch_id = Some(map.next_value::<&serde_json::value::RawValue>()?);
                }
                PullResponseField::Branches => {
                    if branches.is_some() {
                        return Err(serde::de::Error::duplicate_field("branches"));
                    }
                    branches = Some(map.next_value::<&serde_json::value::RawValue>()?);
                }
                PullResponseField::Events => {
                    if events.is_some() {
                        return Err(serde::de::Error::duplicate_field("events"));
                    }
                    events = Some(map.next_value::<&serde_json::value::RawValue>()?);
                }
                PullResponseField::Other => {
                    let _: &serde_json::value::RawValue = map.next_value()?;
                }
            }
        }

        let kind = kind.ok_or_else(|| serde::de::Error::missing_field("kind"))?;
        let cursor = cursor.ok_or_else(|| serde::de::Error::missing_field("cursor"))?;
        match kind.as_str() {
            "snapshot" => Ok(super::SyncRepositoryPullResponse::Snapshot {
                cursor: decode_raw_value(cursor, "decode bounded sync pull cursor")
                    .map_err(serde::de::Error::custom)?,
                lix_id: decode_raw_value::<BoundedString<64>>(
                    lix_id.ok_or_else(|| serde::de::Error::missing_field("lixId"))?,
                    "decode bounded sync pull lixId",
                )
                .map_err(serde::de::Error::custom)?
                .0,
                default_branch_id: decode_raw_value::<BoundedString<64>>(
                    default_branch_id
                        .ok_or_else(|| serde::de::Error::missing_field("defaultBranchId"))?,
                    "decode bounded sync pull defaultBranchId",
                )
                .map_err(serde::de::Error::custom)?
                .0,
                branches: decode_raw_seed(
                    branches.ok_or_else(|| serde::de::Error::missing_field("branches"))?,
                    BoundedVecSeed::<super::SyncBranchHead>::unbounded("sync snapshot branches"),
                    "decode bounded sync snapshot branches",
                )
                .map_err(serde::de::Error::custom)?,
            }),
            "delta" => Ok(super::SyncRepositoryPullResponse::Delta {
                cursor: decode_raw_value(cursor, "decode bounded sync pull cursor")
                    .map_err(serde::de::Error::custom)?,
                events: decode_raw_seed(
                    events.ok_or_else(|| serde::de::Error::missing_field("events"))?,
                    EventsSeed {
                        max_events: self.0.max_events,
                        manifest_budget: self.0.manifest_budget,
                    },
                    "decode bounded sync pull events",
                )
                .map_err(serde::de::Error::custom)?,
            }),
            _ => Err(serde::de::Error::unknown_variant(
                &kind,
                &["snapshot", "delta"],
            )),
        }
    }
}

fn decode_raw_value<T>(
    raw: &serde_json::value::RawValue,
    context: &'static str,
) -> Result<T, LixError>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_str(raw.get())
        .map_err(|error| LixError::new(LixError::CODE_INVALID_PARAM, format!("{context}: {error}")))
}

fn decode_raw_seed<'de, Seed>(
    raw: &'de serde_json::value::RawValue,
    seed: Seed,
    context: &'static str,
) -> Result<Seed::Value, LixError>
where
    Seed: DeserializeSeed<'de>,
{
    let mut deserializer = serde_json::Deserializer::from_str(raw.get());
    let value = seed.deserialize(&mut deserializer).map_err(|error| {
        LixError::new(LixError::CODE_INVALID_PARAM, format!("{context}: {error}"))
    })?;
    deserializer.end().map_err(|error| {
        LixError::new(LixError::CODE_INVALID_PARAM, format!("{context}: {error}"))
    })?;
    Ok(value)
}

struct BoundedVecSeed<T> {
    max_items: Option<usize>,
    label: &'static str,
    marker: std::marker::PhantomData<fn() -> T>,
}

impl<T> BoundedVecSeed<T> {
    fn unbounded(label: &'static str) -> Self {
        Self {
            max_items: None,
            label,
            marker: std::marker::PhantomData,
        }
    }
}

impl<'de, T> DeserializeSeed<'de> for BoundedVecSeed<T>
where
    T: serde::Deserialize<'de>,
{
    type Value = Vec<T>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(BoundedVecVisitor {
            max_items: self.max_items,
            label: self.label,
            marker: std::marker::PhantomData,
        })
    }
}

struct BoundedVecVisitor<T> {
    max_items: Option<usize>,
    label: &'static str,
    marker: std::marker::PhantomData<fn() -> T>,
}

impl<'de, T> Visitor<'de> for BoundedVecVisitor<T>
where
    T: serde::Deserialize<'de>,
{
    type Value = Vec<T>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.max_items {
            Some(max_items) => write!(formatter, "at most {max_items} {}", self.label),
            None => write!(formatter, "a bounded list of {}", self.label),
        }
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        // Do not trust SeqAccess::size_hint: wire cardinality is the only
        // source of growth for lists whose protocol has no count limit.
        let mut values = Vec::new();
        loop {
            if self.max_items.is_some_and(|limit| values.len() == limit) {
                if sequence.next_element::<IgnoredAny>()?.is_some() {
                    return Err(serde::de::Error::custom(format!(
                        "{} exceeds its item limit",
                        self.label
                    )));
                }
                break;
            }
            let Some(value) = sequence.next_element::<T>()? else {
                break;
            };
            values.push(value);
        }
        Ok(values)
    }
}

struct EventsSeed {
    max_events: usize,
    manifest_budget: usize,
}

impl<'de> DeserializeSeed<'de> for EventsSeed {
    type Value = Vec<super::SyncEvent>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(EventsVisitor(self))
    }
}

struct EventsVisitor(EventsSeed);

impl<'de> Visitor<'de> for EventsVisitor {
    type Value = Vec<super::SyncEvent>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "at most {} sync events", self.0.max_events)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut events = Vec::with_capacity(self.0.max_events);
        let mut remaining_manifests = self.0.manifest_budget;
        loop {
            if events.len() == self.0.max_events {
                if sequence.next_element::<IgnoredAny>()?.is_some() {
                    return Err(serde::de::Error::custom(
                        "sync pull response exceeds the requested event count",
                    ));
                }
                break;
            }
            let Some((event, consumed)) = sequence.next_element_seed(EventSeed {
                manifest_budget: remaining_manifests,
            })?
            else {
                break;
            };
            remaining_manifests -= consumed;
            events.push(event);
        }
        Ok(events)
    }
}

#[derive(serde::Deserialize)]
#[serde(field_identifier, rename_all = "camelCase")]
enum EventField {
    Cursor,
    Commits,
    RefUpdates,
    InlineBlobs,
    #[serde(other)]
    Other,
}

struct EventSeed {
    manifest_budget: usize,
}

impl<'de> DeserializeSeed<'de> for EventSeed {
    type Value = (super::SyncEvent, usize);

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(EventVisitor {
            manifest_budget: self.manifest_budget,
        })
    }
}

struct EventVisitor {
    manifest_budget: usize,
}

impl<'de> Visitor<'de> for EventVisitor {
    type Value = (super::SyncEvent, usize);

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded sync event")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut cursor = None;
        let mut commits = None;
        let mut ref_updates = None;
        let mut inline_blobs = None;
        while let Some(field) = map.next_key::<EventField>()? {
            match field {
                EventField::Cursor => {
                    if cursor.is_some() {
                        return Err(serde::de::Error::duplicate_field("cursor"));
                    }
                    cursor = Some(map.next_value::<u64>()?);
                }
                EventField::Commits => {
                    if commits.is_some() {
                        return Err(serde::de::Error::duplicate_field("commits"));
                    }
                    commits = Some(map.next_value::<Vec<super::SyncCommit>>()?);
                }
                EventField::RefUpdates => {
                    if ref_updates.is_some() {
                        return Err(serde::de::Error::duplicate_field("refUpdates"));
                    }
                    ref_updates = Some(map.next_value::<Vec<super::protocol::SyncRefUpdate>>()?);
                }
                EventField::InlineBlobs => {
                    if inline_blobs.is_some() {
                        return Err(serde::de::Error::duplicate_field("inlineBlobs"));
                    }
                    inline_blobs = Some(map.next_value_seed(InlineManifestsSeed {
                        manifest_budget: self.manifest_budget,
                    })?);
                }
                EventField::Other => {
                    let _: IgnoredAny = map.next_value()?;
                }
            }
        }
        let (inline_blobs, consumed) =
            inline_blobs.ok_or_else(|| serde::de::Error::missing_field("inlineBlobs"))?;
        Ok((
            super::SyncEvent {
                cursor: cursor.ok_or_else(|| serde::de::Error::missing_field("cursor"))?,
                commits: commits.ok_or_else(|| serde::de::Error::missing_field("commits"))?,
                ref_updates: ref_updates
                    .ok_or_else(|| serde::de::Error::missing_field("refUpdates"))?,
                inline_blobs,
            },
            consumed,
        ))
    }
}

struct InlineManifestsSeed {
    manifest_budget: usize,
}

impl<'de> DeserializeSeed<'de> for InlineManifestsSeed {
    type Value = (Vec<SyncBlobManifest>, usize);

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(InlineManifestsVisitor {
            manifest_budget: self.manifest_budget,
        })
    }
}

struct InlineManifestsVisitor {
    manifest_budget: usize,
}

impl<'de> Visitor<'de> for InlineManifestsVisitor {
    type Value = (Vec<SyncBlobManifest>, usize);

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "at most {} server-bounded inline sync blob manifests",
            self.manifest_budget
        )
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut manifests = Vec::new();
        loop {
            if manifests.len() == self.manifest_budget {
                if sequence.next_element::<IgnoredAny>()?.is_some() {
                    return Err(serde::de::Error::custom(
                        "sync pull inline manifests exceed their response-derived item budget",
                    ));
                }
                break;
            }
            let Some(manifest) = sequence.next_element_seed(ManifestSeed::pull_inline())? else {
                break;
            };
            manifests.push(manifest);
        }
        let consumed = manifests.len();
        Ok((manifests, consumed))
    }
}

fn has_canonical_sync_blob_id_format(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Decodes a pull response with request-derived event cardinality and a
/// response-body-derived aggregate budget for inline manifests. The current
/// authority caps pull responses at 64 MiB and emits at most `limit` events;
/// each inline manifest has a 64-byte lowercase-hex id, inline data, and no
/// more than one receipt. Commits and ref updates retain their existing
/// producer cardinality and are bounded here by the 64 MiB encoded response
/// limit; this decoder does not claim that all retained DTO allocations are
/// bounded by their encoded size. The inline-field limits cap retained inline
/// ownership; serde may use parser scratch for escaped strings, bounded by the
/// whole response body.
pub(super) fn decode_pull_response(
    bytes: &[u8],
    max_events: usize,
) -> Result<super::SyncRepositoryPullResponse, LixError> {
    if bytes.len() > super::MAX_SYNC_PULL_RESPONSE_BYTES {
        return Err(super::http::response_too_large("pull sync repository"));
    }
    if max_events == 0 || max_events > super::MAX_SYNC_REQUEST_ITEMS {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync pull event count exceeds its request bound",
        ));
    }
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let response = PullResponseSeed {
        max_events,
        manifest_budget: bytes.len() / MIN_INLINE_MANIFEST_JSON_BYTES,
    }
    .deserialize(&mut deserializer)
    .map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("decode bounded sync pull response: {error}"),
        )
    })?;
    deserializer.end().map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("decode bounded sync pull response: {error}"),
        )
    })?;
    Ok(response)
}

struct ManifestResponseSeed {
    expected_count: usize,
}

impl<'de> DeserializeSeed<'de> for ManifestResponseSeed {
    type Value = Vec<SyncBlobManifest>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(ManifestResponseVisitor {
            expected_count: self.expected_count,
        })
    }
}

struct ManifestResponseVisitor {
    expected_count: usize,
}

impl<'de> Visitor<'de> for ManifestResponseVisitor {
    type Value = Vec<SyncBlobManifest>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "exactly {} sync blob manifests",
            self.expected_count
        )
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        // The request is already capped at 16 IDs, so this reservation cannot
        // be driven by untrusted JSON's array length or size hint.
        let mut manifests = Vec::with_capacity(self.expected_count);
        loop {
            if manifests.len() == self.expected_count {
                if sequence.next_element::<IgnoredAny>()?.is_some() {
                    return Err(serde::de::Error::invalid_length(
                        self.expected_count.saturating_add(1),
                        &self,
                    ));
                }
                break;
            }
            let Some(manifest) = sequence.next_element_seed(ManifestSeed::wire())? else {
                break;
            };
            manifests.push(manifest);
        }
        if manifests.len() != self.expected_count {
            return Err(serde::de::Error::invalid_length(manifests.len(), &self));
        }
        Ok(manifests)
    }
}

#[derive(serde::Deserialize)]
#[serde(field_identifier, rename_all = "camelCase")]
enum ManifestField {
    BlobId,
    SizeBytes,
    Chunks,
    InlineBytesBase64,
    #[serde(other)]
    Other,
}

#[derive(Clone, Copy)]
struct ManifestSeed {
    max_chunks: usize,
    require_inline: bool,
    require_canonical_blob_id: bool,
}

impl ManifestSeed {
    const fn wire() -> Self {
        Self {
            max_chunks: MAX_SYNC_BLOB_CHUNKS,
            require_inline: false,
            require_canonical_blob_id: false,
        }
    }

    const fn pull_inline() -> Self {
        Self {
            max_chunks: 1,
            require_inline: true,
            require_canonical_blob_id: true,
        }
    }
}

impl<'de> DeserializeSeed<'de> for ManifestSeed {
    type Value = SyncBlobManifest;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(ManifestVisitor(self))
    }
}

struct ManifestVisitor(ManifestSeed);

impl<'de> Visitor<'de> for ManifestVisitor {
    type Value = SyncBlobManifest;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded sync blob manifest")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut blob_id = None;
        let mut size_bytes = None;
        let mut chunks = None;
        let mut inline_bytes_base64 = None;
        let mut inline_was_present = false;
        while let Some(field) = map.next_key::<ManifestField>()? {
            match field {
                ManifestField::BlobId => {
                    if blob_id.is_some() {
                        return Err(serde::de::Error::duplicate_field("blobId"));
                    }
                    blob_id = Some(map.next_value::<BoundedString<MAX_SYNC_BLOB_ID_BYTES>>()?.0);
                }
                ManifestField::SizeBytes => {
                    if size_bytes.is_some() {
                        return Err(serde::de::Error::duplicate_field("sizeBytes"));
                    }
                    size_bytes = Some(map.next_value::<u64>()?);
                }
                ManifestField::Chunks => {
                    if chunks.is_some() {
                        return Err(serde::de::Error::duplicate_field("chunks"));
                    }
                    chunks = Some(map.next_value_seed(ChunkListSeed {
                        max_chunks: self.0.max_chunks,
                    })?);
                }
                ManifestField::InlineBytesBase64 => {
                    if inline_was_present {
                        return Err(serde::de::Error::duplicate_field("inlineBytesBase64"));
                    }
                    inline_was_present = true;
                    inline_bytes_base64 =
                        map.next_value::<Option<
                            BoundedString<{ MAX_INLINE_SYNC_BLOB_BYTES.div_ceil(3) * 4 }>,
                        >>()?
                        .map(|value| value.0);
                }
                ManifestField::Other => {
                    let _: IgnoredAny = map.next_value()?;
                }
            }
        }
        let blob_id = blob_id.ok_or_else(|| serde::de::Error::missing_field("blobId"))?;
        let size_bytes = size_bytes.ok_or_else(|| serde::de::Error::missing_field("sizeBytes"))?;
        let chunks = chunks.ok_or_else(|| serde::de::Error::missing_field("chunks"))?;
        if self.0.require_canonical_blob_id && !has_canonical_sync_blob_id_format(&blob_id) {
            return Err(serde::de::Error::custom(
                "inline sync blob id must be 64 lowercase hexadecimal characters",
            ));
        }
        if self.0.require_inline {
            if inline_bytes_base64.is_none() {
                return Err(serde::de::Error::custom(
                    "inline sync blob manifest requires inlineBytesBase64",
                ));
            }
            if size_bytes > MAX_INLINE_SYNC_BLOB_BYTES as u64 {
                return Err(serde::de::Error::custom(
                    "inline sync blob exceeds its 256 KiB decoded byte limit",
                ));
            }
        }
        Ok(SyncBlobManifest {
            blob_id,
            size_bytes,
            chunks,
            inline_bytes_base64,
        })
    }
}

struct ChunkListSeed {
    max_chunks: usize,
}

impl<'de> DeserializeSeed<'de> for ChunkListSeed {
    type Value = Vec<SyncBlobChunk>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(ChunkListVisitor {
            max_chunks: self.max_chunks,
        })
    }
}

struct ChunkListVisitor {
    max_chunks: usize,
}

impl<'de> Visitor<'de> for ChunkListVisitor {
    type Value = Vec<SyncBlobChunk>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "at most {} sync blob chunks", self.max_chunks)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut chunks = Vec::new();
        loop {
            if chunks.len() == self.max_chunks {
                if sequence.next_element::<IgnoredAny>()?.is_some() {
                    return Err(serde::de::Error::invalid_length(
                        self.max_chunks.saturating_add(1),
                        &self,
                    ));
                }
                break;
            }
            let Some(chunk) = sequence.next_element_seed(ChunkSeed)? else {
                break;
            };
            chunks.push(chunk);
        }
        Ok(chunks)
    }
}

#[derive(serde::Deserialize)]
#[serde(field_identifier, rename_all = "camelCase")]
enum ChunkField {
    ChunkId,
    SizeBytes,
    #[serde(other)]
    Other,
}

struct ChunkSeed;

impl<'de> DeserializeSeed<'de> for ChunkSeed {
    type Value = SyncBlobChunk;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(ChunkVisitor)
    }
}

struct ChunkVisitor;

impl<'de> Visitor<'de> for ChunkVisitor {
    type Value = SyncBlobChunk;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded sync blob chunk receipt")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut chunk_id = None;
        let mut size_bytes = None;
        while let Some(field) = map.next_key::<ChunkField>()? {
            match field {
                ChunkField::ChunkId => {
                    if chunk_id.is_some() {
                        return Err(serde::de::Error::duplicate_field("chunkId"));
                    }
                    chunk_id = Some(map.next_value::<BoundedString<MAX_SYNC_BLOB_ID_BYTES>>()?.0);
                }
                ChunkField::SizeBytes => {
                    if size_bytes.is_some() {
                        return Err(serde::de::Error::duplicate_field("sizeBytes"));
                    }
                    size_bytes = Some(map.next_value::<u64>()?);
                }
                ChunkField::Other => {
                    let _: IgnoredAny = map.next_value()?;
                }
            }
        }
        Ok(SyncBlobChunk {
            chunk_id: chunk_id.ok_or_else(|| serde::de::Error::missing_field("chunkId"))?,
            size_bytes: size_bytes.ok_or_else(|| serde::de::Error::missing_field("sizeBytes"))?,
        })
    }
}

pub(crate) fn validate_manifest_group(wires: &[SyncBlobManifest]) -> Result<(), LixError> {
    if wires.is_empty()
        || wires.len() > super::transfer::CONTENT_GROUP_ITEMS
        || serde_json::to_vec(wires)
            .map_err(|e| LixError::unknown(e.to_string()))?
            .len()
            > super::transfer::CONTENT_GROUP_BYTES
    {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "blob group exceeds transfer budget",
        ));
    }
    let mut ids = std::collections::BTreeSet::new();
    let mut decoded = 0usize;
    for wire in wires {
        if !ids.insert(&wire.blob_id) {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "blob group repeats an identity",
            ));
        }
        validate_sync_blob_manifest(wire)?;
        decoded = decoded.saturating_add(decode_inline_bytes(wire)?.map_or(0, |bytes| bytes.len()));
        if decoded > super::transfer::CONTENT_GROUP_BYTES {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "blob group decoded content exceeds transfer budget",
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_sync_blob_manifest(manifest: &SyncBlobManifest) -> Result<(), LixError> {
    decode_manifest(manifest).map(|_| ())
}

pub(super) fn decode_manifest(
    manifest: &SyncBlobManifest,
) -> Result<CanonicalBlobManifest, LixError> {
    if manifest.chunks.len() > MAX_SYNC_BLOB_CHUNKS {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("sync blob manifests accept at most {MAX_SYNC_BLOB_CHUNKS} chunks"),
        ));
    }
    let chunks = manifest
        .chunks
        .iter()
        .map(|chunk| {
            Ok(BlobChunkReceipt {
                hash: ChunkHash::from_hex(&chunk.chunk_id)?,
                size_bytes: chunk.size_bytes,
            })
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let canonical = CanonicalBlobManifest {
        blob_id: BlobId::from_hex(&manifest.blob_id)?,
        size_bytes: manifest.size_bytes,
        chunks,
    };
    crate::binary_cas::validate_manifest_receipts(&canonical)?;
    Ok(canonical)
}

pub(super) fn encode_manifest(
    blob_id: BlobId,
    chunks: &[CanonicalBlobChunk],
) -> Result<SyncBlobManifest, LixError> {
    let size_bytes = chunks.iter().try_fold(0_u64, |size, chunk| {
        size.checked_add(chunk.receipt.size_bytes)
            .ok_or_else(|| LixError::unknown("canonical sync blob size overflow"))
    })?;
    Ok(SyncBlobManifest {
        blob_id: blob_id.to_hex(),
        size_bytes,
        chunks: chunks
            .iter()
            .map(|chunk| SyncBlobChunk {
                chunk_id: chunk.receipt.hash.to_hex(),
                size_bytes: chunk.receipt.size_bytes,
            })
            .collect(),
        inline_bytes_base64: match chunks {
            [] => Some(base64::engine::general_purpose::STANDARD.encode([])),
            [chunk] if size_bytes <= MAX_INLINE_SYNC_BLOB_BYTES as u64 => {
                Some(base64::engine::general_purpose::STANDARD.encode(&chunk.bytes))
            }
            _ => None,
        },
    })
}

pub(super) fn decode_inline_bytes(wire: &SyncBlobManifest) -> Result<Option<Vec<u8>>, LixError> {
    let Some(encoded) = wire.inline_bytes_base64.as_deref() else {
        return Ok(None);
    };
    if encoded.len() > MAX_INLINE_SYNC_BLOB_BYTES.div_ceil(3) * 4 {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("sync inline blob exceeds {MAX_INLINE_SYNC_BLOB_BYTES} bytes"),
        ));
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!("sync inline blob is not valid base64: {error}"),
            )
        })?;
    if bytes.len() > MAX_INLINE_SYNC_BLOB_BYTES {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("sync inline blob exceeds {MAX_INLINE_SYNC_BLOB_BYTES} bytes"),
        ));
    }
    Ok(Some(bytes))
}

/// Validates and stages one self-contained hot-path blob in a caller-owned
/// atomic publication. The protocol keeps this separate from the large blob
/// lane so commit/ref admission never observes a half-published inline blob.
pub(crate) fn stage_inline_sync_blob(
    writes: &mut StorageWriteSet,
    wire: &SyncBlobManifest,
) -> Result<(), LixError> {
    let manifest = decode_manifest(wire)?;
    let bytes = decode_inline_bytes(wire)?.ok_or_else(|| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync hot-path blob manifest has no inline payload",
        )
    })?;
    stage_verified_inline_canonical_blob(writes, &manifest, &bytes).map(|_| ())
}

impl<StorageImpl> Lix<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Returns a self-contained hot-path manifest only when metadata proves
    /// the blob can fit inline. Large and missing blobs return `None` without
    /// materializing delta layouts or publishing canonical transfer chunks.
    pub(crate) async fn get_sync_inline_blob_manifest(
        &self,
        blob_id: &str,
    ) -> Result<Option<SyncBlobManifest>, LixError> {
        let blob_id = BlobId::from_hex(blob_id)?;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let metadata = load_metadata_many(&read, &[blob_id])
            .await?
            .into_vec()
            .into_iter()
            .next()
            .flatten();
        if metadata
            .as_ref()
            .is_none_or(|metadata| metadata.size_bytes > MAX_INLINE_SYNC_BLOB_BYTES as u64)
        {
            return Ok(None);
        }
        let bytes = crate::binary_cas::load_bytes_many(&read, &[blob_id])
            .await?
            .into_vec()
            .into_iter()
            .next()
            .flatten()
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("sync inline blob '{}' lost its payload", blob_id.to_hex()),
                )
            })?;
        let canonical = CanonicalBlobManifest::from_bytes(&bytes);
        if canonical.blob_id != blob_id {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "sync inline blob '{}' failed authentication",
                    blob_id.to_hex()
                ),
            ));
        }
        if canonical.chunks.len() > 1 {
            return Ok(None);
        }
        Ok(Some(SyncBlobManifest {
            blob_id: blob_id.to_hex(),
            size_bytes: canonical.size_bytes,
            chunks: canonical
                .chunks
                .into_iter()
                .map(|chunk| SyncBlobChunk {
                    chunk_id: chunk.hash.to_hex(),
                    size_bytes: chunk.size_bytes,
                })
                .collect(),
            inline_bytes_base64: Some(base64::engine::general_purpose::STANDARD.encode(&bytes)),
        }))
    }

    /// Checks only authenticated manifest metadata. Unlike the outbound
    /// transfer accessor, this never materializes the blob or requires its
    /// chunks to be present.
    pub(crate) async fn has_sync_blob_manifest(&self, blob_id: &str) -> Result<bool, LixError> {
        let blob_id = BlobId::from_hex(blob_id)?;
        let adapter = self.storage_adapter();
        crate::handle::retry_expired_read(|| async {
            let read = adapter.begin_read(StorageReadOptions::default()).await?;
            Ok(load_metadata_many(&read, &[blob_id])
                .await?
                .into_vec()
                .into_iter()
                .next()
                .flatten()
                .is_some())
        })
        .await
    }

    /// Returns a canonical flat manifest and ensures all chunks it names can
    /// subsequently be fetched, flattening a delta-backed physical layout on
    /// first transfer.
    pub(crate) async fn get_sync_blob_manifest(
        &self,
        blob_id: &str,
    ) -> Result<Option<SyncBlobManifest>, LixError> {
        // Canonicalization can publish missing raw chunks on first transfer.
        // Keep that conditional write in the collaboration serialization
        // domain even though the common path is read-only.
        let _collaboration_guard = self.lock_collaboration_writes().await;
        self.get_sync_blob_manifest_with_collaboration_guard(blob_id)
            .await
    }

    /// Same operation for a sync import that already owns the collaboration
    /// write gate. Keeping this explicit avoids recursively acquiring the
    /// non-reentrant gate while authority admission validates referenced
    /// blobs.
    pub(crate) async fn get_sync_blob_manifest_with_collaboration_guard(
        &self,
        blob_id: &str,
    ) -> Result<Option<SyncBlobManifest>, LixError> {
        self.get_sync_blob_manifest_with_baseline_lease(blob_id, None)
            .await
    }

    async fn get_sync_blob_manifest_with_baseline_lease(
        &self,
        blob_id: &str,
        lease_id: Option<&str>,
    ) -> Result<Option<SyncBlobManifest>, LixError> {
        let blob_id = BlobId::from_hex(blob_id)?;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        if let Some(id) = lease_id {
            crate::gc::require_native_baseline_lease(
                &read,
                id,
                self.active_account_id(),
                crate::telemetry::unix_time_ms(),
            )
            .await?;
        }

        let metadata = load_metadata_many(&read, &[blob_id])
            .await?
            .into_vec()
            .into_iter()
            .next()
            .flatten();
        drop(read);
        match metadata {
            Some(metadata) => self
                .publish_sync_blob_manifest(&metadata, lease_id)
                .await
                .map(Some),
            None => Ok(None),
        }
    }

    /// One admitted group shares its metadata read and collaboration gate.
    pub(crate) async fn get_sync_blob_manifests_leased(
        &self,
        blob_ids: &[String],
        lease_id: Option<&str>,
    ) -> Result<Vec<Option<SyncBlobManifest>>, LixError> {
        if blob_ids.is_empty() || blob_ids.len() > super::MAX_SYNC_BLOB_BATCH_ITEMS {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "manifest discovery group exceeds its item budget",
            ));
        }
        let ids = blob_ids
            .iter()
            .map(|id| BlobId::from_hex(id))
            .collect::<Result<Vec<_>, _>>()?;
        let _collaboration_guard = self.lock_collaboration_writes().await;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(Default::default()).await?;
        if let Some(id) = lease_id {
            crate::gc::require_native_baseline_lease(
                &read,
                id,
                self.active_account_id(),
                crate::telemetry::unix_time_ms(),
            )
            .await?;
        }
        let metadata = load_metadata_many(&read, &ids).await?.into_vec();
        drop(read);
        let mut manifests = Vec::with_capacity(metadata.len());
        for metadata in metadata {
            manifests.push(match metadata {
                Some(metadata) => Some(self.publish_sync_blob_manifest(&metadata, lease_id).await?),
                None => None,
            });
        }
        Ok(manifests)
    }

    /// Materialize and publish at most one canonical anchor per storage commit.
    /// Raw chunk cache publication does not change the immutable source blob.
    async fn publish_sync_blob_manifest(
        &self,
        metadata: &crate::binary_cas::BlobMetadata,
        lease_id: Option<&str>,
    ) -> Result<SyncBlobManifest, LixError> {
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(Default::default()).await?;
        if let Some(id) = lease_id {
            crate::gc::require_native_baseline_lease(
                &read,
                id,
                self.active_account_id(),
                crate::telemetry::unix_time_ms(),
            )
            .await?;
        }
        let canonical =
            crate::binary_cas::load_streaming_canonical_manifest(&read, metadata).await?;
        let mut manifest = super::transfer::CanonicalUploadPlan {
            metadata: metadata.clone(),
            canonical,
        }
        .manifest();
        let present = chunk_presence_many(
            &read,
            &manifest
                .chunks
                .iter()
                .map(|chunk| ChunkHash::from_hex(&chunk.chunk_id))
                .collect::<Result<Vec<_>, _>>()?,
        )
        .await?;
        drop(read);
        let mut offset = 0u64;
        let mut first = 0usize;
        while first < manifest.chunks.len() {
            let mut end = first;
            let mut size = 0u64;
            while end < manifest.chunks.len() && size < crate::binary_cas::CHUNK_ANCHOR_BYTES as u64
            {
                size += manifest.chunks[end].size_bytes;
                end += 1;
            }
            let inline = metadata.size_bytes <= MAX_INLINE_SYNC_BLOB_BYTES as u64;
            // Manifest availability is also the authority push admission
            // guard. Authenticate resident content as well as missing cache
            // chunks; presence alone cannot authorize a published reference.
            {
                let read = adapter.begin_read(Default::default()).await?;
                if let Some(id) = lease_id {
                    crate::gc::require_native_baseline_lease(
                        &read,
                        id,
                        self.active_account_id(),
                        crate::telemetry::unix_time_ms(),
                    )
                    .await?;
                }
                let chunks =
                    crate::binary_cas::load_canonical_blob_anchor(&read, metadata, offset).await?;
                if chunks.len() != end - first
                    || chunks
                        .iter()
                        .zip(&manifest.chunks[first..end])
                        .any(|(chunk, expected)| {
                            chunk.receipt.hash.to_hex() != expected.chunk_id
                                || chunk.receipt.size_bytes != expected.size_bytes
                        })
                {
                    return Err(LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        "canonical serving anchor differs from its prepared manifest",
                    ));
                }
                if inline {
                    manifest.inline_bytes_base64 =
                        encode_manifest(metadata.hash, &chunks)?.inline_bytes_base64;
                }
                if present[first..end].iter().any(|value| !value) {
                    let mut writes = adapter.new_write_set();
                    let mut preconditions = Vec::new();
                    for (chunk, present) in chunks.iter().zip(&present[first..end]) {
                        if !present {
                            stage_verified_raw_chunk(
                                &mut writes,
                                chunk.receipt.hash,
                                &chunk.bytes,
                            )?;
                        }
                    }
                    stage_transfer_publication_fence(&read, &mut writes, &mut preconditions)
                        .await?;
                    drop(read);
                    let options = StorageWriteOptions {
                        preconditions,
                        await_durable: true,
                        ..Default::default()
                    };
                    if self.sync_mode_state().role() == super::SyncRole::Replica {
                        adapter
                            .commit_certified_replica_write_set(
                                super::certified_replica_write_capability(),
                                writes,
                                options,
                            )
                            .await?;
                    } else {
                        adapter.commit_write_set(writes, options).await?;
                    }
                }
            }
            offset += size;
            first = end;
        }
        if metadata.size_bytes == 0 {
            manifest.inline_bytes_base64 =
                Some(base64::engine::general_purpose::STANDARD.encode([]));
        }
        Ok(manifest)
    }

    pub(crate) async fn get_sync_chunk(&self, chunk_id: &str) -> Result<Option<Vec<u8>>, LixError> {
        self.get_sync_chunk_with_baseline_lease(chunk_id, None)
            .await
    }
    pub(crate) async fn get_sync_chunk_leased(
        &self,
        chunk_id: &str,
        lease_id: &str,
    ) -> Result<Option<Vec<u8>>, LixError> {
        self.get_sync_chunk_with_baseline_lease(chunk_id, Some(lease_id))
            .await
    }
    async fn get_sync_chunk_with_baseline_lease(
        &self,
        chunk_id: &str,
        lease_id: Option<&str>,
    ) -> Result<Option<Vec<u8>>, LixError> {
        let chunk_id = ChunkHash::from_hex(chunk_id)?;
        let adapter = self.storage_adapter();
        crate::handle::retry_expired_read(|| async {
            let read = adapter.begin_read(StorageReadOptions::default()).await?;
            if let Some(id) = lease_id {
                crate::gc::require_native_baseline_lease(
                    &read,
                    id,
                    self.active_account_id(),
                    crate::telemetry::unix_time_ms(),
                )
                .await?;
            }
            load_verified_chunk(&read, chunk_id).await
        })
        .await
    }

    pub(crate) async fn put_sync_chunk(
        &self,
        chunk_id: &str,
        bytes: &[u8],
    ) -> Result<(), LixError> {
        let _collaboration_guard = self.lock_collaboration_writes().await;
        let chunk_id = ChunkHash::from_hex(chunk_id)?;
        let adapter = self.storage_adapter();
        // Only precommit planning is restartable. A physical read may expire
        // during a migration heartbeat, but a commit outcome must not replay.
        let (writes, preconditions) = crate::handle::retry_expired_read(|| async {
            let mut writes = adapter.new_write_set();
            let mut preconditions = Vec::new();
            stage_verified_raw_chunk(&mut writes, chunk_id, bytes)?;
            let read = adapter.begin_read(StorageReadOptions::default()).await?;
            stage_transfer_publication_fence(&read, &mut writes, &mut preconditions).await?;
            Ok((writes, preconditions))
        })
        .await?;
        let options = StorageWriteOptions {
            preconditions,
            await_durable: true,
            ..StorageWriteOptions::default()
        };
        if self.sync_mode_state().role() == super::SyncRole::Replica {
            adapter
                .commit_certified_replica_write_set(
                    super::certified_replica_write_capability(),
                    writes,
                    options,
                )
                .await?;
        } else {
            adapter.commit_write_set(writes, options).await?;
        }
        Ok(())
    }

    /// Register one bounded group in a coherent read and one durable commit.
    /// Validate all members first; coalesce shared chunk/demand keys before
    /// writing so overlapping manifests cannot create duplicate mutations.
    pub(crate) async fn register_sync_blob_manifests(
        &self,
        wires: &[SyncBlobManifest],
    ) -> Result<Vec<SyncBlobRegistration>, LixError> {
        validate_manifest_group(wires)?;
        let decoded = wires
            .iter()
            .map(|wire| Ok((decode_manifest(wire)?, decode_inline_bytes(wire)?)))
            .collect::<Result<Vec<_>, LixError>>()?;
        let _guard = self.lock_collaboration_writes().await;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let hashes = decoded
            .iter()
            .filter(|(_, bytes)| bytes.is_none())
            .flat_map(|(manifest, _)| manifest.chunks.iter().map(|chunk| chunk.hash))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let presence = hashes
            .iter()
            .copied()
            .zip(chunk_presence_many(&read, &hashes).await?)
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut complete = Vec::new();
        let mut provided = Vec::new();
        let mut registrations = Vec::new();
        for (manifest, inline) in decoded {
            let missing_chunk_ids = if inline.is_some() {
                Vec::new()
            } else {
                manifest
                    .chunks
                    .iter()
                    .filter(|chunk| !presence[&chunk.hash])
                    .map(|chunk| chunk.hash.to_hex())
                    .collect::<Vec<_>>()
            };
            if missing_chunk_ids.is_empty() {
                let mut validation = adapter.new_write_set();
                if let Some(bytes) = inline {
                    stage_verified_inline_canonical_blob(&mut validation, &manifest, &bytes)?;
                    let mut offset = 0;
                    for receipt in &manifest.chunks {
                        let end = offset + receipt.size_bytes as usize;
                        provided.push(CanonicalBlobChunk {
                            receipt: *receipt,
                            bytes: bytes[offset..end].to_vec(),
                        });
                        offset = end;
                    }
                } else {
                    stage_verified_canonical_manifest(&read, &mut validation, &manifest).await?;
                }
                complete.push(manifest);
            }
            registrations.push(SyncBlobRegistration { missing_chunk_ids });
        }
        if !complete.is_empty() {
            let mut writes = adapter.new_write_set();
            let missing = crate::binary_cas::stage_deferred_canonical_manifests_with_chunks(
                &read,
                &mut writes,
                &complete,
                &provided,
            )
            .await?;
            if !missing.is_empty() {
                return Err(LixError::unknown("verified group became incomplete"));
            }
            let mut preconditions = Vec::new();
            stage_transfer_publication_fence(&read, &mut writes, &mut preconditions).await?;
            drop(read);
            let options = StorageWriteOptions {
                preconditions,
                await_durable: true,
                ..Default::default()
            };
            if self.sync_mode_state().role() == super::SyncRole::Replica {
                adapter
                    .commit_certified_replica_write_set(
                        super::certified_replica_write_capability(),
                        writes,
                        options,
                    )
                    .await?;
            } else {
                adapter.commit_write_set(writes, options).await?;
            }
        }
        Ok(registrations)
    }

    /// Authority-side manifest registration. The manifest becomes readable
    /// only after every referenced chunk is already present.
    pub(crate) async fn register_sync_blob_manifest(
        &self,
        wire: &SyncBlobManifest,
    ) -> Result<SyncBlobRegistration, LixError> {
        if wire.inline_bytes_base64.is_some() {
            return self.register_deferred_sync_blob_manifest(wire).await;
        }
        let _collaboration_guard = self.lock_collaboration_writes().await;
        let manifest = decode_manifest(wire)?;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let presence = chunk_presence_many(
            &read,
            &manifest
                .chunks
                .iter()
                .map(|chunk| chunk.hash)
                .collect::<Vec<_>>(),
        )
        .await?;
        let missing_chunk_ids = manifest
            .chunks
            .iter()
            .zip(presence)
            .filter_map(|(chunk, present)| (!present).then(|| chunk.hash.to_hex()))
            .collect::<Vec<_>>();
        if !missing_chunk_ids.is_empty() {
            return Ok(SyncBlobRegistration { missing_chunk_ids });
        }
        let mut writes = adapter.new_write_set();
        let mut preconditions = Vec::new();
        stage_verified_canonical_manifest(&read, &mut writes, &manifest).await?;
        stage_transfer_publication_fence(&read, &mut writes, &mut preconditions).await?;
        drop(read);
        let options = StorageWriteOptions {
            preconditions,
            await_durable: true,
            ..StorageWriteOptions::default()
        };
        if self.sync_mode_state().role() == super::SyncRole::Replica {
            adapter
                .commit_certified_replica_write_set(
                    super::certified_replica_write_capability(),
                    writes,
                    options,
                )
                .await?;
        } else {
            adapter.commit_write_set(writes, options).await?;
        }
        Ok(SyncBlobRegistration {
            missing_chunk_ids: Vec::new(),
        })
    }

    /// Durably registers a validated canonical manifest immediately, marking
    /// absent chunks for lazy hydration in the same atomic publication.
    ///
    /// Missing chunks do not roll back the manifest: reads name them with
    /// `LIX_SYNC_CHUNKS_REQUIRED`, and each later [`Self::put_sync_chunk`]
    /// clears its marker atomically with the payload.
    pub(crate) async fn register_deferred_sync_blob_manifest(
        &self,
        wire: &SyncBlobManifest,
    ) -> Result<SyncBlobRegistration, LixError> {
        let _collaboration_guard = self.lock_collaboration_writes().await;
        let manifest = decode_manifest(wire)?;
        let inline = decode_inline_bytes(wire)?;
        let adapter = self.storage_adapter();
        // Rebuild the complete unpublished plan after expiration so chunk
        // presence and the reclamation fence come from one coherent read.
        // Keep the durable commit outside this retry boundary.
        let (writes, preconditions, missing_chunk_ids) =
            crate::handle::retry_expired_read(|| async {
                let mut writes = adapter.new_write_set();
                let mut preconditions = Vec::new();
                if let Some(bytes) = inline.as_ref() {
                    stage_verified_inline_canonical_blob(&mut writes, &manifest, bytes)?;
                }
                let read = adapter.begin_read(StorageReadOptions::default()).await?;
                let missing_chunk_ids = if inline.is_some() {
                    Vec::new()
                } else {
                    stage_deferred_canonical_manifest(&read, &mut writes, &manifest)
                        .await?
                        .into_iter()
                        .map(|chunk| chunk.to_hex())
                        .collect::<Vec<_>>()
                };
                stage_transfer_publication_fence(&read, &mut writes, &mut preconditions).await?;
                Ok((writes, preconditions, missing_chunk_ids))
            })
            .await?;
        let options = StorageWriteOptions {
            preconditions,
            await_durable: true,
            ..StorageWriteOptions::default()
        };
        if self.sync_mode_state().role() == super::SyncRole::Replica {
            adapter
                .commit_certified_replica_write_set(
                    super::certified_replica_write_capability(),
                    writes,
                    options,
                )
                .await?;
        } else {
            adapter.commit_write_set(writes, options).await?;
        }
        Ok(SyncBlobRegistration { missing_chunk_ids })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_adapter::StorageReadOptions;

    fn wire_manifest(
        manifest: CanonicalBlobManifest,
        inline_bytes: Option<&[u8]>,
    ) -> SyncBlobManifest {
        SyncBlobManifest {
            blob_id: manifest.blob_id.to_hex(),
            size_bytes: manifest.size_bytes,
            chunks: manifest
                .chunks
                .into_iter()
                .map(|chunk| SyncBlobChunk {
                    chunk_id: chunk.hash.to_hex(),
                    size_bytes: chunk.size_bytes,
                })
                .collect(),
            inline_bytes_base64: inline_bytes
                .map(|bytes| base64::engine::general_purpose::STANDARD.encode(bytes)),
        }
    }

    fn manifest_response_with_chunks(chunk_count: usize) -> Vec<u8> {
        let chunk = format!(
            r#"{{"chunkId":"{}","sizeBytes":1}}"#,
            "0".repeat(MAX_SYNC_BLOB_ID_BYTES)
        );
        let chunks = std::iter::repeat(chunk.as_str())
            .take(chunk_count)
            .collect::<Vec<_>>()
            .join(",");
        format!(
            r#"[{{"blobId":"{}","sizeBytes":{chunk_count},"chunks":[{chunks}]}}]"#,
            "1".repeat(MAX_SYNC_BLOB_ID_BYTES),
        )
        .into_bytes()
    }

    #[test]
    fn bounded_manifest_response_requires_exact_request_cardinality() {
        assert_eq!(
            manifest_response_body_limit(1).unwrap(),
            crate::sync::transfer::MAX_MANIFEST_SINGLETON_ENCODED_BYTES + 2
        );
        assert_eq!(
            manifest_response_body_limit(crate::sync::MAX_SYNC_BLOB_BATCH_ITEMS).unwrap(),
            crate::sync::transfer::MAX_MANIFEST_SINGLETON_ENCODED_BYTES
                * crate::sync::MAX_SYNC_BLOB_BATCH_ITEMS
                + crate::sync::MAX_SYNC_BLOB_BATCH_ITEMS
                + 1
        );
        let valid = format!(
            r#"{{"blobId":"{}","sizeBytes":0,"chunks":[]}}"#,
            "0".repeat(MAX_SYNC_BLOB_ID_BYTES)
        );
        let too_many = format!(r#"[{valid},{{"blobId":null}}]"#);
        let error = decode_manifest_response(too_many.as_bytes(), 1)
            .expect_err("extra manifests must be rejected at the request bound");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(error.message.contains("exactly 1 sync blob manifests"));

        let error = decode_manifest_response(format!("[{valid}]").as_bytes(), 2)
            .expect_err("an omitted requested manifest must be rejected");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(error.message.contains("exactly 2 sync blob manifests"));

        let maximum_batch = format!(
            "[{}]",
            std::iter::repeat(valid.as_str())
                .take(crate::sync::MAX_SYNC_BLOB_BATCH_ITEMS)
                .collect::<Vec<_>>()
                .join(",")
        );
        assert_eq!(
            decode_manifest_response(
                maximum_batch.as_bytes(),
                crate::sync::MAX_SYNC_BLOB_BATCH_ITEMS,
            )
            .expect("the complete documented request batch remains accepted")
            .len(),
            crate::sync::MAX_SYNC_BLOB_BATCH_ITEMS
        );

        for invalid_count in [0, crate::sync::MAX_SYNC_BLOB_BATCH_ITEMS + 1] {
            let error = decode_manifest_response(b"[]", invalid_count)
                .expect_err("out-of-contract request sizes must fail before parsing");
            assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        }
    }

    #[test]
    fn bounded_manifest_response_accepts_maximum_receipts_and_rejects_one_more() {
        let maximum =
            decode_manifest_response(&manifest_response_with_chunks(MAX_SYNC_BLOB_CHUNKS), 1)
                .expect("the full documented receipt inventory remains accepted");
        assert_eq!(maximum[0].chunks.len(), MAX_SYNC_BLOB_CHUNKS);

        let mut maximum_known_fields =
            String::from_utf8(manifest_response_with_chunks(MAX_SYNC_BLOB_CHUNKS)).unwrap();
        let max_inline =
            base64::engine::general_purpose::STANDARD.encode(vec![0; MAX_INLINE_SYNC_BLOB_BYTES]);
        let inline_field = format!(",\"inlineBytesBase64\":\"{max_inline}\"");
        let insert_at = maximum_known_fields.len() - 2;
        maximum_known_fields.insert_str(insert_at, &inline_field);
        assert!(
            maximum_known_fields.len() <= manifest_response_body_limit(1).unwrap(),
            "all maximum known fields fit the one-manifest response budget"
        );
        let decoded = decode_manifest_response(maximum_known_fields.as_bytes(), 1)
            .expect("maximum independently accepted receipt and inline fields remain decodable");
        assert_eq!(decoded[0].chunks.len(), MAX_SYNC_BLOB_CHUNKS);
        assert_eq!(
            decoded[0].inline_bytes_base64.as_ref().unwrap().len(),
            max_inline.len()
        );

        let error =
            decode_manifest_response(&manifest_response_with_chunks(MAX_SYNC_BLOB_CHUNKS + 1), 1)
                .expect_err("the first receipt above the schema limit must be rejected");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(error.message.contains("at most 16384 sync blob chunks"));
    }

    #[test]
    fn bounded_manifest_response_checks_string_lengths_before_owning_them() {
        let valid_chunk_id = "a".repeat(MAX_SYNC_BLOB_ID_BYTES);
        let valid = format!(
            r#"[{{"blobId":"{}","sizeBytes":1,"chunks":[{{"chunkId":"{valid_chunk_id}","sizeBytes":1}}]}}]"#,
            "b".repeat(MAX_SYNC_BLOB_ID_BYTES)
        );
        assert_eq!(
            decode_manifest_response(valid.as_bytes(), 1)
                .expect("64-byte wire identities are accepted")[0]
                .chunks[0]
                .chunk_id
                .len(),
            MAX_SYNC_BLOB_ID_BYTES
        );

        let invalid = format!(
            r#"[{{"blobId":"{}","sizeBytes":1,"chunks":[{{"chunkId":"{}","sizeBytes":1}}]}}]"#,
            "b".repeat(MAX_SYNC_BLOB_ID_BYTES),
            "a".repeat(MAX_SYNC_BLOB_ID_BYTES + 1),
        );
        let error = decode_manifest_response(invalid.as_bytes(), 1)
            .expect_err("oversized chunk identities must be rejected during decoding");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(error.message.contains("64 byte limit"));

        let max_inline =
            base64::engine::general_purpose::STANDARD.encode(vec![0; MAX_INLINE_SYNC_BLOB_BYTES]);
        let inline_wire = format!(
            r#"[{{"blobId":"{}","sizeBytes":0,"chunks":[],"inlineBytesBase64":"{max_inline}"}}]"#,
            "c".repeat(MAX_SYNC_BLOB_ID_BYTES),
        );
        assert_eq!(
            decode_manifest_response(inline_wire.as_bytes(), 1)
                .expect("the existing 256 KiB decoded inline limit remains accepted")[0]
                .inline_bytes_base64
                .as_ref()
                .expect("inline value should survive bounded decoding")
                .len(),
            max_inline.len()
        );

        let oversized_inline = "A".repeat(MAX_INLINE_SYNC_BLOB_BYTES.div_ceil(3) * 4 + 1);
        let inline_wire = format!(
            r#"[{{"blobId":"{}","sizeBytes":0,"chunks":[],"inlineBytesBase64":"{oversized_inline}"}}]"#,
            "c".repeat(MAX_SYNC_BLOB_ID_BYTES),
        );
        let error = decode_manifest_response(inline_wire.as_bytes(), 1)
            .expect_err("inline strings above the existing encoded cap must be rejected");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(error.message.contains("349528 byte limit"));
    }

    #[test]
    fn bounded_pull_response_caps_events_and_inline_manifest_shape() {
        let minimum_inline = SyncBlobManifest {
            blob_id: "0".repeat(64),
            size_bytes: 0,
            chunks: Vec::new(),
            inline_bytes_base64: Some(String::new()),
        };
        assert_eq!(
            serde_json::to_vec(&minimum_inline).unwrap().len(),
            MIN_INLINE_MANIFEST_JSON_BYTES,
            "the response-derived manifest count uses the accepted wire minimum"
        );

        let empty_event = r#"{"cursor":1,"commits":[],"refUpdates":[],"inlineBlobs":[]}"#;
        let maximum = format!(
            r#"{{"kind":"delta","cursor":512,"events":[{}]}}"#,
            std::iter::repeat(empty_event)
                .take(crate::sync::MAX_SYNC_REQUEST_ITEMS)
                .collect::<Vec<_>>()
                .join(",")
        );
        let decoded =
            decode_pull_response(&maximum.into_bytes(), crate::sync::MAX_SYNC_REQUEST_ITEMS)
                .expect("the requested maximum delta page remains accepted");
        let crate::sync::SyncRepositoryPullResponse::Delta { events, .. } = decoded else {
            panic!("expected a delta response")
        };
        assert_eq!(events.len(), crate::sync::MAX_SYNC_REQUEST_ITEMS);

        let one_event = format!("[{empty_event}]");
        let response = format!(r#"{{"kind":"delta","cursor":1,"events":{one_event}}}"#);
        let too_many_events =
            response.replace(&one_event, &format!("[{empty_event},{empty_event}]"));
        let error = decode_pull_response(too_many_events.as_bytes(), 1)
            .expect_err("a pull cannot return more events than its requested limit");
        assert!(error.message.contains("requested event count"));

        let chunk = format!(r#"{{"chunkId":"{}","sizeBytes":1}}"#, "1".repeat(64));
        let too_many_chunks = format!(
            r#"{{"kind":"delta","cursor":1,"events":[{{"cursor":1,"commits":[],"refUpdates":[],"inlineBlobs":[{{"blobId":"{}","sizeBytes":2,"chunks":[{chunk},{chunk}],"inlineBytesBase64":"AQI="}}]}}]}}"#,
            "0".repeat(64),
        );
        let error = decode_pull_response(too_many_chunks.as_bytes(), 1)
            .expect_err("inline manifests use the server's one-receipt lane");
        assert!(error.message.contains("at most 1 sync blob chunks"));

        let max_inline =
            base64::engine::general_purpose::STANDARD.encode(vec![0; MAX_INLINE_SYNC_BLOB_BYTES]);
        let maximum_inline = format!(
            r#"{{"kind":"delta","cursor":1,"events":[{{"cursor":1,"commits":[],"refUpdates":[],"inlineBlobs":[{{"blobId":"{}","sizeBytes":{},"chunks":[],"inlineBytesBase64":"{max_inline}"}}]}}]}}"#,
            "0".repeat(64),
            MAX_INLINE_SYNC_BLOB_BYTES,
        );
        decode_pull_response(maximum_inline.as_bytes(), 1)
            .expect("the existing 256 KiB inline payload acceptance remains intact");
    }

    #[test]
    fn bounded_pull_response_preserves_large_atomic_ref_update_event() {
        let ref_updates = (0..513)
            .map(|index| crate::sync::protocol::SyncRefUpdate {
                branch_id: format!("01920000-0000-7000-8000-{index:012x}"),
                author_id: Some(crate::ANONYMOUS_ACCOUNT_ID.to_owned()),
                ref_change_id: Some(format!("01920000-0000-7001-8000-{index:012x}")),
                expected_ref_change_id: None,
                expected_head_commit_id: None,
                expected_checkpoint_commit_id: None,
                head_commit_id: Some("01920000-0000-7002-8000-000000000001".to_owned()),
                checkpoint_commit_id: Some("01920000-0000-7002-8000-000000000001".to_owned()),
            })
            .collect();
        let response = crate::sync::SyncRepositoryPullResponse::Delta {
            cursor: 1,
            events: vec![crate::sync::SyncEvent {
                cursor: 1,
                commits: Vec::new(),
                ref_updates,
                inline_blobs: Vec::new(),
            }],
        };
        let wire = serde_json::to_vec(&response).expect("encode valid 513-ref event");

        let decoded = decode_pull_response(&wire, 1)
            .expect("one atomic event may contain more than 512 ref updates");
        let crate::sync::SyncRepositoryPullResponse::Delta { events, .. } = decoded else {
            panic!("expected a delta response")
        };
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].ref_updates.len(), 513);
        assert!(events[0].ref_updates.iter().all(|update| {
            update.branch_id.starts_with("01920000-0000-7000-8000-")
                && update
                    .ref_change_id
                    .as_deref()
                    .is_some_and(|id| id.starts_with("01920000-0000-7001-8000-"))
        }));
    }

    #[tokio::test]
    async fn deferred_manifest_replans_after_heartbeat_before_publication() {
        let storage = crate::migration::CommitExpiringStorage::new();
        let lix = crate::open_lix()
            .with_storage(storage.clone())
            .await
            .unwrap();
        let bytes = vec![42; MAX_INLINE_SYNC_BLOB_BYTES + 1];
        let canonical = CanonicalBlobManifest::from_bytes(&bytes);
        let manifest = wire_manifest(canonical.clone(), None);
        let first = canonical.chunks.first().unwrap();
        // Expire after observing chunk presence, before reading the CAS
        // reclamation fence. Reusing that physical plan must fail.
        storage.expire_after_point_read(
            lix.storage_adapter()
                .epoch_bank()
                .map_space(crate::binary_cas::BINARY_CAS_CHUNK_PRESENCE_SPACE),
            crate::storage_adapter::StorageKey(bytes::Bytes::copy_from_slice(
                first.hash.as_bytes(),
            )),
        );
        let registration = lix
            .register_deferred_sync_blob_manifest(&manifest)
            .await
            .unwrap();
        assert!(storage.point_expiration_was_observed());
        assert_eq!(
            registration.missing_chunk_ids,
            canonical
                .chunks
                .iter()
                .map(|chunk| chunk.hash.to_hex())
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>()
        );
        assert!(lix.has_sync_blob_manifest(&manifest.blob_id).await.unwrap());
        let mut offset = 0;
        for chunk in &canonical.chunks {
            let end = offset + chunk.size_bytes as usize;
            lix.put_sync_chunk(&chunk.hash.to_hex(), &bytes[offset..end])
                .await
                .unwrap();
            offset = end;
        }
        assert!(
            lix.register_deferred_sync_blob_manifest(&manifest)
                .await
                .unwrap()
                .missing_chunk_ids
                .is_empty()
        );
        let adapter = lix.storage_adapter();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        assert_eq!(
            crate::binary_cas::load_bytes_many(&read, &[canonical.blob_id])
                .await
                .unwrap()
                .into_vec(),
            vec![Some(bytes)]
        );
        drop(read);
        lix.close().await.unwrap();
    }

    #[test]
    fn canonical_chunk_materialization_already_contains_the_complete_manifest() {
        let first_bytes = b"first canonical transfer chunk".to_vec();
        let second_bytes = b"second canonical transfer chunk".to_vec();
        let chunks = vec![
            CanonicalBlobChunk {
                receipt: BlobChunkReceipt {
                    hash: ChunkHash::from_content(&first_bytes),
                    size_bytes: first_bytes.len() as u64,
                },
                bytes: first_bytes,
            },
            CanonicalBlobChunk {
                receipt: BlobChunkReceipt {
                    hash: ChunkHash::from_content(&second_bytes),
                    size_bytes: second_bytes.len() as u64,
                },
                bytes: second_bytes,
            },
        ];
        let expected_size = chunks
            .iter()
            .map(|chunk| chunk.receipt.size_bytes)
            .sum::<u64>();
        let blob_id = BlobId::from_chunks(
            expected_size,
            chunks
                .iter()
                .map(|chunk| (chunk.receipt.hash, chunk.receipt.size_bytes)),
        );

        let manifest = encode_manifest(blob_id, &chunks)
            .expect("canonical chunks encode without another blob read");

        assert_eq!(manifest.blob_id, blob_id.to_hex());
        assert_eq!(manifest.size_bytes, expected_size);
        assert_eq!(
            manifest.chunks,
            chunks
                .iter()
                .map(|chunk| SyncBlobChunk {
                    chunk_id: chunk.receipt.hash.to_hex(),
                    size_bytes: chunk.receipt.size_bytes,
                })
                .collect::<Vec<_>>()
        );
        assert!(manifest.inline_bytes_base64.is_none());
    }

    #[tokio::test]
    async fn grouped_registration_is_atomic_and_replayable() {
        let lix = crate::open_lix().await.unwrap();
        let bytes = [
            b"grouped payload one".to_vec(),
            b"grouped payload two".to_vec(),
        ];
        let manifests = bytes
            .iter()
            .map(|bytes| wire_manifest(CanonicalBlobManifest::from_bytes(bytes), Some(bytes)))
            .collect::<Vec<_>>();
        let mut damaged = manifests.clone();
        damaged[1].inline_bytes_base64 =
            Some(base64::engine::general_purpose::STANDARD.encode(b"wrong"));
        assert!(lix.register_sync_blob_manifests(&damaged).await.is_err());
        assert!(
            lix.get_sync_blob_manifest(&manifests[0].blob_id)
                .await
                .unwrap()
                .is_none()
        );
        for _ in 0..2 {
            let registrations = lix.register_sync_blob_manifests(&manifests).await.unwrap();
            assert_eq!(registrations.len(), 2);
            assert!(
                registrations
                    .iter()
                    .all(|registration| registration.missing_chunk_ids.is_empty())
            );
        }
        for (manifest, bytes) in manifests.iter().zip(bytes) {
            let chunk = &manifest.chunks[0].chunk_id;
            assert_eq!(lix.get_sync_chunk(chunk).await.unwrap().unwrap(), bytes);
        }
        lix.close().await.unwrap();
    }

    #[tokio::test]
    async fn inline_manifest_registers_small_payload_without_chunk_demand() {
        let lix = crate::open_lix()
            .await
            .expect("test repository should open");
        let bytes = b"small realtime markdown payload".to_vec();
        let canonical = CanonicalBlobManifest::from_bytes(&bytes);
        assert_eq!(canonical.chunks.len(), 1);
        let manifest = wire_manifest(canonical.clone(), Some(&bytes));

        let deferred = wire_manifest(canonical.clone(), None);
        let registration = lix
            .register_deferred_sync_blob_manifest(&deferred)
            .await
            .expect("manifest-only registration should stage demand");
        assert_eq!(registration.missing_chunk_ids.len(), 1);
        let registration = lix
            .register_deferred_sync_blob_manifest(&manifest)
            .await
            .expect("inline registration should satisfy staged demand");
        assert!(registration.missing_chunk_ids.is_empty());
        let registration = lix
            .register_deferred_sync_blob_manifest(&manifest)
            .await
            .expect("repeated inline registration should be idempotent");
        assert!(registration.missing_chunk_ids.is_empty());

        let adapter = lix.storage_adapter();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("verification read should open");
        let loaded = crate::binary_cas::load_bytes_many(&read, &[canonical.blob_id])
            .await
            .expect("inline payload should already be readable")
            .into_vec();
        assert_eq!(loaded, vec![Some(bytes.clone())]);

        let authority = crate::open_lix()
            .await
            .expect("authority repository should open");
        let registration = authority
            .register_sync_blob_manifest(&manifest)
            .await
            .expect("authority should accept a self-contained manifest");
        assert!(registration.missing_chunk_ids.is_empty());
        let registration = authority
            .register_sync_blob_manifest(&manifest)
            .await
            .expect("authority inline registration should be idempotent");
        assert!(registration.missing_chunk_ids.is_empty());
        let read = authority
            .storage_adapter()
            .begin_read(StorageReadOptions::default())
            .await
            .expect("authority verification read should open");
        let loaded = crate::binary_cas::load_bytes_many(&read, &[canonical.blob_id])
            .await
            .expect("authority inline payload should already be readable")
            .into_vec();
        assert_eq!(loaded, vec![Some(bytes)]);
    }

    #[tokio::test]
    async fn inline_manifest_registers_an_authenticated_empty_blob_without_chunk_demand() {
        let lix = crate::open_lix()
            .await
            .expect("test repository should open");
        let canonical = CanonicalBlobManifest::from_bytes(&[]);
        assert!(canonical.chunks.is_empty());
        let manifest = wire_manifest(canonical.clone(), Some(&[]));

        let registration = lix
            .register_sync_blob_manifest(&manifest)
            .await
            .expect("an authenticated empty inline manifest should register");
        assert!(registration.missing_chunk_ids.is_empty());

        let read = lix
            .storage_adapter()
            .begin_read(StorageReadOptions::default())
            .await
            .expect("verification read should open");
        let loaded = crate::binary_cas::load_bytes_many(&read, &[canonical.blob_id])
            .await
            .expect("the empty inline blob should be readable")
            .into_vec();
        assert_eq!(loaded, vec![Some(Vec::new())]);
    }

    #[tokio::test]
    async fn inline_manifest_rejects_tampered_payload() {
        let lix = crate::open_lix()
            .await
            .expect("test repository should open");
        let bytes = b"authenticated inline payload".to_vec();
        let canonical = CanonicalBlobManifest::from_bytes(&bytes);
        let manifest = wire_manifest(canonical, Some(b"xuthenticated inline payload"));

        let error = lix
            .register_sync_blob_manifest(&manifest)
            .await
            .expect_err("tampered inline payload must fail");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
    }

    #[tokio::test]
    async fn inline_manifest_enforces_the_sixty_four_kibibyte_cap() {
        let lix = crate::open_lix()
            .await
            .expect("test repository should open");
        let at_limit = vec![7; MAX_INLINE_SYNC_BLOB_BYTES];
        let canonical = CanonicalBlobManifest::from_bytes(&at_limit);
        assert_eq!(canonical.chunks.len(), 1);
        let manifest = wire_manifest(canonical, Some(&at_limit));
        lix.register_sync_blob_manifest(&manifest)
            .await
            .expect("an exact-cap one-chunk payload should register inline");

        let over_limit = vec![7; MAX_INLINE_SYNC_BLOB_BYTES + 1];
        let canonical = CanonicalBlobManifest::from_bytes(&over_limit);
        assert_eq!(canonical.chunks.len(), 1);
        let manifest = wire_manifest(canonical, Some(&over_limit));
        let error = lix
            .register_sync_blob_manifest(&manifest)
            .await
            .expect_err("an oversized inline payload must fail");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
    }

    #[tokio::test]
    async fn deferred_registration_publishes_manifest_and_demand_without_payload_rows() {
        let lix = crate::open_lix()
            .await
            .expect("test repository should open");
        let bytes = (0..5 * 1024 * 1024 + 19)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let canonical = CanonicalBlobManifest::from_bytes(&bytes);
        assert!(canonical.chunks.len() > 1);
        let manifest = wire_manifest(canonical.clone(), None);

        let registration = lix
            .register_deferred_sync_blob_manifest(&manifest)
            .await
            .expect("deferred registration should commit");
        let expected_missing = canonical
            .chunks
            .iter()
            .map(|chunk| chunk.hash.to_hex())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(registration.missing_chunk_ids, expected_missing);

        let adapter = lix.storage_adapter();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("verification read should open");
        for chunk in &canonical.chunks {
            assert!(
                load_verified_chunk(&read, chunk.hash)
                    .await
                    .expect("chunk lookup should succeed")
                    .is_none()
            );
        }
        let error = crate::binary_cas::load_bytes_many(&read, &[canonical.blob_id])
            .await
            .expect_err("read must demand absent payloads");
        assert_eq!(error.code, "LIX_SYNC_CHUNKS_REQUIRED");
        assert_eq!(
            error.details.unwrap()["chunkIds"],
            serde_json::json!(registration.missing_chunk_ids)
        );
    }

    #[tokio::test]
    async fn deferred_registration_rejects_an_invalid_manifest_before_requesting_chunks() {
        let lix = crate::open_lix()
            .await
            .expect("test repository should open");
        let bytes = vec![7; 5 * 1024 * 1024];
        let canonical = CanonicalBlobManifest::from_bytes(&bytes);
        let mut manifest = wire_manifest(canonical, None);
        manifest.size_bytes += 1;

        let error = lix
            .register_sync_blob_manifest(&manifest)
            .await
            .expect_err("an invalid manifest must fail before chunk admission");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
    }
}
