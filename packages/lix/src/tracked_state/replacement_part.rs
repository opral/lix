//! Point- and range-addressable immutable identity parts for complete replacements.
//!
//! A replacement part contains the ordered tracked key and canonical
//! snapshot/metadata authority. Small JSON remains inline; large JSON keeps
//! its content-addressed reference to avoid duplicate storage.
//! Commit-wide timestamps, change identifiers, lifecycle metadata, and the
//! row-group set identity belong to the publishing manifest.

// Point/range/ordinal routing is the public handoff to the columnar scan layer;
// UPDATE publication currently consumes only the writer and strict decoder.
#![allow(dead_code)]

use std::borrow::Cow;
use std::ops::Range;

use bytes::Bytes;

use crate::LixError;

pub(crate) const REPLACEMENT_PART_MAX_ROWS: usize = 512;
pub(crate) const REPLACEMENT_PART_TARGET_BYTES: usize = 64 * 1024;
pub(crate) const REPLACEMENT_PART_MAX_BYTES: usize = 4 * 1024 * 1024;

const REPLACEMENT_PART_MAGIC: &[u8; 8] = b"LXRPI003";
const REPLACEMENT_PART_COMPRESSED_MAGIC: &[u8; 8] = b"LXRPZ003";
const REPLACEMENT_PART_MAX_DECODED_BYTES: usize = 16 * 1024 * 1024;
const REPLACEMENT_DIRECTORY_MAGIC: &[u8; 8] = b"LXRPD001";
const REPLACEMENT_PART_DIGEST_CONTEXT: &str = "lix tracked-state replacement identity part v1";
const REPLACEMENT_DIRECTORY_DIGEST_CONTEXT: &str =
    "lix tracked-state replacement part directory v1";
const DIGEST_BYTES: usize = 32;
const DIRECTORY_FIXED_ENTRY_BYTES: usize = DIGEST_BYTES + 4 + 2 + 4 + 4;

#[derive(Clone, Copy, Debug)]
pub(crate) struct ReplacementPartRowRef<'a> {
    /// Canonical bytes produced by the tracked-state key codec.
    pub(crate) encoded_key: &'a [u8],
    pub(crate) snapshot: crate::json_store::JsonSlotRef<'a>,
    pub(crate) metadata: crate::json_store::JsonSlotRef<'a>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EncodedReplacementPart {
    digest: [u8; DIGEST_BYTES],
    bytes: Bytes,
    first_key: Bytes,
    last_key: Bytes,
    row_count: u16,
}

impl EncodedReplacementPart {
    pub(crate) fn digest(&self) -> &[u8; DIGEST_BYTES] {
        &self.digest
    }

    pub(crate) fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    pub(crate) fn first_key(&self) -> &[u8] {
        &self.first_key
    }

    pub(crate) fn last_key(&self) -> &[u8] {
        &self.last_key
    }

    pub(crate) fn row_count(&self) -> u16 {
        self.row_count
    }

    pub(crate) fn directory_entry(&self, first_ordinal: u32) -> ReplacementPartDirectoryEntry {
        ReplacementPartDirectoryEntry {
            digest: self.digest,
            first_key: self.first_key.clone(),
            last_key: self.last_key.clone(),
            first_ordinal,
            row_count: self.row_count,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DecodedReplacementPart {
    key_arena: Bytes,
    key_ranges: Vec<Range<usize>>,
    snapshots: Vec<crate::json_store::JsonSlot>,
    metadata: Vec<crate::json_store::JsonSlot>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReplacementPartMatch {
    pub(crate) ordinal: u16,
}

impl DecodedReplacementPart {
    pub(crate) fn len(&self) -> usize {
        self.key_ranges.len()
    }

    pub(crate) fn first_key(&self) -> Option<&[u8]> {
        self.key_ranges
            .first()
            .map(|range| &self.key_arena[range.clone()])
    }

    pub(crate) fn last_key(&self) -> Option<&[u8]> {
        self.key_ranges
            .last()
            .map(|range| &self.key_arena[range.clone()])
    }

    pub(crate) fn key(&self, ordinal: usize) -> Result<Option<&[u8]>, LixError> {
        Ok(self
            .key_ranges
            .get(ordinal)
            .map(|range| &self.key_arena[range.clone()]))
    }

    pub(crate) fn snapshot(
        &self,
        ordinal: usize,
    ) -> Result<Option<crate::json_store::JsonSlotRef<'_>>, LixError> {
        Ok(self.snapshots.get(ordinal).map(|slot| slot.as_ref_slot()))
    }

    pub(crate) fn metadata(
        &self,
        ordinal: usize,
    ) -> Result<Option<crate::json_store::JsonSlotRef<'_>>, LixError> {
        Ok(self.metadata.get(ordinal).map(|slot| slot.as_ref_slot()))
    }

    pub(crate) fn find(
        &self,
        encoded_key: &[u8],
    ) -> Result<Option<ReplacementPartMatch>, LixError> {
        let mut lower = 0usize;
        let mut upper = self.key_ranges.len();
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            let key = self.key(middle)?.ok_or_else(|| {
                replacement_part_error("replacement part omitted a key during binary search")
            })?;
            if key < encoded_key {
                lower = middle + 1;
            } else {
                upper = middle;
            }
        }
        let Some(key) = self.key(lower)? else {
            return Ok(None);
        };
        if key != encoded_key {
            return Ok(None);
        }
        Ok(Some(ReplacementPartMatch {
            ordinal: u16::try_from(lower)
                .expect("replacement part row count is bounded below u16::MAX"),
        }))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReplacementPartDirectoryEntry {
    digest: [u8; DIGEST_BYTES],
    first_key: Bytes,
    last_key: Bytes,
    first_ordinal: u32,
    row_count: u16,
}

impl ReplacementPartDirectoryEntry {
    pub(crate) fn new(
        digest: [u8; DIGEST_BYTES],
        first_key: &[u8],
        last_key: &[u8],
        first_ordinal: u32,
        row_count: u16,
    ) -> Self {
        Self {
            digest,
            first_key: Bytes::copy_from_slice(first_key),
            last_key: Bytes::copy_from_slice(last_key),
            first_ordinal,
            row_count,
        }
    }

    pub(crate) fn digest(&self) -> &[u8; DIGEST_BYTES] {
        &self.digest
    }

    pub(crate) fn first_key(&self) -> &[u8] {
        &self.first_key
    }

    pub(crate) fn last_key(&self) -> &[u8] {
        &self.last_key
    }

    pub(crate) fn first_ordinal(&self) -> u32 {
        self.first_ordinal
    }

    pub(crate) fn row_count(&self) -> u16 {
        self.row_count
    }

    pub(crate) fn end_ordinal(&self) -> u32 {
        self.first_ordinal + u32::from(self.row_count)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReplacementPartDirectory {
    entries: Vec<ReplacementPartDirectoryEntry>,
    row_count: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReplacementPartOrdinalRoute<'a> {
    pub(crate) entry: &'a ReplacementPartDirectoryEntry,
    pub(crate) local_ordinal: u16,
}

impl ReplacementPartDirectory {
    pub(crate) fn try_new(
        entries: Vec<ReplacementPartDirectoryEntry>,
        row_count: u32,
    ) -> Result<Self, LixError> {
        let directory = Self { entries, row_count };
        directory.validate()?;
        Ok(directory)
    }

    pub(crate) fn entries(&self) -> &[ReplacementPartDirectoryEntry] {
        &self.entries
    }

    pub(crate) fn row_count(&self) -> u32 {
        self.row_count
    }

    pub(crate) fn encode(&self) -> Result<Bytes, LixError> {
        self.validate()?;
        let variable_bytes = self.entries.iter().try_fold(0usize, |total, entry| {
            total
                .checked_add(entry.first_key.len())
                .and_then(|total| total.checked_add(entry.last_key.len()))
                .ok_or_else(|| replacement_part_error("replacement directory size overflows"))
        })?;
        let capacity = REPLACEMENT_DIRECTORY_MAGIC
            .len()
            .checked_add(8)
            .and_then(|size| {
                size.checked_add(
                    self.entries
                        .len()
                        .checked_mul(DIRECTORY_FIXED_ENTRY_BYTES)?,
                )
            })
            .and_then(|size| size.checked_add(variable_bytes))
            .ok_or_else(|| replacement_part_error("replacement directory size overflows"))?;
        let mut encoded = Vec::with_capacity(capacity);
        encoded.extend_from_slice(REPLACEMENT_DIRECTORY_MAGIC);
        encoded.extend_from_slice(&self.row_count.to_be_bytes());
        encoded.extend_from_slice(
            &u32::try_from(self.entries.len())
                .map_err(|_| replacement_part_error("replacement directory has too many parts"))?
                .to_be_bytes(),
        );
        for entry in &self.entries {
            encoded.extend_from_slice(&entry.digest);
            encoded.extend_from_slice(&entry.first_ordinal.to_be_bytes());
            encoded.extend_from_slice(&entry.row_count.to_be_bytes());
            encode_sized_bytes(&mut encoded, &entry.first_key)?;
            encode_sized_bytes(&mut encoded, &entry.last_key)?;
        }
        Ok(Bytes::from(encoded))
    }

    pub(crate) fn digest(&self) -> Result<[u8; DIGEST_BYTES], LixError> {
        Ok(domain_digest(
            REPLACEMENT_DIRECTORY_DIGEST_CONTEXT,
            &self.encode()?,
        ))
    }

    pub(crate) fn decode(encoded: &[u8]) -> Result<Self, LixError> {
        let mut cursor = 0usize;
        take_exact(encoded, &mut cursor, REPLACEMENT_DIRECTORY_MAGIC.len())?
            .eq(REPLACEMENT_DIRECTORY_MAGIC)
            .then_some(())
            .ok_or_else(|| replacement_part_error("replacement directory has invalid magic"))?;
        let row_count = decode_u32(encoded, &mut cursor)?;
        let entry_count = usize::try_from(decode_u32(encoded, &mut cursor)?)
            .expect("u32 directory count fits usize");
        let minimum_remaining = entry_count
            .checked_mul(DIRECTORY_FIXED_ENTRY_BYTES)
            .ok_or_else(|| replacement_part_error("replacement directory count overflows"))?;
        if encoded.len().saturating_sub(cursor) < minimum_remaining {
            return Err(replacement_part_error("replacement directory is truncated"));
        }
        let mut entries = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            let digest = take_exact(encoded, &mut cursor, DIGEST_BYTES)?
                .try_into()
                .expect("replacement digest length was checked");
            let first_ordinal = decode_u32(encoded, &mut cursor)?;
            let row_count = decode_u16(encoded, &mut cursor)?;
            let first_key = Bytes::copy_from_slice(decode_sized_bytes(encoded, &mut cursor)?);
            let last_key = Bytes::copy_from_slice(decode_sized_bytes(encoded, &mut cursor)?);
            entries.push(ReplacementPartDirectoryEntry {
                digest,
                first_key,
                last_key,
                first_ordinal,
                row_count,
            });
        }
        if cursor != encoded.len() {
            return Err(replacement_part_error(
                "replacement directory has trailing bytes",
            ));
        }
        Self::try_new(entries, row_count)
    }

    pub(crate) fn decode_content_addressed(
        expected_digest: &[u8; DIGEST_BYTES],
        encoded: &[u8],
    ) -> Result<Self, LixError> {
        if &domain_digest(REPLACEMENT_DIRECTORY_DIGEST_CONTEXT, encoded) != expected_digest {
            return Err(replacement_part_error(
                "replacement directory content digest mismatch",
            ));
        }
        Self::decode(encoded)
    }

    pub(crate) fn route_key(&self, encoded_key: &[u8]) -> Option<&ReplacementPartDirectoryEntry> {
        let mut lower = 0usize;
        let mut upper = self.entries.len();
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            if self.entries[middle].first_key.as_ref() <= encoded_key {
                lower = middle + 1;
            } else {
                upper = middle;
            }
        }
        let entry = self.entries.get(lower.checked_sub(1)?)?;
        (encoded_key <= entry.last_key.as_ref()).then_some(entry)
    }

    pub(crate) fn route_ordinal(&self, ordinal: u32) -> Option<ReplacementPartOrdinalRoute<'_>> {
        if ordinal >= self.row_count {
            return None;
        }
        let mut lower = 0usize;
        let mut upper = self.entries.len();
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            if self.entries[middle].first_ordinal <= ordinal {
                lower = middle + 1;
            } else {
                upper = middle;
            }
        }
        let entry = self.entries.get(lower.checked_sub(1)?)?;
        let local = ordinal.checked_sub(entry.first_ordinal)?;
        (local < u32::from(entry.row_count)).then_some(ReplacementPartOrdinalRoute {
            entry,
            local_ordinal: u16::try_from(local).expect("local ordinal is bounded by u16 row count"),
        })
    }

    /// Returns the contiguous directory slice whose key bounds may intersect
    /// `[lower, upper)`. Gaps between parts remain gaps; callers still verify
    /// exact membership inside each decoded part.
    pub(crate) fn parts_overlapping(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> Range<usize> {
        if lower
            .zip(upper)
            .is_some_and(|(lower, upper)| lower >= upper)
        {
            return 0..0;
        }
        let start = lower.map_or(0, |lower| {
            self.entries
                .partition_point(|entry| entry.last_key.as_ref() < lower)
        });
        let end = upper.map_or(self.entries.len(), |upper| {
            self.entries
                .partition_point(|entry| entry.first_key.as_ref() < upper)
        });
        start.min(end)..end
    }

    fn validate(&self) -> Result<(), LixError> {
        if self.row_count == 0 || self.entries.is_empty() {
            return Err(replacement_part_error(
                "replacement directory must contain at least one row",
            ));
        }
        let mut expected_ordinal = 0u32;
        let mut previous_last: Option<&[u8]> = None;
        for entry in &self.entries {
            if entry.row_count == 0
                || usize::from(entry.row_count) > REPLACEMENT_PART_MAX_ROWS
                || entry.first_key.is_empty()
                || entry.last_key.is_empty()
                || entry.first_key > entry.last_key
                || previous_last.is_some_and(|previous| previous >= entry.first_key.as_ref())
                || entry.first_ordinal != expected_ordinal
            {
                return Err(replacement_part_error(
                    "replacement directory has invalid bounds or ordinals",
                ));
            }
            expected_ordinal = expected_ordinal
                .checked_add(u32::from(entry.row_count))
                .ok_or_else(|| replacement_part_error("replacement row count overflows u32"))?;
            previous_last = Some(&entry.last_key);
        }
        if expected_ordinal != self.row_count {
            return Err(replacement_part_error(
                "replacement directory row count does not match its parts",
            ));
        }
        Ok(())
    }
}

pub(crate) fn encode_replacement_part(
    rows: &[ReplacementPartRowRef<'_>],
) -> Result<EncodedReplacementPart, LixError> {
    encode_replacement_part_with_compressor(rows, &mut None)
}

pub(crate) fn encode_replacement_part_with_compressor(
    rows: &[ReplacementPartRowRef<'_>],
    compressor: &mut Option<crate::compression::ZstdLevel1Compressor>,
) -> Result<EncodedReplacementPart, LixError> {
    if rows.is_empty() || rows.len() > REPLACEMENT_PART_MAX_ROWS {
        return Err(replacement_part_error(format!(
            "replacement part row count must be in 1..={REPLACEMENT_PART_MAX_ROWS}"
        )));
    }
    if rows.iter().any(|row| {
        row.encoded_key.is_empty() || row.encoded_key.len() > REPLACEMENT_PART_TARGET_BYTES
    }) || rows
        .windows(2)
        .any(|pair| pair[0].encoded_key >= pair[1].encoded_key)
    {
        return Err(replacement_part_error(
            "replacement part keys must be non-empty, bounded, and strictly ordered",
        ));
    }
    let mut encoded =
        Vec::with_capacity(REPLACEMENT_PART_MAGIC.len() + 2 + rows.len().saturating_mul(16));
    encoded.extend_from_slice(REPLACEMENT_PART_MAGIC);
    encoded.extend_from_slice(
        &u16::try_from(rows.len())
            .expect("replacement part row count fits u16")
            .to_be_bytes(),
    );
    let mut previous_key = &[][..];
    for row in rows {
        let shared = previous_key
            .iter()
            .zip(row.encoded_key)
            .take_while(|(left, right)| left == right)
            .count();
        let suffix = &row.encoded_key[shared..];
        encoded.extend_from_slice(
            &u16::try_from(shared)
                .map_err(|_| replacement_part_error("replacement key prefix exceeds u16"))?
                .to_be_bytes(),
        );
        encoded.extend_from_slice(
            &u16::try_from(suffix.len())
                .map_err(|_| replacement_part_error("replacement key suffix exceeds u16"))?
                .to_be_bytes(),
        );
        encoded.extend_from_slice(suffix);
        encode_json_slot(&mut encoded, row.snapshot, true)?;
        encode_json_slot(&mut encoded, row.metadata, false)?;
        previous_key = row.encoded_key;
    }
    if encoded.len() > REPLACEMENT_PART_MAX_DECODED_BYTES {
        return Err(replacement_part_error(
            "replacement part exceeds its decoded byte bound",
        ));
    }
    if encoded.len() >= 1024 {
        if compressor.is_none() {
            *compressor = Some(crate::compression::ZstdLevel1Compressor::new().map_err(
                |error| replacement_part_error(format!("replacement compressor failed: {error}")),
            )?);
        }
        let compressed = compressor
            .as_mut()
            .expect("replacement compressor was initialized")
            .compress(&encoded)
            .map_err(|error| {
                replacement_part_error(format!("replacement part compression failed: {error}"))
            })?;
        let mut physical = Vec::with_capacity(12 + compressed.len());
        physical.extend_from_slice(REPLACEMENT_PART_COMPRESSED_MAGIC);
        physical.extend_from_slice(
            &u32::try_from(encoded.len())
                .expect("decoded replacement part is bounded below u32")
                .to_be_bytes(),
        );
        physical.extend_from_slice(&compressed);
        if physical.len() < encoded.len() {
            encoded = physical;
        }
    }
    if encoded.len() > REPLACEMENT_PART_MAX_BYTES {
        return Err(replacement_part_error(format!(
            "replacement part exceeds {REPLACEMENT_PART_MAX_BYTES} physical bytes"
        )));
    }
    let digest = domain_digest(REPLACEMENT_PART_DIGEST_CONTEXT, &encoded);
    Ok(EncodedReplacementPart {
        digest,
        bytes: Bytes::from(encoded),
        first_key: Bytes::copy_from_slice(
            rows.first()
                .expect("non-empty replacement rows have a first key")
                .encoded_key,
        ),
        last_key: Bytes::copy_from_slice(
            rows.last()
                .expect("non-empty replacement rows have a last key")
                .encoded_key,
        ),
        row_count: u16::try_from(rows.len()).expect("replacement part row count fits u16"),
    })
}

pub(crate) fn decode_replacement_part(
    expected_digest: &[u8; DIGEST_BYTES],
    encoded: &[u8],
) -> Result<DecodedReplacementPart, LixError> {
    if encoded.len() > REPLACEMENT_PART_MAX_BYTES {
        return Err(replacement_part_error(
            "replacement part exceeds its physical byte bound",
        ));
    }
    if &domain_digest(REPLACEMENT_PART_DIGEST_CONTEXT, encoded) != expected_digest {
        return Err(replacement_part_error(
            "replacement part content digest mismatch",
        ));
    }
    let logical: Cow<'_, [u8]> = if let Some(compressed) =
        encoded.strip_prefix(REPLACEMENT_PART_COMPRESSED_MAGIC)
    {
        let (uncompressed_len, compressed) = compressed
            .split_at_checked(4)
            .ok_or_else(|| replacement_part_error("compressed replacement part is truncated"))?;
        let uncompressed_len = usize::try_from(u32::from_be_bytes(
            uncompressed_len
                .try_into()
                .expect("four decoded-length bytes"),
        ))
        .expect("u32 fits usize");
        if uncompressed_len > REPLACEMENT_PART_MAX_DECODED_BYTES {
            return Err(replacement_part_error(
                "compressed replacement part exceeds its decoded byte bound",
            ));
        }
        let decoded =
            crate::compression::decompress_zstd(compressed, uncompressed_len).map_err(|error| {
                replacement_part_error(format!("replacement part decompression failed: {error}"))
            })?;
        Cow::Owned(decoded)
    } else {
        Cow::Borrowed(encoded)
    };
    let Some(body) = logical.strip_prefix(REPLACEMENT_PART_MAGIC) else {
        return Err(replacement_part_error("replacement part has invalid magic"));
    };
    let mut cursor = 0usize;
    let row_count = usize::from(decode_u16(body, &mut cursor)?);
    if row_count == 0 || row_count > REPLACEMENT_PART_MAX_ROWS {
        return Err(replacement_part_error(
            "replacement part has an invalid row count",
        ));
    }
    let mut key_arena = Vec::new();
    let mut key_ranges = Vec::with_capacity(row_count);
    let mut snapshots = Vec::with_capacity(row_count);
    let mut metadata = Vec::with_capacity(row_count);
    let mut previous_key = Vec::new();
    for _ in 0..row_count {
        let shared = usize::from(decode_u16(body, &mut cursor)?);
        let suffix_len = usize::from(decode_u16(body, &mut cursor)?);
        if shared > previous_key.len() {
            return Err(replacement_part_error(
                "replacement part key prefix exceeds the previous key",
            ));
        }
        let suffix = take_exact(body, &mut cursor, suffix_len)?;
        let mut key = Vec::with_capacity(shared + suffix_len);
        key.extend_from_slice(&previous_key[..shared]);
        key.extend_from_slice(suffix);
        if key.is_empty() || (!previous_key.is_empty() && previous_key >= key) {
            return Err(replacement_part_error(
                "replacement part keys are not strictly ordered",
            ));
        }
        let start = key_arena.len();
        key_arena.extend_from_slice(&key);
        key_ranges.push(start..key_arena.len());
        snapshots.push(decode_json_slot(body, &mut cursor, true)?);
        metadata.push(decode_json_slot(body, &mut cursor, false)?);
        previous_key = key;
    }
    if cursor != body.len() {
        return Err(replacement_part_error(
            "replacement part has trailing bytes",
        ));
    }
    Ok(DecodedReplacementPart {
        key_arena: Bytes::from(key_arena),
        key_ranges,
        snapshots,
        metadata,
    })
}

pub(crate) fn decode_replacement_part_for_entry(
    entry: &ReplacementPartDirectoryEntry,
    encoded: &[u8],
) -> Result<DecodedReplacementPart, LixError> {
    let decoded = decode_replacement_part(entry.digest(), encoded)?;
    if decoded.len() != usize::from(entry.row_count)
        || decoded.first_key() != Some(entry.first_key())
        || decoded.last_key() != Some(entry.last_key())
    {
        return Err(replacement_part_error(
            "replacement part does not match its directory entry",
        ));
    }
    Ok(decoded)
}

fn domain_digest(context: &str, encoded: &[u8]) -> [u8; DIGEST_BYTES] {
    let mut hasher = blake3::Hasher::new_derive_key(context);
    hasher.update(encoded);
    *hasher.finalize().as_bytes()
}

fn encode_sized_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), LixError> {
    let len = u32::try_from(bytes.len())
        .map_err(|_| replacement_part_error("replacement directory key exceeds u32"))?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn encode_u32_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), LixError> {
    let len = u32::try_from(bytes.len())
        .map_err(|_| replacement_part_error("replacement payload exceeds u32"))?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn encode_json_slot(
    out: &mut Vec<u8>,
    slot: crate::json_store::JsonSlotRef<'_>,
    required: bool,
) -> Result<(), LixError> {
    match slot {
        crate::json_store::JsonSlotRef::None if required => Err(replacement_part_error(
            "replacement snapshot payload is missing",
        )),
        crate::json_store::JsonSlotRef::None => {
            out.push(0);
            Ok(())
        }
        crate::json_store::JsonSlotRef::Ref(json_ref) => {
            out.push(1);
            out.extend_from_slice(json_ref.as_hash_bytes());
            Ok(())
        }
        crate::json_store::JsonSlotRef::Inline(json) => {
            out.push(2);
            encode_u32_bytes(out, json.as_bytes())
        }
    }
}

fn decode_json_slot(
    encoded: &[u8],
    cursor: &mut usize,
    required: bool,
) -> Result<crate::json_store::JsonSlot, LixError> {
    let tag = *take_exact(encoded, cursor, 1)?
        .first()
        .expect("one tag byte");
    match tag {
        0 if required => Err(replacement_part_error(
            "replacement snapshot payload is missing",
        )),
        0 => Ok(crate::json_store::JsonSlot::None),
        1 => Ok(crate::json_store::JsonSlot::Ref(
            crate::json_store::JsonRef::from_hash_bytes(
                take_exact(encoded, cursor, 32)?
                    .try_into()
                    .expect("JSON reference width checked"),
            ),
        )),
        2 => {
            let bytes = decode_u32_bytes(encoded, cursor)?;
            let json = std::str::from_utf8(bytes)
                .map_err(|_| replacement_part_error("replacement inline JSON is not UTF-8"))?;
            Ok(crate::json_store::JsonSlot::Inline(json.to_owned().into()))
        }
        _ => Err(replacement_part_error(
            "replacement JSON slot has an invalid tag",
        )),
    }
}

fn decode_u32_bytes<'a>(encoded: &'a [u8], cursor: &mut usize) -> Result<&'a [u8], LixError> {
    let len = usize::try_from(decode_u32(encoded, cursor)?).expect("u32 length fits usize");
    take_exact(encoded, cursor, len)
}

fn decode_sized_bytes<'a>(encoded: &'a [u8], cursor: &mut usize) -> Result<&'a [u8], LixError> {
    let len = usize::try_from(decode_u32(encoded, cursor)?).expect("u32 key length fits usize");
    if len == 0 || len > REPLACEMENT_PART_TARGET_BYTES {
        return Err(replacement_part_error(
            "replacement directory key has invalid length",
        ));
    }
    take_exact(encoded, cursor, len)
}

fn decode_u16(encoded: &[u8], cursor: &mut usize) -> Result<u16, LixError> {
    Ok(u16::from_be_bytes(
        take_exact(encoded, cursor, 2)?
            .try_into()
            .expect("u16 width checked"),
    ))
}

fn decode_u32(encoded: &[u8], cursor: &mut usize) -> Result<u32, LixError> {
    Ok(u32::from_be_bytes(
        take_exact(encoded, cursor, 4)?
            .try_into()
            .expect("u32 width checked"),
    ))
}

fn take_exact<'a>(encoded: &'a [u8], cursor: &mut usize, len: usize) -> Result<&'a [u8], LixError> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| replacement_part_error("replacement codec offset overflows"))?;
    let bytes = encoded
        .get(*cursor..end)
        .ok_or_else(|| replacement_part_error("replacement codec value is truncated"))?;
    *cursor = end;
    Ok(bytes)
}

fn replacement_part_error(message: impl Into<String>) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("tracked_state replacement part: {}", message.into()),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        ReplacementPartDirectory, ReplacementPartRowRef, decode_replacement_part,
        decode_replacement_part_for_entry, encode_replacement_part,
    };

    fn rows<'a>(keys: &'a [&'a [u8]]) -> Vec<ReplacementPartRowRef<'a>> {
        keys.iter()
            .map(|key| ReplacementPartRowRef {
                encoded_key: key,
                snapshot: crate::json_store::JsonSlotRef::Inline("{}"),
                metadata: crate::json_store::JsonSlotRef::None,
            })
            .collect()
    }

    #[test]
    fn part_round_trips_exact_points_and_rejects_wrong_digest() {
        let encoded = encode_replacement_part(&rows(&[b"alpha", b"beta", b"gamma"]))
            .expect("encode replacement part");
        let decoded = decode_replacement_part(encoded.digest(), encoded.bytes())
            .expect("decode replacement part");
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded.first_key(), Some(b"alpha".as_slice()));
        assert_eq!(decoded.last_key(), Some(b"gamma".as_slice()));
        assert_eq!(
            decoded.find(b"beta").expect("point lookup"),
            Some(super::ReplacementPartMatch { ordinal: 1 })
        );
        assert_eq!(decoded.find(b"delta").expect("missing point"), None);

        let mut wrong_digest = *encoded.digest();
        wrong_digest[0] ^= 1;
        assert!(decode_replacement_part(&wrong_digest, encoded.bytes()).is_err());
    }

    #[test]
    fn part_rejects_unsorted_rows_and_physical_corruption() {
        assert!(encode_replacement_part(&rows(&[b"beta", b"alpha"])).is_err());
        let encoded =
            encode_replacement_part(&rows(&[b"alpha", b"beta"])).expect("encode replacement part");
        let mut corrupt = encoded.bytes().to_vec();
        let last = corrupt.last_mut().expect("encoded part is non-empty");
        *last ^= 1;
        assert!(decode_replacement_part(encoded.digest(), &corrupt).is_err());
    }

    #[test]
    fn part_preserves_content_references_without_inlining_payloads() {
        let snapshot_ref = crate::json_store::JsonRef::for_content(&vec![b'x'; 8 * 1024 * 1024]);
        let metadata_ref = crate::json_store::JsonRef::for_content(&vec![b'y'; 2 * 1024 * 1024]);
        let rows = [ReplacementPartRowRef {
            encoded_key: b"alpha",
            snapshot: crate::json_store::JsonSlotRef::Ref(&snapshot_ref),
            metadata: crate::json_store::JsonSlotRef::Ref(&metadata_ref),
        }];
        let encoded = encode_replacement_part(&rows).expect("encode referenced replacement part");
        assert!(encoded.bytes().len() < 256);
        let decoded = decode_replacement_part(encoded.digest(), encoded.bytes())
            .expect("decode referenced replacement part");
        assert_eq!(
            decoded.snapshot(0).expect("snapshot slot"),
            Some(crate::json_store::JsonSlotRef::Ref(&snapshot_ref))
        );
        assert_eq!(
            decoded.metadata(0).expect("metadata slot"),
            Some(crate::json_store::JsonSlotRef::Ref(&metadata_ref))
        );
    }

    #[test]
    fn directory_round_trips_and_routes_points_ordinals_and_ranges() {
        let first =
            encode_replacement_part(&rows(&[b"alpha", b"beta"])).expect("encode first part");
        let second =
            encode_replacement_part(&rows(&[b"delta", b"omega"])).expect("encode second part");
        let directory = ReplacementPartDirectory::try_new(
            vec![first.directory_entry(0), second.directory_entry(2)],
            4,
        )
        .expect("build directory");
        let encoded = directory.encode().expect("encode directory");
        let decoded = ReplacementPartDirectory::decode(&encoded).expect("decode directory");
        assert_eq!(decoded, directory);
        assert_eq!(
            decoded.digest().expect("directory digest"),
            directory.digest().unwrap()
        );
        assert_eq!(
            decoded.route_key(b"beta").expect("beta route").digest(),
            first.digest()
        );
        assert!(decoded.route_key(b"charlie").is_none());
        let ordinal = decoded.route_ordinal(3).expect("ordinal route");
        assert_eq!(ordinal.entry.digest(), second.digest());
        assert_eq!(ordinal.local_ordinal, 1);
        assert!(decoded.route_ordinal(4).is_none());
        assert_eq!(
            decoded.parts_overlapping(Some(b"beta"), Some(b"omega")),
            0..2
        );
        assert_eq!(
            decoded.parts_overlapping(Some(b"charlie"), Some(b"delta")),
            1..1
        );
        assert_eq!(decoded.parts_overlapping(Some(b"delta"), None), 1..2);
    }

    #[test]
    fn directory_rejects_gaps_overlaps_and_tampering() {
        let first =
            encode_replacement_part(&rows(&[b"alpha", b"beta"])).expect("encode first part");
        let overlapping =
            encode_replacement_part(&rows(&[b"beta", b"delta"])).expect("encode overlapping part");
        assert!(
            ReplacementPartDirectory::try_new(
                vec![first.directory_entry(0), overlapping.directory_entry(2)],
                4,
            )
            .is_err()
        );

        let second =
            encode_replacement_part(&rows(&[b"delta", b"omega"])).expect("encode second part");
        assert!(
            ReplacementPartDirectory::try_new(
                vec![first.directory_entry(0), second.directory_entry(3)],
                4,
            )
            .is_err()
        );
        let directory = ReplacementPartDirectory::try_new(
            vec![first.directory_entry(0), second.directory_entry(2)],
            4,
        )
        .expect("build directory");
        let mut encoded = directory.encode().expect("encode directory").to_vec();
        encoded.push(0);
        assert!(ReplacementPartDirectory::decode(&encoded).is_err());

        let encoded = directory.encode().expect("encode directory");
        let mut wrong_digest = directory.digest().expect("directory digest");
        wrong_digest[0] ^= 1;
        assert!(
            ReplacementPartDirectory::decode_content_addressed(&wrong_digest, &encoded).is_err()
        );

        let mut wrong_bounds = first.directory_entry(0);
        wrong_bounds.last_key = b"charlie".as_slice().into();
        assert!(decode_replacement_part_for_entry(&wrong_bounds, first.bytes()).is_err());
    }
}
