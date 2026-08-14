use std::collections::HashMap;
use std::mem::size_of;
use std::ops::Range;

use bytes::{Bytes, BytesMut};

use super::{Key, PutBatch, PutEntry, StorageError, StoredValue};

pub const IMMUTABLE_SEGMENT_MAX_BYTES: usize = 64 * 1024 * 1024;
pub const IMMUTABLE_VALUE_MAX_BYTES: usize = 4 * 1024 * 1024 + 1024;

const IMMUTABLE_VALUE_MAGIC: &[u8; 8] = b"LIXIVS1\0";
const IMMUTABLE_LOCATOR_MAGIC: &[u8; 8] = b"LIXIVL2\0";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImmutableValueLocator {
    pub segment_id: Key,
    pub segment_len: usize,
    pub range: Range<usize>,
}

#[derive(Clone, Debug)]
pub struct ImmutableSegment {
    pub id: Key,
    pub frames: Vec<Bytes>,
    pub values: Vec<(Key, Range<usize>)>,
}

#[derive(Debug, Default)]
pub struct ImmutableSegmentWriter {
    positions: HashMap<Key, usize>,
    values: Vec<(Key, Bytes)>,
}

impl ImmutableSegmentWriter {
    pub fn insert(&mut self, key: Key, value: Bytes) -> Result<(), StorageError> {
        if let Some(position) = self.positions.get(&key) {
            let existing = &self.values[*position].1;
            if existing != &value {
                return Err(StorageError::Corruption(
                    "immutable identity was assigned different bytes".to_string(),
                ));
            }
            return Ok(());
        }
        if value.len() > IMMUTABLE_VALUE_MAX_BYTES {
            return Err(StorageError::Io(format!(
                "immutable value exceeds the {IMMUTABLE_VALUE_MAX_BYTES} byte format maximum"
            )));
        }
        self.positions.insert(key.clone(), self.values.len());
        self.values.push((key, value));
        Ok(())
    }

    pub fn insert_batch(&mut self, batch: PutBatch) -> Result<(), StorageError> {
        for PutEntry {
            key,
            value: StoredValue { bytes },
        } in batch.entries
        {
            self.insert(key, bytes)?;
        }
        Ok(())
    }

    pub fn finish(
        self,
        mut retain: impl FnMut(&Key) -> bool,
    ) -> Result<Vec<ImmutableSegment>, StorageError> {
        plan_immutable_segments(
            self.values
                .into_iter()
                .filter(|(key, _)| retain(key))
                .collect(),
            IMMUTABLE_SEGMENT_MAX_BYTES,
        )
    }
}

pub fn validate_immutable_batch(batch: &PutBatch) -> Result<(), StorageError> {
    let mut previous: Option<(&Key, &Bytes)> = None;
    let mut entries = batch.entries.iter().collect::<Vec<_>>();
    entries.sort_unstable_by(|left, right| left.key.cmp(&right.key));
    for entry in entries {
        if let Some((previous_key, previous_value)) = previous
            && previous_key == &entry.key
        {
            if previous_value != &entry.value.bytes {
                return Err(StorageError::Corruption(
                    "immutable identity was assigned different bytes".to_string(),
                ));
            }
            continue;
        }
        if entry.value.bytes.len() > IMMUTABLE_VALUE_MAX_BYTES {
            return Err(StorageError::Io(format!(
                "immutable value exceeds the {IMMUTABLE_VALUE_MAX_BYTES} byte format maximum"
            )));
        }
        previous = Some((&entry.key, &entry.value.bytes));
    }
    Ok(())
}

pub fn encode_immutable_locator(locator: &ImmutableValueLocator) -> Result<Bytes, StorageError> {
    let segment_id: [u8; 32] = locator
        .segment_id
        .0
        .as_ref()
        .try_into()
        .map_err(|_| StorageError::InvalidKey)?;
    let offset = u64::try_from(locator.range.start)
        .map_err(|_| StorageError::Io("immutable segment offset exceeds u64".to_string()))?;
    let segment_len = u64::try_from(locator.segment_len)
        .map_err(|_| StorageError::Io("immutable segment length exceeds u64".to_string()))?;
    let length = u32::try_from(locator.range.len())
        .map_err(|_| StorageError::Io("immutable value exceeds u32".to_string()))?;
    if locator.range.end > locator.segment_len {
        return Err(StorageError::Io(
            "immutable value range exceeds its segment".to_string(),
        ));
    }
    let mut encoded = Vec::with_capacity(IMMUTABLE_LOCATOR_MAGIC.len() + 32 + 8 + 8 + 4);
    encoded.extend_from_slice(IMMUTABLE_LOCATOR_MAGIC);
    encoded.extend_from_slice(&segment_id);
    encoded.extend_from_slice(&segment_len.to_le_bytes());
    encoded.extend_from_slice(&offset.to_le_bytes());
    encoded.extend_from_slice(&length.to_le_bytes());
    Ok(Bytes::from(encoded))
}

pub fn decode_immutable_locator(encoded: &[u8]) -> Result<ImmutableValueLocator, StorageError> {
    const HASH_BYTES: usize = 32;
    const SEGMENT_LENGTH_BYTES: usize = size_of::<u64>();
    const OFFSET_BYTES: usize = size_of::<u64>();
    const LENGTH_BYTES: usize = size_of::<u32>();
    let expected_len = IMMUTABLE_LOCATOR_MAGIC.len()
        + HASH_BYTES
        + SEGMENT_LENGTH_BYTES
        + OFFSET_BYTES
        + LENGTH_BYTES;
    if encoded.len() != expected_len || !encoded.starts_with(IMMUTABLE_LOCATOR_MAGIC) {
        return Err(StorageError::Corruption(
            "immutable segment locator is invalid".to_string(),
        ));
    }
    let hash_start = IMMUTABLE_LOCATOR_MAGIC.len();
    let segment_length_start = hash_start + HASH_BYTES;
    let offset_start = segment_length_start + SEGMENT_LENGTH_BYTES;
    let length_start = offset_start + OFFSET_BYTES;
    let segment_len = usize::try_from(u64::from_le_bytes(
        encoded[segment_length_start..offset_start]
            .try_into()
            .expect("fixed immutable locator segment length"),
    ))
    .map_err(|_| StorageError::Corruption("immutable segment length exceeds usize".to_string()))?;
    let offset = usize::try_from(u64::from_le_bytes(
        encoded[offset_start..length_start]
            .try_into()
            .expect("fixed immutable locator offset"),
    ))
    .map_err(|_| StorageError::Corruption("immutable segment offset exceeds usize".to_string()))?;
    let length = u32::from_le_bytes(
        encoded[length_start..]
            .try_into()
            .expect("fixed immutable locator length"),
    ) as usize;
    let end = offset.checked_add(length).ok_or_else(|| {
        StorageError::Corruption("immutable segment range overflows usize".to_string())
    })?;
    if end > segment_len {
        return Err(StorageError::Corruption(
            "immutable value range exceeds its segment".to_string(),
        ));
    }
    Ok(ImmutableValueLocator {
        segment_id: Key(Bytes::copy_from_slice(
            &encoded[hash_start..segment_length_start],
        )),
        segment_len,
        range: offset..end,
    })
}

pub fn decode_immutable_value(encoded: Bytes) -> Result<Bytes, StorageError> {
    let header_len = IMMUTABLE_VALUE_MAGIC.len() + size_of::<u64>();
    if encoded.len() < header_len || !encoded.starts_with(IMMUTABLE_VALUE_MAGIC) {
        return Err(StorageError::Corruption(
            "immutable value envelope is invalid".to_string(),
        ));
    }
    let value_len = usize::try_from(u64::from_le_bytes(
        encoded[IMMUTABLE_VALUE_MAGIC.len()..header_len]
            .try_into()
            .expect("fixed immutable value length"),
    ))
    .map_err(|_| StorageError::Corruption("immutable value length exceeds usize".to_string()))?;
    let value = encoded.slice(header_len..);
    if value_len > IMMUTABLE_VALUE_MAX_BYTES || value.len() != value_len {
        return Err(StorageError::Corruption(
            "immutable value envelope length is invalid".to_string(),
        ));
    }
    Ok(value)
}

fn plan_immutable_segments(
    values: Vec<(Key, Bytes)>,
    max_segment_bytes: usize,
) -> Result<Vec<ImmutableSegment>, StorageError> {
    let mut segments = Vec::new();
    let mut current = SegmentBuilder::default();
    for (key, value) in values {
        let encoded_len = IMMUTABLE_VALUE_MAGIC
            .len()
            .saturating_add(size_of::<u64>())
            .saturating_add(value.len());
        if !current.values.is_empty()
            && current.bytes.saturating_add(encoded_len) > max_segment_bytes
        {
            segments.push(current.finish());
            current = SegmentBuilder::default();
        }
        current.push(key, value)?;
    }
    if !current.values.is_empty() {
        segments.push(current.finish());
    }
    Ok(segments)
}

#[derive(Default)]
struct SegmentBuilder {
    frames: Vec<Bytes>,
    values: Vec<(Key, Range<usize>)>,
    identity: blake3::Hasher,
    bytes: usize,
}

impl SegmentBuilder {
    fn push(&mut self, key: Key, value: Bytes) -> Result<(), StorageError> {
        let value_len = u64::try_from(value.len())
            .map_err(|_| StorageError::Io("immutable value exceeds u64".to_string()))?;
        let key_len = u64::try_from(key.0.len()).map_err(|_| StorageError::InvalidKey)?;
        if self.values.is_empty() {
            self.identity = blake3::Hasher::new_derive_key("lix immutable segment identity v1");
        }
        self.identity.update(&key_len.to_le_bytes());
        self.identity.update(&key.0);
        self.identity.update(&value_len.to_le_bytes());
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_immutable_segment_identity_hash_bytes(
            size_of::<u64>() + key.0.len() + size_of::<u64>(),
        );
        let mut header = BytesMut::with_capacity(IMMUTABLE_VALUE_MAGIC.len() + size_of::<u64>());
        header.extend_from_slice(IMMUTABLE_VALUE_MAGIC);
        header.extend_from_slice(&value_len.to_le_bytes());
        let start = self.bytes;
        self.bytes = self
            .bytes
            .checked_add(header.len())
            .and_then(|bytes| bytes.checked_add(value.len()))
            .ok_or_else(|| StorageError::Io("immutable segment size exceeds usize".to_string()))?;
        self.frames.push(header.freeze());
        self.frames.push(value);
        self.values.push((key, start..self.bytes));
        Ok(())
    }

    fn finish(self) -> ImmutableSegment {
        ImmutableSegment {
            id: Key(Bytes::copy_from_slice(self.identity.finalize().as_bytes())),
            frames: self.frames,
            values: self.values,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_identity_depends_on_ordered_identitys_and_lengths() {
        let mut left = ImmutableSegmentWriter::default();
        left.insert(Key(Bytes::from_static(b"a")), Bytes::from_static(b"AA"))
            .expect("insert a");
        left.insert(Key(Bytes::from_static(b"b")), Bytes::from_static(b"BBB"))
            .expect("insert b");
        let left = left.finish(|_| true).expect("finish left");

        let mut same_shape = ImmutableSegmentWriter::default();
        same_shape
            .insert(Key(Bytes::from_static(b"a")), Bytes::from_static(b"XX"))
            .expect("insert a");
        same_shape
            .insert(Key(Bytes::from_static(b"b")), Bytes::from_static(b"YYY"))
            .expect("insert b");
        let same_shape = same_shape.finish(|_| true).expect("finish same shape");
        assert_eq!(left[0].id, same_shape[0].id);

        let mut changed_length = ImmutableSegmentWriter::default();
        changed_length
            .insert(Key(Bytes::from_static(b"a")), Bytes::from_static(b"A"))
            .expect("insert changed a");
        let changed_length = changed_length
            .finish(|_| true)
            .expect("finish changed length");
        assert_ne!(left[0].id, changed_length[0].id);
    }

    #[test]
    fn immutable_identity_cannot_be_reassigned() {
        let mut writer = ImmutableSegmentWriter::default();
        let key = Key(Bytes::from_static(b"identity"));
        writer
            .insert(key.clone(), Bytes::from_static(b"first"))
            .expect("insert first");
        let error = writer
            .insert(key, Bytes::from_static(b"other"))
            .expect_err("different bytes corrupt one identity");
        assert!(matches!(error, StorageError::Corruption(_)));
    }

    #[test]
    fn locator_and_envelope_roundtrip() {
        let mut writer = ImmutableSegmentWriter::default();
        writer
            .insert(
                Key(Bytes::from_static(b"a")),
                Bytes::from_static(b"payload"),
            )
            .expect("insert");
        let segment = writer.finish(|_| true).expect("finish").remove(0);
        let locator = ImmutableValueLocator {
            segment_id: segment.id,
            segment_len: segment.values.last().expect("segment value").1.end,
            range: segment.values[0].1.clone(),
        };
        let encoded = encode_immutable_locator(&locator).expect("encode locator");
        assert_eq!(
            decode_immutable_locator(&encoded).expect("decode locator"),
            locator
        );
        let payload = segment
            .frames
            .into_iter()
            .fold(BytesMut::new(), |mut out, frame| {
                out.extend_from_slice(&frame);
                out
            });
        assert_eq!(
            decode_immutable_value(payload.freeze()).expect("decode value"),
            Bytes::from_static(b"payload")
        );
    }
}
