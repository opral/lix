use crate::{ChangeEffect, EntityChange, EntityRecord, InitialChanges};
use std::collections::VecDeque;
use std::sync::Arc;

pub const FORMAT_VERSION: u16 = 1;

#[derive(Clone, Debug)]
pub struct ChangePage {
    pub record_count: u32,
    pub payload: Vec<u8>,
    pub attachments: Vec<Arc<Vec<u8>>>,
}

/// A lazy snapshot value from one side of a three-way entity conflict.
///
/// The host keeps large values in the packet page's attachment table.  A
/// resolver which decides to take one side never reads that attachment through
/// guest linear memory.  CSV only materializes all three values for the small,
/// structured row merge fast path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConflictSnapshot {
    Inline(Vec<u8>),
    Attachment {
        index: u32,
        offset: u64,
        length: u64,
    },
}

impl ConflictSnapshot {
    pub fn len(&self) -> u64 {
        match self {
            Self::Inline(bytes) => u64::try_from(bytes.len()).expect("usize fits u64"),
            Self::Attachment { length, .. } => *length,
        }
    }

    pub fn materialize(
        &self,
        attachment: &mut impl FnMut(u32, u64, u64) -> Result<Vec<u8>, String>,
    ) -> Result<Vec<u8>, String> {
        match self {
            Self::Inline(bytes) => Ok(bytes.clone()),
            Self::Attachment {
                index,
                offset,
                length,
            } => attachment(*index, *offset, *length),
        }
    }
}

/// One same-key, canonically ordered three-way conflict delivered to a static
/// conflict resolver.  `a` and `b` are selected by the engine's stable
/// `(updated_at, change_id)` order, never by the caller's merge direction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConflictRecord {
    pub schema_key: String,
    pub entity_pk: Vec<String>,
    /// The host-assigned position in this conflict transition. The resolver
    /// echoes it in every result so the host can prove alignment even across
    /// independently paged input and output streams.
    pub ordinal: u32,
    pub base: Option<ConflictSnapshot>,
    pub a: Option<ConflictSnapshot>,
    pub b: Option<ConflictSnapshot>,
}

/// An aligned result for one [`ConflictRecord`].  Take variants are deliberately
/// snapshot-free: they let the host retain a large existing semantic value
/// without round-tripping it through the Wasm guest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConflictResolution {
    // CSV's current resolver does not select the base, but the shared wire
    // contract permits it and the decoder/encoder must keep that tag stable.
    #[allow(dead_code)]
    TakeBase,
    TakeA,
    TakeB,
    Replace(Vec<u8>),
    Delete,
}

#[derive(Clone, Debug)]
pub struct ResolutionPage {
    pub record_count: u32,
    pub payload: Vec<u8>,
    pub attachments: Vec<Arc<Vec<u8>>>,
}

/// Bounded cursor state for static conflict-resolution output.
#[derive(Clone, Debug, Default)]
pub struct ResolutionStream {
    resolutions: VecDeque<(u32, ConflictResolution)>,
}

impl ResolutionStream {
    pub fn extend(&mut self, resolutions: impl IntoIterator<Item = (u32, ConflictResolution)>) {
        self.resolutions.extend(resolutions);
    }

    pub fn next_page(
        &mut self,
        max_bytes: u32,
        max_record_bytes: u32,
    ) -> Result<Option<ResolutionPage>, String> {
        if max_bytes == 0 {
            return Err("resolution cursor max-bytes must be positive".to_owned());
        }
        if max_record_bytes == 0 {
            return Err("resolution cursor max-record-bytes must be positive".to_owned());
        }
        let limit = usize::try_from(max_bytes).expect("u32 fits usize");
        let record_limit = usize::try_from(max_record_bytes).expect("u32 fits usize");
        let mut payload = Vec::with_capacity(limit.min(64 * 1024));
        let mut attachments = Vec::new();
        let mut count = 0u32;

        while let Some((ordinal, resolution)) = self.resolutions.front() {
            let snapshot = match resolution {
                ConflictResolution::Replace(snapshot) => Some(snapshot),
                ConflictResolution::TakeBase
                | ConflictResolution::TakeA
                | ConflictResolution::TakeB
                | ConflictResolution::Delete => None,
            };
            let definitely_needs_attachment = snapshot.is_some_and(|snapshot| {
                // `replace` is tag + ordinal + effect + inline blob tag +
                // length, followed by the frame prefix outside the record.
                snapshot.len().checked_add(11).is_none_or(|record_len| {
                    record_len > record_limit
                        || record_len
                            .checked_add(4)
                            .is_none_or(|framed_len| framed_len > limit)
                })
            });
            let inline_record = if definitely_needs_attachment {
                None
            } else {
                Some(encode_resolution(*ordinal, resolution, None)?)
            };
            let inline_framed_len = inline_record
                .as_ref()
                .map(|record| {
                    record
                        .len()
                        .checked_add(4)
                        .ok_or_else(|| "resolution record length overflow".to_owned())
                })
                .transpose()?;
            let inline_fits = inline_record.as_ref().zip(inline_framed_len).is_some_and(
                |(record, framed_len)| record.len() <= record_limit && framed_len <= limit,
            );

            let (record, attach_snapshot) = if inline_fits {
                let record = inline_record.expect("inline record checked before selection");
                let framed_len = inline_framed_len.expect("inline frame checked before selection");
                if payload
                    .len()
                    .checked_add(framed_len)
                    .is_none_or(|next_len| next_len > limit)
                {
                    break;
                }
                (record, false)
            } else if snapshot.is_some() {
                let attachment_index = u32::try_from(attachments.len())
                    .map_err(|_| "resolution page has too many attachments".to_owned())?;
                let record = encode_resolution(*ordinal, resolution, Some(attachment_index))?;
                let framed_len = 4usize
                    .checked_add(record.len())
                    .ok_or_else(|| "resolution record length overflow".to_owned())?;
                if record.len() > record_limit {
                    return Err(format!(
                        "resolution record metadata requires {} bytes, record cap is {record_limit}",
                        record.len()
                    ));
                }
                if framed_len > limit {
                    return Err(format!(
                        "resolution record metadata requires {framed_len} bytes, page cap is {limit}"
                    ));
                }
                if payload
                    .len()
                    .checked_add(framed_len)
                    .is_none_or(|next_len| next_len > limit)
                {
                    break;
                }
                (record, true)
            } else {
                let record_len = inline_record
                    .as_ref()
                    .map(Vec::len)
                    .expect("snapshot-free resolutions always encode inline");
                let framed_len =
                    inline_framed_len.expect("snapshot-free resolutions always have a frame");
                if record_len > record_limit {
                    return Err(format!(
                        "resolution record requires {record_len} bytes, record cap is {record_limit}"
                    ));
                }
                return Err(format!(
                    "resolution record requires {framed_len} framed bytes, page cap is {limit}"
                ));
            };

            put_u32(
                &mut payload,
                u32::try_from(record.len()).map_err(|_| "resolution record exceeds 4GiB")?,
            );
            payload.extend_from_slice(&record);
            let (_, resolution) = self
                .resolutions
                .pop_front()
                .expect("front resolution was checked before selection");
            if attach_snapshot {
                let ConflictResolution::Replace(snapshot) = resolution else {
                    unreachable!("only replacement resolutions have attachments");
                };
                attachments.push(Arc::new(snapshot));
            }
            count = count
                .checked_add(1)
                .ok_or_else(|| "resolution record count overflow".to_owned())?;
        }

        if count == 0 {
            Ok(None)
        } else {
            Ok(Some(ResolutionPage {
                record_count: count,
                payload,
                attachments,
            }))
        }
    }
}

#[derive(Clone, Debug)]
pub enum ChangeStream {
    Initial(InitialChanges),
    Ready(VecDeque<EntityChange>),
    Eof,
}

impl ChangeStream {
    pub fn ready(changes: Vec<EntityChange>) -> Self {
        Self::Ready(changes.into())
    }

    fn take_change(&mut self) -> Result<Option<EntityChange>, String> {
        match self {
            Self::Initial(changes) => changes.next().transpose(),
            Self::Ready(changes) => Ok(changes.pop_front()),
            Self::Eof => Ok(None),
        }
    }

    pub fn next_page(
        &mut self,
        pending: &mut Option<EntityChange>,
        max_bytes: u32,
        max_record_bytes: u32,
    ) -> Result<Option<ChangePage>, String> {
        if max_bytes == 0 {
            return Err("change cursor max-bytes must be positive".to_owned());
        }
        if max_record_bytes == 0 {
            return Err("change cursor max-record-bytes must be positive".to_owned());
        }
        let limit = usize::try_from(max_bytes).expect("u32 fits usize");
        let record_limit = usize::try_from(max_record_bytes).expect("u32 fits usize");
        let mut payload = Vec::with_capacity(limit.min(64 * 1024));
        let mut attachments = Vec::new();
        let mut count = 0u32;
        loop {
            let mut change = match pending.take() {
                Some(change) => Some(change),
                None => self.take_change()?,
            };
            let Some(mut change) = change.take() else {
                *self = Self::Eof;
                break;
            };
            let definitely_needs_attachment = change.snapshot.as_ref().is_some_and(|snapshot| {
                snapshot.len() > record_limit
                    || snapshot
                        .len()
                        .checked_add(4)
                        .is_none_or(|framed_len| framed_len > limit)
            });
            let inline_record = if definitely_needs_attachment {
                None
            } else {
                Some(encode_change(&change, None)?)
            };
            let inline_record_len = inline_record.as_ref().map(Vec::len);
            let inline_framed_len = inline_record
                .as_ref()
                .map(|record| {
                    record
                        .len()
                        .checked_add(4)
                        .ok_or_else(|| "change record length overflow".to_owned())
                })
                .transpose()?;
            let inline_fits = inline_record.as_ref().zip(inline_framed_len).is_some_and(
                |(record, framed_len)| record.len() <= record_limit && framed_len <= limit,
            );
            let (record, attach_snapshot) = if inline_fits {
                let inline_record =
                    inline_record.expect("an inline record was checked before selection");
                let inline_framed_len =
                    inline_framed_len.expect("an inline frame was checked before selection");
                if payload
                    .len()
                    .checked_add(inline_framed_len)
                    .is_none_or(|next_len| next_len > limit)
                {
                    *pending = Some(change);
                    break;
                }
                (inline_record, false)
            } else if change.snapshot.is_some() {
                let attachment_index = u32::try_from(attachments.len())
                    .map_err(|_| "change page has too many attachments".to_owned())?;
                let record = encode_change(&change, Some(attachment_index))?;
                let framed_len = 4usize
                    .checked_add(record.len())
                    .ok_or_else(|| "change record length overflow".to_owned())?;
                if record.len() > record_limit {
                    return Err(format!(
                        "change record metadata requires {} bytes, record cap is {record_limit}",
                        record.len()
                    ));
                }
                if framed_len > limit {
                    return Err(format!(
                        "change record metadata requires {framed_len} bytes, page cap is {limit}"
                    ));
                }
                if payload
                    .len()
                    .checked_add(framed_len)
                    .is_none_or(|next_len| next_len > limit)
                {
                    *pending = Some(change);
                    break;
                }
                (record, true)
            } else {
                return Err(format!(
                    "change record requires {} bytes, record cap is {record_limit}, framed page cap is {limit}",
                    inline_record_len
                        .expect("snapshot-free changes always have an inline encoding")
                ));
            };
            put_u32(
                &mut payload,
                u32::try_from(record.len()).map_err(|_| "change record exceeds 4GiB")?,
            );
            payload.extend_from_slice(&record);
            if attach_snapshot {
                attachments.push(Arc::new(
                    change
                        .snapshot
                        .take()
                        .expect("attachment record has a snapshot"),
                ));
            }
            count += 1;
        }
        if count == 0 {
            Ok(None)
        } else {
            Ok(Some(ChangePage {
                record_count: count,
                payload,
                attachments,
            }))
        }
    }
}

fn encode_change(change: &EntityChange, attachment_index: Option<u32>) -> Result<Vec<u8>, String> {
    let mut output = Vec::new();
    output.push(u8::from(change.snapshot.is_none()));
    encode_key(&mut output, &change.schema_key, &change.entity_pk)?;
    if let Some(snapshot) = &change.snapshot {
        output.push(match change.effect {
            ChangeEffect::Content => 0,
            ChangeEffect::FormatOnly => 1,
        });
        if let Some(index) = attachment_index {
            output.push(1);
            put_u32(&mut output, index);
            output.extend_from_slice(&0u64.to_le_bytes());
            output.extend_from_slice(
                &u64::try_from(snapshot.len())
                    .expect("usize fits u64")
                    .to_le_bytes(),
            );
        } else {
            output.push(0);
            put_u32(
                &mut output,
                u32::try_from(snapshot.len()).map_err(|_| "snapshot exceeds 4GiB")?,
            );
            output.extend_from_slice(snapshot);
        }
    }
    Ok(output)
}

fn encode_resolution(
    ordinal: u32,
    resolution: &ConflictResolution,
    attachment_index: Option<u32>,
) -> Result<Vec<u8>, String> {
    let mut output = Vec::new();
    match resolution {
        ConflictResolution::TakeBase => {
            output.push(0);
            put_u32(&mut output, ordinal);
            output.push(0);
        }
        ConflictResolution::TakeA => {
            output.push(0);
            put_u32(&mut output, ordinal);
            output.push(1);
        }
        ConflictResolution::TakeB => {
            output.push(0);
            put_u32(&mut output, ordinal);
            output.push(2);
        }
        ConflictResolution::Replace(snapshot) => {
            // CSV row snapshots are semantic content updates. Format-only
            // changes have no independent row merge semantics.
            output.push(1);
            put_u32(&mut output, ordinal);
            output.push(0);
            if let Some(index) = attachment_index {
                output.push(1);
                put_u32(&mut output, index);
                output.extend_from_slice(&0u64.to_le_bytes());
                output.extend_from_slice(
                    &u64::try_from(snapshot.len())
                        .expect("usize fits u64")
                        .to_le_bytes(),
                );
            } else {
                output.push(0);
                put_u32(
                    &mut output,
                    u32::try_from(snapshot.len()).map_err(|_| "snapshot exceeds 4GiB")?,
                );
                output.extend_from_slice(snapshot);
            }
        }
        ConflictResolution::Delete => {
            output.push(2);
            put_u32(&mut output, ordinal);
        }
    }
    Ok(output)
}

fn encode_key(output: &mut Vec<u8>, schema_key: &str, pk: &[String]) -> Result<(), String> {
    put_text(output, schema_key)?;
    put_u32(
        output,
        u32::try_from(pk.len()).map_err(|_| "entity primary key has too many components")?,
    );
    for component in pk {
        put_text(output, component)?;
    }
    Ok(())
}

fn put_text(output: &mut Vec<u8>, value: &str) -> Result<(), String> {
    put_u32(
        output,
        u32::try_from(value.len()).map_err(|_| "text value exceeds 4GiB")?,
    );
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

struct Decoder<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], String> {
        let end = self
            .cursor
            .checked_add(len)
            .ok_or_else(|| "packet length overflow".to_owned())?;
        let value = self
            .bytes
            .get(self.cursor..end)
            .ok_or_else(|| "truncated packet".to_owned())?;
        self.cursor = end;
        Ok(value)
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.cursor)
    }

    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, String> {
        let bytes: [u8; 4] = self.take(4)?.try_into().expect("four bytes");
        Ok(u32::from_le_bytes(bytes))
    }

    fn u64(&mut self) -> Result<u64, String> {
        let bytes: [u8; 8] = self.take(8)?.try_into().expect("eight bytes");
        Ok(u64::from_le_bytes(bytes))
    }

    fn text(&mut self) -> Result<String, String> {
        let len = usize::try_from(self.u32()?).expect("u32 fits usize");
        std::str::from_utf8(self.take(len)?)
            .map(ToOwned::to_owned)
            .map_err(|error| format!("packet text is not UTF-8: {error}"))
    }

    fn key(&mut self) -> Result<(String, Vec<String>), String> {
        let schema_key = self.text()?;
        let count = usize::try_from(self.u32()?).expect("u32 fits usize");
        if count > self.remaining() / 4 {
            return Err("entity primary-key component count exceeds packet bounds".to_owned());
        }
        let mut pk = Vec::with_capacity(count);
        for _ in 0..count {
            pk.push(self.text()?);
        }
        Ok((schema_key, pk))
    }

    fn blob(
        &mut self,
        attachment: &mut impl FnMut(u32, u64, u64) -> Result<Vec<u8>, String>,
    ) -> Result<Vec<u8>, String> {
        self.blob_reference()?.materialize(attachment)
    }

    fn blob_reference(&mut self) -> Result<ConflictSnapshot, String> {
        match self.u8()? {
            0 => {
                let len = usize::try_from(self.u32()?).expect("u32 fits usize");
                Ok(ConflictSnapshot::Inline(self.take(len)?.to_vec()))
            }
            1 => Ok(ConflictSnapshot::Attachment {
                index: self.u32()?,
                offset: self.u64()?,
                length: self.u64()?,
            }),
            tag => Err(format!("unknown packet blob-ref tag {tag}")),
        }
    }

    fn conflict_state(&mut self) -> Result<Option<ConflictSnapshot>, String> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.blob_reference().map(Some),
            tag => Err(format!("unknown packet conflict-state tag {tag}")),
        }
    }

    fn finish(self) -> Result<(), String> {
        if self.cursor == self.bytes.len() {
            Ok(())
        } else {
            Err("packet record has trailing bytes".to_owned())
        }
    }
}

fn framed_records(payload: &[u8], count: u32) -> Result<Vec<&[u8]>, String> {
    if count == 0 {
        return Err("packet page must contain at least one record".to_owned());
    }
    let count = usize::try_from(count).expect("u32 fits usize");
    if count > payload.len() / 4 {
        return Err("packet record count exceeds payload bounds".to_owned());
    }
    let mut decoder = Decoder::new(payload);
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        let len = usize::try_from(decoder.u32()?).expect("u32 fits usize");
        records.push(decoder.take(len)?);
    }
    decoder.finish()?;
    Ok(records)
}

pub fn decode_entity_page(
    payload: &[u8],
    count: u32,
    mut attachment: impl FnMut(u32, u64, u64) -> Result<Vec<u8>, String>,
) -> Result<Vec<EntityRecord>, String> {
    let records = framed_records(payload, count)?;
    let mut output = Vec::with_capacity(records.len());
    for record in records {
        let mut decoder = Decoder::new(record);
        let (schema_key, entity_pk) = decoder.key()?;
        let snapshot = decoder.blob(&mut attachment)?;
        decoder.finish()?;
        output.push(EntityRecord {
            schema_key,
            entity_pk,
            snapshot,
        });
    }
    Ok(output)
}

pub fn decode_change_page(
    payload: &[u8],
    count: u32,
    mut attachment: impl FnMut(u32, u64, u64) -> Result<Vec<u8>, String>,
) -> Result<Vec<EntityChange>, String> {
    let records = framed_records(payload, count)?;
    let mut output = Vec::with_capacity(records.len());
    for record in records {
        let mut decoder = Decoder::new(record);
        let tag = decoder.u8()?;
        let (schema_key, entity_pk) = decoder.key()?;
        let (snapshot, effect) = match tag {
            0 => {
                let effect = match decoder.u8()? {
                    0 => ChangeEffect::Content,
                    1 => ChangeEffect::FormatOnly,
                    value => return Err(format!("unknown packet effect tag {value}")),
                };
                (Some(decoder.blob(&mut attachment)?), effect)
            }
            1 => (None, ChangeEffect::Content),
            value => return Err(format!("unknown packet change tag {value}")),
        };
        decoder.finish()?;
        output.push(EntityChange {
            schema_key,
            entity_pk,
            snapshot,
            effect,
        });
    }
    Ok(output)
}

/// Decodes a conflict page without fetching attachment-backed snapshots.  The
/// binding decides whether a particular record needs the bytes: a canonical
/// `take(b)` fallback can stay entirely lazy, while the CSV row merge reads only the three
/// snapshots it actually composes.
pub fn decode_conflict_page(payload: &[u8], count: u32) -> Result<Vec<ConflictRecord>, String> {
    let records = framed_records(payload, count)?;
    let mut output = Vec::with_capacity(records.len());
    for record in records {
        let mut decoder = Decoder::new(record);
        let (schema_key, entity_pk) = decoder.key()?;
        let ordinal = decoder.u32()?;
        let base = decoder.conflict_state()?;
        let a = decoder.conflict_state()?;
        let b = decoder.conflict_state()?;
        decoder.finish()?;
        output.push(ConflictRecord {
            schema_key,
            entity_pk,
            ordinal,
            base,
            a,
            b,
        });
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conflict_decoder_leaves_attachment_snapshots_lazy() {
        let mut record = Vec::new();
        encode_key(&mut record, "csv_v2_row", &["row".to_owned()]).unwrap();
        put_u32(&mut record, 0x0102_0304);
        // base: attachment index 3, then an absent a value, then a tiny
        // inline b value. Decoding the record must not invoke an attachment
        // callback just to make an ordinary b-wins decision.
        record.push(1);
        record.push(1);
        put_u32(&mut record, 3);
        record.extend_from_slice(&7u64.to_le_bytes());
        record.extend_from_slice(&11u64.to_le_bytes());
        record.push(0);
        record.push(1);
        record.push(0);
        put_u32(&mut record, 3);
        record.extend_from_slice(b"{}\n");

        let mut payload = Vec::new();
        put_u32(&mut payload, u32::try_from(record.len()).unwrap());
        payload.extend_from_slice(&record);
        let decoded = decode_conflict_page(&payload, 1).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].ordinal, 0x0102_0304);
        assert!(matches!(
            decoded[0].base,
            Some(ConflictSnapshot::Attachment {
                index: 3,
                offset: 7,
                length: 11
            })
        ));
        assert!(decoded[0].a.is_none());
        assert_eq!(
            decoded[0].b,
            Some(ConflictSnapshot::Inline(b"{}\n".to_vec()))
        );
    }

    #[test]
    fn resolution_stream_encodes_take_and_large_replacement() {
        let replacement = vec![b'x'; 128];
        let mut stream = ResolutionStream::default();
        stream.extend([
            (5, ConflictResolution::TakeBase),
            (7, ConflictResolution::TakeB),
            (9, ConflictResolution::Replace(replacement.clone())),
        ]);
        let page = stream.next_page(512, 32).unwrap().unwrap();
        assert_eq!(page.record_count, 3);
        assert_eq!(page.attachments.len(), 1);
        assert_eq!(page.attachments[0].as_ref(), &replacement);

        let records = framed_records(&page.payload, page.record_count).unwrap();
        assert_eq!(records[0], [0, 5, 0, 0, 0, 0]);
        assert_eq!(records[1], [0, 7, 0, 0, 0, 2]);
        let mut replacement_record = Decoder::new(records[2]);
        assert_eq!(replacement_record.u8().unwrap(), 1);
        assert_eq!(replacement_record.u32().unwrap(), 9);
        assert_eq!(replacement_record.u8().unwrap(), 0);
        assert_eq!(replacement_record.u8().unwrap(), 1);
        assert_eq!(replacement_record.u32().unwrap(), 0);
        assert_eq!(replacement_record.u64().unwrap(), 0);
        assert_eq!(replacement_record.u64().unwrap(), replacement.len() as u64);
        replacement_record.finish().unwrap();
    }

    #[test]
    fn change_stream_pages_and_roundtrips_complete_changes() {
        let expected = EntityChange {
            schema_key: "csv_v2_row".to_owned(),
            entity_pk: vec!["id".to_owned()],
            snapshot: Some(br#"{"id":"id","order_key":"01","cells":["x"]}"#.to_vec()),
            effect: ChangeEffect::Content,
        };
        let mut stream = ChangeStream::ready(vec![expected.clone()]);
        let mut pending = None;
        let page = stream.next_page(&mut pending, 4096, 4096).unwrap().unwrap();
        assert!(page.attachments.is_empty());
        let decoded = decode_change_page(&page.payload, page.record_count, |_, _, _| {
            Err("unexpected attachment".to_owned())
        })
        .unwrap();
        assert_eq!(decoded, [expected]);
        assert!(
            stream
                .next_page(&mut pending, 4096, 4096)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn oversized_row_snapshot_uses_one_page_local_attachment() {
        let expected = EntityChange {
            schema_key: "csv_v2_row".to_owned(),
            entity_pk: vec!["large".to_owned()],
            snapshot: Some(vec![b'x'; 2 * 1024 * 1024]),
            effect: ChangeEffect::Content,
        };
        let mut stream = ChangeStream::ready(vec![expected.clone()]);
        let mut pending = None;
        let page = stream.next_page(&mut pending, 4096, 256).unwrap().unwrap();
        assert_eq!(page.attachments.len(), 1);
        assert_eq!(page.attachments[0].len(), 2 * 1024 * 1024);
        let decoded =
            decode_change_page(&page.payload, page.record_count, |index, offset, length| {
                let value = page
                    .attachments
                    .get(usize::try_from(index).expect("u32 fits usize"))
                    .ok_or_else(|| "missing attachment".to_owned())?;
                let start = usize::try_from(offset).map_err(|_| "offset overflow".to_owned())?;
                let end = start
                    .checked_add(usize::try_from(length).map_err(|_| "length overflow".to_owned())?)
                    .ok_or_else(|| "range overflow".to_owned())?;
                value
                    .get(start..end)
                    .map(ToOwned::to_owned)
                    .ok_or_else(|| "attachment range exceeds value".to_owned())
            })
            .unwrap();
        assert_eq!(decoded, [expected]);
    }

    #[test]
    fn impossible_record_count_is_rejected_before_allocation() {
        let error = decode_entity_page(&[], u32::MAX, |_, _, _| {
            Err("unexpected attachment".to_owned())
        })
        .unwrap_err();
        assert!(
            error.contains("record count exceeds payload bounds"),
            "{error}"
        );
    }
}
