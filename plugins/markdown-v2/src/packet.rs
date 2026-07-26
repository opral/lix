use crate::core::{ChangeEffect, EntityChange, EntityRecord};
use std::collections::VecDeque;
use std::sync::Arc;

pub const FORMAT_VERSION: u16 = 1;

/// One conflict side decoded from the host packet. Attachment values deliberately
/// remain descriptors until the resolver decides it needs to inspect them: the
/// common deterministic b-wins path must not copy a large entity snapshot
/// through guest linear memory.
#[derive(Clone, Debug)]
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
}

/// One same-key base/a/b triple. The engine owns the canonical conflict
/// ordering, while the plugin only chooses an aligned resolution. a and b are
/// canonically ordered by the host; their names deliberately do not imply a
/// branch direction or time-based preference to plugin authors.
#[derive(Clone, Debug)]
pub struct EntityConflict {
    pub schema_key: String,
    /// Host-assigned ordinal within this static resolution transition. The
    /// guest must echo it in the result so the host can reject reordered or
    /// replayed answers instead of trusting cursor position alone.
    pub ordinal: u32,
    pub base: Option<ConflictSnapshot>,
    pub a: Option<ConflictSnapshot>,
    pub b: Option<ConflictSnapshot>,
}

/// An aligned resolver choice. `Take*` retains one of the immutable input
/// snapshots entirely on the host, so an author can express the common
/// deterministic choices without copying a value through guest memory.
/// `Replace` is reference-counted so an oversized merged paragraph can become
/// one page-local output attachment without another guest-side copy.
#[derive(Clone, Debug)]
pub enum ConflictResolution {
    TakeBase,
    TakeA,
    TakeB,
    Replace(Arc<Vec<u8>>),
    Delete,
}

#[derive(Clone, Debug)]
pub struct ResolutionPage {
    pub record_count: u32,
    pub payload: Vec<u8>,
    pub attachments: Vec<Arc<Vec<u8>>>,
}

#[derive(Clone, Debug, Default)]
pub struct ResolutionStream {
    resolutions: VecDeque<(u32, ConflictResolution)>,
}

impl ResolutionStream {
    /// Appends one already-bounded source page of results. The binding owns
    /// the conflict `PacketSource` and calls this only after it has decoded a
    /// single input page, keeping guest memory proportional to a page rather
    /// than the total number of conflicts in a file.
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
        let page_limit = usize::try_from(max_bytes).expect("u32 fits usize");
        let record_limit = usize::try_from(max_record_bytes).expect("u32 fits usize");
        let mut payload = Vec::with_capacity(page_limit.min(64 * 1024));
        let mut attachments = Vec::new();
        let mut record_count = 0u32;

        while let Some((ordinal, resolution)) = self.resolutions.pop_front() {
            let definitely_needs_attachment = match &resolution {
                ConflictResolution::Replace(snapshot) => snapshot.len() > record_limit,
                ConflictResolution::TakeBase
                | ConflictResolution::TakeA
                | ConflictResolution::TakeB
                | ConflictResolution::Delete => false,
            };
            let inline = if definitely_needs_attachment {
                None
            } else {
                Some(encode_resolution(ordinal, &resolution, None)?)
            };
            let inline_len = inline
                .as_ref()
                .map(|record| {
                    record
                        .len()
                        .checked_add(4)
                        .ok_or_else(|| "resolution record length overflow".to_owned())
                })
                .transpose()?;
            let inline_fits = inline
                .as_ref()
                .zip(inline_len)
                .is_some_and(|(record, framed)| {
                    record.len() <= record_limit && framed <= page_limit
                });

            let (record, attachment) = if inline_fits {
                let record = inline.expect("inline resolution was checked before selection");
                let framed = inline_len.expect("inline frame was checked before selection");
                if payload
                    .len()
                    .checked_add(framed)
                    .is_none_or(|next| next > page_limit)
                {
                    self.resolutions.push_front((ordinal, resolution));
                    break;
                }
                (record, None)
            } else if let ConflictResolution::Replace(snapshot) = &resolution {
                let attachment_index = u32::try_from(attachments.len())
                    .map_err(|_| "resolution page has too many attachments".to_owned())?;
                let record = encode_resolution(ordinal, &resolution, Some(attachment_index))?;
                let framed = record
                    .len()
                    .checked_add(4)
                    .ok_or_else(|| "resolution record length overflow".to_owned())?;
                if record.len() > record_limit {
                    return Err(format!(
                        "resolution record metadata requires {} bytes, record cap is {record_limit}",
                        record.len()
                    ));
                }
                if framed > page_limit {
                    return Err(format!(
                        "resolution record metadata requires {framed} bytes, page cap is {page_limit}"
                    ));
                }
                if payload
                    .len()
                    .checked_add(framed)
                    .is_none_or(|next| next > page_limit)
                {
                    self.resolutions.push_front((ordinal, resolution));
                    break;
                }
                (record, Some(Arc::clone(snapshot)))
            } else {
                return Err(format!(
                    "resolution record requires {} bytes, record cap is {record_limit}, framed page cap is {page_limit}",
                    inline_len.expect("take variants and delete always encode inline")
                ));
            };

            put_u32(
                &mut payload,
                u32::try_from(record.len()).map_err(|_| "resolution record exceeds 4GiB")?,
            );
            payload.extend_from_slice(&record);
            if let Some(attachment) = attachment {
                attachments.push(attachment);
            }
            record_count = record_count
                .checked_add(1)
                .ok_or_else(|| "resolution page record count overflow".to_owned())?;
        }

        if record_count == 0 {
            Ok(None)
        } else {
            Ok(Some(ResolutionPage {
                record_count,
                payload,
                attachments,
            }))
        }
    }
}

#[derive(Clone, Debug)]
pub struct ChangePage {
    pub record_count: u32,
    pub payload: Vec<u8>,
    pub attachments: Vec<Arc<Vec<u8>>>,
}

#[derive(Clone, Debug)]
pub struct ChangeStream {
    changes: VecDeque<EntityChange>,
}

impl ChangeStream {
    pub fn new(changes: Vec<EntityChange>) -> Self {
        Self {
            changes: changes.into(),
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
        let mut record_count = 0u32;
        loop {
            let mut change = pending.take().or_else(|| self.changes.pop_front());
            let Some(mut change) = change.take() else {
                break;
            };
            let definitely_needs_attachment = change.snapshot.as_ref().is_some_and(|snapshot| {
                snapshot.len() > record_limit
                    || snapshot
                        .len()
                        .checked_add(4)
                        .is_none_or(|framed_len| framed_len > limit)
            });
            let inline = if definitely_needs_attachment {
                None
            } else {
                Some(encode_change(&change, None)?)
            };
            let inline_record_len = inline.as_ref().map(Vec::len);
            let inline_len = inline
                .as_ref()
                .map(|record| {
                    record
                        .len()
                        .checked_add(4)
                        .ok_or_else(|| "change record length overflow".to_owned())
                })
                .transpose()?;
            let inline_fits =
                inline
                    .as_ref()
                    .zip(inline_len)
                    .is_some_and(|(record, framed_len)| {
                        record.len() <= record_limit && framed_len <= limit
                    });
            let (record, attached) = if inline_fits {
                let inline = inline.expect("an inline record was checked before selection");
                let inline_len = inline_len.expect("an inline frame was checked before selection");
                if payload
                    .len()
                    .checked_add(inline_len)
                    .is_none_or(|next_len| next_len > limit)
                {
                    *pending = Some(change);
                    break;
                }
                (inline, false)
            } else if change.snapshot.is_some() {
                let attachment_index = u32::try_from(attachments.len())
                    .map_err(|_| "change page has too many attachments".to_owned())?;
                let record = encode_change(&change, Some(attachment_index))?;
                let framed_len = record
                    .len()
                    .checked_add(4)
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
            if attached {
                attachments.push(Arc::new(
                    change
                        .snapshot
                        .take()
                        .expect("attached change must have a snapshot"),
                ));
            }
            record_count += 1;
        }
        if record_count == 0 {
            Ok(None)
        } else {
            Ok(Some(ChangePage {
                record_count,
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
        match attachment_index {
            Some(index) => {
                output.push(1);
                put_u32(&mut output, index);
                output.extend_from_slice(&0_u64.to_le_bytes());
                output.extend_from_slice(
                    &u64::try_from(snapshot.len())
                        .expect("usize fits u64")
                        .to_le_bytes(),
                );
            }
            None => {
                output.push(0);
                put_u32(
                    &mut output,
                    u32::try_from(snapshot.len()).map_err(|_| "snapshot exceeds 4GiB")?,
                );
                output.extend_from_slice(snapshot);
            }
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
            output.push(1);
            put_u32(&mut output, ordinal);
            // A three-way paragraph merge changes semantic content, never only
            // formatting. The engine's resolution packet decoder uses this
            // same v1 effect tag as entity-change packets.
            output.push(0);
            match attachment_index {
                Some(index) => {
                    output.push(1);
                    put_u32(&mut output, index);
                    output.extend_from_slice(&0_u64.to_le_bytes());
                    output.extend_from_slice(
                        &u64::try_from(snapshot.len())
                            .expect("usize fits u64")
                            .to_le_bytes(),
                    );
                }
                None => {
                    output.push(0);
                    put_u32(
                        &mut output,
                        u32::try_from(snapshot.len()).map_err(|_| "snapshot exceeds 4GiB")?,
                    );
                    output.extend_from_slice(snapshot);
                }
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
        u32::try_from(pk.len()).map_err(|_| "entity key has too many components")?,
    );
    for component in pk {
        put_text(output, component)?;
    }
    Ok(())
}

fn put_text(output: &mut Vec<u8>, value: &str) -> Result<(), String> {
    put_u32(
        output,
        u32::try_from(value.len()).map_err(|_| "packet text exceeds 4GiB")?,
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
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("four bytes"),
        ))
    }

    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("eight bytes"),
        ))
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
        match self.u8()? {
            0 => {
                let len = usize::try_from(self.u32()?).expect("u32 fits usize");
                Ok(self.take(len)?.to_vec())
            }
            1 => attachment(self.u32()?, self.u64()?, self.u64()?),
            tag => Err(format!("unknown packet blob-ref tag {tag}")),
        }
    }

    fn conflict_snapshot(&mut self) -> Result<ConflictSnapshot, String> {
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
                    tag => return Err(format!("unknown packet effect tag {tag}")),
                };
                (Some(decoder.blob(&mut attachment)?), effect)
            }
            1 => (None, ChangeEffect::Content),
            tag => return Err(format!("unknown packet change tag {tag}")),
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

/// Decodes conflict packet framing without dereferencing any snapshot
/// attachment. The resolver can therefore choose `TakeB` for a large
/// value without importing it into WebAssembly memory.
pub fn decode_conflict_page(payload: &[u8], count: u32) -> Result<Vec<EntityConflict>, String> {
    let records = framed_records(payload, count)?;
    let mut output = Vec::with_capacity(records.len());
    for record in records {
        let mut decoder = Decoder::new(record);
        let (schema_key, _) = decoder.key()?;
        let ordinal = decoder.u32()?;
        let mut state = || -> Result<Option<ConflictSnapshot>, String> {
            match decoder.u8()? {
                0 => Ok(None),
                1 => decoder.conflict_snapshot().map(Some),
                tag => Err(format!("unknown packet conflict-state tag {tag}")),
            }
        };
        let base = state()?;
        let a = state()?;
        let b = state()?;
        decoder.finish()?;
        output.push(EntityConflict {
            schema_key,
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
    fn record_cap_moves_a_snapshot_to_one_page_local_attachment() {
        let expected = EntityChange {
            schema_key: "markdown_node_v2".to_owned(),
            entity_pk: vec!["large".to_owned()],
            snapshot: Some(vec![b'x'; 2 * 1024 * 1024]),
            effect: ChangeEffect::Content,
        };
        let mut stream = ChangeStream::new(vec![expected.clone()]);
        let mut pending = None;
        let page = stream.next_page(&mut pending, 4096, 256).unwrap().unwrap();
        assert_eq!(page.attachments.len(), 1);
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

    #[test]
    fn conflict_decoder_keeps_attachment_snapshot_lazy() {
        let mut record = Vec::new();
        encode_key(
            &mut record,
            "markdown_node_v2",
            &["paragraph-id".to_owned()],
        )
        .unwrap();
        // Host ordinal 0x44332211, then base = present attachment(7, 11, 13);
        // a/b are absent.
        put_u32(&mut record, 0x4433_2211);
        record.push(1);
        record.push(1);
        put_u32(&mut record, 7);
        record.extend_from_slice(&11_u64.to_le_bytes());
        record.extend_from_slice(&13_u64.to_le_bytes());
        record.push(0);
        record.push(0);
        let mut payload = Vec::new();
        put_u32(&mut payload, u32::try_from(record.len()).unwrap());
        payload.extend_from_slice(&record);

        let conflicts = decode_conflict_page(&payload, 1).unwrap();
        assert!(matches!(
            conflicts[0].base,
            Some(ConflictSnapshot::Attachment {
                index: 7,
                offset: 11,
                length: 13,
            })
        ));
        assert_eq!(conflicts[0].ordinal, 0x4433_2211);
        assert!(conflicts[0].a.is_none());
        assert!(conflicts[0].b.is_none());
    }

    #[test]
    fn resolution_stream_encodes_all_typed_take_sides_without_attachments() {
        let mut stream = ResolutionStream::default();
        stream.extend([
            (0x4433_2211, ConflictResolution::TakeBase),
            (7, ConflictResolution::TakeA),
            (9, ConflictResolution::TakeB),
            (11, ConflictResolution::Delete),
        ]);
        let page = stream.next_page(64, 64).unwrap().unwrap();
        assert_eq!(page.record_count, 4);
        assert_eq!(
            page.payload,
            [
                6, 0, 0, 0, 0, 0x11, 0x22, 0x33, 0x44, 0, // take base
                6, 0, 0, 0, 0, 7, 0, 0, 0, 1, // take a
                6, 0, 0, 0, 0, 9, 0, 0, 0, 2, // take b
                5, 0, 0, 0, 2, 11, 0, 0, 0, // delete
            ]
        );
        assert!(page.attachments.is_empty());
        assert!(stream.next_page(64, 64).unwrap().is_none());
    }

    #[test]
    fn resolution_stream_moves_large_merged_snapshot_to_one_output_attachment() {
        let snapshot = Arc::new(vec![b'x'; 4096]);
        let mut stream = ResolutionStream::default();
        stream.extend([(7, ConflictResolution::Replace(Arc::clone(&snapshot)))]);
        let page = stream.next_page(128, 32).unwrap().unwrap();
        assert_eq!(page.record_count, 1);
        assert_eq!(page.attachments, [snapshot]);
        // Frame length is 27 bytes. Replace(1), ordinal(4), content(0),
        // attachment(1), index(4), offset(8), length(8).
        assert_eq!(
            u32::from_le_bytes(page.payload[..4].try_into().unwrap()),
            27
        );
        assert_eq!(page.payload[4..11], [1, 7, 0, 0, 0, 0, 1]);
    }
}
