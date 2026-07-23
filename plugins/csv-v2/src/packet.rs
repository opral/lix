use crate::{ChangeEffect, EntityChange, EntityRecord, InitialChanges};
use std::collections::VecDeque;

pub const FORMAT_VERSION: u16 = 1;

#[derive(Clone, Debug)]
pub struct ChangePage {
    pub record_count: u32,
    pub payload: Vec<u8>,
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
    ) -> Result<Option<ChangePage>, String> {
        if max_bytes == 0 {
            return Err("change cursor max-bytes must be positive".to_owned());
        }
        let limit = usize::try_from(max_bytes).expect("u32 fits usize");
        let mut payload = Vec::with_capacity(limit.min(64 * 1024));
        let mut count = 0u32;
        loop {
            let change = match pending.take() {
                Some(change) => Some(change),
                None => self.take_change()?,
            };
            let Some(change) = change else {
                *self = Self::Eof;
                break;
            };
            let record = encode_merge_group(&change)?;
            let framed_len = 4usize
                .checked_add(record.len())
                .ok_or_else(|| "change record length overflow".to_owned())?;
            if framed_len > limit {
                if count == 0 {
                    return Err(format!(
                        "change record requires {framed_len} bytes, page cap is {limit}"
                    ));
                }
                *pending = Some(change);
                break;
            }
            if payload.len() + framed_len > limit {
                *pending = Some(change);
                break;
            }
            put_u32(
                &mut payload,
                u32::try_from(record.len()).map_err(|_| "change record exceeds 4GiB")?,
            );
            payload.extend_from_slice(&record);
            count += 1;
        }
        if count == 0 {
            Ok(None)
        } else {
            Ok(Some(ChangePage {
                record_count: count,
                payload,
            }))
        }
    }
}

fn encode_merge_group(change: &EntityChange) -> Result<Vec<u8>, String> {
    let mut output = Vec::new();
    put_u32(&mut output, 1);
    output.push(u8::from(change.snapshot.is_none()));
    encode_key(&mut output, &change.schema_key, &change.entity_pk)?;
    if let Some(snapshot) = &change.snapshot {
        output.push(match change.effect {
            ChangeEffect::Content => 0,
            ChangeEffect::FormatOnly => 1,
        });
        output.push(0); // inline blob-ref
        put_u32(
            &mut output,
            u32::try_from(snapshot.len()).map_err(|_| "snapshot exceeds 4GiB")?,
        );
        output.extend_from_slice(snapshot);
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
            1 => {
                let index = self.u32()?;
                let offset = self.u64()?;
                let length = self.u64()?;
                attachment(index, offset, length)
            }
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
    let mut decoder = Decoder::new(payload);
    let mut records = Vec::with_capacity(usize::try_from(count).expect("u32 fits usize"));
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
    let mut output = Vec::with_capacity(usize::try_from(count).expect("u32 fits usize"));
    for record in framed_records(payload, count)? {
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
    let mut output = Vec::new();
    for record in framed_records(payload, count)? {
        let mut decoder = Decoder::new(record);
        let members = decoder.u32()?;
        if members == 0 {
            return Err("merge group must contain at least one member".to_owned());
        }
        for _ in 0..members {
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
            output.push(EntityChange {
                schema_key,
                entity_pk,
                snapshot,
                effect,
            });
        }
        decoder.finish()?;
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn change_stream_pages_and_roundtrips_complete_changes() {
        let expected = EntityChange {
            schema_key: "csv_row".to_owned(),
            entity_pk: vec!["id".to_owned()],
            snapshot: Some(br#"{"id":"id","order_key":"01","cells":["x"]}"#.to_vec()),
            effect: ChangeEffect::Content,
        };
        let mut stream = ChangeStream::ready(vec![expected.clone()]);
        let mut pending = None;
        let page = stream.next_page(&mut pending, 4096).unwrap().unwrap();
        let decoded = decode_change_page(&page.payload, page.record_count, |_, _, _| {
            Err("unexpected attachment".to_owned())
        })
        .unwrap();
        assert_eq!(decoded, [expected]);
        assert!(stream.next_page(&mut pending, 4096).unwrap().is_none());
    }
}
