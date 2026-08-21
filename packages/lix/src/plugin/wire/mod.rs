//! Host/guest-neutral encoding for the universal Lix plugin row page.
//!
//! Payloads precede a fixed descriptor and magic trailer. A guest can therefore turn
//! its existing batch buffer into a page by appending metadata without copying
//! or moving the batch bytes.

#![allow(clippy::missing_errors_doc)]
#![cfg_attr(not(feature = "default_wasm_runtime"), allow(dead_code))]
#![cfg_attr(
    all(target_arch = "wasm32", target_os = "wasi", target_env = "p2"),
    allow(dead_code)
)]

pub(crate) mod typed;

const MAGIC: &[u8; 8] = b"LIXEPG03";
const DESCRIPTOR_BYTES: usize = 8;
const SCHEMA_FINGERPRINT_BYTES: usize = 32;
const TRAILER_BYTES: usize = MAGIC.len();

/// Exact bytes appended to one typed page payload.
pub(crate) fn typed_page_overhead(schema_key: &str) -> Result<usize, Error> {
    schema_key
        .len()
        .checked_add(SCHEMA_FINGERPRINT_BYTES)
        .and_then(|bytes| bytes.checked_add(DESCRIPTOR_BYTES + TRAILER_BYTES))
        .ok_or(Error::LengthOverflow)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Section<'a> {
    pub schema_key: &'a str,
    /// Exact Schema v1 fingerprint for the page's schema.
    pub schema_fingerprint: &'a [u8],
    pub record_count: u32,
    pub payload: &'a [u8],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Page<'a> {
    bytes: &'a [u8],
    descriptor_offset: usize,
    record_count: u32,
}

impl<'a> Page<'a> {
    pub fn decode(bytes: &'a [u8]) -> Result<Self, Error> {
        if bytes.len() < DESCRIPTOR_BYTES + TRAILER_BYTES {
            return Err(Error::Truncated);
        }
        let trailer = bytes.len() - TRAILER_BYTES;
        if &bytes[trailer..] != MAGIC {
            return Err(Error::InvalidMagic);
        }
        let descriptor_offset = trailer - DESCRIPTOR_BYTES;
        let record_count = read_u32(bytes, descriptor_offset + 4)?;
        if record_count == 0 {
            return Err(Error::EmptyPage);
        }
        let page = Self {
            bytes,
            descriptor_offset,
            record_count,
        };
        page.section()?;
        Ok(page)
    }

    pub fn record_count(self) -> u32 {
        self.record_count
    }

    pub fn section(self) -> Result<Section<'a>, Error> {
        decode_section(self.bytes, self.descriptor_offset)
    }
}

/// Appends the typed-page descriptor and v2 magic trailer without copying the payload.
pub(crate) fn encode_typed_page(
    schema_key: &str,
    schema_fingerprint: &[u8; 32],
    record_count: u32,
    mut payload: Vec<u8>,
) -> Result<Vec<u8>, Error> {
    validate_section(&Section {
        schema_key,
        schema_fingerprint,
        record_count,
        payload: &payload,
    })?;
    payload.reserve(typed_page_overhead(schema_key)?);
    payload.extend_from_slice(schema_key.as_bytes());
    payload.extend_from_slice(schema_fingerprint);
    append_descriptor(
        &mut payload,
        u32::try_from(schema_key.len()).map_err(|_| Error::LengthOverflow)?,
        record_count,
    );
    payload.extend_from_slice(MAGIC);
    Ok(payload)
}

fn append_descriptor(output: &mut Vec<u8>, schema_len: u32, record_count: u32) {
    output.extend_from_slice(&schema_len.to_le_bytes());
    output.extend_from_slice(&record_count.to_le_bytes());
}

fn validate_section(section: &Section<'_>) -> Result<(), Error> {
    if section.record_count == 0 || section.payload.is_empty() {
        return Err(Error::EmptySection);
    }
    if section.schema_key.is_empty() || section.schema_fingerprint.len() != 32 {
        return Err(Error::InvalidTypedRowSection);
    }
    Ok(())
}

fn decode_section<'a>(bytes: &'a [u8], offset: usize) -> Result<Section<'a>, Error> {
    let end = offset
        .checked_add(DESCRIPTOR_BYTES)
        .ok_or(Error::LengthOverflow)?;
    if end > bytes.len() {
        return Err(Error::Truncated);
    }
    let schema_len = read_u32(bytes, offset)? as usize;
    let record_count = read_u32(bytes, offset + 4)?;
    let schema_offset = offset
        .checked_sub(SCHEMA_FINGERPRINT_BYTES)
        .and_then(|end| end.checked_sub(schema_len))
        .ok_or(Error::InvalidOffsets)?;
    let layout_offset = schema_offset
        .checked_add(schema_len)
        .ok_or(Error::LengthOverflow)?;
    let schema = &bytes[schema_offset..layout_offset];
    let schema_fingerprint = &bytes[layout_offset..offset];
    let payload = &bytes[..schema_offset];
    let section = Section {
        schema_key: std::str::from_utf8(schema).map_err(|_| Error::InvalidUtf8)?,
        schema_fingerprint,
        record_count,
        payload,
    };
    validate_section(&section)?;
    Ok(section)
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, Error> {
    Ok(u32::from_le_bytes(
        bytes
            .get(offset..offset + 4)
            .ok_or(Error::Truncated)?
            .try_into()
            .expect("four-byte slice"),
    ))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    InvalidMagic,
    EmptyPage,
    EmptySection,
    InvalidTypedRowSection,
    InvalidUtf8,
    InvalidOffsets,
    Truncated,
    LengthOverflow,
}

#[cfg(test)]
mod tests {
    use super::{Error, Page, encode_typed_page};

    #[test]
    fn round_trips_typed_representation() {
        let rows = encode_typed_page("line", &[7; 32], 3, b"typed-rows".to_vec()).unwrap();
        let page = Page::decode(&rows).unwrap();
        assert_eq!(page.record_count(), 3);
        assert_eq!(page.section().unwrap().schema_key, "line");
    }

    #[test]
    fn single_section_encoding_preserves_payload_at_offset_zero() {
        let payload = vec![9; 1024];
        let encoded = encode_typed_page("line", &[7; 32], 1, payload).unwrap();
        let page = Page::decode(&encoded).unwrap();
        let section = page.section().unwrap();
        assert_eq!(section.payload, &encoded[..1024]);
    }

    #[test]
    fn rejects_invalid_section_shapes_and_trailers() {
        let error = encode_typed_page("line", &[7; 32], 0, vec![1]).unwrap_err();
        assert_eq!(error, Error::EmptySection);

        let mut encoded = encode_typed_page("line", &[7; 32], 1, vec![1]).unwrap();
        encoded.push(0);
        assert_eq!(Page::decode(&encoded), Err(Error::InvalidMagic));

        let mut v1 = encode_typed_page("line", &[7; 32], 1, vec![1]).unwrap();
        let magic = v1.len() - super::TRAILER_BYTES;
        v1[magic..].copy_from_slice(b"LIXEPG01");
        assert_eq!(Page::decode(&v1), Err(Error::InvalidMagic));
    }

    #[test]
    fn rejects_impossible_lengths_and_empty_record_counts() {
        let encoded = || encode_typed_page("line", &[7; 32], 1, vec![1]).unwrap();

        let mut bad_length = encoded();
        let descriptor = bad_length.len() - super::TRAILER_BYTES - super::DESCRIPTOR_BYTES;
        bad_length[descriptor..descriptor + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(Page::decode(&bad_length), Err(Error::InvalidOffsets));

        let mut empty_count = encoded();
        let descriptor = empty_count.len() - super::TRAILER_BYTES - super::DESCRIPTOR_BYTES;
        empty_count[descriptor + 4..descriptor + 8].copy_from_slice(&0_u32.to_le_bytes());
        assert_eq!(Page::decode(&empty_count), Err(Error::EmptyPage));
    }
}
