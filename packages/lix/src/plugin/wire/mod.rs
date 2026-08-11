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

#[cfg(not(all(target_arch = "wasm32", target_os = "wasi", target_env = "p2")))]
mod layout;

#[cfg(not(all(target_arch = "wasm32", target_os = "wasi", target_env = "p2")))]
pub(crate) use layout::{CompiledLayout, insert_generated_id, validate_generated_id};

const MAGIC: &[u8; 8] = b"LIXEPG01";
const DESCRIPTOR_BYTES: usize = 16;
const TRAILER_BYTES: usize = MAGIC.len();

/// Exact bytes appended around one owned section payload.
pub fn single_section_overhead(schema_key: &str, layout: &[u8]) -> Result<usize, Error> {
    schema_key
        .len()
        .checked_add(layout.len())
        .and_then(|bytes| bytes.checked_add(DESCRIPTOR_BYTES + TRAILER_BYTES))
        .ok_or(Error::LengthOverflow)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Representation {
    Snapshots = 0,
    SchemaRows = 1,
}

impl TryFrom<u8> for Representation {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Snapshots),
            1 => Ok(Self::SchemaRows),
            value => Err(Error::UnknownRepresentation(value)),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Operation {
    Mixed = 0,
    Create = 1,
    Update = 2,
    Delete = 3,
}

impl TryFrom<u8> for Operation {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Mixed),
            1 => Ok(Self::Create),
            2 => Ok(Self::Update),
            3 => Ok(Self::Delete),
            value => Err(Error::UnknownOperation(value)),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Section<'a> {
    pub representation: Representation,
    pub operation: Operation,
    /// Empty only for mixed snapshot pages whose records carry schema keys.
    pub schema_key: &'a str,
    /// Empty for snapshots. Schema rows carry one declarative layout per page.
    pub layout: &'a [u8],
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
        let record_count = read_u32(bytes, descriptor_offset + 12)?;
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

/// Appends the descriptor and magic trailer to an owned payload without copying it.
pub fn encode_single_section(
    representation: Representation,
    operation: Operation,
    schema_key: &str,
    layout: &[u8],
    record_count: u32,
    mut payload: Vec<u8>,
) -> Result<Vec<u8>, Error> {
    validate_section(&Section {
        representation,
        operation,
        schema_key,
        layout,
        record_count,
        payload: &payload,
    })?;
    payload.reserve(single_section_overhead(schema_key, layout)?);
    payload.extend_from_slice(schema_key.as_bytes());
    payload.extend_from_slice(layout);
    append_descriptor(
        &mut payload,
        representation,
        operation,
        u32::try_from(schema_key.len()).map_err(|_| Error::LengthOverflow)?,
        u32::try_from(layout.len()).map_err(|_| Error::LengthOverflow)?,
        record_count,
    );
    payload.extend_from_slice(MAGIC);
    Ok(payload)
}

fn append_descriptor(
    output: &mut Vec<u8>,
    representation: Representation,
    operation: Operation,
    schema_len: u32,
    layout_len: u32,
    record_count: u32,
) {
    output.push(representation as u8);
    output.push(operation as u8);
    output.extend_from_slice(&0_u16.to_le_bytes());
    output.extend_from_slice(&schema_len.to_le_bytes());
    output.extend_from_slice(&layout_len.to_le_bytes());
    output.extend_from_slice(&record_count.to_le_bytes());
}

fn validate_section(section: &Section<'_>) -> Result<(), Error> {
    if section.record_count == 0 || section.payload.is_empty() {
        return Err(Error::EmptySection);
    }
    match section.representation {
        Representation::Snapshots => {
            if section.operation != Operation::Mixed
                || !section.schema_key.is_empty()
                || !section.layout.is_empty()
            {
                return Err(Error::InvalidSnapshotSection);
            }
        }
        Representation::SchemaRows => {
            if section.operation == Operation::Mixed
                || section.schema_key.is_empty()
                || section.layout.is_empty()
            {
                return Err(Error::InvalidSchemaRowSection);
            }
        }
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
    let representation = Representation::try_from(bytes[offset])?;
    let operation = Operation::try_from(bytes[offset + 1])?;
    if read_u16(bytes, offset + 2)? != 0 {
        return Err(Error::InvalidOffsets);
    }
    let schema_len = read_u32(bytes, offset + 4)? as usize;
    let layout_len = read_u32(bytes, offset + 8)? as usize;
    let record_count = read_u32(bytes, offset + 12)?;
    let schema_offset = offset
        .checked_sub(layout_len)
        .and_then(|end| end.checked_sub(schema_len))
        .ok_or(Error::InvalidOffsets)?;
    let layout_offset = schema_offset
        .checked_add(schema_len)
        .ok_or(Error::LengthOverflow)?;
    let schema = &bytes[schema_offset..layout_offset];
    let layout = &bytes[layout_offset..offset];
    let payload = &bytes[..schema_offset];
    let section = Section {
        representation,
        operation,
        schema_key: std::str::from_utf8(schema).map_err(|_| Error::InvalidUtf8)?,
        layout,
        record_count,
        payload,
    };
    validate_section(&section)?;
    Ok(section)
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, Error> {
    Ok(u16::from_le_bytes(
        bytes
            .get(offset..offset + 2)
            .ok_or(Error::Truncated)?
            .try_into()
            .expect("two-byte slice"),
    ))
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
    InvalidSnapshotSection,
    InvalidSchemaRowSection,
    UnknownRepresentation(u8),
    UnknownOperation(u8),
    InvalidUtf8,
    InvalidOffsets,
    Truncated,
    LengthOverflow,
}

#[cfg(test)]
mod tests {
    use super::{Error, Operation, Page, Representation, encode_single_section};

    #[test]
    fn round_trips_each_supported_representation() {
        let snapshots = encode_single_section(
            Representation::Snapshots,
            Operation::Mixed,
            "",
            &[],
            2,
            b"snapshot-packet".to_vec(),
        )
        .unwrap();
        let page = Page::decode(&snapshots).unwrap();
        assert_eq!(page.record_count(), 2);
        assert_eq!(page.section().unwrap().payload, b"snapshot-packet");

        let rows = encode_single_section(
            Representation::SchemaRows,
            Operation::Create,
            "line",
            br#"{"wire":[]}"#,
            3,
            b"column-buffers".to_vec(),
        )
        .unwrap();
        let page = Page::decode(&rows).unwrap();
        assert_eq!(page.record_count(), 3);
        assert_eq!(page.section().unwrap().schema_key, "line");
    }

    #[test]
    fn single_section_encoding_preserves_payload_at_offset_zero() {
        let payload = vec![9; 1024];
        let encoded = encode_single_section(
            Representation::Snapshots,
            Operation::Mixed,
            "",
            &[],
            1,
            payload,
        )
        .unwrap();
        let page = Page::decode(&encoded).unwrap();
        let section = page.section().unwrap();
        assert_eq!(section.payload, &encoded[..1024]);
    }

    #[test]
    fn rejects_invalid_section_shapes_and_trailers() {
        let error = encode_single_section(
            Representation::SchemaRows,
            Operation::Mixed,
            "line",
            b"layout",
            1,
            vec![1],
        )
        .unwrap_err();
        assert_eq!(error, Error::InvalidSchemaRowSection);

        let mut encoded = encode_single_section(
            Representation::Snapshots,
            Operation::Mixed,
            "",
            &[],
            1,
            vec![1],
        )
        .unwrap();
        encoded.push(0);
        assert_eq!(Page::decode(&encoded), Err(Error::InvalidMagic));
    }

    #[test]
    fn rejects_impossible_lengths_and_empty_record_counts() {
        let encoded = || {
            encode_single_section(
                Representation::Snapshots,
                Operation::Mixed,
                "",
                &[],
                1,
                vec![1],
            )
            .unwrap()
        };

        let mut bad_length = encoded();
        let descriptor = bad_length.len() - super::TRAILER_BYTES - super::DESCRIPTOR_BYTES;
        bad_length[descriptor + 4..descriptor + 8].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(Page::decode(&bad_length), Err(Error::InvalidOffsets));

        let mut empty_count = encoded();
        let descriptor = empty_count.len() - super::TRAILER_BYTES - super::DESCRIPTOR_BYTES;
        empty_count[descriptor + 12..descriptor + 16].copy_from_slice(&0_u32.to_le_bytes());
        assert_eq!(Page::decode(&empty_count), Err(Error::EmptyPage));
    }
}
