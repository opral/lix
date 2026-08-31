use bytes::Bytes;
use futures_io::{AsyncRead, AsyncWrite};
use futures_lite::io::{AsyncReadExt as _, AsyncWriteExt as _};

use crate::LixError;

pub(super) const MAGIC: &[u8; 8] = b"LIXSNAP\0";
const TRAILER_MAGIC: &[u8; 8] = b"LIXEND\0\x01";
const CONTAINER_VERSION: u16 = 1;
const CHECKSUM_BLAKE3: u8 = 1;
const ENTRY_TAG: u8 = 1;
const TRAILER_TAG: u8 = 0xff;
pub(crate) const HEADER_BYTES: usize = MAGIC.len() + 2 + 1 + 1 + 4;
const ENTRY_HEADER_BYTES: usize = 1 + 4 + 4 + 4;
pub(crate) const TRAILER_BYTES: usize = 1 + 8 + 8 + 32 + TRAILER_MAGIC.len();

// Wire-level resource budgets. Binary content is chunked well below the value
// ceiling before reaching storage. Changing these does not change the encoding,
// but does change which otherwise well-formed snapshots this implementation
// accepts.
const MAX_KEY_BYTES: usize = 16 * 1024 * 1024;
const MAX_VALUE_BYTES: usize = 256 * 1024 * 1024;
const MAX_ENTRY_COUNT: u64 = 10_000_000;
const MAX_PAYLOAD_BYTES: u64 = 64 * 1024 * 1024 * 1024;
const READ_ALLOCATION_CHUNK_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotEntry {
    pub(crate) space_id: u32,
    pub(crate) key: Bytes,
    pub(crate) value: Bytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotHeader {
    pub(crate) lix_format_version: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotTrailer {
    pub(crate) entry_count: u64,
    pub(crate) payload_bytes: u64,
    pub(crate) digest: [u8; 32],
}

pub(crate) fn decode_streamed_snapshot_header(
    header: &[u8],
) -> Result<SnapshotHeader, LixError> {
    if header.len() != HEADER_BYTES {
        return Err(invalid_snapshot("authority snapshot has an invalid header length"));
    }
    if &header[..MAGIC.len()] != MAGIC {
        return Err(invalid_snapshot("unsupported snapshot magic"));
    }
    let mut offset = MAGIC.len();
    let version = u16::from_be_bytes([header[offset], header[offset + 1]]);
    offset += 2;
    if version != CONTAINER_VERSION {
        return Err(invalid_snapshot(format!(
            "unsupported snapshot container version {version}"
        )));
    }
    let checksum = header[offset];
    offset += 1;
    if checksum != CHECKSUM_BLAKE3 {
        return Err(invalid_snapshot(format!(
            "unsupported snapshot checksum algorithm {checksum}"
        )));
    }
    let flags = header[offset];
    offset += 1;
    if flags != 0 {
        return Err(invalid_snapshot(format!(
            "unsupported snapshot flags 0x{flags:02x}"
        )));
    }
    let lix_format_version = u32::from_be_bytes(
        header[offset..offset + 4]
            .try_into()
            .expect("snapshot format version has a fixed width"),
    );
    Ok(SnapshotHeader { lix_format_version })
}

pub(crate) fn decode_streamed_snapshot_trailer(
    total_bytes: u64,
    digest: [u8; 32],
    trailer: &[u8],
) -> Result<SnapshotTrailer, LixError> {
    if trailer.len() != TRAILER_BYTES
        || trailer[0] != TRAILER_TAG
        || trailer[49..] != *TRAILER_MAGIC
    {
        return Err(invalid_snapshot("authority snapshot has an invalid trailer"));
    }
    let entry_count = u64::from_be_bytes(trailer[1..9].try_into().expect("fixed trailer count"));
    let payload_bytes =
        u64::from_be_bytes(trailer[9..17].try_into().expect("fixed trailer byte count"));
    let expected_digest: [u8; 32] = trailer[17..49]
        .try_into()
        .expect("fixed trailer digest");
    let framing = u64::try_from(HEADER_BYTES + TRAILER_BYTES)
        .expect("snapshot framing length fits u64");
    if total_bytes.checked_sub(framing) != Some(payload_bytes) {
        return Err(invalid_snapshot("authority snapshot payload length is invalid"));
    }
    if digest != expected_digest {
        return Err(invalid_snapshot("authority snapshot digest does not match its payload"));
    }
    Ok(SnapshotTrailer {
        entry_count,
        payload_bytes,
        digest,
    })
}

pub(crate) struct SnapshotEncoder<'a, W: ?Sized> {
    writer: &'a mut W,
    hasher: blake3::Hasher,
    entry_count: u64,
    payload_bytes: u64,
    previous: Option<(u32, Bytes)>,
}

impl<'a, W> SnapshotEncoder<'a, W>
where
    W: AsyncWrite + Unpin + ?Sized,
{
    pub(crate) async fn new(writer: &'a mut W, lix_format_version: u32) -> Result<Self, LixError> {
        let mut header = [0_u8; HEADER_BYTES];
        header[..MAGIC.len()].copy_from_slice(MAGIC);
        let mut offset = MAGIC.len();
        header[offset..offset + 2].copy_from_slice(&CONTAINER_VERSION.to_be_bytes());
        offset += 2;
        header[offset] = CHECKSUM_BLAKE3;
        offset += 1;
        header[offset] = 0;
        offset += 1;
        header[offset..offset + 4].copy_from_slice(&lix_format_version.to_be_bytes());
        writer.write_all(&header).await.map_err(snapshot_io_error)?;
        Ok(Self {
            writer,
            hasher: blake3::Hasher::new(),
            entry_count: 0,
            payload_bytes: 0,
            previous: None,
        })
    }

    pub(crate) async fn write_entry(&mut self, entry: &SnapshotEntry) -> Result<(), LixError> {
        validate_component_lengths(entry.key.len(), entry.value.len())?;
        validate_entry_budget(
            self.entry_count,
            self.payload_bytes,
            entry.key.len(),
            entry.value.len(),
        )?;
        if self.previous.as_ref().is_some_and(|previous| {
            previous.0 > entry.space_id
                || (previous.0 == entry.space_id && previous.1.as_ref() >= entry.key.as_ref())
        }) {
            return Err(invalid_snapshot(
                "snapshot entries are duplicated or out of canonical order",
            ));
        }
        let key_len = u32::try_from(entry.key.len())
            .map_err(|_| invalid_snapshot("snapshot key length exceeds u32"))?;
        let value_len = u32::try_from(entry.value.len())
            .map_err(|_| invalid_snapshot("snapshot value length exceeds u32"))?;
        let mut record_header = [0_u8; ENTRY_HEADER_BYTES];
        record_header[0] = ENTRY_TAG;
        record_header[1..5].copy_from_slice(&entry.space_id.to_be_bytes());
        record_header[5..9].copy_from_slice(&key_len.to_be_bytes());
        record_header[9..13].copy_from_slice(&value_len.to_be_bytes());

        self.write_payload(&record_header).await?;
        self.write_payload(&entry.key).await?;
        self.write_payload(&entry.value).await?;
        self.entry_count = self
            .entry_count
            .checked_add(1)
            .ok_or_else(|| invalid_snapshot("snapshot entry count overflowed"))?;
        self.previous = Some((entry.space_id, entry.key.clone()));
        Ok(())
    }

    pub(crate) async fn finish(self) -> Result<SnapshotTrailer, LixError> {
        let digest = *self.hasher.finalize().as_bytes();
        let mut trailer = [0_u8; TRAILER_BYTES];
        trailer[0] = TRAILER_TAG;
        trailer[1..9].copy_from_slice(&self.entry_count.to_be_bytes());
        trailer[9..17].copy_from_slice(&self.payload_bytes.to_be_bytes());
        trailer[17..49].copy_from_slice(&digest);
        trailer[49..].copy_from_slice(TRAILER_MAGIC);
        self.writer
            .write_all(&trailer)
            .await
            .map_err(snapshot_io_error)?;
        self.writer.flush().await.map_err(snapshot_io_error)?;
        Ok(SnapshotTrailer {
            entry_count: self.entry_count,
            payload_bytes: self.payload_bytes,
            digest,
        })
    }

    async fn write_payload(&mut self, bytes: &[u8]) -> Result<(), LixError> {
        let byte_count = u64::try_from(bytes.len())
            .map_err(|_| invalid_snapshot("snapshot payload length exceeds u64"))?;
        let next_payload_bytes = self
            .payload_bytes
            .checked_add(byte_count)
            .ok_or_else(|| invalid_snapshot("snapshot payload length overflowed"))?;
        if next_payload_bytes > MAX_PAYLOAD_BYTES {
            return Err(invalid_snapshot(format!(
                "snapshot payload exceeds {MAX_PAYLOAD_BYTES} bytes"
            )));
        }
        self.writer
            .write_all(bytes)
            .await
            .map_err(snapshot_io_error)?;
        self.hasher.update(bytes);
        self.payload_bytes = next_payload_bytes;
        Ok(())
    }
}

pub(crate) struct SnapshotDecoder<R> {
    reader: R,
    hasher: blake3::Hasher,
    entry_count: u64,
    payload_bytes: u64,
    previous: Option<(u32, Bytes)>,
    complete: bool,
    trailer: Option<SnapshotTrailer>,
}

impl<R> SnapshotDecoder<R>
where
    R: AsyncRead + Unpin,
{
    pub(crate) async fn new(mut reader: R) -> Result<(SnapshotHeader, Self), LixError> {
        let mut header = [0_u8; HEADER_BYTES];
        read_exact(&mut reader, &mut header, "snapshot header").await?;
        let header = decode_streamed_snapshot_header(&header)?;
        Ok((
            header,
            Self {
                reader,
                hasher: blake3::Hasher::new(),
                entry_count: 0,
                payload_bytes: 0,
                previous: None,
                complete: false,
                trailer: None,
            },
        ))
    }

    pub(crate) async fn next_entry(&mut self) -> Result<Option<SnapshotEntry>, LixError> {
        if self.complete {
            return Ok(None);
        }
        let mut tag = [0_u8; 1];
        read_exact(&mut self.reader, &mut tag, "snapshot record tag").await?;
        match tag[0] {
            ENTRY_TAG => self.read_entry(tag[0]).await.map(Some),
            TRAILER_TAG => {
                self.trailer = Some(self.read_trailer().await?);
                self.complete = true;
                Ok(None)
            }
            tag => Err(invalid_snapshot(format!(
                "unknown snapshot record tag 0x{tag:02x}"
            ))),
        }
    }

    pub(crate) fn trailer(&self) -> Option<SnapshotTrailer> {
        self.trailer
    }

    async fn read_entry(&mut self, tag: u8) -> Result<SnapshotEntry, LixError> {
        let mut rest = [0_u8; ENTRY_HEADER_BYTES - 1];
        read_exact(&mut self.reader, &mut rest, "snapshot entry header").await?;
        let space_id = u32::from_be_bytes(rest[0..4].try_into().expect("fixed-width space id"));
        let key_len = u32::from_be_bytes(rest[4..8].try_into().expect("fixed-width key length"));
        let value_len =
            u32::from_be_bytes(rest[8..12].try_into().expect("fixed-width value length"));
        let key_len = usize::try_from(key_len)
            .map_err(|_| invalid_snapshot("snapshot key length exceeds usize"))?;
        let value_len = usize::try_from(value_len)
            .map_err(|_| invalid_snapshot("snapshot value length exceeds usize"))?;
        validate_component_lengths(key_len, value_len)?;
        validate_entry_budget(self.entry_count, self.payload_bytes, key_len, value_len)?;

        let key = read_bytes(&mut self.reader, key_len, "snapshot entry key").await?;
        let value = read_bytes(&mut self.reader, value_len, "snapshot entry value").await?;
        if self.previous.as_ref().is_some_and(|previous| {
            previous.0 > space_id || (previous.0 == space_id && previous.1.as_ref() >= key.as_ref())
        }) {
            return Err(invalid_snapshot(
                "snapshot entries are duplicated or out of canonical order",
            ));
        }

        self.hash_payload(&[tag])?;
        self.hash_payload(&rest)?;
        self.hash_payload(&key)?;
        self.hash_payload(&value)?;
        self.entry_count = self
            .entry_count
            .checked_add(1)
            .ok_or_else(|| invalid_snapshot("snapshot entry count overflowed"))?;
        self.previous = Some((space_id, key.clone()));
        Ok(SnapshotEntry {
            space_id,
            key,
            value,
        })
    }

    async fn read_trailer(&mut self) -> Result<SnapshotTrailer, LixError> {
        let mut trailer = [0_u8; TRAILER_BYTES - 1];
        read_exact(&mut self.reader, &mut trailer, "snapshot trailer").await?;
        let expected_count =
            u64::from_be_bytes(trailer[0..8].try_into().expect("fixed-width entry count"));
        let expected_bytes = u64::from_be_bytes(
            trailer[8..16]
                .try_into()
                .expect("fixed-width payload length"),
        );
        let expected_digest: [u8; 32] = trailer[16..48]
            .try_into()
            .expect("fixed-width snapshot digest");
        if &trailer[48..] != TRAILER_MAGIC {
            return Err(invalid_snapshot("invalid snapshot trailer magic"));
        }
        if expected_count != self.entry_count {
            return Err(invalid_snapshot(format!(
                "snapshot entry count mismatch: trailer {expected_count}, decoded {}",
                self.entry_count
            )));
        }
        if expected_bytes != self.payload_bytes {
            return Err(invalid_snapshot(format!(
                "snapshot payload byte count mismatch: trailer {expected_bytes}, decoded {}",
                self.payload_bytes
            )));
        }
        let actual_digest = *self.hasher.finalize().as_bytes();
        if expected_digest != actual_digest {
            return Err(invalid_snapshot("snapshot BLAKE3 digest mismatch"));
        }
        let mut trailing = [0_u8; 1];
        if self
            .reader
            .read(&mut trailing)
            .await
            .map_err(snapshot_io_error)?
            != 0
        {
            return Err(invalid_snapshot("snapshot contains trailing data"));
        }
        Ok(SnapshotTrailer {
            entry_count: self.entry_count,
            payload_bytes: self.payload_bytes,
            digest: actual_digest,
        })
    }

    fn hash_payload(&mut self, bytes: &[u8]) -> Result<(), LixError> {
        let byte_count = u64::try_from(bytes.len())
            .map_err(|_| invalid_snapshot("snapshot payload length exceeds u64"))?;
        let next_payload_bytes = self
            .payload_bytes
            .checked_add(byte_count)
            .ok_or_else(|| invalid_snapshot("snapshot payload length overflowed"))?;
        if next_payload_bytes > MAX_PAYLOAD_BYTES {
            return Err(invalid_snapshot(format!(
                "snapshot payload exceeds {MAX_PAYLOAD_BYTES} bytes"
            )));
        }
        self.hasher.update(bytes);
        self.payload_bytes = next_payload_bytes;
        Ok(())
    }
}

fn validate_entry_budget(
    entry_count: u64,
    payload_bytes: u64,
    key_len: usize,
    value_len: usize,
) -> Result<(), LixError> {
    let next_entry_count = entry_count
        .checked_add(1)
        .ok_or_else(|| invalid_snapshot("snapshot entry count overflowed"))?;
    if next_entry_count > MAX_ENTRY_COUNT {
        return Err(invalid_snapshot(format!(
            "snapshot entry count exceeds {MAX_ENTRY_COUNT}"
        )));
    }

    let record_bytes = ENTRY_HEADER_BYTES
        .checked_add(key_len)
        .and_then(|bytes| bytes.checked_add(value_len))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| invalid_snapshot("snapshot entry byte length overflowed"))?;
    let next_payload_bytes = payload_bytes
        .checked_add(record_bytes)
        .ok_or_else(|| invalid_snapshot("snapshot payload length overflowed"))?;
    if next_payload_bytes > MAX_PAYLOAD_BYTES {
        return Err(invalid_snapshot(format!(
            "snapshot payload exceeds {MAX_PAYLOAD_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_component_lengths(key_len: usize, value_len: usize) -> Result<(), LixError> {
    if key_len > MAX_KEY_BYTES {
        return Err(invalid_snapshot(format!(
            "snapshot key length {key_len} exceeds {MAX_KEY_BYTES} bytes"
        )));
    }
    if value_len > MAX_VALUE_BYTES {
        return Err(invalid_snapshot(format!(
            "snapshot value length {value_len} exceeds {MAX_VALUE_BYTES} bytes"
        )));
    }
    Ok(())
}

async fn read_bytes<R>(reader: &mut R, len: usize, label: &str) -> Result<Bytes, LixError>
where
    R: AsyncRead + Unpin + ?Sized,
{
    let mut bytes = Vec::new();
    while bytes.len() < len {
        let additional = (len - bytes.len()).min(READ_ALLOCATION_CHUNK_BYTES);
        // Keep reads bounded, but let Vec grow geometrically. Repeated
        // `try_reserve_exact` would copy the complete prefix for every 64 KiB
        // chunk on allocators that honor the exact request.
        bytes.try_reserve(additional).map_err(|error| {
            invalid_snapshot(format!("cannot allocate {label} buffer: {error}"))
        })?;
        let start = bytes.len();
        bytes.resize(start + additional, 0);
        read_exact(reader, &mut bytes[start..], label).await?;
    }
    Ok(Bytes::from(bytes))
}

async fn read_exact<R>(reader: &mut R, bytes: &mut [u8], label: &str) -> Result<(), LixError>
where
    R: AsyncRead + Unpin + ?Sized,
{
    reader.read_exact(bytes).await.map_err(|error| {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            invalid_snapshot(format!("snapshot truncated in {label}"))
        } else {
            snapshot_io_error(error)
        }
    })
}

pub(crate) fn invalid_snapshot(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INVALID_SNAPSHOT, message)
}

fn snapshot_io_error(error: std::io::Error) -> LixError {
    LixError::new(
        LixError::CODE_SNAPSHOT_IO,
        format!("snapshot I/O failed: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        io,
        pin::Pin,
        rc::Rc,
        task::{Context, Poll},
    };

    use super::*;

    // Complete LIXSNAP v1 encoding of an empty format-76 repository. The
    // digest is the standard BLAKE3 digest of the empty canonical payload;
    // keeping these bytes literal prevents encoder and decoder changes from
    // silently redefining the wire format together.
    const EMPTY_V1_GOLDEN: &[u8] = &[
        0x4c, 0x49, 0x58, 0x53, 0x4e, 0x41, 0x50, 0x00, // LIXSNAP\0
        0x00, 0x01, // container version 1
        0x01, // BLAKE3
        0x00, // flags
        0x00, 0x00, 0x00, 0x4c, // Lix format 76
        0xff, // trailer tag
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // entry count
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // payload bytes
        0xaf, 0x13, 0x49, 0xb9, 0xf5, 0xf9, 0xa1, 0xa6, // BLAKE3(empty)
        0xa0, 0x40, 0x4d, 0xea, 0x36, 0xdc, 0xc9, 0x49, 0x9b, 0xcb, 0x25, 0xc9, 0xad, 0xc1, 0x12,
        0xb7, 0xcc, 0x9a, 0x93, 0xca, 0xe4, 0x1f, 0x32, 0x62, 0x4c, 0x49, 0x58, 0x45, 0x4e, 0x44,
        0x00, 0x01, // LIXEND\0\x01
    ];

    // Complete v1 encoding of two canonical entries. Unlike the empty vector,
    // this pins record tags, integer endianness, zero-length components, body
    // placement, ordering, payload accounting, and a non-empty digest.
    const NON_EMPTY_V1_GOLDEN: &[u8] = &[
        0x4c, 0x49, 0x58, 0x53, 0x4e, 0x41, 0x50, 0x00, // LIXSNAP\0
        0x00, 0x01, 0x01, 0x00, // version, BLAKE3, flags
        0x00, 0x00, 0x00, 0x4c, // Lix format 76
        0x01, 0x01, 0x02, 0x03, 0x04, // entry tag + space 0x01020304
        0x00, 0x00, 0x00, 0x00, // empty key
        0x00, 0x00, 0x00, 0x01, 0xaa, // one-byte value
        0x01, 0x01, 0x02, 0x03, 0x04, // entry tag + same space
        0x00, 0x00, 0x00, 0x02, // two-byte key
        0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // empty value
        0xff, // trailer tag
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // entry count
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1d, // payload bytes
        0x6f, 0xe4, 0xe5, 0x2b, 0x01, 0x62, 0xbe, 0x13, // BLAKE3(payload)
        0x21, 0xe3, 0x5b, 0x52, 0x7e, 0x64, 0x4c, 0x47, 0xae, 0xbe, 0xa2, 0x54, 0x47, 0x30, 0x50,
        0xea, 0x5e, 0x0f, 0x82, 0x8c, 0x5c, 0x02, 0xa2, 0x4d, // digest remainder
        0x4c, 0x49, 0x58, 0x45, 0x4e, 0x44, 0x00, 0x01, // LIXEND\0\x01
    ];

    struct RecordingReader<'a> {
        bytes: &'a [u8],
        offset: usize,
        largest_read: Rc<Cell<usize>>,
    }

    impl AsyncRead for RecordingReader<'_> {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buffer: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            self.largest_read
                .set(self.largest_read.get().max(buffer.len()));
            let available = &self.bytes[self.offset..];
            let read = available.len().min(buffer.len());
            buffer[..read].copy_from_slice(&available[..read]);
            self.offset += read;
            Poll::Ready(Ok(read))
        }
    }

    fn entries() -> [SnapshotEntry; 3] {
        [
            SnapshotEntry {
                space_id: 7,
                key: Bytes::from_static(b"a"),
                value: Bytes::from_static(b"A"),
            },
            SnapshotEntry {
                space_id: 7,
                key: Bytes::from_static(b"b"),
                value: Bytes::from_static(b"B"),
            },
            SnapshotEntry {
                space_id: 9,
                key: Bytes::new(),
                value: Bytes::from_static(b"value"),
            },
        ]
    }

    async fn encode(entries: &[SnapshotEntry]) -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut encoder = SnapshotEncoder::new(&mut bytes, 76).await.unwrap();
        for entry in entries {
            encoder.write_entry(entry).await.unwrap();
        }
        encoder.finish().await.unwrap();
        bytes
    }

    async fn decode(bytes: &[u8]) -> Result<Vec<SnapshotEntry>, LixError> {
        let (_, mut decoder) = SnapshotDecoder::new(bytes).await?;
        let mut entries = Vec::new();
        while let Some(entry) = decoder.next_entry().await? {
            entries.push(entry);
        }
        Ok(entries)
    }

    fn declared_entry(key_len: u32, value_len: u32) -> Vec<u8> {
        let mut bytes = EMPTY_V1_GOLDEN[..HEADER_BYTES].to_vec();
        bytes.push(ENTRY_TAG);
        bytes.extend_from_slice(&7_u32.to_be_bytes());
        bytes.extend_from_slice(&key_len.to_be_bytes());
        bytes.extend_from_slice(&value_len.to_be_bytes());
        bytes
    }

    #[tokio::test]
    async fn empty_snapshot_matches_immutable_v1_golden_vector() {
        assert_eq!(encode(&[]).await, EMPTY_V1_GOLDEN);

        let (header, mut decoder) = SnapshotDecoder::new(EMPTY_V1_GOLDEN).await.unwrap();
        assert_eq!(header.lix_format_version, 76);
        assert_eq!(decoder.next_entry().await.unwrap(), None);
        assert_eq!(
            decoder.trailer(),
            Some(SnapshotTrailer {
                entry_count: 0,
                payload_bytes: 0,
                digest: [
                    0xaf, 0x13, 0x49, 0xb9, 0xf5, 0xf9, 0xa1, 0xa6, 0xa0, 0x40, 0x4d, 0xea, 0x36,
                    0xdc, 0xc9, 0x49, 0x9b, 0xcb, 0x25, 0xc9, 0xad, 0xc1, 0x12, 0xb7, 0xcc, 0x9a,
                    0x93, 0xca, 0xe4, 0x1f, 0x32, 0x62,
                ],
            })
        );
    }

    #[tokio::test]
    async fn entries_match_immutable_v1_golden_vector() {
        let entries = [
            SnapshotEntry {
                space_id: 0x0102_0304,
                key: Bytes::new(),
                value: Bytes::from_static(&[0xaa]),
            },
            SnapshotEntry {
                space_id: 0x0102_0304,
                key: Bytes::from_static(&[0x00, 0x01]),
                value: Bytes::new(),
            },
        ];
        assert_eq!(encode(&entries).await, NON_EMPTY_V1_GOLDEN);
        assert_eq!(decode(NON_EMPTY_V1_GOLDEN).await.unwrap(), entries);
    }

    #[tokio::test]
    async fn multi_chunk_body_roundtrips() {
        let entry = SnapshotEntry {
            space_id: 7,
            key: Bytes::from_static(b"key"),
            value: Bytes::from(vec![0x5a; READ_ALLOCATION_CHUNK_BYTES * 3 + 17]),
        };
        assert_eq!(decode(&encode(std::slice::from_ref(&entry)).await).await.unwrap(), vec![entry]);
    }

    #[tokio::test]
    async fn roundtrip_is_deterministic_and_stream_framed() {
        let entries = entries();
        let first = encode(&entries).await;
        let second = encode(&entries).await;
        assert_eq!(first, second);

        let (header, mut decoder) = SnapshotDecoder::new(first.as_slice()).await.unwrap();
        assert_eq!(header.lix_format_version, 76);
        let mut decoded = Vec::new();
        while let Some(entry) = decoder.next_entry().await.unwrap() {
            decoded.push(entry);
        }
        assert_eq!(decoded, entries);
    }

    #[tokio::test]
    async fn malformed_inputs_are_rejected() {
        let valid = encode(&entries()[..1]).await;
        for truncate_at in 0..valid.len() {
            let result = async {
                let (_, mut decoder) = SnapshotDecoder::new(&valid[..truncate_at]).await?;
                while decoder.next_entry().await?.is_some() {}
                Ok::<_, LixError>(())
            }
            .await;
            assert!(result.is_err(), "accepted truncation at {truncate_at}");
        }

        let mut corrupt = valid.clone();
        corrupt[HEADER_BYTES + ENTRY_HEADER_BYTES] ^= 1;
        let (_, mut decoder) = SnapshotDecoder::new(corrupt.as_slice()).await.unwrap();
        assert!(decoder.next_entry().await.is_ok());
        assert!(decoder.next_entry().await.is_err());

        let mut trailing = valid.clone();
        trailing.push(0);
        let (_, mut decoder) = SnapshotDecoder::new(trailing.as_slice()).await.unwrap();
        assert!(decoder.next_entry().await.is_ok());
        assert!(decoder.next_entry().await.is_err());

        assert!(
            SnapshotDecoder::new(b"LIXMEM\0\x01".as_slice())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn header_validation_and_payload_only_digest_are_explicit() {
        let mut bad_magic = EMPTY_V1_GOLDEN.to_vec();
        bad_magic[0] ^= 1;
        assert!(SnapshotDecoder::new(bad_magic.as_slice()).await.is_err());

        let mut bad_version = EMPTY_V1_GOLDEN.to_vec();
        bad_version[MAGIC.len() + 1] = 2;
        assert!(SnapshotDecoder::new(bad_version.as_slice()).await.is_err());

        let mut bad_algorithm = EMPTY_V1_GOLDEN.to_vec();
        bad_algorithm[MAGIC.len() + 2] = 0xff;
        assert!(
            SnapshotDecoder::new(bad_algorithm.as_slice())
                .await
                .is_err()
        );

        let mut bad_flags = EMPTY_V1_GOLDEN.to_vec();
        bad_flags[MAGIC.len() + 3] = 1;
        assert!(SnapshotDecoder::new(bad_flags.as_slice()).await.is_err());

        // The v1 checksum contract covers the canonical entry payload, not
        // the header. Semantic restore separately checks this version against
        // the repository's format marker.
        let mut changed_lix_format = EMPTY_V1_GOLDEN.to_vec();
        changed_lix_format[HEADER_BYTES - 1] = 77;
        let (header, mut decoder) = SnapshotDecoder::new(changed_lix_format.as_slice())
            .await
            .unwrap();
        assert_eq!(header.lix_format_version, 77);
        assert_eq!(decoder.next_entry().await.unwrap(), None);
    }

    #[tokio::test]
    async fn hostile_declared_lengths_are_rejected_without_eager_allocation() {
        for bytes in [declared_entry(u32::MAX, 0), declared_entry(0, u32::MAX)] {
            let (_, mut decoder) = SnapshotDecoder::new(bytes.as_slice()).await.unwrap();
            let error = decoder.next_entry().await.unwrap_err();
            assert_eq!(error.code, LixError::CODE_INVALID_SNAPSHOT);
        }

        // A large, allowed declaration that is immediately truncated grows
        // only one bounded chunk, rather than eagerly allocating its body.
        const DECLARED_BODY_BYTES: u32 = 8 * 1024 * 1024;
        let bytes = declared_entry(0, DECLARED_BODY_BYTES);
        let largest_read = Rc::new(Cell::new(0));
        let reader = RecordingReader {
            bytes: &bytes,
            offset: 0,
            largest_read: largest_read.clone(),
        };
        let (_, mut decoder) = SnapshotDecoder::new(reader).await.unwrap();
        let error = decoder.next_entry().await.unwrap_err();
        assert_eq!(error.code, LixError::CODE_INVALID_SNAPSHOT);
        assert!(largest_read.get() <= READ_ALLOCATION_CHUNK_BYTES);
    }

    #[tokio::test]
    async fn encoder_and_decoder_enforce_total_budgets_before_entry_bodies() {
        assert!(
            validate_entry_budget(
                MAX_ENTRY_COUNT - 1,
                MAX_PAYLOAD_BYTES - ENTRY_HEADER_BYTES as u64,
                0,
                0,
            )
            .is_ok()
        );
        assert!(validate_entry_budget(MAX_ENTRY_COUNT, 0, 0, 0).is_err());
        assert!(validate_entry_budget(0, MAX_PAYLOAD_BYTES, 0, 0).is_err());

        let one_entry = encode(&entries()[..1]).await;

        let (_, mut count_limited) = SnapshotDecoder::new(one_entry.as_slice()).await.unwrap();
        count_limited.entry_count = MAX_ENTRY_COUNT;
        let error = count_limited.next_entry().await.unwrap_err();
        assert!(error.message.contains("entry count"));

        let (_, mut payload_limited) = SnapshotDecoder::new(one_entry.as_slice()).await.unwrap();
        payload_limited.payload_bytes = MAX_PAYLOAD_BYTES;
        let error = payload_limited.next_entry().await.unwrap_err();
        assert!(error.message.contains("payload"));

        let mut output = Vec::new();
        let mut encoder = SnapshotEncoder::new(&mut output, 76).await.unwrap();
        encoder.entry_count = MAX_ENTRY_COUNT;
        let error = encoder.write_entry(&entries()[0]).await.unwrap_err();
        assert!(error.message.contains("entry count"));
        assert_eq!(output.len(), HEADER_BYTES);
    }

    #[tokio::test]
    async fn versions_algorithms_order_counts_and_lengths_are_validated() {
        let valid = encode(&entries()[..2]).await;

        let mut bad_version = valid.clone();
        bad_version[MAGIC.len() + 1] = 2;
        assert!(decode(&bad_version).await.is_err());

        let mut bad_algorithm = valid.clone();
        bad_algorithm[MAGIC.len() + 2] = 0xff;
        assert!(decode(&bad_algorithm).await.is_err());

        let mut bad_flags = valid.clone();
        bad_flags[MAGIC.len() + 3] = 1;
        assert!(decode(&bad_flags).await.is_err());

        let mut unknown_tag = valid.clone();
        unknown_tag[HEADER_BYTES] = 0x7f;
        assert!(decode(&unknown_tag).await.is_err());

        let mut duplicate = valid.clone();
        let first_record_bytes = ENTRY_HEADER_BYTES + 1 + 1;
        let second_key = HEADER_BYTES + first_record_bytes + ENTRY_HEADER_BYTES;
        duplicate[second_key] = b'a';
        assert!(decode(&duplicate).await.is_err());

        let mut bad_count = valid.clone();
        let count = bad_count.len() - TRAILER_BYTES + 1;
        bad_count[count + 7] ^= 1;
        assert!(decode(&bad_count).await.is_err());

        let mut impossible_key = valid;
        let key_length = HEADER_BYTES + 1 + 4;
        impossible_key[key_length..key_length + 4].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(decode(&impossible_key).await.is_err());
    }
}
