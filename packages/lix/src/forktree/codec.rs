use crate::storage::StorageError;

pub(super) type CodecResult<T> = Result<T, StorageError>;

pub(super) fn corruption(message: impl Into<String>) -> StorageError {
    StorageError::Corruption(format!("ForkTree: {}", message.into()))
}

#[derive(Debug, Default)]
pub(super) struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    pub(super) fn with_prefix(prefix: &[u8]) -> Self {
        Self {
            bytes: prefix.to_vec(),
        }
    }

    pub(super) fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    pub(super) fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub(super) fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub(super) fn fixed(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }

    pub(super) fn bytes(&mut self, value: &[u8]) -> CodecResult<()> {
        let length = u32::try_from(value.len())
            .map_err(|_| corruption("canonical byte string exceeds u32"))?;
        self.u32(length);
        self.fixed(value);
        Ok(())
    }

    pub(super) fn optional_fixed(&mut self, value: Option<&[u8]>) {
        match value {
            Some(value) => {
                self.u8(1);
                self.fixed(value);
            }
            None => self.u8(0),
        }
    }

    pub(super) fn into_vec(self) -> Vec<u8> {
        self.bytes
    }
}

#[derive(Debug)]
pub(super) struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    pub(super) fn after_prefix(bytes: &'a [u8], prefix: &[u8]) -> CodecResult<Self> {
        if !bytes.starts_with(prefix) {
            return Err(corruption("canonical magic or version is invalid"));
        }
        Ok(Self {
            bytes,
            offset: prefix.len(),
        })
    }

    pub(super) fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    pub(super) fn u8(&mut self) -> CodecResult<u8> {
        Ok(self.take(1)?[0])
    }

    pub(super) fn u32(&mut self) -> CodecResult<u32> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .expect("decoder returned exactly four bytes"),
        ))
    }

    pub(super) fn usize(&mut self, label: &str) -> CodecResult<usize> {
        usize::try_from(self.u32()?).map_err(|_| corruption(format!("{label} exceeds usize")))
    }

    pub(super) fn u64(&mut self) -> CodecResult<u64> {
        Ok(u64::from_be_bytes(
            self.take(8)?
                .try_into()
                .expect("decoder returned exactly eight bytes"),
        ))
    }

    pub(super) fn fixed<const N: usize>(&mut self) -> CodecResult<[u8; N]> {
        Ok(self
            .take(N)?
            .try_into()
            .expect("decoder returned the requested fixed width"))
    }

    pub(super) fn bytes(&mut self, label: &str) -> CodecResult<Vec<u8>> {
        let length = self.usize(label)?;
        Ok(self.take(length)?.to_vec())
    }

    pub(super) fn bytes_borrowed(&mut self, label: &str) -> CodecResult<&'a [u8]> {
        let length = self.usize(label)?;
        self.take(length)
    }

    pub(super) fn optional_fixed<const N: usize>(
        &mut self,
        label: &str,
    ) -> CodecResult<Option<[u8; N]>> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.fixed().map(Some),
            tag => Err(corruption(format!(
                "{label} has invalid optional tag {tag}"
            ))),
        }
    }

    pub(super) fn take(&mut self, length: usize) -> CodecResult<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| corruption("canonical length overflowed"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| corruption("canonical value is truncated"))?;
        self.offset = end;
        Ok(value)
    }

    pub(super) fn finish(self) -> CodecResult<()> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(corruption("canonical value has trailing bytes"))
        }
    }
}

pub(super) fn encode_authenticated(
    domain: &'static str,
    magic: &[u8],
    encode: impl FnOnce(&mut Encoder) -> CodecResult<()>,
) -> CodecResult<Vec<u8>> {
    let mut encoder = Encoder::with_prefix(magic);
    encode(&mut encoder)?;
    let mut bytes = encoder.into_vec();
    let checksum = keyed_hash(domain, &bytes);
    bytes.extend_from_slice(&checksum);
    Ok(bytes)
}

pub(super) fn authenticated_body<'a>(
    domain: &'static str,
    magic: &[u8],
    bytes: &'a [u8],
) -> CodecResult<Decoder<'a>> {
    let checksum_offset = bytes
        .len()
        .checked_sub(32)
        .ok_or_else(|| corruption("authenticated value is shorter than its checksum"))?;
    let (body, encoded_checksum) = bytes.split_at(checksum_offset);
    if keyed_hash(domain, body).as_slice() != encoded_checksum {
        return Err(corruption("authenticated value checksum mismatch"));
    }
    Decoder::after_prefix(body, magic)
}

pub(super) fn keyed_hash(domain: &'static str, bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key(domain);
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}
