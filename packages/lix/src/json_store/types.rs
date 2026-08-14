/// A snapshot or metadata payload slot in tracked-state values and change
/// records. Durable large payloads are authenticated ForkTree objects; inline
/// text is retained only for small semantic records such as branch metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum JsonSlot {
    None,
    Inline(Box<str>),
    ForkTreeObject([u8; 32]),
}

/// Inline threshold in bytes. Payloads at or under this length remain inline;
/// larger semantic payloads use authenticated ForkTree objects during
/// publication.
pub(crate) const JSON_INLINE_MAX_BYTES: usize = 1024;

impl JsonSlot {
    pub(crate) fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    pub(crate) fn is_some(&self) -> bool {
        !self.is_none()
    }

    pub(crate) fn as_ref_slot(&self) -> JsonSlotRef<'_> {
        match self {
            Self::None => JsonSlotRef::None,
            Self::Inline(json) => JsonSlotRef::Inline(json),
            Self::ForkTreeObject(object_id) => JsonSlotRef::ForkTreeObject(object_id),
        }
    }
}

/// Borrowed form of [`JsonSlot`] for zero-copy staging paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JsonSlotRef<'a> {
    None,
    Inline(&'a str),
    ForkTreeObject(&'a [u8; 32]),
}

/// Musli codec for [`JsonSlot`]. Tag 1 is rejected so the retired legacy
/// reference representation cannot re-enter the authenticated layout.
pub(crate) mod json_slot_storage {
    use musli::Context;
    use musli::de::SequenceDecoder;

    use super::JsonSlot;

    pub(crate) fn decode<'de, D>(decoder: D) -> Result<JsonSlot, D::Error>
    where
        D: musli::Decoder<'de>,
    {
        let cx = decoder.cx();
        decoder.decode_pack(|pack| {
            let tag: u8 = pack.next()?;
            match tag {
                0 => Ok(JsonSlot::None),
                1 => Err(cx.message("legacy JSON side-plane reference tag is unsupported")),
                2 => {
                    let bytes: Vec<u8> = pack.next()?;
                    String::from_utf8(bytes).map_or_else(
                        |_| Err(cx.message(format_args!("inline json payload is not UTF-8"))),
                        |json| Ok(JsonSlot::Inline(json.into_boxed_str())),
                    )
                }
                3 => {
                    let bytes: Vec<u8> = pack.next()?;
                    let object_id = <[u8; 32]>::try_from(bytes.as_slice()).map_err(|_| {
                        cx.message(format_args!("forktree json object id is not 32 bytes"))
                    })?;
                    Ok(JsonSlot::ForkTreeObject(object_id))
                }
                other => Err(cx.message(format_args!("unknown json slot tag {other}"))),
            }
        })
    }
}

/// Encode-only musli codec for borrowed [`JsonSlotRef`] fields.
pub(crate) mod json_slot_storage_ref {
    use musli::en::SequenceEncoder;

    use super::JsonSlotRef;

    pub(crate) fn encode<E>(value: &JsonSlotRef<'_>, encoder: E) -> Result<(), E::Error>
    where
        E: musli::Encoder,
    {
        encoder.encode_pack_fn(|pack| match value {
            JsonSlotRef::None => pack.push(0u8),
            JsonSlotRef::Inline(json) => {
                pack.push(2u8)?;
                pack.push(json.as_bytes())
            }
            JsonSlotRef::ForkTreeObject(object_id) => {
                pack.push(3u8)?;
                pack.push(object_id.as_slice())
            }
        })
    }
}
