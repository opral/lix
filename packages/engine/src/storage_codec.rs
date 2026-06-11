use crate::LixError;

pub(crate) mod option {
    use musli::de::SequenceDecoder;
    use musli::en::SequenceEncoder;

    #[expect(clippy::ref_option)]
    pub(crate) fn encode<T, E>(value: &Option<T>, encoder: E) -> Result<(), E::Error>
    where
        T: musli::Encode<E::Mode>,
        E: musli::Encoder,
    {
        encoder.encode_pack_fn(|pack| {
            pack.push(value.is_some())?;

            if let Some(value) = value {
                pack.push(value)?;
            }

            Ok(())
        })
    }

    pub(crate) fn decode<'de, T, D>(decoder: D) -> Result<Option<T>, D::Error>
    where
        T: musli::Decode<'de, D::Mode, D::Allocator>,
        D: musli::Decoder<'de>,
    {
        decoder.decode_pack(|pack| {
            if pack.next()? {
                Ok(Some(pack.next()?))
            } else {
                Ok(None)
            }
        })
    }
}

pub(crate) mod vec_option {
    use musli::de::SequenceDecoder;
    use musli::en::SequenceEncoder;

    pub(crate) fn encode<T, E>(value: &Vec<Option<T>>, encoder: E) -> Result<(), E::Error>
    where
        T: musli::Encode<E::Mode>,
        E: musli::Encoder,
    {
        encoder.encode_sequence_fn(value.len(), |sequence| {
            for item in value {
                super::option::encode(item, sequence.encode_next()?)?;
            }

            Ok(())
        })
    }

    pub(crate) fn decode<'de, T, D>(decoder: D) -> Result<Vec<Option<T>>, D::Error>
    where
        T: musli::Decode<'de, D::Mode, D::Allocator>,
        D: musli::Decoder<'de>,
    {
        decoder.decode_sequence(|sequence| {
            let mut out = Vec::with_capacity(sequence.size_hint().or_default());

            while let Some(decoder) = sequence.try_decode_next()? {
                out.push(super::option::decode(decoder)?);
            }

            Ok(out)
        })
    }
}

/// Opportunistic UUID packing for stored id strings.
///
/// Lix-generated ids are canonical lowercase hyphenated UUIDs; as text they
/// cost 36 bytes where 16 carry the information. Each id is stored as a
/// 1-byte tag followed by either the raw 16 UUID bytes or the original text.
/// Only the exact canonical form takes the UUID arm, so decode re-hyphenates
/// byte-identically; every other string (plugin-chosen entity keys, test
/// labels) keeps its text form.
pub(crate) mod id_string {
    use musli::Context;
    use musli::de::SequenceDecoder;
    use musli::en::SequenceEncoder;

    const TAG_TEXT: u8 = 0;
    const TAG_UUID: u8 = 1;

    /// Raw 16-byte arm. The tag already implies the length, so this encodes
    /// via `encode_array` (no length prefix), like the uuid id types in
    /// `changelog::types`.
    struct UuidArm([u8; 16]);

    impl<M> musli::Encode<M> for UuidArm {
        type Encode = Self;

        fn encode<E>(&self, encoder: E) -> Result<(), E::Error>
        where
            E: musli::Encoder<Mode = M>,
        {
            encoder.encode_array(&self.0)
        }

        fn size_hint(&self) -> Option<usize> {
            Some(16)
        }

        fn as_encode(&self) -> &Self {
            self
        }
    }

    impl<'de, M, A> musli::Decode<'de, M, A> for UuidArm
    where
        A: musli::Allocator,
    {
        fn decode<D>(decoder: D) -> Result<Self, D::Error>
        where
            D: musli::Decoder<'de, Mode = M, Allocator = A>,
        {
            Ok(Self(decoder.decode_array()?))
        }
    }

    pub(crate) fn uuid_bytes_from_canonical(value: &str) -> Option<[u8; 16]> {
        let bytes = value.as_bytes();
        if bytes.len() != 36 {
            return None;
        }
        let mut out = [0u8; 16];
        let mut nibble_index = 0usize;
        for (position, &byte) in bytes.iter().enumerate() {
            if matches!(position, 8 | 13 | 18 | 23) {
                if byte != b'-' {
                    return None;
                }
                continue;
            }
            // Lowercase hex only: uppercase input would re-hyphenate
            // differently and break round-tripping.
            let nibble = match byte {
                b'0'..=b'9' => byte - b'0',
                b'a'..=b'f' => byte - b'a' + 10,
                _ => return None,
            };
            out[nibble_index / 2] |= nibble << (4 * (1 - nibble_index % 2));
            nibble_index += 1;
        }
        Some(out)
    }

    pub(crate) fn uuid_string_from_bytes(bytes: [u8; 16]) -> String {
        uuid::Uuid::from_bytes(bytes).as_hyphenated().to_string()
    }

    pub(crate) fn encode_one<E>(value: &str, pack: &mut E) -> Result<(), E::Error>
    where
        E: SequenceEncoder,
    {
        match uuid_bytes_from_canonical(value) {
            Some(bytes) => {
                pack.push(TAG_UUID)?;
                pack.push(UuidArm(bytes))
            }
            None => {
                pack.push(TAG_TEXT)?;
                pack.push(value.as_bytes())
            }
        }
    }

    pub(crate) fn decode_one<'de, D>(pack: &mut D) -> Result<String, D::Error>
    where
        D: SequenceDecoder<'de>,
    {
        let cx = pack.cx();
        let tag: u8 = pack.next()?;
        match tag {
            TAG_UUID => Ok(uuid_string_from_bytes(pack.next::<UuidArm>()?.0)),
            TAG_TEXT => {
                let bytes: Vec<u8> = pack.next()?;
                String::from_utf8(bytes).map_err(|_| cx.message("id string is not UTF-8"))
            }
            other => Err(cx.message(format_args!("unknown id string tag {other}"))),
        }
    }

}

/// [`id_string`] values behind the same bool prefix as [`option`].
pub(crate) mod option_id_string {
    use musli::de::SequenceDecoder;
    use musli::en::SequenceEncoder;

    #[expect(clippy::ref_option_ref)]
    pub(crate) fn encode<E>(value: &Option<&str>, encoder: E) -> Result<(), E::Error>
    where
        E: musli::Encoder,
    {
        encoder.encode_pack_fn(|pack| {
            pack.push(value.is_some())?;
            if let Some(value) = value {
                super::id_string::encode_one(value, pack)?;
            }
            Ok(())
        })
    }

    pub(crate) fn decode<'de, D>(decoder: D) -> Result<Option<String>, D::Error>
    where
        D: musli::Decoder<'de>,
    {
        decoder.decode_pack(|pack| {
            if pack.next()? {
                Ok(Some(super::id_string::decode_one(pack)?))
            } else {
                Ok(None)
            }
        })
    }
}

/// A length-prefixed sequence of [`id_string`] values.
pub(crate) mod id_string_seq {
    use musli::de::SequenceDecoder;
    use musli::en::SequenceEncoder;

    pub(crate) fn encode<S, E>(value: &S, encoder: E) -> Result<(), E::Error>
    where
        S: AsRef<[String]> + ?Sized,
        E: musli::Encoder,
    {
        let parts = value.as_ref();
        encoder.encode_pack_fn(|pack| {
            pack.push(parts.len())?;
            for part in parts {
                super::id_string::encode_one(part, pack)?;
            }
            Ok(())
        })
    }

    pub(crate) fn decode<'de, D>(decoder: D) -> Result<Vec<String>, D::Error>
    where
        D: musli::Decoder<'de>,
    {
        decoder.decode_pack(|pack| {
            let len: usize = pack.next()?;
            let mut out = Vec::with_capacity(len.min(4096));
            for _ in 0..len {
                out.push(super::id_string::decode_one(pack)?);
            }
            Ok(out)
        })
    }
}

pub(crate) fn encode<T>(context: &str, value: &T) -> Result<Vec<u8>, LixError>
where
    T: ?Sized + musli::Encode<musli::mode::Binary>,
{
    musli::storage::to_vec(value).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to encode {context} with musli storage: {error}"),
        )
    })
}

pub(crate) fn decode<'de, T>(context: &str, mut bytes: &'de [u8]) -> Result<T, LixError>
where
    T: musli::Decode<'de, musli::mode::Binary, musli::alloc::Global>,
{
    #[expect(clippy::needless_borrows_for_generic_args)]
    let value = musli::storage::decode(&mut bytes).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to decode {context} with musli storage: {error}"),
        )
    })?;
    if !bytes.is_empty() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "failed to decode {context} with musli storage: {} trailing bytes",
                bytes.len()
            ),
        ));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    #[derive(Debug, Eq, PartialEq, musli::Encode, musli::Decode)]
    #[musli(packed)]
    struct OptionRoundtrip<'a> {
        #[musli(with = crate::storage_codec::option)]
        value: Option<&'a str>,
        marker: &'a str,
    }

    #[derive(Debug, Eq, PartialEq, musli::Encode, musli::Decode)]
    #[musli(packed)]
    struct VecOptionRoundtrip<'a> {
        #[musli(with = crate::storage_codec::vec_option)]
        values: Vec<Option<&'a str>>,
    }

    #[test]
    fn option_none_does_not_consume_following_packed_field() {
        let value = OptionRoundtrip {
            value: None,
            marker: "after",
        };

        let bytes = super::encode("option roundtrip", &value).expect("value should encode cleanly");
        let decoded: OptionRoundtrip<'_> =
            super::decode("option roundtrip", &bytes).expect("value should decode cleanly");

        assert_eq!(decoded, value);
    }

    #[derive(Debug, Eq, PartialEq, musli::Encode)]
    #[musli(packed)]
    struct IdStringEncode<'a> {
        #[musli(with = crate::storage_codec::id_string_seq)]
        parts: &'a [String],
        #[musli(with = crate::storage_codec::option_id_string)]
        file_id: Option<&'a str>,
    }

    #[derive(Debug, Eq, PartialEq, musli::Decode)]
    #[musli(packed)]
    struct IdStringDecode {
        #[musli(with = crate::storage_codec::id_string_seq)]
        parts: Vec<String>,
        #[musli(with = crate::storage_codec::option_id_string)]
        file_id: Option<String>,
    }

    #[test]
    fn uuid_bytes_from_canonical_accepts_only_the_exact_canonical_form() {
        use super::id_string::uuid_bytes_from_canonical;
        let canonical = "019eb805-60d0-71c0-ade3-b0f0efab9d9a";
        let bytes = uuid_bytes_from_canonical(canonical).expect("canonical uuid should pack");
        assert_eq!(super::id_string::uuid_string_from_bytes(bytes), canonical);

        for rejected in [
            "019EB805-60D0-71C0-ADE3-B0F0EFAB9D9A",   // uppercase
            "019eb80560d071c0ade3b0f0efab9d9a",       // simple form
            "{019eb805-60d0-71c0-ade3-b0f0efab9d9a}", // braced
            "019eb805-60d0-71c0-ade3-b0f0efab9d9",    // short
            "019eb805-60d0-71c0-ade3-b0f0efab9d9ax",  // long
            "019eb805x60d0-71c0-ade3-b0f0efab9d9a",   // 36 bytes, hyphen replaced
            "019eb805-60d0x71c0-ade3-b0f0efab9d9a",   // 36 bytes, hex at hyphen slot
            "019eb805-60d0-71c0-ade3-b0f0efab9d\u{e9}", // 36 bytes via multi-byte char
            "019eb805-60d0-71c0-ade3-b0f0efab9d9\0",  // embedded NUL
            "------------------------------------",   // all hyphens
            "not-a-uuid",
            "",
        ] {
            assert_eq!(uuid_bytes_from_canonical(rejected), None, "{rejected:?}");
        }
    }

    #[test]
    fn id_strings_round_trip_through_both_arms() {
        let parts = vec![
            "019eb805-60d0-71c0-ade3-b0f0efab9d9a".to_string(),
            "row 5 of sheet 2".to_string(),
        ];
        let value = IdStringEncode {
            parts: &parts,
            file_id: Some("019eb805-5e65-7270-861d-cb341bc904c8"),
        };
        let bytes = super::encode("id string roundtrip", &value).expect("encode");
        let decoded: IdStringDecode =
            super::decode("id string roundtrip", &bytes).expect("decode");
        assert_eq!(decoded.parts, parts);
        assert_eq!(
            decoded.file_id.as_deref(),
            Some("019eb805-5e65-7270-861d-cb341bc904c8")
        );
    }

    #[test]
    fn id_string_wire_format_is_pinned() {
        // Persisted layout: uuid arm = tag 1 + 16 raw bytes (no length
        // prefix); text arm = tag 0 + varint length + bytes. Sequences are
        // count-prefixed; options are bool-prefixed.
        let parts = vec!["019eb805-60d0-71c0-ade3-b0f0efab9d9a".to_string()];
        let value = IdStringEncode {
            parts: &parts,
            file_id: Some("ab"),
        };
        let bytes = super::encode("id string wire pin", &value).expect("encode");
        let expected: Vec<u8> = [
            &[1u8][..],     // parts count
            &[1u8][..],     // TAG_UUID
            &[
                0x01, 0x9e, 0xb8, 0x05, 0x60, 0xd0, 0x71, 0xc0, 0xad, 0xe3, 0xb0, 0xf0, 0xef,
                0xab, 0x9d, 0x9a,
            ][..],          // raw uuid bytes
            &[1u8][..],     // file_id Some
            &[0u8][..],     // TAG_TEXT
            &[2u8][..],     // text length
            b"ab",          // text bytes
        ]
        .concat();
        assert_eq!(bytes, expected);
    }

    #[test]
    fn id_string_decode_rejects_unknown_tag_loudly() {
        let parts = vec!["a".to_string()];
        let value = IdStringEncode {
            parts: &parts,
            file_id: None,
        };
        let mut bytes = super::encode("id string tag", &value).expect("encode");
        // Layout: [count=1][tag][len=1][b'a'][file_id=false]; corrupt the tag.
        assert_eq!(bytes[1], 0, "expected the text tag at offset 1");
        bytes[1] = 9;
        let error = super::decode::<IdStringDecode>("id string tag", &bytes)
            .expect_err("unknown tag should fail decode");
        assert!(
            error.message.contains("unknown id string tag 9"),
            "{}",
            error.message
        );
    }

    #[test]
    fn id_string_decode_rejects_non_utf8_text_loudly() {
        let parts = vec!["ab".to_string()];
        let value = IdStringEncode {
            parts: &parts,
            file_id: None,
        };
        let mut bytes = super::encode("id string utf8", &value).expect("encode");
        // Layout: [count=1][tag=0][len=2][b'a'][b'b'][file_id=false].
        bytes[3] = 0xFF;
        let error = super::decode::<IdStringDecode>("id string utf8", &bytes)
            .expect_err("invalid UTF-8 should fail decode");
        assert!(
            error.message.contains("id string is not UTF-8"),
            "{}",
            error.message
        );
    }

    #[test]
    fn id_string_none_file_id_round_trips() {
        let parts = vec!["plain".to_string()];
        let value = IdStringEncode {
            parts: &parts,
            file_id: None,
        };
        let bytes = super::encode("id string roundtrip", &value).expect("encode");
        let decoded: IdStringDecode =
            super::decode("id string roundtrip", &bytes).expect("decode");
        assert_eq!(decoded.parts, parts);
        assert_eq!(decoded.file_id, None);
    }

    #[test]
    fn vec_option_roundtrips_borrowed_values() {
        let value = VecOptionRoundtrip {
            values: vec![Some("first"), None, Some("third")],
        };

        let bytes =
            super::encode("vec option roundtrip", &value).expect("value should encode cleanly");
        let decoded: VecOptionRoundtrip<'_> =
            super::decode("vec option roundtrip", &bytes).expect("value should decode cleanly");

        assert_eq!(decoded, value);
    }
}
