use crate::LixError;

pub(crate) mod option {
    use musli::de::SequenceDecoder;
    use musli::en::SequenceEncoder;

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

pub(crate) fn decode<'de, T>(context: &str, bytes: &'de [u8]) -> Result<T, LixError>
where
    T: musli::Decode<'de, musli::mode::Binary, musli::alloc::Global>,
{
    let mut remaining = bytes;
    let value = musli::storage::decode(&mut remaining).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to decode {context} with musli storage: {error}"),
        )
    })?;
    if !remaining.is_empty() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "failed to decode {context} with musli storage: {} trailing bytes",
                remaining.len()
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
