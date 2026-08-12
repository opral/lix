//! The order-preserving key byte format, shared by the two planes that address
//! rows by identity.
//!
//! The hot-head serving index (`hot_state::tracked_head`) and the tracked-state
//! tree (`tracked_state::codec`) must agree byte-for-byte about how a key
//! encodes. The head table's storage ordering *is* the visible row ordering,
//! and the tree's ordering is what makes prefix scans exact, so a divergence
//! between the two is not a bug in either plane — it is two planes disagreeing
//! about what a key means. This module is the single authority for that format;
//! neither plane may declare its own tags or writers.
//!
//! NUL bytes are escaped as `00 ff`, so no encoded part can contain the
//! terminator. Tags match `EntityPk`'s cross-type order. UUIDs use raw bytes and
//! signed integers use sign-bit-flipped big endian, so lexical byte order is
//! logical order.
//!
//! # The decoders are still duplicated
//!
//! Encoding is unified here. **Decoding is not** — there are three independent
//! implementations of this same grammar, and this module owns only the
//! primitives whose divergence would corrupt silently (the field widths, the
//! terminator predicate, and the sign flip):
//!
//! | implementation | ownership | scanner | terminator validated |
//! |---|---|---|---|
//! | `hot_state::tracked_head` | `Vec<u8>` / `String` | byte loop | by callers |
//! | `hot_state::tracked_head::hot` | `Bytes` / `SharedStr` | byte loop | by callers |
//! | `tracked_state::codec` | `Vec` / `Cow` / `Bytes` | `memchr` | inside the scanner |
//!
//! They agree today — [`tests::all_three_decoders_agree`] proves it over every
//! truncation and every single-byte mutation of a seed corpus — but they agree
//! by coincidence of maintenance, not by construction. Unifying them means
//! standardising on one scanner (the `memchr` one) behind a borrowable span so
//! all three ownership modes keep zero-copy, and mapping a structured error
//! enum to each plane's own error code and prefix. That rewrites a hot inner
//! loop, so it wants a benchmark rather than only a green gate.
//!
//! Treat the differential test as the contract until then: it fails the moment
//! a fourth divergence appears.

pub(crate) const KEY_ESCAPE: u8 = 0xff;
pub(crate) const KEY_PART_FINAL: u8 = 0x00;
pub(crate) const KEY_PART_MORE: u8 = 0x01;
pub(crate) const FILE_ID_NONE: u8 = 0x00;
pub(crate) const FILE_ID_SOME: u8 = 0x01;
pub(crate) const ENTITY_PK_CODEC_V1: u8 = 0x01;
pub(crate) const ENTITY_PK_UUID: u8 = 0x00;
pub(crate) const ENTITY_PK_INTEGER: u8 = 0x01;
pub(crate) const ENTITY_PK_STRING: u8 = 0x02;
pub(crate) const ENTITY_PK_BYTES: u8 = 0x03;

/// Byte width of a UUID entity-primary-key component.
pub(crate) const ENTITY_PK_UUID_BYTES: usize = 16;
/// Byte width of an integer entity-primary-key component.
pub(crate) const ENTITY_PK_INTEGER_BYTES: usize = 8;

/// A part terminator is either "more components follow" or "this was the last".
/// Any other byte in that position is a malformed key.
pub(crate) fn is_key_part_terminator(byte: u8) -> bool {
    matches!(byte, KEY_PART_FINAL | KEY_PART_MORE)
}

/// Signed integers are stored sign-bit-flipped big endian so that lexical byte
/// order is numeric order. These two are exact inverses and must only ever be
/// changed together, which is why they live next to each other rather than at
/// each of the six sites that used to open-code the flip.
pub(crate) fn ordered_integer_from_i64(value: i64) -> u64 {
    u64::from_be_bytes(value.to_be_bytes()) ^ (1_u64 << 63)
}

pub(crate) fn i64_from_ordered_integer(ordered: u64) -> i64 {
    i64::from_be_bytes((ordered ^ (1_u64 << 63)).to_be_bytes())
}

/// Encoding an empty primary key is deliberately not an assertion failure.
/// Non-emptiness is enforced where it can produce a real error —
/// `EntityPk::from_components` rejects it, and `decode_key` rejects the encoded
/// form — and `tracked_state::codec` relies on being able to encode one so that
/// the decoder is what reports it. (The two former copies of this writer
/// disagreed on exactly this point: the hot-head copy carried a `debug_assert`
/// the tracked-state copy did not, so they produced the same bytes under
/// different contracts. That divergence is the reason this module exists.)
pub(crate) fn write_entity_pk(out: &mut Vec<u8>, entity_pk: &crate::entity_pk::EntityPk) {
    out.push(ENTITY_PK_CODEC_V1);
    for (index, component) in entity_pk.components.iter().enumerate() {
        let terminator = if index + 1 == entity_pk.components.len() {
            KEY_PART_FINAL
        } else {
            KEY_PART_MORE
        };
        match component {
            crate::entity_pk::EntityPkComponent::Uuid(bytes) => {
                out.push(ENTITY_PK_UUID);
                out.extend_from_slice(bytes);
                out.push(terminator);
            }
            crate::entity_pk::EntityPkComponent::Integer(value) => {
                out.push(ENTITY_PK_INTEGER);
                out.extend_from_slice(&ordered_integer_from_i64(*value).to_be_bytes());
                out.push(terminator);
            }
            crate::entity_pk::EntityPkComponent::String(value) => {
                out.push(ENTITY_PK_STRING);
                write_key_bytes(out, value.as_bytes(), terminator);
            }
            crate::entity_pk::EntityPkComponent::Bytes(value) => {
                out.push(ENTITY_PK_BYTES);
                write_key_bytes(out, value, terminator);
            }
        }
    }
}

pub(crate) fn write_file_id(out: &mut Vec<u8>, file_id: Option<&str>) {
    match file_id {
        None => out.push(FILE_ID_NONE),
        Some(file_id) => {
            out.push(FILE_ID_SOME);
            write_key_string(out, file_id, KEY_PART_FINAL);
        }
    }
}

pub(crate) fn write_key_string(out: &mut Vec<u8>, value: &str, terminator: u8) {
    write_key_bytes(out, value.as_bytes(), terminator);
}

pub(crate) fn write_key_bytes(out: &mut Vec<u8>, value: &[u8], terminator: u8) {
    for &byte in value {
        if byte == KEY_PART_FINAL {
            out.extend_from_slice(&[KEY_PART_FINAL, KEY_ESCAPE]);
        } else {
            out.push(byte);
        }
    }
    out.extend_from_slice(&[KEY_PART_FINAL, terminator]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity_pk::{EntityPk, EntityPkComponent};

    fn encoded_entity_pk(components: Vec<EntityPkComponent>) -> Vec<u8> {
        let entity_pk = EntityPk::from_components(components.into_iter().collect())
            .expect("golden entity primary keys are non-empty");
        let mut out = Vec::new();
        write_entity_pk(&mut out, &entity_pk);
        out
    }


    /// The corpus the three-way differential runs over: valid encodings, every
    /// truncation of them, every single-byte mutation to a grammar-significant
    /// value, and shapes the generators cannot reach. Decode is where a
    /// divergence between the planes becomes a wrong row rather than a wrong
    /// message, so malformed input is the interesting part.
    fn differential_corpus() -> Vec<Vec<u8>> {
        let seeds = vec![
            encoded_entity_pk(vec![EntityPkComponent::String("x".into())]),
            encoded_entity_pk(vec![EntityPkComponent::String("a\0b".into())]),
            encoded_entity_pk(vec![EntityPkComponent::Integer(-1)]),
            encoded_entity_pk(vec![EntityPkComponent::Uuid([7; 16])]),
            encoded_entity_pk(vec![EntityPkComponent::Bytes(
                bytes::Bytes::from_static(&[0, 0xff]),
            )]),
            encoded_entity_pk(vec![
                EntityPkComponent::String("a".into()),
                EntityPkComponent::Integer(7),
            ]),
            encoded_entity_pk(vec![
                EntityPkComponent::Uuid([0; 16]),
                EntityPkComponent::Bytes(bytes::Bytes::from_static(&[9])),
                EntityPkComponent::String("z".into()),
            ]),
        ];

        let mut corpus = seeds.clone();
        for seed in &seeds {
            for len in 0..seed.len() {
                corpus.push(seed[..len].to_vec());
            }
            for index in 0..seed.len() {
                for replacement in [0x00u8, 0x01, 0x02, 0x03, 0x04, 0x7f, 0xfe, 0xff] {
                    if seed[index] == replacement {
                        continue;
                    }
                    let mut mutated = seed.clone();
                    mutated[index] = replacement;
                    corpus.push(mutated);
                }
            }
        }
        corpus.extend([
            vec![],
            vec![ENTITY_PK_CODEC_V1],
            vec![0x00],
            vec![ENTITY_PK_CODEC_V1, ENTITY_PK_STRING, 0x00, 0x02],
            vec![ENTITY_PK_CODEC_V1, ENTITY_PK_STRING, 0xff, 0x00, 0x00],
            vec![ENTITY_PK_CODEC_V1, ENTITY_PK_STRING, 0x00, 0xff, 0x00, 0x00],
            vec![ENTITY_PK_CODEC_V1, 0x05, 0x00, 0x00],
        ]);
        corpus
    }

    /// A rendering that compares accept/reject *and* the decoded value, without
    /// depending on any plane's error strings — which deliberately differ, since
    /// each plane keeps its own error code and prefix. Decoding and re-encoding
    /// canonicalises the value.
    fn render(decoded: Option<(EntityPk, usize)>) -> String {
        match decoded {
            None => "rejected".to_string(),
            Some((entity_pk, offset)) => {
                let mut out = Vec::new();
                write_entity_pk(&mut out, &entity_pk);
                format!(
                    "accepted off={offset} {}",
                    out.iter().map(|b| format!("{b:02x}")).collect::<String>()
                )
            }
        }
    }

    /// Three implementations of one grammar, held to each other byte for byte.
    ///
    /// This is the contract that keeps them from drifting apart. The duplication
    /// has already drifted twice — an encode-side `debug_assert` present on one
    /// plane and not the other, and `tracked_head`'s scanner accepting any
    /// non-escape byte as a terminator where `codec`'s rejects it — and both
    /// times it was harmless only by luck. A divergence in *decode* is not
    /// harmless: it is two planes reading different rows out of the same bytes.
    #[test]
    fn all_three_decoders_agree() {
        let corpus = differential_corpus();
        assert!(
            corpus.len() > 700,
            "differential corpus collapsed to {} cases",
            corpus.len()
        );
        let mut accepted = 0usize;
        for (index, input) in corpus.iter().enumerate() {
            let head = render(crate::hot_state::head_decode_entity_pk_probe(input));
            let hot = render(crate::hot_state::hot_decode_entity_pk_probe(input));
            let tree = render(crate::tracked_state::tree_decode_entity_pk_probe(input));
            assert_eq!(
                head, hot,
                "tracked_head and hot disagree on case {index}: {input:02x?}"
            );
            assert_eq!(
                head, tree,
                "tracked_head and tracked_state disagree on case {index}: {input:02x?}"
            );
            if head != "rejected" {
                accepted += 1;
            }
        }
        // Guards the corpus itself: a mutation that made every case invalid
        // would leave the test asserting three agreeing rejections forever.
        assert!(
            accepted >= 7,
            "differential corpus accepted only {accepted} cases; it is no longer exercising decode"
        );
    }

    /// The sign flip and its inverse must stay exact inverses; they are used by
    /// the encoder here and by all three decoders.
    #[test]
    fn ordered_integer_round_trips() {
        for value in [i64::MIN, i64::MIN + 1, -2, -1, 0, 1, 2, i64::MAX - 1, i64::MAX] {
            assert_eq!(i64_from_ordered_integer(ordered_integer_from_i64(value)), value);
        }
        assert_eq!(ordered_integer_from_i64(i64::MIN), 0);
        assert_eq!(ordered_integer_from_i64(0), 1_u64 << 63);
        assert_eq!(ordered_integer_from_i64(i64::MAX), u64::MAX);
    }

    #[test]
    fn key_part_terminators_are_exactly_two() {
        assert!(is_key_part_terminator(KEY_PART_FINAL));
        assert!(is_key_part_terminator(KEY_PART_MORE));
        for byte in [0x02u8, 0x03, 0x7f, KEY_ESCAPE] {
            assert!(!is_key_part_terminator(byte), "{byte:#04x} is not a terminator");
        }
    }

    /// Golden bytes. Both planes now call these writers, so byte-identity
    /// across the planes holds by construction — what still needs pinning is
    /// the format itself, because a single edit here now moves both planes at
    /// once. Every shape either plane emits is covered.
    #[test]
    fn key_part_encoding_is_byte_pinned() {
        let mut out = Vec::new();
        write_key_string(&mut out, "ab", KEY_PART_FINAL);
        assert_eq!(out, vec![b'a', b'b', 0x00, 0x00]);

        let mut out = Vec::new();
        write_key_string(&mut out, "ab", KEY_PART_MORE);
        assert_eq!(out, vec![b'a', b'b', 0x00, 0x01]);

        // A NUL inside a part is escaped so it can never be read as the
        // terminator that ends the part.
        let mut out = Vec::new();
        write_key_bytes(&mut out, b"a\0b", KEY_PART_FINAL);
        assert_eq!(out, vec![b'a', 0x00, 0xff, b'b', 0x00, 0x00]);

        let mut out = Vec::new();
        write_key_bytes(&mut out, b"", KEY_PART_FINAL);
        assert_eq!(out, vec![0x00, 0x00]);
    }

    #[test]
    fn file_id_encoding_is_byte_pinned() {
        let mut out = Vec::new();
        write_file_id(&mut out, None);
        assert_eq!(out, vec![0x00]);

        let mut out = Vec::new();
        write_file_id(&mut out, Some("f"));
        assert_eq!(out, vec![0x01, b'f', 0x00, 0x00]);
    }

    #[test]
    fn entity_pk_encoding_is_byte_pinned() {
        assert_eq!(
            encoded_entity_pk(vec![EntityPkComponent::String("x".into())]),
            vec![0x01, 0x02, b'x', 0x00, 0x00]
        );
        assert_eq!(
            encoded_entity_pk(vec![EntityPkComponent::Bytes(bytes::Bytes::from_static(&[0xaa]))]),
            vec![0x01, 0x03, 0xaa, 0x00, 0x00]
        );
        assert_eq!(
            encoded_entity_pk(vec![EntityPkComponent::Uuid([7; 16])]),
            [vec![0x01, 0x00], vec![7; 16], vec![0x00]].concat()
        );
        assert_eq!(
            encoded_entity_pk(vec![EntityPkComponent::Integer(1)]),
            vec![0x01, 0x01, 0x80, 0, 0, 0, 0, 0, 0, 0x01, 0x00]
        );

        // Non-final components carry KEY_PART_MORE, the final one
        // KEY_PART_FINAL, so a shorter key never prefixes a longer one at the
        // same component boundary.
        assert_eq!(
            encoded_entity_pk(vec![
                EntityPkComponent::String("a".into()),
                EntityPkComponent::String("b".into()),
            ]),
            vec![0x01, 0x02, b'a', 0x00, 0x01, 0x02, b'b', 0x00, 0x00]
        );
    }

    /// The reason the integer tag flips the sign bit: encoded order must be
    /// logical order, because these bytes *are* the storage ordering.
    #[test]
    fn integer_components_encode_in_signed_order() {
        let ordered = [i64::MIN, -2, -1, 0, 1, 2, i64::MAX]
            .into_iter()
            .map(|value| encoded_entity_pk(vec![EntityPkComponent::Integer(value)]))
            .collect::<Vec<_>>();
        let mut sorted = ordered.clone();
        sorted.sort();
        assert_eq!(ordered, sorted);
    }

    /// Escaping is what keeps lexical order faithful for strings that contain
    /// the terminator byte.
    #[test]
    fn escaped_strings_keep_lexical_order() {
        let ordered = ["a", "a\0", "a\0b", "ab", "b"]
            .into_iter()
            .map(|value| encoded_entity_pk(vec![EntityPkComponent::String(value.into())]))
            .collect::<Vec<_>>();
        let mut sorted = ordered.clone();
        sorted.sort();
        assert_eq!(ordered, sorted);
    }
}
