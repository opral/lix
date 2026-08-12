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
                let ordered = u64::from_be_bytes(value.to_be_bytes()) ^ (1_u64 << 63);
                out.extend_from_slice(&ordered.to_be_bytes());
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
