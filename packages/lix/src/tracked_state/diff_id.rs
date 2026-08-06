use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use uuid::Uuid;

use crate::LixError;
use crate::changelog::ChangeId;

const PREFIX: &str = "d1.";
const BEFORE_PRESENT: u8 = 1;
const AFTER_PRESENT: u8 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DiffSides {
    pub(crate) before: Option<ChangeId>,
    pub(crate) after: Option<ChangeId>,
}

pub(crate) fn encode_diff_id(
    before: Option<ChangeId>,
    after: Option<ChangeId>,
) -> Result<String, LixError> {
    if before.is_none() && after.is_none() {
        return Err(invalid_diff_id("a diff must contain at least one side"));
    }
    let mut bytes = Vec::with_capacity(33);
    bytes.push(
        u8::from(before.is_some()) * BEFORE_PRESENT + u8::from(after.is_some()) * AFTER_PRESENT,
    );
    if let Some(change_id) = before {
        bytes.extend_from_slice(change_id.as_uuid().as_bytes());
    }
    if let Some(change_id) = after {
        bytes.extend_from_slice(change_id.as_uuid().as_bytes());
    }
    Ok(format!("{PREFIX}{}", URL_SAFE_NO_PAD.encode(bytes)))
}

pub(crate) fn decode_diff_id(value: &str) -> Result<DiffSides, LixError> {
    let payload = value
        .strip_prefix(PREFIX)
        .ok_or_else(|| invalid_diff_id("unsupported or missing version"))?;
    let bytes = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| invalid_diff_id("payload is not valid base64url"))?;
    let Some(flags) = bytes.first().copied() else {
        return Err(invalid_diff_id("payload is empty"));
    };
    if flags == 0 || flags & !(BEFORE_PRESENT | AFTER_PRESENT) != 0 {
        return Err(invalid_diff_id("side flags are invalid"));
    }
    let expected = 1
        + usize::from(flags & BEFORE_PRESENT != 0) * 16
        + usize::from(flags & AFTER_PRESENT != 0) * 16;
    if bytes.len() != expected {
        return Err(invalid_diff_id(
            "payload length does not match its side flags",
        ));
    }
    let mut offset = 1;
    let mut take_change_id = |present: bool| {
        if !present {
            return None;
        }
        let uuid = Uuid::from_slice(&bytes[offset..offset + 16])
            .expect("validated diff id UUID slice length");
        offset += 16;
        Some(ChangeId::new(uuid))
    };
    Ok(DiffSides {
        before: take_change_id(flags & BEFORE_PRESENT != 0),
        after: take_change_id(flags & AFTER_PRESENT != 0),
    })
}

fn invalid_diff_id(message: &str) -> LixError {
    LixError::new(
        LixError::CODE_TYPE_MISMATCH,
        format!("invalid diff_id: {message}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_all_valid_side_shapes() {
        let before = ChangeId::for_test_label("before");
        let after = ChangeId::for_test_label("after");
        for sides in [
            DiffSides {
                before: Some(before),
                after: Some(after),
            },
            DiffSides {
                before: Some(before),
                after: None,
            },
            DiffSides {
                before: None,
                after: Some(after),
            },
        ] {
            let encoded = encode_diff_id(sides.before, sides.after).expect("encode diff id");
            assert!(encoded.starts_with("d1."));
            assert_eq!(decode_diff_id(&encoded).expect("decode diff id"), sides);
        }
    }

    #[test]
    fn rejects_malformed_and_unknown_versions() {
        for value in ["", "d2.AQ", "d1.", "d1.AA", "d1.AQ"] {
            assert!(decode_diff_id(value).is_err(), "{value}");
        }
    }
}
