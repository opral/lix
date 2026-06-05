use std::fmt;

const MIN_BYTE: u8 = 0;
const BYTE_BASE: u16 = 256;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OrderKey(Vec<u8>);

impl fmt::Debug for OrderKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("OrderKey")
            .field(&self.to_snapshot_string())
            .finish()
    }
}

impl OrderKey {
    pub fn evenly_between(
        previous: Option<&Self>,
        next: Option<&Self>,
        ids: &[String],
    ) -> Result<Vec<Self>, String> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        if let (Some(previous), Some(next)) = (previous, next) {
            if previous >= next {
                return Err(format!(
                    "order key bounds are out of order: previous={previous:?}, next={next:?}"
                ));
            }
        }

        let mut keys = Vec::with_capacity(ids.len());
        fill_evenly(previous, next, ids, &mut keys)?;
        Ok(keys)
    }

    pub fn to_snapshot_string(&self) -> String {
        encode_hex(&self.0)
    }

    pub fn from_snapshot_string(raw: &str) -> Result<Self, String> {
        let bytes = decode_hex(raw)?;
        validate_bytes(&bytes)?;
        Ok(Self(bytes))
    }

    fn between(previous: Option<&Self>, next: Option<&Self>, id: &str) -> Result<Self, String> {
        let mut bytes = midpoint(previous.map(Self::as_bytes), next.map(Self::as_bytes))?;
        let suffix = suffix_from_id(id);
        bytes.extend(suffix);
        validate_bytes(&bytes)?;
        Ok(Self(bytes))
    }

    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

fn fill_evenly(
    previous: Option<&OrderKey>,
    next: Option<&OrderKey>,
    ids: &[String],
    out: &mut Vec<OrderKey>,
) -> Result<(), String> {
    if ids.is_empty() {
        return Ok(());
    }

    let mid = ids.len() / 2;
    let key = OrderKey::between(previous, next, &ids[mid])?;
    fill_evenly(previous, Some(&key), &ids[..mid], out)?;
    out.push(key.clone());
    fill_evenly(Some(&key), next, &ids[mid + 1..], out)
}

fn validate_bytes(bytes: &[u8]) -> Result<(), String> {
    if bytes.is_empty() {
        return Err("must not be empty".to_string());
    }
    if bytes.last() == Some(&MIN_BYTE) {
        return Err("must not end with 00".to_string());
    }
    Ok(())
}

fn midpoint(previous: Option<&[u8]>, next: Option<&[u8]>) -> Result<Vec<u8>, String> {
    if let (Some(previous), Some(next)) = (previous, next) {
        if previous >= next {
            return Err(format!(
                "order key bounds are out of order: previous={previous:?}, next={next:?}"
            ));
        }
    }

    let previous = previous.unwrap_or_default();
    let next = next.unwrap_or_default();
    let mut prefix = Vec::new();
    let mut index = 0usize;

    loop {
        let previous_digit = previous
            .get(index)
            .map(|byte| u16::from(*byte))
            .unwrap_or(u16::from(MIN_BYTE));
        let next_digit = next
            .get(index)
            .map(|byte| u16::from(*byte))
            .unwrap_or(BYTE_BASE);

        if next_digit > previous_digit + 1 {
            let mid_digit = previous_digit + (next_digit - previous_digit) / 2;
            prefix.push(u8::try_from(mid_digit).expect("midpoint byte is always in range"));
            return Ok(prefix);
        }

        prefix.push(u8::try_from(previous_digit).expect("previous byte is always in range"));
        index += 1;
    }
}

fn suffix_from_id(id: &str) -> Vec<u8> {
    let mut suffix = id.as_bytes().to_vec();
    suffix.push(1);
    suffix
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn decode_hex(raw: &str) -> Result<Vec<u8>, String> {
    if raw.is_empty() {
        return Err("must not be empty".to_string());
    }
    if !raw.len().is_multiple_of(2) {
        return Err("must contain an even number of lowercase hexadecimal digits".to_string());
    }

    raw.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_digit(pair[0])
                .ok_or_else(|| "must contain only lowercase hexadecimal digits".to_string())?;
            let low = hex_digit(pair[1])
                .ok_or_else(|| "must contain only lowercase hexadecimal digits".to_string())?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(10 + byte - b'a'),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocates_ordered_keys_between_open_bounds() {
        let ids = ids(200);

        let keys = OrderKey::evenly_between(None, None, &ids).unwrap();

        assert_eq!(keys.len(), ids.len());
        assert_strictly_increasing(&keys);
        assert_serialized_order_matches(&keys);
        assert!(keys.iter().all(|key| key.to_snapshot_string().len() < 192));
    }

    #[test]
    fn repeatedly_allocates_in_the_same_gap_without_exhaustion() {
        let lower = OrderKey::from_snapshot_string("80").unwrap();
        let upper = OrderKey::from_snapshot_string("8001").unwrap();
        let mut previous = lower;
        let mut keys = Vec::new();

        for offset in 0..256 {
            let id = vec![format!("id-{offset}")];
            let key = OrderKey::evenly_between(Some(&previous), Some(&upper), &id)
                .unwrap()
                .remove(0);
            assert!(key > previous);
            assert!(key < upper);
            previous = key.clone();
            keys.push(key);
        }

        assert_strictly_increasing(&keys);
        assert_serialized_order_matches(&keys);
    }

    #[test]
    fn allocates_multiple_keys_inside_a_narrow_gap() {
        let lower = OrderKey::from_snapshot_string("80").unwrap();
        let upper = OrderKey::from_snapshot_string("8001").unwrap();
        let ids = ids(64);

        let keys = OrderKey::evenly_between(Some(&lower), Some(&upper), &ids).unwrap();

        assert_eq!(keys.len(), ids.len());
        assert!(keys.first().unwrap() > &lower);
        assert!(keys.last().unwrap() < &upper);
        assert_strictly_increasing(&keys);
        assert_serialized_order_matches(&keys);
    }

    #[test]
    fn id_suffix_disambiguates_same_bounds() {
        let left = OrderKey::evenly_between(None, None, &["id-a".to_string()])
            .unwrap()
            .remove(0);
        let right = OrderKey::evenly_between(None, None, &["id-b".to_string()])
            .unwrap()
            .remove(0);

        assert_ne!(left, right);
        assert_eq!(
            left.cmp(&right),
            left.to_snapshot_string().cmp(&right.to_snapshot_string())
        );
    }

    #[test]
    fn rejects_unusable_snapshot_strings() {
        assert!(OrderKey::from_snapshot_string("").is_err());
        assert!(OrderKey::from_snapshot_string("A").is_err());
        assert!(OrderKey::from_snapshot_string("zz").is_err());
        assert!(OrderKey::from_snapshot_string("abc").is_err());
        assert!(OrderKey::from_snapshot_string("ab00").is_err());
    }

    fn ids(count: usize) -> Vec<String> {
        (0..count).map(|offset| format!("id-{offset}")).collect()
    }

    fn assert_strictly_increasing(keys: &[OrderKey]) {
        for pair in keys.windows(2) {
            assert!(
                pair[0] < pair[1],
                "{:?} is not less than {:?}",
                pair[0],
                pair[1]
            );
        }
    }

    fn assert_serialized_order_matches(keys: &[OrderKey]) {
        for pair in keys.windows(2) {
            assert!(
                pair[0].to_snapshot_string() < pair[1].to_snapshot_string(),
                "{:?} does not serialize before {:?}",
                pair[0],
                pair[1]
            );
        }
    }
}
