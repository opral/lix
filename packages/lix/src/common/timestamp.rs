use std::fmt;

use jiff::{Timestamp, fmt::temporal::DateTimeParser, tz::Offset};
use musli::{Allocator, Context, Decode, Decoder, Encode, Encoder};

const MILLIS_BITS: u32 = 52;
const OFFSET_BITS: u32 = 12;
const OFFSET_MASK: u64 = (1u64 << OFFSET_BITS) - 1;
const MAX_PACKED_MILLIS: u64 = (1u64 << MILLIS_BITS) - 1;
const MIN_OFFSET_MINUTES: i16 = -(23 * 60 + 59);
const MAX_OFFSET_MINUTES: i16 = 23 * 60 + 59;

static TIMESTAMP_PARSER: DateTimeParser = DateTimeParser::new();

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct LixTimestamp(u64);

impl LixTimestamp {
    #[expect(clippy::cast_sign_loss)]
    pub(crate) fn from_unix_millis_utc_lossy(millis: i64) -> Self {
        let millis = (millis.max(0) as u64).min(MAX_PACKED_MILLIS);
        Self::from_parts(millis, 0)
            .unwrap_or_else(|_| Self::from_parts(0, 0).expect("Unix epoch is a valid timestamp"))
    }

    pub(crate) fn parse(timestamp: &str) -> Result<Self, String> {
        let instant = TIMESTAMP_PARSER
            .parse_timestamp(timestamp)
            .map_err(|error| format!("failed to parse timestamp `{timestamp}`: {error}"))?;
        let pieces = TIMESTAMP_PARSER
            .parse_pieces(timestamp)
            .map_err(|error| format!("failed to parse timestamp `{timestamp}`: {error}"))?;
        let offset = pieces
            .to_numeric_offset()
            .ok_or_else(|| format!("timestamp `{timestamp}` must include a numeric offset"))?;
        Self::from_jiff(instant, offset)
            .map_err(|error| format!("timestamp `{timestamp}` is not supported: {error}"))
    }

    pub(crate) fn expect_parse(field: &str, timestamp: &str) -> Self {
        Self::parse(timestamp)
            .unwrap_or_else(|error| panic!("{field} must be a Lix timestamp: {error}"))
    }

    pub(crate) fn packed(self) -> u64 {
        self.0
    }

    pub(crate) fn milliseconds_since_unix_epoch(self) -> u64 {
        self.0 >> OFFSET_BITS
    }

    pub(crate) fn offset_minutes(self) -> i16 {
        decode_offset_minutes(self.0)
    }

    #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn from_jiff(timestamp: Timestamp, offset: Offset) -> Result<Self, String> {
        let offset_seconds = offset.seconds();
        if offset_seconds % 60 != 0 {
            return Err("timezone offsets must have minute precision".to_string());
        }

        // Public write schemas accept ISO-8601 timestamps. The packed form only
        // stores non-negative whole milliseconds, so parse valid timestamps
        // lossily instead of rejecting common RFC3339 values.
        let millis = timestamp.as_millisecond().max(0) as u64;
        Self::from_parts(millis, (offset_seconds / 60) as i16)
    }

    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        clippy::cast_sign_loss
    )]
    fn from_parts(millis: u64, offset_minutes: i16) -> Result<Self, String> {
        if millis > MAX_PACKED_MILLIS {
            return Err(format!(
                "milliseconds since Unix epoch must fit in {MILLIS_BITS} bits"
            ));
        }

        if !valid_offset_minutes(offset_minutes) {
            return Err(format!(
                "timezone offset minutes must be in {MIN_OFFSET_MINUTES}..={MAX_OFFSET_MINUTES}"
            ));
        }

        Timestamp::from_millisecond(millis as i64)
            .map_err(|error| format!("milliseconds are outside Jiff's display range: {error}"))?;

        let offset_bits = (i32::from(offset_minutes) & OFFSET_MASK as i32) as u64;
        Ok(Self((millis << OFFSET_BITS) | offset_bits))
    }

    #[expect(clippy::cast_possible_wrap)]
    pub(crate) fn from_packed(packed: u64) -> Result<Self, String> {
        let timestamp = Self(packed);
        if !valid_offset_minutes(timestamp.offset_minutes()) {
            return Err(format!(
                "packed timezone offset minutes must be in {MIN_OFFSET_MINUTES}..={MAX_OFFSET_MINUTES}"
            ));
        }

        Timestamp::from_millisecond(timestamp.milliseconds_since_unix_epoch() as i64).map_err(
            |error| format!("packed milliseconds are outside Jiff's display range: {error}"),
        )?;

        Ok(timestamp)
    }

    #[expect(clippy::cast_possible_wrap)]
    fn to_jiff(self) -> Timestamp {
        Timestamp::from_millisecond(self.milliseconds_since_unix_epoch() as i64)
            .expect("packed timestamp milliseconds are validated")
    }

    fn to_offset(self) -> Offset {
        Offset::from_seconds(i32::from(self.offset_minutes()) * 60)
            .expect("packed timestamp offset minutes are validated")
    }
}

impl fmt::Display for LixTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let timestamp = self.to_jiff();
        let offset = self.to_offset();
        if offset == Offset::UTC {
            write!(f, "{timestamp:.3}")
        } else {
            write!(f, "{:.3}", timestamp.display_with_offset(offset))
        }
    }
}

impl fmt::Debug for LixTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("LixTimestamp")
            .field(&self.to_string())
            .field(&self.0)
            .finish()
    }
}

impl<M> Encode<M> for LixTimestamp {
    type Encode = u64;

    fn encode<E>(&self, encoder: E) -> Result<(), E::Error>
    where
        E: Encoder<Mode = M>,
    {
        let packed = self.packed();
        encoder.encode(packed)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(size_of::<u64>())
    }

    fn as_encode(&self) -> &Self::Encode {
        &self.0
    }
}

impl<'de, M, A> Decode<'de, M, A> for LixTimestamp
where
    A: Allocator,
{
    fn decode<D>(decoder: D) -> Result<Self, D::Error>
    where
        D: Decoder<'de, Mode = M, Allocator = A>,
    {
        let cx = decoder.cx();
        let packed = u64::decode(decoder)?;
        Self::from_packed(packed).map_err(|error| cx.message(format_args!("{error}")))
    }
}

fn decode_offset_minutes(packed: u64) -> i16 {
    let raw = (packed & OFFSET_MASK) as i16;
    (raw << (i16::BITS - OFFSET_BITS)) >> (i16::BITS - OFFSET_BITS)
}

fn valid_offset_minutes(offset_minutes: i16) -> bool {
    (MIN_OFFSET_MINUTES..=MAX_OFFSET_MINUTES).contains(&offset_minutes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_codec;

    #[test]
    fn timestamp_roundtrips_utc_text() {
        let timestamp = LixTimestamp::parse("2026-05-19T00:00:00.000Z").unwrap();

        assert_eq!(timestamp.to_string(), "2026-05-19T00:00:00.000Z");
        assert_eq!(timestamp.offset_minutes(), 0);
    }

    #[test]
    fn timestamp_builds_from_utc_millis() {
        let timestamp = LixTimestamp::from_unix_millis_utc_lossy(1);
        let negative = LixTimestamp::from_unix_millis_utc_lossy(-1);

        assert_eq!(timestamp.to_string(), "1970-01-01T00:00:00.001Z");
        assert_eq!(timestamp.offset_minutes(), 0);
        assert_eq!(negative.to_string(), "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn timestamp_roundtrips_non_utc_offset_text() {
        let timestamp = LixTimestamp::parse("2026-05-19T08:30:01.234-07:30").unwrap();

        assert_eq!(timestamp.to_string(), "2026-05-19T08:30:01.234-07:30");
        assert_eq!(timestamp.offset_minutes(), -(7 * 60 + 30));
    }

    #[test]
    fn timestamp_encodes_as_packed_u64() {
        let timestamp = LixTimestamp::parse("2026-05-19T08:30:01.234-07:30").unwrap();
        let bytes = storage_codec::encode("timestamp", &timestamp).unwrap();
        let packed: u64 = storage_codec::decode("timestamp", &bytes).unwrap();
        let decoded: LixTimestamp = storage_codec::decode("timestamp", &bytes).unwrap();

        assert_eq!(packed, timestamp.packed());
        assert_eq!(decoded, timestamp);
    }

    #[test]
    fn timestamp_lossily_accepts_public_iso_values() {
        let sub_millisecond = LixTimestamp::parse("2026-05-19T00:00:00.123456789Z").unwrap();
        let pre_unix = LixTimestamp::parse("1969-12-31T23:59:59.999999Z").unwrap();
        let pre_unix_offset = LixTimestamp::parse("1969-12-31T15:59:59.999999-08:00").unwrap();

        assert_eq!(sub_millisecond.to_string(), "2026-05-19T00:00:00.123Z");
        assert_eq!(pre_unix.to_string(), "1970-01-01T00:00:00.000Z");
        assert_eq!(pre_unix_offset.milliseconds_since_unix_epoch(), 0);
        assert_eq!(pre_unix_offset.offset_minutes(), -(8 * 60));
    }

    #[test]
    fn timestamp_rejects_offsets_outside_packed_model() {
        assert!(LixTimestamp::parse("2026-05-19T00:00:00.000+00:00:01").is_err());
        assert!(LixTimestamp::parse("2026-05-19T00:00:00.000+24:00").is_err());
    }
}
