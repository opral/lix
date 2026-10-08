use std::borrow::Cow;
use std::ops::{Deref, DerefMut, Range};
use std::sync::{Arc, OnceLock};

use serde_json::Value as JsonValue;

const MAX_BYTES: usize = 16 * 1024 * 1024;
const MAX_DEPTH: usize = 128;
const MAX_RENDERED_BYTES: usize = MAX_BYTES * 8;
const MAX_INTEGER_DIGITS: i64 = 131_072;
const MAX_FRACTIONAL_DIGITS: i64 = 16_383;
const MAX_EXACT_NUMBER_BYTES: usize =
    MAX_INTEGER_DIGITS as usize + MAX_FRACTIONAL_DIGITS as usize + 2;
const MAX_NUMBER_TOKEN_BYTES: usize = MAX_EXACT_NUMBER_BYTES + 21;

/// Normalizes a JSON number to Lix's exact PostgreSQL JSONB decimal spelling.
///
/// The serde_json parser owns JSON number syntax validation. This function
/// only normalizes the validated number token, preserving every supported
/// decimal digit and enforcing PostgreSQL's numeric precision and scale
/// bounds.
#[doc(hidden)]
pub fn normalize_jsonb_number(
    number: &serde_json::Number,
) -> Result<serde_json::Number, JsonbError> {
    normalize_jsonb_number_with_limit(number, MAX_EXACT_NUMBER_BYTES)
}

struct JsonbNumberParts<'a> {
    negative: bool,
    integer: &'a str,
    fraction: &'a str,
    input_digits_len: usize,
    leading_zeroes: usize,
    decimal_position: i64,
    display_length: usize,
    normalized_length: usize,
    coefficient_len: usize,
    zero: bool,
}

fn parse_jsonb_number_parts(raw: &str) -> Result<JsonbNumberParts<'_>, JsonbError> {
    let (negative, raw) = raw
        .strip_prefix('-')
        .map_or((false, raw), |raw| (true, raw));
    let exponent_index = raw.find(['e', 'E']);
    let (mantissa, exponent) = match exponent_index {
        Some(index) => (&raw[..index], &raw[index + 1..]),
        None => (raw, "0"),
    };
    if exponent_index.is_some() {
        let exponent_digits = exponent
            .strip_prefix('+')
            .or_else(|| exponent.strip_prefix('-'))
            .unwrap_or(exponent);
        if exponent_digits.is_empty() || !exponent_digits.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(JsonbError("invalid canonical JSON number"));
        }
    }
    let exponent = exponent.parse::<i64>().map_err(|_| {
        JsonbError("JSONB numeric exponent is outside PostgreSQL's supported range")
    })?;
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if integer.is_empty()
        || (integer.len() > 1 && integer.starts_with('0'))
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || (mantissa.contains('.')
            && (fraction.is_empty() || !fraction.bytes().all(|byte| byte.is_ascii_digit())))
    {
        return Err(JsonbError("invalid canonical JSON number"));
    }

    // PostgreSQL applies NUMERIC's scale limit to the input spelling before
    // insignificant zeroes are stripped.
    let input_scale = i64::try_from(fraction.len())
        .ok()
        .and_then(|scale| scale.checked_sub(exponent))
        .ok_or(JsonbError(
            "JSONB numeric exponent is outside PostgreSQL's supported range",
        ))?
        .max(0);
    if input_scale > MAX_FRACTIONAL_DIGITS {
        return Err(JsonbError(
            "JSONB number exceeds PostgreSQL numeric precision or scale limits",
        ));
    }

    let input_digits_len = integer.len().checked_add(fraction.len()).ok_or(JsonbError(
        "JSONB number exceeds PostgreSQL numeric precision or scale limits",
    ))?;
    let leading_zeroes = integer
        .bytes()
        .chain(fraction.bytes())
        .take_while(|digit| *digit == b'0')
        .count();
    if leading_zeroes == input_digits_len {
        return Ok(JsonbNumberParts {
            negative,
            integer,
            fraction,
            input_digits_len,
            leading_zeroes,
            decimal_position: 0,
            display_length: 1,
            normalized_length: 1,
            coefficient_len: 0,
            zero: true,
        });
    }

    let decimal_position = i64::try_from(integer.len())
        .ok()
        .and_then(|position| position.checked_add(exponent))
        .and_then(|position| position.checked_sub(i64::try_from(leading_zeroes).ok()?))
        .ok_or(JsonbError(
            "JSONB numeric exponent is outside PostgreSQL's supported range",
        ))?;

    let integer_digits = decimal_position.max(0);
    let significant_digits_len = input_digits_len - leading_zeroes;
    let fractional_digits = i64::try_from(significant_digits_len)
        .ok()
        .and_then(|length| length.checked_sub(decimal_position))
        .ok_or(JsonbError(
            "JSONB numeric exponent is outside PostgreSQL's supported range",
        ))?
        .max(0);
    if integer_digits > MAX_INTEGER_DIGITS || fractional_digits > MAX_FRACTIONAL_DIGITS {
        return Err(JsonbError(
            "JSONB number exceeds PostgreSQL numeric precision or scale limits",
        ));
    }

    let sign_length: usize = usize::from(negative);
    let integer_digits_usize = usize::try_from(integer_digits).map_err(|_| {
        JsonbError("JSONB number exceeds PostgreSQL numeric precision or scale limits")
    })?;
    let fractional_digits_usize = usize::try_from(fractional_digits).map_err(|_| {
        JsonbError("JSONB number exceeds PostgreSQL numeric precision or scale limits")
    })?;
    let display_length = if decimal_position <= 0 {
        sign_length
            .checked_add(2)
            .and_then(|length| length.checked_add(fractional_digits_usize))
    } else {
        sign_length
            .checked_add(integer_digits_usize)
            .and_then(|length| {
                if fractional_digits > 0 {
                    length.checked_add(1)?.checked_add(fractional_digits_usize)
                } else {
                    Some(length)
                }
            })
    }
    .ok_or(JsonbError(
        "JSONB number exceeds PostgreSQL numeric precision or scale limits",
    ))?;
    let trailing_fractional_zeroes = if fractional_digits > 0 {
        fraction
            .bytes()
            .rev()
            .chain(integer.bytes().rev())
            .take_while(|digit| *digit == b'0')
            .count()
            .min(fractional_digits_usize)
    } else {
        0
    };
    let normalized_length = display_length
        .checked_sub(trailing_fractional_zeroes)
        .and_then(|length| {
            (trailing_fractional_zeroes != fractional_digits_usize)
                .then_some(length)
                .or_else(|| length.checked_sub(1))
        })
        .ok_or(JsonbError(
            "JSONB number exceeds PostgreSQL numeric precision or scale limits",
        ))?;
    let trailing_zeroes = fraction
        .bytes()
        .rev()
        .chain(integer.bytes().rev())
        .take_while(|digit| *digit == b'0')
        .count();

    Ok(JsonbNumberParts {
        negative,
        integer,
        fraction,
        input_digits_len,
        leading_zeroes,
        decimal_position,
        display_length,
        normalized_length,
        coefficient_len: significant_digits_len.saturating_sub(trailing_zeroes),
        zero: false,
    })
}

fn normalize_jsonb_number_with_limit(
    number: &serde_json::Number,
    max_output_bytes: usize,
) -> Result<serde_json::Number, JsonbError> {
    let parts = parse_jsonb_number_parts(number.as_str())?;
    if parts.zero {
        return Ok(serde_json::Number::from_string_unchecked("0".to_owned()));
    }
    if parts.normalized_length > max_output_bytes {
        return Err(JsonbError("JSONB SQL equality key is too large"));
    }
    let mut digits = String::with_capacity(parts.input_digits_len);
    digits.push_str(parts.integer);
    digits.push_str(parts.fraction);
    digits.drain(..parts.leading_zeroes);
    let mut canonical = String::with_capacity(parts.display_length);
    if parts.negative {
        canonical.push('-');
    }
    if parts.decimal_position <= 0 {
        canonical.push_str("0.");
        canonical.extend(std::iter::repeat('0').take((-parts.decimal_position) as usize));
        canonical.push_str(&digits);
    } else if parts.decimal_position >= i64::try_from(digits.len()).unwrap_or(i64::MAX) {
        canonical.push_str(&digits);
        let integer_zeroes =
            parts.decimal_position - i64::try_from(digits.len()).unwrap_or(i64::MAX);
        canonical.extend(std::iter::repeat('0').take(integer_zeroes as usize));
    } else {
        let split = parts.decimal_position as usize;
        canonical.push_str(&digits[..split]);
        canonical.push('.');
        canonical.push_str(&digits[split..]);
    }

    if let Some(decimal_point) = canonical.find('.') {
        while canonical.ends_with('0') {
            canonical.pop();
        }
        if canonical.len() == decimal_point + 1 {
            canonical.pop();
        }
    }

    if canonical.len() > MAX_EXACT_NUMBER_BYTES {
        return Err(JsonbError(
            "JSONB number exceeds PostgreSQL numeric precision or scale limits",
        ));
    }
    Ok(serde_json::Number::from_string_unchecked(canonical))
}

/// Appends the semantic SQL comparison key for canonical JSON text.
///
/// JSONB's storage renderer retains legacy Ryu spellings for tag-5 floats so
/// old typed rows keep their byte identity. SQL comparison and hash operations
/// need one spelling for each exact numeric value, so this bounded lexical
/// pass rewrites only number tokens and leaves strings and object layout
/// untouched. Callers must supply compact, valid JSON with canonical object
/// key order; normal SQL casts establish that form before reaching this API.
/// On failure, `output` is left unchanged.
#[doc(hidden)]
pub fn append_jsonb_equality_key(
    output: &mut Vec<u8>,
    canonical_json: &str,
) -> Result<(), JsonbError> {
    let input = canonical_json.as_bytes();
    if input.len() > MAX_RENDERED_BYTES {
        return Err(JsonbError("JSONB SQL equality key is too large"));
    }
    let output_start = output.len();
    let mut offset = 0;
    let mut span_start = 0;
    let mut depth = 0usize;
    let result = (|| {
        while offset < input.len() {
            match input[offset] {
                b'"' => {
                    let start = offset;
                    offset = canonical_string_end(input, offset)
                        .ok_or(JsonbError("invalid canonical JSON string"))?;
                    if canonical_string_has_nul(&input[start..offset]) {
                        return Err(JsonbError(
                            "PostgreSQL JSONB does not support the Unicode NUL escape (\\u0000)",
                        ));
                    }
                }
                byte if is_json_number_start(byte) => {
                    let end = json_number_end(input, offset);
                    let token = &input[offset..end];
                    // Most stored exact numbers already use a shortest plain
                    // spelling. Keep them inside the current input span so a
                    // whole array/object is copied in one bulk append instead
                    // of parsing and appending once per number.
                    if !is_canonical_shortest_plain_number(token) {
                        append_equality_key_bytes(
                            output,
                            &input[span_start..offset],
                            output_start,
                        )?;
                        append_jsonb_number_equality_key(output, token, output_start)?;
                        span_start = end;
                    }
                    offset = end;
                }
                b'[' | b'{' => {
                    depth = depth
                        .checked_add(1)
                        .filter(|depth| *depth <= MAX_DEPTH)
                        .ok_or(JsonbError("JSONB nesting is too deep"))?;
                    offset += 1;
                }
                b']' | b'}' => {
                    depth = depth
                        .checked_sub(1)
                        .ok_or(JsonbError("invalid canonical JSON structure"))?;
                    offset += 1;
                }
                _ => offset += 1,
            }
        }
        if depth != 0 {
            return Err(JsonbError("invalid canonical JSON structure"));
        }
        append_equality_key_bytes(output, &input[span_start..], output_start)
    })();
    if result.is_err() {
        output.truncate(output_start);
    }
    result
}

/// Recognizes plain decimal tokens that are already the deterministic SQL
/// key spelling. The token is validated while scanning it; exponent forms,
/// zero aliases, trailing-zero aliases, and plain zero-fraction spellings
/// that lose to scientific notation fall through to the exact formatter.
fn is_canonical_shortest_plain_number(token: &[u8]) -> bool {
    if token.len() > MAX_NUMBER_TOKEN_BYTES {
        return false;
    }
    let (negative, digits) = token
        .strip_prefix(b"-")
        .map_or((false, token), |digits| (true, digits));
    if digits.is_empty() {
        return false;
    }

    let mut decimal_point = None;
    let mut first_fraction_nonzero = None;
    for (index, byte) in digits.iter().copied().enumerate() {
        match byte {
            b'0'..=b'9' => {
                if let Some(point) = decimal_point
                    && index > point
                    && first_fraction_nonzero.is_none()
                    && byte != b'0'
                {
                    first_fraction_nonzero = Some(index - point - 1);
                }
            }
            b'.' if decimal_point.is_none() => decimal_point = Some(index),
            // Exponents and malformed numeric syntax use the checked slow
            // path, which retains the PostgreSQL range/error behavior.
            _ => return false,
        }
    }

    let integer_end = decimal_point.unwrap_or(digits.len());
    let integer = &digits[..integer_end];
    let fraction = decimal_point.map_or(&[][..], |point| &digits[point + 1..]);
    if integer.is_empty()
        || (decimal_point.is_some() && fraction.is_empty())
        || (integer.len() > 1 && integer[0] == b'0')
        || fraction.len() > MAX_FRACTIONAL_DIGITS as usize
    {
        return false;
    }

    let integer_is_zero = integer == b"0";
    if !integer_is_zero && integer.len() > MAX_INTEGER_DIGITS as usize {
        return false;
    }

    if !integer_is_zero {
        if fraction.last().or_else(|| integer.last()) == Some(&b'0') {
            return false;
        }
        // With a nonzero integer part and no trailing coefficient zero, a
        // scientific spelling keeps every digit and adds an exponent, so
        // plain notation is always shorter.
        return true;
    }
    if fraction.is_empty() {
        // Positive zero is already canonical; negative zero is rewritten to
        // the single JSONB zero spelling.
        return !negative;
    }
    let Some(first_nonzero) = first_fraction_nonzero else {
        return false;
    };

    // For 0.xxx, compare the plain spelling with its scientific candidate.
    // Ties retain plain form. This avoids the decimal-position/expanded-form
    // work for common values such as 0.1 and 0.01.
    let coefficient_len = fraction.len() - first_nonzero;
    let exponent = -i64::try_from(first_nonzero + 1).unwrap_or(i64::MAX);
    let scientific_len = usize::from(negative)
        .checked_add(coefficient_len)
        .and_then(|length| length.checked_add(usize::from(coefficient_len > 1)))
        .and_then(|length| length.checked_add(1)) // e
        .and_then(|length| length.checked_add(i64_decimal_len(exponent)));
    scientific_len.is_some_and(|length| token.len() <= length)
}

fn append_jsonb_number_equality_key(
    output: &mut Vec<u8>,
    token: &[u8],
    output_start: usize,
) -> Result<(), JsonbError> {
    if token.len() > MAX_NUMBER_TOKEN_BYTES {
        return Err(JsonbError(
            "JSONB number exceeds PostgreSQL numeric precision or scale limits",
        ));
    }
    let raw =
        std::str::from_utf8(token).map_err(|_| JsonbError("invalid canonical JSON number"))?;
    let parts = parse_jsonb_number_parts(raw)?;
    if parts.zero {
        return append_equality_key_bytes(output, b"0", output_start);
    }

    let sign_length = usize::from(parts.negative);
    let coefficient_len = parts.coefficient_len;
    let decimal_position = parts.decimal_position;
    let plain_length = if decimal_position <= 0 {
        sign_length
            .checked_add(2)
            .and_then(|length| {
                length.checked_add(usize::try_from(decimal_position.checked_neg()?).ok()?)
            })
            .and_then(|length| length.checked_add(coefficient_len))
    } else if decimal_position >= i64::try_from(coefficient_len).unwrap_or(i64::MAX) {
        usize::try_from(decimal_position)
            .ok()
            .and_then(|position| sign_length.checked_add(position))
    } else {
        sign_length
            .checked_add(coefficient_len)
            .and_then(|length| length.checked_add(1))
    }
    .ok_or(JsonbError(
        "JSONB number exceeds PostgreSQL numeric precision or scale limits",
    ))?;
    let scientific_exponent = decimal_position.checked_sub(1).ok_or(JsonbError(
        "JSONB numeric exponent is outside PostgreSQL's supported range",
    ))?;
    let scientific_length = sign_length
        .checked_add(coefficient_len)
        .and_then(|length| length.checked_add(usize::from(coefficient_len > 1)))
        .and_then(|length| length.checked_add(1)) // e
        .and_then(|length| length.checked_add(i64_decimal_len(scientific_exponent)))
        .ok_or(JsonbError(
            "JSONB number exceeds PostgreSQL numeric precision or scale limits",
        ))?;
    let use_scientific = scientific_length < plain_length;
    let key_length = if use_scientific {
        scientific_length
    } else {
        plain_length
    };

    let used = output
        .len()
        .checked_sub(output_start)
        .ok_or(JsonbError("JSONB SQL equality key is too large"))?;
    let new_length = used
        .checked_add(key_length)
        .ok_or(JsonbError("JSONB SQL equality key is too large"))?;
    if new_length > MAX_RENDERED_BYTES {
        return Err(JsonbError("JSONB SQL equality key is too large"));
    }

    if (use_scientific && canonical_scientific_token_matches(token, &parts, scientific_exponent))
        || (!use_scientific && canonical_plain_token_matches(token, &parts))
    {
        return append_equality_key_bytes(output, token, output_start);
    }

    output
        .try_reserve(key_length)
        .map_err(|_| JsonbError("JSONB SQL equality key is too large"))?;
    if use_scientific {
        append_compact_scientific_number(output, &parts, scientific_exponent);
    } else {
        append_compact_plain_number(output, &parts);
    }
    Ok(())
}

fn canonical_plain_token_matches(token: &[u8], parts: &JsonbNumberParts<'_>) -> bool {
    if token.contains(&b'e') || token.contains(&b'E') {
        return false;
    }
    if parts.fraction.ends_with('0') {
        return false;
    }
    // Without an exponent, valid JSON's integer/fraction spelling is already
    // the normalized plain decimal unless fractional zeroes or signed zero
    // need rewriting.
    !parts.zero
}

fn canonical_scientific_token_matches(
    token: &[u8],
    parts: &JsonbNumberParts<'_>,
    scientific_exponent: i64,
) -> bool {
    if !token.contains(&b'e') || token.contains(&b'E') {
        return false;
    }
    let Some(exponent_index) = token.iter().position(|byte| *byte == b'e') else {
        return false;
    };
    let mut mantissa = &token[..exponent_index];
    if parts.negative {
        let Some(unsigned) = mantissa.strip_prefix(b"-") else {
            return false;
        };
        mantissa = unsigned;
    }
    let Some(exponent_text) = token.get(exponent_index + 1..) else {
        return false;
    };
    if !i64_matches_ascii(scientific_exponent, exponent_text) {
        return false;
    }
    let mut expected = parts
        .integer
        .bytes()
        .chain(parts.fraction.bytes())
        .skip(parts.leading_zeroes)
        .take(parts.coefficient_len);
    let Some(first) = expected.next() else {
        return false;
    };
    if mantissa.first() != Some(&first) {
        return false;
    }
    let mut offset = 1;
    if parts.coefficient_len > 1 {
        if mantissa.get(offset) != Some(&b'.') {
            return false;
        }
        offset += 1;
        for digit in expected {
            if mantissa.get(offset) != Some(&digit) {
                return false;
            }
            offset += 1;
        }
    }
    offset == mantissa.len()
}

fn i64_decimal_len(value: i64) -> usize {
    let mut magnitude = value.unsigned_abs();
    let mut length = usize::from(value < 0);
    loop {
        length += 1;
        magnitude /= 10;
        if magnitude == 0 {
            return length;
        }
    }
}

fn i64_matches_ascii(value: i64, bytes: &[u8]) -> bool {
    let mut buffer = [0u8; 20];
    let mut offset = buffer.len();
    let mut magnitude = value.unsigned_abs();
    loop {
        offset -= 1;
        buffer[offset] = b'0' + u8::try_from(magnitude % 10).unwrap_or(0);
        magnitude /= 10;
        if magnitude == 0 {
            break;
        }
    }
    if value < 0 {
        offset -= 1;
        buffer[offset] = b'-';
    }
    &buffer[offset..] == bytes
}

fn append_compact_plain_number(output: &mut Vec<u8>, parts: &JsonbNumberParts<'_>) {
    if parts.negative {
        output.push(b'-');
    }
    let mut digits = parts
        .integer
        .bytes()
        .chain(parts.fraction.bytes())
        .skip(parts.leading_zeroes)
        .take(parts.coefficient_len);
    if parts.decimal_position <= 0 {
        output.extend_from_slice(b"0.");
        let zeroes = usize::try_from(parts.decimal_position.saturating_abs()).unwrap_or(0);
        output.resize(output.len() + zeroes, b'0');
        output.extend(digits);
    } else if parts.decimal_position >= i64::try_from(parts.coefficient_len).unwrap_or(i64::MAX) {
        output.extend(digits);
        let zeroes = usize::try_from(parts.decimal_position)
            .unwrap_or(0)
            .saturating_sub(parts.coefficient_len);
        output.resize(output.len() + zeroes, b'0');
    } else {
        let integer_digits = usize::try_from(parts.decimal_position).unwrap_or(0);
        output.extend(digits.by_ref().take(integer_digits));
        output.push(b'.');
        output.extend(digits);
    }
}

fn append_compact_scientific_number(
    output: &mut Vec<u8>,
    parts: &JsonbNumberParts<'_>,
    exponent: i64,
) {
    if parts.negative {
        output.push(b'-');
    }
    let mut digits = parts
        .integer
        .bytes()
        .chain(parts.fraction.bytes())
        .skip(parts.leading_zeroes)
        .take(parts.coefficient_len);
    if let Some(first) = digits.next() {
        output.push(first);
    }
    if parts.coefficient_len > 1 {
        output.push(b'.');
        output.extend(digits);
    }
    output.push(b'e');
    append_i64_ascii(output, exponent);
}

fn append_i64_ascii(output: &mut Vec<u8>, value: i64) {
    let mut buffer = [0u8; 20];
    let mut offset = buffer.len();
    let mut magnitude = value.unsigned_abs();
    loop {
        offset -= 1;
        buffer[offset] = b'0' + u8::try_from(magnitude % 10).unwrap_or(0);
        magnitude /= 10;
        if magnitude == 0 {
            break;
        }
    }
    if value < 0 {
        offset -= 1;
        buffer[offset] = b'-';
    }
    output.extend_from_slice(&buffer[offset..]);
}

fn canonical_string_has_nul(string: &[u8]) -> bool {
    let mut offset = 1;
    while offset + 1 < string.len() {
        if string[offset] != b'\\' {
            offset += 1;
            continue;
        }
        match string.get(offset + 1) {
            Some(b'u') => {
                if string.get(offset + 2..offset + 6) == Some(b"0000") {
                    return true;
                }
                offset += 6;
            }
            Some(_) => offset += 2,
            None => return false,
        }
    }
    false
}

/// Returns the semantic SQL comparison key for canonical JSON text.
#[doc(hidden)]
pub fn jsonb_equality_key(canonical_json: &str) -> Result<String, JsonbError> {
    let mut output = Vec::new();
    append_jsonb_equality_key(&mut output, canonical_json)?;
    // Copied spans are whole slices of the valid UTF-8 input, and normalized
    // number tokens contain ASCII only.
    Ok(unsafe { String::from_utf8_unchecked(output) })
}

fn append_equality_key_bytes(
    output: &mut Vec<u8>,
    bytes: &[u8],
    output_start: usize,
) -> Result<(), JsonbError> {
    let output_len = output
        .len()
        .checked_sub(output_start)
        .and_then(|length| length.checked_add(bytes.len()))
        .ok_or(JsonbError("JSONB SQL equality key is too large"))?;
    if output_len > MAX_RENDERED_BYTES {
        return Err(JsonbError("JSONB SQL equality key is too large"));
    }
    output
        .try_reserve(bytes.len())
        .map_err(|_| JsonbError("JSONB SQL equality key is too large"))?;
    output.extend_from_slice(bytes);
    Ok(())
}

/// A native JSONB value.
///
/// Component ingress retains the validated canonical binary representation
/// and materializes a JSON DOM only when a consumer inspects it. Values built
/// by plugins remain ordinary owned JSON until their first wire encoding.
#[derive(Debug, Clone)]
pub struct Jsonb(JsonbRepr);

#[derive(Debug, Clone)]
enum JsonbRepr {
    Value(JsonValue),
    Binary(BinaryJsonb),
    CanonicalText(CanonicalTextJsonb),
    TextArray(TextArrayJsonb),
}

#[derive(Debug, Clone)]
struct TextArrayJsonb {
    values: Vec<String>,
    binary_len: usize,
    value: OnceLock<JsonValue>,
}

#[derive(Debug, Clone)]
struct BinaryJsonb {
    bytes: BinaryBytes,
    value: OnceLock<JsonValue>,
}

#[derive(Debug, Clone)]
struct CanonicalTextJsonb {
    bytes: BinaryBytes,
    reformat_numbers: bool,
    value: OnceLock<JsonValue>,
    binary: OnceLock<Result<Vec<u8>, JsonbError>>,
}

#[derive(Debug, Clone)]
enum BinaryBytes {
    Shared(Arc<[u8]>),
    SharedSlice {
        bytes: Arc<[u8]>,
        range: Range<usize>,
    },
    SharedVecSlice {
        bytes: Arc<Vec<u8>>,
        range: Range<usize>,
    },
    Owned(Vec<u8>),
}

impl Deref for BinaryBytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Shared(bytes) => bytes,
            Self::SharedSlice { bytes, range } => &bytes[range.clone()],
            Self::SharedVecSlice { bytes, range } => &bytes[range.clone()],
            Self::Owned(bytes) => bytes,
        }
    }
}

impl AsRef<[u8]> for BinaryBytes {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl PartialEq for BinaryBytes {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JsonbError(pub &'static str);

impl std::fmt::Display for JsonbError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for JsonbError {}

impl Jsonb {
    pub fn from_value(value: JsonValue) -> Self {
        Self(JsonbRepr::Value(value))
    }

    /// Builds the canonical binary representation of a JSON string array
    /// directly, consuming the strings without first allocating a JSON DOM.
    pub fn from_text_array(values: Vec<String>) -> Result<Self, JsonbError> {
        let size = values.iter().try_fold(5usize, |size, value| {
            if value.contains('\0') {
                return Err(JsonbError("JSONB string contains a NUL"));
            }
            size.checked_add(5 + value.len())
                .ok_or(JsonbError("JSONB array is too large"))
        })?;
        u32::try_from(values.len()).map_err(|_| JsonbError("JSONB array is too large"))?;
        if size > MAX_BYTES {
            return Err(JsonbError("JSONB value is too large"));
        }
        Ok(Self(JsonbRepr::TextArray(TextArrayJsonb {
            values,
            binary_len: size,
            value: OnceLock::new(),
        })))
    }

    /// Validates and retains one canonical binary JSONB value without
    /// constructing a DOM.
    pub fn from_binary(bytes: Arc<[u8]>) -> Result<Self, JsonbError> {
        validate_binary(&bytes)?;
        Ok(Self(JsonbRepr::Binary(BinaryJsonb {
            bytes: BinaryBytes::Shared(bytes),
            value: OnceLock::new(),
        })))
    }

    /// Retains a validated JSONB value as a range of a shared typed page.
    ///
    /// This is intentionally hidden from the public schema model: it lets the
    /// component decoder keep one page allocation instead of copying every
    /// inline JSONB cell into a separate allocation.
    #[doc(hidden)]
    pub fn from_binary_slice(bytes: Arc<[u8]>, range: Range<usize>) -> Result<Self, JsonbError> {
        let value = bytes
            .get(range.clone())
            .ok_or(JsonbError("JSONB shared range is out of bounds"))?;
        validate_binary(value)?;
        Ok(Self(JsonbRepr::Binary(BinaryJsonb {
            bytes: BinaryBytes::SharedSlice { bytes, range },
            value: OnceLock::new(),
        })))
    }

    /// Retains a validated range of an owned typed-page buffer without
    /// converting that `Vec` into a second page allocation.
    #[doc(hidden)]
    pub fn from_binary_vec_slice(
        bytes: Arc<Vec<u8>>,
        range: Range<usize>,
    ) -> Result<Self, JsonbError> {
        let value = bytes
            .get(range.clone())
            .ok_or(JsonbError("JSONB shared range is out of bounds"))?;
        validate_binary(value)?;
        Ok(Self(JsonbRepr::Binary(BinaryJsonb {
            bytes: BinaryBytes::SharedVecSlice { bytes, range },
            value: OnceLock::new(),
        })))
    }

    /// Validates and consumes canonical binary JSONB bytes without copying the
    /// component attachment allocation.
    pub fn from_binary_vec(bytes: Vec<u8>) -> Result<Self, JsonbError> {
        validate_binary(&bytes)?;
        Ok(Self(JsonbRepr::Binary(BinaryJsonb {
            bytes: BinaryBytes::Owned(bytes),
            value: OnceLock::new(),
        })))
    }

    /// Retains canonical JSON text as a range of an owned typed-page buffer.
    /// Consumers that only forward canonical JSON avoid constructing a DOM.
    #[doc(hidden)]
    pub fn from_canonical_text_vec_slice(
        bytes: Arc<Vec<u8>>,
        range: Range<usize>,
    ) -> Result<Self, JsonbError> {
        let value = bytes
            .get(range.clone())
            .ok_or(JsonbError("JSONB shared range is out of bounds"))?;
        let (_, reformat_numbers) = validate_canonical_json_text_with_number_rendering(value)?;
        Ok(Self(JsonbRepr::CanonicalText(CanonicalTextJsonb {
            bytes: BinaryBytes::SharedVecSlice { bytes, range },
            reformat_numbers,
            value: OnceLock::new(),
            binary: OnceLock::new(),
        })))
    }

    /// Validates and consumes canonical JSON text without materializing a DOM.
    #[doc(hidden)]
    pub fn from_canonical_text_vec(bytes: Vec<u8>) -> Result<Self, JsonbError> {
        let (_, reformat_numbers) = validate_canonical_json_text_with_number_rendering(&bytes)?;
        Ok(Self(JsonbRepr::CanonicalText(CanonicalTextJsonb {
            bytes: BinaryBytes::Owned(bytes),
            reformat_numbers,
            value: OnceLock::new(),
            binary: OnceLock::new(),
        })))
    }

    pub fn as_value(&self) -> &JsonValue {
        match &self.0 {
            JsonbRepr::Value(value) => value,
            JsonbRepr::Binary(binary) => binary
                .value
                .get_or_init(|| decode_binary(&binary.bytes).expect("validated JSONB decodes")),
            JsonbRepr::CanonicalText(text) => text.value.get_or_init(|| {
                deserialize_validated_json_text(&text.bytes)
                    .expect("validated canonical JSON decodes")
            }),
            JsonbRepr::TextArray(array) => array.value.get_or_init(|| {
                JsonValue::Array(
                    array
                        .values
                        .iter()
                        .cloned()
                        .map(JsonValue::String)
                        .collect(),
                )
            }),
        }
    }

    pub fn into_value(self) -> JsonValue {
        match self.0 {
            JsonbRepr::Value(value) => value,
            JsonbRepr::Binary(binary) => binary
                .value
                .into_inner()
                .unwrap_or_else(|| decode_binary(&binary.bytes).expect("validated JSONB decodes")),
            JsonbRepr::CanonicalText(text) => text.value.into_inner().unwrap_or_else(|| {
                deserialize_validated_json_text(&text.bytes)
                    .expect("validated canonical JSON decodes")
            }),
            JsonbRepr::TextArray(array) => array.value.into_inner().unwrap_or_else(|| {
                JsonValue::Array(array.values.into_iter().map(JsonValue::String).collect())
            }),
        }
    }

    /// Deserializes this JSONB value directly into a typed value without
    /// materializing an intermediate JSON DOM.
    ///
    /// Canonical text is borrowed by serde_json. Binary values are read
    /// directly from their canonical tags; text arrays use a capped
    /// incremental writer before parsing.
    pub fn deserialize_into<T>(&self) -> Result<T, JsonbError>
    where
        T: serde::de::DeserializeOwned,
    {
        match &self.0 {
            JsonbRepr::Value(value) => <T as serde::Deserialize>::deserialize(value)
                .map_err(|_| JsonbError("JSONB value does not match the requested type")),
            JsonbRepr::CanonicalText(text) => deserialize_validated_json_text(&text.bytes),
            JsonbRepr::Binary(binary) => deserialize_binary_into(&binary.bytes),
            JsonbRepr::TextArray(_) => {
                let bytes = self.render_canonical_json_bounded()?;
                serde_json::from_slice(&bytes)
                    .map_err(|_| JsonbError("JSONB value does not match the requested type"))
            }
        }
    }

    fn render_canonical_json_bounded(&self) -> Result<Vec<u8>, JsonbError> {
        match &self.0 {
            JsonbRepr::Binary(_) | JsonbRepr::TextArray(_) => {}
            JsonbRepr::Value(_) | JsonbRepr::CanonicalText(_) => {
                return Err(JsonbError("JSONB representation does not need rendering"));
            }
        }
        // Legacy f64 tags keep their compact Ryu spelling, while new exact
        // number tags store their full canonical decimal. String escaping is
        // the largest expansion, so the 8x envelope-wide cap remains ample.
        let mut bytes = Vec::new();
        let mut output = CappedJsonOutput {
            bytes: &mut bytes,
            max_bytes: MAX_RENDERED_BYTES,
        };
        match &self.0 {
            JsonbRepr::Binary(binary) => {
                let mut reader = Reader {
                    bytes: &binary.bytes,
                    offset: 0,
                };
                reader.write_json(&mut output, 0)?;
            }
            JsonbRepr::TextArray(array) => {
                append_text_array_json(&mut output, &array.values)?;
            }
            JsonbRepr::Value(_) | JsonbRepr::CanonicalText(_) => unreachable!(
                "representations needing no rendering returned before allocating output"
            ),
        }
        Ok(bytes)
    }

    pub fn as_value_mut(&mut self) -> &mut JsonValue {
        if !matches!(self.0, JsonbRepr::Value(_)) {
            let value = self.as_value().clone();
            self.0 = JsonbRepr::Value(value);
        }
        match &mut self.0 {
            JsonbRepr::Value(value) => value,
            JsonbRepr::Binary(_) | JsonbRepr::CanonicalText(_) | JsonbRepr::TextArray(_) => {
                unreachable!("native JSONB was materialized above")
            }
        }
    }

    pub fn binary(&self) -> Result<Cow<'_, [u8]>, JsonbError> {
        match &self.0 {
            JsonbRepr::Value(value) => encode_binary(value).map(Cow::Owned),
            JsonbRepr::Binary(binary) => Ok(Cow::Borrowed(&binary.bytes)),
            JsonbRepr::CanonicalText(text) => match text.binary.get_or_init(|| {
                let value = text.value.get_or_init(|| {
                    deserialize_validated_json_text(&text.bytes)
                        .expect("validated canonical JSON decodes")
                });
                encode_binary(value)
            }) {
                Ok(bytes) => Ok(Cow::Borrowed(bytes)),
                Err(error) => Err(*error),
            },
            JsonbRepr::TextArray(array) => {
                let mut bytes = Vec::with_capacity(array.binary_len);
                append_text_array_binary(&mut bytes, &array.values)?;
                Ok(Cow::Owned(bytes))
            }
        }
    }

    /// Exact canonical binary width without materializing an intermediate
    /// JSONB buffer for native text arrays.
    #[doc(hidden)]
    pub fn binary_len(&self) -> Result<usize, JsonbError> {
        match &self.0 {
            JsonbRepr::Value(value) => estimated_value_size(value, MAX_BYTES),
            JsonbRepr::Binary(binary) => Ok(binary.bytes.len()),
            JsonbRepr::CanonicalText(_) => self.binary().map(|bytes| bytes.len()),
            JsonbRepr::TextArray(array) => Ok(array.binary_len),
        }
    }

    /// Appends canonical binary bytes directly to an existing typed page.
    #[doc(hidden)]
    pub fn append_binary(&self, output: &mut Vec<u8>) -> Result<(), JsonbError> {
        let start = output.len();
        let result = match &self.0 {
            JsonbRepr::Value(value) => {
                let max_bytes = start
                    .checked_add(MAX_BYTES)
                    .ok_or(JsonbError("JSONB value is too large"))?;
                let mut bounded = CappedJsonOutput {
                    bytes: output,
                    max_bytes,
                };
                encode_node(&mut bounded, value, 0).map_err(binary_output_error)
            }
            JsonbRepr::Binary(binary) => {
                output.extend_from_slice(&binary.bytes);
                Ok(())
            }
            JsonbRepr::CanonicalText(_) => {
                output.extend_from_slice(&self.binary()?);
                Ok(())
            }
            JsonbRepr::TextArray(array) => append_text_array_binary(output, &array.values),
        };
        if result.is_err() {
            output.truncate(start);
        }
        result
    }

    pub fn is_valid(&self) -> bool {
        match &self.0 {
            JsonbRepr::Value(value) => json_value_valid(value),
            JsonbRepr::Binary(_) => true,
            JsonbRepr::CanonicalText(_) => true,
            JsonbRepr::TextArray(_) => true,
        }
    }

    pub fn is_binary(&self) -> bool {
        matches!(self.0, JsonbRepr::Binary(_) | JsonbRepr::TextArray(_))
    }

    pub fn estimated_binary_size(&self) -> u64 {
        match &self.0 {
            JsonbRepr::Binary(binary) => binary.bytes.len() as u64,
            JsonbRepr::CanonicalText(text) => text.bytes.len() as u64,
            JsonbRepr::Value(value) => estimated_value_size(value, MAX_BYTES)
                .map(|size| size as u64)
                .unwrap_or(MAX_BYTES as u64),
            JsonbRepr::TextArray(array) => {
                u64::try_from(text_array_binary_size(&array.values).unwrap_or(usize::MAX))
                    .unwrap_or(u64::MAX)
            }
        }
    }

    /// Renders canonical JSON text without materializing a DOM for values
    /// already held in the native binary representation.
    pub fn to_json_string(&self) -> Result<String, JsonbError> {
        let capacity = self.initial_json_capacity();
        let mut output = Vec::with_capacity(capacity);
        self.append_canonical_json(&mut output)?;
        // SAFETY: the appenders emit ASCII syntax around validated UTF-8.
        Ok(unsafe { String::from_utf8_unchecked(output) })
    }

    fn initial_json_capacity(&self) -> usize {
        match &self.0 {
            JsonbRepr::CanonicalText(text) => text.bytes.len(),
            JsonbRepr::Binary(binary) => binary.bytes.len(),
            JsonbRepr::TextArray(array) => array.binary_len,
            // A value may use compact exponent notation while its exact
            // canonical decimal rendering is much larger. Let the capped
            // writer grow incrementally instead of reserving from its binary
            // estimate, which can exceed the renderer's output budget.
            JsonbRepr::Value(_) => 0,
        }
    }

    /// Appends this value's canonical compact JSON text without constructing
    /// an intermediate string or materializing binary-backed values as a DOM.
    pub fn append_canonical_json(&self, output: &mut Vec<u8>) -> Result<(), JsonbError> {
        let start = output.len();
        let max_bytes = start
            .checked_add(MAX_RENDERED_BYTES)
            .ok_or(JsonbError("JSONB JSON rendering is too large"))?;
        let mut output = CappedJsonOutput {
            bytes: output,
            max_bytes,
        };
        let result = match &self.0 {
            JsonbRepr::Value(value) => append_value_json(&mut output, value, 0),
            JsonbRepr::Binary(binary) => {
                let mut reader = Reader {
                    bytes: &binary.bytes,
                    offset: 0,
                };
                reader.write_json(&mut output, 0)
            }
            JsonbRepr::CanonicalText(text) => {
                if text.reformat_numbers {
                    let value = text.value.get_or_init(|| {
                        deserialize_validated_json_text(&text.bytes)
                            .expect("validated canonical JSON decodes")
                    });
                    append_value_json(&mut output, value, 0)
                } else {
                    output.extend_from_slice(&text.bytes)?;
                    Ok(())
                }
            }
            JsonbRepr::TextArray(array) => append_text_array_json(&mut output, &array.values),
        };
        if result.is_err() {
            output.bytes.truncate(start);
            return result;
        }
        Ok(())
    }
}

impl From<JsonValue> for Jsonb {
    fn from(value: JsonValue) -> Self {
        Self::from_value(value)
    }
}

impl Deref for Jsonb {
    type Target = JsonValue;

    fn deref(&self) -> &Self::Target {
        self.as_value()
    }
}

impl DerefMut for Jsonb {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_value_mut()
    }
}

impl AsRef<JsonValue> for Jsonb {
    fn as_ref(&self) -> &JsonValue {
        self.as_value()
    }
}

impl PartialEq for Jsonb {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (JsonbRepr::Value(left), JsonbRepr::Value(right)) => json_values_eq(left, right),
            (JsonbRepr::CanonicalText(left), JsonbRepr::CanonicalText(right)) => {
                left.bytes == right.bytes || canonical_jsonb_eq(self, other)
            }
            _ => {
                let (Ok(left), Ok(right)) = (self.binary(), other.binary()) else {
                    return false;
                };
                left == right || canonical_jsonb_eq(self, other)
            }
        }
    }
}

// Canonical JSONB equality is reflexive: every constructor validates values
// that can be encoded canonically, and equality compares that canonical form.
impl Eq for Jsonb {}

impl PartialEq<JsonValue> for Jsonb {
    fn eq(&self, other: &JsonValue) -> bool {
        match &self.0 {
            JsonbRepr::Value(value) => json_values_eq(value, other),
            JsonbRepr::Binary(binary) => encode_binary(other).is_ok_and(|encoded| {
                encoded.as_slice() == binary.bytes.as_ref() || canonical_jsonb_value_eq(self, other)
            }),
            JsonbRepr::CanonicalText(_) | JsonbRepr::TextArray(_) => {
                self.binary().is_ok_and(|left| {
                    encode_binary(other)
                        .is_ok_and(|right| left == right || canonical_jsonb_value_eq(self, other))
                })
            }
        }
    }
}

impl PartialEq<Jsonb> for JsonValue {
    fn eq(&self, other: &Jsonb) -> bool {
        other == self
    }
}

fn json_values_eq(left: &JsonValue, right: &JsonValue) -> bool {
    left == right
        || match (encode_binary(left), encode_binary(right)) {
            (Ok(left_bytes), Ok(right_bytes)) => {
                left_bytes == right_bytes || canonical_value_json_eq(left, right)
            }
            // `from_value` is intentionally infallible, so invalid JSONB can
            // exist until validation or encoding. Raw equality above preserves
            // reflexivity without letting invalid data equal valid binary data.
            (Ok(_), Err(_)) | (Err(_), Ok(_)) | (Err(_), Err(_)) => false,
        }
}

fn canonical_jsonb_eq(left: &Jsonb, right: &Jsonb) -> bool {
    let mut left_json = Vec::new();
    let mut right_json = Vec::new();
    left.append_canonical_json(&mut left_json).is_ok()
        && right.append_canonical_json(&mut right_json).is_ok()
        && canonical_json_bytes_eq(&left_json, &right_json)
}

fn canonical_jsonb_value_eq(jsonb: &Jsonb, value: &JsonValue) -> bool {
    let mut left = Vec::new();
    let mut right = Vec::new();
    jsonb.append_canonical_json(&mut left).is_ok()
        && append_value_json(&mut right, value, 0).is_ok()
        && canonical_json_bytes_eq(&left, &right)
}

fn canonical_value_json_eq(left: &JsonValue, right: &JsonValue) -> bool {
    let mut left_json = Vec::new();
    let mut right_json = Vec::new();
    append_value_json(&mut left_json, left, 0).is_ok()
        && append_value_json(&mut right_json, right, 0).is_ok()
        && canonical_json_bytes_eq(&left_json, &right_json)
}

/// Compares already-rendered canonical JSON while treating only numeric value
/// tokens as exact PostgreSQL decimals. Strings and structural bytes retain
/// their byte-for-byte canonical comparison; number syntax and normalization
/// remain owned by serde_json and the shared JSONB number normalizer.
fn canonical_json_bytes_eq(left: &[u8], right: &[u8]) -> bool {
    if left == right {
        return true;
    }

    let (mut left_offset, mut right_offset) = (0, 0);
    while left_offset < left.len() && right_offset < right.len() {
        let left_byte = left[left_offset];
        let right_byte = right[right_offset];

        if left_byte == b'"' && right_byte == b'"' {
            let Some(left_end) = canonical_string_end(left, left_offset) else {
                return false;
            };
            let Some(right_end) = canonical_string_end(right, right_offset) else {
                return false;
            };
            if left[left_offset..left_end] != right[right_offset..right_end] {
                return false;
            }
            left_offset = left_end;
            right_offset = right_end;
            continue;
        }

        if is_json_number_start(left_byte) || is_json_number_start(right_byte) {
            if !is_json_number_start(left_byte) || !is_json_number_start(right_byte) {
                return false;
            }
            let left_end = json_number_end(left, left_offset);
            let right_end = json_number_end(right, right_offset);
            let left_token = &left[left_offset..left_end];
            let right_token = &right[right_offset..right_end];
            if left_token != right_token {
                let (Ok(left_number), Ok(right_number)) = (
                    serde_json::from_slice::<serde_json::Number>(left_token),
                    serde_json::from_slice::<serde_json::Number>(right_token),
                ) else {
                    return false;
                };
                let (Ok(left_number), Ok(right_number)) = (
                    normalize_jsonb_number(&left_number),
                    normalize_jsonb_number(&right_number),
                ) else {
                    return false;
                };
                if left_number.as_str() != right_number.as_str() {
                    return false;
                }
            }
            left_offset = left_end;
            right_offset = right_end;
            continue;
        }

        if left_byte != right_byte {
            return false;
        }
        left_offset += 1;
        right_offset += 1;
    }
    left_offset == left.len() && right_offset == right.len()
}

fn canonical_string_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut offset = start.checked_add(1)?;
    while offset < bytes.len() {
        match bytes[offset] {
            b'\\' => offset = offset.checked_add(2)?,
            b'"' => return offset.checked_add(1),
            _ => offset += 1,
        }
    }
    None
}

fn is_json_number_start(byte: u8) -> bool {
    matches!(byte, b'-' | b'0'..=b'9')
}

fn json_number_end(bytes: &[u8], start: usize) -> usize {
    let mut offset = start;
    while offset < bytes.len() && !matches!(bytes[offset], b',' | b']' | b'}') {
        offset += 1;
    }
    offset
}

impl serde::Serialize for Jsonb {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serde::Serialize::serialize(self.as_value(), serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Jsonb {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        serde::Deserialize::deserialize(deserializer).map(Self::from_value)
    }
}

fn binary_output_error(error: JsonbError) -> JsonbError {
    if error == JsonbError("JSONB JSON rendering is too large") {
        JsonbError("JSONB value is too large")
    } else {
        error
    }
}

pub fn encode_binary(value: &JsonValue) -> Result<Vec<u8>, JsonbError> {
    let mut bytes = Vec::new();
    let mut output = CappedJsonOutput {
        bytes: &mut bytes,
        max_bytes: MAX_BYTES,
    };
    encode_node(&mut output, value, 0).map_err(binary_output_error)?;
    Ok(bytes)
}

fn text_array_binary_size(values: &[String]) -> Result<usize, JsonbError> {
    let size = values.iter().try_fold(5usize, |size, value| {
        size.checked_add(5 + value.len())
            .ok_or(JsonbError("JSONB array is too large"))
    })?;
    if size > MAX_BYTES || u32::try_from(values.len()).is_err() {
        return Err(JsonbError("JSONB array is too large"));
    }
    Ok(size)
}

fn append_text_array_binary(output: &mut Vec<u8>, values: &[String]) -> Result<(), JsonbError> {
    // `TextArrayJsonb` validates all lengths and interior NULs once at
    // construction. Page encoding is its hot path, so do not rescan every
    // string after `binary_len` has already supplied the exact framed width.
    output.push(7);
    output.extend_from_slice(&(values.len() as u32).to_be_bytes());
    for value in values {
        output.push(6);
        output.extend_from_slice(&(value.len() as u32).to_be_bytes());
        output.extend_from_slice(value.as_bytes());
    }
    Ok(())
}

pub fn validate_binary(bytes: &[u8]) -> Result<(), JsonbError> {
    if bytes.len() > MAX_BYTES {
        return Err(JsonbError("JSONB value is too large"));
    }
    let mut reader = Reader { bytes, offset: 0 };
    reader.validate_value(0)?;
    if reader.offset != bytes.len() {
        return Err(JsonbError("JSONB value has trailing bytes"));
    }
    Ok(())
}

/// Validates compact UTF-8 JSON text using the same normalization rules as
/// native JSONB, without constructing a JSON DOM. Numbers may use either the
/// exact PostgreSQL decimal spelling or the strict legacy Ryu spelling when
/// that spelling represents the same exact normalized decimal.
///
/// The returned string borrows the input and is safe to retain as canonical
/// JSON text. Object-key decoding allocates only when a key contains an escape
/// sequence and therefore cannot be compared in its borrowed representation.
pub fn validate_canonical_json_text(bytes: &[u8]) -> Result<&str, JsonbError> {
    validate_canonical_json_text_with_number_rendering(bytes).map(|(text, _)| text)
}

fn validate_canonical_json_text_with_number_rendering(
    bytes: &[u8],
) -> Result<(&str, bool), JsonbError> {
    if bytes.len() > MAX_BYTES {
        return Err(JsonbError("JSONB value is too large"));
    }
    let text = std::str::from_utf8(bytes).map_err(|_| JsonbError("JSONB text is not UTF-8"))?;
    let mut reader = CanonicalTextReader {
        text,
        bytes,
        offset: 0,
        reformat_numbers: false,
    };
    reader.value(0)?;
    if reader.offset != bytes.len() {
        return Err(JsonbError("JSONB text has trailing bytes"));
    }
    Ok((text, reader.reformat_numbers))
}

struct CanonicalTextReader<'a> {
    text: &'a str,
    bytes: &'a [u8],
    offset: usize,
    reformat_numbers: bool,
}

impl<'a> CanonicalTextReader<'a> {
    fn value(&mut self, depth: usize) -> Result<(), JsonbError> {
        if depth > MAX_DEPTH {
            return Err(JsonbError("JSONB value exceeds its nesting limit"));
        }
        match self.peek()? {
            b'n' => self.literal(b"null"),
            b'f' => self.literal(b"false"),
            b't' => self.literal(b"true"),
            b'"' => self.string(false).map(|_| ()),
            b'[' => self.array(depth),
            b'{' => self.object(depth),
            b'-' | b'0'..=b'9' => self.number(),
            _ => Err(JsonbError("JSONB text value is invalid")),
        }
    }

    fn peek(&self) -> Result<u8, JsonbError> {
        self.bytes
            .get(self.offset)
            .copied()
            .ok_or(JsonbError("JSONB text is truncated"))
    }

    fn byte(&mut self, expected: u8) -> Result<(), JsonbError> {
        if self.peek()? != expected {
            return Err(JsonbError("JSONB text punctuation is invalid"));
        }
        self.offset += 1;
        Ok(())
    }

    fn literal(&mut self, literal: &[u8]) -> Result<(), JsonbError> {
        if self.bytes.get(self.offset..self.offset + literal.len()) != Some(literal) {
            return Err(JsonbError("JSONB text literal is invalid"));
        }
        self.offset += literal.len();
        Ok(())
    }

    fn array(&mut self, depth: usize) -> Result<(), JsonbError> {
        self.byte(b'[')?;
        if self.peek()? == b']' {
            self.offset += 1;
            return Ok(());
        }
        loop {
            self.value(depth + 1)?;
            match self.peek()? {
                b',' => self.offset += 1,
                b']' => {
                    self.offset += 1;
                    return Ok(());
                }
                _ => return Err(JsonbError("JSONB text array is invalid")),
            }
        }
    }

    fn object(&mut self, depth: usize) -> Result<(), JsonbError> {
        self.byte(b'{')?;
        if self.peek()? == b'}' {
            self.offset += 1;
            return Ok(());
        }
        let mut previous: Option<Cow<'a, str>> = None;
        loop {
            let key = self.string(true)?;
            if previous
                .as_deref()
                .is_some_and(|previous| previous >= key.as_ref())
            {
                return Err(JsonbError("JSONB text keys are not canonical"));
            }
            self.byte(b':')?;
            self.value(depth + 1)?;
            previous = Some(key);
            match self.peek()? {
                b',' => self.offset += 1,
                b'}' => {
                    self.offset += 1;
                    return Ok(());
                }
                _ => return Err(JsonbError("JSONB text object is invalid")),
            }
        }
    }

    fn string(&mut self, decode_escapes: bool) -> Result<Cow<'a, str>, JsonbError> {
        self.byte(b'"')?;
        let start = self.offset;
        let mut escaped = false;
        loop {
            match self.peek()? {
                b'"' => {
                    let end = self.offset;
                    self.offset += 1;
                    let encoded = &self.text[start..end];
                    return if escaped && decode_escapes {
                        decode_canonical_string(encoded).map(Cow::Owned)
                    } else {
                        Ok(Cow::Borrowed(encoded))
                    };
                }
                b'\\' => {
                    escaped = true;
                    self.offset += 1;
                    self.validate_escape()?;
                }
                0x00..=0x1f => {
                    return Err(JsonbError("JSONB text string contains a control byte"));
                }
                _ => self.offset += 1,
            }
        }
    }

    fn validate_escape(&mut self) -> Result<(), JsonbError> {
        match self.peek()? {
            b'"' | b'\\' | b'b' | b't' | b'n' | b'f' | b'r' => {
                self.offset += 1;
                Ok(())
            }
            b'u' => {
                let digits = self
                    .bytes
                    .get(self.offset + 1..self.offset + 5)
                    .ok_or(JsonbError("JSONB text escape is truncated"))?;
                let code = canonical_control_escape(digits)?;
                if code == 0 {
                    return Err(JsonbError("JSONB string contains a NUL"));
                }
                if matches!(code, 0x08 | 0x09 | 0x0a | 0x0c | 0x0d) {
                    return Err(JsonbError("JSONB text escape is not canonical"));
                }
                self.offset += 5;
                Ok(())
            }
            _ => Err(JsonbError("JSONB text escape is not canonical")),
        }
    }

    fn number(&mut self) -> Result<(), JsonbError> {
        let start = self.offset;
        while self
            .bytes
            .get(self.offset)
            .is_some_and(|byte| matches!(byte, b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E'))
        {
            self.offset += 1;
        }
        let encoded = &self.text[start..self.offset];
        let number = serde_json::from_str::<serde_json::Number>(encoded)
            .map_err(|_| JsonbError("JSONB text number is invalid"))?;
        let canonical = normalize_jsonb_number(&number)?;
        if canonical.as_str().as_bytes() == encoded.as_bytes() {
            // Preserve a legacy integer's plain spelling even when the same
            // decimal also has a compact tag-5 spelling (for example,
            // integer 10000000000000000 versus float 1e16). For plain
            // decimals that have no legacy integer image, keep the old Ryu
            // rendering when it exactly represents the same decimal.
            let preferred = match legacy_encoding_from_raw(&number)
                .or_else(|| legacy_encoding_from_canonical(&canonical))
            {
                Some(LegacyNumberEncoding::Signed(number)) => {
                    Some(itoa::Buffer::new().format(number).to_owned())
                }
                Some(LegacyNumberEncoding::Unsigned(number)) => {
                    Some(itoa::Buffer::new().format(number).to_owned())
                }
                Some(LegacyNumberEncoding::Float(number)) => {
                    Some(ryu::Buffer::new().format(number + 0.0).to_owned())
                }
                None => None,
            };
            self.reformat_numbers |=
                preferred.is_some_and(|spelling| spelling.as_bytes() != encoded.as_bytes());
            return Ok(());
        }
        if legacy_float_from_raw(&number).is_some()
            || legacy_float_spelling(&canonical)
                .is_some_and(|spelling| spelling.as_bytes() == encoded.as_bytes())
        {
            Ok(())
        } else {
            Err(JsonbError("JSONB text number is not canonical"))
        }
    }
}

fn canonical_control_escape(digits: &[u8]) -> Result<u8, JsonbError> {
    if digits.len() != 4 || digits[0] != b'0' || digits[1] != b'0' {
        return Err(JsonbError("JSONB text escape is not canonical"));
    }
    let hex = |digit| match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        _ => None,
    };
    let high = hex(digits[2]).ok_or(JsonbError("JSONB text escape is not canonical"))?;
    let low = hex(digits[3]).ok_or(JsonbError("JSONB text escape is not canonical"))?;
    let code = high * 16 + low;
    if code > 0x1f {
        return Err(JsonbError("JSONB text escape is not canonical"));
    }
    Ok(code)
}

fn decode_canonical_string(encoded: &str) -> Result<String, JsonbError> {
    let bytes = encoded.as_bytes();
    let mut output = String::with_capacity(encoded.len());
    let mut offset = 0usize;
    let mut plain_start = 0usize;
    while offset < bytes.len() {
        if bytes[offset] != b'\\' {
            offset += 1;
            continue;
        }
        output.push_str(&encoded[plain_start..offset]);
        offset += 1;
        match bytes[offset] {
            b'"' => output.push('"'),
            b'\\' => output.push('\\'),
            b'b' => output.push('\u{0008}'),
            b't' => output.push('\t'),
            b'n' => output.push('\n'),
            b'f' => output.push('\u{000c}'),
            b'r' => output.push('\r'),
            b'u' => {
                let code = canonical_control_escape(&bytes[offset + 1..offset + 5])?;
                output.push(char::from(code));
                offset += 4;
            }
            _ => return Err(JsonbError("JSONB text escape is not canonical")),
        }
        offset += 1;
        plain_start = offset;
    }
    output.push_str(&encoded[plain_start..]);
    Ok(output)
}

/// Validates and renders borrowed canonical binary JSONB without constructing
/// a [`Jsonb`], retaining the input, or materializing a JSON DOM.
pub fn binary_to_json_string(bytes: &[u8]) -> Result<String, JsonbError> {
    validate_binary(bytes)?;
    // SAFETY: `validate_binary` proved the complete canonical JSONB envelope.
    unsafe { validated_binary_to_json_string(bytes) }
}

/// Renders binary JSONB after the caller has already validated the complete
/// envelope. This avoids repeating recursive validation at typed wire and SQL
/// boundaries that carry an explicit validation proof.
///
/// # Safety
///
/// `bytes` must have passed [`validate_binary`] unchanged.
#[doc(hidden)]
pub unsafe fn validated_binary_to_json_string(bytes: &[u8]) -> Result<String, JsonbError> {
    let mut output = Vec::with_capacity(bytes.len().min(MAX_RENDERED_BYTES));
    let mut reader = Reader { bytes, offset: 0 };
    let mut capped = CappedJsonOutput {
        bytes: &mut output,
        max_bytes: MAX_RENDERED_BYTES,
    };
    reader.write_json(&mut capped, 0)?;
    debug_assert_eq!(
        reader.offset,
        bytes.len(),
        "validated JSONB is fully consumed"
    );
    // SAFETY: JSON punctuation and numeric formatters emit ASCII; every copied
    // string was validated as UTF-8 by `Reader::string`, and the fallback JSON
    // serializer emits UTF-8. The caller's envelope proof excludes trailing
    // bytes that this projection deliberately does not re-scan.
    Ok(unsafe { String::from_utf8_unchecked(output) })
}

pub fn decode_binary(bytes: &[u8]) -> Result<JsonValue, JsonbError> {
    if bytes.len() > MAX_BYTES {
        return Err(JsonbError("JSONB value is too large"));
    }
    let mut reader = Reader { bytes, offset: 0 };
    let value = reader.decode_value(0)?;
    if reader.offset != bytes.len() {
        return Err(JsonbError("JSONB value has trailing bytes"));
    }
    Ok(value)
}

fn json_value_valid(value: &JsonValue) -> bool {
    match value {
        JsonValue::String(value) => !value.contains('\0'),
        JsonValue::Array(values) => values.iter().all(json_value_valid),
        JsonValue::Object(values) => values
            .iter()
            .all(|(key, value)| !key.contains('\0') && json_value_valid(value)),
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) => true,
    }
}

fn estimated_value_size(value: &JsonValue, limit: usize) -> Result<usize, JsonbError> {
    fn add(size: usize, amount: usize, limit: usize) -> Result<usize, JsonbError> {
        size.checked_add(amount)
            .filter(|next| *next <= limit)
            .ok_or(JsonbError("JSONB value is too large"))
    }

    fn estimate(value: &JsonValue, depth: usize, limit: usize) -> Result<usize, JsonbError> {
        if depth > MAX_DEPTH {
            return Err(JsonbError("JSONB value exceeds its nesting limit"));
        }
        match value {
            JsonValue::Null | JsonValue::Bool(_) => Ok(1),
            JsonValue::Number(number) => match classify_number(number)? {
                NumberEncoding::Signed(_)
                | NumberEncoding::Unsigned(_)
                | NumberEncoding::Float(_) => Ok(9),
                NumberEncoding::Exact(number) => add(5, number.as_str().len(), limit),
            },
            JsonValue::String(value) => {
                u32::try_from(value.len()).map_err(|_| JsonbError("JSONB string is too large"))?;
                add(5, value.len(), limit)
            }
            JsonValue::Array(values) => {
                u32::try_from(values.len()).map_err(|_| JsonbError("JSONB array is too large"))?;
                let mut size = 5;
                if size > limit {
                    return Err(JsonbError("JSONB value is too large"));
                }
                for value in values {
                    size = add(size, estimate(value, depth + 1, limit - size)?, limit)?;
                }
                Ok(size)
            }
            JsonValue::Object(values) => {
                u32::try_from(values.len()).map_err(|_| JsonbError("JSONB object is too large"))?;
                let mut size = 5;
                if size > limit {
                    return Err(JsonbError("JSONB value is too large"));
                }
                for (key, value) in values {
                    u32::try_from(key.len())
                        .map_err(|_| JsonbError("JSONB string is too large"))?;
                    size = add(size, add(4, key.len(), limit)?, limit)?;
                    size = add(size, estimate(value, depth + 1, limit - size)?, limit)?;
                }
                Ok(size)
            }
        }
    }

    estimate(value, 0, limit)
}

fn encode_node<O: JsonOutput>(
    bytes: &mut O,
    value: &JsonValue,
    depth: usize,
) -> Result<(), JsonbError> {
    if depth > MAX_DEPTH {
        return Err(JsonbError("JSONB value exceeds its nesting limit"));
    }
    match value {
        JsonValue::Null => bytes.push(0)?,
        JsonValue::Bool(false) => bytes.push(1)?,
        JsonValue::Bool(true) => bytes.push(2)?,
        JsonValue::Number(number) => encode_number(bytes, number)?,
        JsonValue::String(value) => {
            if value.contains('\0') {
                return Err(JsonbError("JSONB string contains a NUL"));
            }
            bytes.push(6)?;
            encode_string(bytes, value)?;
        }
        JsonValue::Array(values) => {
            bytes.push(7)?;
            bytes.extend_from_slice(
                &u32::try_from(values.len())
                    .map_err(|_| JsonbError("JSONB array is too large"))?
                    .to_be_bytes(),
            )?;
            for value in values {
                encode_node(bytes, value, depth + 1)?;
            }
        }
        JsonValue::Object(values) => {
            bytes.push(8)?;
            bytes.extend_from_slice(
                &u32::try_from(values.len())
                    .map_err(|_| JsonbError("JSONB object is too large"))?
                    .to_be_bytes(),
            )?;
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            for (key, value) in entries {
                if key.contains('\0') {
                    return Err(JsonbError("JSONB key contains a NUL"));
                }
                encode_string(bytes, key)?;
                encode_node(bytes, value, depth + 1)?;
            }
        }
    }
    Ok(())
}

fn encode_string<O: JsonOutput>(bytes: &mut O, value: &str) -> Result<(), JsonbError> {
    bytes.extend_from_slice(
        &u32::try_from(value.len())
            .map_err(|_| JsonbError("JSONB string is too large"))?
            .to_be_bytes(),
    )?;
    bytes.extend_from_slice(value.as_bytes())?;
    Ok(())
}

enum LegacyNumberEncoding {
    Signed(i64),
    Unsigned(u64),
    Float(f64),
}

enum NumberEncoding {
    Signed(i64),
    Unsigned(u64),
    Float(f64),
    Exact(serde_json::Number),
}

fn legacy_float_number(value: f64) -> Result<serde_json::Number, JsonbError> {
    let rendered = ryu::Buffer::new().format(value + 0.0).to_owned();
    serde_json::from_str::<serde_json::Number>(&rendered)
        .map_err(|_| JsonbError("JSONB number is invalid"))
}

fn exact_legacy_float_from_canonical(canonical: &serde_json::Number) -> Option<f64> {
    let value = canonical.as_f64().filter(|number| number.is_finite())?;
    if value.fract() == 0.0 && value.abs() <= 9_007_199_254_740_992.0 {
        return None;
    }
    let rendered = normalize_jsonb_number(&legacy_float_number(value).ok()?).ok()?;
    (rendered.as_str() == canonical.as_str()).then_some(value)
}

fn legacy_float_from_raw(number: &serde_json::Number) -> Option<f64> {
    let value = number.as_f64().filter(|number| number.is_finite())?;
    if value.fract() == 0.0 && value.abs() <= 9_007_199_254_740_992.0 {
        return None;
    }
    let mut buffer = ryu::Buffer::new();
    let rendered = buffer.format(value + 0.0);
    (rendered == number.as_str()).then_some(value)
}

fn legacy_encoding_from_raw(number: &serde_json::Number) -> Option<LegacyNumberEncoding> {
    if let Some(value) = number.as_i64() {
        return Some(LegacyNumberEncoding::Signed(value));
    }
    if let Some(value) = number.as_u64() {
        return Some(LegacyNumberEncoding::Unsigned(value));
    }
    legacy_float_from_raw(number).map(LegacyNumberEncoding::Float)
}

fn legacy_float_spelling(number: &serde_json::Number) -> Option<String> {
    let value = exact_legacy_float_from_canonical(number)?;
    Some(ryu::Buffer::new().format(value + 0.0).to_owned())
}

fn legacy_encoding_from_canonical(number: &serde_json::Number) -> Option<LegacyNumberEncoding> {
    if let Some(value) = exact_legacy_float_from_canonical(number) {
        return Some(LegacyNumberEncoding::Float(value));
    }
    if let Some(value) = number.as_i64() {
        return Some(LegacyNumberEncoding::Signed(value));
    }
    number.as_u64().map(LegacyNumberEncoding::Unsigned)
}

fn classify_number(number: &serde_json::Number) -> Result<NumberEncoding, JsonbError> {
    // A raw token which is exactly Ryu's shortest spelling is already proof
    // that its decimal value round-trips to this finite f64. Avoid decimal
    // expansion on the common float path; nonmatching tokens still take the
    // exact-normalization path below.
    if let Some(encoding) = legacy_encoding_from_raw(number) {
        return Ok(match encoding {
            LegacyNumberEncoding::Signed(value) => NumberEncoding::Signed(value),
            LegacyNumberEncoding::Unsigned(value) => NumberEncoding::Unsigned(value),
            LegacyNumberEncoding::Float(value) => NumberEncoding::Float(value),
        });
    }
    let canonical = normalize_jsonb_number(number)?;
    if let Some(value) = exact_legacy_float_from_canonical(&canonical) {
        return Ok(NumberEncoding::Float(value));
    }
    if let Some(value) = canonical.as_i64() {
        return Ok(NumberEncoding::Signed(value));
    }
    if let Some(value) = canonical.as_u64() {
        return Ok(NumberEncoding::Unsigned(value));
    }
    Ok(NumberEncoding::Exact(canonical))
}

// New readers accept and pass through old tags 0 through 8, so existing
// physical repositories remain readable without rewriting unchanged values.
// Older software cannot read tag 9; release matching engine/native/client
// components together. This preserves old stored data, not old-reader forward
// compatibility. Tag 3/4 integers keep plain decimal text, while tag 5 keeps
// its Ryu spelling; overlapping values therefore retain their historical
// representation-specific spelling.
// Classify legacy images before creating an expanded decimal. A raw token
// matching Ryu proves exact round-trip equality; all other values are checked
// through the exact canonical decimal fallback.
fn encode_number<O: JsonOutput>(
    bytes: &mut O,
    number: &serde_json::Number,
) -> Result<(), JsonbError> {
    match classify_number(number)? {
        NumberEncoding::Signed(number) => {
            bytes.push(3)?;
            bytes.extend_from_slice(&number.to_be_bytes())?;
        }
        NumberEncoding::Unsigned(number) => {
            bytes.push(4)?;
            bytes.extend_from_slice(&number.to_be_bytes())?;
        }
        NumberEncoding::Float(number) => {
            bytes.push(5)?;
            bytes.extend_from_slice(&(number + 0.0).to_be_bytes())?;
        }
        NumberEncoding::Exact(canonical) => {
            let canonical = canonical.as_str();
            if canonical.len() > MAX_EXACT_NUMBER_BYTES {
                return Err(JsonbError("JSONB number is too large"));
            }
            bytes.push(9)?;
            bytes.extend_from_slice(
                &u32::try_from(canonical.len())
                    .map_err(|_| JsonbError("JSONB number is too large"))?
                    .to_be_bytes(),
            )?;
            bytes.extend_from_slice(canonical.as_bytes())?;
        }
    }
    Ok(())
}

trait JsonOutput {
    fn push(&mut self, byte: u8) -> Result<(), JsonbError>;
    fn extend_from_slice(&mut self, bytes: &[u8]) -> Result<(), JsonbError>;
}

impl JsonOutput for Vec<u8> {
    fn push(&mut self, byte: u8) -> Result<(), JsonbError> {
        Vec::push(self, byte);
        Ok(())
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) -> Result<(), JsonbError> {
        Vec::extend_from_slice(self, bytes);
        Ok(())
    }
}

struct CappedJsonOutput<'a> {
    bytes: &'a mut Vec<u8>,
    max_bytes: usize,
}

impl JsonOutput for CappedJsonOutput<'_> {
    fn push(&mut self, byte: u8) -> Result<(), JsonbError> {
        self.extend_from_slice(&[byte])
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) -> Result<(), JsonbError> {
        let next_len = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or(JsonbError("JSONB JSON rendering is too large"))?;
        if next_len > self.max_bytes {
            return Err(JsonbError("JSONB JSON rendering is too large"));
        }
        self.bytes
            .try_reserve(bytes.len())
            .map_err(|_| JsonbError("JSONB JSON rendering is too large"))?;
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
}

fn append_text_array_json<O: JsonOutput>(
    output: &mut O,
    values: &[String],
) -> Result<(), JsonbError> {
    output.push(b'[')?;
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            output.push(b',')?;
        }
        append_json_string(output, value)?;
    }
    output.push(b']')?;
    Ok(())
}

fn append_value_json<O: JsonOutput>(
    output: &mut O,
    value: &JsonValue,
    depth: usize,
) -> Result<(), JsonbError> {
    if depth > MAX_DEPTH {
        return Err(JsonbError("JSONB value exceeds its nesting limit"));
    }
    match value {
        JsonValue::Null => output.extend_from_slice(b"null")?,
        JsonValue::Bool(false) => output.extend_from_slice(b"false")?,
        JsonValue::Bool(true) => output.extend_from_slice(b"true")?,
        JsonValue::Number(number) => append_canonical_number(output, number)?,
        JsonValue::String(value) => append_json_string(output, value)?,
        JsonValue::Array(values) => {
            output.push(b'[')?;
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(b',')?;
                }
                append_value_json(output, value, depth + 1)?;
            }
            output.push(b']')?;
        }
        JsonValue::Object(values) => {
            output.push(b'{')?;
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index != 0 {
                    output.push(b',')?;
                }
                append_json_string(output, key)?;
                output.push(b':')?;
                append_value_json(output, value, depth + 1)?;
            }
            output.push(b'}')?;
        }
    }
    Ok(())
}

fn append_canonical_number<O: JsonOutput>(
    output: &mut O,
    number: &serde_json::Number,
) -> Result<(), JsonbError> {
    match classify_number(number)? {
        NumberEncoding::Signed(number) => {
            output.extend_from_slice(itoa::Buffer::new().format(number).as_bytes())?;
        }
        NumberEncoding::Unsigned(number) => {
            output.extend_from_slice(itoa::Buffer::new().format(number).as_bytes())?;
        }
        NumberEncoding::Float(number) => {
            output.extend_from_slice(ryu::Buffer::new().format(number + 0.0).as_bytes())?;
        }
        NumberEncoding::Exact(number) => output.extend_from_slice(number.as_str().as_bytes())?,
    }
    Ok(())
}

fn append_json_string<O: JsonOutput>(output: &mut O, value: &str) -> Result<(), JsonbError> {
    if value.contains('\0') {
        return Err(JsonbError("JSONB string contains a NUL"));
    }
    const HEX: &[u8; 16] = b"0123456789abcdef";
    output.push(b'"')?;
    let mut plain_start = 0usize;
    for (index, byte) in value.bytes().enumerate() {
        let escaped = match byte {
            b'"' => Some(&b"\\\""[..]),
            b'\\' => Some(&b"\\\\"[..]),
            0x08 => Some(&b"\\b"[..]),
            b'\t' => Some(&b"\\t"[..]),
            b'\n' => Some(&b"\\n"[..]),
            0x0c => Some(&b"\\f"[..]),
            b'\r' => Some(&b"\\r"[..]),
            0x00..=0x1f => {
                output.extend_from_slice(&value.as_bytes()[plain_start..index])?;
                output.extend_from_slice(b"\\u00")?;
                output.push(HEX[(byte >> 4) as usize])?;
                output.push(HEX[(byte & 0x0f) as usize])?;
                plain_start = index + 1;
                None
            }
            _ => None,
        };
        if let Some(escaped) = escaped {
            output.extend_from_slice(&value.as_bytes()[plain_start..index])?;
            output.extend_from_slice(escaped)?;
            plain_start = index + 1;
        }
    }
    output.extend_from_slice(&value.as_bytes()[plain_start..])?;
    output.push(b'"')?;
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn write_string<O: JsonOutput>(output: &mut O, value: &str) -> Result<(), JsonbError> {
        if value
            .as_bytes()
            .iter()
            .all(|byte| *byte >= 0x20 && *byte != b'"' && *byte != b'\\')
        {
            output.push(b'"')?;
            output.extend_from_slice(value.as_bytes())?;
            output.push(b'"')?;
            return Ok(());
        }
        append_json_string(output, value)
    }

    fn exact(&mut self, length: usize) -> Result<&'a [u8], JsonbError> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or(JsonbError("JSONB value is truncated"))?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn u32(&mut self) -> Result<usize, JsonbError> {
        Ok(u32::from_be_bytes(self.exact(4)?.try_into().expect("four JSONB bytes")) as usize)
    }

    fn string(&mut self) -> Result<&'a str, JsonbError> {
        let length = self.u32()?;
        if length > MAX_BYTES {
            return Err(JsonbError("JSONB string is too large"));
        }
        let value = std::str::from_utf8(self.exact(length)?)
            .map_err(|_| JsonbError("JSONB string is not UTF-8"))?;
        if value.contains('\0') {
            return Err(JsonbError("JSONB string contains a NUL"));
        }
        Ok(value)
    }

    /// Reads a string from a JSONB envelope that has already passed the full
    /// recursive validator.
    unsafe fn validated_string(&mut self) -> Result<&'a str, JsonbError> {
        let length = self.u32()?;
        if length > MAX_BYTES {
            return Err(JsonbError("JSONB string is too large"));
        }
        let value = self.exact(length)?;
        // SAFETY: the caller's validation proof covered UTF-8 and interior-NUL
        // checks for this exact immutable byte range.
        Ok(unsafe { std::str::from_utf8_unchecked(value) })
    }

    fn count(&mut self, divisor: usize, kind: &'static str) -> Result<usize, JsonbError> {
        let count = self.u32()?;
        if count > self.bytes.len().saturating_sub(self.offset) / divisor {
            return Err(JsonbError(kind));
        }
        Ok(count)
    }

    fn exact_number(&mut self) -> Result<&'a str, JsonbError> {
        let length = self.u32()?;
        if length > MAX_EXACT_NUMBER_BYTES {
            return Err(JsonbError("JSONB number is too large"));
        }
        let bytes = self.exact(length)?;
        let text =
            std::str::from_utf8(bytes).map_err(|_| JsonbError("JSONB number is not UTF-8"))?;
        let number = serde_json::from_str::<serde_json::Number>(text)
            .map_err(|_| JsonbError("JSONB number is invalid"))?;
        let canonical = normalize_jsonb_number(&number)?;
        if canonical.as_str() != text || legacy_encoding_from_canonical(&canonical).is_some() {
            return Err(JsonbError("JSONB number is not canonical"));
        }
        Ok(text)
    }

    fn validate_value(&mut self, depth: usize) -> Result<(), JsonbError> {
        if depth > MAX_DEPTH {
            return Err(JsonbError("JSONB value exceeds its nesting limit"));
        }
        match self.exact(1)?[0] {
            0..=2 => {}
            3 => {
                self.exact(8)?;
            }
            4 => {
                let value = u64::from_be_bytes(
                    self.exact(8)?
                        .try_into()
                        .expect("eight JSONB integer bytes"),
                );
                if value <= i64::MAX as u64 {
                    return Err(JsonbError("JSONB number is not canonical"));
                }
            }
            5 => validate_float(self.exact(8)?)?,
            9 => {
                self.exact_number()?;
            }
            6 => {
                self.string()?;
            }
            7 => {
                let count = self.count(1, "JSONB array count is invalid")?;
                for _ in 0..count {
                    self.validate_value(depth + 1)?;
                }
            }
            8 => {
                let count = self.count(5, "JSONB object count is invalid")?;
                let mut previous = None;
                for _ in 0..count {
                    let key = self.string()?;
                    if previous.is_some_and(|previous| previous >= key) {
                        return Err(JsonbError("JSONB keys are not canonical"));
                    }
                    self.validate_value(depth + 1)?;
                    previous = Some(key);
                }
            }
            _ => return Err(JsonbError("JSONB tag is invalid")),
        }
        Ok(())
    }

    fn write_json<O: JsonOutput>(
        &mut self,
        output: &mut O,
        depth: usize,
    ) -> Result<(), JsonbError> {
        if depth > MAX_DEPTH {
            return Err(JsonbError("JSONB value exceeds its nesting limit"));
        }
        match self.exact(1)?[0] {
            0 => output.extend_from_slice(b"null")?,
            1 => output.extend_from_slice(b"false")?,
            2 => output.extend_from_slice(b"true")?,
            3 => {
                let value = i64::from_be_bytes(
                    self.exact(8)?
                        .try_into()
                        .expect("eight JSONB integer bytes"),
                );
                output.extend_from_slice(itoa::Buffer::new().format(value).as_bytes())?;
            }
            4 => {
                let value = u64::from_be_bytes(
                    self.exact(8)?
                        .try_into()
                        .expect("eight JSONB integer bytes"),
                );
                output.extend_from_slice(itoa::Buffer::new().format(value).as_bytes())?;
            }
            5 => {
                let bytes = self.exact(8)?;
                validate_float(bytes)?;
                let number = f64::from_be_bytes(bytes.try_into().expect("eight JSONB float bytes"));
                output.extend_from_slice(ryu::Buffer::new().format(number + 0.0).as_bytes())?;
            }
            9 => {
                let number = self.exact_number()?;
                output.extend_from_slice(number.as_bytes())?;
            }
            6 => Self::write_string(output, unsafe { self.validated_string()? })?,
            7 => {
                let count = self.count(1, "JSONB array count is invalid")?;
                output.push(b'[')?;
                for index in 0..count {
                    if index != 0 {
                        output.push(b',')?;
                    }
                    self.write_json(output, depth + 1)?;
                }
                output.push(b']')?;
            }
            8 => {
                let count = self.count(5, "JSONB object count is invalid")?;
                output.push(b'{')?;
                for index in 0..count {
                    if index != 0 {
                        output.push(b',')?;
                    }
                    Self::write_string(output, unsafe { self.validated_string()? })?;
                    output.push(b':')?;
                    self.write_json(output, depth + 1)?;
                }
                output.push(b'}')?;
            }
            _ => return Err(JsonbError("JSONB tag is invalid")),
        }
        Ok(())
    }

    fn decode_value(&mut self, depth: usize) -> Result<JsonValue, JsonbError> {
        if depth > MAX_DEPTH {
            return Err(JsonbError("JSONB value exceeds its nesting limit"));
        }
        Ok(match self.exact(1)?[0] {
            0 => JsonValue::Null,
            1 => JsonValue::Bool(false),
            2 => JsonValue::Bool(true),
            3 => JsonValue::from(i64::from_be_bytes(
                self.exact(8)?
                    .try_into()
                    .expect("eight JSONB integer bytes"),
            )),
            4 => {
                let value = u64::from_be_bytes(
                    self.exact(8)?
                        .try_into()
                        .expect("eight JSONB integer bytes"),
                );
                if value <= i64::MAX as u64 {
                    return Err(JsonbError("JSONB number is not canonical"));
                }
                JsonValue::from(value)
            }
            5 => {
                let bytes = self.exact(8)?;
                validate_float(bytes)?;
                let number = f64::from_be_bytes(bytes.try_into().expect("eight JSONB float bytes"));
                JsonValue::Number(legacy_float_number(number)?)
            }
            9 => {
                let number = self.exact_number()?;
                JsonValue::Number(
                    serde_json::from_str(number)
                        .map_err(|_| JsonbError("JSONB number is invalid"))?,
                )
            }
            6 => JsonValue::String(self.string()?.to_owned()),
            7 => {
                let count = self.count(1, "JSONB array count is invalid")?;
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    values.push(self.decode_value(depth + 1)?);
                }
                JsonValue::Array(values)
            }
            8 => {
                let count = self.count(5, "JSONB object count is invalid")?;
                let mut values = serde_json::Map::new();
                let mut previous = None;
                for _ in 0..count {
                    let key = self.string()?;
                    if previous.is_some_and(|previous| previous >= key) {
                        return Err(JsonbError("JSONB keys are not canonical"));
                    }
                    values.insert(key.to_owned(), self.decode_value(depth + 1)?);
                    previous = Some(key);
                }
                JsonValue::Object(values)
            }
            _ => return Err(JsonbError("JSONB tag is invalid")),
        })
    }
}

// Canonical text constructors have already checked MAX_DEPTH. serde_json's
// default counter rejects the 128th container, while this codec admits depth
// 128; use the codec's validated bound consistently across representations.
fn deserialize_validated_json_text<T>(bytes: &[u8]) -> Result<T, JsonbError>
where
    T: serde::de::DeserializeOwned,
{
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    decoder.disable_recursion_limit();
    let value = T::deserialize(&mut decoder)
        .map_err(|_| JsonbError("JSONB value does not match the requested type"))?;
    decoder
        .end()
        .map_err(|_| JsonbError("JSONB value does not match the requested type"))?;
    Ok(value)
}

fn deserialize_binary_into<T>(bytes: &[u8]) -> Result<T, JsonbError>
where
    T: serde::de::DeserializeOwned,
{
    if bytes.len() > MAX_BYTES {
        return Err(JsonbError("JSONB value is too large"));
    }
    let mut reader = Reader { bytes, offset: 0 };
    let value = <T as serde::Deserialize>::deserialize(BinaryDeserializer {
        reader: &mut reader,
        depth: 0,
    })?;
    if reader.offset != bytes.len() {
        return Err(JsonbError("JSONB value has trailing bytes"));
    }
    Ok(value)
}

impl serde::de::Error for JsonbError {
    fn custom<T: std::fmt::Display>(_message: T) -> Self {
        JsonbError("JSONB value does not match the requested type")
    }
}

macro_rules! parse_json_numeric {
    ($text:expr, f32) => {
        $text
            .parse::<f32>()
            .ok()
            .filter(|number| number.is_finite())
            .ok_or(JsonbError("JSONB value does not match the requested type"))?
    };
    ($text:expr, f64) => {
        $text
            .parse::<f64>()
            .ok()
            .filter(|number| number.is_finite())
            .ok_or(JsonbError("JSONB value does not match the requested type"))?
    };
    ($text:expr, $number_type:ty) => {
        $text
            .parse::<$number_type>()
            .map_err(|_| JsonbError("JSONB value does not match the requested type"))?
    };
}

macro_rules! deserialize_json_number {
    ($name:ident, $number_type:ty, $visit:ident) => {
        fn $name<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            self.check_depth()?;
            let value = match self.reader.bytes.get(self.reader.offset).copied() {
                Some(9) => {
                    self.reader.exact(1)?;
                    parse_json_numeric!(self.reader.exact_number()?, $number_type)
                }
                Some(5) => {
                    self.reader.exact(1)?;
                    let bytes = self.reader.exact(8)?;
                    validate_float(bytes)?;
                    let number =
                        f64::from_be_bytes(bytes.try_into().expect("eight JSONB float bytes"));
                    return visitor.visit_f64(number);
                }
                _ => return self.deserialize_any(visitor),
            };
            visitor.$visit(value)
        }
    };
}

struct BinaryDeserializer<'de, 'reader> {
    reader: &'reader mut Reader<'de>,
    depth: usize,
}

impl<'de, 'reader> BinaryDeserializer<'de, 'reader> {
    fn check_depth(&self) -> Result<(), JsonbError> {
        if self.depth > MAX_DEPTH {
            Err(JsonbError("JSONB value exceeds its nesting limit"))
        } else {
            Ok(())
        }
    }

    fn tag(&mut self) -> Result<u8, JsonbError> {
        Ok(self.reader.exact(1)?[0])
    }

    fn sequence<V>(mut self, visitor: V) -> Result<V::Value, JsonbError>
    where
        V: serde::de::Visitor<'de>,
    {
        self.check_depth()?;
        if self.tag()? != 7 {
            return Err(JsonbError("JSONB value does not match the requested type"));
        }
        let remaining = self.reader.count(1, "JSONB array count is invalid")?;
        let mut access = BinarySeqAccess {
            reader: self.reader,
            remaining,
            depth: self.depth + 1,
        };
        let value = visitor.visit_seq(&mut access)?;
        if access.remaining != 0 {
            return Err(JsonbError("JSONB value does not match the requested type"));
        }
        Ok(value)
    }

    fn map<V>(mut self, visitor: V) -> Result<V::Value, JsonbError>
    where
        V: serde::de::Visitor<'de>,
    {
        self.check_depth()?;
        if self.tag()? != 8 {
            return Err(JsonbError("JSONB value does not match the requested type"));
        }
        let remaining = self.reader.count(5, "JSONB object count is invalid")?;
        let mut access = BinaryMapAccess {
            reader: self.reader,
            remaining,
            value_pending: false,
            depth: self.depth + 1,
        };
        let value = visitor.visit_map(&mut access)?;
        if access.remaining != 0 || access.value_pending {
            return Err(JsonbError("JSONB value does not match the requested type"));
        }
        Ok(value)
    }

    fn struct_value<V>(self, visitor: V) -> Result<V::Value, JsonbError>
    where
        V: serde::de::Visitor<'de>,
    {
        self.check_depth()?;
        match self.reader.bytes.get(self.reader.offset).copied() {
            Some(7) => self.sequence(visitor),
            Some(8) => self.map(visitor),
            _ => Err(JsonbError("JSONB value does not match the requested type")),
        }
    }
}

impl<'de, 'reader> serde::Deserializer<'de> for BinaryDeserializer<'de, 'reader> {
    type Error = JsonbError;

    fn deserialize_any<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.check_depth()?;
        match self.tag()? {
            0 => visitor.visit_unit(),
            1 => visitor.visit_bool(false),
            2 => visitor.visit_bool(true),
            3 => {
                let value = i64::from_be_bytes(
                    self.reader
                        .exact(8)?
                        .try_into()
                        .expect("eight JSONB integer bytes"),
                );
                if value >= 0 {
                    visitor.visit_u64(value as u64)
                } else {
                    visitor.visit_i64(value)
                }
            }
            4 => {
                let value = u64::from_be_bytes(
                    self.reader
                        .exact(8)?
                        .try_into()
                        .expect("eight JSONB integer bytes"),
                );
                if value <= i64::MAX as u64 {
                    return Err(JsonbError("JSONB number is not canonical"));
                }
                visitor.visit_u64(value)
            }
            5 => {
                let bytes = self.reader.exact(8)?;
                validate_float(bytes)?;
                let number = f64::from_be_bytes(bytes.try_into().expect("eight JSONB float bytes"));
                visitor.visit_f64(number)
            }
            9 => {
                let text = self.reader.exact_number()?;
                let number = serde_json::from_str::<serde_json::Number>(text)
                    .map_err(|_| JsonbError("JSONB number is invalid"))?;
                serde::Deserializer::deserialize_any(number, visitor)
                    .map_err(|_| JsonbError("JSONB value does not match the requested type"))
            }
            6 => visitor.visit_borrowed_str(self.reader.string()?),
            7 => {
                // The common container readers own their tag check. Reuse
                // them without consuming the discriminant twice.
                self.reader.offset -= 1;
                self.sequence(visitor)
            }
            8 => {
                self.reader.offset -= 1;
                self.map(visitor)
            }
            _ => Err(JsonbError("JSONB tag is invalid")),
        }
    }

    fn deserialize_bool<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.check_depth()?;
        match self.tag()? {
            1 => visitor.visit_bool(false),
            2 => visitor.visit_bool(true),
            _ => Err(JsonbError("JSONB value does not match the requested type")),
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.check_depth()?;
        if self.reader.bytes.get(self.reader.offset) == Some(&0) {
            self.reader.exact(1)?;
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.check_depth()?;
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.sequence(visitor)
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.sequence(visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.sequence(visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.map(visitor)
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.struct_value(visitor)
    }

    fn deserialize_enum<V>(
        mut self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.check_depth()?;
        match self.tag()? {
            6 => {
                let variant = self.reader.string()?;
                visitor.visit_enum(BinaryEnumAccess {
                    variant,
                    reader: None,
                    depth: self.depth + 1,
                })
            }
            8 => {
                let count = self.reader.count(5, "JSONB object count is invalid")?;
                if count != 1 {
                    return Err(JsonbError("JSONB value does not match the requested type"));
                }
                let variant = self.reader.string()?;
                let payload_offset = self.reader.offset;
                let value = visitor.visit_enum(BinaryEnumAccess {
                    variant,
                    reader: Some(&mut *self.reader),
                    depth: self.depth + 1,
                })?;
                if self.reader.offset == payload_offset {
                    return Err(JsonbError("JSONB value does not match the requested type"));
                }
                Ok(value)
            }
            _ => Err(JsonbError("JSONB value does not match the requested type")),
        }
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.check_depth()?;
        match self.reader.bytes.get(self.reader.offset).copied() {
            Some(6) => {
                self.reader.exact(1)?;
                visitor.visit_borrowed_bytes(self.reader.string()?.as_bytes())
            }
            Some(7) => self.sequence(visitor),
            _ => Err(JsonbError("JSONB value does not match the requested type")),
        }
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.check_depth()?;
        match self.reader.bytes.get(self.reader.offset).copied() {
            Some(6) => {
                self.reader.exact(1)?;
                visitor.visit_byte_buf(self.reader.string()?.as_bytes().to_vec())
            }
            Some(7) => self.sequence(visitor),
            _ => Err(JsonbError("JSONB value does not match the requested type")),
        }
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        BinaryDeserializer::deserialize_str(self, visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.check_depth()?;
        self.reader.validate_value(self.depth)?;
        visitor.visit_unit()
    }

    deserialize_json_number!(deserialize_i8, i8, visit_i8);
    deserialize_json_number!(deserialize_i16, i16, visit_i16);
    deserialize_json_number!(deserialize_i32, i32, visit_i32);
    deserialize_json_number!(deserialize_i64, i64, visit_i64);
    deserialize_json_number!(deserialize_i128, i128, visit_i128);
    deserialize_json_number!(deserialize_u8, u8, visit_u8);
    deserialize_json_number!(deserialize_u16, u16, visit_u16);
    deserialize_json_number!(deserialize_u32, u32, visit_u32);
    deserialize_json_number!(deserialize_u64, u64, visit_u64);
    deserialize_json_number!(deserialize_u128, u128, visit_u128);
    deserialize_json_number!(deserialize_f32, f32, visit_f32);
    deserialize_json_number!(deserialize_f64, f64, visit_f64);

    serde::forward_to_deserialize_any! {
        char str string unit unit_struct
    }
}

impl<'de, 'reader> BinaryDeserializer<'de, 'reader> {
    fn deserialize_str<V>(mut self, visitor: V) -> Result<V::Value, JsonbError>
    where
        V: serde::de::Visitor<'de>,
    {
        self.check_depth()?;
        if self.tag()? != 6 {
            return Err(JsonbError("JSONB value does not match the requested type"));
        }
        visitor.visit_borrowed_str(self.reader.string()?)
    }
}

struct BinarySeqAccess<'de, 'reader> {
    reader: &'reader mut Reader<'de>,
    remaining: usize,
    depth: usize,
}

impl<'de, 'reader> serde::de::SeqAccess<'de> for &mut BinarySeqAccess<'de, 'reader> {
    type Error = JsonbError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: serde::de::DeserializeSeed<'de>,
    {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        seed.deserialize(BinaryDeserializer {
            reader: self.reader,
            depth: self.depth,
        })
        .map(Some)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.remaining)
    }
}

struct BinaryMapAccess<'de, 'reader> {
    reader: &'reader mut Reader<'de>,
    remaining: usize,
    value_pending: bool,
    depth: usize,
}

impl<'de, 'reader> serde::de::MapAccess<'de> for &mut BinaryMapAccess<'de, 'reader> {
    type Error = JsonbError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: serde::de::DeserializeSeed<'de>,
    {
        if self.value_pending {
            return Err(JsonbError("JSONB value does not match the requested type"));
        }
        if self.remaining == 0 {
            return Ok(None);
        }
        let key = self.reader.string()?;
        self.value_pending = true;
        seed.deserialize(BinaryMapKeyDeserializer { value: key })
            .map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        if !self.value_pending {
            return Err(JsonbError("JSONB value does not match the requested type"));
        }
        self.value_pending = false;
        self.remaining -= 1;
        seed.deserialize(BinaryDeserializer {
            reader: self.reader,
            depth: self.depth,
        })
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.remaining)
    }
}

struct BinaryMapKeyDeserializer<'de> {
    value: &'de str,
}

macro_rules! numeric_map_key_methods {
    ($($method:ident => $number:ty => $visit:ident,)+) => {
        $(
            fn $method<V>(self, visitor: V) -> Result<V::Value, Self::Error>
            where
                V: serde::de::Visitor<'de>,
            {
                if !matches!(self.value.as_bytes().first(), Some(b'0'..=b'9' | b'-'))
                    || self.value.as_bytes().last().is_some_and(u8::is_ascii_whitespace)
                {
                    return Err(JsonbError("JSONB value does not match the requested type"));
                }
                let mut decoder = serde_json::Deserializer::from_str(self.value);
                let value = serde::Deserializer::$method(&mut decoder, visitor)
                    .map_err(|_| JsonbError("JSONB value does not match the requested type"))?;
                decoder.end()
                    .map_err(|_| JsonbError("JSONB value does not match the requested type"))?;
                Ok(value)
            }
        )+
    };
}

impl<'de> serde::Deserializer<'de> for BinaryMapKeyDeserializer<'de> {
    type Error = JsonbError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_borrowed_str(self.value)
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let value = self
            .value
            .parse::<bool>()
            .map_err(|_| JsonbError("JSONB value does not match the requested type"))?;
        visitor.visit_bool(value)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_some(self)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_borrowed_bytes(self.value.as_bytes())
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_byte_buf(self.value.as_bytes().to_vec())
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_enum(BinaryMapKeyEnumAccess {
            variant: self.value,
        })
    }

    numeric_map_key_methods! {
        deserialize_i8 => i8 => visit_i8,
        deserialize_i16 => i16 => visit_i16,
        deserialize_i32 => i32 => visit_i32,
        deserialize_i64 => i64 => visit_i64,
        deserialize_i128 => i128 => visit_i128,
        deserialize_u8 => u8 => visit_u8,
        deserialize_u16 => u16 => visit_u16,
        deserialize_u32 => u32 => visit_u32,
        deserialize_u64 => u64 => visit_u64,
        deserialize_u128 => u128 => visit_u128,
        deserialize_f32 => f32 => visit_f32,
        deserialize_f64 => f64 => visit_f64,
    }

    serde::forward_to_deserialize_any! {
        char str string unit unit_struct seq tuple tuple_struct map struct identifier ignored_any
    }
}

struct BinaryEnumAccess<'de, 'reader> {
    variant: &'de str,
    reader: Option<&'reader mut Reader<'de>>,
    depth: usize,
}

impl<'de, 'reader> serde::de::EnumAccess<'de> for BinaryEnumAccess<'de, 'reader> {
    type Error = JsonbError;
    type Variant = BinaryVariantAccess<'de, 'reader>;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        let variant = seed.deserialize(
            serde::de::value::BorrowedStrDeserializer::<JsonbError>::new(self.variant),
        )?;
        Ok((
            variant,
            BinaryVariantAccess {
                reader: self.reader,
                depth: self.depth,
            },
        ))
    }
}

struct BinaryVariantAccess<'de, 'reader> {
    reader: Option<&'reader mut Reader<'de>>,
    depth: usize,
}

impl<'de, 'reader> serde::de::VariantAccess<'de> for BinaryVariantAccess<'de, 'reader> {
    type Error = JsonbError;

    fn unit_variant(self) -> Result<(), Self::Error> {
        if let Some(reader) = self.reader {
            <() as serde::Deserialize>::deserialize(BinaryDeserializer {
                reader,
                depth: self.depth,
            })
        } else {
            Ok(())
        }
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: serde::de::DeserializeSeed<'de>,
    {
        let reader = self
            .reader
            .ok_or(JsonbError("JSONB value does not match the requested type"))?;
        seed.deserialize(BinaryDeserializer {
            reader,
            depth: self.depth,
        })
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let reader = self
            .reader
            .ok_or(JsonbError("JSONB value does not match the requested type"))?;
        serde::Deserializer::deserialize_tuple(
            BinaryDeserializer {
                reader,
                depth: self.depth,
            },
            len,
            visitor,
        )
    }

    fn struct_variant<V>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let reader = self
            .reader
            .ok_or(JsonbError("JSONB value does not match the requested type"))?;
        serde::Deserializer::deserialize_struct(
            BinaryDeserializer {
                reader,
                depth: self.depth,
            },
            "",
            fields,
            visitor,
        )
    }
}

struct BinaryMapKeyEnumAccess<'de> {
    variant: &'de str,
}

impl<'de> serde::de::EnumAccess<'de> for BinaryMapKeyEnumAccess<'de> {
    type Error = JsonbError;
    type Variant = BinaryMapKeyVariantAccess;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        let variant = seed.deserialize(
            serde::de::value::BorrowedStrDeserializer::<JsonbError>::new(self.variant),
        )?;
        Ok((variant, BinaryMapKeyVariantAccess))
    }
}

struct BinaryMapKeyVariantAccess;

impl<'de> serde::de::VariantAccess<'de> for BinaryMapKeyVariantAccess {
    type Error = JsonbError;

    fn unit_variant(self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, _seed: T) -> Result<T::Value, Self::Error>
    where
        T: serde::de::DeserializeSeed<'de>,
    {
        Err(JsonbError("JSONB value does not match the requested type"))
    }

    fn tuple_variant<V>(self, _len: usize, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(JsonbError("JSONB value does not match the requested type"))
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(JsonbError("JSONB value does not match the requested type"))
    }
}

fn validate_float(bytes: &[u8]) -> Result<(), JsonbError> {
    let number = f64::from_be_bytes(bytes.try_into().expect("eight JSONB float bytes"));
    if !number.is_finite()
        || (number == 0.0 && number.is_sign_negative())
        || (number.fract() == 0.0 && number.abs() <= 9_007_199_254_740_992.0)
    {
        return Err(JsonbError("JSONB number is not canonical"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[test]
    fn binary_jsonb_validates_without_materializing_and_decodes_lazily() {
        let value = serde_json::json!({"a": [true, null, 1.5], "z": "text"});
        let bytes: Arc<[u8]> = encode_binary(&value).unwrap().into();
        let jsonb = Jsonb::from_binary(bytes).unwrap();
        assert!(jsonb.is_binary());
        assert!(matches!(
            &jsonb.0,
            JsonbRepr::Binary(binary) if binary.value.get().is_none()
        ));
        assert!(jsonb.estimated_binary_size() > 0);
        assert!(matches!(
            &jsonb.0,
            JsonbRepr::Binary(binary) if binary.value.get().is_none()
        ));
        assert_eq!(jsonb.as_value(), &value);
    }

    #[test]
    fn binary_jsonb_renders_without_materializing_a_dom() {
        let value = serde_json::json!({"z": [true, null, "x"], "a": -7});
        let jsonb = Jsonb::from_binary(encode_binary(&value).unwrap().into()).unwrap();

        assert_eq!(
            jsonb.to_json_string().unwrap(),
            r#"{"a":-7,"z":[true,null,"x"]}"#
        );
        assert!(matches!(
            &jsonb.0,
            JsonbRepr::Binary(binary) if binary.value.get().is_none()
        ));
    }

    #[test]
    fn typed_deserialization_keeps_native_representations_lazy() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Entry {
            count: usize,
            name: String,
        }

        let value = serde_json::json!({"count": 3, "name": "plugin_a"});
        let binary = Jsonb::from_binary(encode_binary(&value).unwrap().into()).unwrap();
        let canonical_text =
            Jsonb::from_canonical_text_vec(br#"{"count":3,"name":"plugin_a"}"#.to_vec()).unwrap();
        let value_repr = Jsonb::from_value(value);

        for jsonb in [&binary, &canonical_text, &value_repr] {
            assert_eq!(
                jsonb.deserialize_into::<Entry>().unwrap(),
                Entry {
                    count: 3,
                    name: "plugin_a".to_owned(),
                }
            );
        }
        assert!(matches!(
            &binary.0,
            JsonbRepr::Binary(binary) if binary.value.get().is_none()
        ));
        assert!(matches!(
            &canonical_text.0,
            JsonbRepr::CanonicalText(text) if text.value.get().is_none()
        ));

        let text_array = Jsonb::from_text_array(vec!["a".into(), "β".into()]).unwrap();
        assert_eq!(
            text_array.deserialize_into::<Vec<String>>().unwrap(),
            vec!["a".to_owned(), "β".to_owned()]
        );
        assert!(matches!(
            &text_array.0,
            JsonbRepr::TextArray(array) if array.value.get().is_none()
        ));
    }

    #[test]
    fn numeric_map_keys_use_json_number_syntax_in_every_representation() {
        for key in ["+1", "01", "1 ", " 1", "NaN", "1.0", "1e1"] {
            let mut map = serde_json::Map::new();
            map.insert(key.to_owned(), serde_json::json!(7));
            let value = JsonValue::Object(map);
            let native = Jsonb::from_value(value.clone());
            let text =
                Jsonb::from_canonical_text_vec(native.to_json_string().unwrap().into_bytes())
                    .unwrap();
            let binary = Jsonb::from_binary(encode_binary(&value).unwrap().into()).unwrap();
            for jsonb in [native, text, binary] {
                assert!(
                    jsonb
                        .deserialize_into::<std::collections::BTreeMap<i32, u8>>()
                        .is_err(),
                    "numeric key {key:?} must reject consistently"
                );
            }
        }
    }

    #[test]
    fn wide_integer_map_keys_deserialize_in_every_representation() {
        use std::collections::BTreeMap;
        fn jsonb_representations(value: JsonValue) -> [Jsonb; 3] {
            let native = Jsonb::from_value(value.clone());
            let text =
                Jsonb::from_canonical_text_vec(native.to_json_string().unwrap().into_bytes())
                    .unwrap();
            let binary = Jsonb::from_binary_vec(encode_binary(&value).unwrap()).unwrap();
            [native, text, binary]
        }
        for key in [i128::MIN.to_string(), i128::MAX.to_string()] {
            let value = serde_json::json!({key.clone(): 7});
            for jsonb in jsonb_representations(value) {
                assert_eq!(
                    jsonb.deserialize_into::<BTreeMap<i128, u8>>().unwrap(),
                    BTreeMap::from([(key.parse::<i128>().unwrap(), 7)])
                );
            }
        }
        let key = u128::MAX.to_string();
        for jsonb in jsonb_representations(serde_json::json!({key: 7})) {
            assert_eq!(
                jsonb.deserialize_into::<BTreeMap<u128, u8>>().unwrap(),
                BTreeMap::from([(u128::MAX, 7)])
            );
        }
    }

    #[test]
    fn exact_wide_numbers_round_trip_and_deserialize_in_every_representation() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Numbers {
            signed: i128,
            unsigned: u128,
            nested: Vec<serde_json::Number>,
        }

        let precise_decimal = "12345678901234567890.123456789012345678901";
        let raw = format!(
            r#"{{"signed":{},"unsigned":{},"nested":[{},{}]}}"#,
            i128::MIN,
            u128::MAX,
            precise_decimal,
            "-0.000000000000000000000000000000000000000123456789",
        );
        let value: JsonValue = serde_json::from_str(&raw).unwrap();
        let native = Jsonb::from_value(value.clone());
        let text =
            Jsonb::from_canonical_text_vec(native.to_json_string().unwrap().into_bytes()).unwrap();
        let binary = Jsonb::from_binary_vec(encode_binary(&value).unwrap()).unwrap();
        let representations = [native, text, binary];
        let expected_nested_decimal = normalize_jsonb_number(
            &serde_json::from_str::<serde_json::Number>(
                "-0.000000000000000000000000000000000000000123456789",
            )
            .unwrap(),
        )
        .unwrap();

        for jsonb in &representations {
            let numbers = jsonb.deserialize_into::<Numbers>().unwrap();
            assert_eq!(numbers.signed, i128::MIN);
            assert_eq!(numbers.unsigned, u128::MAX);
            assert_eq!(numbers.nested[0].to_string(), precise_decimal);
            assert_eq!(
                normalize_jsonb_number(&numbers.nested[1]).unwrap().as_str(),
                expected_nested_decimal.as_str()
            );
            assert_eq!(
                jsonb.to_json_string().unwrap(),
                representations[0].to_json_string().unwrap()
            );
        }

        let rounded_f64: JsonValue = serde_json::from_str("0.10000000000000001").unwrap();
        let rounded_representations = [
            Jsonb::from_value(rounded_f64.clone()),
            Jsonb::from_canonical_text_vec(
                Jsonb::from_value(rounded_f64.clone())
                    .to_json_string()
                    .unwrap()
                    .into_bytes(),
            )
            .unwrap(),
            Jsonb::from_binary_vec(encode_binary(&rounded_f64).unwrap()).unwrap(),
        ];
        for jsonb in &rounded_representations {
            assert_eq!(jsonb.deserialize_into::<f64>().unwrap(), 0.1);
        }

        for exact in [
            i128::MIN.to_string(),
            u128::MAX.to_string(),
            precise_decimal.to_owned(),
        ] {
            let value: JsonValue = serde_json::from_str(&exact).unwrap();
            let encoded = encode_binary(&value).unwrap();
            assert_eq!(encoded[0], 9, "{exact} must use exact-number tag 9");
            validate_binary(&encoded).unwrap();
            assert_eq!(decode_binary(&encoded).unwrap(), value);
        }
    }

    #[test]
    fn exact_number_tag_preserves_old_images_and_rejects_noncanonical_payloads() {
        for (raw, expected) in [
            ("42", [3, 0, 0, 0, 0, 0, 0, 0, 42]),
            (
                "18446744073709551615",
                [4, 255, 255, 255, 255, 255, 255, 255, 255],
            ),
            ("1.5", [5, 0x3f, 0xf8, 0, 0, 0, 0, 0, 0]),
        ] {
            let value: JsonValue = serde_json::from_str(raw).unwrap();
            assert_eq!(encode_binary(&value).unwrap().as_slice(), &expected);
        }

        for (raw, expected_tag) in [
            ("42", 3),
            ("18446744073709551615", 4),
            ("10000000000000000", 3),
            ("0.1", 5),
            ("1.5", 5),
            ("1e16", 5),
            ("1e20", 5),
            ("1e-6", 5),
        ] {
            let value: JsonValue = serde_json::from_str(raw).unwrap();
            let legacy = encode_binary(&value).unwrap();
            assert_eq!(legacy[0], expected_tag);
            let expected_hash = blake3::hash(&legacy);
            let retained = Jsonb::from_binary_vec(legacy.clone()).unwrap();
            assert_eq!(retained.binary().unwrap().as_ref(), legacy);
            assert_eq!(
                blake3::hash(retained.binary().unwrap().as_ref()),
                expected_hash
            );
        }

        // Existing integer and float images can denote the same exact decimal
        // while retaining different historical JSON spellings. Keep both
        // physical encodings and render them according to their old tags.
        let integer: JsonValue = serde_json::from_str("10000000000000000").unwrap();
        let float: JsonValue = serde_json::from_str("1e16").unwrap();
        let integer_bytes = encode_binary(&integer).unwrap();
        let float_bytes = encode_binary(&float).unwrap();
        assert_eq!(integer_bytes[0], 3);
        assert_eq!(float_bytes[0], 5);
        assert_eq!(
            binary_to_json_string(&integer_bytes).unwrap(),
            "10000000000000000"
        );
        assert_eq!(binary_to_json_string(&float_bytes).unwrap(), "1e16");
        assert_eq!(
            Jsonb::from_value(integer).to_json_string().unwrap(),
            "10000000000000000"
        );
        assert_eq!(Jsonb::from_value(float).to_json_string().unwrap(), "1e16");
        assert_eq!(
            Jsonb::from_canonical_text_vec(b"10000000000000000".to_vec())
                .unwrap()
                .to_json_string()
                .unwrap(),
            "10000000000000000"
        );
        assert_eq!(
            Jsonb::from_canonical_text_vec(b"1e16".to_vec())
                .unwrap()
                .to_json_string()
                .unwrap(),
            "1e16"
        );

        let exact_payload = |number: &[u8]| {
            let mut bytes = vec![9];
            bytes.extend_from_slice(&(number.len() as u32).to_be_bytes());
            bytes.extend_from_slice(number);
            bytes
        };
        validate_binary(&exact_payload(b"18446744073709551616")).unwrap();
        for noncanonical in [b"1.0".as_slice(), b"1.5", b"01", b"1e0"] {
            assert!(
                validate_binary(&exact_payload(noncanonical)).is_err(),
                "tag 9 must reject noncanonical or legacy-representable {noncanonical:?}"
            );
        }

        let mut too_long = vec![9];
        too_long.extend_from_slice(&((MAX_EXACT_NUMBER_BYTES + 1) as u32).to_be_bytes());
        assert_eq!(
            validate_binary(&too_long),
            Err(JsonbError("JSONB number is too large"))
        );
    }

    #[test]
    fn accepted_exact_numeric_text_can_report_binary_size_overflow_without_panicking() {
        const COUNT: usize = 700_000;
        const NUMBER: &str = "18446744073709551616";

        let mut text = String::with_capacity(COUNT * (NUMBER.len() + 1) + 2);
        text.push('[');
        for index in 0..COUNT {
            if index != 0 {
                text.push(',');
            }
            text.push_str(NUMBER);
        }
        text.push(']');
        assert!(text.len() < MAX_BYTES);

        // The valid JSON text is smaller than the text bound, while tag 9
        // adds five framing bytes to each number and exceeds the binary cap.
        let jsonb = Jsonb::from_canonical_text_vec(text.into_bytes()).unwrap();
        assert_eq!(
            jsonb.binary().unwrap_err(),
            JsonbError("JSONB value is too large")
        );
        assert_eq!(
            jsonb.binary_len().unwrap_err(),
            JsonbError("JSONB value is too large")
        );
    }

    #[test]
    fn tiny_exponent_arrays_do_not_drive_unbounded_size_estimates_or_render_reserves() {
        let number: serde_json::Number = serde_json::from_str("1e-16383").unwrap();
        const COUNT: usize = 700_000;
        let jsonb = Jsonb::from_value(JsonValue::Array(vec![JsonValue::Number(number); COUNT]));

        assert_eq!(jsonb.estimated_binary_size(), MAX_BYTES as u64);
        assert_eq!(jsonb.initial_json_capacity(), 0);
        assert_eq!(
            jsonb.binary_len().unwrap_err(),
            JsonbError("JSONB value is too large")
        );
    }

    #[test]
    fn canonical_text_accepts_only_exact_and_legacy_float_spellings() {
        for (plain, compact) in [("100000000000000000000", "1e20"), ("0.000001", "1e-6")] {
            let plain_text = format!(r#"{{"number":{plain}}}"#);
            let compact_text = format!(r#"{{"number":{compact}}}"#);
            let plain_text = Jsonb::from_canonical_text_vec(plain_text.into_bytes()).unwrap();
            let compact_text = Jsonb::from_canonical_text_vec(compact_text.into_bytes()).unwrap();

            let value: JsonValue =
                serde_json::from_str(&format!(r#"{{"number":{plain}}}"#)).unwrap();
            let native = Jsonb::from_value(value.clone());
            let binary = Jsonb::from_binary_vec(encode_binary(&value).unwrap()).unwrap();
            let expected = format!(r#"{{"number":{compact}}}"#);

            for jsonb in [&plain_text, &compact_text, &native, &binary] {
                assert_eq!(jsonb.to_json_string().unwrap(), expected);
            }
            assert_eq!(plain_text, compact_text);
        }

        for noncanonical in ["1.000e20", "1e+20", "1.0e20"] {
            assert!(
                Jsonb::from_canonical_text_vec(noncanonical.as_bytes().to_vec()).is_err(),
                "accepted noncanonical legacy spelling {noncanonical}"
            );
        }
    }

    #[test]
    fn max_binary_legacy_subnormal_array_renders_with_compact_old_spelling() {
        let count = (MAX_BYTES - 5) / 9;
        let number = (-f64::from_bits(1)).to_be_bytes();
        let mut binary = Vec::with_capacity(5 + count * 9);
        binary.push(7);
        binary.extend_from_slice(&(count as u32).to_be_bytes());
        for _ in 0..count {
            binary.push(5);
            binary.extend_from_slice(&number);
        }
        assert!(binary.len() <= MAX_BYTES);

        let rendered = binary_to_json_string(&binary).unwrap();
        assert_eq!(rendered.len(), count * 8 + 1);
        assert!(rendered.starts_with("[-5e-324,-5e-324,"));
        assert!(rendered.len() <= MAX_RENDERED_BYTES);
    }

    #[test]
    fn maximum_codec_depth_deserializes_in_every_representation() {
        let mut value = JsonValue::Null;
        for _ in 0..MAX_DEPTH {
            value = JsonValue::Array(vec![value]);
        }
        let native = Jsonb::from_value(value.clone());
        let text =
            Jsonb::from_canonical_text_vec(native.to_json_string().unwrap().into_bytes()).unwrap();
        let binary = Jsonb::from_binary(encode_binary(&value).unwrap().into()).unwrap();
        for jsonb in [native, text, binary] {
            assert_eq!(jsonb.deserialize_into::<JsonValue>().unwrap(), value);
            assert_eq!(jsonb.as_value(), &value);
            assert_eq!(jsonb.clone().into_value(), value);
            assert_eq!(
                jsonb.binary().unwrap().as_ref(),
                encode_binary(&value).unwrap()
            );
            let mut appended = Vec::new();
            jsonb.append_binary(&mut appended).unwrap();
            assert_eq!(appended, encode_binary(&value).unwrap());
            assert_eq!(jsonb, Jsonb::from_value(value.clone()));
        }
    }

    #[test]
    fn direct_binary_deserialization_matches_serde_json_for_generic_types() {
        use std::collections::BTreeMap;

        #[derive(Debug, Deserialize, PartialEq)]
        struct Envelope {
            nested: Nested,
        }

        #[derive(Debug, Deserialize, PartialEq)]
        struct Nested {
            signed: i64,
            unsigned: u64,
            fraction: f64,
            absent: Option<String>,
            present: Option<String>,
            wrapped: Count,
            unit: UnitKind,
            event: Event,
            numeric_keys: BTreeMap<i64, String>,
            bytes: ByteString,
        }

        #[derive(Debug, Deserialize, PartialEq)]
        struct Count(u64);

        #[derive(Debug, Deserialize, PartialEq)]
        enum UnitKind {
            Ready,
        }

        #[derive(Debug, Deserialize, PartialEq)]
        enum Event {
            Count(u64),
        }

        #[derive(Debug, PartialEq)]
        struct ByteString(Vec<u8>);

        impl<'de> Deserialize<'de> for ByteString {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct BytesVisitor;

                impl<'de> serde::de::Visitor<'de> for BytesVisitor {
                    type Value = ByteString;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                        formatter.write_str("a UTF-8 string or a byte sequence")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        Ok(ByteString(value.as_bytes().to_vec()))
                    }

                    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        Ok(ByteString(value.as_bytes().to_vec()))
                    }

                    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        Ok(ByteString(value.into_bytes()))
                    }

                    fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        Ok(ByteString(value.to_vec()))
                    }

                    fn visit_borrowed_bytes<E>(self, value: &'de [u8]) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        Ok(ByteString(value.to_vec()))
                    }

                    fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        Ok(ByteString(value))
                    }
                }

                deserializer.deserialize_bytes(BytesVisitor)
            }
        }

        let value = serde_json::json!({
            "nested": {
                "signed": -42,
                "unsigned": u64::MAX,
                "fraction": 1.25,
                "absent": null,
                "present": "there",
                "wrapped": 19,
                "unit": "Ready",
                "event": {"Count": u64::MAX},
                "numeric_keys": {"-8": "negative", "12": "positive"},
                "bytes": "raw β bytes",
                "ignored": {"deep": [true, null, {"n": 3}]}
            }
        });
        let expected: Envelope = serde_json::from_value(value.clone()).unwrap();
        let text = Jsonb::from_value(value.clone())
            .to_json_string()
            .unwrap()
            .into_bytes();
        let representations = [
            Jsonb::from_value(value.clone()),
            Jsonb::from_canonical_text_vec(text).unwrap(),
            Jsonb::from_binary(encode_binary(&value).unwrap().into()).unwrap(),
        ];

        for jsonb in &representations {
            assert_eq!(jsonb.deserialize_into::<Envelope>().unwrap(), expected);
        }
        assert!(matches!(
            &representations[2].0,
            JsonbRepr::Binary(binary) if binary.value.get().is_none()
        ));
        let _debug = format!("{:?}", representations[2]);
        assert!(matches!(
            &representations[2].0,
            JsonbRepr::Binary(binary) if binary.value.get().is_none()
        ));

        #[derive(Debug, Deserialize)]
        struct WrongEnvelope {
            #[serde(rename = "nested")]
            _nested: WrongNested,
        }

        #[derive(Debug, Deserialize)]
        struct WrongNested {
            #[serde(rename = "signed")]
            _signed: String,
        }

        let field_error = JsonbError("JSONB value does not match the requested type");
        assert_eq!(
            representations[0]
                .deserialize_into::<WrongEnvelope>()
                .unwrap_err(),
            field_error
        );
        assert_eq!(
            representations[1]
                .deserialize_into::<WrongEnvelope>()
                .unwrap_err(),
            field_error
        );
        assert_eq!(
            representations[2]
                .deserialize_into::<WrongEnvelope>()
                .unwrap_err(),
            field_error
        );
    }

    #[test]
    fn binary_reader_rejects_impossible_container_counts_before_visiting() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        static SEQUENCE_VISITS: AtomicUsize = AtomicUsize::new(0);

        struct CountedSequence;

        impl<'de> Deserialize<'de> for CountedSequence {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct SequenceVisitor;

                impl<'de> serde::de::Visitor<'de> for SequenceVisitor {
                    type Value = CountedSequence;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                        formatter.write_str("a sequence")
                    }

                    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
                    where
                        A: serde::de::SeqAccess<'de>,
                    {
                        SEQUENCE_VISITS.fetch_add(1, Ordering::SeqCst);
                        while let Some(serde::de::IgnoredAny) = sequence.next_element()? {}
                        Ok(CountedSequence)
                    }
                }

                deserializer.deserialize_seq(SequenceVisitor)
            }
        }

        SEQUENCE_VISITS.store(0, Ordering::SeqCst);
        let impossible = [7, 0, 0, 0, 2];
        assert!(deserialize_binary_into::<CountedSequence>(&impossible).is_err());
        assert_eq!(SEQUENCE_VISITS.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn binary_container_visitors_must_consume_nested_values() {
        #[derive(Debug)]
        struct PartialSequence;

        impl<'de> Deserialize<'de> for PartialSequence {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct FirstOnly;

                impl<'de> serde::de::Visitor<'de> for FirstOnly {
                    type Value = PartialSequence;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                        formatter.write_str("a sequence")
                    }

                    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
                    where
                        A: serde::de::SeqAccess<'de>,
                    {
                        let _: Option<serde::de::IgnoredAny> = sequence.next_element()?;
                        Ok(PartialSequence)
                    }
                }

                deserializer.deserialize_seq(FirstOnly)
            }
        }

        #[derive(Debug, Deserialize)]
        struct NestedPartial {
            #[serde(rename = "nested")]
            _nested: PartialSequence,
        }

        let value = serde_json::json!({"nested": [1, 2]});
        let jsonb = Jsonb::from_binary(encode_binary(&value).unwrap().into()).unwrap();
        assert!(jsonb.deserialize_into::<NestedPartial>().is_err());
    }

    #[test]
    fn from_binary_rejects_invalid_tags_duplicate_keys_and_utf8() {
        let invalid_tag = vec![9];
        assert!(Jsonb::from_binary(invalid_tag.into()).is_err());

        let mut duplicate_keys = vec![8];
        duplicate_keys.extend_from_slice(&2_u32.to_be_bytes());
        encode_string(&mut duplicate_keys, "same").unwrap();
        duplicate_keys.push(0);
        encode_string(&mut duplicate_keys, "same").unwrap();
        duplicate_keys.push(0);
        assert!(Jsonb::from_binary(duplicate_keys.into()).is_err());

        let mut invalid_utf8 = vec![6];
        invalid_utf8.extend_from_slice(&1_u32.to_be_bytes());
        invalid_utf8.push(0xff);
        assert!(Jsonb::from_binary(invalid_utf8.into()).is_err());
    }

    #[test]
    fn capped_binary_renderer_stops_before_exceeding_its_output_bound() {
        let value = serde_json::json!({"name": "a string longer than the cap"});
        let encoded = encode_binary(&value).unwrap();
        let mut rendered = Vec::new();
        let mut output = CappedJsonOutput {
            bytes: &mut rendered,
            max_bytes: 8,
        };
        let mut reader = Reader {
            bytes: &encoded,
            offset: 0,
        };

        assert_eq!(
            reader.write_json(&mut output, 0),
            Err(JsonbError("JSONB JSON rendering is too large"))
        );
        assert!(rendered.len() <= 8);
    }

    #[test]
    fn borrowed_binary_jsonb_renders_nested_canonical_json() {
        let value = serde_json::json!({
            "array": [null, true, {"escaped": "line\n\"quoted\"", "number": 1.5}],
            "object": {"a": -7, "z": 18_446_744_073_709_551_615_u64}
        });
        let bytes = encode_binary(&value).unwrap();

        assert_eq!(
            binary_to_json_string(&bytes).unwrap(),
            r#"{"array":[null,true,{"escaped":"line\n\"quoted\"","number":1.5}],"object":{"a":-7,"z":18446744073709551615}}"#
        );
    }

    #[test]
    fn canonical_json_append_is_equivalent_across_representations() {
        let value: JsonValue = serde_json::from_str(
            r#"{"z":[1.0,"line\nquoted\"",null],"a":{"β":1.5,"control":"\u000b"}}"#,
        )
        .unwrap();
        let expected = r#"{"a":{"control":"\u000b","β":1.5},"z":[1,"line\nquoted\"",null]}"#;
        let values = [
            Jsonb::from_value(value.clone()),
            Jsonb::from_binary(encode_binary(&value).unwrap().into()).unwrap(),
        ];

        for jsonb in &values {
            let mut output = b"prefix:".to_vec();
            jsonb.append_canonical_json(&mut output).unwrap();
            assert_eq!(&output[b"prefix:".len()..], expected.as_bytes());
            assert_eq!(jsonb.to_json_string().unwrap(), expected);
        }
        assert!(matches!(
            &values[1].0,
            JsonbRepr::Binary(binary) if binary.value.get().is_none()
        ));

        let text_array =
            Jsonb::from_text_array(vec!["alpha".to_owned(), "line\nβ".to_owned()]).unwrap();
        let mut output = Vec::new();
        text_array.append_canonical_json(&mut output).unwrap();
        assert_eq!(output, r#"["alpha","line\nβ"]"#.as_bytes());
        assert!(matches!(
            &text_array.0,
            JsonbRepr::TextArray(array) if array.value.get().is_none()
        ));
    }

    #[test]
    fn canonical_json_append_rolls_back_on_invalid_value() {
        let jsonb = Jsonb::from_value(JsonValue::String("bad\0value".to_owned()));
        let mut output = b"prefix".to_vec();
        assert_eq!(
            jsonb.append_canonical_json(&mut output),
            Err(JsonbError("JSONB string contains a NUL"))
        );
        assert_eq!(output, b"prefix");
    }

    #[test]
    fn canonical_json_text_validator_accepts_renderer_output() {
        let cases = [
            serde_json::json!(null),
            serde_json::json!(false),
            serde_json::json!(18_446_744_073_709_551_615_u64),
            serde_json::from_str::<JsonValue>("1.0").unwrap(),
            serde_json::json!(["quote\"", "slash\\", "line\n", "\u{000b}", "β"]),
            serde_json::json!({"a": {"nested": true}, "z": [1.5, -7]}),
        ];

        for value in cases {
            let jsonb = Jsonb::from_value(value);
            let mut encoded = Vec::new();
            jsonb.append_canonical_json(&mut encoded).unwrap();
            assert_eq!(
                validate_canonical_json_text(&encoded).unwrap().as_bytes(),
                encoded
            );
        }

        assert!(validate_canonical_json_text(br#"{"\n":0,"a":1}"#).is_ok());
    }

    #[test]
    fn canonical_json_text_validator_rejects_noncanonical_text() {
        let cases: &[&[u8]] = &[
            b" null",
            b"null ",
            br#"{"b":1,"a":2}"#,
            br#"{"a":1,"a":2}"#,
            br#"{"a":1,}"#,
            br#"[1,]"#,
            br#""\/""#,
            br#""\u0061""#,
            br#""\u000a""#,
            br#""\u000B""#,
            br#""\u0000""#,
            b"1.0",
            b"-0",
            b"1e0",
            b"01",
            b"truefalse",
            &[b'"', 0xff, b'"'],
        ];
        for encoded in cases {
            assert!(
                validate_canonical_json_text(encoded).is_err(),
                "accepted noncanonical JSON: {:?}",
                String::from_utf8_lossy(encoded)
            );
        }

        assert!(validate_canonical_json_text(br#""\u000b""#).is_ok());
        assert!(validate_canonical_json_text(br#""quote\"slash\\line\n""#).is_ok());
    }

    #[test]
    fn canonical_json_text_validator_preserves_wide_integer_digits() {
        for encoded in [
            "0",
            "1",
            "-1",
            "-9223372036854775808",
            "9223372036854775807",
            "18446744073709551615",
            "18446744073709551616",
            "-9223372036854775809",
            "170141183460469231731687303715884105727",
            "340282366920938463463374607431768211455",
            "9007199254740992",
            "9007199254740993",
        ] {
            assert!(
                validate_canonical_json_text(encoded.as_bytes()).is_ok(),
                "rejected canonical integer {encoded}"
            );
        }
        for encoded in [
            "-0",
            "+0",
            "00",
            "01",
            "-01",
            "-",
            "18446744073709551616.0",
            "-9223372036854775809.0",
        ] {
            assert!(
                validate_canonical_json_text(encoded.as_bytes()).is_err(),
                "accepted noncanonical integer {encoded}"
            );
        }
    }

    #[test]
    fn exact_jsonb_numeric_bounds_match_postgresql_precision_and_scale() {
        let max_integer: serde_json::Number =
            serde_json::from_str(&format!("1e{}", MAX_INTEGER_DIGITS - 1)).unwrap();
        let max_integer = normalize_jsonb_number(&max_integer).unwrap();
        assert_eq!(max_integer.as_str().len(), MAX_INTEGER_DIGITS as usize);
        assert!(
            normalize_jsonb_number(
                &serde_json::from_str::<serde_json::Number>("1e131072").unwrap()
            )
            .is_err()
        );

        let max_scale = normalize_jsonb_number(
            &serde_json::from_str::<serde_json::Number>("1e-16383").unwrap(),
        )
        .unwrap();
        assert_eq!(max_scale.as_str().len(), 16_385);
        assert!(
            normalize_jsonb_number(
                &serde_json::from_str::<serde_json::Number>("1e-16384").unwrap()
            )
            .is_err()
        );
    }

    #[test]
    fn canonical_json_text_validator_enforces_depth_limit() {
        let accepted = format!("{}null{}", "[".repeat(MAX_DEPTH), "]".repeat(MAX_DEPTH));
        assert!(validate_canonical_json_text(accepted.as_bytes()).is_ok());

        let rejected = format!(
            "{}null{}",
            "[".repeat(MAX_DEPTH + 1),
            "]".repeat(MAX_DEPTH + 1)
        );
        assert_eq!(
            validate_canonical_json_text(rejected.as_bytes()),
            Err(JsonbError("JSONB value exceeds its nesting limit"))
        );
    }

    #[test]
    fn borrowed_binary_jsonb_rejects_corruption_and_noncanonical_input() {
        let canonical = encode_binary(&serde_json::json!({"a": 1})).unwrap();

        let mut truncated = canonical.clone();
        truncated.pop();
        assert_eq!(
            binary_to_json_string(&truncated),
            Err(JsonbError("JSONB value is truncated"))
        );

        let mut trailing = canonical;
        trailing.push(0);
        assert_eq!(
            binary_to_json_string(&trailing),
            Err(JsonbError("JSONB value has trailing bytes"))
        );

        let mut unordered = vec![8];
        unordered.extend_from_slice(&2_u32.to_be_bytes());
        encode_string(&mut unordered, "z").unwrap();
        unordered.push(0);
        encode_string(&mut unordered, "a").unwrap();
        unordered.push(0);
        assert_eq!(
            binary_to_json_string(&unordered),
            Err(JsonbError("JSONB keys are not canonical"))
        );

        let mut noncanonical_integer = vec![4];
        noncanonical_integer.extend_from_slice(&1_u64.to_be_bytes());
        assert_eq!(
            binary_to_json_string(&noncanonical_integer),
            Err(JsonbError("JSONB number is not canonical"))
        );
    }

    #[test]
    fn text_array_builds_canonical_binary_without_materializing_a_dom() {
        let jsonb = Jsonb::from_text_array(vec!["alpha".to_owned(), "βeta".to_owned()]).unwrap();
        assert!(jsonb.is_binary());
        assert!(matches!(
            &jsonb.0,
            JsonbRepr::TextArray(array) if array.value.get().is_none()
        ));
        validate_binary(&jsonb.binary().unwrap()).unwrap();
        assert!(matches!(
            &jsonb.0,
            JsonbRepr::TextArray(array) if array.value.get().is_none()
        ));
        assert_eq!(jsonb.as_value(), &serde_json::json!(["alpha", "βeta"]));
    }

    #[test]
    fn canonical_text_render_and_equality_do_not_materialize_binary_or_dom() {
        let left = Jsonb::from_canonical_text_vec(br#"{"a":1}"#.to_vec()).unwrap();
        let right = Jsonb::from_canonical_text_vec(br#"{"a":1}"#.to_vec()).unwrap();

        assert_eq!(left.to_json_string().unwrap(), r#"{"a":1}"#);
        assert_eq!(left, right);
        for value in [&left, &right] {
            assert!(matches!(
                &value.0,
                JsonbRepr::CanonicalText(text)
                    if text.value.get().is_none() && text.binary.get().is_none()
            ));
        }
    }

    #[test]
    fn binary_jsonb_can_render_text_larger_than_the_binary_limit() {
        let escaped = "\u{0001}".repeat(2_800_000);
        let binary = encode_binary(&JsonValue::String(escaped)).unwrap();
        assert!(binary.len() < MAX_BYTES);
        let jsonb = Jsonb::from_binary(binary.into()).unwrap();

        let rendered = jsonb.to_json_string().unwrap();
        assert!(rendered.len() > MAX_BYTES);
        assert!(rendered.starts_with("\"\\u0001\\u0001"));
        assert!(rendered.ends_with("\\u0001\""));
    }

    #[test]
    fn text_array_rejects_non_json_strings() {
        assert_eq!(
            Jsonb::from_text_array(vec!["bad\0value".to_owned()]).unwrap_err(),
            JsonbError("JSONB string contains a NUL")
        );
    }

    #[test]
    fn binary_jsonb_rejects_noncanonical_object_order() {
        let mut bytes = vec![8];
        bytes.extend_from_slice(&2_u32.to_be_bytes());
        encode_string(&mut bytes, "z").unwrap();
        bytes.push(0);
        encode_string(&mut bytes, "a").unwrap();
        bytes.push(0);
        assert_eq!(
            validate_binary(&bytes),
            Err(JsonbError("JSONB keys are not canonical"))
        );
    }

    #[test]
    fn binary_jsonb_rejects_small_integer_with_unsigned_tag() {
        let mut bytes = vec![4];
        bytes.extend_from_slice(&1_u64.to_be_bytes());
        assert_eq!(
            validate_binary(&bytes),
            Err(JsonbError("JSONB number is not canonical"))
        );
        assert_eq!(
            decode_binary(&bytes),
            Err(JsonbError("JSONB number is not canonical"))
        );
    }

    #[test]
    fn equality_is_canonical_across_the_full_representation_matrix() {
        let integer: JsonValue = serde_json::from_str("1").unwrap();
        let decimal: JsonValue = serde_json::from_str("1.0").unwrap();
        assert_ne!(
            integer, decimal,
            "the test inputs must retain distinct spellings"
        );

        let values = [
            Jsonb::from_value(integer.clone()),
            Jsonb::from_value(decimal.clone()),
            Jsonb::from_binary(encode_binary(&integer).unwrap().into()).unwrap(),
            Jsonb::from_binary(encode_binary(&decimal).unwrap().into()).unwrap(),
        ];

        for (left_index, left) in values.iter().enumerate() {
            for (right_index, right) in values.iter().enumerate() {
                assert_eq!(
                    left, right,
                    "representation matrix entry ({left_index}, {right_index})"
                );
            }
        }

        for left in &values {
            for middle in &values {
                for right in &values {
                    assert!(left == middle && middle == right && left == right);
                }
            }
        }

        for value in &values {
            assert_eq!(value, &integer);
            assert_eq!(value, &decimal);
            assert_eq!(&integer, value);
            assert_eq!(&decimal, value);
        }

        for binary in &values[2..] {
            assert!(matches!(
                &binary.0,
                JsonbRepr::Binary(binary) if binary.value.get().is_none()
            ));
        }
    }

    #[test]
    fn equality_normalizes_nested_numeric_tokens_without_rewriting_legacy_text() {
        let integer_json = r#"{"nested":[{"text":"1e16","value":10000000000000000}]}"#;
        let float_json = r#"{"nested":[{"text":"1e16","value":1e16}]}"#;
        let integer_value = serde_json::from_str::<JsonValue>(integer_json).unwrap();
        let float_value = serde_json::from_str::<JsonValue>(float_json).unwrap();
        let integers = [
            Jsonb::from_value(integer_value.clone()),
            Jsonb::from_binary_vec(encode_binary(&integer_value).unwrap()).unwrap(),
            Jsonb::from_canonical_text_vec(integer_json.as_bytes().to_vec()).unwrap(),
        ];
        let floats = [
            Jsonb::from_value(float_value.clone()),
            Jsonb::from_binary_vec(encode_binary(&float_value).unwrap()).unwrap(),
            Jsonb::from_canonical_text_vec(float_json.as_bytes().to_vec()).unwrap(),
        ];

        assert_ne!(
            integers[0].to_json_string().unwrap(),
            floats[0].to_json_string().unwrap()
        );
        for integer in &integers {
            for float in &floats {
                assert_eq!(integer, float);
            }
        }
        for left in &integers {
            for right in &integers {
                assert_eq!(left, right);
            }
        }

        let different_string: JsonValue =
            serde_json::from_str(r#"{"nested":[{"text":"10000000000000000","value":1e16}]}"#)
                .unwrap();
        assert_ne!(integers[0], Jsonb::from_value(different_string));

        let different_decimal: JsonValue =
            serde_json::from_str(r#"{"nested":[{"text":"1e16","value":1.2500000000000001}]}"#)
                .unwrap();
        let exact_decimal: JsonValue =
            serde_json::from_str(r#"{"nested":[{"text":"1e16","value":1.25}]}"#).unwrap();
        assert_ne!(
            Jsonb::from_value(different_decimal),
            Jsonb::from_value(exact_decimal)
        );
    }

    #[test]
    fn sql_equality_key_unifies_legacy_float_spelling_and_exact_decimal() {
        let legacy = r#"{"values":[-1.23456789e-40,"-1.23456789e-40"]}"#;
        let exact =
            r#"{"values":[-0.000000000000000000000000000000000000000123456789,"-1.23456789e-40"]}"#;
        let adjacent =
            r#"{"values":[-0.000000000000000000000000000000000000000123456788,"-1.23456789e-40"]}"#;

        assert_eq!(
            jsonb_equality_key(legacy).unwrap(),
            jsonb_equality_key(exact).unwrap()
        );
        assert_ne!(
            jsonb_equality_key(exact).unwrap(),
            jsonb_equality_key(adjacent).unwrap()
        );
        assert_eq!(
            jsonb_equality_key(legacy).unwrap(),
            r#"{"values":[-1.23456789e-40,"-1.23456789e-40"]}"#
        );
    }

    #[test]
    fn sql_equality_key_copies_canonical_numbers_and_normalizes_aliases() {
        assert_eq!(
            jsonb_equality_key(r#"{"values":[42,42.0,4.2e1,-0,0.000,"42.0"]}"#).unwrap(),
            r#"{"values":[42,42,42,0,0,"42.0"]}"#
        );
        assert_eq!(
            jsonb_equality_key("[1000,1e3,100,1e2,0.0001,1e-4]").unwrap(),
            "[1e3,1e3,100,100,1e-4,1e-4]"
        );
    }

    #[test]
    fn sql_equality_key_keeps_legacy_subnormal_arrays_compact() {
        let legacy_subnormal =
            Jsonb::from_value(serde_json::from_str::<JsonValue>("1e-323").unwrap());
        assert_eq!(legacy_subnormal.binary().unwrap().as_ref()[0], 5);
        let number = serde_json::Number::from_f64(1e-323).unwrap();
        let values = (0..500_000)
            .map(|_| JsonValue::Number(number.clone()))
            .collect();
        let mut object = serde_json::Map::new();
        object.insert("n".to_owned(), JsonValue::Array(values));
        let legacy_array = Jsonb::from_value(JsonValue::Object(object));

        let binary = legacy_array.binary().unwrap();
        assert!(binary.len() <= MAX_BYTES);
        let rendered = legacy_array.to_json_string().unwrap();
        let key = jsonb_equality_key(&rendered).unwrap();
        assert_eq!(key, rendered);
    }

    #[test]
    fn sql_equality_key_unifies_compact_scientific_and_exact_subnormal_spelling() {
        let decimal = format!("0.{}1", "0".repeat(322));

        assert_eq!(
            jsonb_equality_key("1e-323").unwrap(),
            jsonb_equality_key(&decimal).unwrap()
        );
    }

    #[test]
    fn sql_equality_key_does_not_rewrite_legacy_jsonb_rendering() {
        let value = Jsonb::from_value(serde_json::from_str("-1.23456789e-40").unwrap());

        assert_eq!(value.binary().unwrap().as_ref()[0], 5);
        assert_eq!(value.to_json_string().unwrap(), "-1.23456789e-40");
        assert_eq!(
            jsonb_equality_key(&value.to_json_string().unwrap()).unwrap(),
            "-1.23456789e-40"
        );
        assert_eq!(value.to_json_string().unwrap(), "-1.23456789e-40");
    }

    #[test]
    fn sql_equality_key_rejects_jsonb_nul_escapes() {
        assert_eq!(
            jsonb_equality_key(r#"{"key":"\u0000"}"#).unwrap_err(),
            JsonbError("PostgreSQL JSONB does not support the Unicode NUL escape (\\u0000)")
        );
    }

    #[test]
    fn sql_equality_key_is_bounded_by_jsonb_depth() {
        let too_deep = format!(
            "{}0{}",
            "[".repeat(MAX_DEPTH + 1),
            "]".repeat(MAX_DEPTH + 1)
        );
        assert_eq!(
            jsonb_equality_key(&too_deep).unwrap_err(),
            JsonbError("JSONB nesting is too deep")
        );

        let mut output = b"prefix:".to_vec();
        let malformed_after_expansion = format!(
            "[1e1,{}0{}]",
            "[".repeat(MAX_DEPTH + 1),
            "]".repeat(MAX_DEPTH + 1)
        );
        assert_eq!(
            append_jsonb_equality_key(&mut output, &malformed_after_expansion).unwrap_err(),
            JsonbError("JSONB nesting is too deep")
        );
        assert_eq!(output, b"prefix:");
    }

    #[test]
    fn sql_number_normalization_checks_remaining_budget_before_expansion() {
        let number = serde_json::from_str::<serde_json::Number>("1e-100").unwrap();
        assert_eq!(
            normalize_jsonb_number_with_limit(&number, 10).unwrap_err(),
            JsonbError("JSONB SQL equality key is too large")
        );
    }
}
