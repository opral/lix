use crate::LixError;

pub(crate) fn parse_json_pointer(pointer: &str) -> Result<Vec<String>, LixError> {
    if pointer.is_empty() {
        return Ok(Vec::new());
    }
    if !pointer.starts_with('/') {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!("invalid JSON pointer '{pointer}'"),
        ));
    }
    pointer[1..]
        .split('/')
        .map(decode_json_pointer_segment)
        .collect()
}

pub(crate) fn format_json_pointer(segments: &[String]) -> String {
    if segments.is_empty() {
        return String::new();
    }
    format!(
        "/{}",
        segments
            .iter()
            .map(|segment| segment.replace('~', "~0").replace('/', "~1"))
            .collect::<Vec<_>>()
            .join("/")
    )
}

pub(crate) fn top_level_property_name(pointer: &str) -> Result<Option<String>, LixError> {
    if pointer.is_empty() {
        return Ok(None);
    }
    if !pointer.starts_with('/') {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!("invalid JSON pointer '{pointer}'"),
        ));
    }
    let segment = pointer[1..].split('/').next().unwrap_or_default();
    Ok(Some(decode_json_pointer_segment(segment)?))
}

fn decode_json_pointer_segment(segment: &str) -> Result<String, LixError> {
    let mut out = String::new();
    let mut chars = segment.chars();
    while let Some(ch) = chars.next() {
        if ch != '~' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('0') => out.push('~'),
            Some('1') => out.push('/'),
            _ => {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    "invalid JSON pointer escape",
                ))
            }
        }
    }
    Ok(out)
}
