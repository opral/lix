use serde_json::Value;

use super::common::{
    canonical_text, parse_jsonb, rewrite_value, semantic_diff_count, value_at_path, JsonbCodec,
    PathSegment,
};

pub struct CanonicalText;

impl JsonbCodec for CanonicalText {
    const NAME: &'static str = "canonical_text";

    fn encode(value: &Value) -> Result<Vec<u8>, String> {
        canonical_text(value)
    }

    fn decode(bytes: &[u8]) -> Result<Value, String> {
        let raw = std::str::from_utf8(bytes).map_err(|error| error.to_string())?;
        let value = parse_jsonb(raw)?;
        if canonical_text(&value)? != bytes {
            return Err("canonical JSON text has a non-canonical spelling".to_owned());
        }
        Ok(value)
    }

    fn project_path(bytes: &[u8], path: &[PathSegment]) -> Result<Option<Vec<u8>>, String> {
        let value = Self::decode(bytes)?;
        value_at_path(&value, path).map(canonical_text).transpose()
    }

    fn rewrite_path(
        bytes: &[u8],
        path: &[PathSegment],
        replacement: &Value,
    ) -> Result<Vec<u8>, String> {
        let mut value = Self::decode(bytes)?;
        rewrite_value(&mut value, path, replacement.clone())?;
        canonical_text(&value)
    }

    fn diff_count(before: &[u8], after: &[u8]) -> Result<usize, String> {
        Ok(semantic_diff_count(
            &Self::decode(before)?,
            &Self::decode(after)?,
        ))
    }
}
