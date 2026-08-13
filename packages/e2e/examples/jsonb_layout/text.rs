use serde_json::Value;

use super::common::{
    JsonbCodec, PathSegment, canonical_text, normalize_jsonb, parse_jsonb, rewrite_value,
    semantic_diff_count, value_at_path,
};

pub struct CanonicalText;

impl JsonbCodec for CanonicalText {
    const NAME: &'static str = "canonical_text";

    fn encode(value: &Value) -> Result<Vec<u8>, String> {
        let mut value = value.clone();
        normalize_jsonb(&mut value)?;
        canonical_text(&value)
    }

    fn decode(bytes: &[u8]) -> Result<Value, String> {
        let raw = std::str::from_utf8(bytes).map_err(|error| error.to_string())?;
        let value = parse_jsonb(raw)?;
        let canonical = canonical_text(&value)?;
        if canonical != bytes {
            let first = canonical
                .iter()
                .zip(bytes)
                .position(|(left, right)| left != right)
                .unwrap_or(canonical.len().min(bytes.len()));
            return Err(format!(
                "canonical JSON text has a non-canonical spelling at byte {first}: encoded={} decoded={}",
                String::from_utf8_lossy(bytes.get(first.saturating_sub(24)..).unwrap_or(bytes)),
                String::from_utf8_lossy(
                    canonical
                        .get(first.saturating_sub(24)..)
                        .unwrap_or(&canonical)
                ),
            ));
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
        normalize_jsonb(&mut value)?;
        canonical_text(&value)
    }

    fn diff_count(before: &[u8], after: &[u8]) -> Result<usize, String> {
        Ok(semantic_diff_count(
            &Self::decode(before)?,
            &Self::decode(after)?,
        ))
    }
}
