//! Compare retained number spellings without rounding them through JSONB.

use lix::plugin as sdk;

pub(crate) fn rows_have_equal_numbers(before: &sdk::TypedRow, after: &sdk::TypedRow) -> bool {
    if !matches!(before.get("kind"), Some(sdk::TypedValue::Text(kind)) if kind == "number") {
        return true;
    }
    let spelling = |row: &sdk::TypedRow| match row.get("scalar_text") {
        Some(sdk::TypedValue::Text(text)) => Some(text.clone()),
        _ => match row.get("scalar_json") {
            Some(sdk::TypedValue::Jsonb(value)) => value.to_json_string().ok(),
            _ => None,
        },
    };
    let (Some(before), Some(after)) = (spelling(before), spelling(after)) else {
        return false;
    };
    equal_numbers(&before, &after)
}

fn equal_numbers(before: &str, after: &str) -> bool {
    if before == after {
        return true;
    }
    match (normalized_number(before), normalized_number(after)) {
        (Some(before), Some(after)) => before == after,
        // An exponent beyond i128 is still allowed by JSON grammar. Treat
        // distinct spellings conservatively as content, never silently as
        // formatting just because the native numeric representation underflows.
        _ => false,
    }
}

fn normalized_number(text: &str) -> Option<(bool, String, i128)> {
    let (negative, unsigned) = text.strip_prefix('-').map_or((false, text), |n| (true, n));
    let (mantissa, exponent) = unsigned
        .split_once(['e', 'E'])
        .map_or((unsigned, "0"), |parts| parts);
    let fraction_len = mantissa
        .split_once('.')
        .map_or(0, |(_, fraction)| fraction.len());
    let mut digits = mantissa.chars().filter(|&c| c != '.').collect::<String>();
    if !digits.bytes().all(|c| c.is_ascii_digit()) || digits.is_empty() {
        return None;
    }
    let first = digits.bytes().position(|c| c != b'0');
    let Some(first) = first else {
        return Some((false, String::new(), 0));
    };
    let trailing = digits.bytes().rev().take_while(|&c| c == b'0').count();
    digits.truncate(digits.len() - trailing);
    digits.drain(..first);
    let scale = exponent
        .parse::<i128>()
        .ok()?
        .checked_sub(fraction_len as i128)?
        .checked_add(trailing as i128)?;
    Some((negative, digits, scale))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{ChangeEffect, Document, FileEdit, IdNamespace};

    #[test]
    fn exact_decimal_equivalence_does_not_round() {
        for (a, b) in [
            ("1", "1.000e+0"),
            ("-0", "0e-9999"),
            ("12300", "1.2300e4"),
            ("0.001", "10e-4"),
            ("1e-9999", "10e-10000"),
        ] {
            assert!(equal_numbers(a, b), "{a} vs {b}");
        }
        for (a, b) in [
            ("1e-9999", "2e-9999"),
            ("0", "1e-9999"),
            ("0.123456789012345678901", "0.123456789012345678902"),
            ("18446744073709551616", "18446744073709551617"),
            ("-1", "1"),
        ] {
            assert!(!equal_numbers(a, b), "{a} vs {b}");
        }
    }

    #[test]
    fn rounded_numeric_changes_are_content_and_roundtrip_exactly() {
        for (before, after, effect) in [
            ("1e-9999", "2e-9999", ChangeEffect::Content),
            ("1e-9999", "10e-10000", ChangeEffect::FormatOnly),
            (
                "18446744073709551616",
                "18446744073709551617",
                ChangeEffect::Content,
            ),
            (
                "0.123456789012345678901",
                "0.123456789012345678902",
                ChangeEffect::Content,
            ),
        ] {
            let namespace = IdNamespace::from_halves(13, 17);
            let (document, _) =
                Document::open_file(before.as_bytes().to_vec(), None, namespace).unwrap();
            let (successor, changes) = document
                .file_changed(
                    &[FileEdit {
                        offset: 0,
                        delete_len: before.len() as u64,
                        insert: after.as_bytes(),
                    }],
                    namespace,
                )
                .unwrap();
            assert_eq!(changes.len(), 1);
            assert_eq!(changes[0].effect, effect, "{before} -> {after}");
            let (restored, _) = Document::open_rows(successor.row_records().unwrap()).unwrap();
            assert_eq!(restored.bytes(), after.as_bytes());
        }
    }

    #[test]
    fn sparse_numeric_file_edits_preserve_content_classification() {
        use sdk::testing::{Harness, Snapshot};
        let harness = Harness::<crate::JsonPlugin>::default();
        let creates = sdk::CreateContext::from_namespace_bytes([0x35; 12]);
        for (before, after, expected) in [
            ("1e-9999", "2e-9999", sdk::ChangeEffect::Content),
            ("1e-9999", "10e-10000", sdk::ChangeEffect::FormatOnly),
            (
                "18446744073709551616",
                "18446744073709551617",
                sdk::ChangeEffect::Content,
            ),
        ] {
            let initial = Snapshot {
                file_id: "exact-number".into(),
                path: "number.json".into(),
                bytes: before.as_bytes().to_vec(),
                ..Snapshot::default()
            };
            let file = harness.parse(&initial, creates).unwrap().into_snapshot();
            let transition = harness
                .parse_changes(
                    &file,
                    &file.path,
                    &[sdk::FileEdit {
                        offset: 0,
                        delete_len: before.len() as u64,
                        insert: after.as_bytes().to_vec(),
                    }],
                    None,
                    creates,
                )
                .unwrap();
            assert_eq!(transition.row_changes.len(), 1);
            assert_eq!(transition.row_changes[0].effect, expected);
            assert_eq!(transition.snapshot().bytes, after.as_bytes());
        }
    }
}
