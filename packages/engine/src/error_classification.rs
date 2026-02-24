use crate::LixError;

pub(crate) fn is_missing_relation_error(err: &LixError) -> bool {
    let lower = err.message.to_lowercase();
    lower.contains("no such table")
        || lower.contains("relation")
            && (lower.contains("does not exist")
                || lower.contains("undefined table")
                || lower.contains("unknown"))
}

#[cfg(test)]
mod tests {
    use super::is_missing_relation_error;
    use crate::LixError;

    #[test]
    fn classifies_missing_relation_messages() {
        assert!(is_missing_relation_error(&LixError {
            message: "no such table: foo".to_string(),
        }));
        assert!(is_missing_relation_error(&LixError {
            message: "ERROR: relation \"foo\" does not exist".to_string(),
        }));
        assert!(is_missing_relation_error(&LixError {
            message: "undefined table: relation foo".to_string(),
        }));
        assert!(!is_missing_relation_error(&LixError {
            message: "CHECK constraint failed".to_string(),
        }));
    }
}
