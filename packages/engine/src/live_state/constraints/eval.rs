use crate::Value;

use super::{ScanConstraint, ScanField, ScanOperator};

pub(crate) fn matches_constraints(
    entity_id: &str,
    file_id: &str,
    plugin_key: &str,
    schema_version: &str,
    constraints: &[ScanConstraint],
) -> bool {
    constraints.iter().all(|constraint| {
        let candidate = match constraint.field {
            ScanField::EntityId => entity_id,
            ScanField::FileId => file_id,
            ScanField::PluginKey => plugin_key,
            ScanField::SchemaVersion => schema_version,
        };
        matches_constraint(candidate, &constraint.operator)
    })
}

fn matches_constraint(candidate: &str, operator: &ScanOperator) -> bool {
    match operator {
        ScanOperator::Eq(value) => text_value(value).is_some_and(|value| value == candidate),
        ScanOperator::In(values) => values
            .iter()
            .filter_map(text_value)
            .any(|value| value == candidate),
        ScanOperator::Range { lower, upper } => {
            lower
                .as_ref()
                .is_none_or(|bound| compare_lower(candidate, &bound.value, bound.inclusive))
                && upper
                    .as_ref()
                    .is_none_or(|bound| compare_upper(candidate, &bound.value, bound.inclusive))
        }
    }
}

fn compare_lower(candidate: &str, bound: &Value, inclusive: bool) -> bool {
    text_value(bound).is_some_and(|value| {
        if inclusive {
            candidate >= value
        } else {
            candidate > value
        }
    })
}

fn compare_upper(candidate: &str, bound: &Value, inclusive: bool) -> bool {
    text_value(bound).is_some_and(|value| {
        if inclusive {
            candidate <= value
        } else {
            candidate < value
        }
    })
}

fn text_value(value: &Value) -> Option<&str> {
    match value {
        Value::Text(value) => Some(value.as_str()),
        _ => None,
    }
}
