use lix_engine::live_state::constraints::{Bound, ScanConstraint, ScanField, ScanOperator};
use lix_engine::Value;

#[test]
fn constraints_module_exposes_all_scan_constraint_shapes() {
    let eq = ScanConstraint {
        field: ScanField::EntityId,
        operator: ScanOperator::Eq(Value::Text("entity-1".to_string())),
    };

    let in_list = ScanConstraint {
        field: ScanField::PluginKey,
        operator: ScanOperator::In(vec![
            Value::Text("plugin.a".to_string()),
            Value::Text("plugin.b".to_string()),
        ]),
    };

    let range = ScanConstraint {
        field: ScanField::SchemaVersion,
        operator: ScanOperator::Range {
            lower: Some(Bound {
                value: Value::Integer(10),
                inclusive: true,
            }),
            upper: Some(Bound {
                value: Value::Integer(20),
                inclusive: false,
            }),
        },
    };

    assert_eq!(
        eq,
        ScanConstraint {
            field: ScanField::EntityId,
            operator: ScanOperator::Eq(Value::Text("entity-1".to_string())),
        }
    );
    assert_eq!(
        in_list,
        ScanConstraint {
            field: ScanField::PluginKey,
            operator: ScanOperator::In(vec![
                Value::Text("plugin.a".to_string()),
                Value::Text("plugin.b".to_string()),
            ]),
        }
    );
    assert_eq!(
        range,
        ScanConstraint {
            field: ScanField::SchemaVersion,
            operator: ScanOperator::Range {
                lower: Some(Bound {
                    value: Value::Integer(10),
                    inclusive: true,
                }),
                upper: Some(Bound {
                    value: Value::Integer(20),
                    inclusive: false,
                }),
            },
        }
    );
}

#[test]
fn scan_constraints_roundtrip_through_serde() {
    let constraint = ScanConstraint {
        field: ScanField::FileId,
        operator: ScanOperator::Range {
            lower: Some(Bound {
                value: Value::Text("a.txt".to_string()),
                inclusive: true,
            }),
            upper: Some(Bound {
                value: Value::Text("m.txt".to_string()),
                inclusive: false,
            }),
        },
    };

    let encoded = serde_json::to_string(&constraint).expect("constraint should serialize");
    let decoded: ScanConstraint =
        serde_json::from_str(&encoded).expect("constraint should deserialize");

    assert_eq!(decoded, constraint);
}
