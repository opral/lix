//! Versioned SQL request identity. Hash typed, length-delimited bytes rather
//! than serializing file contents into a second representation.
use crate::{ExecuteBatchStatement, Value};

pub(super) fn execute(sql: &str, params: &[Value], origin_key: Option<&str>) -> [u8; 32] {
    let mut hash = Fingerprint::new(0, origin_key, 1);
    hash.statement(sql, params, None);
    hash.finish()
}

pub(super) fn batch(statements: &[ExecuteBatchStatement], origin_key: Option<&str>) -> [u8; 32] {
    let mut hash = Fingerprint::new(1, origin_key, statements.len());
    for statement in statements {
        hash.statement(
            &statement.sql,
            &statement.params,
            statement.label.as_deref(),
        );
    }
    hash.finish()
}

struct Fingerprint(blake3::Hasher);

impl Fingerprint {
    fn new(operation: u8, origin_key: Option<&str>, statements: usize) -> Self {
        // A new domain intentionally invalidates pre-upgrade retry hashes. Old
        // receipts remain present, so reusing those keys fails closed instead
        // of executing the mutation again. Repository content is unaffected.
        let mut hash = Self(blake3::Hasher::new_derive_key(
            "lix sql request fingerprint v2",
        ));
        hash.tag(operation);
        hash.optional_text(origin_key);
        hash.length(statements);
        hash
    }

    fn tag(&mut self, tag: u8) {
        self.0.update(&[tag]);
    }
    fn length(&mut self, length: usize) {
        self.0.update(&(length as u64).to_le_bytes());
    }
    fn bytes(&mut self, bytes: &[u8]) {
        self.length(bytes.len());
        self.0.update(bytes);
    }
    fn optional_text(&mut self, text: Option<&str>) {
        match text {
            None => self.tag(0),
            Some(text) => {
                self.tag(1);
                self.bytes(text.as_bytes());
            }
        }
    }
    fn statement(&mut self, sql: &str, params: &[Value], label: Option<&str>) {
        self.bytes(sql.as_bytes());
        self.optional_text(label);
        self.length(params.len());
        for param in params {
            self.value(param);
        }
    }
    fn value(&mut self, value: &Value) {
        // Explicit tags are format identifiers, not Rust enum discriminants.
        // Exhaustiveness requires every new SQL value type to choose a tag.
        match value {
            Value::Null => self.tag(0),
            Value::Boolean(value) => {
                self.tag(1);
                self.tag(u8::from(*value));
            }
            Value::Integer(value) => {
                self.tag(2);
                self.0.update(&value.to_le_bytes());
            }
            Value::Real(value) => {
                self.tag(3);
                self.0.update(&value.to_bits().to_le_bytes());
            }
            Value::Text(value) => {
                self.tag(4);
                self.bytes(value.as_bytes());
            }
            Value::Jsonb(value) => {
                self.tag(5);
                self.bytes(value.as_bytes());
            }
            Value::RowRef(value) => {
                self.tag(6);
                self.bytes(value.as_str().as_bytes());
            }
            Value::Timestamptz(value) => {
                self.tag(7);
                self.0.update(&value.to_le_bytes());
            }
            Value::Blob(value) => {
                self.tag(8);
                self.bytes(value.as_ref());
            }
        }
    }
    fn finish(self) -> [u8; 32] {
        *self.0.finalize().as_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Blob, Json};
    use std::collections::HashSet;

    #[test]
    fn idempotency_typed_fingerprint_v2_stays_stable() {
        // These vectors pin the persisted receipt format across refactors.
        // Intentional encoding changes must use a new fingerprint domain.
        assert_eq!(
            blake3::Hash::from(execute(
                "SELECT $1",
                &[Value::Text("hello λ".into())],
                Some("origin"),
            ))
            .to_hex()
            .as_str(),
            "92e6b86f1472722022fc4b25f02c28cf2b1fc68f178a77171572894622ae5746"
        );
        assert_eq!(
            blake3::Hash::from(batch(&[], None)).to_hex().as_str(),
            "a8a424c84d07dda5c556c70d7d21ccc959164c9004b29bb3dc363d955dc832f7"
        );
    }

    #[test]
    fn idempotency_typed_fingerprint_distinguishes_types_and_boundaries() {
        let params = vec![
            vec![],
            vec![Value::Null],
            vec![Value::Boolean(false)],
            vec![Value::Integer(0)],
            vec![Value::Real(0.0)],
            vec![Value::Real(-0.0)],
            vec![Value::Text(String::new())],
            vec![Value::Blob(Blob::from(Vec::new()))],
            vec![Value::Jsonb(Json::parse("null").unwrap())],
            vec![Value::Timestamptz(0)],
            vec![Value::RowRef(crate::RowRef("0".into()))],
            vec![Value::Text("a".into()), Value::Text("bc".into())],
            vec![Value::Text("ab".into()), Value::Text("c".into())],
            vec![Value::Text("\0\"λ😀\r\n".into())],
            vec![Value::Blob(Blob::from("\0\"λ😀\r\n".as_bytes().to_vec()))],
        ];
        let hashes = params
            .iter()
            .map(|p| execute("SELECT $1", p, None))
            .collect::<HashSet<_>>();
        assert_eq!(hashes.len(), params.len());
        assert_ne!(
            execute("ab", &[Value::Text("c".into())], None),
            execute("a", &[Value::Text("bc".into())], None)
        );
        assert_ne!(
            execute("SELECT 1", &[], None),
            execute("SELECT 1", &[], Some(""))
        );
        assert_ne!(
            execute("SELECT 1", &[], Some("a")),
            execute("SELECT 1", &[], Some("b"))
        );
    }

    #[test]
    fn idempotency_typed_fingerprint_covers_batch_order_labels_and_operation() {
        let first = ExecuteBatchStatement {
            sql: "SELECT $1".into(),
            params: vec![Value::Integer(1)],
            label: None,
        };
        let second = ExecuteBatchStatement {
            sql: "SELECT $1".into(),
            params: vec![Value::Integer(2)],
            label: None,
        };
        assert_ne!(
            batch(&[first.clone(), second.clone()], None),
            batch(&[second, first.clone()], None)
        );
        assert_ne!(
            batch(std::slice::from_ref(&first), None),
            execute(&first.sql, &first.params, None)
        );
        let mut labeled = first.clone();
        labeled.label = Some(String::new());
        assert_ne!(batch(&[first.clone()], None), batch(&[labeled], None));
        assert_ne!(
            batch(std::slice::from_ref(&first), None),
            batch(&[first], Some("origin"))
        );
    }

    #[test]
    fn idempotency_typed_fingerprint_hashes_canonical_json_and_exact_content() {
        let left = Value::Jsonb(Json::parse("{\"b\":2,\"a\":1}").unwrap());
        let right = Value::Jsonb(Json::parse("{ \"a\":1, \"b\":2 }").unwrap());
        assert_eq!(
            execute("SELECT $1", &[left], None),
            execute("SELECT $1", &[right], None)
        );
        let original = "\u{feff}\r\n\"quoted\"\\\0λ😀".repeat(16384);
        let mut changed = original.clone();
        changed.push('x');
        assert_eq!(
            execute("SELECT $1", &[Value::Text(original.clone())], None),
            execute("SELECT $1", &[Value::Text(original.clone())], None)
        );
        assert_ne!(
            execute("SELECT $1", &[Value::Text(original)], None),
            execute("SELECT $1", &[Value::Text(changed)], None)
        );
    }
}
