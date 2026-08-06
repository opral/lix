use std::ops::Bound;

use bytes::Bytes;

use crate::storage::{Key, KeyRange, Prefix};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoragePredicate {
    pub id: PredicateId,
    pub expr: PredicateExpr,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PredicateId(pub u32);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PredicateExpr {
    Key(KeyPredicate),
    Header(HeaderPredicate),
    Refs(RefsPredicate),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyPredicate {
    Eq(Key),
    StartsWith(Prefix),
    Range(KeyRange),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HeaderPredicate {
    FieldEq {
        field: HeaderFieldId,
        value: ScalarValue,
    },
    FieldIn {
        field: HeaderFieldId,
        values: Vec<ScalarValue>,
    },
    FieldRange {
        field: HeaderFieldId,
        lower: Bound<ScalarValue>,
        upper: Bound<ScalarValue>,
    },
    IsDeleted(bool),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RefsPredicate {
    HasRef { kind: RefKind, value: Bytes },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct HeaderFieldId(pub u16);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RefKind(pub u16);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ScalarValue {
    Bool(bool),
    U64(u64),
    I64(i64),
    Bytes(Bytes),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Support {
    Exact,
    Inexact,
    Unsupported,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PredicateSupportLevel {
    #[default]
    None,
    Inexact,
    Exact,
}
