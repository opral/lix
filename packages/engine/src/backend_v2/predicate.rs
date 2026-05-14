use std::ops::Bound;

use bytes::Bytes;

use crate::backend_v2::{Key, KeyRange, Prefix, ValueProjection};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendPredicate {
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
    HasRef {
        kind: RefKind,
        value: Bytes,
    },
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadSupport {
    pub projection: ProjectionSupport,
    pub predicates: Vec<PredicatePushdown>,
    pub order: OrderSupport,
    pub limit: LimitSupport,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionSupport {
    pub requested: ValueProjection,
    pub returned: ValueProjection,
    pub support: Support,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PredicatePushdown {
    pub id: PredicateId,
    pub support: Support,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Support {
    Exact,
    Inexact,
    Unsupported,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderSupport {
    Exact,
    ChangedToKeyAsc,
    Unordered,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LimitSupport {
    Final,
    PageHintOnly,
    NotApplied,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PredicateSupportLevel {
    #[default]
    None,
    Inexact,
    Exact,
}

impl ReadSupport {
    pub fn exact(projection: ValueProjection) -> Self {
        Self {
            projection: ProjectionSupport {
                requested: projection,
                returned: projection,
                support: Support::Exact,
            },
            predicates: Vec::new(),
            order: OrderSupport::Exact,
            limit: LimitSupport::Final,
        }
    }
}
