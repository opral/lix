#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BoundExpr {
    Column(BoundColumnRef),
    Param(BoundParamRef),
    Literal(BoundLiteral),
    Function { name: String, args: Vec<Self> },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BoundLiteral {
    Null,
    Bool(bool),
    Integer(i64),
    Text(String),
    Json(serde_json::Value),
    Blob(Vec<u8>),
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct BoundColumnRef {
    pub(crate) table: String,
    pub(crate) column_id: usize,
    pub(crate) name: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct BoundParamRef {
    pub(crate) index: usize,
}
