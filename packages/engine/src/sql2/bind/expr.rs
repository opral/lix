use datafusion::sql::sqlparser::ast::{CastKind, DataType as SqlDataType, Expr};

use crate::LixError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BoundExpr {
    Column(BoundColumnRef),
    ExcludedColumn(BoundColumnRef),
    Param(BoundParamRef),
    Literal(BoundLiteral),
    Cast {
        expr: Box<Self>,
        data_type: BoundCastType,
    },
    Function {
        name: String,
        args: Vec<Self>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BoundCastType {
    Text,
    Binary,
    BigInt,
    Double,
    Boolean,
}

impl BoundCastType {
    pub(crate) fn canonical_sql_name(self) -> &'static str {
        match self {
            Self::Text => "TEXT",
            Self::Binary => "BYTEA",
            Self::BigInt => "BIGINT",
            Self::Double => "DOUBLE PRECISION",
            Self::Boolean => "BOOLEAN",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BoundLiteral {
    Null,
    Bool(bool),
    Integer(i64),
    Number {
        raw: String,
        value: serde_json::Number,
    },
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

pub(crate) fn bind_public_cast_type(
    kind: &CastKind,
    expr: &Expr,
    data_type: &SqlDataType,
    array: bool,
    has_format: bool,
) -> Result<BoundCastType, LixError> {
    let cast_type = match data_type {
        SqlDataType::Text => Some(BoundCastType::Text),
        SqlDataType::Bytea => Some(BoundCastType::Binary),
        SqlDataType::BigInt(None) => Some(BoundCastType::BigInt),
        SqlDataType::DoublePrecision => Some(BoundCastType::Double),
        SqlDataType::Boolean => Some(BoundCastType::Boolean),
        _ => None,
    };
    if kind == &CastKind::Cast && !array && !has_format {
        if let Some(cast_type) = cast_type {
            return Ok(cast_type);
        }
    }
    Err(unsupported_public_cast(expr, data_type))
}

fn unsupported_public_cast(expr: &Expr, data_type: &SqlDataType) -> LixError {
    LixError::new(
        LixError::CODE_UNSUPPORTED_SQL,
        format!("unsupported SQL cast 'CAST({expr} AS {data_type})'"),
    )
    .with_hint(
        "Use one of the canonical Lix SQL cast types: TEXT, BYTEA, BIGINT, DOUBLE PRECISION, or BOOLEAN.",
    )
}
