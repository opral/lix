use datafusion::sql::sqlparser::ast::{CastKind, DataType as SqlDataType, Expr};

use crate::LixError;
use crate::sql2::plan::predicate::BoundPredicate;

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
    Binary {
        left: Box<Self>,
        op: BoundBinaryOperator,
        right: Box<Self>,
    },
    Not(Box<Self>),
    Predicate(Box<BoundPredicate>),
    Case {
        operand: Option<Box<Self>>,
        conditions: Vec<(Self, Self)>,
        else_result: Option<Box<Self>>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BoundBinaryOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    StringConcat,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BoundCastType {
    Text,
    Uuid,
    Binary,
    BigInt,
    Double,
    Boolean,
    Jsonb,
}

impl BoundCastType {
    pub(crate) fn canonical_sql_name(self) -> &'static str {
        match self {
            Self::Text => "TEXT",
            Self::Uuid => "UUID",
            Self::Binary => "BYTEA",
            Self::BigInt => "BIGINT",
            Self::Double => "DOUBLE PRECISION",
            Self::Boolean => "BOOLEAN",
            Self::Jsonb => "JSONB",
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum ReturningImage {
    Old,
    New,
}

impl ReturningImage {
    pub(crate) fn qualifier(self) -> &'static str {
        match self {
            Self::Old => "old",
            Self::New => "new",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct BoundColumnRef {
    pub(crate) image: Option<ReturningImage>,
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
        SqlDataType::Uuid => Some(BoundCastType::Uuid),
        SqlDataType::Bytea => Some(BoundCastType::Binary),
        SqlDataType::Int8(None) | SqlDataType::BigInt(None) => Some(BoundCastType::BigInt),
        SqlDataType::Float8 | SqlDataType::DoublePrecision => Some(BoundCastType::Double),
        SqlDataType::Boolean => Some(BoundCastType::Boolean),
        SqlDataType::JSONB => Some(BoundCastType::Jsonb),
        _ => None,
    };
    if matches!(kind, CastKind::Cast | CastKind::DoubleColon) && !array && !has_format {
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
        "Use one of the canonical PostgreSQL cast types supported by Lix: TEXT, UUID, BYTEA, BIGINT, DOUBLE PRECISION, BOOLEAN, or JSONB.",
    )
}

impl BoundExpr {
    pub(crate) fn references_image(&self, image: ReturningImage) -> bool {
        match self {
            Self::Column(column) => column.image == Some(image),
            Self::Cast { expr, .. } => expr.references_image(image),
            Self::Function { args, .. } => args.iter().any(|expr| expr.references_image(image)),
            Self::Binary { left, right, .. } => {
                left.references_image(image) || right.references_image(image)
            }
            Self::Not(expr) => expr.references_image(image),
            Self::Predicate(predicate) => predicate_references_image(predicate, image),
            Self::Case {
                operand,
                conditions,
                else_result,
            } => {
                operand
                    .as_deref()
                    .is_some_and(|expr| expr.references_image(image))
                    || conditions.iter().any(|(condition, result)| {
                        condition.references_image(image) || result.references_image(image)
                    })
                    || else_result
                        .as_deref()
                        .is_some_and(|expr| expr.references_image(image))
            }
            _ => false,
        }
    }
}

fn predicate_references_image(predicate: &BoundPredicate, image: ReturningImage) -> bool {
    match predicate {
        BoundPredicate::Eq(left, right) => {
            left.references_image(image) || right.references_image(image)
        }
        BoundPredicate::Like { expr, pattern, .. } => {
            expr.references_image(image) || pattern.references_image(image)
        }
        BoundPredicate::IsNull(expr) | BoundPredicate::IsNotNull(expr) => {
            expr.references_image(image)
        }
        BoundPredicate::In { expr, values } => {
            expr.references_image(image) || values.iter().any(|value| value.references_image(image))
        }
        BoundPredicate::And(predicates) | BoundPredicate::Or(predicates) => predicates
            .iter()
            .any(|predicate| predicate_references_image(predicate, image)),
        BoundPredicate::True | BoundPredicate::False => false,
    }
}
