use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::expr::{Between, InList};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};

use crate::LixError;
use crate::common::{ParsedFilePath, normalize_directory_path};

use super::error::lix_error_to_datafusion_error;

#[derive(Debug, Clone, Copy)]
pub(crate) enum FilesystemPathKind {
    File,
    Directory,
}

pub(crate) fn canonicalize_filesystem_path_filters(
    filters: &[Expr],
    kind: FilesystemPathKind,
) -> Result<Vec<Expr>> {
    filters
        .iter()
        .cloned()
        .map(|filter| canonicalize_filesystem_path_filter(filter, kind))
        .collect()
}

fn canonicalize_filesystem_path_filter(expr: Expr, kind: FilesystemPathKind) -> Result<Expr> {
    expr.transform(|expr| canonicalize_filesystem_path_expr(expr, kind))
        .map(|transformed| transformed.data)
}

fn canonicalize_filesystem_path_expr(
    expr: Expr,
    kind: FilesystemPathKind,
) -> Result<Transformed<Expr>> {
    match expr {
        Expr::BinaryExpr(binary_expr) if is_path_comparison_operator(binary_expr.op) => {
            canonicalize_path_binary_expr(binary_expr, kind)
        }
        Expr::InList(in_list) if is_path_column(&in_list.expr) => {
            canonicalize_path_in_list(in_list, kind)
        }
        Expr::Between(between) if is_path_column(&between.expr) => {
            canonicalize_path_between(between, kind)
        }
        _ => Ok(Transformed::no(expr)),
    }
}

fn canonicalize_path_binary_expr(
    binary_expr: BinaryExpr,
    kind: FilesystemPathKind,
) -> Result<Transformed<Expr>> {
    let BinaryExpr { left, op, right } = binary_expr;
    let left_is_path = is_path_column(&left);
    let right_is_path = is_path_column(&right);

    let left = if right_is_path {
        Box::new(canonicalize_path_literal_expr(*left, kind)?)
    } else {
        left
    };
    let right = if left_is_path {
        Box::new(canonicalize_path_literal_expr(*right, kind)?)
    } else {
        right
    };

    Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
        left, op, right,
    ))))
}

fn canonicalize_path_in_list(
    in_list: InList,
    kind: FilesystemPathKind,
) -> Result<Transformed<Expr>> {
    let list = in_list
        .list
        .into_iter()
        .map(|expr| canonicalize_path_literal_expr(expr, kind))
        .collect::<Result<Vec<_>>>()?;
    Ok(Transformed::yes(Expr::InList(InList::new(
        in_list.expr,
        list,
        in_list.negated,
    ))))
}

fn canonicalize_path_between(
    between: Between,
    kind: FilesystemPathKind,
) -> Result<Transformed<Expr>> {
    Ok(Transformed::yes(Expr::Between(Between {
        expr: between.expr,
        negated: between.negated,
        low: Box::new(canonicalize_path_literal_expr(*between.low, kind)?),
        high: Box::new(canonicalize_path_literal_expr(*between.high, kind)?),
    })))
}

fn canonicalize_path_literal_expr(expr: Expr, kind: FilesystemPathKind) -> Result<Expr> {
    let Expr::Literal(literal, metadata) = expr else {
        return Err(unsupported_dynamic_path_predicate_error(expr));
    };

    match literal {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => {
            let normalized = canonicalize_path_value(&value, kind)?;
            Ok(Expr::Literal(ScalarValue::Utf8(Some(normalized)), metadata))
        }
        _ => Ok(Expr::Literal(literal, metadata)),
    }
}

fn canonicalize_path_value(value: &str, kind: FilesystemPathKind) -> Result<String> {
    match kind {
        FilesystemPathKind::File => ParsedFilePath::try_from_path(value)
            .map(|parsed| parsed.normalized_path.to_string())
            .map_err(lix_error_to_datafusion_error),
        FilesystemPathKind::Directory => {
            normalize_directory_path(value).map_err(lix_error_to_datafusion_error)
        }
    }
}

fn is_path_column(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(column) if column.name == "path")
}

fn is_path_comparison_operator(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
    )
}

fn unsupported_dynamic_path_predicate_error(expr: Expr) -> DataFusionError {
    lix_error_to_datafusion_error(
        LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            format!(
                "filesystem path predicates only support literal path values; found expression {expr:?}"
            ),
        )
        .with_hint(
            "Compare lix_file.path or lix_directory.path to a string literal or bound parameter. \
             Computed path expressions are not supported until path canonicalization can run at evaluation time.",
        ),
    )
}
