//! Keeps Lix's logical value identities at the edge of DataFusion planning.
//!
//! JSONB and ROW_REF are stored as UTF-8 for Arrow execution, but are distinct
//! SQL values. This pass checks only those type-specific boundaries; DataFusion
//! still plans and evaluates supported expressions and comparisons.

use std::collections::HashSet;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, ScalarValue};
use datafusion::logical_expr::expr::{
    AggregateFunction, Between, Case, InList, ScalarFunction, SetComparison,
};
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, Operator};

use crate::LixError;

use super::logical_value_metadata::{
    expr_lix_array_element_kind_with_lookup, expr_lix_array_leaf_kind_with_lookup,
    expr_lix_value_kind, field_lix_value_kind, is_sql_null_expression,
};
use super::result_metadata::{
    LIX_VALUE_TYPE_JSONB, LIX_VALUE_TYPE_METADATA_KEY, LIX_VALUE_TYPE_ROW_REF,
    field_array_element_value_kind, field_array_value_shape,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ValueIdentity {
    Plain,
    Null,
    Unknown,
    Jsonb,
    RowRef,
    Mixed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum UnionIdentity {
    Plain,
    Null,
    Jsonb,
    RowRef,
    List(Box<UnionIdentity>),
    Mixed,
}

impl UnionIdentity {
    fn contains_lix_type(&self) -> bool {
        match self {
            Self::Jsonb | Self::RowRef => true,
            Self::List(element) => element.contains_lix_type(),
            Self::Plain | Self::Null | Self::Mixed => false,
        }
    }
}

pub(crate) fn validate_lix_value_compatibility(plan: &LogicalPlan) -> Result<(), LixError> {
    validate_plan(plan)
}

pub(crate) fn validate_lix_expr_compatibility(
    expr: &Expr,
    schemas: &[&DFSchema],
) -> Result<(), LixError> {
    let mut datafusion_all_equality_extrema = HashSet::<*const Expr>::new();
    expr.apply(|nested| {
        if let Expr::Case(case) = nested {
            collect_datafusion_all_equality_extrema(case, &mut datafusion_all_equality_extrema);
        }
        let is_datafusion_all_equality_extrema =
            datafusion_all_equality_extrema.contains(&std::ptr::from_ref(nested));
        validate_expr_node(nested, schemas, is_datafusion_all_equality_extrema)
            .map_err(crate::sql2::error::lix_error_to_datafusion_error)?;
        let subquery = match nested {
            Expr::ScalarSubquery(subquery) => Some(&subquery.subquery),
            Expr::InSubquery(subquery) => Some(&subquery.subquery.subquery),
            Expr::Exists(subquery) => Some(&subquery.subquery.subquery),
            Expr::SetComparison(comparison) => Some(&comparison.subquery.subquery),
            _ => None,
        };
        if let Some(subquery) = subquery {
            validate_plan(subquery)
                .map_err(crate::sql2::error::lix_error_to_datafusion_error)?;
            Ok(TreeNodeRecursion::Jump)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })
    .map(|_| ())
    .map_err(crate::sql2::error::datafusion_error_to_lix_error)
}

fn validate_plan(plan: &LogicalPlan) -> Result<(), LixError> {
    let input_schemas = plan
        .inputs()
        .into_iter()
        .map(|input| input.schema().as_ref())
        .collect::<Vec<_>>();

    for expr in plan.expressions() {
        validate_lix_expr_compatibility(&expr, &input_schemas)?;
    }

    match plan {
        LogicalPlan::TableScan(scan) => {
            let schema = scan.projected_schema.as_ref();
            for filter in &scan.filters {
                validate_lix_expr_compatibility(filter, &[schema])?;
            }
        }
        LogicalPlan::Sort(sort) => {
            for sort_expr in &sort.expr {
                reject_ordering(&sort_expr.expr, &input_schemas, "ORDER BY")?;
            }
        }
        LogicalPlan::Union(union) => validate_union_types(union.inputs.as_slice())?,
        _ => {}
    }

    for input in plan.inputs() {
        validate_plan(input)?;
    }
    Ok(())
}

fn validate_union_types(inputs: &[std::sync::Arc<LogicalPlan>]) -> Result<(), LixError> {
    let Some(first) = inputs.first() else {
        return Ok(());
    };
    for column_index in 0..first.schema().fields().len() {
        let values = inputs
            .iter()
            .filter_map(|input| {
                input
                    .schema()
                    .fields()
                    .get(column_index)
                    .map(|field| union_identity_from_plan_output(input, column_index, field.as_ref()))
            })
            .collect::<Vec<_>>();
        if values.iter().any(UnionIdentity::contains_lix_type) {
            let identity = values
                .iter()
                .cloned()
                .fold(UnionIdentity::Null, combine_union_identity);
            if identity == UnionIdentity::Mixed {
                return Err(logical_type_mismatch(
                    ValueIdentity::Jsonb,
                    ValueIdentity::Plain,
                    "set operation",
                ));
            }
        }
    }
    Ok(())
}

fn union_identity_from_plan_output(plan: &LogicalPlan, index: usize, field: &Field) -> UnionIdentity {
    if let LogicalPlan::Projection(projection) = plan {
        if projection
            .expr
            .get(index)
            .is_some_and(is_sql_null_expression)
        {
            return UnionIdentity::Null;
        }
    }
    union_identity_from_field(field)
}

fn union_identity_from_field(field: &Field) -> UnionIdentity {
    if let Some(kind) = field_lix_value_kind(field) {
        return union_identity_from_kind(kind);
    }
    if let Some((depth, kind)) = field_array_value_shape(field) {
        return union_array_depth(depth, union_identity_from_kind(kind));
    }
    if let Some(kind) = field_array_element_value_kind(field) {
        return UnionIdentity::List(Box::new(union_identity_from_kind(kind)));
    }
    match field.data_type() {
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::FixedSizeList(item, _)
        | DataType::ListView(item)
        | DataType::LargeListView(item) => {
            UnionIdentity::List(Box::new(union_identity_from_field(item)))
        }
        DataType::Null => UnionIdentity::Null,
        _ => UnionIdentity::Plain,
    }
}

fn union_identity_from_kind(kind: &str) -> UnionIdentity {
    match kind {
        LIX_VALUE_TYPE_JSONB => UnionIdentity::Jsonb,
        LIX_VALUE_TYPE_ROW_REF => UnionIdentity::RowRef,
        _ => UnionIdentity::Plain,
    }
}

fn union_array_depth(depth: usize, mut identity: UnionIdentity) -> UnionIdentity {
    for _ in 0..depth {
        identity = UnionIdentity::List(Box::new(identity));
    }
    identity
}

fn combine_union_identity(left: UnionIdentity, right: UnionIdentity) -> UnionIdentity {
    use UnionIdentity::{Jsonb, List, Mixed, Null, Plain, RowRef};
    match (left, right) {
        (Mixed, _) | (_, Mixed) => Mixed,
        (Null, value) | (value, Null) => value,
        (Plain, Plain) => Plain,
        (Jsonb, Jsonb) => Jsonb,
        (RowRef, RowRef) => RowRef,
        (List(left), List(right)) => {
            List(Box::new(combine_union_identity(*left, *right)))
        }
        _ => Mixed,
    }
}

/// DataFusion 55 lowers `= ALL(array)` to a CASE expression whose decisive
/// branch checks that both array bounds equal the needle. The exact function
/// nodes are used only as equality operands; admitting them keeps
/// JSONB/ROW_REF identity equality available without exposing their UTF-8 sort
/// order through `array_min` or `array_max`.
///
/// DataFusion currently exposes no planner hook for `ALL`, so this exception
/// recognizes that equality-only expression shape. A separate occurrence of
/// either function (including one in a CASE result) is still rejected. The
/// integration regression test pins this behavior across DataFusion upgrades.
fn collect_datafusion_all_equality_extrema(case: &Case, allowed: &mut HashSet<*const Expr>) {
    for (condition, result) in &case.when_then_expr {
        if is_boolean_literal(result, false)
            && let Some((minimum, maximum)) = all_equality_extrema(condition)
        {
            allowed.insert(std::ptr::from_ref(minimum));
            allowed.insert(std::ptr::from_ref(maximum));
        }
    }
}

fn all_equality_extrema(condition: &Expr) -> Option<(&Expr, &Expr)> {
    let Expr::Not(inner) = condition else {
        return None;
    };
    let Expr::BinaryExpr(conjunction) = inner.as_ref() else {
        return None;
    };
    if conjunction.op != Operator::And {
        return None;
    }
    let (minimum, min_needle) = equality_function_and_needle(&conjunction.left, "array_min")?;
    let (maximum, max_needle) = equality_function_and_needle(&conjunction.right, "array_max")?;
    let min_array = array_function_input(minimum, "array_min")?;
    let max_array = array_function_input(maximum, "array_max")?;
    (min_needle == max_needle && min_array == max_array).then_some((minimum, maximum))
}

fn equality_function_and_needle<'a>(
    expr: &'a Expr,
    function_name: &str,
) -> Option<(&'a Expr, &'a Expr)> {
    let Expr::BinaryExpr(comparison) = expr else {
        return None;
    };
    if comparison.op != Operator::Eq {
        return None;
    }
    let function = comparison.left.as_ref();
    let Expr::ScalarFunction(scalar_function) = function else {
        return None;
    };
    if !scalar_function
        .func
        .name()
        .eq_ignore_ascii_case(function_name)
    {
        return None;
    }
    Some((function, comparison.right.as_ref()))
}

fn array_function_input<'a>(expr: &'a Expr, function_name: &str) -> Option<&'a Expr> {
    let Expr::ScalarFunction(function) = expr else {
        return None;
    };
    (function.func.name().eq_ignore_ascii_case(function_name) && function.args.len() == 1)
        .then(|| function.args[0].as_ref())
}

fn is_boolean_literal(expr: &Expr, value: bool) -> bool {
    matches!(
        expr,
        Expr::Literal(ScalarValue::Boolean(Some(literal)), _) if *literal == value
    )
}

fn validate_expr_node(
    expr: &Expr,
    schemas: &[&DFSchema],
    is_datafusion_all_equality_extrema: bool,
) -> Result<(), LixError> {
    match expr {
        Expr::BinaryExpr(binary) => match binary.op {
            Operator::Eq
            | Operator::NotEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom => {
                ensure_comparable(&binary.left, &binary.right, schemas, "comparison")?;
            }
            Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
                reject_ordering(&binary.left, schemas, "range comparisons (<, <=, >, >=)")?;
                reject_ordering(&binary.right, schemas, "range comparisons (<, <=, >, >=)")?;
            }
            Operator::StringConcat
            | Operator::LikeMatch
            | Operator::NotLikeMatch
            | Operator::ILikeMatch
            | Operator::NotILikeMatch
            | Operator::RegexMatch
            | Operator::RegexIMatch
            | Operator::RegexNotMatch
            | Operator::RegexNotIMatch => {
                reject_text_operation(&binary.left, schemas, &binary.op.to_string())?;
                reject_text_operation(&binary.right, schemas, &binary.op.to_string())?;
            }
            _ => {}
        },
        Expr::InList(InList { expr, list, .. }) => {
            for item in list {
                ensure_comparable(expr, item, schemas, "IN")?;
            }
        }
        Expr::Between(Between { expr, low, high, .. }) => {
            reject_ordering(expr, schemas, "BETWEEN")?;
            reject_ordering(low, schemas, "BETWEEN")?;
            reject_ordering(high, schemas, "BETWEEN")?;
        }
        Expr::Like(like) | Expr::SimilarTo(like) => {
            reject_text_operation(&like.expr, schemas, "pattern matching")?;
            reject_text_operation(&like.pattern, schemas, "pattern matching")?;
        }
        Expr::Case(case) => {
            if let Some(operand) = &case.expr {
                for (when, _) in &case.when_then_expr {
                    ensure_compatible(
                        identity_of_expr(operand, schemas),
                        identity_of_expr(when, schemas),
                        "simple CASE",
                    )?;
                }
            }
            let result_identities = case
                .when_then_expr
                .iter()
                .map(|(_, value)| identity_of_expr(value, schemas))
                .chain(case.else_expr.iter().map(|value| identity_of_expr(value, schemas)))
                .collect::<Vec<_>>();
            validate_identity_set(&result_identities, "CASE result")?;
        }
        Expr::InSubquery(in_subquery) => {
            if let Some(field) = in_subquery.subquery.subquery.schema().fields().first() {
                ensure_compatible(
                    identity_of_expr(&in_subquery.expr, schemas),
                    identity_from_field(field.as_ref()),
                    "IN subquery",
                )?;
            }
        }
        Expr::SetComparison(SetComparison {
            expr,
            subquery,
            op,
            ..
        }) => {
            if matches!(op, Operator::Eq | Operator::NotEq) {
                if let Some(field) = subquery.subquery.schema().fields().first() {
                    ensure_compatible(
                        identity_of_expr(expr, schemas),
                        identity_from_field(field.as_ref()),
                        "ANY/ALL",
                    )?;
                }
            } else {
                reject_ordering(expr, schemas, "ANY/ALL ordering comparisons")?;
                if let Some(field) = subquery.subquery.schema().fields().first() {
                    reject_ordering_field(field.as_ref(), "ANY/ALL ordering comparisons")?;
                }
            }
        }
        Expr::ScalarFunction(ScalarFunction { func, args }) => {
            let name = func.name().to_ascii_lowercase();
            match name.as_str() {
                "make_array" => validate_identity_set(
                    &args
                        .iter()
                        .map(|value| identity_of_expr(value, schemas))
                        .collect::<Vec<_>>(),
                    "array constructor",
                )?,
                "coalesce" | "ifnull" | "nvl" => validate_identity_set(
                    &args
                        .iter()
                        .map(|value| identity_of_expr(value, schemas))
                        .collect::<Vec<_>>(),
                    "conditional expression",
                )?,
                "nullif" if args.len() >= 2 => ensure_compatible(
                    identity_of_expr(&args[0], schemas),
                    identity_of_expr(&args[1], schemas),
                    "NULLIF",
                )?,
                "array_has" if args.len() == 2 => {
                    ensure_compatible(
                        array_element_identity(&args[0], schemas),
                        identity_of_expr(&args[1], schemas),
                        "array membership",
                    )?;
                }
                "array_position" if (2..=3).contains(&args.len()) => {
                    ensure_compatible(
                        array_element_identity(&args[0], schemas),
                        identity_of_expr(&args[1], schemas),
                        "array_position",
                    )?;
                }
                "array_positions" if args.len() == 2 => {
                    ensure_compatible(
                        array_element_identity(&args[0], schemas),
                        identity_of_expr(&args[1], schemas),
                        "array_positions",
                    )?;
                }
                "array_remove" | "array_remove_all" if args.len() >= 2 => {
                    ensure_compatible(
                        array_element_identity(&args[0], schemas),
                        identity_of_expr(&args[1], schemas),
                        name.as_str(),
                    )?;
                }
                "array_remove_n" if args.len() >= 2 => {
                    ensure_compatible(
                        array_element_identity(&args[0], schemas),
                        identity_of_expr(&args[1], schemas),
                        "array_remove_n",
                    )?;
                }
                "array_append" if args.len() == 2 => {
                    ensure_compatible(
                        array_element_identity(&args[0], schemas),
                        identity_of_expr(&args[1], schemas),
                        "array_append",
                    )?;
                }
                "array_prepend" if args.len() == 2 => {
                    ensure_compatible(
                        array_element_identity(&args[1], schemas),
                        identity_of_expr(&args[0], schemas),
                        "array_prepend",
                    )?;
                }
                "array_concat" => {
                    let mut identities = args
                        .iter()
                        .map(|argument| array_element_identity(argument, schemas));
                    if let Some(first) = identities.next() {
                        for identity in identities {
                            ensure_compatible(first, identity, "array_concat")?;
                        }
                    }
                }
                "array_has_any" | "array_has_all" if args.len() == 2 => {
                    ensure_compatible(
                        array_element_identity(&args[0], schemas),
                        array_element_identity(&args[1], schemas),
                        name.as_str(),
                    )?;
                }
                // DataFusion implements these by comparing array elements.
                // Keep its generic planner/evaluator, but prevent UTF-8-backed
                // JSONB and ROW_REF encodings from silently becoming their
                // ordering semantics, including through array-preserving
                // transforms.
                "array_min" | "array_max" if !args.is_empty() => {
                    if !is_datafusion_all_equality_extrema {
                        reject_array_element_ordering(&args[0], schemas, func.name())?;
                    }
                }
                "array_sort" if !args.is_empty() => {
                    reject_array_element_ordering(&args[0], schemas, func.name())?;
                }
                "array_distinct" if args.len() == 1 => {}
                "array_union" | "array_intersect" | "array_except" if args.len() == 2 => {
                    ensure_compatible(
                        array_element_identity(&args[0], schemas),
                        array_element_identity(&args[1], schemas),
                        name.as_str(),
                    )?;
                }
                // DataFusion's array_to_string formats UTF-8 leaf elements
                // directly. JSONB stores its canonical PostgreSQL text form,
                // so this is the JSONB output representation; ROW_REF remains
                // opaque and must be explicitly cast before string formatting.
                "array_to_string" if !args.is_empty() => {
                    let identity = array_leaf_identity(&args[0], schemas);
                    match identity {
                        ValueIdentity::Jsonb => {
                            for argument in args.iter().skip(1) {
                                reject_text_operation(argument, schemas, func.name())?;
                            }
                        }
                        ValueIdentity::RowRef | ValueIdentity::Mixed => {
                            return Err(unsupported_logical_operation(identity, func.name()));
                        }
                        _ => {
                            for argument in args {
                                reject_text_operation(argument, schemas, func.name())?;
                            }
                        }
                    }
                }
                // These nested functions inspect list structure but never
                // compare or interpret the logical values stored in it.
                "cardinality"
                | "array_length"
                | "array_ndims"
                | "array_dims"
                | "array_empty"
                | "array_lower"
                | "array_upper"
                | "array_reverse"
                | "array_slice"
                | "array_pop_front"
                | "array_pop_back"
                | "array_element"
                | "array_any_value"
                | "array_repeat" => {}
                "__lix_json_contains" if args.len() == 2 => {
                    require_kind(identity_of_expr(&args[0], schemas), "JSONB", "@>")?;
                    require_jsonb_containment_rhs(&args[1], schemas)?;
                }
                "__lix_json_exists" if args.len() == 2 => {
                    require_kind(identity_of_expr(&args[0], schemas), "JSONB", "?")?;
                    reject_extension_type_argument(&args[1], schemas, "JSONB ? key", false)?;
                    require_json_operator_argument_type(
                        &args[1],
                        schemas,
                        "JSONB ? key",
                        is_text_type,
                    )?;
                }
                "__lix_json_get" | "__lix_json_get_text" | "__lix_json_path_get"
                | "__lix_json_path_get_text"
                    if !args.is_empty() =>
                {
                    require_kind(identity_of_expr(&args[0], schemas), "JSONB", "JSON access")?;
                    for argument in args.iter().skip(1) {
                        reject_extension_type_argument(
                            argument,
                            schemas,
                            "JSONB key or path",
                            name.contains("path_get"),
                        )?;
                    }
                    if let Some(selector) = args.get(1) {
                        let valid_type = if name.contains("path_get") {
                            is_text_type_or_array
                        } else {
                            is_text_or_integer_type
                        };
                        require_json_operator_argument_type(
                            selector,
                            schemas,
                            "JSONB key or path",
                            valid_type,
                        )?;
                    }
                }
                "__lix_jsonb" | "__lix_text_cast" => {}
                // Let DataFusion CONCAT consume JSONB's existing compact
                // canonical text representation. ROW_REF remains opaque.
                "concat" | "concat_ws" => {
                    for argument in args {
                        let identity = identity_of_expr(argument, schemas);
                        if matches!(identity, ValueIdentity::RowRef | ValueIdentity::Mixed) {
                            return Err(unsupported_logical_operation(identity, func.name()));
                        }
                    }
                }
                "lix_row_ref" | "lix_row_ref_parts" => {}
                _ => {
                    for argument in args {
                        reject_text_operation(argument, schemas, func.name())?;
                    }
                }
            }
        }
        Expr::AggregateFunction(AggregateFunction { func, params, .. }) => {
            let name = func.name().to_ascii_lowercase();
            if matches!(name.as_str(), "min" | "max") {
                for argument in &params.args {
                    reject_ordering(argument, schemas, "MIN/MAX")?;
                }
            }
            for sort in &params.order_by {
                reject_ordering(&sort.expr, schemas, "aggregate ORDER BY")?;
            }
        }
        Expr::WindowFunction(window) => {
            let name = window.fun.name().to_ascii_lowercase();
            if matches!(name.as_str(), "lead" | "lag") {
                let result_identities = window
                    .params
                    .args
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| *index == 0 || *index == 2)
                    .map(|(_, value)| identity_of_expr(value, schemas))
                    .collect::<Vec<_>>();
                validate_identity_set(&result_identities, "window value/default")?;
            }
            if matches!(name.as_str(), "min" | "max") {
                for argument in &window.params.args {
                    reject_ordering(argument, schemas, "MIN/MAX window functions")?;
                }
            }
            for sort in &window.params.order_by {
                reject_ordering(&sort.expr, schemas, "window ORDER BY")?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn identity_of_expr(expr: &Expr, schemas: &[&DFSchema]) -> ValueIdentity {
    for schema in schemas {
        if let Some(kind) = expr_lix_value_kind(expr, schema) {
            return identity_from_kind(kind);
        }
    }
    match expr {
        Expr::Column(column) => schemas
            .iter()
            .find_map(|schema| field_for_column(schema, column))
            .map_or(ValueIdentity::Plain, identity_from_field),
        Expr::Alias(alias) => identity_of_expr(&alias.expr, schemas),
        Expr::Literal(ScalarValue::Null, _) => ValueIdentity::Null,
        Expr::Literal(_, Some(metadata)) => metadata
            .inner()
            .get(LIX_VALUE_TYPE_METADATA_KEY)
            .map_or(ValueIdentity::Plain, |kind| identity_from_kind(kind)),
        Expr::Placeholder(_) => ValueIdentity::Unknown,
        Expr::OuterReferenceColumn(field, _) | Expr::ScalarVariable(field, _) => {
            identity_from_field(field.as_ref())
        }
        Expr::ScalarSubquery(subquery) => subquery
            .subquery
            .schema()
            .fields()
            .first()
            .map_or(ValueIdentity::Plain, |field| identity_from_field(field.as_ref())),
        Expr::Case(case) => case
            .when_then_expr
            .iter()
            .map(|(_, value)| identity_of_expr(value, schemas))
            .chain(case.else_expr.iter().map(|value| identity_of_expr(value, schemas)))
            .fold(ValueIdentity::Null, combine_identity),
        Expr::ScalarFunction(function) => {
            let name = function.func.name().to_ascii_lowercase();
            match name.as_str() {
                "__lix_jsonb" | "__lix_json_get" | "__lix_json_path_get" | "lix_row_ref_parts" => {
                    ValueIdentity::Jsonb
                }
                "lix_row_ref" => ValueIdentity::RowRef,
                "coalesce" | "ifnull" | "nvl" => function
                    .args
                    .iter()
                    .map(|value| identity_of_expr(value, schemas))
                    .fold(ValueIdentity::Null, combine_identity),
                "make_array" => function
                    .args
                    .iter()
                    .map(|value| identity_of_expr(value, schemas))
                    .fold(ValueIdentity::Null, combine_identity),
                "nullif" => {
                    let first = function
                        .args
                        .first()
                        .map_or(ValueIdentity::Plain, |value| identity_of_expr(value, schemas));
                    if first == ValueIdentity::Null {
                        function
                            .args
                            .get(1)
                            .map_or(first, |value| identity_of_expr(value, schemas))
                    } else {
                        first
                    }
                }
                "array_min" | "array_max" => function
                    .args
                    .first()
                    .map_or(ValueIdentity::Plain, |value| array_element_identity(value, schemas)),
                _ => ValueIdentity::Plain,
            }
        }
        Expr::WindowFunction(window) => {
            let name = window.fun.name().to_ascii_lowercase();
            match name.as_str() {
                "first_value" | "last_value" | "nth_value" => window
                    .params
                    .args
                    .first()
                    .map_or(ValueIdentity::Plain, |value| identity_of_expr(value, schemas)),
                "lead" | "lag" => window
                    .params
                    .args
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| *index == 0 || *index == 2)
                    .map(|(_, value)| identity_of_expr(value, schemas))
                    .fold(ValueIdentity::Null, combine_identity),
                _ => ValueIdentity::Plain,
            }
        }
        Expr::Cast(_) | Expr::TryCast(_) => ValueIdentity::Plain,
        _ => ValueIdentity::Plain,
    }
}

fn array_element_identity(expr: &Expr, schemas: &[&DFSchema]) -> ValueIdentity {
    for schema in schemas {
        let element_kind = expr_lix_array_element_kind_with_lookup(expr, &|column| {
            field_for_column(schema, column)
        });
        if let Some(kind) = element_kind {
            return identity_from_kind(kind);
        }
    }
    match expr {
        Expr::Alias(alias) => array_element_identity(&alias.expr, schemas),
        Expr::ScalarFunction(function)
            if function.func.name().eq_ignore_ascii_case("make_array") => function
                .args
                .iter()
                .map(|value| identity_of_expr(value, schemas))
                .fold(ValueIdentity::Null, combine_identity),
        Expr::Case(case) => case
            .when_then_expr
            .iter()
            .map(|(_, value)| array_element_identity(value, schemas))
            .chain(case.else_expr.iter().map(|value| array_element_identity(value, schemas)))
            .fold(ValueIdentity::Null, combine_identity),
        Expr::Column(column) => schemas
            .iter()
            .find_map(|schema| field_for_column(schema, column))
            .map_or(ValueIdentity::Plain, array_identity_from_field),
        Expr::ScalarSubquery(subquery) => subquery
            .subquery
            .schema()
            .fields()
            .first()
            .map_or(ValueIdentity::Plain, |field| {
                array_identity_from_field(field.as_ref())
            }),
        Expr::Cast(_) | Expr::TryCast(_) => ValueIdentity::Plain,
        _ => ValueIdentity::Plain,
    }
}

fn array_leaf_identity(expr: &Expr, schemas: &[&DFSchema]) -> ValueIdentity {
    for schema in schemas {
        if let Some(kind) = expr_lix_array_leaf_kind_with_lookup(expr, &|column| {
            field_for_column(schema, column)
        }) {
            return identity_from_kind(kind);
        }
    }
    array_element_identity(expr, schemas)
}

fn array_identity_from_field(field: &Field) -> ValueIdentity {
    if let Some(kind) = field_array_element_value_kind(field) {
        return identity_from_kind(kind);
    }
    match field.data_type() {
        DataType::List(element) | DataType::LargeList(element) | DataType::FixedSizeList(element, _) => {
            if element.data_type().is_null() {
                ValueIdentity::Null
            } else {
                ValueIdentity::Plain
            }
        }
        _ => ValueIdentity::Plain,
    }
}

fn identity_from_field(field: &Field) -> ValueIdentity {
    field_lix_value_kind_recursive(field).map_or_else(
        || {
            if field.data_type().is_null() {
                ValueIdentity::Null
            } else {
                ValueIdentity::Plain
            }
        },
        identity_from_kind,
    )
}

fn field_lix_value_kind_recursive(field: &Field) -> Option<&'static str> {
    field_lix_value_kind(field)
        .or_else(|| field_array_value_shape(field).map(|(_, kind)| kind))
        .or_else(|| match field.data_type() {
            DataType::List(item)
            | DataType::LargeList(item)
            | DataType::FixedSizeList(item, _)
            | DataType::ListView(item)
            | DataType::LargeListView(item) => field_lix_value_kind_recursive(item),
            _ => None,
        })
}

fn identity_from_kind(kind: &str) -> ValueIdentity {
    match kind {
        LIX_VALUE_TYPE_JSONB => ValueIdentity::Jsonb,
        LIX_VALUE_TYPE_ROW_REF => ValueIdentity::RowRef,
        _ => ValueIdentity::Plain,
    }
}

fn combine_identity(left: ValueIdentity, right: ValueIdentity) -> ValueIdentity {
    use ValueIdentity::{Jsonb, Mixed, Null, Plain, RowRef, Unknown};
    match (left, right) {
        (Mixed, _) | (_, Mixed) => Mixed,
        (Null, value) | (value, Null) | (Unknown, value) | (value, Unknown) => value,
        (Plain, Plain) => Plain,
        (Jsonb, Jsonb) => Jsonb,
        (RowRef, RowRef) => RowRef,
        _ => Mixed,
    }
}

fn ensure_compatible(
    left: ValueIdentity,
    right: ValueIdentity,
    operation: &str,
) -> Result<(), LixError> {
    match (left, right) {
        (ValueIdentity::Plain, ValueIdentity::Plain)
        | (ValueIdentity::Null, _)
        | (_, ValueIdentity::Null)
        | (ValueIdentity::Unknown, _)
        | (_, ValueIdentity::Unknown) => Ok(()),
        (ValueIdentity::Jsonb, ValueIdentity::Jsonb)
        | (ValueIdentity::RowRef, ValueIdentity::RowRef) => Ok(()),
        (left, right) if left == ValueIdentity::Mixed || right == ValueIdentity::Mixed => {
            Err(logical_type_mismatch(left, right, operation))
        }
        (left, right) => Err(logical_type_mismatch(left, right, operation)),
    }
}

/// Equality between two expressions. As in PostgreSQL, an untyped string
/// literal (including a bound TEXT parameter) takes the type of a ROW_REF
/// operand, so it must be a canonical row reference. Any other TEXT value
/// requires an explicit CAST of the ROW_REF side to TEXT.
fn ensure_comparable(
    left: &Expr,
    right: &Expr,
    schemas: &[&DFSchema],
    operation: &str,
) -> Result<(), LixError> {
    let left_identity = identity_of_expr(left, schemas);
    let right_identity = identity_of_expr(right, schemas);
    for (identity, other) in [(left_identity, right), (right_identity, left)] {
        if identity == ValueIdentity::RowRef
            && let Some(text) = untyped_string_literal(other)
        {
            crate::row_ref::decode_str(text).map_err(|error| {
                LixError::new(
                    LixError::CODE_TYPE_MISMATCH,
                    format!(
                        "a TEXT value compared with a ROW_REF must be a canonical row reference: {}",
                        error.message
                    ),
                )
                .with_hint(
                    "Compare with lix_row_ref(...), or CAST the ROW_REF to TEXT for a text comparison.",
                )
            })?;
            return Ok(());
        }
    }
    ensure_compatible(left_identity, right_identity, operation)
}

fn untyped_string_literal(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Alias(alias) => untyped_string_literal(&alias.expr),
        Expr::Literal(
            ScalarValue::Utf8(Some(text))
            | ScalarValue::LargeUtf8(Some(text))
            | ScalarValue::Utf8View(Some(text)),
            None,
        ) => Some(text),
        _ => None,
    }
}

fn validate_identity_set(values: &[ValueIdentity], operation: &str) -> Result<(), LixError> {
    let mut identity = ValueIdentity::Null;
    for value in values {
        identity = combine_identity(identity, *value);
    }
    if identity == ValueIdentity::Mixed {
        Err(logical_type_mismatch(identity, identity, operation))
    } else {
        Ok(())
    }
}

fn require_kind(identity: ValueIdentity, kind: &str, operation: &str) -> Result<(), LixError> {
    let expected = match kind {
        "JSONB" => ValueIdentity::Jsonb,
        "ROW_REF" => ValueIdentity::RowRef,
        _ => unreachable!("only Lix logical types are checked here"),
    };
    if identity == ValueIdentity::Null || identity == ValueIdentity::Unknown || identity == expected {
        Ok(())
    } else {
        Err(logical_type_mismatch(expected, identity, operation))
    }
}

fn require_jsonb_containment_rhs(expr: &Expr, schemas: &[&DFSchema]) -> Result<(), LixError> {
    let identity = identity_of_expr(expr, schemas);
    if identity == ValueIdentity::Jsonb
        || identity == ValueIdentity::Null
        || identity == ValueIdentity::Unknown
        || is_untyped_string_literal(expr)
    {
        // PostgreSQL resolves an unknown string literal against the JSONB
        // operand signature. Lix lowers @> to a private UDF before DataFusion
        // can perform that operator overload resolution, so preserve only this
        // narrow dialect rule here. Explicit TEXT values still require ::jsonb.
        Ok(())
    } else {
        Err(logical_type_mismatch(
            ValueIdentity::Jsonb,
            identity,
            "@>",
        ))
    }
}

fn is_untyped_string_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Alias(alias) => is_untyped_string_literal(&alias.expr),
        Expr::Literal(
            ScalarValue::Utf8(Some(_))
            | ScalarValue::LargeUtf8(Some(_))
            | ScalarValue::Utf8View(Some(_)),
            None,
        ) => true,
        _ => false,
    }
}

fn reject_ordering(expr: &Expr, schemas: &[&DFSchema], operation: &str) -> Result<(), LixError> {
    let identity = identity_of_expr(expr, schemas);
    if matches!(identity, ValueIdentity::Jsonb | ValueIdentity::RowRef | ValueIdentity::Mixed) {
        return Err(unsupported_logical_operation(identity, operation));
    }
    Ok(())
}

fn reject_array_element_ordering(
    expr: &Expr,
    schemas: &[&DFSchema],
    operation: &str,
) -> Result<(), LixError> {
    let identity = array_leaf_identity(expr, schemas);
    if matches!(identity, ValueIdentity::Jsonb | ValueIdentity::RowRef | ValueIdentity::Mixed) {
        return Err(unsupported_logical_operation(identity, operation));
    }
    Ok(())
}

fn reject_ordering_field(field: &Field, operation: &str) -> Result<(), LixError> {
    let identity = identity_from_field(field);
    if matches!(identity, ValueIdentity::Jsonb | ValueIdentity::RowRef | ValueIdentity::Mixed) {
        return Err(unsupported_logical_operation(identity, operation));
    }
    Ok(())
}

fn reject_text_operation(expr: &Expr, schemas: &[&DFSchema], operation: &str) -> Result<(), LixError> {
    let identity = identity_of_expr(expr, schemas);
    if matches!(identity, ValueIdentity::Jsonb | ValueIdentity::RowRef | ValueIdentity::Mixed) {
        return Err(unsupported_logical_operation(identity, operation));
    }
    Ok(())
}

fn reject_extension_type_argument(
    expr: &Expr,
    schemas: &[&DFSchema],
    operation: &str,
    array_elements: bool,
) -> Result<(), LixError> {
    let identity = identity_of_expr(expr, schemas);
    let identity = if array_elements {
        combine_identity(identity, array_element_identity(expr, schemas))
    } else {
        identity
    };
    if matches!(identity, ValueIdentity::Jsonb | ValueIdentity::RowRef | ValueIdentity::Mixed) {
        Err(logical_type_mismatch(identity, ValueIdentity::Plain, operation))
    } else {
        Ok(())
    }
}

fn require_json_operator_argument_type(
    expr: &Expr,
    schemas: &[&DFSchema],
    operation: &str,
    accepts: fn(&DataType) -> bool,
) -> Result<(), LixError> {
    let data_type = schemas
        .iter()
        .find_map(|schema| expr.get_type(*schema).ok());
    // PostgreSQL gives these custom operators an operand signature that can
    // infer a prepared parameter's type. DataFusion keeps a scalar UDF
    // placeholder untyped until parameter binding, so defer this narrow
    // extension check. `validate_lix_value_compatibility` runs again after
    // binding and validates the actual type then.
    if matches!(expr, Expr::Placeholder(_))
        || data_type.as_ref().is_some_and(|ty| {
            *ty == DataType::Null || accepts(ty)
        })
    {
        Ok(())
    } else {
        let found = data_type.map_or_else(|| "unknown".to_owned(), |ty| format!("{ty:?}"));
        Err(LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            format!("{operation} does not accept argument type {found}"),
        ))
    }
}

fn is_text_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_text_or_integer_type(data_type: &DataType) -> bool {
    is_text_type(data_type)
        || matches!(
            data_type,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        )
}

fn is_text_type_or_array(data_type: &DataType) -> bool {
    is_text_type(data_type)
        || match data_type {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _) => is_text_type(field.data_type()),
            _ => false,
        }
}

fn logical_type_mismatch(left: ValueIdentity, right: ValueIdentity, operation: &str) -> LixError {
    let kind = if matches!(left, ValueIdentity::RowRef) || matches!(right, ValueIdentity::RowRef) {
        "ROW_REF"
    } else {
        "JSONB"
    };
    LixError::new(
        LixError::CODE_TYPE_MISMATCH,
        format!("{kind} values require matching logical types for {operation}"),
    )
    .with_hint(
        "Cast explicitly to TEXT for text comparison, or cast text values to JSONB with ::jsonb.",
    )
}

fn unsupported_logical_operation(identity: ValueIdentity, operation: &str) -> LixError {
    let kind = match identity {
        ValueIdentity::RowRef => "ROW_REF",
        _ => "JSONB",
    };
    LixError::new(
        LixError::CODE_UNSUPPORTED_SQL,
        format!("{operation} is not supported for {kind} values"),
    )
    .with_hint("Cast the value to TEXT explicitly to use DataFusion text behavior.")
}

fn field_for_column<'a>(schema: &'a DFSchema, column: &Column) -> Option<&'a Field> {
    schema
        .field_with_name(column.relation.as_ref(), &column.name)
        .ok()
        .map(AsRef::as_ref)
}
