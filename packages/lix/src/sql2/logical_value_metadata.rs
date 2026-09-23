//! Propagation of Lix's logical value kinds through DataFusion plans.
//!
//! JSONB and row references use Arrow UTF-8 arrays, so their logical kind is
//! carried in root-field metadata, with array depth recorded separately from
//! Arrow's nested data type. DataFusion's generic expression schema rules
//! intentionally drop metadata for conditional expressions and intersect UNION
//! metadata across every input. That is correct for ordinary Arrow metadata,
//! but loses Lix's value contract when a branch is a SQL NULL. Keep this pass
//! at the logical-plan boundary so predicates, subqueries, and final result
//! fields all see the same logical kind.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::tree_node::Transformed;
use datafusion::common::{Column, DFSchema, DataFusionError};
use datafusion::logical_expr::expr::{Alias, ScalarFunction};
use datafusion::logical_expr::utils::grouping_set_to_exprlist;
use datafusion::logical_expr::{Expr, LogicalPlan};

use super::result_metadata::{
    LIX_ARRAY_ELEMENT_VALUE_TYPE_METADATA_KEY, LIX_VALUE_SHAPE_METADATA_KEY,
    LIX_VALUE_TYPE_JSONB, LIX_VALUE_TYPE_METADATA_KEY, LIX_VALUE_TYPE_ROW_REF,
    field_array_element_value_kind, field_array_value_shape, field_is_json, field_is_row_ref,
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct LixValueShape {
    list_depth: usize,
    kind: Option<&'static str>,
}

impl LixValueShape {
    fn scalar(kind: &'static str) -> Self {
        Self {
            list_depth: 0,
            kind: Some(kind),
        }
    }

    fn null() -> Self {
        Self {
            list_depth: 0,
            kind: None,
        }
    }

    fn as_array_item(mut self) -> Self {
        self.list_depth += 1;
        self
    }

    fn unnest(mut self, depth: usize) -> Option<Self> {
        self.list_depth = self.list_depth.checked_sub(depth)?;
        Some(self)
    }
}

/// Rebuilds logical-plan output schemas with Lix value-kind metadata inferred
/// from expressions and UNION inputs.
pub(crate) fn propagate_lix_value_metadata(
    plan: LogicalPlan,
) -> Result<LogicalPlan, DataFusionError> {
    plan.transform_up_with_subqueries(|node| {
        // Schema-owning nodes (aliases, joins, aggregates, windows, etc.) must
        // see the rewritten child fields before we infer their expressions.
        let node = match node.recompute_schema()? {
            LogicalPlan::Projection(mut projection) => {
                let input = projection.input.as_ref();
                let fields = projection
                    .schema
                    .fields()
                    .iter()
                    .zip(projection.expr.iter())
                    .map(|(field, expr)| field_with_expr_kind(field.as_ref(), expr, input.schema()))
                    .collect::<Vec<_>>();
                projection.schema = replace_schema_fields(&projection.schema, fields)?;
                LogicalPlan::Projection(projection)
            }
            LogicalPlan::Union(mut union) => {
                let fields = union
                    .schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(index, field)| {
                        field_with_lix_value_shape(
                            field.as_ref(),
                            union_value_shape(&union.inputs, index).as_ref(),
                        )
                    })
                    .collect::<Vec<_>>();
                union.schema = replace_schema_fields(&union.schema, fields)?;
                LogicalPlan::Union(union)
            }
            LogicalPlan::Aggregate(mut aggregate) => {
                let input_schema = aggregate.input.schema();
                let grouping_exprs = grouping_set_to_exprlist(&aggregate.group_expr)?;
                let fields = aggregate
                    .schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(index, field)| {
                        let expr = grouping_exprs.get(index).copied().or_else(|| {
                            index
                                .checked_sub(grouping_exprs.len())
                                .and_then(|index| aggregate.aggr_expr.get(index))
                        });
                        expr.map_or_else(
                                || field.as_ref().clone(),
                                |expr| field_with_expr_kind(field.as_ref(), expr, input_schema),
                            )
                    })
                    .collect::<Vec<_>>();
                aggregate.schema = replace_schema_fields(&aggregate.schema, fields)?;
                LogicalPlan::Aggregate(aggregate)
            }
            LogicalPlan::Unnest(mut unnest) => {
                let input_fields = unnest.input.schema().fields();
                let fields = unnest
                    .schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(index, field)| {
                        let Some(source_index) = unnest.dependency_indices.get(index) else {
                            return field.as_ref().clone();
                        };
                        let Some(source_field) = input_fields.get(*source_index) else {
                            return field.as_ref().clone();
                        };
                        // A single source column may be unnested more than
                        // once at different depths in the same plan. Match
                        // both its source index and output column so each
                        // output receives the identity of its actual leaf.
                        let list_unnesting = unnest
                            .list_type_columns
                            .iter()
                            .find(|(column_index, unnesting)| {
                                column_index == source_index
                                    && unnesting.output_column.name.as_str()
                                        == field.name().as_str()
                            });
                        let Some((_, list_unnesting)) = list_unnesting else {
                            return field.as_ref().clone();
                        };
                        let shape = field_lix_value_shape(source_field.as_ref())
                            .and_then(|shape| shape.unnest(list_unnesting.depth));
                        field_with_lix_value_shape(field.as_ref(), shape.as_ref())
                    })
                    .collect::<Vec<_>>();
                unnest.schema = replace_schema_fields(&unnest.schema, fields)?;
                LogicalPlan::Unnest(unnest)
            }
            LogicalPlan::Window(mut window) => {
                let input_schema = window.input.schema();
                let input_len = input_schema.fields().len();
                let fields = window
                    .schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(index, field)| {
                        let Some(expr_index) = index.checked_sub(input_len) else {
                            return field.as_ref().clone();
                        };
                        let Some(expr) = window.window_expr.get(expr_index) else {
                            return field.as_ref().clone();
                        };
                        field_with_expr_kind(field.as_ref(), expr, input_schema)
                    })
                    .collect::<Vec<_>>();
                window.schema = replace_schema_fields(&window.schema, fields)?;
                LogicalPlan::Window(window)
            }
            other => other,
        };
        Ok(Transformed::yes(node))
    })
    .map(|transformed| transformed.data)
}

/// Returns a Lix logical kind for an expression when all value-producing
/// branches agree. SQL NULL has no kind and is ignored, so CASE/COALESCE with
/// JSONB or row_ref plus NULL retain their declared logical type.
pub(crate) fn expr_lix_value_kind(expr: &Expr, schema: &DFSchema) -> Option<&'static str> {
    expr_lix_value_shape(expr, schema)
        .filter(|shape| shape.list_depth == 0)
        .and_then(|shape| shape.kind)
}

/// Returns the logical kind carried by elements of an array expression.
/// Array constructors and projected aliases retain this separately from the
/// array's physical `List<Utf8>` type so Lix extension checks can keep
/// ROW_REF and JSONB distinct from ordinary text.
pub(crate) fn expr_lix_array_element_kind_with_lookup<'a>(
    expr: &Expr,
    lookup_field: &impl Fn(&Column) -> Option<&'a Field>,
) -> Option<&'static str> {
    expr_lix_value_shape_with_lookup(expr, lookup_field)
        .and_then(|shape| (shape.list_depth == 1).then_some(shape.kind).flatten())
}

/// Returns the Lix logical kind of an array's leaf values, including nested
/// arrays. Functions such as `array_to_string` recursively process nested
/// lists, so their extension checks must inspect beyond the first list level.
pub(crate) fn expr_lix_array_leaf_kind_with_lookup<'a>(
    expr: &Expr,
    lookup_field: &impl Fn(&Column) -> Option<&'a Field>,
) -> Option<&'static str> {
    expr_lix_value_shape_with_lookup(expr, lookup_field)
        .and_then(|shape| (shape.list_depth > 0).then_some(shape.kind).flatten())
}

fn expr_lix_value_shape(expr: &Expr, schema: &DFSchema) -> Option<LixValueShape> {
    expr_lix_value_shape_with_lookup(expr, &|column| {
        schema
            .field_with_name(column.relation.as_ref(), &column.name)
            .ok()
            .map(AsRef::as_ref)
    })
}

fn expr_lix_value_shape_with_lookup<'a>(
    expr: &Expr,
    lookup_field: &impl Fn(&Column) -> Option<&'a Field>,
) -> Option<LixValueShape> {
    match expr {
        Expr::Alias(Alias { expr, .. }) => expr_lix_value_shape_with_lookup(expr, lookup_field),
        Expr::Column(column) => lookup_field(column).and_then(field_lix_value_shape),
        Expr::OuterReferenceColumn(field, _) | Expr::ScalarVariable(field, _) => {
            field_lix_value_shape(field.as_ref())
        }
        Expr::LambdaVariable(variable) => variable
            .field
            .as_deref()
            .and_then(field_lix_value_shape),
        Expr::Literal(value, metadata) => metadata
            .as_ref()
            .and_then(field_metadata_kind)
            .map(LixValueShape::scalar)
            .or_else(|| value.is_null().then(LixValueShape::null)),
        Expr::ScalarSubquery(subquery) => subquery
            .subquery
            .schema()
            .fields()
            .first()
            .and_then(|field| field_lix_value_shape(field.as_ref())),
        Expr::Case(case) => common_expr_shape(
            case.when_then_expr
                .iter()
                .map(|(_, expr)| expr.as_ref())
                .chain(case.else_expr.iter().map(AsRef::as_ref)),
            &|expr| expr_lix_value_shape_with_lookup(expr, lookup_field),
        ),
        Expr::ScalarFunction(ScalarFunction { func, args }) => {
            match func.name().to_ascii_lowercase().as_str() {
                "__lix_jsonb" | "__lix_json_get" | "__lix_json_path_get" => {
                    Some(LixValueShape::scalar(LIX_VALUE_TYPE_JSONB))
                }
                "lix_row_ref" => Some(LixValueShape::scalar(LIX_VALUE_TYPE_ROW_REF)),
                "make_array" => common_expr_shape(args.iter(), &|expr| {
                    expr_lix_value_shape_with_lookup(expr, lookup_field)
                })
                .map(LixValueShape::as_array_item),
                "coalesce" | "ifnull" | "nvl" => common_expr_shape(args.iter(), &|expr| {
                    expr_lix_value_shape_with_lookup(expr, lookup_field)
                }),
                // NULLIF returns its first argument, or SQL NULL when the
                // arguments compare equal. Its logical type is therefore the
                // first argument's logical type. If that argument is an
                // untyped SQL NULL, DataFusion resolves the result type from
                // the second argument, so carry its Lix identity as metadata.
                "nullif" => args.first().and_then(|first| {
                    let result = if is_sql_null_expression(first) {
                        args.get(1)
                    } else {
                        Some(first)
                    };
                    result.and_then(|expr| expr_lix_value_shape_with_lookup(expr, lookup_field))
                }),
                // These DataFusion nested functions preserve or transform
                // array structure without changing the logical identity of
                // their elements. Keep the Lix sidecar attached to the
                // resolved DataFusion output type.
                "array_reverse"
                | "array_slice"
                | "array_remove"
                | "array_remove_all"
                | "array_remove_n"
                | "array_pop_front"
                | "array_pop_back"
                | "array_distinct" => args
                    .first()
                    .and_then(|expr| expr_lix_value_shape_with_lookup(expr, lookup_field)),
                "array_append" if args.len() == 2 => {
                    appended_array_shape(&args[0], &args[1], lookup_field)
                }
                "array_prepend" if args.len() == 2 => {
                    appended_array_shape(&args[1], &args[0], lookup_field)
                }
                "array_concat" | "array_union" | "array_intersect" | "array_except" => common_expr_shape(args.iter(), &|expr| {
                    expr_lix_value_shape_with_lookup(expr, lookup_field)
                }),
                "array_repeat" => args
                    .first()
                    .and_then(|expr| expr_lix_value_shape_with_lookup(expr, lookup_field))
                    .map(LixValueShape::as_array_item),
                "array_element" | "array_any_value" => args
                    .first()
                    .and_then(|expr| expr_lix_value_shape_with_lookup(expr, lookup_field))
                    .and_then(|shape| shape.unnest(1)),
                _ => None,
            }
        }
        Expr::AggregateFunction(function)
            if function.func.name().eq_ignore_ascii_case("array_agg") => function
            .params
            .args
            .first()
            .and_then(|expr| expr_lix_value_shape_with_lookup(expr, lookup_field))
            .map(LixValueShape::as_array_item),
        Expr::WindowFunction(window) => {
            let name = window.fun.name().to_ascii_lowercase();
            match name.as_str() {
                "first_value" | "last_value" | "nth_value" => window
                    .params
                    .args
                    .first()
                    .and_then(|expr| expr_lix_value_shape_with_lookup(expr, lookup_field)),
                "lead" | "lag" => common_expr_shape(
                    window
                        .params
                        .args
                        .first()
                        .into_iter()
                        .chain(window.params.args.get(2)),
                    &|expr| expr_lix_value_shape_with_lookup(expr, lookup_field),
                ),
                _ => None,
            }
        }
        // An explicit cast establishes a Lix logical-type boundary. Supported
        // JSONB/ROW_REF casts are lowered to metadata-producing UDFs.
        Expr::Cast(_) | Expr::TryCast(_) => None,
        _ => None,
    }
}

fn appended_array_shape<'a>(
    array: &Expr,
    value: &Expr,
    lookup_field: &impl Fn(&Column) -> Option<&'a Field>,
) -> Option<LixValueShape> {
    let array_shape = expr_lix_value_shape_with_lookup(array, lookup_field)?;
    if array_shape.list_depth == 0 {
        return None;
    }
    let value_shape = expr_lix_value_shape_with_lookup(value, lookup_field)?;
    combine_value_shapes(array_shape, value_shape.as_array_item())
}

fn field_lix_value_shape(field: &Field) -> Option<LixValueShape> {
    if let Some(kind) = field_lix_value_kind(field) {
        return Some(LixValueShape::scalar(kind));
    }
    if let Some((list_depth, kind)) = field_array_value_shape(field) {
        return Some(LixValueShape {
            list_depth,
            kind: Some(kind),
        });
    }

    let item_field = match field.data_type() {
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::FixedSizeList(item, _)
        | DataType::ListView(item)
        | DataType::LargeListView(item) => item.as_ref(),
        DataType::Null => return Some(LixValueShape::null()),
        _ => return None,
    };

    field_lix_value_shape(item_field)
        .map(LixValueShape::as_array_item)
        .or_else(|| (item_field.data_type() == &DataType::Null).then(LixValueShape::null))
        .or_else(|| {
            // Older persisted schemas recorded only a one-dimensional array
            // tag on the parent field. Keep reading it through migration.
            field_array_element_value_kind(field).map(|kind| LixValueShape {
                list_depth: 1,
                kind: Some(kind),
            })
        })
}

fn common_expr_shape<'a>(
    expressions: impl IntoIterator<Item = &'a Expr>,
    lookup_shape: &impl Fn(&Expr) -> Option<LixValueShape>,
) -> Option<LixValueShape> {
    let mut shape = None;
    let mut saw_null = false;
    for expr in expressions {
        if is_sql_null_expression(expr) {
            saw_null = true;
            continue;
        }
        if matches!(expr, Expr::Placeholder(_)) {
            continue;
        }
        let current = lookup_shape(expr)?;
        shape = Some(match shape {
            Some(shape) => combine_value_shapes(shape, current)?,
            None => current,
        });
    }
    shape.or_else(|| saw_null.then(LixValueShape::null))
}

fn combine_value_shapes(left: LixValueShape, right: LixValueShape) -> Option<LixValueShape> {
    if left.list_depth != right.list_depth {
        return None;
    }
    let kind = match (left.kind, right.kind) {
        (None, kind) | (kind, None) => kind,
        (Some(left), Some(right)) if left == right => Some(left),
        _ => return None,
    };
    Some(LixValueShape {
        list_depth: left.list_depth,
        kind,
    })
}

pub(crate) fn field_lix_value_kind(field: &Field) -> Option<&'static str> {
    if field_is_json(field) {
        Some(LIX_VALUE_TYPE_JSONB)
    } else if field_is_row_ref(field) {
        Some(LIX_VALUE_TYPE_ROW_REF)
    } else {
        None
    }
}

fn union_value_shape(inputs: &[Arc<LogicalPlan>], index: usize) -> Option<LixValueShape> {
    let mut shape = None;
    for input in inputs {
        if plan_output_is_sql_null(input, index) {
            continue;
        }
        let Some(field) = input.schema().fields().get(index) else {
            return None;
        };
        let Some(input_shape) = field_lix_value_shape(field.as_ref()) else {
            return None;
        };
        shape = Some(match shape {
            Some(shape) => combine_value_shapes(shape, input_shape)?,
            None => input_shape,
        });
    }
    shape
}

fn field_with_expr_kind(field: &Field, expr: &Expr, schema: &DFSchema) -> Field {
    field_with_lix_value_shape(field, expr_lix_value_shape(expr, schema).as_ref())
}

fn field_with_lix_value_shape(field: &Field, shape: Option<&LixValueShape>) -> Field {
    let field = clear_lix_value_metadata(field);
    let Some(shape) = shape else {
        return field;
    };
    match shape.kind {
        Some(kind) if shape.list_depth == 0 => field_with_kind(&field, Some(kind)),
        Some(kind) => {
            let mut metadata = field.metadata().clone();
            metadata.insert(
                LIX_VALUE_SHAPE_METADATA_KEY.to_owned(),
                format!("v1:{}:{kind}", shape.list_depth),
            );
            field.with_metadata(metadata)
        }
        None => field,
    }
}

fn clear_lix_value_metadata(field: &Field) -> Field {
    let mut metadata = field.metadata().clone();
    metadata.remove(LIX_VALUE_TYPE_METADATA_KEY);
    metadata.remove(LIX_ARRAY_ELEMENT_VALUE_TYPE_METADATA_KEY);
    metadata.remove(LIX_VALUE_SHAPE_METADATA_KEY);
    field.clone().with_metadata(metadata)
}

fn plan_output_is_sql_null(plan: &LogicalPlan, index: usize) -> bool {
    matches!(plan, LogicalPlan::Projection(projection)
        if projection.expr.get(index).is_some_and(is_sql_null_expression))
}

pub(crate) fn field_with_kind(field: &Field, kind: Option<&'static str>) -> Field {
    let mut metadata = field.metadata().clone();
    match kind {
        Some(kind) => {
            metadata.insert(LIX_VALUE_TYPE_METADATA_KEY.to_string(), kind.to_string());
        }
        None => {
            metadata.remove(LIX_VALUE_TYPE_METADATA_KEY);
        }
    }
    field.clone().with_metadata(metadata)
}

pub(crate) fn is_sql_null_expression(expr: &Expr) -> bool {
    match expr {
        Expr::Alias(alias) => is_sql_null_expression(&alias.expr),
        Expr::Literal(value, _) => value.is_null(),
        Expr::Case(case) => {
            case.when_then_expr
                .iter()
                .all(|(_, result)| is_sql_null_expression(result))
                && case
                    .else_expr
                    .as_deref()
                    .is_none_or(is_sql_null_expression)
        }
        Expr::ScalarFunction(function)
            if matches!(
                function.func.name().to_ascii_lowercase().as_str(),
                "coalesce" | "ifnull" | "nvl"
            ) =>
        {
            function.args.iter().all(is_sql_null_expression)
        }
        Expr::ScalarFunction(function)
            if function.func.name().eq_ignore_ascii_case("nullif")
                && function.args.len() == 2 =>
        {
            function.args.iter().all(is_sql_null_expression)
        }
        _ => false,
    }
}

fn field_metadata_kind(
    metadata: &datafusion::common::metadata::FieldMetadata,
) -> Option<&'static str> {
    metadata
        .inner()
        .get(LIX_VALUE_TYPE_METADATA_KEY)
        .and_then(|kind| match kind.as_str() {
            LIX_VALUE_TYPE_JSONB => Some(LIX_VALUE_TYPE_JSONB),
            LIX_VALUE_TYPE_ROW_REF => Some(LIX_VALUE_TYPE_ROW_REF),
            _ => None,
        })
}

fn replace_schema_fields(
    schema: &DFSchema,
    fields: Vec<Field>,
) -> Result<Arc<DFSchema>, DataFusionError> {
    let functional_dependencies = schema.functional_dependencies().clone();
    let qualified_fields = fields
        .into_iter()
        .enumerate()
        .map(|(index, field)| (schema.qualified_field(index).0.cloned(), Arc::new(field)))
        .collect();
    let schema = DFSchema::new_with_metadata(qualified_fields, schema.metadata().clone())?;
    schema
        .with_functional_dependencies(functional_dependencies)
        .map(Arc::new)
}
