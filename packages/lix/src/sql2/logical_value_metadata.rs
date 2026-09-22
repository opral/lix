//! Propagation of Lix's logical value kinds through DataFusion plans.
//!
//! JSONB and row references use Arrow UTF-8 arrays, so their logical kind is
//! carried in field metadata. DataFusion's generic expression schema rules
//! intentionally drop metadata for conditional expressions and intersect UNION
//! metadata across every input. That is correct for ordinary Arrow metadata,
//! but loses Lix's value contract when a branch is a SQL NULL. Keep this pass
//! at the logical-plan boundary so predicates, subqueries, and final result
//! fields all see the same logical kind.

use std::sync::Arc;

use datafusion::arrow::datatypes::Field;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, DataFusionError};
use datafusion::logical_expr::expr::{Alias, ScalarFunction};
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan};

use super::result_metadata::{
    LIX_VALUE_TYPE_JSONB, LIX_VALUE_TYPE_METADATA_KEY, LIX_VALUE_TYPE_ROW_REF, field_is_json,
    field_is_row_ref,
};

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
                        let kind = union_value_kind(&union.inputs, index);
                        field_with_kind(field.as_ref(), kind)
                    })
                    .collect::<Vec<_>>();
                union.schema = replace_schema_fields(&union.schema, fields)?;
                LogicalPlan::Union(union)
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
    match expr {
        Expr::Alias(Alias { expr, .. }) => expr_lix_value_kind(expr, schema),
        Expr::Column(column) => schema
            .field_with_name(column.relation.as_ref(), &column.name)
            .ok()
            .and_then(|field| field_lix_value_kind(field.as_ref())),
        Expr::OuterReferenceColumn(field, _) | Expr::ScalarVariable(field, _) => {
            field_lix_value_kind(field.as_ref())
        }
        Expr::Literal(_, metadata) => metadata.as_ref().and_then(field_metadata_kind),
        Expr::ScalarSubquery(subquery) => subquery
            .subquery
            .schema()
            .fields()
            .first()
            .and_then(|field| field_lix_value_kind(field.as_ref())),
        Expr::Case(case) => common_expr_kind(
            case.when_then_expr
                .iter()
                .map(|(_, expr)| expr.as_ref())
                .chain(case.else_expr.iter().map(AsRef::as_ref)),
            schema,
        ),
        Expr::ScalarFunction(ScalarFunction { func, args }) => {
            match func.name().to_ascii_lowercase().as_str() {
                "coalesce" | "ifnull" | "nvl" => common_expr_kind(args.iter(), schema),
                // NULLIF returns its first argument, or SQL NULL when the
                // arguments compare equal. Its logical type is therefore the
                // first argument's logical type.
                "nullif" => args.first().and_then(|first| {
                    expr_lix_value_kind(first, schema).or_else(|| {
                        is_sql_null_literal(first)
                            .then(|| args.get(1).and_then(|expr| expr_lix_value_kind(expr, schema)))
                            .flatten()
                    })
                }),
                _ => expr
                    .to_field(schema)
                    .ok()
                    .and_then(|(_, field)| field_lix_value_kind(field.as_ref())),
            }
        }
        Expr::WindowFunction(window)
            if matches!(
                window.fun.name().to_ascii_lowercase().as_str(),
                "first_value" | "last_value" | "nth_value" | "lead" | "lag"
            ) =>
        {
            window
                .params
                .args
                .first()
                .and_then(|expr| expr_lix_value_kind(expr, schema))
        }
        // DataFusion's Cast schema preserves input metadata even when the
        // target type is no longer the UTF-8 representation used by Lix's
        // logical JSONB/ROW_REF values. A cast is therefore an explicit
        // logical-type boundary; supported JSONB casts are lowered to the
        // metadata-producing __lix_jsonb UDF before this pass.
        Expr::Cast(_) | Expr::TryCast(_) => None,
        _ => expr
            .to_field(schema)
            .ok()
            .and_then(|(_, field)| field_lix_value_kind(field.as_ref())),
    }
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

fn common_expr_kind<'a>(
    expressions: impl IntoIterator<Item = &'a Expr>,
    schema: &DFSchema,
) -> Option<&'static str> {
    let mut kinds = Vec::new();
    for expr in expressions {
        if is_sql_null_literal(expr) {
            continue;
        }
        kinds.push(expr_lix_value_kind(expr, schema)?);
    }
    common_kind(kinds)
}

fn common_kind<I>(kinds: I) -> Option<&'static str>
where
    I: IntoIterator<Item = &'static str>,
{
    let mut kinds = kinds.into_iter();
    let first = kinds.next()?;
    kinds.all(|kind| kind == first).then_some(first)
}

fn union_value_kind(inputs: &[Arc<LogicalPlan>], index: usize) -> Option<&'static str> {
    let mut saw_value = false;
    let mut kind = None;
    for input in inputs {
        let Some(field) = input.schema().fields().get(index) else {
            return None;
        };
        if field.data_type().is_null() {
            continue;
        }
        saw_value = true;
        let Some(input_kind) = field_lix_value_kind(field.as_ref()) else {
            return None;
        };
        if let Some(kind) = kind {
            if kind != input_kind {
                return None;
            }
        } else {
            kind = Some(input_kind);
        }
    }
    saw_value.then_some(kind?)
}

fn field_with_expr_kind(field: &Field, expr: &Expr, schema: &DFSchema) -> Field {
    field_with_kind(field, expr_lix_value_kind(expr, schema))
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

fn is_sql_null_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(value, _) if value.is_null())
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
