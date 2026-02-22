use sqlparser::ast::{Delete, Insert, Statement, Update};

use crate::functions::LixFunctionProvider;
use crate::sql::steps::vtable_write;
use crate::sql::types::{PostprocessPlan, RewriteOutput, UpdateValidationPlan};
use crate::sql::{expr_references_column_name, ColumnReferenceOptions, DetectedFileDomainChange};
use crate::{LixBackend, LixError, Value};

pub(crate) async fn rewrite_insert_with_backend<P: LixFunctionProvider>(
    backend: &dyn LixBackend,
    insert: Insert,
    params: &[Value],
    generated_param_offset: usize,
    detected_file_domain_changes: &[DetectedFileDomainChange],
    writer_key: Option<&str>,
    functions: &mut P,
) -> Result<Option<vtable_write::VtableWriteRewrite>, LixError> {
    vtable_write::rewrite_insert_with_backend(
        backend,
        insert,
        params,
        generated_param_offset,
        detected_file_domain_changes,
        writer_key,
        functions,
    )
    .await
}

pub(crate) fn rewrite_update(update: Update, params: &[Value]) -> Result<RewriteOutput, LixError> {
    let rewritten = vtable_write::rewrite_update(update.clone(), params)?;
    match rewritten {
        Some(vtable_write::UpdateRewrite::Statement(rewrite)) => Ok(RewriteOutput {
            statements: vec![rewrite.statement],
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: None,
            mutations: Vec::new(),
            update_validations: rewrite.validation.into_iter().collect(),
        }),
        Some(vtable_write::UpdateRewrite::Planned(rewrite)) => Ok(RewriteOutput {
            statements: vec![rewrite.statement],
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: Some(PostprocessPlan::VtableUpdate(rewrite.plan)),
            mutations: Vec::new(),
            update_validations: rewrite.validation.into_iter().collect(),
        }),
        None => Ok(RewriteOutput {
            statements: vec![Statement::Update(update)],
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: None,
            mutations: Vec::new(),
            update_validations: Vec::<UpdateValidationPlan>::new(),
        }),
    }
}

pub(crate) fn rewrite_delete(
    delete: Delete,
    effective_scope_fallback: bool,
) -> Result<RewriteOutput, LixError> {
    let rewritten = if effective_scope_fallback {
        vtable_write::rewrite_delete_with_options(delete.clone(), true)?
    } else {
        vtable_write::rewrite_delete(delete.clone())?
    };

    match rewritten {
        Some(vtable_write::DeleteRewrite::Statement(statement)) => Ok(RewriteOutput {
            statements: vec![statement],
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: None,
            mutations: Vec::new(),
            update_validations: Vec::new(),
        }),
        Some(vtable_write::DeleteRewrite::Planned(rewrite)) => Ok(RewriteOutput {
            statements: vec![rewrite.statement],
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: Some(PostprocessPlan::VtableDelete(rewrite.plan)),
            mutations: Vec::new(),
            update_validations: Vec::new(),
        }),
        None => Ok(RewriteOutput {
            statements: vec![Statement::Delete(delete)],
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: None,
            mutations: Vec::new(),
            update_validations: Vec::new(),
        }),
    }
}

pub(crate) fn selection_mentions_inherited_from_version_id(
    selection: Option<&sqlparser::ast::Expr>,
) -> bool {
    selection
        .map(|expr| {
            expr_references_column_name(
                expr,
                "inherited_from_version_id",
                ColumnReferenceOptions {
                    include_from_derived_subqueries: true,
                },
            )
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::selection_mentions_inherited_from_version_id;
    use sqlparser::ast::{Expr, Value, ValueWithSpan};

    #[test]
    fn inherited_column_detection_ignores_string_literals() {
        let selection = Expr::BinaryOp {
            left: Box::new(Expr::Identifier("metadata".into())),
            op: sqlparser::ast::BinaryOperator::Eq,
            right: Box::new(Expr::Value(ValueWithSpan::from(Value::SingleQuotedString(
                "inherited_from_version_id".to_string(),
            )))),
        };
        assert!(!selection_mentions_inherited_from_version_id(Some(
            &selection
        )));
    }

    #[test]
    fn inherited_column_detection_matches_real_column_reference() {
        let selection = Expr::IsNull(Box::new(Expr::CompoundIdentifier(vec![
            "ranked".into(),
            "inherited_from_version_id".into(),
        ])));
        assert!(selection_mentions_inherited_from_version_id(Some(
            &selection
        )));
    }
}
