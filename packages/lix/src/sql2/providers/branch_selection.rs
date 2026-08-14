use datafusion::logical_expr::Expr;

use crate::LixError;
use crate::branch::{BranchHead, BranchRefReader};

use super::file::FileIdConstraint;

pub(super) async fn selected_heads(
    branch_ref: &dyn BranchRefReader,
    active_branch_id: Option<&str>,
    branch_ids: &FileIdConstraint,
) -> Result<Vec<BranchHead>, LixError> {
    if let Some(branch_id) = active_branch_id {
        if !string_constraint_allows(branch_ids, branch_id) {
            return Ok(Vec::new());
        }
        return Ok(branch_ref.load_head(branch_id).await?.into_iter().collect());
    }
    if let FileIdConstraint::Ids(branch_ids) = branch_ids {
        let mut heads = Vec::with_capacity(branch_ids.len());
        for branch_id in branch_ids {
            if branch_id == crate::GLOBAL_BRANCH_ID {
                continue;
            }
            if let Some(head) = branch_ref.load_head(branch_id).await? {
                heads.push(head);
            }
        }
        return Ok(heads);
    }
    if matches!(branch_ids, FileIdConstraint::None) {
        return Ok(Vec::new());
    }
    let mut heads = branch_ref.scan_heads().await?;
    heads.retain(|head| head.branch_id != crate::GLOBAL_BRANCH_ID);
    Ok(heads)
}

fn string_constraint_allows(constraint: &FileIdConstraint, value: &str) -> bool {
    match constraint {
        FileIdConstraint::All => true,
        FileIdConstraint::None => false,
        FileIdConstraint::Ids(values) => values.contains(value),
    }
}

pub(super) fn filter_conjuncts(filters: &[Expr]) -> Vec<Expr> {
    fn append(expr: &Expr, conjuncts: &mut Vec<Expr>) {
        match expr {
            Expr::BinaryExpr(binary) if binary.op == datafusion::logical_expr::Operator::And => {
                append(&binary.left, conjuncts);
                append(&binary.right, conjuncts);
            }
            _ => conjuncts.push(expr.clone()),
        }
    }

    let mut conjuncts = Vec::new();
    for filter in filters {
        append(filter, &mut conjuncts);
    }
    conjuncts
}
