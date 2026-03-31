//! Explicit owner-hydration seams for compiler-core SQL.
//!
//! These helpers isolate the owner lookups that must happen before or during
//! SQL planning so the surrounding semantic/logical stages can stay focused on
//! SQL-owned data.

use crate::sql::catalog::{SurfaceFamily, SurfaceVariant};
use crate::sql::logical_plan::public_ir::StructuredPublicRead;
use crate::sql::semantic_ir::semantics::effective_state_resolver::{
    resolve_exact_effective_state_row_with_pending_transaction_view, ExactEffectiveStateRow,
    ExactEffectiveStateRowRequest,
};
use crate::transaction::PendingTransactionView;
use crate::{LixBackend, LixError};
use sqlparser::ast::{BinaryOperator, Expr, Ident, Value as SqlValue};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone)]
pub(crate) struct HydratedVersionAdminRow {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) hidden: bool,
    pub(crate) commit_id: String,
    pub(crate) descriptor_change_id: Option<String>,
    pub(crate) has_local_head: bool,
}

pub(crate) async fn hydrate_structured_public_read(
    backend: &dyn LixBackend,
    mut structured_read: StructuredPublicRead,
    active_version_id: &str,
) -> Result<Option<StructuredPublicRead>, LixError> {
    let descriptor = &structured_read.surface_binding.descriptor;
    let public_name = descriptor.public_name.as_str();
    let uses_active_history_root = descriptor.surface_variant == SurfaceVariant::History
        && matches!(
            descriptor.surface_family,
            SurfaceFamily::State | SurfaceFamily::Entity | SurfaceFamily::Filesystem
        )
        && !public_name.ends_with("_history_by_version");
    if !uses_active_history_root || structured_read_has_root_commit_predicate(&structured_read) {
        return Ok(Some(structured_read));
    }

    let Some(version_context) = crate::version::context::load_target_version_context_with_backend(
        backend,
        Some(active_version_id),
        "active_version_id",
    )
    .await?
    else {
        return Ok(None);
    };
    let root_commit_id = version_context.history_root_commit_id().to_string();
    let root_predicate = Expr::BinaryOp {
        left: Box::new(Expr::Identifier(Ident::new("lixcol_root_commit_id"))),
        op: BinaryOperator::Eq,
        right: Box::new(Expr::Value(
            SqlValue::SingleQuotedString(root_commit_id).into(),
        )),
    };

    structured_read.query.selection = Some(match structured_read.query.selection.take() {
        Some(existing) => Expr::BinaryOp {
            left: Box::new(existing),
            op: BinaryOperator::And,
            right: Box::new(root_predicate.clone()),
        },
        None => root_predicate.clone(),
    });
    structured_read
        .query
        .selection_predicates
        .push(root_predicate);
    Ok(Some(structured_read))
}

pub(crate) struct PublicWriteHydrator<'a> {
    backend: &'a dyn LixBackend,
    pending_transaction_view: Option<&'a PendingTransactionView>,
    version_admin_rows: BTreeMap<String, Option<HydratedVersionAdminRow>>,
    validated_version_targets: BTreeSet<String>,
}

impl<'a> PublicWriteHydrator<'a> {
    pub(crate) fn new(
        backend: &'a dyn LixBackend,
        pending_transaction_view: Option<&'a PendingTransactionView>,
    ) -> Self {
        Self {
            backend,
            pending_transaction_view,
            version_admin_rows: BTreeMap::new(),
            validated_version_targets: BTreeSet::new(),
        }
    }

    pub(crate) fn backend(&self) -> &dyn LixBackend {
        self.backend
    }

    pub(crate) fn pending_transaction_view(&self) -> Option<&'a PendingTransactionView> {
        self.pending_transaction_view
    }

    pub(crate) async fn load_version_admin_row(
        &mut self,
        version_id: &str,
    ) -> Result<Option<HydratedVersionAdminRow>, LixError> {
        if let Some(row) = self.version_admin_rows.get(version_id) {
            return Ok(row.clone());
        }

        let row = self.fetch_version_admin_row(version_id).await?;
        self.version_admin_rows
            .insert(version_id.to_string(), row.clone());
        Ok(row)
    }

    async fn fetch_version_admin_row(
        &self,
        version_id: &str,
    ) -> Result<Option<HydratedVersionAdminRow>, LixError> {
        let Some(descriptor_row) =
            crate::canonical::read::load_version_descriptor_with_backend(self.backend, version_id)
                .await?
        else {
            return Ok(None);
        };
        let pointer_row =
            crate::refs::load_committed_version_ref_with_backend(self.backend, version_id).await?;
        let has_local_head = pointer_row.is_some();
        Ok(Some(HydratedVersionAdminRow {
            id: version_id.to_string(),
            name: descriptor_row.name,
            hidden: descriptor_row.hidden,
            commit_id: pointer_row
                .as_ref()
                .map(|row| row.commit_id.clone())
                .unwrap_or_default(),
            descriptor_change_id: descriptor_row.change_id,
            has_local_head,
        }))
    }

    pub(crate) async fn validate_version_target(
        &mut self,
        version_id: &str,
    ) -> Result<(), LixError> {
        if version_id == crate::version::GLOBAL_VERSION_ID
            || !self
                .validated_version_targets
                .insert(version_id.to_string())
        {
            return Ok(());
        }

        let Some(row) = self.load_version_admin_row(version_id).await? else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("version with id '{version_id}' does not exist"),
            ));
        };
        if !row.has_local_head {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "public write invariant violation: version with id '{version_id}' exists but its local version head is missing"
                ),
            ));
        }

        Ok(())
    }

    pub(crate) async fn resolve_exact_effective_state_row(
        &self,
        request: &ExactEffectiveStateRowRequest,
    ) -> Result<Option<ExactEffectiveStateRow>, LixError> {
        resolve_exact_effective_state_row_with_pending_transaction_view(
            self.backend,
            request,
            self.pending_transaction_view,
        )
        .await
    }
}

fn structured_read_has_root_commit_predicate(structured_read: &StructuredPublicRead) -> bool {
    structured_read
        .query
        .selection_predicates
        .iter()
        .any(expr_has_root_commit_predicate)
}

fn expr_has_root_commit_predicate(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            expr_references_root_commit(left) || expr_references_root_commit(right)
        }
        Expr::Nested(inner) => expr_has_root_commit_predicate(inner),
        _ => false,
    }
}

fn expr_references_root_commit(expr: &Expr) -> bool {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => matches!(
            parts[1].value.to_ascii_lowercase().as_str(),
            "lixcol_root_commit_id" | "root_commit_id"
        ),
        Expr::Identifier(identifier) => matches!(
            identifier.value.to_ascii_lowercase().as_str(),
            "lixcol_root_commit_id" | "root_commit_id"
        ),
        Expr::Nested(inner) => expr_references_root_commit(inner),
        _ => false,
    }
}
