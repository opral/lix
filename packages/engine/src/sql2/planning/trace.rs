use std::fmt::Write as _;

use super::super::contracts::planned_statement::PlannedStatementSet;
use super::super::contracts::postprocess_actions::PostprocessPlan;

pub(crate) fn plan_fingerprint(output: &PlannedStatementSet) -> String {
    let mut serialized = String::new();

    serialized.push_str("sql:");
    serialized.push_str(&output.sql);
    serialized.push('\u{1f}');

    for statement in &output.prepared_statements {
        serialized.push_str("stmt:");
        serialized.push_str(&statement.sql);
        serialized.push('\u{1e}');
        for value in &statement.params {
            let encoded =
                serde_json::to_string(value).expect("serializing statement params must succeed");
            serialized.push_str(&encoded);
            serialized.push('\u{1d}');
        }
        serialized.push('\u{1f}');
    }

    for registration in &output.registrations {
        serialized.push_str("registration:");
        serialized.push_str(&registration.schema_key);
        serialized.push('\u{1f}');
    }

    match &output.postprocess {
        None => serialized.push_str("postprocess:none"),
        Some(PostprocessPlan::VtableUpdate(plan)) => {
            serialized.push_str("postprocess:vtable_update:");
            serialized.push_str(&plan.schema_key);
            serialized.push('\u{1e}');
            match &plan.explicit_writer_key {
                Some(Some(value)) => {
                    serialized.push_str("writer:");
                    serialized.push_str(value);
                }
                Some(None) => serialized.push_str("writer:null"),
                None => serialized.push_str("writer:implicit"),
            }
            serialized.push('\u{1e}');
            let _ = write!(
                serialized,
                "writer_assignment:{}",
                plan.writer_key_assignment_present
            );
        }
        Some(PostprocessPlan::VtableDelete(plan)) => {
            serialized.push_str("postprocess:vtable_delete:");
            serialized.push_str(&plan.schema_key);
            serialized.push('\u{1e}');
            let _ = write!(
                serialized,
                "scope_fallback:{}",
                plan.effective_scope_fallback
            );
            serialized.push('\u{1e}');
            match &plan.effective_scope_selection_sql {
                Some(sql) => serialized.push_str(sql),
                None => serialized.push_str("scope_sql:none"),
            }
        }
    }
    serialized.push('\u{1f}');

    for mutation in &output.mutations {
        let _ = write!(
            serialized,
            "mutation:{:?}|{}|{}|{}|{}|{}|{}|{}|",
            mutation.operation,
            mutation.entity_id,
            mutation.schema_key,
            mutation.schema_version,
            mutation.file_id,
            mutation.version_id,
            mutation.plugin_key,
            mutation.untracked
        );
        match &mutation.snapshot_content {
            Some(snapshot) => serialized.push_str(&snapshot.to_string()),
            None => serialized.push_str("snapshot:none"),
        }
        serialized.push('\u{1f}');
    }

    for validation in &output.update_validations {
        serialized.push_str("validation:");
        serialized.push_str(&validation.table);
        serialized.push('\u{1e}');
        match &validation.where_clause {
            Some(clause) => serialized.push_str(&clause.to_string()),
            None => serialized.push_str("where:none"),
        }
        serialized.push('\u{1e}');
        match &validation.snapshot_content {
            Some(snapshot) => serialized.push_str(&snapshot.to_string()),
            None => serialized.push_str("snapshot_content:none"),
        }
        serialized.push('\u{1e}');
        match &validation.snapshot_patch {
            Some(patch) => serialized.push_str(
                &serde_json::to_string(patch).expect("snapshot patch serialization must succeed"),
            ),
            None => serialized.push_str("snapshot_patch:none"),
        }
        serialized.push('\u{1f}');
    }

    blake3::hash(serialized.as_bytes()).to_hex().to_string()
}
