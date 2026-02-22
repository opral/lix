use sqlparser::ast::Insert;

use crate::functions::LixFunctionProvider;
use crate::sql::steps::vtable_write;
use crate::sql::DetectedFileDomainChange;
use crate::{LixBackend, LixError, Value};

use super::types::WriteRewriteOutput;

pub(crate) fn merge_rewrite_output(
    base: &mut WriteRewriteOutput,
    mut next: WriteRewriteOutput,
) -> Result<(), LixError> {
    if base.postprocess.is_some() && next.postprocess.is_some() {
        return Err(LixError {
            message: "only one postprocess rewrite is supported per query".to_string(),
        });
    }
    if base.postprocess.is_none() {
        base.postprocess = next.postprocess.take();
    }
    base.statements.extend(next.statements);
    base.params.extend(next.params);
    base.registrations.extend(next.registrations);
    base.mutations.extend(next.mutations);
    base.update_validations.extend(next.update_validations);
    Ok(())
}

pub(crate) async fn rewrite_vtable_inserts_with_backend<P: LixFunctionProvider>(
    backend: &dyn LixBackend,
    inserts: Vec<Insert>,
    params: &[Value],
    functions: &mut P,
    detected_file_domain_changes: &[DetectedFileDomainChange],
    writer_key: Option<&str>,
) -> Result<WriteRewriteOutput, LixError> {
    let mut statements = Vec::new();
    let mut generated_params = Vec::new();
    let mut generated_param_offset = 0usize;
    let mut registrations = Vec::new();
    let mut mutations = Vec::new();

    for insert in inserts {
        let Some(rewritten) = vtable_write::rewrite_insert_with_backend(
            backend,
            insert,
            params,
            generated_param_offset,
            detected_file_domain_changes,
            writer_key,
            functions,
        )
        .await?
        else {
            return Err(LixError {
                message: "lix_version rewrite expected backend vtable insert rewrite".to_string(),
            });
        };
        statements.extend(rewritten.statements);
        generated_params.extend(rewritten.params);
        generated_param_offset = generated_params.len();
        registrations.extend(rewritten.registrations);
        mutations.extend(rewritten.mutations);
    }

    Ok(WriteRewriteOutput {
        statements,
        params: generated_params,
        registrations,
        postprocess: None,
        mutations,
        update_validations: Vec::new(),
    })
}
