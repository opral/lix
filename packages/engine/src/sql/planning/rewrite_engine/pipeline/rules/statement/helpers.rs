use crate::engine::sql::planning::rewrite_engine::RewriteOutput;
use crate::LixError;

pub(crate) fn merge_rewrite_output(
    base: &mut RewriteOutput,
    mut next: RewriteOutput,
) -> Result<(), LixError> {
    if base.postprocess.is_some() && next.postprocess.is_some() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "only one postprocess rewrite is supported per query".to_string(),
        });
    }
    if base.postprocess.is_none() {
        base.postprocess = next.postprocess.take();
    }
    base.statements.extend(next.statements);
    base.effect_only = base.effect_only || next.effect_only;
    base.params.extend(next.params);
    base.registrations.extend(next.registrations);
    base.mutations.extend(next.mutations);
    base.update_validations.extend(next.update_validations);
    Ok(())
}
