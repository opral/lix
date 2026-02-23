use crate::sql::PreprocessOutput;

pub(crate) fn plan_fingerprint(output: &PreprocessOutput) -> String {
    crate::sql::preprocess_plan_fingerprint(output)
}
