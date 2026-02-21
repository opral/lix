mod entry;
mod fallback;
mod in_transaction;
mod prepared;
mod scripts;
mod side_effects;
mod transaction;

#[cfg(test)]
pub(super) use fallback::should_sequentialize_postprocess_multi_statement;
pub(super) use fallback::should_sequentialize_postprocess_multi_statement_with_statements;
pub(super) use prepared::{execute_prepared_with_backend, execute_prepared_with_transaction};
