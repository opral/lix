pub(crate) mod api;
pub(crate) mod fallback;
pub(crate) mod contracts;
pub(crate) mod in_transaction;
pub(crate) mod prepared;
pub(crate) mod scripts;
pub(crate) mod side_effects;
pub(crate) mod transaction;
pub(crate) mod execution;
pub(crate) mod planning;

#[cfg(test)]
pub(super) use fallback::should_sequentialize_postprocess_multi_statement;
pub(super) use prepared::execute_prepared_with_transaction;
