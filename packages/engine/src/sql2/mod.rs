pub(crate) mod api;
pub(crate) mod ast;
pub(crate) mod contracts;
pub(crate) mod execution;
pub(crate) mod fallback;
pub(crate) mod in_transaction;
pub(crate) mod planning;
pub(crate) mod prepared;
pub(crate) mod scripts;
pub(crate) mod semantics;
pub(crate) mod side_effects;
pub(crate) mod surfaces;
pub(crate) mod transaction;
pub(crate) mod type_bridge;
pub(crate) mod vtable;

#[cfg(test)]
pub(super) use fallback::should_sequentialize_postprocess_multi_statement;
pub(super) use prepared::execute_prepared_with_transaction;
