//! Canonical Lix Server Protocol implementation.

#![cfg_attr(test, allow(clippy::large_futures))]

#[cfg(feature = "server-protocol")]
mod handler;
#[cfg(feature = "server-protocol")]
pub use handler::*;

#[cfg(feature = "server-protocol-client")]
pub mod client;

/// Runs one public transaction statement for a protocol session.
///
/// Protocol handlers must not call `transaction.execute(` directly; this
/// module is the allowed SQL boundary for that dispatch.
#[cfg(feature = "server-protocol")]
pub(crate) fn execute_protocol_transaction<'a, S>(
    transaction: &'a mut crate::handle::LixTransaction<S>,
    sql: &'a str,
    params: &'a [crate::Value],
    options: crate::session::ExecuteOptions,
) -> crate::handle::TransactionExecuteBuilder<'a, S>
where
    S: crate::storage::Storage + Clone + Send + Sync + 'static,
{
    let execution = transaction.execute(sql, params);
    match options.origin_key {
        Some(origin_key) => execution.with_origin_key(origin_key),
        None => execution,
    }
}
