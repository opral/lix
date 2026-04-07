//! Version-scoped committed-history undo/redo.
//!
//! This owner resolves a target version head, reconstructs semantic undo/redo
//! state from committed lineage plus operation records, and authors inverse or
//! replay commits against that version.

mod apply;
mod init;
mod types;

use crate::{ExecuteOptions, LixError, Session};

pub(crate) use init::init;
pub use types::{RedoOptions, RedoResult, UndoOptions, UndoResult};

pub(crate) async fn undo_with_options_in_session(
    session: &Session,
    options: UndoOptions,
) -> Result<UndoResult, LixError> {
    session
        .transaction(ExecuteOptions::default(), move |tx| {
            let requested_version_id = options.version_id.clone();
            Box::pin(async move {
                apply::undo_in_session_transaction(tx, requested_version_id.as_deref()).await
            })
        })
        .await
}

pub(crate) async fn redo_with_options_in_session(
    session: &Session,
    options: RedoOptions,
) -> Result<RedoResult, LixError> {
    session
        .transaction(ExecuteOptions::default(), move |tx| {
            let requested_version_id = options.version_id.clone();
            Box::pin(async move {
                apply::redo_in_session_transaction(tx, requested_version_id.as_deref()).await
            })
        })
        .await
}
