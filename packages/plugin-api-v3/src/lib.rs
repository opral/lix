//! Author-facing hard-cut API for Lix Component v3 plugins.
//!
//! Plugins retain no document object. Every call receives host capabilities
//! for immutable accepted and prospective roots, and returns only a bounded
//! cursor plus the same staged transaction capability.

#![allow(clippy::missing_errors_doc)]

use std::cell::RefCell;
use std::collections::VecDeque;
use std::marker::PhantomData;

wit_bindgen::generate!({
    path: "../rs-sdk/wit/v3",
    world: "plugin",
    pub_export_macro: true,
    export_macro_name: "__export_component_v3",
    default_bindings_module: "lix_plugin_api_v3",
});

pub use exports::lix::plugin::api::{
    ByteEdit, CreateContext, EntityChange, EntityUpdate, FileDescriptor, FileUpdate,
    OpenEntitiesInput, OpenFileInput,
};
use exports::lix::plugin::api::{
    ChangeCursor, EditCursor, EntityTransition, FileTransition, Guest, GuestChangeCursor,
    GuestEditCursor, PluginError,
};
pub use lix::plugin::arena::{Budget, Root, Transaction};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    InvalidInput(String),
    RecordTooLarge(u64),
    LimitExceeded(String),
    DeadlineExceeded,
    Internal(String),
}

impl Error {
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::InvalidInput(message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct FileResult {
    pub successor: Transaction,
    pub changes: Vec<EntityChange>,
}

#[derive(Debug)]
pub struct EntityResult {
    pub successor: Transaction,
    pub edits: Vec<ByteEdit>,
}

/// Stateless format behavior over host-owned roots. Implementations may cache
/// call-local decoded pages, but no value survives a transition in guest
/// memory.
pub trait FormatPlugin {
    fn open_file(budget: &Budget, input: OpenFileInput) -> Result<FileResult>;
    fn file_changed(budget: &Budget, input: FileUpdate) -> Result<FileResult>;
    fn open_entities(budget: &Budget, input: OpenEntitiesInput) -> Result<EntityResult>;
    fn entities_changed(budget: &Budget, input: EntityUpdate) -> Result<EntityResult>;
}

#[doc(hidden)]
#[derive(Debug)]
pub struct Component<P>(PhantomData<P>);

#[doc(hidden)]
#[derive(Debug)]
pub struct AuthorChangeCursor {
    state: RefCell<CursorState<EntityChange>>,
}

#[doc(hidden)]
#[derive(Debug)]
pub struct AuthorEditCursor {
    state: RefCell<CursorState<ByteEdit>>,
}

#[derive(Debug)]
struct CursorState<T> {
    pending: VecDeque<T>,
    eof: bool,
}

impl<P: FormatPlugin> Guest for Component<P> {
    type ChangeCursor = AuthorChangeCursor;
    type EditCursor = AuthorEditCursor;

    fn open_file(
        budget: &Budget,
        input: OpenFileInput,
    ) -> std::result::Result<FileTransition, PluginError> {
        P::open_file(budget, input)
            .map(file_transition)
            .map_err(plugin_error)
    }

    fn file_changed(
        budget: &Budget,
        input: FileUpdate,
    ) -> std::result::Result<FileTransition, PluginError> {
        P::file_changed(budget, input)
            .map(file_transition)
            .map_err(plugin_error)
    }

    fn open_entities(
        budget: &Budget,
        input: OpenEntitiesInput,
    ) -> std::result::Result<EntityTransition, PluginError> {
        P::open_entities(budget, input)
            .map(entity_transition)
            .map_err(plugin_error)
    }

    fn entities_changed(
        budget: &Budget,
        input: EntityUpdate,
    ) -> std::result::Result<EntityTransition, PluginError> {
        P::entities_changed(budget, input)
            .map(entity_transition)
            .map_err(plugin_error)
    }
}

impl GuestChangeCursor for AuthorChangeCursor {
    fn next(
        &self,
        _budget: &Budget,
        max_bytes: u32,
    ) -> std::result::Result<Option<Vec<EntityChange>>, PluginError> {
        next_page(&mut self.state.borrow_mut(), max_bytes, entity_change_bytes)
            .map_err(plugin_error)
    }
}

impl GuestEditCursor for AuthorEditCursor {
    fn next(
        &self,
        _budget: &Budget,
        max_bytes: u32,
    ) -> std::result::Result<Option<Vec<ByteEdit>>, PluginError> {
        next_page(&mut self.state.borrow_mut(), max_bytes, byte_edit_bytes).map_err(plugin_error)
    }
}

fn file_transition(result: FileResult) -> FileTransition {
    FileTransition {
        successor: result.successor,
        changes: ChangeCursor::new(AuthorChangeCursor {
            state: RefCell::new(CursorState {
                pending: result.changes.into(),
                eof: false,
            }),
        }),
    }
}

fn entity_transition(result: EntityResult) -> EntityTransition {
    EntityTransition {
        successor: result.successor,
        edits: EditCursor::new(AuthorEditCursor {
            state: RefCell::new(CursorState {
                pending: result.edits.into(),
                eof: false,
            }),
        }),
    }
}

fn next_page<T>(
    state: &mut CursorState<T>,
    max_bytes: u32,
    measure: impl Fn(&T) -> u64,
) -> Result<Option<Vec<T>>> {
    if state.eof {
        return Ok(None);
    }
    if state.pending.is_empty() {
        state.eof = true;
        return Ok(None);
    }
    let mut bytes = 0_u64;
    let mut output = Vec::new();
    while let Some(item) = state.pending.front() {
        let item_bytes = measure(item);
        if output.is_empty() && item_bytes > u64::from(max_bytes) {
            return Err(Error::RecordTooLarge(item_bytes));
        }
        if bytes.saturating_add(item_bytes) > u64::from(max_bytes) {
            break;
        }
        bytes = bytes.saturating_add(item_bytes);
        output.push(
            state
                .pending
                .pop_front()
                .expect("front item must still be present"),
        );
    }
    Ok(Some(output))
}

fn entity_change_bytes(change: &EntityChange) -> u64 {
    let mut bytes = 16_u64.saturating_add(change.schema_key.len() as u64);
    for part in &change.entity_pk {
        bytes = bytes.saturating_add(4).saturating_add(part.len() as u64);
    }
    bytes.saturating_add(change.snapshot.as_ref().map_or(0, Vec::len) as u64)
}

fn byte_edit_bytes(edit: &ByteEdit) -> u64 {
    24_u64.saturating_add(edit.insert.len() as u64)
}

fn plugin_error(error: Error) -> PluginError {
    match error {
        Error::InvalidInput(message) => PluginError::InvalidInput(message),
        Error::RecordTooLarge(bytes) => PluginError::RecordTooLarge(bytes),
        Error::LimitExceeded(message) => PluginError::LimitExceeded(message),
        Error::DeadlineExceeded => PluginError::DeadlineExceeded,
        Error::Internal(message) => PluginError::Internal(message),
    }
}

#[macro_export]
macro_rules! export_v3 {
    ($plugin:ty) => {
        type __LixPluginApiV3Component = $crate::Component<$plugin>;
        $crate::__export_component_v3!(__LixPluginApiV3Component);
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_never_emits_an_empty_non_eof_page() {
        let mut state = CursorState {
            pending: VecDeque::from([ByteEdit {
                offset: 0,
                delete_len: 0,
                insert: b"x".to_vec(),
            }]),
            eof: false,
        };
        assert_eq!(
            next_page(&mut state, 25, byte_edit_bytes)
                .unwrap()
                .unwrap()
                .len(),
            1
        );
        assert!(
            next_page(&mut state, 25, byte_edit_bytes)
                .unwrap()
                .is_none()
        );
        assert!(
            next_page(&mut state, 25, byte_edit_bytes)
                .unwrap()
                .is_none()
        );
    }
}
