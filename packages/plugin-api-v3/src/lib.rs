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
    ByteEdit, ChangedEntity, CreateContext, EntityChange, EntityUpdate, FileDescriptor, FileUpdate,
    InputBytes, InputSplice, OpenEntitiesInput, OpenFileInput,
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

pub fn entity_key(schema_key: &str, entity_pk: &[String]) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    append_key_part(&mut output, schema_key.as_bytes())?;
    for part in entity_pk {
        append_key_part(&mut output, part.as_bytes())?;
    }
    Ok(output)
}

pub fn decode_entity_key(key: &[u8]) -> Result<(String, Vec<String>)> {
    let mut offset = 0usize;
    let schema_key = take_key_part(key, &mut offset)?;
    let mut entity_pk = Vec::new();
    while offset < key.len() {
        entity_pk.push(take_key_part(key, &mut offset)?);
    }
    if entity_pk.is_empty() {
        return Err(Error::invalid_input(
            "v3 entity arena key has no primary-key components",
        ));
    }
    Ok((schema_key, entity_pk))
}

fn append_key_part(output: &mut Vec<u8>, part: &[u8]) -> Result<()> {
    let len = u32::try_from(part.len())
        .map_err(|_| Error::invalid_input("v3 entity key component exceeds u32"))?;
    output.extend_from_slice(&len.to_le_bytes());
    output.extend_from_slice(part);
    Ok(())
}

fn take_key_part(key: &[u8], offset: &mut usize) -> Result<String> {
    let len_end = offset
        .checked_add(4)
        .ok_or_else(|| Error::invalid_input("v3 entity arena key overflowed"))?;
    let len = key
        .get(*offset..len_end)
        .ok_or_else(|| Error::invalid_input("truncated v3 entity arena key length"))?;
    let len = u32::from_le_bytes(len.try_into().expect("length slice is exactly four bytes"));
    let value_end = len_end
        .checked_add(len as usize)
        .ok_or_else(|| Error::invalid_input("v3 entity arena key overflowed"))?;
    let value = key
        .get(len_end..value_end)
        .ok_or_else(|| Error::invalid_input("truncated v3 entity arena key value"))?;
    *offset = value_end;
    String::from_utf8(value.to_vec())
        .map_err(|_| Error::invalid_input("v3 entity arena key is not UTF-8"))
}

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
            .and_then(|result| file_transition(result, budget.limits().max_page_bytes))
            .map_err(plugin_error)
    }

    fn file_changed(
        budget: &Budget,
        input: FileUpdate,
    ) -> std::result::Result<FileTransition, PluginError> {
        P::file_changed(budget, input)
            .and_then(|result| file_transition(result, budget.limits().max_page_bytes))
            .map_err(plugin_error)
    }

    fn open_entities(
        budget: &Budget,
        input: OpenEntitiesInput,
    ) -> std::result::Result<EntityTransition, PluginError> {
        P::open_entities(budget, input)
            .and_then(|result| entity_transition(result, budget.limits().max_page_bytes))
            .map_err(plugin_error)
    }

    fn entities_changed(
        budget: &Budget,
        input: EntityUpdate,
    ) -> std::result::Result<EntityTransition, PluginError> {
        P::entities_changed(budget, input)
            .and_then(|result| entity_transition(result, budget.limits().max_page_bytes))
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

fn file_transition(result: FileResult, max_bytes: u32) -> Result<FileTransition> {
    let (first_changes, remaining) =
        split_first_page(result.changes, max_bytes, entity_change_bytes)?;
    Ok(FileTransition {
        successor: result.successor,
        first_changes,
        changes: (!remaining.is_empty()).then(|| {
            ChangeCursor::new(AuthorChangeCursor {
                state: RefCell::new(CursorState {
                    pending: remaining,
                    eof: false,
                }),
            })
        }),
    })
}

fn entity_transition(result: EntityResult, max_bytes: u32) -> Result<EntityTransition> {
    let (first_edits, remaining) = split_first_page(result.edits, max_bytes, byte_edit_bytes)?;
    Ok(EntityTransition {
        successor: result.successor,
        first_edits,
        edits: (!remaining.is_empty()).then(|| {
            EditCursor::new(AuthorEditCursor {
                state: RefCell::new(CursorState {
                    pending: remaining,
                    eof: false,
                }),
            })
        }),
    })
}

fn split_first_page<T>(
    values: Vec<T>,
    max_bytes: u32,
    measure: impl Copy + Fn(&T) -> u64,
) -> Result<(Vec<T>, VecDeque<T>)> {
    let mut state = CursorState {
        pending: values.into(),
        eof: false,
    };
    let first = next_page(&mut state, max_bytes, measure)?.unwrap_or_default();
    Ok((first, state.pending))
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

    #[test]
    fn entity_arena_key_round_trips_without_delimiter_ambiguity() {
        let expected = (
            "json_object_member".to_owned(),
            vec!["a/b".to_owned(), "c".to_owned()],
        );
        let encoded = entity_key(&expected.0, &expected.1).unwrap();
        assert_eq!(decode_entity_key(&encoded).unwrap(), expected);
        assert_ne!(
            entity_key("a", &["bc".to_owned()]).unwrap(),
            entity_key("ab", &["c".to_owned()]).unwrap()
        );
    }
}
