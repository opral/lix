//! Host-neutral contract for the hard-cut Wasm Component v3 protocol.
//!
//! Accepted bytes, durable entities, and plugin-private state are represented
//! by immutable host arena roots. Guest instances receive capabilities to
//! bounded pages and never own a long-lived document resource.

use async_trait::async_trait;
use lix_plugin_arena::{Root, Transaction};

use crate::LixError;

const MIB: u64 = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasmV3TransitionLimits {
    pub max_page_bytes: u32,
    pub max_pages: u32,
    pub max_total_bytes: u64,
    pub deadline_nanoseconds: u64,
}

impl Default for WasmV3TransitionLimits {
    fn default() -> Self {
        Self {
            max_page_bytes: MIB as u32,
            max_pages: 1_024,
            max_total_bytes: 128 * MIB,
            deadline_nanoseconds: 5_000_000_000,
        }
    }
}

impl WasmV3TransitionLimits {
    pub fn validate(self) -> Result<Self, LixError> {
        if self.max_page_bytes == 0
            || self.max_pages == 0
            || self.max_total_bytes == 0
            || self.deadline_nanoseconds == 0
        {
            return Err(invalid_param(
                "v3 transition limits must be strictly positive",
            ));
        }
        if u64::from(self.max_page_bytes) > self.max_total_bytes {
            return Err(invalid_param(
                "v3 max-page-bytes must not exceed max-total-bytes",
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmV3FileDescriptor {
    pub path: Option<String>,
    pub media_type: Option<String>,
    pub plugin_key: String,
    pub generation: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasmV3CreateContext {
    pub high: u64,
    pub low: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasmV3SourceRange {
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WasmV3InputBytes {
    Inline(Vec<u8>),
    AfterRange(WasmV3SourceRange),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmV3InputSplice {
    pub offset: u64,
    pub delete_len: u64,
    pub insert: WasmV3InputBytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmV3EntityChange {
    pub schema_key: String,
    pub entity_pk: Vec<String>,
    pub snapshot: Option<Vec<u8>>,
    pub format_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmV3ByteEdit {
    pub offset: u64,
    pub delete_len: u64,
    pub insert: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmV3ChangedEntity {
    pub key: Vec<u8>,
    pub format_only: bool,
}

#[derive(Debug)]
pub struct WasmV3OpenFileInput {
    pub descriptor: WasmV3FileDescriptor,
    pub accepted: Root,
    pub successor: Transaction,
    pub creates: WasmV3CreateContext,
}

#[derive(Debug)]
pub struct WasmV3FileUpdate {
    pub before_descriptor: WasmV3FileDescriptor,
    pub after_descriptor: WasmV3FileDescriptor,
    pub before: Root,
    pub edits: Vec<WasmV3InputSplice>,
    pub successor: Transaction,
    pub creates: WasmV3CreateContext,
}

#[derive(Debug)]
pub struct WasmV3OpenEntitiesInput {
    pub descriptor: WasmV3FileDescriptor,
    pub durable: Root,
    pub successor: Transaction,
    pub creates: WasmV3CreateContext,
}

#[derive(Debug)]
pub struct WasmV3EntityUpdate {
    pub before_descriptor: WasmV3FileDescriptor,
    pub after_descriptor: WasmV3FileDescriptor,
    pub before: Root,
    pub changed_entities: Vec<WasmV3ChangedEntity>,
    pub successor: Transaction,
    pub creates: WasmV3CreateContext,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WasmV3TransitionHandle(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WasmV3ChangeCursorHandle(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WasmV3EditCursorHandle(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasmV3FileTransition {
    pub transition: WasmV3TransitionHandle,
    pub changes: WasmV3ChangeCursorHandle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasmV3EntityTransition {
    pub transition: WasmV3TransitionHandle,
    pub edits: WasmV3EditCursorHandle,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WasmV3TransitionCounters {
    pub component_boundary_bytes: u64,
    pub file_page_reads: u64,
    pub file_page_bytes: u64,
    pub entity_page_reads: u64,
    pub entity_page_bytes: u64,
    pub state_page_reads: u64,
    pub state_page_bytes: u64,
    pub guest_linear_memory_high_water_bytes: u64,
}

#[async_trait]
pub trait WasmComponentV3Factory: Send + Sync {
    async fn instantiate_actor(&self) -> Result<Box<dyn WasmComponentV3Actor>, LixError>;
}

#[async_trait]
pub trait WasmComponentV3Actor: Send {
    async fn open_file(
        &mut self,
        limits: WasmV3TransitionLimits,
        input: WasmV3OpenFileInput,
    ) -> Result<WasmV3FileTransition, LixError>;

    async fn file_changed(
        &mut self,
        limits: WasmV3TransitionLimits,
        input: WasmV3FileUpdate,
    ) -> Result<WasmV3FileTransition, LixError>;

    async fn open_entities(
        &mut self,
        limits: WasmV3TransitionLimits,
        input: WasmV3OpenEntitiesInput,
    ) -> Result<WasmV3EntityTransition, LixError>;

    async fn entities_changed(
        &mut self,
        limits: WasmV3TransitionLimits,
        input: WasmV3EntityUpdate,
    ) -> Result<WasmV3EntityTransition, LixError>;

    async fn next_change_page(
        &mut self,
        transition: WasmV3TransitionHandle,
        cursor: WasmV3ChangeCursorHandle,
        max_bytes: u32,
    ) -> Result<Option<Vec<WasmV3EntityChange>>, LixError>;

    async fn next_edit_page(
        &mut self,
        transition: WasmV3TransitionHandle,
        cursor: WasmV3EditCursorHandle,
        max_bytes: u32,
    ) -> Result<Option<Vec<WasmV3ByteEdit>>, LixError>;

    async fn finish_transition(
        &mut self,
        transition: WasmV3TransitionHandle,
    ) -> Result<(Root, WasmV3TransitionCounters), LixError>;

    async fn abort_transition(
        &mut self,
        transition: WasmV3TransitionHandle,
    ) -> Result<(), LixError>;
}

/// Collision-free durable key encoding shared by the host's entity arena and
/// changed-entity notifications. Length framing avoids delimiter ambiguity.
pub fn entity_arena_key(schema_key: &str, entity_pk: &[String]) -> Result<Vec<u8>, LixError> {
    let mut output = Vec::new();
    append_key_part(&mut output, schema_key.as_bytes())?;
    for part in entity_pk {
        append_key_part(&mut output, part.as_bytes())?;
    }
    Ok(output)
}

fn append_key_part(output: &mut Vec<u8>, part: &[u8]) -> Result<(), LixError> {
    let len = u32::try_from(part.len())
        .map_err(|_| invalid_param("v3 entity key component exceeds u32"))?;
    output.extend_from_slice(&len.to_le_bytes());
    output.extend_from_slice(part);
    Ok(())
}

fn invalid_param(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INVALID_PARAM, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_arena_keys_are_unambiguous_and_stable() {
        let split = entity_arena_key("a", &["bc".to_owned()]).unwrap();
        let other_split = entity_arena_key("ab", &["c".to_owned()]).unwrap();
        assert_ne!(split, other_split);
        assert_eq!(split, vec![1, 0, 0, 0, b'a', 2, 0, 0, 0, b'b', b'c']);
    }

    #[test]
    fn transition_limits_reject_zero_and_inverted_values() {
        assert!(WasmV3TransitionLimits::default().validate().is_ok());
        assert!(
            WasmV3TransitionLimits {
                max_page_bytes: 2,
                max_total_bytes: 1,
                ..WasmV3TransitionLimits::default()
            }
            .validate()
            .is_err()
        );
    }
}
