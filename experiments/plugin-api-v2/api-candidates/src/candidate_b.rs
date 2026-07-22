//! Persistent immutable document with splice input and patch output.

use crate::{
    Change, ChangeSet, Checkpoint, EntitySource, FileEdits, IdAllocator, Result, Source, Splice,
};

pub struct OpenDocument<'a> {
    pub file: Source<'a>,
    pub entities: EntitySource<'a>,
    pub checkpoint: Option<&'a Checkpoint>,
}

pub struct FileUpdate<'a> {
    /// Lazy view of the accepted document bytes. The host owns this immutable
    /// version, so an optimized document need not retain a complete file copy.
    pub before: Source<'a>,
    /// All positions refer to the accepted document's bytes.
    pub edits: &'a [Splice],
    /// Lazy view of the complete result for a simple full-parse fallback.
    pub after: Source<'a>,
    pub ids: &'a mut IdAllocator,
}

pub struct EntityUpdate<'a> {
    /// Lazy view of the accepted rendered bytes. This lets an optimized
    /// renderer copy or inspect only the ranges surrounding its output edits.
    pub before: Source<'a>,
    /// Final merged, committed changes—not a client's unmerged proposal.
    pub changes: &'a [Change],
    /// Lazy full-state fallback. A warm incremental renderer need not iterate
    /// it; a simple implementation may do so and return `replace_all`.
    pub current_entities: EntitySource<'a>,
}

pub struct FileTransition<D> {
    pub document: D,
    pub changes: ChangeSet,
}

pub struct EntityTransition<D> {
    pub document: D,
    pub edits: FileEdits,
}

pub trait FilePlugin {
    type Document: Document;

    fn open(&self, input: OpenDocument<'_>) -> Result<Self::Document>;
}

pub trait Document: Sized + Send + Sync + 'static {
    fn file_changed(&self, input: FileUpdate<'_>) -> Result<FileTransition<Self>>;

    fn entities_changed(&self, input: EntityUpdate<'_>) -> Result<EntityTransition<Self>>;

    fn checkpoint(&self) -> Result<Option<Checkpoint>> {
        Ok(None)
    }
}
