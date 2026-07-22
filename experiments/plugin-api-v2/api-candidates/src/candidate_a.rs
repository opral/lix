//! Persistent immutable document with complete blob input/output.

use crate::{Change, ChangeSet, EntitySource, IdAllocator, Result, Source};

pub struct OpenDocument<'a> {
    pub file: Source<'a>,
    pub entities: EntitySource<'a>,
}

pub struct FileUpdate<'a> {
    pub next_file: Source<'a>,
    pub ids: &'a mut IdAllocator,
}

pub struct EntityUpdate<'a> {
    pub changes: &'a [Change],
    pub current_entities: EntitySource<'a>,
}

pub struct FileTransition<D> {
    pub document: D,
    pub changes: ChangeSet,
}

pub struct EntityTransition<D> {
    pub document: D,
    pub complete_file: Vec<u8>,
}

pub trait FilePlugin {
    type Document: Document;

    fn open(&self, input: OpenDocument<'_>) -> Result<Self::Document>;
}

pub trait Document: Sized + Send + Sync + 'static {
    fn file_changed(&self, input: FileUpdate<'_>) -> Result<FileTransition<Self>>;

    fn entities_changed(&self, input: EntityUpdate<'_>) -> Result<EntityTransition<Self>>;
}
