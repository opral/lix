//! Candidate B after the format-complete AX evaluation.
//!
//! The first facade reused v1's optional `snapshot` and generic `metadata`
//! fields. One otherwise plausible CSV submission represented a reorder only
//! in local plugin state, so the committed entity delta could not reproduce
//! the requested file. This refinement makes a deletion a distinct variant
//! and makes every upsert carry the complete schema entity. Ordering therefore
//! lives in the schema snapshot (for example `order_key`), not in an optional
//! transport escape hatch.

use crate::{Checkpoint, FileEdits, IdAllocator, Result, Source, Splice};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntityKey {
    pub schema_key: String,
    pub entity_pk: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entity {
    pub key: EntityKey,
    /// The complete canonical snapshot for this schema. If order is semantic,
    /// its order key belongs here and is therefore committed with the upsert.
    pub snapshot_content: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ChangeEffect {
    #[default]
    Content,
    /// The snapshot changed only to preserve file formatting. This replaces
    /// the v1 `{"impact":"format"}` metadata convention with a typed hint.
    FormatOnly,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EntityChange {
    Upsert {
        entity: Entity,
        effect: ChangeEffect,
    },
    Delete(EntityKey),
}

impl EntityChange {
    pub fn upsert(entity: Entity) -> Self {
        Self::Upsert {
            entity,
            effect: ChangeEffect::Content,
        }
    }

    pub fn delete(key: EntityKey) -> Self {
        Self::Delete(key)
    }
}

/// Every change produced by one transition commits atomically.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EntityChanges(pub Vec<EntityChange>);

#[derive(Clone, Copy, Debug)]
pub struct EntitySource<'a> {
    entities: &'a [Entity],
}

impl<'a> EntitySource<'a> {
    pub fn new(entities: &'a [Entity]) -> Self {
        Self { entities }
    }

    pub fn get(&self, key: &EntityKey) -> Option<&'a Entity> {
        self.entities.iter().find(|entity| &entity.key == key)
    }

    /// Explicit full-state fallback. Optimized warm transitions do not call
    /// this method; they receive complete changed entities directly.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &'a Entity> {
        self.entities.iter()
    }
}

pub struct OpenDocument<'a> {
    pub file: Source<'a>,
    pub entities: EntitySource<'a>,
    pub checkpoint: Option<&'a Checkpoint>,
}

pub struct FileUpdate<'a> {
    pub before: Source<'a>,
    /// Sorted, non-overlapping, base-relative byte splices.
    pub edits: &'a [Splice],
    /// Lazy complete-result fallback for a simple plugin.
    pub after: Source<'a>,
    pub ids: &'a mut IdAllocator,
}

pub struct EntityUpdate<'a> {
    pub before: Source<'a>,
    /// Final merged and committed changes, never the client's proposal.
    pub changes: &'a [EntityChange],
    /// Lazy complete-state fallback for a simple renderer or cold recovery.
    pub current_entities: EntitySource<'a>,
}

pub struct FileTransition<D> {
    pub document: D,
    pub changes: EntityChanges,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deletion_cannot_be_confused_with_an_incomplete_upsert() {
        let key = EntityKey {
            schema_key: "csv_row".to_owned(),
            entity_pk: vec!["row-1".to_owned()],
        };
        let delete = EntityChange::delete(key.clone());
        let reorder = EntityChange::upsert(Entity {
            key,
            snapshot_content: r#"{"id":"row-1","order_key":"c0","cells":["a"]}"#.to_owned(),
        });

        assert!(matches!(delete, EntityChange::Delete(_)));
        let EntityChange::Upsert { entity, .. } = reorder else {
            panic!("reorder must be a complete upsert");
        };
        assert!(entity.snapshot_content.contains("order_key"));
    }
}
