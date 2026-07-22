//! Candidate B after the controlled AX evaluation and correctness review.
//!
//! The warm data flow is still the measured B2 design: immutable document,
//! byte splices in, resolved entity changes out, and the inverse transition for
//! rendering. The refinement makes cold bootstrap explicit, separates delete
//! from complete upsert, and represents coupled semantic facts as merge groups.

use std::collections::BTreeSet;

use crate::{Error, Result, Source};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PluginSelection {
    pub plugin_key: String,
    /// Content-addressed plugin generation selected by the host.
    pub generation: String,
}

/// File facts that can affect parsing even when the bytes do not change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileDescriptor {
    pub path: Option<String>,
    pub media_type: Option<String>,
    pub plugin: PluginSelection,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntityKey {
    pub schema_key: String,
    pub entity_pk: Vec<String>,
}

pub enum EntityContent {
    Inline(String),
    /// Host-backed for entity input and guest-backed for change output. The SDK
    /// presents one lazy byte abstraction; WIT lowers the two ownership
    /// directions to `byte-source` and `byte-output` attachments.
    Lazy(Box<dyn LazyBytes>),
}

pub struct Entity {
    pub key: EntityKey,
    /// The complete canonical snapshot for this schema. If order, parentage,
    /// or references are semantic, they live here and commit with the upsert.
    pub snapshot_content: EntityContent,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ChangeEffect {
    #[default]
    Content,
    /// Typed replacement for v1's `{"impact":"format"}` metadata convention.
    FormatOnly,
}

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

    pub fn key(&self) -> &EntityKey {
        match self {
            Self::Upsert { entity, .. } => &entity.key,
            Self::Delete(key) => key,
        }
    }
}

/// Changes in one group win or lose merge conflicts together. Most edits use
/// singleton groups. A two-sided Excalidraw binding or coupled Markdown table
/// fact can use one multi-entity group without coupling unrelated changes from
/// the same file write.
pub struct MergeGroup(pub Vec<EntityChange>);

#[derive(Default)]
pub struct EntityChanges(pub Vec<MergeGroup>);

impl EntityChanges {
    pub fn singleton(change: EntityChange) -> Self {
        Self(vec![MergeGroup(vec![change])])
    }

    pub fn validate(&self) -> Result<()> {
        let mut seen = BTreeSet::new();
        for group in &self.0 {
            if group.0.is_empty() {
                return Err(Error("entity merge groups must not be empty".to_owned()));
            }
            for change in &group.0 {
                if !seen.insert(change.key()) {
                    return Err(Error(
                        "an entity key may occur only once in one transition".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntityCursor(pub Vec<u8>);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EntityPageLimits {
    pub max_entities: u32,
    pub max_encoded_bytes: u32,
}

pub struct EntityPage {
    /// A non-empty page bounded by `EntityPageReader::limits`.
    pub entities: Vec<Entity>,
    /// Opaque resume token. It must differ from the token used for this page.
    pub resume_after: EntityCursor,
}

/// Host-backed entity access. `next_page` returns `None` at EOF, and every
/// successful page is non-empty and advances its opaque cursor.
pub trait EntityPageReader {
    fn limits(&self) -> EntityPageLimits;

    fn get(&self, key: &EntityKey) -> Result<Option<Entity>>;

    fn next_page(&self, after: Option<&EntityCursor>) -> Result<Option<EntityPage>>;
}

#[derive(Clone, Copy)]
pub struct EntitySource<'a> {
    reader: &'a dyn EntityPageReader,
}

impl<'a> EntitySource<'a> {
    pub fn new(reader: &'a dyn EntityPageReader) -> Self {
        Self { reader }
    }

    pub fn limits(&self) -> EntityPageLimits {
        self.reader.limits()
    }

    pub fn get(&self, key: &EntityKey) -> Result<Option<Entity>> {
        self.reader.get(key)
    }

    /// Full-state cold/fallback path. Warm renderers receive complete changed
    /// entities directly and normally never page through this source.
    pub fn next_page(&self, after: Option<&EntityCursor>) -> Result<Option<EntityPage>> {
        self.reader.next_page(after)
    }
}

/// The host binds this capability to an operation identity, file incarnation,
/// and plugin generation. The same `(schema_key, scope, ordinal)` request must
/// return the same non-empty component on retries; distinct requests must not
/// collide.
pub trait StableIdAllocation {
    fn allocate_component(
        &self,
        schema_key: &str,
        scope: &[String],
        ordinal: u64,
    ) -> Result<String>;
}

/// Retry-stable composite primary keys. `scope` is copied verbatim into the
/// primary-key prefix and one host-generated component is appended. Therefore
/// every result has exactly `scope.len() + 1` components. Authors choose a
/// deterministic ordinal within each schema/scope, not call order.
pub struct IdAllocator<'a> {
    inner: &'a dyn StableIdAllocation,
}

impl<'a> IdAllocator<'a> {
    pub fn new(inner: &'a dyn StableIdAllocation) -> Self {
        Self { inner }
    }

    pub fn allocate(
        &self,
        schema_key: &str,
        scope: &[String],
        ordinal: u64,
    ) -> Result<Vec<String>> {
        if schema_key.is_empty() {
            return Err(Error("schema_key must not be empty".to_owned()));
        }
        let component = self.inner.allocate_component(schema_key, scope, ordinal)?;
        if component.is_empty() {
            return Err(Error("allocated ID component must not be empty".to_owned()));
        }
        let mut entity_pk = scope.to_vec();
        entity_pk.push(component);
        Ok(entity_pk)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SourceRange {
    pub offset: u64,
    pub length: u64,
}

/// Large inserted bytes can refer to the lazy `after` source instead of being
/// copied into every splice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InputBytes<'a> {
    Inline(&'a [u8]),
    AfterRange(SourceRange),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InputSplice<'a> {
    pub offset: u64,
    pub delete_len: u64,
    pub insert: InputBytes<'a>,
}

/// Lazy random-access bytes. Reads are bounded by the transition's page/byte
/// budget and must make progress before EOF.
pub trait LazyBytes: 'static {
    fn len(&self) -> u64;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn read(&self, offset: u64, max_bytes: u32) -> Result<Vec<u8>>;
}

pub enum OutputBytes {
    Inline(Vec<u8>),
    Lazy(Box<dyn LazyBytes>),
}

pub struct OutputSplice {
    pub offset: u64,
    pub delete_len: u64,
    pub insert: OutputBytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EditPageLimits {
    pub max_edits: u32,
    pub max_inline_bytes: u32,
}

/// Optional streaming form for very large patch sets. Every returned page is
/// non-empty and bounded by `limits`; `None` is permanent EOF.
pub trait EditPageReader: 'static {
    fn limits(&self) -> EditPageLimits;
    fn next_page(&mut self) -> Result<Option<Vec<OutputSplice>>>;
}

pub enum FileEdits {
    Inline(Vec<OutputSplice>),
    Paged(Box<dyn EditPageReader>),
}

impl FileEdits {
    pub fn replace_all_inline(previous_len: usize, bytes: Vec<u8>) -> Self {
        Self::Inline(vec![OutputSplice {
            offset: 0,
            delete_len: previous_len as u64,
            insert: OutputBytes::Inline(bytes),
        }])
    }

    pub fn replace_all_lazy(previous_len: usize, bytes: Box<dyn LazyBytes>) -> Self {
        Self::Inline(vec![OutputSplice {
            offset: 0,
            delete_len: previous_len as u64,
            insert: OutputBytes::Lazy(bytes),
        }])
    }
}

/// Initial import when bytes exist but durable semantic entities do not.
pub struct OpenFile<'a> {
    pub descriptor: &'a FileDescriptor,
    pub file: Source<'a>,
    pub ids: IdAllocator<'a>,
}

/// Cold restart/eviction when durable entities exist but plugin-backed file
/// bytes do not. This path may stream all entities; warm calls stay sparse.
pub struct OpenEntities<'a> {
    pub descriptor: &'a FileDescriptor,
    pub entities: EntitySource<'a>,
}

pub struct FileUpdate<'a> {
    /// Descriptor paired with `before`. A rename-only update still invokes the
    /// plugin with different before/after descriptors and zero byte edits.
    pub before_descriptor: &'a FileDescriptor,
    pub after_descriptor: &'a FileDescriptor,
    /// Exact accepted bytes selected by the session/path-bound observation
    /// handle. The base hash validates these bytes; it does not select an
    /// identity root by itself.
    pub before: Source<'a>,
    /// Sorted, non-overlapping, base-relative byte splices.
    pub edits: &'a [InputSplice<'a>],
    /// Lazy complete-result fallback for a simple plugin.
    pub after: Source<'a>,
    pub ids: IdAllocator<'a>,
}

pub struct EntityUpdate<'a> {
    pub before_descriptor: &'a FileDescriptor,
    pub after_descriptor: &'a FileDescriptor,
    pub before: Source<'a>,
    /// Final merge-resolved changes. The engine invokes this transition and
    /// validates the prospective file before committing durable state.
    pub changes: &'a EntityChanges,
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

    /// Parse a newly imported file and create its first semantic state.
    fn open_file(&self, input: OpenFile<'_>) -> Result<FileTransition<Self::Document>>;

    /// Render durable semantic state after a cold start or document eviction.
    /// Returned edits replace the empty base with the complete rendered file.
    fn open_entities(&self, input: OpenEntities<'_>) -> Result<EntityTransition<Self::Document>>;
}

/// The host serializes one file actor. Guest document values need not satisfy
/// host `Send`/`Sync` constraints; generated bindings implement WIT resources.
pub trait Document: Sized + 'static {
    fn file_changed(&self, input: FileUpdate<'_>) -> Result<FileTransition<Self>>;

    fn entities_changed(&self, input: EntityUpdate<'_>) -> Result<EntityTransition<Self>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(id: &str) -> EntityKey {
        EntityKey {
            schema_key: "csv_row".to_owned(),
            entity_pk: vec![id.to_owned()],
        }
    }

    #[test]
    fn deletion_cannot_be_confused_with_an_incomplete_upsert() {
        let delete = EntityChange::delete(key("row-1"));
        let reorder = EntityChange::upsert(Entity {
            key: key("row-2"),
            snapshot_content: EntityContent::Inline(
                r#"{"id":"row-2","order_key":"c0","cells":["a"]}"#.to_owned(),
            ),
        });

        assert!(matches!(delete, EntityChange::Delete(_)));
        let EntityChange::Upsert { entity, .. } = reorder else {
            panic!("reorder must be a complete upsert");
        };
        assert!(matches!(
            entity.snapshot_content,
            EntityContent::Inline(content) if content.contains("order_key")
        ));
    }

    #[test]
    fn rejects_duplicate_keys_across_merge_groups() {
        let changes = EntityChanges(vec![
            MergeGroup(vec![EntityChange::delete(key("row-1"))]),
            MergeGroup(vec![EntityChange::delete(key("row-1"))]),
        ]);
        assert!(changes.validate().is_err());
    }

    struct DeterministicIds;

    impl StableIdAllocation for DeterministicIds {
        fn allocate_component(
            &self,
            schema_key: &str,
            scope: &[String],
            ordinal: u64,
        ) -> Result<String> {
            Ok(format!("{schema_key}:{}:{ordinal}", scope.join("/")))
        }
    }

    #[test]
    fn allocation_is_explicitly_scoped_and_retry_stable() {
        let ids = IdAllocator::new(&DeterministicIds);
        let scope = vec!["table-1".to_owned()];
        let first = ids.allocate("csv_row", &scope, 7).unwrap();
        let retry = ids.allocate("csv_row", &scope, 7).unwrap();

        assert_eq!(first, retry);
        assert_eq!(first.len(), scope.len() + 1);
        assert_eq!(&first[..scope.len()], scope.as_slice());
    }

    #[test]
    fn descriptor_changes_are_independent_of_byte_edits() {
        let plugin = PluginSelection {
            plugin_key: "csv".to_owned(),
            generation: "sha256:abc".to_owned(),
        };
        let before = FileDescriptor {
            path: Some("data.csv".to_owned()),
            media_type: Some("text/csv".to_owned()),
            plugin: plugin.clone(),
        };
        let after = FileDescriptor {
            path: Some("archive/data.csv".to_owned()),
            media_type: Some("text/csv".to_owned()),
            plugin,
        };

        assert_ne!(before, after);
        let edits: [InputSplice<'_>; 0] = [];
        assert!(edits.is_empty());
    }
}
