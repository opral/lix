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

/// Warm calls may observe path/media-type changes only while the host-selected
/// plugin remains identical. Reselection is an engine migration/handoff, never
/// a call that asks one guest to turn into another plugin generation.
pub fn validate_warm_plugin_selection(
    before: &FileDescriptor,
    after: &FileDescriptor,
) -> Result<()> {
    if before.plugin != after.plugin {
        return Err(Error(
            "warm transitions require the same selected plugin and generation".to_owned(),
        ));
    }
    Ok(())
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChangePageLimits {
    pub max_groups: u32,
    pub max_encoded_bytes: u32,
}

/// Stateful bounded merge-group pages. Guest output uses this for initial or
/// broad detected changes; host input uses the same shape for broad
/// merge-resolved renderer changes. Every page is non-empty and contains only
/// complete groups; `None` is permanent EOF. The host uses
/// `ChangeDrainValidator` to enforce key uniqueness across guest-output pages.
pub trait ChangePageReader {
    fn limits(&self) -> ChangePageLimits;
    fn next_page(&mut self) -> Result<Option<EntityChanges>>;
}

pub enum EntityChangeOutput {
    Inline(EntityChanges),
    Paged(Box<dyn ChangePageReader>),
}

impl EntityChangeOutput {
    pub fn inline(changes: EntityChanges) -> Result<Self> {
        changes.validate()?;
        Ok(Self::Inline(changes))
    }

    pub fn paged(reader: Box<dyn ChangePageReader>) -> Self {
        Self::Paged(reader)
    }
}

/// Merge-resolved semantic input to `entities_changed`, backed by WIT's
/// stateful `packet-source`. A normal sparse render consumes one small page;
/// broad merges remain bounded instead of first becoming one guest `Vec`.
pub struct EntityChangeSource<'a> {
    reader: &'a mut dyn ChangePageReader,
}

impl<'a> EntityChangeSource<'a> {
    pub fn new(reader: &'a mut dyn ChangePageReader) -> Self {
        Self { reader }
    }

    pub fn limits(&self) -> ChangePageLimits {
        self.reader.limits()
    }

    pub fn next_page(&mut self) -> Result<Option<EntityChanges>> {
        self.reader.next_page()
    }
}

/// Host-side transition-wide validation state used while draining a guest
/// change cursor. Page-local validation is insufficient because the same key
/// must not appear in two different pages.
#[derive(Default)]
pub struct ChangeDrainValidator {
    seen: BTreeSet<EntityKey>,
    reached_eof: bool,
}

impl ChangeDrainValidator {
    pub fn accept_page(&mut self, page: &EntityChanges) -> Result<()> {
        if self.reached_eof {
            return Err(Error("a change cursor cannot advance after EOF".to_owned()));
        }
        if page.0.is_empty() {
            return Err(Error("a change cursor page must not be empty".to_owned()));
        }
        page.validate()?;
        for group in &page.0 {
            for change in &group.0 {
                if !self.seen.insert(change.key().clone()) {
                    return Err(Error(
                        "an entity key may occur only once across all change pages".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn accept_eof(&mut self) {
        self.reached_eof = true;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EntityPageLimits {
    pub max_encoded_bytes: u32,
}

pub struct EntityPage {
    /// A non-empty page bounded by `EntityPageReader::limits`.
    pub entities: Vec<Entity>,
}

/// Stateful host-backed entity access, matching WIT `packet-source.next`.
/// `next_page` returns `None` permanently at EOF; every successful page is
/// non-empty and advances. The generated SDK supplies the aggregate budget and
/// configured maximum encoded page size.
pub trait EntityPageReader {
    fn limits(&self) -> EntityPageLimits;
    fn next_page(&mut self) -> Result<Option<EntityPage>>;
}

pub struct EntitySource<'a> {
    reader: &'a mut dyn EntityPageReader,
}

impl<'a> EntitySource<'a> {
    pub fn new(reader: &'a mut dyn EntityPageReader) -> Self {
        Self { reader }
    }

    pub fn limits(&self) -> EntityPageLimits {
        self.reader.limits()
    }

    /// Full-state cold/fallback path. Warm renderers receive complete changed
    /// entities directly and normally never page through this source.
    pub fn next_page(&mut self) -> Result<Option<EntityPage>> {
        self.reader.next_page()
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InlineInputLimits {
    pub max_edits: u32,
    pub max_inline_bytes: u64,
}

/// Host pre-call validation. It runs before Canonical-ABI lowering, so an
/// attacker cannot make an unbounded `list<input-splice>` consume guest memory
/// before the transition budget is observed. Large replacement bytes use an
/// `AfterRange` and therefore do not count as inline bytes.
pub fn validate_input_splices(
    edits: &[InputSplice<'_>],
    before_len: u64,
    after_len: u64,
    limits: InlineInputLimits,
) -> Result<()> {
    if edits.len() > limits.max_edits as usize {
        return Err(Error(
            "input splice count exceeds the pre-call limit".to_owned(),
        ));
    }

    let mut previous_end = 0u64;
    let mut inline_bytes = 0u64;
    for edit in edits {
        let end = edit
            .offset
            .checked_add(edit.delete_len)
            .ok_or_else(|| Error("input splice deletion range overflowed".to_owned()))?;
        if edit.offset < previous_end || end > before_len {
            return Err(Error(
                "input splices must be sorted, non-overlapping, and in bounds".to_owned(),
            ));
        }
        match edit.insert {
            InputBytes::Inline(bytes) => {
                inline_bytes = inline_bytes
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| Error("inline input byte count overflowed".to_owned()))?;
            }
            InputBytes::AfterRange(range) => {
                let range_end = range
                    .offset
                    .checked_add(range.length)
                    .ok_or_else(|| Error("after-source range overflowed".to_owned()))?;
                if range_end > after_len {
                    return Err(Error("after-source range is out of bounds".to_owned()));
                }
            }
        }
        previous_end = end;
    }

    if inline_bytes > limits.max_inline_bytes {
        return Err(Error(
            "inline input bytes exceed the pre-call limit".to_owned(),
        ));
    }
    Ok(())
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

/// Host-side validation across all renderer edit pages. Coordinates in every
/// page refer to the same accepted base, so page-local checks are insufficient.
pub struct EditDrainValidator {
    base_len: u64,
    previous_end: u64,
    reached_eof: bool,
}

impl EditDrainValidator {
    pub fn new(base_len: u64) -> Self {
        Self {
            base_len,
            previous_end: 0,
            reached_eof: false,
        }
    }

    pub fn accept_page(&mut self, edits: &[OutputSplice]) -> Result<()> {
        if self.reached_eof {
            return Err(Error("an edit cursor cannot advance after EOF".to_owned()));
        }
        if edits.is_empty() {
            return Err(Error("an edit cursor page must not be empty".to_owned()));
        }
        for edit in edits {
            let end = edit
                .offset
                .checked_add(edit.delete_len)
                .ok_or_else(|| Error("output splice deletion range overflowed".to_owned()))?;
            if edit.offset < self.previous_end || end > self.base_len {
                return Err(Error(
                    "output splices must be globally sorted, non-overlapping, and in bounds"
                        .to_owned(),
                ));
            }
            self.previous_end = end;
        }
        Ok(())
    }

    pub fn accept_eof(&mut self) {
        self.reached_eof = true;
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
    /// plugin with different before/after descriptors and zero byte edits, but
    /// both descriptors must select the same plugin key/generation.
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
    /// Both descriptors select the same plugin key/generation. Reselection is
    /// handled as a cold migration outside this warm call.
    pub before_descriptor: &'a FileDescriptor,
    pub after_descriptor: &'a FileDescriptor,
    pub before: Source<'a>,
    /// Final merge-resolved changes as bounded stateful pages. The engine
    /// validates the prospective file before committing durable state.
    pub changes: EntityChangeSource<'a>,
    /// Transaction-local complete prospective state after applying `changes`
    /// and resolving the merge, but before commit. A simple renderer may use
    /// this instead of incrementally applying the change pages.
    pub current_entities: EntitySource<'a>,
}

pub struct FileTransition<D> {
    pub document: D,
    /// Usually inline for a warm localized edit. Initial import and other
    /// broad transitions can stream bounded merge-group pages.
    pub changes: EntityChangeOutput,
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

    #[test]
    fn rejects_duplicate_keys_across_change_pages_and_progress_after_eof() {
        let first = EntityChanges::singleton(EntityChange::delete(key("row-1")));
        let duplicate = EntityChanges::singleton(EntityChange::delete(key("row-1")));
        let next = EntityChanges::singleton(EntityChange::delete(key("row-2")));
        let mut validator = ChangeDrainValidator::default();

        validator.accept_page(&first).unwrap();
        assert!(validator.accept_page(&duplicate).is_err());
        validator.accept_eof();
        assert!(validator.accept_page(&next).is_err());
    }

    #[test]
    fn bounds_input_splices_before_guest_lowering() {
        let inserts = [1u8, 2u8];
        let edits = [
            InputSplice {
                offset: 0,
                delete_len: 1,
                insert: InputBytes::Inline(&inserts[..1]),
            },
            InputSplice {
                offset: 2,
                delete_len: 1,
                insert: InputBytes::Inline(&inserts[1..]),
            },
        ];

        assert!(
            validate_input_splices(
                &edits,
                3,
                3,
                InlineInputLimits {
                    max_edits: 2,
                    max_inline_bytes: 2,
                },
            )
            .is_ok()
        );
        assert!(
            validate_input_splices(
                &edits,
                3,
                3,
                InlineInputLimits {
                    max_edits: 1,
                    max_inline_bytes: 2,
                },
            )
            .is_err()
        );
        assert!(
            validate_input_splices(
                &edits,
                3,
                3,
                InlineInputLimits {
                    max_edits: 2,
                    max_inline_bytes: 1,
                },
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_output_overlap_across_edit_pages_and_progress_after_eof() {
        let first = [OutputSplice {
            offset: 1,
            delete_len: 3,
            insert: OutputBytes::Inline(vec![1]),
        }];
        let overlapping = [OutputSplice {
            offset: 3,
            delete_len: 1,
            insert: OutputBytes::Inline(vec![2]),
        }];
        let later = [OutputSplice {
            offset: 5,
            delete_len: 1,
            insert: OutputBytes::Inline(vec![3]),
        }];
        let mut validator = EditDrainValidator::new(8);

        validator.accept_page(&first).unwrap();
        assert!(validator.accept_page(&overlapping).is_err());
        validator.accept_eof();
        assert!(validator.accept_page(&later).is_err());
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
        validate_warm_plugin_selection(&before, &after).unwrap();

        let reselected = FileDescriptor {
            path: after.path.clone(),
            media_type: after.media_type.clone(),
            plugin: PluginSelection {
                plugin_key: "csv".to_owned(),
                generation: "sha256:def".to_owned(),
            },
        };
        assert!(validate_warm_plugin_selection(&after, &reselected).is_err());

        let edits: [InputSplice<'_>; 0] = [];
        assert!(edits.is_empty());
    }
}
