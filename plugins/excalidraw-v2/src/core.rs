use lix_order_key::OrderKey;
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

pub const SCENE_SCHEMA_KEY: &str = "excalidraw_scene";
pub const ELEMENT_SCHEMA_KEY: &str = "excalidraw_element";
pub const FILE_SCHEMA_KEY: &str = "excalidraw_file";

const SCENE_ID: &str = "scene";
const ELEMENTS_MARKER: &str = "\0lix-excalidraw-elements-v2\0";
const FILES_MARKER: &str = "\0lix-excalidraw-files-v2\0";
const MAX_JSON_DEPTH: usize = 512;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IdNamespace(pub [u8; 16]);

impl IdNamespace {
    pub fn from_halves(high: u64, low: u64) -> Self {
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&high.to_be_bytes());
        bytes[8..].copy_from_slice(&low.to_be_bytes());
        Self(bytes)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InputSplice<'a> {
    pub offset: u64,
    pub delete_len: u64,
    pub insert: &'a [u8],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChangeEffect {
    Content,
    FormatOnly,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntityChange {
    pub schema_key: String,
    pub entity_pk: Vec<String>,
    pub snapshot: Option<Vec<u8>>,
    pub effect: ChangeEffect,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntityRecord {
    pub schema_key: String,
    pub entity_pk: Vec<String>,
    pub snapshot: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ByteEdit {
    pub offset: u64,
    pub delete_len: u64,
    pub insert: Arc<Vec<u8>>,
}

type EntityKey = (String, Vec<String>);

#[derive(Clone, Debug, PartialEq, Eq)]
struct SceneEntity {
    template_json: String,
    elements_tail_json: String,
    files_tail_json: String,
    files_present: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ElementEntity {
    id: String,
    order_key: String,
    leading_json: String,
    element_type: String,
    is_deleted: bool,
    element_json: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FileEntity {
    id: String,
    order_key: String,
    prefix_json: String,
    file_json: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Span {
    offset: u64,
    length: u64,
}

#[derive(Clone, Debug)]
pub struct Document(Arc<DocumentInner>);

#[derive(Debug)]
struct DocumentInner {
    bytes: Arc<Vec<u8>>,
    scene: SceneEntity,
    elements: Arc<Vec<ElementEntity>>,
    files: Arc<Vec<FileEntity>>,
    element_spans: Arc<HashMap<String, Span>>,
    file_spans: Arc<HashMap<String, Span>>,
}

#[derive(Clone, Debug)]
pub struct InitialChanges {
    changes: VecDeque<EntityChange>,
}

impl Iterator for InitialChanges {
    type Item = Result<EntityChange, String>;

    fn next(&mut self) -> Option<Self::Item> {
        self.changes.pop_front().map(Ok)
    }
}

impl EntityChange {
    fn upsert(record: EntityRecord) -> Self {
        Self {
            schema_key: record.schema_key,
            entity_pk: record.entity_pk,
            snapshot: Some(record.snapshot),
            effect: ChangeEffect::Content,
        }
    }

    fn delete(schema_key: &str, id: &str) -> Self {
        Self {
            schema_key: schema_key.to_owned(),
            entity_pk: vec![id.to_owned()],
            snapshot: None,
            effect: ChangeEffect::Content,
        }
    }
}

impl SceneEntity {
    fn record(&self) -> Result<EntityRecord, String> {
        let snapshot = serde_json::to_vec(&json!({
            "id": SCENE_ID,
            "template_json": self.template_json,
            "elements_tail_json": self.elements_tail_json,
            "files_tail_json": self.files_tail_json,
            "files_present": self.files_present,
        }))
        .map_err(|error| format!("serialize Excalidraw scene snapshot: {error}"))?;
        Ok(EntityRecord {
            schema_key: SCENE_SCHEMA_KEY.to_owned(),
            entity_pk: vec![SCENE_ID.to_owned()],
            snapshot,
        })
    }

    fn parse(record: &EntityRecord) -> Result<Self, String> {
        require_key(record, SCENE_SCHEMA_KEY)?;
        if record.entity_pk.as_slice() != [SCENE_ID] {
            return Err("excalidraw_scene requires the single primary key \"scene\"".to_owned());
        }
        let object = snapshot_object(record)?;
        require_fields(
            &object,
            &[
                "id",
                "template_json",
                "elements_tail_json",
                "files_tail_json",
                "files_present",
            ],
        )?;
        if required_string(&object, "id")? != SCENE_ID {
            return Err("excalidraw_scene snapshot id must be \"scene\"".to_owned());
        }
        let files_present = required_bool(&object, "files_present")?;
        let scene = Self {
            template_json: required_string(&object, "template_json")?.to_owned(),
            elements_tail_json: required_string(&object, "elements_tail_json")?.to_owned(),
            files_tail_json: required_string(&object, "files_tail_json")?.to_owned(),
            files_present,
        };
        scene.validate_template()?;
        Ok(scene)
    }

    fn validate_template(&self) -> Result<(), String> {
        require_marker_count(&self.template_json, ELEMENTS_MARKER, 1, "elements")?;
        require_marker_count(
            &self.template_json,
            FILES_MARKER,
            usize::from(self.files_present),
            "files",
        )?;
        if !is_json_whitespace(&self.elements_tail_json) {
            return Err("elements_tail_json must contain only JSON whitespace".to_owned());
        }
        if !is_json_whitespace(&self.files_tail_json) {
            return Err("files_tail_json must contain only JSON whitespace".to_owned());
        }
        Ok(())
    }
}

impl ElementEntity {
    fn from_source(
        order_key: String,
        leading_json: String,
        element_json: String,
    ) -> Result<Self, String> {
        validate_order_key(&order_key)?;
        if !is_json_whitespace(&leading_json) {
            return Err("element leading_json must contain only JSON whitespace".to_owned());
        }
        let value: Value = serde_json::from_str(&element_json)
            .map_err(|error| format!("invalid Excalidraw element JSON: {error}"))?;
        let object = value
            .as_object()
            .ok_or_else(|| "Excalidraw elements must be JSON objects".to_owned())?;
        let id = required_string(object, "id")?.to_owned();
        if id.is_empty() {
            return Err("Excalidraw element id must not be empty".to_owned());
        }
        let element_type = required_string(object, "type")?.to_owned();
        if element_type.is_empty() {
            return Err("Excalidraw element type must not be empty".to_owned());
        }
        let is_deleted = match object.get("isDeleted") {
            None => false,
            Some(Value::Bool(value)) => *value,
            Some(_) => return Err("Excalidraw element isDeleted must be a boolean".to_owned()),
        };
        Ok(Self {
            id,
            order_key,
            leading_json,
            element_type,
            is_deleted,
            element_json,
        })
    }

    fn record(&self) -> Result<EntityRecord, String> {
        let snapshot = serde_json::to_vec(&json!({
            "id": self.id,
            "order_key": self.order_key,
            "leading_json": self.leading_json,
            "element_type": self.element_type,
            "is_deleted": self.is_deleted,
            "element_json": self.element_json,
        }))
        .map_err(|error| format!("serialize Excalidraw element snapshot: {error}"))?;
        Ok(EntityRecord {
            schema_key: ELEMENT_SCHEMA_KEY.to_owned(),
            entity_pk: vec![self.id.clone()],
            snapshot,
        })
    }

    fn parse(record: &EntityRecord) -> Result<Self, String> {
        require_key(record, ELEMENT_SCHEMA_KEY)?;
        let [id] = record.entity_pk.as_slice() else {
            return Err("excalidraw_element requires one primary-key component".to_owned());
        };
        let object = snapshot_object(record)?;
        require_fields(
            &object,
            &[
                "id",
                "order_key",
                "leading_json",
                "element_type",
                "is_deleted",
                "element_json",
            ],
        )?;
        if required_string(&object, "id")? != id {
            return Err("excalidraw_element snapshot id does not match its key".to_owned());
        }
        let declared_type = required_string(&object, "element_type")?;
        let declared_deleted = required_bool(&object, "is_deleted")?;
        let entity = Self::from_source(
            required_string(&object, "order_key")?.to_owned(),
            required_string(&object, "leading_json")?.to_owned(),
            required_string(&object, "element_json")?.to_owned(),
        )?;
        if entity.id != *id {
            return Err("element_json id does not match the entity key".to_owned());
        }
        if entity.element_type != declared_type {
            return Err("element_type does not match element_json".to_owned());
        }
        if entity.is_deleted != declared_deleted {
            return Err("is_deleted does not match element_json".to_owned());
        }
        Ok(entity)
    }
}

impl FileEntity {
    fn from_source(
        id: String,
        order_key: String,
        prefix_json: String,
        file_json: String,
    ) -> Result<Self, String> {
        if id.is_empty() {
            return Err("Excalidraw file id must not be empty".to_owned());
        }
        validate_order_key(&order_key)?;
        let prefix_id = parse_file_prefix(&prefix_json)?;
        if prefix_id != id {
            return Err("Excalidraw file prefix key does not match its entity id".to_owned());
        }
        let file_value: Value = serde_json::from_str(&file_json)
            .map_err(|error| format!("invalid Excalidraw file JSON: {error}"))?;
        if !file_value.is_object() {
            return Err("Excalidraw file payload must be a JSON object".to_owned());
        }
        Ok(Self {
            id,
            order_key,
            prefix_json,
            file_json,
        })
    }

    fn record(&self) -> Result<EntityRecord, String> {
        let snapshot = serde_json::to_vec(&json!({
            "id": self.id,
            "order_key": self.order_key,
            "prefix_json": self.prefix_json,
            "file_json": self.file_json,
        }))
        .map_err(|error| format!("serialize Excalidraw file snapshot: {error}"))?;
        Ok(EntityRecord {
            schema_key: FILE_SCHEMA_KEY.to_owned(),
            entity_pk: vec![self.id.clone()],
            snapshot,
        })
    }

    fn parse(record: &EntityRecord) -> Result<Self, String> {
        require_key(record, FILE_SCHEMA_KEY)?;
        let [id] = record.entity_pk.as_slice() else {
            return Err("excalidraw_file requires one primary-key component".to_owned());
        };
        let object = snapshot_object(record)?;
        require_fields(&object, &["id", "order_key", "prefix_json", "file_json"])?;
        if required_string(&object, "id")? != id {
            return Err("excalidraw_file snapshot id does not match its key".to_owned());
        }
        Self::from_source(
            id.clone(),
            required_string(&object, "order_key")?.to_owned(),
            required_string(&object, "prefix_json")?.to_owned(),
            required_string(&object, "file_json")?.to_owned(),
        )
    }
}

impl Document {
    pub fn open_file(
        bytes: Vec<u8>,
        _path: Option<&str>,
        _namespace: IdNamespace,
    ) -> Result<(Self, InitialChanges), String> {
        let parsed = parse_file(&bytes)?;
        let document = Self::from_entities(parsed.scene, parsed.elements, parsed.files)?;
        if document.0.bytes.as_slice() != bytes {
            return Err("Excalidraw source layout did not round-trip exactly".to_owned());
        }
        let changes = document.initial_changes();
        Ok((document, changes))
    }

    fn from_entities(
        scene: SceneEntity,
        mut elements: Vec<ElementEntity>,
        mut files: Vec<FileEntity>,
    ) -> Result<Self, String> {
        scene.validate_template()?;
        sort_and_validate_elements(&mut elements)?;
        sort_and_validate_files(&mut files)?;
        if !scene.files_present && !files.is_empty() {
            return Err("file entities require a files marker in the scene".to_owned());
        }
        let rendered = render_document(&scene, &elements, &files)?;
        validate_rendered_graph(&rendered.bytes, &elements, &files, scene.files_present)?;
        Ok(Self(Arc::new(DocumentInner {
            bytes: Arc::new(rendered.bytes),
            scene,
            elements: Arc::new(elements),
            files: Arc::new(files),
            element_spans: Arc::new(rendered.element_spans),
            file_spans: Arc::new(rendered.file_spans),
        })))
    }

    pub fn fork(&self) -> Self {
        self.clone()
    }

    pub fn bytes(&self) -> Vec<u8> {
        self.0.bytes.as_ref().clone()
    }

    pub fn initial_changes(&self) -> InitialChanges {
        let mut changes = VecDeque::with_capacity(1 + self.0.elements.len() + self.0.files.len());
        changes.push_back(EntityChange::upsert(
            self.0.scene.record().expect("validated scene serializes"),
        ));
        for element in self.0.elements.iter() {
            changes.push_back(EntityChange::upsert(
                element.record().expect("validated element serializes"),
            ));
        }
        for file in self.0.files.iter() {
            changes.push_back(EntityChange::upsert(
                file.record().expect("validated file serializes"),
            ));
        }
        InitialChanges { changes }
    }

    pub fn file_changed(
        &self,
        splices: &[InputSplice<'_>],
        _namespace: IdNamespace,
    ) -> Result<(Self, Vec<EntityChange>), String> {
        if splices.is_empty() {
            return Ok((self.clone(), Vec::new()));
        }
        let bytes = apply_splices(&self.0.bytes, splices)?;
        let mut parsed = parse_file(&bytes)?;
        reconcile_order_keys(&self.0.elements, &mut parsed.elements)?;
        reconcile_order_keys(&self.0.files, &mut parsed.files)?;
        let after = Self::from_entities(parsed.scene, parsed.elements, parsed.files)?;
        if after.0.bytes.as_slice() != bytes {
            return Err("changed Excalidraw source layout did not round-trip exactly".to_owned());
        }
        let changes = diff_records(self.records()?, after.records()?);
        Ok((after, changes))
    }

    pub fn entities_changed(
        &self,
        changes: &[EntityChange],
    ) -> Result<(Self, Vec<ByteEdit>), String> {
        if changes.is_empty() {
            return Ok((self.clone(), Vec::new()));
        }
        if changes.len() == 1
            && let Some(result) = self.single_entity_changed(&changes[0])?
        {
            return Ok(result);
        }

        let mut records = self
            .records()?
            .into_iter()
            .map(|record| (record_key(&record), record))
            .collect::<HashMap<_, _>>();
        for change in changes {
            validate_change_key(change)?;
            let key = (change.schema_key.clone(), change.entity_pk.clone());
            if let Some(snapshot) = &change.snapshot {
                records.insert(
                    key,
                    EntityRecord {
                        schema_key: change.schema_key.clone(),
                        entity_pk: change.entity_pk.clone(),
                        snapshot: snapshot.clone(),
                    },
                );
            } else {
                records.remove(&key);
            }
        }
        let (after, _) = Self::open_entities(records.into_values().collect())?;
        if after.0.bytes == self.0.bytes {
            return Ok((after, Vec::new()));
        }
        Ok((
            after.clone(),
            vec![ByteEdit {
                offset: 0,
                delete_len: u64::try_from(self.0.bytes.len())
                    .map_err(|_| "Excalidraw file length exceeds u64".to_owned())?,
                insert: Arc::clone(&after.0.bytes),
            }],
        ))
    }

    fn single_entity_changed(
        &self,
        change: &EntityChange,
    ) -> Result<Option<(Self, Vec<ByteEdit>)>, String> {
        let Some(snapshot) = &change.snapshot else {
            return Ok(None);
        };
        if change.entity_pk.len() != 1 {
            validate_change_key(change)?;
            return Ok(None);
        }
        let record = EntityRecord {
            schema_key: change.schema_key.clone(),
            entity_pk: change.entity_pk.clone(),
            snapshot: snapshot.clone(),
        };
        match change.schema_key.as_str() {
            ELEMENT_SCHEMA_KEY => {
                let replacement = ElementEntity::parse(&record)?;
                let Some(index) = self
                    .0
                    .elements
                    .iter()
                    .position(|element| element.id == replacement.id)
                else {
                    return Ok(None);
                };
                let before = &self.0.elements[index];
                if before == &replacement {
                    return Ok(Some((self.clone(), Vec::new())));
                }
                if before.order_key != replacement.order_key
                    || before.leading_json != replacement.leading_json
                {
                    return Ok(None);
                }
                let mut elements = self.0.elements.as_ref().clone();
                elements[index] = replacement.clone();
                let after = Self::from_entities(
                    self.0.scene.clone(),
                    elements,
                    self.0.files.as_ref().clone(),
                )?;
                let span = *self
                    .0
                    .element_spans
                    .get(&replacement.id)
                    .ok_or_else(|| "element source span is missing".to_owned())?;
                let edits = localized_edit(
                    &self.0.bytes,
                    &after.0.bytes,
                    span,
                    replacement.element_json.as_bytes(),
                )?;
                Ok(Some((after, edits)))
            }
            FILE_SCHEMA_KEY => {
                let replacement = FileEntity::parse(&record)?;
                let Some(index) = self
                    .0
                    .files
                    .iter()
                    .position(|file| file.id == replacement.id)
                else {
                    return Ok(None);
                };
                let before = &self.0.files[index];
                if before == &replacement {
                    return Ok(Some((self.clone(), Vec::new())));
                }
                if before.order_key != replacement.order_key
                    || before.prefix_json != replacement.prefix_json
                {
                    return Ok(None);
                }
                let mut files = self.0.files.as_ref().clone();
                files[index] = replacement.clone();
                let after = Self::from_entities(
                    self.0.scene.clone(),
                    self.0.elements.as_ref().clone(),
                    files,
                )?;
                let span = *self
                    .0
                    .file_spans
                    .get(&replacement.id)
                    .ok_or_else(|| "file source span is missing".to_owned())?;
                let edits = localized_edit(
                    &self.0.bytes,
                    &after.0.bytes,
                    span,
                    replacement.file_json.as_bytes(),
                )?;
                Ok(Some((after, edits)))
            }
            SCENE_SCHEMA_KEY => {
                SceneEntity::parse(&record)?;
                Ok(None)
            }
            other => Err(format!("unsupported Excalidraw entity schema {other:?}")),
        }
    }

    pub fn open_entities(entities: Vec<EntityRecord>) -> Result<(Self, ByteEdit), String> {
        let mut builder = EntityImportBuilder::new();
        for entity in entities {
            builder.push(entity)?;
        }
        builder.finish()
    }

    fn records(&self) -> Result<Vec<EntityRecord>, String> {
        let mut records = Vec::with_capacity(1 + self.0.elements.len() + self.0.files.len());
        records.push(self.0.scene.record()?);
        records.extend(
            self.0
                .elements
                .iter()
                .map(ElementEntity::record)
                .collect::<Result<Vec<_>, _>>()?,
        );
        records.extend(
            self.0
                .files
                .iter()
                .map(FileEntity::record)
                .collect::<Result<Vec<_>, _>>()?,
        );
        Ok(records)
    }
}

fn localized_edit(
    before: &[u8],
    after: &[u8],
    span: Span,
    insert: &[u8],
) -> Result<Vec<ByteEdit>, String> {
    let start = usize::try_from(span.offset).map_err(|_| "edit offset exceeds usize".to_owned())?;
    let length =
        usize::try_from(span.length).map_err(|_| "edit length exceeds usize".to_owned())?;
    let end = start
        .checked_add(length)
        .ok_or_else(|| "edit range overflow".to_owned())?;
    if end > before.len() {
        return Err("edit range exceeds the accepted file".to_owned());
    }
    let mut applied = Vec::with_capacity(before.len() - length + insert.len());
    applied.extend_from_slice(&before[..start]);
    applied.extend_from_slice(insert);
    applied.extend_from_slice(&before[end..]);
    if applied != after {
        return Err(
            "localized entity edit does not reproduce rendered Excalidraw bytes".to_owned(),
        );
    }
    Ok(vec![ByteEdit {
        offset: span.offset,
        delete_len: span.length,
        insert: Arc::new(insert.to_vec()),
    }])
}

#[derive(Debug, Default)]
pub struct EntityImportBuilder {
    scene: Option<SceneEntity>,
    elements: Vec<ElementEntity>,
    files: Vec<FileEntity>,
    identities: HashSet<EntityKey>,
}

impl EntityImportBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, record: EntityRecord) -> Result<(), String> {
        let key = record_key(&record);
        if !self.identities.insert(key.clone()) {
            return Err(format!("duplicate Excalidraw entity {key:?}"));
        }
        match record.schema_key.as_str() {
            SCENE_SCHEMA_KEY => {
                let scene = SceneEntity::parse(&record)?;
                if self.scene.replace(scene).is_some() {
                    return Err("Excalidraw graph contains multiple scene roots".to_owned());
                }
            }
            ELEMENT_SCHEMA_KEY => self.elements.push(ElementEntity::parse(&record)?),
            FILE_SCHEMA_KEY => self.files.push(FileEntity::parse(&record)?),
            other => return Err(format!("unsupported Excalidraw entity schema {other:?}")),
        }
        Ok(())
    }

    pub fn finish(self) -> Result<(Document, ByteEdit), String> {
        let scene = self
            .scene
            .ok_or_else(|| "Excalidraw entity graph requires one scene root".to_owned())?;
        let document = Document::from_entities(scene, self.elements, self.files)?;
        Ok((
            document.clone(),
            ByteEdit {
                offset: 0,
                delete_len: 0,
                insert: Arc::clone(&document.0.bytes),
            },
        ))
    }
}

#[derive(Debug)]
struct ParsedFile {
    scene: SceneEntity,
    elements: Vec<ElementEntity>,
    files: Vec<FileEntity>,
}

#[derive(Clone, Debug)]
struct RawEntry {
    id: Option<String>,
    prefix: String,
    raw: String,
}

#[derive(Debug)]
struct RawCollection {
    entries: Vec<RawEntry>,
    tail: String,
    content_start: usize,
    content_end: usize,
}

#[derive(Clone, Debug)]
struct RootField {
    key: String,
    value_start: usize,
    value_end: usize,
}

fn parse_file(bytes: &[u8]) -> Result<ParsedFile, String> {
    let source =
        std::str::from_utf8(bytes).map_err(|error| format!("Excalidraw must be UTF-8: {error}"))?;
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|error| format!("invalid Excalidraw JSON: {error}"))?;
    if !value.is_object() {
        return Err("Excalidraw document root must be a JSON object".to_owned());
    }
    let fields = scan_root_fields(bytes)?;
    let elements_field = unique_field(&fields, "elements")?
        .ok_or_else(|| "Excalidraw document requires a top-level elements array".to_owned())?;
    let elements_raw =
        scan_array_collection(bytes, elements_field.value_start, elements_field.value_end)?;

    let files_field = unique_field(&fields, "files")?;
    let files_raw = files_field
        .map(|field| scan_object_collection(bytes, field.value_start, field.value_end))
        .transpose()?;

    let mut replacements = vec![(
        elements_raw.content_start,
        elements_raw.content_end,
        ELEMENTS_MARKER,
    )];
    if let Some(files) = &files_raw {
        replacements.push((files.content_start, files.content_end, FILES_MARKER));
    }
    replacements.sort_unstable_by_key(|replacement| replacement.0);
    let template_json = replace_ranges(source, &replacements)?;

    let element_keys = OrderKey::evenly_between(None, None, elements_raw.entries.len())?;
    let mut element_ids = HashSet::new();
    let elements = elements_raw
        .entries
        .into_iter()
        .zip(element_keys)
        .map(|(entry, key)| {
            let entity =
                ElementEntity::from_source(key.to_snapshot_string(), entry.prefix, entry.raw)?;
            if !element_ids.insert(entity.id.clone()) {
                return Err(format!("duplicate Excalidraw element id {:?}", entity.id));
            }
            Ok(entity)
        })
        .collect::<Result<Vec<_>, String>>()?;

    let files_tail_json = files_raw
        .as_ref()
        .map_or_else(String::new, |files| files.tail.clone());
    let file_count = files_raw.as_ref().map_or(0, |files| files.entries.len());
    let file_keys = OrderKey::evenly_between(None, None, file_count)?;
    let mut file_ids = HashSet::new();
    let files = files_raw
        .map_or_else(Vec::new, |files| files.entries)
        .into_iter()
        .zip(file_keys)
        .map(|(entry, key)| {
            let id = entry
                .id
                .ok_or_else(|| "files map entry is missing its decoded key".to_owned())?;
            if !file_ids.insert(id.clone()) {
                return Err(format!("duplicate Excalidraw file id {id:?}"));
            }
            FileEntity::from_source(id, key.to_snapshot_string(), entry.prefix, entry.raw)
        })
        .collect::<Result<Vec<_>, String>>()?;

    let scene = SceneEntity {
        template_json,
        elements_tail_json: elements_raw.tail,
        files_tail_json,
        files_present: files_field.is_some(),
    };
    scene.validate_template()?;
    Ok(ParsedFile {
        scene,
        elements,
        files,
    })
}

fn unique_field<'a>(fields: &'a [RootField], key: &str) -> Result<Option<&'a RootField>, String> {
    let mut found = None;
    for field in fields.iter().filter(|field| field.key == key) {
        if found.replace(field).is_some() {
            return Err(format!("duplicate Excalidraw top-level key {key:?}"));
        }
    }
    Ok(found)
}

fn scan_root_fields(bytes: &[u8]) -> Result<Vec<RootField>, String> {
    let mut scanner = Scanner::new(bytes);
    scanner.skip_whitespace();
    scanner.expect(b'{', "Excalidraw root object")?;
    let mut fields = Vec::new();
    let mut keys = HashSet::new();
    scanner.skip_whitespace();
    if scanner.peek() == Some(b'}') {
        scanner.cursor += 1;
    } else {
        loop {
            scanner.skip_whitespace();
            let key = scanner.string()?;
            if !keys.insert(key.clone()) {
                return Err(format!("duplicate Excalidraw top-level key {key:?}"));
            }
            scanner.skip_whitespace();
            scanner.expect(b':', "top-level object field")?;
            scanner.skip_whitespace();
            let value_start = scanner.cursor;
            scanner.value(0)?;
            let value_end = scanner.cursor;
            fields.push(RootField {
                key,
                value_start,
                value_end,
            });
            scanner.skip_whitespace();
            match scanner.peek() {
                Some(b',') => scanner.cursor += 1,
                Some(b'}') => {
                    scanner.cursor += 1;
                    break;
                }
                _ => {
                    return Err(format!(
                        "Excalidraw root requires ',' or '}}' at byte {}",
                        scanner.cursor
                    ));
                }
            }
        }
    }
    scanner.skip_whitespace();
    if scanner.cursor != bytes.len() {
        return Err("Excalidraw document has trailing bytes".to_owned());
    }
    Ok(fields)
}

fn scan_array_collection(bytes: &[u8], start: usize, end: usize) -> Result<RawCollection, String> {
    let mut scanner = Scanner::at(bytes, start);
    scanner.expect(b'[', "Excalidraw elements")?;
    let content_start = scanner.cursor;
    let mut segment_start = content_start;
    let mut entries = Vec::new();
    scanner.skip_whitespace();
    if scanner.peek() == Some(b']') {
        let content_end = scanner.cursor;
        scanner.cursor += 1;
        require_collection_end(scanner.cursor, end, "elements")?;
        return Ok(RawCollection {
            entries,
            tail: bytes_to_string(&bytes[content_start..content_end])?,
            content_start,
            content_end,
        });
    }
    loop {
        scanner.skip_whitespace();
        let value_start = scanner.cursor;
        scanner.value(0)?;
        let value_end = scanner.cursor;
        entries.push(RawEntry {
            id: None,
            prefix: bytes_to_string(&bytes[segment_start..value_start])?,
            raw: bytes_to_string(&bytes[value_start..value_end])?,
        });
        scanner.skip_whitespace();
        match scanner.peek() {
            Some(b',') => {
                scanner.cursor += 1;
                segment_start = scanner.cursor;
            }
            Some(b']') => {
                let content_end = scanner.cursor;
                scanner.cursor += 1;
                require_collection_end(scanner.cursor, end, "elements")?;
                return Ok(RawCollection {
                    entries,
                    tail: bytes_to_string(&bytes[value_end..content_end])?,
                    content_start,
                    content_end,
                });
            }
            _ => {
                return Err(format!(
                    "Excalidraw elements requires ',' or ']' at byte {}",
                    scanner.cursor
                ));
            }
        }
    }
}

fn scan_object_collection(bytes: &[u8], start: usize, end: usize) -> Result<RawCollection, String> {
    let mut scanner = Scanner::at(bytes, start);
    scanner.expect(b'{', "Excalidraw files")?;
    let content_start = scanner.cursor;
    let mut segment_start = content_start;
    let mut entries = Vec::new();
    let mut keys = HashSet::new();
    scanner.skip_whitespace();
    if scanner.peek() == Some(b'}') {
        let content_end = scanner.cursor;
        scanner.cursor += 1;
        require_collection_end(scanner.cursor, end, "files")?;
        return Ok(RawCollection {
            entries,
            tail: bytes_to_string(&bytes[content_start..content_end])?,
            content_start,
            content_end,
        });
    }
    loop {
        scanner.skip_whitespace();
        let id = scanner.string()?;
        if !keys.insert(id.clone()) {
            return Err(format!("duplicate Excalidraw file id {id:?}"));
        }
        scanner.skip_whitespace();
        scanner.expect(b':', "Excalidraw files entry")?;
        scanner.skip_whitespace();
        let value_start = scanner.cursor;
        scanner.value(0)?;
        let value_end = scanner.cursor;
        entries.push(RawEntry {
            id: Some(id),
            prefix: bytes_to_string(&bytes[segment_start..value_start])?,
            raw: bytes_to_string(&bytes[value_start..value_end])?,
        });
        scanner.skip_whitespace();
        match scanner.peek() {
            Some(b',') => {
                scanner.cursor += 1;
                segment_start = scanner.cursor;
            }
            Some(b'}') => {
                let content_end = scanner.cursor;
                scanner.cursor += 1;
                require_collection_end(scanner.cursor, end, "files")?;
                return Ok(RawCollection {
                    entries,
                    tail: bytes_to_string(&bytes[value_end..content_end])?,
                    content_start,
                    content_end,
                });
            }
            _ => {
                return Err(format!(
                    "Excalidraw files requires ',' or '}}' at byte {}",
                    scanner.cursor
                ));
            }
        }
    }
}

fn require_collection_end(actual: usize, expected: usize, name: &str) -> Result<(), String> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "Excalidraw {name} container range ended at {actual}, expected {expected}"
        ))
    }
}

fn replace_ranges(source: &str, replacements: &[(usize, usize, &str)]) -> Result<String, String> {
    let mut output = String::with_capacity(source.len());
    let mut cursor = 0usize;
    for &(start, end, replacement) in replacements {
        if start < cursor || end < start || end > source.len() {
            return Err("Excalidraw template replacement ranges overlap".to_owned());
        }
        output.push_str(
            source
                .get(cursor..start)
                .ok_or_else(|| "template range is not on a UTF-8 boundary".to_owned())?,
        );
        output.push_str(replacement);
        cursor = end;
    }
    output.push_str(
        source
            .get(cursor..)
            .ok_or_else(|| "template suffix is not on a UTF-8 boundary".to_owned())?,
    );
    Ok(output)
}

struct Scanner<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> Scanner<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    fn at(bytes: &'a [u8], cursor: usize) -> Self {
        Self { bytes, cursor }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.cursor).copied()
    }

    fn skip_whitespace(&mut self) {
        while self
            .peek()
            .is_some_and(|byte| matches!(byte, b' ' | b'\t' | b'\n' | b'\r'))
        {
            self.cursor += 1;
        }
    }

    fn expect(&mut self, byte: u8, context: &str) -> Result<(), String> {
        if self.peek() != Some(byte) {
            return Err(format!(
                "{context} expected {:?} at byte {}",
                char::from(byte),
                self.cursor
            ));
        }
        self.cursor += 1;
        Ok(())
    }

    fn string(&mut self) -> Result<String, String> {
        let start = self.cursor;
        if self.peek() != Some(b'"') {
            return Err(format!("expected JSON string at byte {start}"));
        }
        self.cursor += 1;
        let mut escaped = false;
        while let Some(byte) = self.peek() {
            self.cursor += 1;
            if escaped {
                escaped = false;
                continue;
            }
            match byte {
                b'\\' => escaped = true,
                b'"' => {
                    return serde_json::from_slice(&self.bytes[start..self.cursor])
                        .map_err(|error| format!("invalid JSON string at byte {start}: {error}"));
                }
                0x00..=0x1f => {
                    return Err(format!(
                        "unescaped control byte in JSON string at byte {}",
                        self.cursor - 1
                    ));
                }
                _ => {}
            }
        }
        Err(format!("unterminated JSON string at byte {start}"))
    }

    fn value(&mut self, depth: usize) -> Result<(), String> {
        if depth > MAX_JSON_DEPTH {
            return Err(format!(
                "Excalidraw JSON nesting exceeds {MAX_JSON_DEPTH} levels"
            ));
        }
        match self.peek() {
            Some(b'"') => {
                self.string()?;
                Ok(())
            }
            Some(b'{') => self.object(depth + 1),
            Some(b'[') => self.array(depth + 1),
            Some(_) => self.primitive(),
            None => Err("missing JSON value at end of file".to_owned()),
        }
    }

    fn object(&mut self, depth: usize) -> Result<(), String> {
        self.expect(b'{', "JSON object")?;
        self.skip_whitespace();
        if self.peek() == Some(b'}') {
            self.cursor += 1;
            return Ok(());
        }
        loop {
            self.skip_whitespace();
            self.string()?;
            self.skip_whitespace();
            self.expect(b':', "JSON object field")?;
            self.skip_whitespace();
            self.value(depth)?;
            self.skip_whitespace();
            match self.peek() {
                Some(b',') => self.cursor += 1,
                Some(b'}') => {
                    self.cursor += 1;
                    return Ok(());
                }
                _ => {
                    return Err(format!(
                        "JSON object requires ',' or '}}' at byte {}",
                        self.cursor
                    ));
                }
            }
        }
    }

    fn array(&mut self, depth: usize) -> Result<(), String> {
        self.expect(b'[', "JSON array")?;
        self.skip_whitespace();
        if self.peek() == Some(b']') {
            self.cursor += 1;
            return Ok(());
        }
        loop {
            self.skip_whitespace();
            self.value(depth)?;
            self.skip_whitespace();
            match self.peek() {
                Some(b',') => self.cursor += 1,
                Some(b']') => {
                    self.cursor += 1;
                    return Ok(());
                }
                _ => {
                    return Err(format!(
                        "JSON array requires ',' or ']' at byte {}",
                        self.cursor
                    ));
                }
            }
        }
    }

    fn primitive(&mut self) -> Result<(), String> {
        let start = self.cursor;
        while self
            .peek()
            .is_some_and(|byte| !matches!(byte, b' ' | b'\t' | b'\n' | b'\r' | b',' | b']' | b'}'))
        {
            self.cursor += 1;
        }
        if self.cursor == start {
            Err(format!("missing JSON primitive at byte {start}"))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
struct RenderedDocument {
    bytes: Vec<u8>,
    element_spans: HashMap<String, Span>,
    file_spans: HashMap<String, Span>,
}

fn render_document(
    scene: &SceneEntity,
    elements: &[ElementEntity],
    files: &[FileEntity],
) -> Result<RenderedDocument, String> {
    let template = scene.template_json.as_bytes();
    let mut markers = vec![(
        find_unique(template, ELEMENTS_MARKER.as_bytes(), "elements")?,
        Marker::Elements,
    )];
    if scene.files_present {
        markers.push((
            find_unique(template, FILES_MARKER.as_bytes(), "files")?,
            Marker::Files,
        ));
    }
    markers.sort_unstable_by_key(|marker| marker.0);
    let mut bytes = Vec::with_capacity(scene.template_json.len());
    let mut element_spans = HashMap::with_capacity(elements.len());
    let mut file_spans = HashMap::with_capacity(files.len());
    let mut cursor = 0usize;
    for (offset, marker) in markers {
        bytes.extend_from_slice(&template[cursor..offset]);
        match marker {
            Marker::Elements => render_elements(
                &mut bytes,
                elements,
                &scene.elements_tail_json,
                &mut element_spans,
            )?,
            Marker::Files => {
                render_files(&mut bytes, files, &scene.files_tail_json, &mut file_spans)?;
            }
        }
        cursor = offset + marker.bytes().len();
    }
    bytes.extend_from_slice(&template[cursor..]);
    Ok(RenderedDocument {
        bytes,
        element_spans,
        file_spans,
    })
}

#[derive(Clone, Copy, Debug)]
enum Marker {
    Elements,
    Files,
}

impl Marker {
    const fn bytes(self) -> &'static [u8] {
        match self {
            Self::Elements => ELEMENTS_MARKER.as_bytes(),
            Self::Files => FILES_MARKER.as_bytes(),
        }
    }
}

fn render_elements(
    output: &mut Vec<u8>,
    elements: &[ElementEntity],
    tail: &str,
    spans: &mut HashMap<String, Span>,
) -> Result<(), String> {
    for (index, element) in elements.iter().enumerate() {
        if index > 0 {
            output.push(b',');
        }
        output.extend_from_slice(element.leading_json.as_bytes());
        let offset = u64::try_from(output.len()).map_err(|_| "element offset exceeds u64")?;
        output.extend_from_slice(element.element_json.as_bytes());
        let length =
            u64::try_from(element.element_json.len()).map_err(|_| "element length exceeds u64")?;
        if spans
            .insert(element.id.clone(), Span { offset, length })
            .is_some()
        {
            return Err(format!("duplicate Excalidraw element id {:?}", element.id));
        }
    }
    output.extend_from_slice(tail.as_bytes());
    Ok(())
}

fn render_files(
    output: &mut Vec<u8>,
    files: &[FileEntity],
    tail: &str,
    spans: &mut HashMap<String, Span>,
) -> Result<(), String> {
    for (index, file) in files.iter().enumerate() {
        if index > 0 {
            output.push(b',');
        }
        output.extend_from_slice(file.prefix_json.as_bytes());
        let offset = u64::try_from(output.len()).map_err(|_| "file offset exceeds u64")?;
        output.extend_from_slice(file.file_json.as_bytes());
        let length = u64::try_from(file.file_json.len()).map_err(|_| "file length exceeds u64")?;
        if spans
            .insert(file.id.clone(), Span { offset, length })
            .is_some()
        {
            return Err(format!("duplicate Excalidraw file id {:?}", file.id));
        }
    }
    output.extend_from_slice(tail.as_bytes());
    Ok(())
}

fn find_unique(haystack: &[u8], needle: &[u8], name: &str) -> Result<usize, String> {
    let positions = haystack
        .windows(needle.len())
        .enumerate()
        .filter_map(|(offset, window)| (window == needle).then_some(offset))
        .collect::<Vec<_>>();
    match positions.as_slice() {
        [offset] => Ok(*offset),
        [] => Err(format!(
            "Excalidraw scene template is missing its {name} marker"
        )),
        _ => Err(format!(
            "Excalidraw scene template contains multiple {name} markers"
        )),
    }
}

trait OrderedEntity {
    fn id(&self) -> &str;
    fn order_key(&self) -> &str;
    fn set_order_key(&mut self, order_key: String);
}

impl OrderedEntity for ElementEntity {
    fn id(&self) -> &str {
        &self.id
    }

    fn order_key(&self) -> &str {
        &self.order_key
    }

    fn set_order_key(&mut self, order_key: String) {
        self.order_key = order_key;
    }
}

impl OrderedEntity for FileEntity {
    fn id(&self) -> &str {
        &self.id
    }

    fn order_key(&self) -> &str {
        &self.order_key
    }

    fn set_order_key(&mut self, order_key: String) {
        self.order_key = order_key;
    }
}

fn sort_and_validate_elements(elements: &mut [ElementEntity]) -> Result<(), String> {
    sort_and_validate_ordered(elements, "element")
}

fn sort_and_validate_files(files: &mut [FileEntity]) -> Result<(), String> {
    sort_and_validate_ordered(files, "file")
}

fn sort_and_validate_ordered<T: OrderedEntity>(
    entities: &mut [T],
    kind: &str,
) -> Result<(), String> {
    let mut ids = HashSet::with_capacity(entities.len());
    let mut keys = HashSet::with_capacity(entities.len());
    for entity in entities.iter() {
        if !ids.insert(entity.id().to_owned()) {
            return Err(format!("duplicate Excalidraw {kind} id {:?}", entity.id()));
        }
        validate_order_key(entity.order_key())?;
        if !keys.insert(entity.order_key().to_owned()) {
            return Err(format!(
                "duplicate Excalidraw {kind} order key {:?}",
                entity.order_key()
            ));
        }
    }
    entities.sort_unstable_by(|left, right| {
        (left.order_key(), left.id()).cmp(&(right.order_key(), right.id()))
    });
    Ok(())
}

fn reconcile_order_keys<T: OrderedEntity>(before: &[T], after: &mut [T]) -> Result<(), String> {
    if after.is_empty() {
        return Ok(());
    }
    let before_index = before
        .iter()
        .enumerate()
        .map(|(index, entity)| (entity.id(), index))
        .collect::<HashMap<_, _>>();
    let common_positions = after
        .iter()
        .filter_map(|entity| before_index.get(entity.id()).copied())
        .collect::<Vec<_>>();
    let relative_order_unchanged = common_positions.windows(2).all(|pair| pair[0] < pair[1]);
    if !relative_order_unchanged {
        assign_even_order_keys(after)?;
        return Ok(());
    }

    let old_keys = before
        .iter()
        .map(|entity| {
            Ok((
                entity.id().to_owned(),
                OrderKey::from_snapshot_string(entity.order_key())?,
            ))
        })
        .collect::<Result<HashMap<_, _>, String>>()?;
    for entity in after.iter_mut() {
        if let Some(key) = old_keys.get(entity.id()) {
            entity.set_order_key(key.to_snapshot_string());
        }
    }

    let mut cursor = 0usize;
    while cursor < after.len() {
        if old_keys.contains_key(after[cursor].id()) {
            cursor += 1;
            continue;
        }
        let start = cursor;
        while cursor < after.len() && !old_keys.contains_key(after[cursor].id()) {
            cursor += 1;
        }
        let previous = start
            .checked_sub(1)
            .and_then(|index| old_keys.get(after[index].id()));
        let next = after
            .get(cursor)
            .and_then(|entity| old_keys.get(entity.id()));
        let allocated = OrderKey::evenly_between(previous, next, cursor - start)?;
        for (entity, key) in after[start..cursor].iter_mut().zip(allocated) {
            entity.set_order_key(key.to_snapshot_string());
        }
    }
    Ok(())
}

fn assign_even_order_keys<T: OrderedEntity>(entities: &mut [T]) -> Result<(), String> {
    let keys = OrderKey::evenly_between(None, None, entities.len())?;
    for (entity, key) in entities.iter_mut().zip(keys) {
        entity.set_order_key(key.to_snapshot_string());
    }
    Ok(())
}

fn validate_rendered_graph(
    bytes: &[u8],
    elements: &[ElementEntity],
    files: &[FileEntity],
    files_present: bool,
) -> Result<(), String> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|error| format!("rendered Excalidraw JSON is invalid: {error}"))?;
    let object = value
        .as_object()
        .ok_or_else(|| "rendered Excalidraw root must be an object".to_owned())?;
    let rendered_elements = object
        .get("elements")
        .and_then(Value::as_array)
        .ok_or_else(|| "rendered Excalidraw elements must be an array".to_owned())?;
    let rendered_element_ids = rendered_elements
        .iter()
        .map(|value| {
            value
                .as_object()
                .ok_or_else(|| "rendered Excalidraw element must be an object".to_owned())
                .and_then(|value| required_string(value, "id").map(ToOwned::to_owned))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let expected_element_ids = elements
        .iter()
        .map(|entity| entity.id.clone())
        .collect::<Vec<_>>();
    if rendered_element_ids != expected_element_ids {
        return Err("rendered elements do not match the semantic element order".to_owned());
    }

    match object.get("files") {
        Some(Value::Object(rendered_files)) if files_present => {
            let rendered_file_ids = rendered_files.keys().cloned().collect::<HashSet<_>>();
            let expected_file_ids = files
                .iter()
                .map(|entity| entity.id.clone())
                .collect::<HashSet<_>>();
            if rendered_file_ids != expected_file_ids {
                return Err("rendered files do not match the semantic file identities".to_owned());
            }
        }
        Some(_) if files_present => {
            return Err("rendered Excalidraw files must be an object".to_owned());
        }
        None if !files_present && files.is_empty() => {}
        None if files_present => {
            return Err("rendered Excalidraw document is missing files".to_owned());
        }
        Some(_) => {
            return Err("files marker state does not match the rendered scene".to_owned());
        }
        None => {
            return Err("file entities require a rendered files object".to_owned());
        }
    }
    Ok(())
}

fn diff_records(before: Vec<EntityRecord>, after: Vec<EntityRecord>) -> Vec<EntityChange> {
    let before = before
        .into_iter()
        .map(|record| (record_key(&record), record.snapshot))
        .collect::<HashMap<_, _>>();
    let after = after
        .into_iter()
        .map(|record| (record_key(&record), record))
        .collect::<HashMap<_, _>>();
    let mut changes = Vec::new();
    for (schema_key, entity_pk) in before.keys() {
        if !after.contains_key(&(schema_key.clone(), entity_pk.clone())) {
            changes.push(EntityChange::delete(
                schema_key,
                entity_pk.first().expect("validated Excalidraw primary key"),
            ));
        }
    }
    for (key, record) in after {
        if before.get(&key) != Some(&record.snapshot) {
            changes.push(EntityChange::upsert(record));
        }
    }
    changes.sort_unstable_by(|left, right| {
        (&left.schema_key, &left.entity_pk).cmp(&(&right.schema_key, &right.entity_pk))
    });
    changes
}

fn apply_splices(before: &[u8], splices: &[InputSplice<'_>]) -> Result<Vec<u8>, String> {
    let mut output = Vec::new();
    let mut cursor = 0usize;
    for splice in splices {
        let start =
            usize::try_from(splice.offset).map_err(|_| "splice offset exceeds usize".to_owned())?;
        let delete_len = usize::try_from(splice.delete_len)
            .map_err(|_| "splice delete length exceeds usize".to_owned())?;
        let end = start
            .checked_add(delete_len)
            .ok_or_else(|| "splice end overflow".to_owned())?;
        if start < cursor {
            return Err("file splices must be sorted and non-overlapping".to_owned());
        }
        if end > before.len() {
            return Err("file splice exceeds the accepted Excalidraw bytes".to_owned());
        }
        output.extend_from_slice(&before[cursor..start]);
        output.extend_from_slice(splice.insert);
        cursor = end;
    }
    output.extend_from_slice(&before[cursor..]);
    Ok(output)
}

fn record_key(record: &EntityRecord) -> EntityKey {
    (record.schema_key.clone(), record.entity_pk.clone())
}

fn validate_change_key(change: &EntityChange) -> Result<(), String> {
    match (change.schema_key.as_str(), change.entity_pk.as_slice()) {
        (SCENE_SCHEMA_KEY, [id]) if id == SCENE_ID => Ok(()),
        (ELEMENT_SCHEMA_KEY | FILE_SCHEMA_KEY, [id]) if !id.is_empty() => Ok(()),
        (SCENE_SCHEMA_KEY | ELEMENT_SCHEMA_KEY | FILE_SCHEMA_KEY, _) => {
            Err("invalid Excalidraw entity primary key".to_owned())
        }
        (other, _) => Err(format!("unsupported Excalidraw entity schema {other:?}")),
    }
}

fn require_key(record: &EntityRecord, expected: &str) -> Result<(), String> {
    if record.schema_key == expected {
        Ok(())
    } else {
        Err(format!(
            "expected Excalidraw schema {expected:?}, got {:?}",
            record.schema_key
        ))
    }
}

fn snapshot_object(record: &EntityRecord) -> Result<Map<String, Value>, String> {
    let value: Value = serde_json::from_slice(&record.snapshot)
        .map_err(|error| format!("invalid Excalidraw entity snapshot: {error}"))?;
    reject_numbers(&value)?;
    let object = value
        .as_object()
        .ok_or_else(|| "Excalidraw entity snapshot must be an object".to_owned())?
        .clone();
    Ok(object)
}

fn reject_numbers(value: &Value) -> Result<(), String> {
    match value {
        Value::Number(_) => Err(
            "durable Excalidraw snapshots cannot contain JSON numbers; encode payload JSON as text"
                .to_owned(),
        ),
        Value::Array(values) => {
            for value in values {
                reject_numbers(value)?;
            }
            Ok(())
        }
        Value::Object(values) => {
            for value in values.values() {
                reject_numbers(value)?;
            }
            Ok(())
        }
        Value::Null | Value::Bool(_) | Value::String(_) => Ok(()),
    }
}

fn require_fields(object: &Map<String, Value>, required: &[&str]) -> Result<(), String> {
    let expected = required.iter().copied().collect::<HashSet<_>>();
    for field in required {
        if !object.contains_key(*field) {
            return Err(format!("Excalidraw snapshot is missing field {field:?}"));
        }
    }
    for field in object.keys() {
        if !expected.contains(field.as_str()) {
            return Err(format!(
                "Excalidraw snapshot contains unsupported field {field:?}"
            ));
        }
    }
    Ok(())
}

fn required_string<'a>(object: &'a Map<String, Value>, field: &str) -> Result<&'a str, String> {
    object
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("Excalidraw snapshot field {field:?} must be a string"))
}

fn required_bool(object: &Map<String, Value>, field: &str) -> Result<bool, String> {
    object
        .get(field)
        .and_then(Value::as_bool)
        .ok_or_else(|| format!("Excalidraw snapshot field {field:?} must be a boolean"))
}

fn validate_order_key(raw: &str) -> Result<(), String> {
    OrderKey::from_snapshot_string(raw)
        .map(|_| ())
        .map_err(|error| format!("invalid Excalidraw order key {raw:?}: {error}"))
}

fn parse_file_prefix(prefix: &str) -> Result<String, String> {
    let mut scanner = Scanner::new(prefix.as_bytes());
    scanner.skip_whitespace();
    let key = scanner.string()?;
    scanner.skip_whitespace();
    scanner.expect(b':', "Excalidraw file prefix")?;
    scanner.skip_whitespace();
    if scanner.cursor != prefix.len() {
        return Err("Excalidraw file prefix must end immediately before its value".to_owned());
    }
    Ok(key)
}

fn require_marker_count(
    source: &str,
    marker: &str,
    expected: usize,
    name: &str,
) -> Result<(), String> {
    let actual = source
        .as_bytes()
        .windows(marker.len())
        .filter(|window| *window == marker.as_bytes())
        .count();
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "Excalidraw scene template requires {expected} {name} marker(s), found {actual}"
        ))
    }
}

fn is_json_whitespace(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| matches!(byte, b' ' | b'\t' | b'\n' | b'\r'))
}

fn bytes_to_string(bytes: &[u8]) -> Result<String, String> {
    std::str::from_utf8(bytes)
        .map(ToOwned::to_owned)
        .map_err(|error| format!("Excalidraw source range is not UTF-8: {error}"))
}
