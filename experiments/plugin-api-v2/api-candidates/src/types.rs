use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error(pub String);

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl StdError for Error {}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntityId(pub String);

impl From<&str> for EntityId {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entity {
    pub id: EntityId,
    pub schema: String,
    pub snapshot: String,
    pub metadata: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Change {
    pub id: EntityId,
    pub schema: String,
    /// `None` is an entity tombstone.
    pub snapshot: Option<String>,
    pub metadata: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChangeSet(pub Vec<Change>);

impl ChangeSet {
    pub fn one(change: Change) -> Self {
        Self(vec![change])
    }
}

/// A byte splice whose offset and deletion length refer to the same base
/// document as every other splice in the batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Splice {
    pub offset: u64,
    pub delete_len: u64,
    pub insert: Vec<u8>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FileEdits(pub Vec<Splice>);

impl FileEdits {
    pub fn replace_all(previous_len: usize, bytes: Vec<u8>) -> Self {
        Self(vec![Splice {
            offset: 0,
            delete_len: previous_len as u64,
            insert: bytes,
        }])
    }

    pub fn apply(&self, previous: &[u8]) -> Result<Vec<u8>> {
        apply_splices(previous, &self.0)
    }
}

pub fn apply_splices(previous: &[u8], edits: &[Splice]) -> Result<Vec<u8>> {
    let mut last_end = 0usize;
    let mut capacity = previous.len();
    for edit in edits {
        let start = usize::try_from(edit.offset)
            .map_err(|_| Error("splice offset does not fit this host".to_owned()))?;
        let delete_len = usize::try_from(edit.delete_len)
            .map_err(|_| Error("splice deletion length does not fit this host".to_owned()))?;
        let end = start
            .checked_add(delete_len)
            .ok_or_else(|| Error("splice deletion range overflowed".to_owned()))?;
        if start < last_end || end > previous.len() {
            return Err(Error(
                "splices must be sorted, non-overlapping, and in bounds".to_owned(),
            ));
        }
        capacity = capacity
            .checked_sub(delete_len)
            .and_then(|value| value.checked_add(edit.insert.len()))
            .ok_or_else(|| Error("splice result length overflowed".to_owned()))?;
        last_end = end;
    }

    let mut result = Vec::with_capacity(capacity);
    let mut cursor = 0usize;
    for edit in edits {
        let start = edit.offset as usize;
        let end = start + edit.delete_len as usize;
        result.extend_from_slice(&previous[cursor..start]);
        result.extend_from_slice(&edit.insert);
        cursor = end;
    }
    result.extend_from_slice(&previous[cursor..]);
    Ok(result)
}

/// Evaluation stand-in for an engine source capability. `read_all` is the
/// explicit simple-plugin fallback; optimized implementations use `read`.
#[derive(Clone, Copy, Debug)]
pub struct Source<'a> {
    bytes: &'a [u8],
}

impl<'a> Source<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    pub fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub fn read(&self, offset: u64, length: u64) -> Result<&'a [u8]> {
        let start = usize::try_from(offset)
            .map_err(|_| Error("source offset does not fit this host".to_owned()))?;
        let length = usize::try_from(length)
            .map_err(|_| Error("source length does not fit this host".to_owned()))?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| Error("source range overflowed".to_owned()))?;
        self.bytes
            .get(start..end)
            .ok_or_else(|| Error("source range is out of bounds".to_owned()))
    }

    pub fn read_all(&self) -> &'a [u8] {
        self.bytes
    }
}

#[derive(Clone, Copy, Debug)]
pub struct EntitySource<'a> {
    entities: &'a [Entity],
}

impl<'a> EntitySource<'a> {
    pub fn new(entities: &'a [Entity]) -> Self {
        Self { entities }
    }

    pub fn get(&self, id: &EntityId) -> Option<&'a Entity> {
        self.entities.iter().find(|entity| &entity.id == id)
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &'a Entity> {
        self.entities.iter()
    }
}

/// Retry-stable IDs for the evaluation. Production IDs are keyed by operation
/// identity and ordinal rather than exposing this string encoding.
#[derive(Clone, Debug)]
pub struct IdAllocator {
    operation: String,
    next: u64,
}

impl IdAllocator {
    pub fn new(operation: impl Into<String>) -> Self {
        Self {
            operation: operation.into(),
            next: 0,
        }
    }

    pub fn allocate(&mut self) -> EntityId {
        let id = EntityId(format!("{}:{}", self.operation, self.next));
        self.next += 1;
        id
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Checkpoint(pub Vec<u8>);

/// Host-owned, transactional private index used by Candidate D.
#[derive(Clone, Debug, Default)]
pub struct PrivateIndex {
    values: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl PrivateIndex {
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.values.get(key).map(Vec::as_slice)
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.values.insert(key, value);
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.values.remove(key);
    }

    pub fn scan_prefix(&self, prefix: &[u8]) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.values
            .iter()
            .filter(move |(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.as_slice(), value.as_slice()))
    }
}
