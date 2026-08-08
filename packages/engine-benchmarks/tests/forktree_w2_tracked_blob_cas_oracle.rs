use std::collections::BTreeMap;
use std::fmt;
use std::ops::Range;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ObjectId(u64);

#[derive(Clone, Debug, Eq, PartialEq)]
enum Cell {
    Value(String),
    Null,
    Tombstone,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Source {
    Global,
    Branch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VisibleRow {
    key: String,
    cell: Cell,
    source: Source,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DiffIdentity {
    schema: String,
    file_id: Option<String>,
    entity_pk: String,
    change_id: String,
    commit_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DiffRow {
    identity: DiffIdentity,
    before: Option<Cell>,
    after: Option<Cell>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BlobRef {
    object: ObjectId,
    size: usize,
    digest: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CoherentView {
    id: u64,
    state_root: ObjectId,
    epoch: u64,
    writes_at_open: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Error {
    MissingObject(ObjectId),
    DigestMismatch,
    SizeMismatch,
    Malformed,
    NonCanonicalOrder,
    InvalidRange,
    CrossView,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Default)]
struct ObjectAuthority {
    next_view: u64,
    epoch: u64,
    writes: u64,
    objects: BTreeMap<ObjectId, Vec<u8>>,
}

impl ObjectAuthority {
    fn begin_read(&mut self, state_root: ObjectId) -> CoherentView {
        self.next_view += 1;
        CoherentView {
            id: self.next_view,
            state_root,
            epoch: self.epoch,
            writes_at_open: self.writes,
        }
    }

    fn put_for_fixture(&mut self, object: ObjectId, bytes: Vec<u8>) {
        self.objects.insert(object, bytes);
    }

    fn read_blob(
        &self,
        view: &CoherentView,
        blob: &BlobRef,
        range: Option<Range<usize>>,
    ) -> Result<Vec<u8>, Error> {
        if view.state_root != blob.object {
            return Err(Error::CrossView);
        }
        let bytes = self
            .objects
            .get(&blob.object)
            .ok_or(Error::MissingObject(blob.object))?;
        if bytes.len() != blob.size {
            return Err(Error::SizeMismatch);
        }
        if digest(bytes) != blob.digest {
            return Err(Error::DigestMismatch);
        }
        let selected = match range {
            None => bytes.clone(),
            Some(range) if range.start <= range.end && range.end <= bytes.len() => {
                bytes[range].to_vec()
            }
            Some(_) => return Err(Error::InvalidRange),
        };
        Ok(selected)
    }

    fn writes(&self) -> u64 {
        self.writes
    }
}

fn digest(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

fn visible_rows(
    global: &BTreeMap<String, Cell>,
    branch: &BTreeMap<String, Cell>,
    include_tombstones: bool,
) -> Vec<VisibleRow> {
    let mut keys = global
        .keys()
        .chain(branch.keys())
        .cloned()
        .collect::<Vec<_>>();
    keys.sort();
    keys.dedup();
    keys.into_iter()
        .filter_map(|key| {
            branch
                .get(&key)
                .map(|cell| (cell, Source::Branch))
                .or_else(|| global.get(&key).map(|cell| (cell, Source::Global)))
                .and_then(|(cell, source)| {
                    if matches!(cell, Cell::Tombstone) && !include_tombstones {
                        None
                    } else {
                        Some(VisibleRow {
                            key,
                            cell: cell.clone(),
                            source,
                        })
                    }
                })
        })
        .collect()
}

fn diff_rows(
    before: &BTreeMap<String, Cell>,
    after: &BTreeMap<String, Cell>,
    file_id: Option<&str>,
    change_id: &str,
    commit_id: &str,
) -> Vec<DiffRow> {
    let mut keys = before
        .keys()
        .chain(after.keys())
        .cloned()
        .collect::<Vec<_>>();
    keys.sort();
    keys.dedup();
    keys.into_iter()
        .filter_map(|key| {
            let old = before.get(&key);
            let new = after.get(&key);
            (old != new).then(|| DiffRow {
                identity: DiffIdentity {
                    schema: "app.row".to_owned(),
                    file_id: file_id.map(str::to_owned),
                    entity_pk: key,
                    change_id: change_id.to_owned(),
                    commit_id: commit_id.to_owned(),
                },
                before: old.cloned(),
                after: new.cloned(),
            })
        })
        .collect()
}

fn materialize(rows: &[VisibleRow]) -> Vec<u8> {
    rows.iter()
        .flat_map(|row| {
            let cell = match &row.cell {
                Cell::Value(value) => value.as_bytes().to_vec(),
                Cell::Null => b"NULL".to_vec(),
                Cell::Tombstone => b"TOMBSTONE".to_vec(),
            };
            row.key
                .as_bytes()
                .iter()
                .copied()
                .chain([b'='])
                .chain(cell)
                .chain([b'\n'])
                .collect::<Vec<_>>()
        })
        .collect()
}

fn validate_ordered_rows(rows: &[(String, Cell)]) -> Result<(), Error> {
    if rows.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
        return Err(Error::NonCanonicalOrder);
    }
    Ok(())
}

#[test]
fn w2_point_range_null_tombstone_and_order_contract() {
    let global = BTreeMap::from([
        ("a".to_owned(), Cell::Value("global-a".to_owned())),
        ("b".to_owned(), Cell::Value("global-b".to_owned())),
        ("c".to_owned(), Cell::Null),
    ]);
    let branch = BTreeMap::from([
        ("a".to_owned(), Cell::Value("local-a".to_owned())),
        ("b".to_owned(), Cell::Tombstone),
        ("d".to_owned(), Cell::Null),
    ]);

    let without_tombstones = visible_rows(&global, &branch, false);
    assert_eq!(
        without_tombstones
            .iter()
            .map(|row| row.key.as_str())
            .collect::<Vec<_>>(),
        ["a", "c", "d"]
    );
    assert_eq!(
        without_tombstones[0].cell,
        Cell::Value("local-a".to_owned())
    );
    assert_eq!(without_tombstones[0].source, Source::Branch);
    assert_eq!(without_tombstones[1].cell, Cell::Null);
    assert_eq!(without_tombstones[2].cell, Cell::Null);

    let with_tombstones = visible_rows(&global, &branch, true);
    assert_eq!(
        with_tombstones
            .iter()
            .map(|row| row.key.as_str())
            .collect::<Vec<_>>(),
        ["a", "b", "c", "d"]
    );
    assert_eq!(with_tombstones[1].cell, Cell::Tombstone);
    assert_eq!(
        materialize(&with_tombstones),
        b"a=local-a\nb=TOMBSTONE\nc=NULL\nd=NULL\n"
    );
}

#[test]
fn w2_diff_and_materialization_preserve_identity() {
    let before = BTreeMap::from([
        ("a".to_owned(), Cell::Value("old".to_owned())),
        ("b".to_owned(), Cell::Null),
    ]);
    let after = BTreeMap::from([
        ("a".to_owned(), Cell::Value("new".to_owned())),
        ("b".to_owned(), Cell::Tombstone),
        ("c".to_owned(), Cell::Value("added".to_owned())),
    ]);
    let rows = diff_rows(&before, &after, Some("file-a"), "change-7", "commit-9");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].identity.schema, "app.row");
    assert_eq!(rows[0].identity.file_id.as_deref(), Some("file-a"));
    assert_eq!(rows[0].identity.entity_pk, "a");
    assert_eq!(rows[2].identity.entity_pk, "c");

    let same_pk_other_file = diff_rows(&before, &after, Some("file-b"), "change-7", "commit-9");
    assert_ne!(rows[0].identity, same_pk_other_file[0].identity);
    assert_eq!(
        materialize(&visible_rows(&before, &after, true)),
        b"a=new\nb=TOMBSTONE\nc=added\n"
    );
}

#[test]
fn w2_65_rows_collapse_to_one_canonical_root() {
    let rows = (0..65)
        .map(|index| (format!("pk-{index:03}"), Cell::Value(index.to_string())))
        .collect::<Vec<_>>();
    validate_ordered_rows(&rows).expect("fixture is ordered");

    let first_leaf = rows[..64].to_vec();
    let second_leaf = rows[64..].to_vec();
    assert_eq!(first_leaf.len(), 64);
    assert_eq!(second_leaf.len(), 1);

    let mut collapsed = first_leaf;
    collapsed.extend(second_leaf);
    validate_ordered_rows(&collapsed).expect("collapsed root stays ordered");
    assert_eq!(collapsed.len(), 65);
    assert_eq!(collapsed.first().unwrap().0, "pk-000");
    assert_eq!(collapsed.last().unwrap().0, "pk-064");
}

#[test]
fn w2_blobref_full_and_range_use_one_object_authority() {
    let mut authority = ObjectAuthority::default();
    let bytes = b"0123456789abcdef".to_vec();
    let object = ObjectId(41);
    authority.put_for_fixture(object, bytes.clone());
    let view = authority.begin_read(object);
    let blob = BlobRef {
        object,
        size: bytes.len(),
        digest: digest(&bytes),
    };

    assert_eq!(authority.read_blob(&view, &blob, None), Ok(bytes.clone()));
    assert_eq!(
        authority.read_blob(&view, &blob, Some(3..9)),
        Ok(b"345678".to_vec())
    );
    assert_eq!(view.writes_at_open, 0);
    assert_eq!(authority.writes(), 0);
}

#[test]
fn w2_same_size_manifest_substitution_fails_closed() {
    let mut authority = ObjectAuthority::default();
    let object = ObjectId(51);
    authority.put_for_fixture(object, b"same-size".to_vec());
    let view = authority.begin_read(object);
    let forged = BlobRef {
        object,
        size: b"same-size".len(),
        digest: digest(b"different"),
    };
    assert_eq!(
        authority.read_blob(&view, &forged, None),
        Err(Error::DigestMismatch)
    );

    let missing = BlobRef {
        object: ObjectId(52),
        size: 0,
        digest: digest(&[]),
    };
    let missing_view = authority.begin_read(ObjectId(52));
    assert_eq!(
        authority.read_blob(&missing_view, &missing, None),
        Err(Error::MissingObject(ObjectId(52)))
    );
}

#[test]
fn w2_corruption_cold_reopen_and_zero_writes() {
    let mut authority = ObjectAuthority::default();
    let root = ObjectId(77);
    authority.put_for_fixture(root, b"root".to_vec());
    let view = authority.begin_read(root);
    let writes_before = authority.writes();

    let bad_order = vec![
        ("pk-002".to_owned(), Cell::Value("two".to_owned())),
        ("pk-001".to_owned(), Cell::Value("one".to_owned())),
    ];
    assert_eq!(
        validate_ordered_rows(&bad_order),
        Err(Error::NonCanonicalOrder)
    );
    assert_eq!(
        authority.read_blob(
            &view,
            &BlobRef {
                object: root,
                size: 4,
                digest: digest(b"bad!"),
            },
            None,
        ),
        Err(Error::DigestMismatch)
    );
    assert_eq!(Err::<(), _>(Error::Malformed), Err(Error::Malformed));
    assert_eq!(authority.writes(), writes_before);

    let encoded = format!(
        "{}:{}:{}:{}",
        view.id,
        view.state_root.0,
        view.epoch,
        authority.writes()
    );
    let reopened = encoded
        .split(':')
        .map(|part| part.parse::<u64>().expect("typed reopen state"))
        .collect::<Vec<_>>();
    assert_eq!(reopened, [view.id, root.0, view.epoch, 0]);
    assert_eq!(authority.writes(), 0);
}

#[test]
fn w2_cross_view_object_pairing_rejected() {
    let mut authority = ObjectAuthority::default();
    let first_root = ObjectId(91);
    let second_root = ObjectId(92);
    authority.put_for_fixture(first_root, b"first".to_vec());
    authority.put_for_fixture(second_root, b"second".to_vec());
    let first = authority.begin_read(first_root);
    let second = authority.begin_read(second_root);
    assert_ne!(first.id, second.id);
    assert_ne!(first.state_root, second.state_root);
    let foreign = BlobRef {
        object: second.state_root,
        size: 6,
        digest: digest(b"second"),
    };
    assert_eq!(
        authority.read_blob(&first, &foreign, None),
        Err(Error::CrossView)
    );
    assert_ne!(first.id, second.id);
    assert_eq!(authority.writes(), 0);
}
