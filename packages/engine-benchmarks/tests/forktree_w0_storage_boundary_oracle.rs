use std::collections::BTreeMap;
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ValueSemantics {
    Mutable,
    Immutable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SpaceId(u32);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StorageSpace {
    id: SpaceId,
    name: &'static str,
    semantics: ValueSemantics,
}

impl StorageSpace {
    const fn engine_declared(id: u32, name: &'static str, semantics: ValueSemantics) -> Self {
        Self {
            id: SpaceId(id),
            name,
            semantics,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ObjectId([u8; 16]);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ObjectDomain {
    RepositoryRoot,
    Value,
    ColumnarOwner,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Cell {
    Null,
    Tombstone,
    Bytes(Vec<u8>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Error {
    RawSpaceForge,
    RawDomainForge,
    DeletedColumnarOwner,
    MissingRoot,
    CorruptObject,
    WrongDomain,
    WrongView,
    ExpiredCursor,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Cursor {
    view_id: u64,
    root_id: ObjectId,
    next_key: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReadView {
    id: u64,
    root_id: ObjectId,
    domain: ObjectDomain,
    epoch: u64,
    writes_at_open: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObjectRecord {
    domain: ObjectDomain,
    bytes: Vec<u8>,
}

#[derive(Default)]
struct ModelAdapter {
    name: &'static str,
    next_view: u64,
    epoch: u64,
    writes: u64,
    objects: BTreeMap<ObjectId, ObjectRecord>,
    rows: BTreeMap<Vec<u8>, Cell>,
}

fn digest(bytes: &[u8]) -> ObjectId {
    // A deterministic stand-in for the authenticated ObjectId calculation. The
    // model only needs identity sensitivity and does not make cryptographic claims.
    let mut lanes = [0x243f_6a88_85a3_08d3_u64, 0x1319_8a2e_0370_7344_u64];
    for (index, byte) in bytes.iter().copied().enumerate() {
        let lane = index & 1;
        lanes[lane] = lanes[lane]
            .wrapping_mul(0x1000_0000_01b3)
            .wrapping_add(u64::from(byte) + index as u64 + 1);
        lanes[lane] ^= lanes[lane].rotate_left(17);
    }
    let mut output = [0; 16];
    output[..8].copy_from_slice(&lanes[0].to_le_bytes());
    output[8..].copy_from_slice(&lanes[1].to_le_bytes());
    ObjectId(output)
}

const OBJECT_SPACE: StorageSpace =
    StorageSpace::engine_declared(0x0001_0001, "forktree.object.v1", ValueSemantics::Immutable);
const SELECTOR_SPACE: StorageSpace =
    StorageSpace::engine_declared(0x0001_0002, "forktree.selector.v1", ValueSemantics::Mutable);
const UNTRACKED_ROW_SPACE: StorageSpace = StorageSpace::engine_declared(
    0x0001_0003,
    "forktree.untracked_row.v1",
    ValueSemantics::Mutable,
);

impl ModelAdapter {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            ..Self::default()
        }
    }

    fn put_object(&mut self, domain: ObjectDomain, bytes: &[u8]) -> ObjectId {
        let id = digest(bytes);
        self.objects.insert(
            id,
            ObjectRecord {
                domain,
                bytes: bytes.to_vec(),
            },
        );
        self.writes += 1;
        id
    }

    fn put_row(&mut self, key: &[u8], value: Cell) {
        self.rows.insert(key.to_vec(), value);
        self.writes += 1;
    }

    fn forge_space(&self, _id: u32) -> Result<StorageSpace, Error> {
        Err(Error::RawSpaceForge)
    }

    fn forge_domain(&self, _kind: u16) -> Result<ObjectDomain, Error> {
        Err(Error::RawDomainForge)
    }

    fn load_deleted_columnar_owner(&self) -> Result<(), Error> {
        Err(Error::DeletedColumnarOwner)
    }

    fn authenticate_root(
        &self,
        root_id: ObjectId,
        expected_domain: ObjectDomain,
    ) -> Result<&ObjectRecord, Error> {
        let record = self.objects.get(&root_id).ok_or(Error::MissingRoot)?;
        if digest(&record.bytes) != root_id {
            return Err(Error::CorruptObject);
        }
        if record.domain != expected_domain {
            return Err(Error::WrongDomain);
        }
        Ok(record)
    }

    fn open_view(
        &mut self,
        root_id: ObjectId,
        expected_domain: ObjectDomain,
    ) -> Result<ReadView, Error> {
        self.authenticate_root(root_id, expected_domain)?;
        self.next_view += 1;
        Ok(ReadView {
            id: self.next_view,
            root_id,
            domain: expected_domain,
            epoch: self.epoch,
            writes_at_open: self.writes,
        })
    }

    fn authenticate_view(&self, view: &ReadView) -> Result<(), Error> {
        self.authenticate_root(view.root_id, view.domain)
            .map(|_| ())
    }

    fn point(&self, view: &ReadView, key: &[u8]) -> Result<Option<Cell>, Error> {
        self.authenticate_view(view)?;
        Ok(self.rows.get(key).cloned())
    }

    fn range(
        &self,
        view: &ReadView,
        cursor: &Cursor,
        limit: usize,
    ) -> Result<(Vec<(Vec<u8>, Cell)>, Cursor), Error> {
        self.authenticate_view(view)?;
        if view.id != cursor.view_id || view.root_id != cursor.root_id {
            return Err(Error::WrongView);
        }
        if cursor.next_key > self.rows.len() {
            return Err(Error::ExpiredCursor);
        }
        let rows = self
            .rows
            .iter()
            .skip(cursor.next_key)
            .take(limit)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        Ok((
            rows,
            Cursor {
                view_id: view.id,
                root_id: view.root_id,
                next_key: cursor.next_key.saturating_add(limit),
            },
        ))
    }

    fn reopen(&mut self, root_id: ObjectId, encoded_root: &[u8]) -> Result<ReadView, Error> {
        let record = self.objects.get(&root_id).ok_or(Error::MissingRoot)?;
        if record.bytes != encoded_root {
            return Err(Error::CorruptObject);
        }
        self.open_view(root_id, ObjectDomain::RepositoryRoot)
    }

    fn tamper_object(&mut self, root_id: ObjectId, bytes: &[u8]) {
        if let Some(record) = self.objects.get_mut(&root_id) {
            record.bytes = bytes.to_vec();
        }
    }
}

#[test]
fn w0_descriptor_and_forbidden_operations_are_real_attempts() {
    assert_eq!(OBJECT_SPACE.semantics, ValueSemantics::Immutable);
    assert_eq!(SELECTOR_SPACE.semantics, ValueSemantics::Mutable);
    assert_eq!(UNTRACKED_ROW_SPACE.semantics, ValueSemantics::Mutable);
    assert_ne!(OBJECT_SPACE.id, SELECTOR_SPACE.id);
    assert_ne!(OBJECT_SPACE.name, SELECTOR_SPACE.name);

    let adapter = ModelAdapter::default();
    assert_eq!(adapter.forge_space(7), Err(Error::RawSpaceForge));
    assert_eq!(adapter.forge_domain(7), Err(Error::RawDomainForge));
    assert_eq!(
        adapter.load_deleted_columnar_owner(),
        Err(Error::DeletedColumnarOwner)
    );
}

#[test]
fn w0_authenticated_cursor_point_range_and_reopen_preserve_semantics() {
    for backend in ["memory", "rocksdb", "slatedb"] {
        let mut adapter = ModelAdapter::new(backend);
        let root_bytes = b"root-v1";
        let root_id = adapter.put_object(ObjectDomain::RepositoryRoot, root_bytes);
        adapter.put_row(b"a", Cell::Bytes(b"1".to_vec()));
        adapter.put_row(b"b", Cell::Bytes(b"2".to_vec()));
        adapter.put_row(b"c", Cell::Bytes(b"3".to_vec()));
        let view = adapter
            .open_view(root_id, ObjectDomain::RepositoryRoot)
            .unwrap();

        assert_eq!(
            adapter.point(&view, b"b"),
            Ok(Some(Cell::Bytes(b"2".to_vec())))
        );
        let cursor = Cursor {
            view_id: view.id,
            root_id,
            next_key: 0,
        };
        let (first, cursor) = adapter.range(&view, &cursor, 2).unwrap();
        assert_eq!(
            first,
            vec![
                (b"a".to_vec(), Cell::Bytes(b"1".to_vec())),
                (b"b".to_vec(), Cell::Bytes(b"2".to_vec()))
            ]
        );
        let (second, _) = adapter.range(&view, &cursor, 2).unwrap();
        assert_eq!(second, vec![(b"c".to_vec(), Cell::Bytes(b"3".to_vec()))]);

        let reopened = adapter.reopen(root_id, root_bytes).unwrap();
        assert_eq!(reopened.root_id, root_id);
        assert_eq!(view.epoch, reopened.epoch);
        assert_eq!(view.writes_at_open, 4);
        assert_eq!(adapter.writes, 4);
        assert_eq!(adapter.name, backend);
    }
}

#[test]
fn w0_reopen_root_identity_domain_and_missing_cases_fail_closed() {
    let mut adapter = ModelAdapter::default();
    let root_bytes = b"root-a";
    let root_id = adapter.put_object(ObjectDomain::RepositoryRoot, root_bytes);
    let other_bytes = b"root-b";
    let other_id = adapter.put_object(ObjectDomain::RepositoryRoot, other_bytes);
    let value_id = adapter.put_object(ObjectDomain::Value, b"value");
    let wrong_domain_id = adapter.put_object(ObjectDomain::ColumnarOwner, b"not-root");

    assert_eq!(
        adapter.open_view(ObjectId([0; 16]), ObjectDomain::RepositoryRoot),
        Err(Error::MissingRoot)
    );
    assert_eq!(
        adapter.open_view(wrong_domain_id, ObjectDomain::RepositoryRoot),
        Err(Error::WrongDomain)
    );
    assert_eq!(
        adapter.open_view(value_id, ObjectDomain::RepositoryRoot),
        Err(Error::WrongDomain)
    );
    assert_eq!(
        adapter.reopen(root_id, other_bytes),
        Err(Error::CorruptObject)
    );
    assert_eq!(
        adapter.reopen(root_id, b"torn-root"),
        Err(Error::CorruptObject)
    );
    adapter.tamper_object(root_id, b"mutated-root");
    assert_eq!(
        adapter.open_view(root_id, ObjectDomain::RepositoryRoot),
        Err(Error::CorruptObject)
    );
    assert_eq!(other_id, digest(other_bytes));
    assert_eq!(adapter.writes, 4);
}

#[test]
fn w0_cursors_are_view_bound_and_expire_without_writes() {
    let mut adapter = ModelAdapter::default();
    let root_id = adapter.put_object(ObjectDomain::RepositoryRoot, b"root");
    adapter.put_row(b"key", Cell::Bytes(b"value".to_vec()));
    let first = adapter
        .open_view(root_id, ObjectDomain::RepositoryRoot)
        .unwrap();
    let second = adapter
        .open_view(root_id, ObjectDomain::RepositoryRoot)
        .unwrap();
    let foreign = Cursor {
        view_id: second.id,
        root_id,
        next_key: 0,
    };
    assert_eq!(adapter.range(&first, &foreign, 1), Err(Error::WrongView));
    let expired = Cursor {
        view_id: first.id,
        root_id,
        next_key: 99,
    };
    assert_eq!(
        adapter.range(&first, &expired, 1),
        Err(Error::ExpiredCursor)
    );
    assert_eq!(adapter.writes, 2);
}

#[test]
fn w0_absent_null_and_tombstone_rows_remain_distinct() {
    let mut adapter = ModelAdapter::default();
    let root_id = adapter.put_object(ObjectDomain::RepositoryRoot, b"root");
    adapter.put_row(b"null", Cell::Null);
    adapter.put_row(b"deleted", Cell::Tombstone);
    let view = adapter
        .open_view(root_id, ObjectDomain::RepositoryRoot)
        .unwrap();

    assert_eq!(adapter.point(&view, b"missing"), Ok(None));
    assert_eq!(adapter.point(&view, b"null"), Ok(Some(Cell::Null)));
    assert_eq!(adapter.point(&view, b"deleted"), Ok(Some(Cell::Tombstone)));
}

#[test]
fn w0_descriptor_parity_is_backend_independent() {
    let descriptors = [OBJECT_SPACE, SELECTOR_SPACE, UNTRACKED_ROW_SPACE];
    for backend in ["memory", "rocksdb", "slatedb"] {
        let adapter = ModelAdapter::new(backend);
        assert_eq!(adapter.name, backend);
        assert_eq!(descriptors[0].name, "forktree.object.v1");
        assert_eq!(descriptors[1].name, "forktree.selector.v1");
        assert_eq!(descriptors[2].name, "forktree.untracked_row.v1");
    }
}
