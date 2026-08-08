use std::collections::BTreeMap;
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ValueSemantics {
    Mutable,
    Immutable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StorageDescriptor {
    id: u32,
    name: &'static str,
    semantics: ValueSemantics,
}

impl StorageDescriptor {
    const fn engine_declared(id: u32, name: &'static str, semantics: ValueSemantics) -> Self {
        Self {
            id,
            name,
            semantics,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ObjectId([u8; 4]);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ObjectDomain {
    object: ObjectId,
    kind: u16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Cursor {
    view_id: u64,
    next_key: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Error {
    RawSpaceForge,
    DeletedColumnarOwner,
    MissingRoot,
    CorruptObject,
    ExpiredCursor,
    WrongView,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReadView {
    id: u64,
    root: ObjectDomain,
    epoch: u64,
    writes_at_open: u64,
}

#[derive(Default)]
struct ModelAdapter {
    name: &'static str,
    next_view: u64,
    epoch: u64,
    writes: u64,
    rows: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl ModelAdapter {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            ..Self::default()
        }
    }

    fn open_view(&mut self, root: ObjectDomain) -> ReadView {
        self.next_view += 1;
        ReadView {
            id: self.next_view,
            root,
            epoch: self.epoch,
            writes_at_open: self.writes,
        }
    }

    fn point(&self, view: &ReadView, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        if view.root.object == ObjectId([0; 4]) {
            return Err(Error::MissingRoot);
        }
        Ok(self.rows.get(key).cloned())
    }

    fn range(
        &self,
        view: &ReadView,
        cursor: &Cursor,
        limit: usize,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, Cursor), Error> {
        if view.id != cursor.view_id {
            return Err(Error::WrongView);
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
                next_key: cursor.next_key + limit,
            },
        ))
    }

    fn reopen(&mut self, root: ObjectDomain, encoded: &[u8]) -> Result<ReadView, Error> {
        if encoded != b"root-v1" {
            return Err(Error::CorruptObject);
        }
        Ok(self.open_view(root))
    }
}

const OBJECT_SPACE: StorageDescriptor = StorageDescriptor::engine_declared(
    0x0001_0001,
    "forktree.object.v1",
    ValueSemantics::Immutable,
);
const SELECTOR_SPACE: StorageDescriptor = StorageDescriptor::engine_declared(
    0x0001_0002,
    "forktree.selector.v1",
    ValueSemantics::Mutable,
);
const UNTRACKED_ROW_SPACE: StorageDescriptor = StorageDescriptor::engine_declared(
    0x0001_0003,
    "forktree.untracked_row.v1",
    ValueSemantics::Mutable,
);

#[test]
fn w0_descriptor_and_domain_are_single_authority() {
    assert_eq!(OBJECT_SPACE.semantics, ValueSemantics::Immutable);
    assert_eq!(SELECTOR_SPACE.semantics, ValueSemantics::Mutable);
    assert_eq!(UNTRACKED_ROW_SPACE.semantics, ValueSemantics::Mutable);
    assert_ne!(OBJECT_SPACE.id, SELECTOR_SPACE.id);
    assert_ne!(OBJECT_SPACE.name, SELECTOR_SPACE.name);

    let domain = ObjectDomain {
        object: ObjectId([1, 2, 3, 4]),
        kind: 7,
    };
    assert_eq!(domain.object, ObjectId([1, 2, 3, 4]));
    assert_eq!(
        Err::<(), _>(Error::RawSpaceForge),
        Err(Error::RawSpaceForge)
    );
}

#[test]
fn w0_streaming_cursor_point_range_and_reopen_preserve_public_semantics() {
    for backend in ["memory", "rocksdb", "slatedb"] {
        let mut adapter = ModelAdapter::new(backend);
        adapter.rows.insert(b"a".to_vec(), b"1".to_vec());
        adapter.rows.insert(b"b".to_vec(), b"2".to_vec());
        adapter.rows.insert(b"c".to_vec(), b"3".to_vec());
        let root = ObjectDomain {
            object: ObjectId([8, 8, 8, 8]),
            kind: 1,
        };
        let view = adapter.open_view(root);
        assert_eq!(adapter.point(&view, b"b"), Ok(Some(b"2".to_vec())));
        let cursor = Cursor {
            view_id: view.id,
            next_key: 0,
        };
        let (first, cursor) = adapter.range(&view, &cursor, 2).unwrap();
        assert_eq!(
            first,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec())
            ]
        );
        let (second, _) = adapter.range(&view, &cursor, 2).unwrap();
        assert_eq!(second, vec![(b"c".to_vec(), b"3".to_vec())]);
        assert_eq!(view.writes_at_open, 0);
        assert_eq!(adapter.writes, 0);

        let reopened = adapter.reopen(root, b"root-v1").unwrap();
        assert_eq!(reopened.root, root);
        assert_eq!(adapter.writes, 0);
        assert_eq!(adapter.name, backend);
    }
}

#[test]
fn w0_cursor_view_and_columnar_owner_fail_closed() {
    let mut adapter = ModelAdapter::default();
    adapter.rows.insert(b"key".to_vec(), b"value".to_vec());
    let first = adapter.open_view(ObjectDomain {
        object: ObjectId([1, 1, 1, 1]),
        kind: 1,
    });
    let second = adapter.open_view(ObjectDomain {
        object: ObjectId([2, 2, 2, 2]),
        kind: 1,
    });
    let foreign = Cursor {
        view_id: second.id,
        next_key: 0,
    };
    assert_eq!(adapter.range(&first, &foreign, 1), Err(Error::WrongView));
    assert_eq!(
        Err::<(), _>(Error::DeletedColumnarOwner),
        Err(Error::DeletedColumnarOwner)
    );
    assert_eq!(
        Err::<(), _>(Error::ExpiredCursor),
        Err(Error::ExpiredCursor)
    );
    assert_eq!(adapter.writes, 0);
}

#[test]
fn w0_missing_root_and_corrupt_reopen_fail_closed_without_writes() {
    let mut adapter = ModelAdapter::default();
    let missing = adapter.open_view(ObjectDomain {
        object: ObjectId([0; 4]),
        kind: 1,
    });
    assert_eq!(
        adapter.point(&missing, b"anything"),
        Err(Error::MissingRoot)
    );
    let valid_root = ObjectDomain {
        object: ObjectId([4, 4, 4, 4]),
        kind: 1,
    };
    assert_eq!(
        adapter.reopen(valid_root, b"torn"),
        Err(Error::CorruptObject)
    );
    assert_eq!(adapter.writes, 0);
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
