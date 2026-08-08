//! W2 test/report-only model for the ForkTree tracked-state + Blob/CAS cut.
//!
//! This file intentionally has no workspace imports.  It is a typed, warnings-
//! denied executable contract for the first compile-green production head.
//! The durable adapters are dormant until that head exists.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::ops::Range;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Domain {
    StateRoot,
    Manifest,
    Chunk,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ObjectId {
    domain: Domain,
    number: u64,
}

impl ObjectId {
    const fn new(domain: Domain, number: u64) -> Self {
        Self { domain, number }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct BlobId(u128);

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RowIdentity {
    schema: String,
    file_id: Option<BlobId>,
    entity_pk: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Cell {
    Value(String),
    Null,
    Tombstone,
    Blob(BlobLink),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BlobLink {
    blob_id: BlobId,
    manifest: ObjectId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RowRecord {
    identity: RowIdentity,
    cell: Cell,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Source {
    Global,
    Branch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VisibleRow {
    identity: RowIdentity,
    cell: Cell,
    source: Source,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DiffRow {
    identity: RowIdentity,
    before: Option<Cell>,
    after: Option<Cell>,
    change_id: String,
    commit_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ChunkRef {
    object: ObjectId,
    blob_id: BlobId,
    ordinal: u32,
    len: usize,
    digest: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ManifestRecord {
    object: ObjectId,
    blob_id: BlobId,
    chunks: Vec<ChunkRef>,
    total_len: usize,
    digest: u64,
    authenticated: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ChunkRecord {
    reference: ChunkRef,
    bytes: Vec<u8>,
    authenticated: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StateRoot {
    object: ObjectId,
    rows: Vec<RowRecord>,
    digest: u64,
    authenticated: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BlobRef {
    view_id: u64,
    view_owner: u64,
    blob_id: BlobId,
    manifest: ObjectId,
    total_len: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CoherentView {
    view_id: u64,
    owner_id: u64,
    state_root: ObjectId,
    epoch: u64,
    writes_at_open: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct Counters {
    writes: u64,
    commits: u64,
    scans: u64,
    point_reads: u64,
    metadata_reads: u64,
    payload_bytes: u64,
    full_payload_reads: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Error {
    MissingObject(ObjectId),
    MissingChunk(ObjectId),
    WrongKind,
    DomainMismatch,
    IdentityMismatch,
    DigestMismatch,
    SizeMismatch,
    Malformed,
    DuplicateIdentity,
    NonCanonicalOrder,
    InvalidRange,
    CrossView,
    NoRoot,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PersistedImage {
    epoch: u64,
    root: ObjectId,
    states: Vec<StateRoot>,
    manifests: Vec<ManifestRecord>,
    chunks: Vec<ChunkRecord>,
    malformed: bool,
}

#[derive(Default)]
struct ObjectAuthority {
    next_view: u64,
    epoch: u64,
    begin_reads: u64,
    counters: Counters,
    states: BTreeMap<ObjectId, StateRoot>,
    manifests: BTreeMap<ObjectId, ManifestRecord>,
    chunks: BTreeMap<ObjectId, ChunkRecord>,
}

fn digest(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

fn feed_bytes(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash = (*hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
    }
}

fn feed_u64(hash: &mut u64, value: u64) {
    feed_bytes(hash, &value.to_be_bytes());
}

fn manifest_digest(manifest: &ManifestRecord) -> u64 {
    let mut hash = digest(&manifest.blob_id.0.to_be_bytes());
    feed_u64(&mut hash, manifest.total_len as u64);
    for chunk in &manifest.chunks {
        feed_u64(&mut hash, chunk.object.number);
        feed_u64(&mut hash, chunk.ordinal as u64);
        feed_u64(&mut hash, chunk.len as u64);
        feed_u64(&mut hash, chunk.digest);
        feed_bytes(&mut hash, &chunk.blob_id.0.to_be_bytes());
    }
    hash
}

fn row_digest(rows: &[RowRecord]) -> u64 {
    let mut hash = 0xcbf29ce484222325;
    for row in rows {
        feed_bytes(&mut hash, row.identity.schema.as_bytes());
        feed_bytes(&mut hash, &[0]);
        if let Some(file_id) = row.identity.file_id {
            feed_bytes(&mut hash, &file_id.0.to_be_bytes());
        } else {
            feed_bytes(&mut hash, &[0]);
        }
        feed_bytes(&mut hash, row.identity.entity_pk.as_bytes());
        feed_bytes(&mut hash, &[0, cell_tag(&row.cell)]);
        if let Cell::Value(value) = &row.cell {
            feed_bytes(&mut hash, value.as_bytes());
        }
        if let Cell::Blob(link) = &row.cell {
            feed_bytes(&mut hash, &link.blob_id.0.to_be_bytes());
            feed_u64(&mut hash, link.manifest.number);
        }
    }
    hash
}

fn cell_tag(cell: &Cell) -> u8 {
    match cell {
        Cell::Value(_) => 1,
        Cell::Null => 2,
        Cell::Tombstone => 3,
        Cell::Blob(_) => 4,
    }
}

fn validate_row_stream(rows: &[RowRecord]) -> Result<(), Error> {
    let mut seen = BTreeSet::new();
    for pair in rows.windows(2) {
        if pair[0].identity >= pair[1].identity {
            return Err(Error::NonCanonicalOrder);
        }
    }
    for row in rows {
        if !seen.insert(row.identity.clone()) {
            return Err(Error::DuplicateIdentity);
        }
    }
    Ok(())
}

impl ObjectAuthority {
    fn begin_read(&mut self, owner_id: u64, state_root: ObjectId) -> Result<CoherentView, Error> {
        if state_root.domain != Domain::StateRoot {
            return Err(Error::DomainMismatch);
        }
        self.states.get(&state_root).ok_or(Error::NoRoot)?;
        self.next_view += 1;
        self.begin_reads += 1;
        Ok(CoherentView {
            view_id: self.next_view,
            owner_id,
            state_root,
            epoch: self.epoch,
            writes_at_open: self.counters.writes,
        })
    }

    fn install_blob(&mut self, blob_id: BlobId, bytes: &[u8], chunk_len: usize) -> ObjectId {
        let manifest_object = ObjectId::new(Domain::Manifest, blob_id.0 as u64 + 100);
        let mut chunks = Vec::new();
        for (ordinal, part) in bytes.chunks(chunk_len).enumerate() {
            let object = ObjectId::new(Domain::Chunk, blob_id.0 as u64 * 1000 + ordinal as u64 + 1);
            let reference = ChunkRef {
                object,
                blob_id,
                ordinal: ordinal as u32,
                len: part.len(),
                digest: digest(part),
            };
            self.chunks.insert(
                object,
                ChunkRecord {
                    reference: reference.clone(),
                    bytes: part.to_vec(),
                    authenticated: true,
                },
            );
            chunks.push(reference);
        }
        let mut manifest = ManifestRecord {
            object: manifest_object,
            blob_id,
            chunks,
            total_len: bytes.len(),
            digest: 0,
            authenticated: true,
        };
        manifest.digest = manifest_digest(&manifest);
        self.manifests.insert(manifest_object, manifest);
        manifest_object
    }

    fn install_state(&mut self, number: u64, rows: Vec<RowRecord>) -> ObjectId {
        validate_row_stream(&rows).expect("fixture state is canonical");
        let object = ObjectId::new(Domain::StateRoot, number);
        let state = StateRoot {
            object,
            digest: row_digest(&rows),
            rows,
            authenticated: true,
        };
        self.states.insert(object, state);
        object
    }

    fn verify_manifest(
        &mut self,
        object: ObjectId,
        blob_id: BlobId,
    ) -> Result<&ManifestRecord, Error> {
        self.counters.metadata_reads += 1;
        if object.domain != Domain::Manifest {
            return Err(Error::WrongKind);
        }
        let manifest = self
            .manifests
            .get(&object)
            .ok_or(Error::MissingObject(object))?;
        if !manifest.authenticated || manifest.object != object || manifest.blob_id != blob_id {
            return Err(Error::IdentityMismatch);
        }
        if manifest.digest != manifest_digest(manifest) {
            return Err(Error::DigestMismatch);
        }
        if manifest
            .chunks
            .windows(2)
            .any(|pair| pair[0].ordinal >= pair[1].ordinal || pair[0].object >= pair[1].object)
        {
            return Err(Error::NonCanonicalOrder);
        }
        if manifest.chunks.iter().any(|chunk| {
            chunk.object.domain != Domain::Chunk || chunk.blob_id != blob_id || chunk.len == 0
        }) {
            return Err(Error::Malformed);
        }
        if manifest.chunks.iter().map(|chunk| chunk.len).sum::<usize>() != manifest.total_len {
            return Err(Error::SizeMismatch);
        }
        Ok(manifest)
    }

    fn verify_state(&self, root: &StateRoot) -> Result<(), Error> {
        if root.object.domain != Domain::StateRoot || !root.authenticated {
            return Err(Error::WrongKind);
        }
        validate_row_stream(&root.rows)?;
        if root.digest != row_digest(&root.rows) {
            return Err(Error::DigestMismatch);
        }
        Ok(())
    }

    fn verify_state_links(&mut self, root: &StateRoot) -> Result<(), Error> {
        self.verify_state(root)?;
        for row in &root.rows {
            if let Cell::Blob(link) = &row.cell {
                self.verify_manifest(link.manifest, link.blob_id)?;
            }
        }
        Ok(())
    }

    fn assert_view(&self, view: &CoherentView) -> Result<&StateRoot, Error> {
        let root = self.states.get(&view.state_root).ok_or(Error::NoRoot)?;
        self.verify_state(root)?;
        Ok(root)
    }

    fn make_blob_ref(&mut self, view: &CoherentView, link: &BlobLink) -> Result<BlobRef, Error> {
        let manifest = self.verify_manifest(link.manifest, link.blob_id)?;
        Ok(BlobRef {
            view_id: view.view_id,
            view_owner: view.owner_id,
            blob_id: link.blob_id,
            manifest: manifest.object,
            total_len: manifest.total_len,
        })
    }

    fn verify_chunk_metadata(
        &mut self,
        manifest: &ManifestRecord,
        reference: &ChunkRef,
    ) -> Result<&ChunkRecord, Error> {
        self.counters.metadata_reads += 1;
        if reference.object.domain != Domain::Chunk {
            return Err(Error::WrongKind);
        }
        let chunk = self
            .chunks
            .get(&reference.object)
            .ok_or(Error::MissingChunk(reference.object))?;
        if !chunk.authenticated
            || chunk.reference != *reference
            || chunk.reference.blob_id != manifest.blob_id
            || chunk.bytes.len() != reference.len
        {
            return Err(Error::IdentityMismatch);
        }
        Ok(chunk)
    }

    fn read_blob(
        &mut self,
        view: &CoherentView,
        blob: &BlobRef,
        range: Option<Range<usize>>,
    ) -> Result<Vec<u8>, Error> {
        if blob.view_id != view.view_id || blob.view_owner != view.owner_id {
            return Err(Error::CrossView);
        }
        let manifest = self.verify_manifest(blob.manifest, blob.blob_id)?.clone();
        if blob.total_len != manifest.total_len {
            return Err(Error::SizeMismatch);
        }
        let full = range.is_none();
        let requested = range.unwrap_or(0..manifest.total_len);
        if requested.start > requested.end || requested.end > manifest.total_len {
            return Err(Error::InvalidRange);
        }
        let mut output = Vec::with_capacity(requested.end - requested.start);
        let mut offset = 0;
        for reference in &manifest.chunks {
            let end = offset + reference.len;
            let chunk = self.verify_chunk_metadata(&manifest, reference)?.clone();
            let local_start = requested.start.max(offset).saturating_sub(offset);
            let local_end = requested.end.min(end).saturating_sub(offset);
            if local_start < local_end {
                let bytes = &chunk.bytes[local_start..local_end];
                if full {
                    if digest(&chunk.bytes) != reference.digest {
                        return Err(Error::DigestMismatch);
                    }
                }
                output.extend_from_slice(bytes);
                self.counters.payload_bytes += bytes.len() as u64;
            }
            offset = end;
        }
        if full {
            self.counters.full_payload_reads += 1;
        }
        Ok(output)
    }

    fn validate_rows(&mut self, view: &CoherentView, rows: &[RowRecord]) -> Result<(), Error> {
        validate_row_stream(rows)?;
        for row in rows {
            if let Cell::Blob(link) = &row.cell {
                let blob = self.make_blob_ref(view, link)?;
                self.read_blob(view, &blob, Some(0..blob.total_len.min(0)))?;
            }
        }
        Ok(())
    }

    fn point(
        &mut self,
        view: &CoherentView,
        global: &[RowRecord],
        branch: &[RowRecord],
        key: &RowIdentity,
    ) -> Result<Cell, Error> {
        self.assert_view(view)?;
        self.validate_rows(view, global)?;
        self.validate_rows(view, branch)?;
        self.counters.point_reads += 1;
        branch
            .iter()
            .find(|row| row.identity == *key)
            .or_else(|| global.iter().find(|row| row.identity == *key))
            .map(|row| row.cell.clone())
            .ok_or(Error::NoRoot)
    }

    fn range(
        &mut self,
        view: &CoherentView,
        global: &[RowRecord],
        branch: &[RowRecord],
        include_tombstones: bool,
    ) -> Result<Vec<VisibleRow>, Error> {
        self.assert_view(view)?;
        self.validate_rows(view, global)?;
        self.validate_rows(view, branch)?;
        self.counters.scans += 1;
        let mut merged = BTreeMap::<RowIdentity, VisibleRow>::new();
        for row in global {
            merged.insert(
                row.identity.clone(),
                VisibleRow {
                    identity: row.identity.clone(),
                    cell: row.cell.clone(),
                    source: Source::Global,
                },
            );
        }
        for row in branch {
            merged.insert(
                row.identity.clone(),
                VisibleRow {
                    identity: row.identity.clone(),
                    cell: row.cell.clone(),
                    source: Source::Branch,
                },
            );
        }
        Ok(merged
            .into_values()
            .filter(|row| include_tombstones || !matches!(row.cell, Cell::Tombstone))
            .collect())
    }

    fn diff(
        &mut self,
        view: &CoherentView,
        before: &[RowRecord],
        after: &[RowRecord],
        change_id: &str,
        commit_id: &str,
    ) -> Result<Vec<DiffRow>, Error> {
        self.assert_view(view)?;
        self.validate_rows(view, before)?;
        self.validate_rows(view, after)?;
        self.counters.scans += 1;
        let mut left = BTreeMap::new();
        let mut right = BTreeMap::new();
        for row in before {
            left.insert(row.identity.clone(), row.cell.clone());
        }
        for row in after {
            right.insert(row.identity.clone(), row.cell.clone());
        }
        let keys = left
            .keys()
            .chain(right.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        Ok(keys
            .into_iter()
            .filter_map(|identity| {
                let old = left.get(&identity).cloned();
                let new = right.get(&identity).cloned();
                (old != new).then(|| DiffRow {
                    identity,
                    before: old,
                    after: new,
                    change_id: change_id.to_owned(),
                    commit_id: commit_id.to_owned(),
                })
            })
            .collect())
    }

    fn materialize(&mut self, view: &CoherentView, rows: &[VisibleRow]) -> Result<Vec<u8>, Error> {
        let mut output = Vec::new();
        for row in rows {
            output.extend_from_slice(row.identity.entity_pk.as_bytes());
            output.push(b'=');
            match &row.cell {
                Cell::Value(value) => output.extend_from_slice(value.as_bytes()),
                Cell::Null => output.extend_from_slice(b"NULL"),
                Cell::Tombstone => output.extend_from_slice(b"TOMBSTONE"),
                Cell::Blob(link) => {
                    let blob = self.make_blob_ref(view, link)?;
                    output.extend_from_slice(&self.read_blob(view, &blob, None)?);
                }
            }
            output.push(b'\n');
        }
        Ok(output)
    }

    fn flush(&mut self, root: ObjectId) -> Result<PersistedImage, Error> {
        self.states.get(&root).ok_or(Error::NoRoot)?;
        self.counters.writes += 1;
        self.counters.commits += 1;
        Ok(PersistedImage {
            epoch: self.epoch,
            root,
            states: self.states.values().cloned().collect(),
            manifests: self.manifests.values().cloned().collect(),
            chunks: self.chunks.values().cloned().collect(),
            malformed: false,
        })
    }

    fn reopen(image: PersistedImage) -> Result<Self, Error> {
        if image.malformed || image.root.domain != Domain::StateRoot {
            return Err(Error::Malformed);
        }
        let mut authority = Self {
            epoch: image.epoch,
            ..Self::default()
        };
        for state in image.states {
            authority.verify_state(&state)?;
            if authority.states.insert(state.object, state).is_some() {
                return Err(Error::DuplicateIdentity);
            }
        }
        for manifest in image.manifests {
            if authority
                .manifests
                .insert(manifest.object, manifest.clone())
                .is_some()
            {
                return Err(Error::DuplicateIdentity);
            }
            authority.verify_manifest(manifest.object, manifest.blob_id)?;
        }
        for chunk in image.chunks {
            if authority
                .chunks
                .insert(chunk.reference.object, chunk.clone())
                .is_some()
            {
                return Err(Error::DuplicateIdentity);
            }
            if chunk.reference.object.domain != Domain::Chunk
                || !chunk.authenticated
                || chunk.bytes.len() != chunk.reference.len
                || digest(&chunk.bytes) != chunk.reference.digest
            {
                return Err(Error::DigestMismatch);
            }
        }
        let states = authority.states.values().cloned().collect::<Vec<_>>();
        for state in &states {
            authority.verify_state_links(state)?;
        }
        authority.states.get(&image.root).ok_or(Error::NoRoot)?;
        for manifest in authority.manifests.values() {
            for reference in &manifest.chunks {
                authority
                    .chunks
                    .get(&reference.object)
                    .ok_or(Error::MissingChunk(reference.object))?;
            }
        }
        Ok(authority)
    }
}

fn blob_link(id: BlobId, manifest: ObjectId) -> BlobLink {
    BlobLink {
        blob_id: id,
        manifest,
    }
}

fn row(schema: &str, file_id: Option<BlobId>, entity_pk: &str, cell: Cell) -> RowRecord {
    RowRecord {
        identity: RowIdentity {
            schema: schema.to_owned(),
            file_id,
            entity_pk: entity_pk.to_owned(),
        },
        cell,
    }
}

fn fixture() -> (ObjectAuthority, ObjectId, Vec<RowRecord>, Vec<RowRecord>) {
    let mut authority = ObjectAuthority::default();
    let blob_id = BlobId(7);
    let manifest = authority.install_blob(blob_id, b"0123456789abcdef", 4);
    let global = vec![
        row("app.row", None, "a", Cell::Value("global-a".to_owned())),
        row("app.row", None, "b", Cell::Null),
        row(
            "lix_file",
            Some(blob_id),
            "file-a",
            Cell::Blob(blob_link(blob_id, manifest)),
        ),
    ];
    let root = authority.install_state(77, global.clone());
    let branch = vec![
        row("app.row", None, "a", Cell::Value("branch-a".to_owned())),
        row("app.row", None, "b", Cell::Tombstone),
        row("app.row", None, "c", Cell::Null),
    ];
    (authority, root, global, branch)
}

fn assert_read_side_effect_free(before: &Counters, after: &Counters) {
    assert_eq!(before.writes, after.writes, "read changed durable writes");
    assert_eq!(
        before.commits, after.commits,
        "read changed durable commits"
    );
}

#[test]
fn w2_point_range_null_tombstone_and_order_contract() {
    let (mut authority, root, global, branch) = fixture();
    let view = authority.begin_read(11, root).expect("root");
    let before = authority.counters.clone();
    let key = RowIdentity {
        schema: "app.row".into(),
        file_id: None,
        entity_pk: "a".into(),
    };
    assert_eq!(
        authority.point(&view, &global, &branch, &key),
        Ok(Cell::Value("branch-a".into()))
    );
    assert_eq!(authority.counters.scans, before.scans);
    let without = authority
        .range(&view, &global, &branch, false)
        .expect("range");
    assert_eq!(
        without
            .iter()
            .map(|row| row.identity.entity_pk.as_str())
            .collect::<Vec<_>>(),
        ["a", "c", "file-a"]
    );
    assert_eq!(without[0].source, Source::Branch);
    assert_eq!(without[1].cell, Cell::Null);
    let with = authority
        .range(&view, &global, &branch, true)
        .expect("tombstones");
    assert_eq!(
        with.iter()
            .map(|row| row.identity.entity_pk.as_str())
            .collect::<Vec<_>>(),
        ["a", "b", "c", "file-a"]
    );
    assert_eq!(with[1].cell, Cell::Tombstone);
    assert_read_side_effect_free(&before, &authority.counters);
}

#[test]
fn w2_diff_and_materialization_preserve_identity() {
    let (mut authority, root, global, branch) = fixture();
    let view = authority.begin_read(12, root).expect("root");
    let rows = authority
        .diff(&view, &global, &branch, "change-7", "commit-9")
        .expect("diff");
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0].identity.entity_pk, "a");
    assert_eq!(rows[0].change_id, "change-7");
    assert!(rows.iter().any(|row| row.after == Some(Cell::Tombstone)));
    let visible = authority
        .range(&view, &global, &branch, true)
        .expect("visible");
    assert_eq!(
        authority.materialize(&view, &visible).expect("materialize"),
        b"a=branch-a\nb=TOMBSTONE\nc=NULL\nfile-a=0123456789abcdef\n"
    );
}

#[test]
fn w2_65_rows_collapse_to_one_canonical_root() {
    let leaves = (0..65)
        .map(|index| {
            vec![row(
                "app.row",
                None,
                &format!("pk-{index:03}"),
                Cell::Value(index.to_string()),
            )]
        })
        .collect::<Vec<_>>();
    let mut collapsed = Vec::new();
    for leaf in &leaves {
        validate_row_stream(leaf).expect("leaf");
        collapsed.extend(leaf.clone());
    }
    validate_row_stream(&collapsed).expect("canonical root");
    assert_eq!(collapsed.len(), 65);
    assert_eq!(collapsed.first().unwrap().identity.entity_pk, "pk-000");
    assert_eq!(collapsed.last().unwrap().identity.entity_pk, "pk-064");
}

#[test]
fn w2_blobref_full_and_bounded_range_use_one_object_authority() {
    let (mut authority, root, global, _) = fixture();
    let view = authority.begin_read(13, root).expect("root");
    let link = match &global[2].cell {
        Cell::Blob(link) => link.clone(),
        _ => unreachable!(),
    };
    let blob = authority.make_blob_ref(&view, &link).expect("blob ref");
    let before = authority.counters.clone();
    assert_eq!(
        authority
            .read_blob(&view, &blob, Some(3..9))
            .expect("range"),
        b"345678"
    );
    assert_eq!(
        authority.counters.full_payload_reads,
        before.full_payload_reads
    );
    assert_eq!(authority.counters.payload_bytes - before.payload_bytes, 6);
    assert_eq!(authority.counters.writes, before.writes);
    assert_eq!(
        authority.read_blob(&view, &blob, None).expect("full"),
        b"0123456789abcdef"
    );
    assert_eq!(
        authority.counters.full_payload_reads - before.full_payload_reads,
        1
    );
}

#[test]
fn w2_same_size_manifest_chunk_substitution_and_wrong_kind_fail_closed() {
    let (mut authority, root, global, _) = fixture();
    let view = authority.begin_read(14, root).expect("root");
    assert_eq!(
        authority.begin_read(14, ObjectId::new(Domain::Chunk, 1)),
        Err(Error::DomainMismatch)
    );
    let link = match &global[2].cell {
        Cell::Blob(link) => link.clone(),
        _ => unreachable!(),
    };
    let mut forged = authority.manifests.get(&link.manifest).unwrap().clone();
    forged.digest ^= 1;
    authority.manifests.insert(link.manifest, forged);
    assert_eq!(
        authority.make_blob_ref(&view, &link),
        Err(Error::DigestMismatch)
    );

    let (mut authority, root, global, _) = fixture();
    let view = authority.begin_read(15, root).expect("root");
    let link = match &global[2].cell {
        Cell::Blob(link) => link.clone(),
        _ => unreachable!(),
    };
    let manifest = authority.manifests.get_mut(&link.manifest).unwrap();
    manifest.chunks.swap(0, 1);
    manifest.digest = manifest_digest(manifest);
    assert_eq!(
        authority.make_blob_ref(&view, &link),
        Err(Error::NonCanonicalOrder)
    );

    let wrong = BlobLink {
        blob_id: link.blob_id,
        manifest: ObjectId::new(Domain::Chunk, 1),
    };
    assert_eq!(
        authority.make_blob_ref(&view, &wrong),
        Err(Error::WrongKind)
    );
}

#[test]
fn w2_duplicate_missing_malformed_and_reordered_identities_fail_closed() {
    let (mut authority, root, global, branch) = fixture();
    let view = authority.begin_read(16, root).expect("root");
    let mut duplicate = branch.clone();
    duplicate.push(duplicate[0].clone());
    assert_eq!(
        authority.range(&view, &global, &duplicate, true),
        Err(Error::NonCanonicalOrder)
    );

    let mut missing = global.clone();
    missing[2].cell = Cell::Blob(BlobLink {
        blob_id: BlobId(7),
        manifest: ObjectId::new(Domain::Manifest, 9999),
    });
    assert_eq!(
        authority.range(&view, &missing, &branch, true),
        Err(Error::MissingObject(ObjectId::new(Domain::Manifest, 9999)))
    );

    let mut malformed = global.clone();
    malformed[2].cell = Cell::Blob(BlobLink {
        blob_id: BlobId(7),
        manifest: ObjectId::new(Domain::Manifest, 0),
    });
    assert_eq!(
        authority.range(&view, &malformed, &branch, true),
        Err(Error::MissingObject(ObjectId::new(Domain::Manifest, 0)))
    );
}

#[test]
fn w2_same_root_cross_view_and_owner_pairing_is_rejected() {
    let (mut authority, root, global, _) = fixture();
    let first = authority.begin_read(17, root).expect("first");
    let second = authority.begin_read(17, root).expect("second");
    let link = match &global[2].cell {
        Cell::Blob(link) => link.clone(),
        _ => unreachable!(),
    };
    let blob = authority.make_blob_ref(&first, &link).expect("blob");
    assert_eq!(
        authority.read_blob(&second, &blob, Some(0..1)),
        Err(Error::CrossView)
    );
    let third = authority.begin_read(18, root).expect("third");
    assert_eq!(
        authority.read_blob(&third, &blob, None),
        Err(Error::CrossView)
    );
    assert_eq!(authority.begin_reads, 3);
}

#[test]
fn w2_persisted_flush_drop_reopen_reauthenticates_rows_manifest_and_chunks() {
    let (mut authority, root, global, _) = fixture();
    let view = authority.begin_read(19, root).expect("root");
    let before = authority.counters.clone();
    let image = authority.flush(root).expect("flush");
    assert_eq!(authority.counters.writes - before.writes, 1);
    assert_eq!(authority.counters.commits - before.commits, 1);
    let mut reopened = ObjectAuthority::reopen(image.clone()).expect("reopen");
    let reopened_view = reopened.begin_read(19, root).expect("reopened root");
    let rows = reopened
        .range(&reopened_view, &global, &[], false)
        .expect("reopened rows");
    assert_eq!(
        reopened
            .materialize(&reopened_view, &rows)
            .expect("reopened materialize"),
        b"a=global-a\nb=NULL\nfile-a=0123456789abcdef\n"
    );

    let mut missing_chunk = image.clone();
    missing_chunk.chunks.clear();
    assert!(matches!(
        ObjectAuthority::reopen(missing_chunk),
        Err(Error::MissingChunk(_))
    ));
    let mut malformed = image.clone();
    malformed.malformed = true;
    assert!(matches!(
        ObjectAuthority::reopen(malformed),
        Err(Error::Malformed)
    ));
    let mut bad_state = image;
    bad_state.states[0].digest ^= 1;
    assert!(matches!(
        ObjectAuthority::reopen(bad_state),
        Err(Error::DigestMismatch)
    ));
    let (mut authority, root, _, _) = fixture();
    let image = authority.flush(root).expect("flush for row identity");
    let mut substituted_row = image;
    substituted_row.states[0].rows[0].identity.entity_pk = "aa".into();
    assert!(matches!(
        ObjectAuthority::reopen(substituted_row),
        Err(Error::DigestMismatch)
    ));
    assert_eq!(view.writes_at_open, 0);
}

#[test]
fn w2_corrupt_rows_reject_before_partial_materialization_and_reads_do_not_write() {
    let (mut authority, root, mut global, branch) = fixture();
    let view = authority.begin_read(20, root).expect("root");
    global[1].identity = global[0].identity.clone();
    let before = authority.counters.clone();
    assert_eq!(
        authority.range(&view, &global, &branch, true),
        Err(Error::NonCanonicalOrder)
    );
    assert_read_side_effect_free(&before, &authority.counters);
    assert_eq!(authority.counters.scans, before.scans);
}
