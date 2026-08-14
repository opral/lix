//! Test-only physical model for a persistent repository history directory.
//!
//! This is intentionally not production wiring. It establishes whether one
//! canonical entry tree plus commit/change route trees can update and query in
//! bounded page work before the storage protocol is changed.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::time::Instant;

type Id = [u8; 32];

const NODE_DOMAIN: &str = "lix history directory v2 model node";
const ENTRY_DOMAIN: &str = "lix history directory v2 model entry";
const ROOT_DOMAIN: &str = "lix history directory v2 model repository root";

#[derive(Clone, Debug, Eq, PartialEq)]
struct Entry {
    domain: u8,
    commit: [u8; 16],
    generation: u64,
    timestamp: u64,
    schema: Vec<u8>,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    first_change_id: [u8; 16],
    last_change_id: [u8; 16],
    segment_ordinal: u32,
    segment_space: u32,
    segment_key: Vec<u8>,
    segment_digest: Id,
    member_count: u32,
    member_id_digest: Id,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Child {
    first: Vec<u8>,
    last: Vec<u8>,
    id: Id,
    entries: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Node {
    Leaf(Vec<(Vec<u8>, Id)>),
    Internal(Vec<Child>),
}

#[derive(Clone, Copy, Debug, Default)]
struct Work {
    reads: u64,
    read_bytes: u64,
    writes: u64,
    write_bytes: u64,
}

impl std::ops::AddAssign for Work {
    fn add_assign(&mut self, rhs: Self) {
        self.reads += rhs.reads;
        self.read_bytes += rhs.read_bytes;
        self.writes += rhs.writes;
        self.write_bytes += rhs.write_bytes;
    }
}

#[derive(Default)]
struct Store {
    nodes: HashMap<Id, Vec<u8>>,
    entries: HashMap<Id, Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RouteRoot {
    id: Id,
    entries: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RepositoryRoot {
    generation: u64,
    entry_root: RouteRoot,
    commit_root: RouteRoot,
    change_root: RouteRoot,
    entry_set_digest: Id,
}

struct Directory {
    fanout: usize,
    store: Store,
    selected_root: Option<(Id, RepositoryRoot)>,
}

impl Directory {
    fn new(fanout: usize) -> Self {
        assert!(fanout >= 4);
        Self {
            fanout,
            store: Store::default(),
            selected_root: None,
        }
    }

    fn publish(&mut self, additions: &[Entry]) -> Work {
        let mut work = Work::default();
        let prior = self.selected_root.map(|(_, root)| root);
        if prior.is_none() {
            let mut entry_items = Vec::with_capacity(additions.len());
            let mut commit_items = Vec::with_capacity(additions.len());
            let mut change_items = Vec::with_capacity(additions.len());
            for entry in additions {
                let encoded = encode_entry(entry);
                let entry_id = hash(ENTRY_DOMAIN, &encoded);
                if self.store.entries.insert(entry_id, encoded.clone()).is_some() {
                    panic!("duplicate initial entry identity");
                }
                work.writes += 1;
                work.write_bytes += encoded.len() as u64;
                entry_items.push((entry_id.to_vec(), entry_id));
                commit_items.push((commit_route_key(entry), entry_id));
                change_items.push((change_route_key(entry), entry_id));
            }
            let entry_root = self.bulk_route(entry_items, &mut work);
            let commit_root = self.bulk_route(commit_items, &mut work);
            let change_root = self.bulk_route(change_items, &mut work);
            let root = RepositoryRoot {
                generation: 1,
                entry_root,
                commit_root,
                change_root,
                entry_set_digest: entry_root.id,
            };
            assert_eq!(entry_root.entries, commit_root.entries);
            assert_eq!(entry_root.entries, change_root.entries);
            let bytes = encode_repository_root(root);
            let id = hash(ROOT_DOMAIN, &bytes);
            work.writes += 1;
            work.write_bytes += bytes.len() as u64;
            self.selected_root = Some((id, root));
            return work;
        }
        let mut entry_root = prior.map(|root| root.entry_root);
        let mut commit_root = prior.map(|root| root.commit_root);
        let mut change_root = prior.map(|root| root.change_root);
        for entry in additions {
            let encoded = encode_entry(entry);
            let entry_id = hash(ENTRY_DOMAIN, &encoded);
            match self.store.entries.entry(entry_id) {
                std::collections::hash_map::Entry::Vacant(slot) => {
                    work.writes += 1;
                    work.write_bytes += encoded.len() as u64;
                    slot.insert(encoded);
                }
                std::collections::hash_map::Entry::Occupied(slot) => {
                    assert_eq!(slot.get(), &encoded, "entry identity collision");
                }
            }
            entry_root = Some(self.insert_route(entry_root, entry_id.to_vec(), entry_id, &mut work));
            commit_root = Some(self.insert_route(
                commit_root,
                commit_route_key(entry),
                entry_id,
                &mut work,
            ));
            change_root = Some(self.insert_route(
                change_root,
                change_route_key(entry),
                entry_id,
                &mut work,
            ));
        }
        let entry_root = entry_root.unwrap_or_else(|| self.empty_root(&mut work));
        let commit_root = commit_root.unwrap_or_else(|| self.empty_root(&mut work));
        let change_root = change_root.unwrap_or_else(|| self.empty_root(&mut work));
        assert_eq!(entry_root.entries, commit_root.entries);
        assert_eq!(entry_root.entries, change_root.entries);
        let root = RepositoryRoot {
            generation: prior.map_or(1, |root| root.generation + 1),
            entry_root,
            commit_root,
            change_root,
            entry_set_digest: entry_root.id,
        };
        let bytes = encode_repository_root(root);
        let id = hash(ROOT_DOMAIN, &bytes);
        work.writes += 1;
        work.write_bytes += bytes.len() as u64;
        self.selected_root = Some((id, root));
        work
    }

    fn bulk_route(&mut self, mut items: Vec<(Vec<u8>, Id)>, work: &mut Work) -> RouteRoot {
        items.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        assert!(items.windows(2).all(|pair| pair[0].0 < pair[1].0));
        if items.is_empty() {
            return self.empty_root(work);
        }
        let mut level = items
            .chunks(self.fanout)
            .map(|chunk| self.store_node(Node::Leaf(chunk.to_vec()), work))
            .collect::<Vec<_>>();
        while level.len() > 1 {
            level = level
                .chunks(self.fanout)
                .map(|chunk| self.store_node(Node::Internal(chunk.to_vec()), work))
                .collect();
        }
        let root = level.pop().expect("nonempty route has a root");
        RouteRoot {
            id: root.id,
            entries: root.entries,
        }
    }

    fn insert_route(
        &mut self,
        root: Option<RouteRoot>,
        key: Vec<u8>,
        value: Id,
        work: &mut Work,
    ) -> RouteRoot {
        let root = root.unwrap_or_else(|| self.empty_root(work));
        let children = self.insert_node(root.id, key, value, work);
        let child = if children.len() == 1 {
            children[0].clone()
        } else {
            self.store_node(Node::Internal(children), work)
        };
        RouteRoot {
            id: child.id,
            entries: child.entries,
        }
    }

    fn empty_root(&mut self, work: &mut Work) -> RouteRoot {
        let child = self.store_node(Node::Leaf(Vec::new()), work);
        RouteRoot {
            id: child.id,
            entries: 0,
        }
    }

    fn insert_node(&mut self, id: Id, key: Vec<u8>, value: Id, work: &mut Work) -> Vec<Child> {
        let node = self.load_node(id, work);
        match node {
            Node::Leaf(mut entries) => {
                match entries.binary_search_by(|entry| entry.0.cmp(&key)) {
                    Ok(_) => panic!("duplicate route key"),
                    Err(index) => entries.insert(index, (key, value)),
                }
                self.split_and_store_leaf(entries, work)
            }
            Node::Internal(mut children) => {
                let index = children
                    .partition_point(|child| child.last.as_slice() < key.as_slice())
                    .min(children.len().saturating_sub(1));
                let old = children.remove(index);
                let replacement = self.insert_node(old.id, key, value, work);
                children.splice(index..index, replacement);
                self.split_and_store_internal(children, work)
            }
        }
    }

    fn split_and_store_leaf(&mut self, entries: Vec<(Vec<u8>, Id)>, work: &mut Work) -> Vec<Child> {
        if entries.len() <= self.fanout {
            return vec![self.store_node(Node::Leaf(entries), work)];
        }
        let split = entries.len() / 2;
        vec![
            self.store_node(Node::Leaf(entries[..split].to_vec()), work),
            self.store_node(Node::Leaf(entries[split..].to_vec()), work),
        ]
    }

    fn split_and_store_internal(&mut self, children: Vec<Child>, work: &mut Work) -> Vec<Child> {
        if children.len() <= self.fanout {
            return vec![self.store_node(Node::Internal(children), work)];
        }
        let split = children.len() / 2;
        vec![
            self.store_node(Node::Internal(children[..split].to_vec()), work),
            self.store_node(Node::Internal(children[split..].to_vec()), work),
        ]
    }

    fn store_node(&mut self, node: Node, work: &mut Work) -> Child {
        validate_node(&node).expect("model must build canonical nodes");
        let bytes = encode_node(&node);
        let id = hash(NODE_DOMAIN, &bytes);
        if let Some(existing) = self.store.nodes.get(&id) {
            assert_eq!(existing, &bytes, "node identity collision");
        } else {
            work.writes += 1;
            work.write_bytes += bytes.len() as u64;
            self.store.nodes.insert(id, bytes);
        }
        node_summary(&node, id)
    }

    fn load_node(&self, id: Id, work: &mut Work) -> Node {
        let bytes = self.store.nodes.get(&id).expect("selected node must exist");
        work.reads += 1;
        work.read_bytes += bytes.len() as u64;
        assert_eq!(hash(NODE_DOMAIN, bytes), id, "node substitution");
        let node = decode_node(bytes).expect("selected node must decode");
        validate_node(&node).expect("selected node must be canonical");
        node
    }

    fn exact(&self, root: RouteRoot, key: &[u8]) -> (Option<Id>, Work) {
        let mut work = Work::default();
        let mut id = root.id;
        loop {
            match self.load_node(id, &mut work) {
                Node::Leaf(entries) => {
                    return (
                        entries
                            .binary_search_by(|entry| entry.0.as_slice().cmp(key))
                            .ok()
                            .map(|index| entries[index].1),
                        work,
                    );
                }
                Node::Internal(children) => {
                    let index = children
                        .partition_point(|child| child.last.as_slice() < key)
                        .min(children.len().saturating_sub(1));
                    id = children[index].id;
                }
            }
        }
    }

    fn bounded(&self, root: RouteRoot, lower: &[u8], upper: &[u8]) -> (Vec<Id>, Work) {
        let mut out = Vec::new();
        let mut work = Work::default();
        self.visit_range(root.id, lower, upper, &mut out, &mut work);
        (out, work)
    }

    fn visit_range(&self, id: Id, lower: &[u8], upper: &[u8], out: &mut Vec<Id>, work: &mut Work) {
        match self.load_node(id, work) {
            Node::Leaf(entries) => {
                let range = (Bound::Included(lower.to_vec()), Bound::Included(upper.to_vec()));
                out.extend(
                    entries
                        .into_iter()
                        .collect::<BTreeMap<_, _>>()
                        .range(range)
                        .map(|(_, id)| *id),
                );
            }
            Node::Internal(children) => {
                for child in children {
                    if child.last.as_slice() >= lower && child.first.as_slice() <= upper {
                        self.visit_range(child.id, lower, upper, out, work);
                    }
                }
            }
        }
    }

    fn verify_selected(&self) -> Result<RepositoryRoot, &'static str> {
        let (selected_id, root) = self.selected_root.ok_or("missing selected root")?;
        let bytes = encode_repository_root(root);
        if hash(ROOT_DOMAIN, &bytes) != selected_id {
            return Err("repository root substitution");
        }
        if root.entry_set_digest != root.entry_root.id
            || root.entry_root.entries != root.commit_root.entries
            || root.entry_root.entries != root.change_root.entries
        {
            return Err("root completeness summary mismatch");
        }
        for route in [root.entry_root, root.commit_root, root.change_root] {
            let bytes = self.store.nodes.get(&route.id).ok_or("missing route root")?;
            if hash(NODE_DOMAIN, bytes) != route.id {
                return Err("route root substitution");
            }
            let node = decode_node(bytes)?;
            validate_node(&node)?;
            if node_summary(&node, route.id).entries != route.entries {
                return Err("route count mismatch");
            }
        }
        Ok(root)
    }
}

fn node_summary(node: &Node, id: Id) -> Child {
    match node {
        Node::Leaf(entries) => Child {
            first: entries.first().map(|entry| entry.0.clone()).unwrap_or_default(),
            last: entries.last().map(|entry| entry.0.clone()).unwrap_or_default(),
            id,
            entries: entries.len() as u64,
        },
        Node::Internal(children) => Child {
            first: children.first().map(|child| child.first.clone()).unwrap_or_default(),
            last: children.last().map(|child| child.last.clone()).unwrap_or_default(),
            id,
            entries: children.iter().map(|child| child.entries).sum(),
        },
    }
}

fn validate_node(node: &Node) -> Result<(), &'static str> {
    match node {
        Node::Leaf(entries) => {
            if entries.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
                return Err("duplicate or unordered leaf key");
            }
        }
        Node::Internal(children) => {
            if children.is_empty()
                || children.iter().any(|child| child.first > child.last || child.entries == 0)
                || children.windows(2).any(|pair| pair[0].last >= pair[1].first)
            {
                return Err("gapped, overlapping, duplicate, or empty child summary");
            }
        }
    }
    Ok(())
}

fn hash(context: &str, bytes: &[u8]) -> Id {
    let mut hasher = blake3::Hasher::new_derive_key(context);
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

fn take_bytes<'a>(input: &mut &'a [u8]) -> Result<&'a [u8], &'static str> {
    let len = u32::from_be_bytes(input.get(..4).ok_or("truncated length")?.try_into().unwrap()) as usize;
    *input = &input[4..];
    let bytes = input.get(..len).ok_or("truncated bytes")?;
    *input = &input[len..];
    Ok(bytes)
}

fn encode_node(node: &Node) -> Vec<u8> {
    let mut out = Vec::new();
    match node {
        Node::Leaf(entries) => {
            out.push(0);
            out.extend_from_slice(&(entries.len() as u32).to_be_bytes());
            for (key, id) in entries {
                put_bytes(&mut out, key);
                out.extend_from_slice(id);
            }
        }
        Node::Internal(children) => {
            out.push(1);
            out.extend_from_slice(&(children.len() as u32).to_be_bytes());
            for child in children {
                put_bytes(&mut out, &child.first);
                put_bytes(&mut out, &child.last);
                out.extend_from_slice(&child.id);
                out.extend_from_slice(&child.entries.to_be_bytes());
            }
        }
    }
    out
}

fn decode_node(mut input: &[u8]) -> Result<Node, &'static str> {
    let kind = *input.first().ok_or("truncated kind")?;
    input = &input[1..];
    let count = u32::from_be_bytes(input.get(..4).ok_or("truncated count")?.try_into().unwrap()) as usize;
    input = &input[4..];
    let node = match kind {
        0 => {
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let key = take_bytes(&mut input)?.to_vec();
                let id: Id = input.get(..32).ok_or("truncated entry id")?.try_into().unwrap();
                input = &input[32..];
                entries.push((key, id));
            }
            Node::Leaf(entries)
        }
        1 => {
            let mut children = Vec::with_capacity(count);
            for _ in 0..count {
                let first = take_bytes(&mut input)?.to_vec();
                let last = take_bytes(&mut input)?.to_vec();
                let id: Id = input.get(..32).ok_or("truncated child id")?.try_into().unwrap();
                input = &input[32..];
                let entries = u64::from_be_bytes(input.get(..8).ok_or("truncated child count")?.try_into().unwrap());
                input = &input[8..];
                children.push(Child { first, last, id, entries });
            }
            Node::Internal(children)
        }
        _ => return Err("wrong node domain"),
    };
    if !input.is_empty() {
        return Err("trailing node bytes");
    }
    Ok(node)
}

fn encode_entry(entry: &Entry) -> Vec<u8> {
    let mut out = vec![entry.domain];
    out.extend_from_slice(&entry.commit);
    out.extend_from_slice(&entry.generation.to_be_bytes());
    out.extend_from_slice(&entry.timestamp.to_be_bytes());
    put_bytes(&mut out, &entry.schema);
    put_bytes(&mut out, &entry.first_key);
    put_bytes(&mut out, &entry.last_key);
    out.extend_from_slice(&entry.first_change_id);
    out.extend_from_slice(&entry.last_change_id);
    out.extend_from_slice(&entry.segment_ordinal.to_be_bytes());
    out.extend_from_slice(&entry.segment_space.to_be_bytes());
    put_bytes(&mut out, &entry.segment_key);
    out.extend_from_slice(&entry.segment_digest);
    out.extend_from_slice(&entry.member_count.to_be_bytes());
    out.extend_from_slice(&entry.member_id_digest);
    out
}

fn encode_repository_root(root: RepositoryRoot) -> Vec<u8> {
    let mut out = Vec::with_capacity(152);
    out.extend_from_slice(&root.generation.to_be_bytes());
    for route in [root.entry_root, root.commit_root, root.change_root] {
        out.extend_from_slice(&route.id);
        out.extend_from_slice(&route.entries.to_be_bytes());
    }
    out.extend_from_slice(&root.entry_set_digest);
    out
}

fn commit_route_key(entry: &Entry) -> Vec<u8> {
    let mut key = Vec::with_capacity(20);
    key.extend_from_slice(&entry.commit);
    key.extend_from_slice(&entry.segment_ordinal.to_be_bytes());
    key
}

fn change_route_key(entry: &Entry) -> Vec<u8> {
    let mut key = Vec::with_capacity(20);
    key.extend_from_slice(&entry.first_change_id);
    key.extend_from_slice(&entry.segment_ordinal.to_be_bytes());
    key
}

fn fixture_entry(index: u64) -> Entry {
    let commit = index.to_be_bytes();
    let mut commit_id = [0; 16];
    commit_id[8..].copy_from_slice(&commit);
    let key = format!("schema-{}/key-{index:012}", index % 16).into_bytes();
    Entry {
        domain: 1,
        commit: commit_id,
        generation: index,
        timestamp: index,
        schema: format!("schema-{}", index % 16).into_bytes(),
        first_key: key.clone(),
        last_key: key,
        first_change_id: commit_id,
        last_change_id: commit_id,
        segment_ordinal: 0,
        segment_space: 0x0004_0004,
        segment_key: commit_id.to_vec(),
        segment_digest: hash("segment", &commit_id),
        member_count: 64,
        member_id_digest: hash("members", &commit_id),
    }
}

#[test]
fn authority_corruption_crash_and_route_controls() {
    let mut directory = Directory::new(64);
    directory.publish(&(0..200).map(fixture_entry).collect::<Vec<_>>());
    let root = directory.verify_selected().unwrap();
    let target = fixture_entry(77);
    let entry_id = hash(ENTRY_DOMAIN, &encode_entry(&target));
    assert_eq!(directory.exact(root.entry_root, &entry_id).0, Some(entry_id));
    assert_eq!(directory.exact(root.commit_root, &commit_route_key(&target)).0, Some(entry_id));

    let selected = directory.selected_root;
    let staged_only = fixture_entry(201);
    let mut work = Work::default();
    let _unselected = directory.insert_route(Some(root.entry_root), vec![0xff], hash(ENTRY_DOMAIN, &encode_entry(&staged_only)), &mut work);
    assert_eq!(directory.selected_root, selected, "unselected staged pages cannot publish");

    let root_id = root.entry_root.id;
    let original = directory.store.nodes[&root_id].clone();
    directory.store.nodes.get_mut(&root_id).unwrap()[0] ^= 1;
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        directory.exact(root.entry_root, &entry_id)
    })).is_err());
    directory.store.nodes.insert(root_id, original);

    let malformed = Node::Leaf(vec![(vec![1], [1; 32]), (vec![1], [2; 32])]);
    assert_eq!(validate_node(&malformed), Err("duplicate or unordered leaf key"));
    let gap = Node::Internal(vec![
        Child { first: vec![0], last: vec![9], id: [1; 32], entries: 1 },
        Child { first: vec![9], last: vec![10], id: [2; 32], entries: 1 },
    ]);
    assert!(validate_node(&gap).is_err());
}

#[test]
fn fanout_crossover_reports_bounded_physical_work() {
    for fanout in [64, 128, 256] {
        for history in [1_000_u64, 10_000, 50_000] {
            let mut directory = Directory::new(fanout);
            let initial = (0..history).map(fixture_entry).collect::<Vec<_>>();
            let build_started = Instant::now();
            let build = directory.publish(&initial);
            let build_elapsed = build_started.elapsed();
            let root = directory.verify_selected().unwrap();
            let mut next_index = history;
            for delta in [1_u64, 10, history / 100] {
                let delta = delta.max(1);
                let additions = (next_index..next_index + delta)
                    .map(fixture_entry)
                    .collect::<Vec<_>>();
                let update_started = Instant::now();
                let update = directory.publish(&additions);
                let update_elapsed = update_started.elapsed();
                let current = directory.verify_selected().unwrap();
                let probe = fixture_entry(next_index + delta - 1);
                let (_, exact) = directory.exact(current.commit_root, &commit_route_key(&probe));
                let lower = change_route_key(&fixture_entry(next_index.saturating_sub(50)));
                let upper = change_route_key(&fixture_entry(next_index + delta));
                let (range, bounded) = directory.bounded(current.change_root, &lower, &upper);
                assert!(!range.is_empty());
                let flat_entry_bytes = initial.iter().map(|entry| encode_entry(entry).len() as u64).sum::<u64>();
                eprintln!(
                    "HISTORY_DIR_V2 fanout={fanout} H={history} D={} build_us={} build_writes={} build_bytes={} update_us={} update_reads={} update_read_bytes={} update_writes={} update_write_bytes={} exact_reads={} exact_read_bytes={} range_rows={} range_reads={} range_read_bytes={} flat_rewrite_bytes={flat_entry_bytes}",
                    delta,
                    build_elapsed.as_micros(), build.writes, build.write_bytes,
                    update_elapsed.as_micros(), update.reads, update.read_bytes,
                    update.writes, update.write_bytes, exact.reads, exact.read_bytes,
                    range.len(), bounded.reads, bounded.read_bytes,
                );
                next_index += delta;
            }
            assert_eq!(root.entry_root.entries, history);
        }
    }
}
