//! Test-only model for a canonical authenticated commit/segment pack.

use std::collections::{BTreeSet, HashMap, HashSet};

type Id = [u8; 32];

const PACK_DOMAIN: &str = "lix history pack model pack";
const NODE_DOMAIN: &str = "lix history pack model directory node";
const ROOT_DOMAIN: &str = "lix history pack model repository root";
const ENTRY_DOMAIN: &str = "lix history pack model entry";
const ENTRY_SET_DOMAIN: &str = "lix history pack model entry set";

#[derive(Clone, Debug, Eq, PartialEq)]
struct Entry {
    owner_domain: u8,
    owner_id: [u8; 16],
    commit_id: [u8; 16],
    generation: u64,
    segment_ordinal: u32,
    first_change_id: [u8; 16],
    last_change_id: [u8; 16],
    schema: Vec<u8>,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    segment_space: u32,
    segment_key: Vec<u8>,
    segment_digest: Id,
    member_count: u32,
    member_ids_digest: Id,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Pack {
    entries: Vec<Entry>,
    change_order: Vec<u16>,
    schema_order: Vec<u16>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Summary {
    id: Id,
    packs: u64,
    entries: u64,
    commit_min: Vec<u8>,
    commit_max: Vec<u8>,
    change_min: Vec<u8>,
    change_max: Vec<u8>,
    schema_min: Vec<u8>,
    schema_max: Vec<u8>,
    owner_min: Vec<u8>,
    owner_max: Vec<u8>,
    entry_set_digest: Id,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Node {
    Leaf(Vec<Summary>),
    Internal(Vec<Summary>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RepositoryRoot {
    generation: u64,
    target_entries: u16,
    directory: Summary,
    entry_set_digest: Id,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
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

#[derive(Clone, Default)]
struct Store {
    packs: HashMap<Id, Vec<u8>>,
    nodes: HashMap<Id, Vec<u8>>,
}

#[derive(Clone)]
struct Directory {
    target: usize,
    fanout: usize,
    store: Store,
    selected: Option<(Id, RepositoryRoot)>,
}

#[derive(Clone, Copy)]
enum Route {
    Commit,
    Change,
    Schema,
}

impl Directory {
    fn new(target: usize, fanout: usize) -> Self {
        assert!((1..=u16::MAX as usize).contains(&target));
        assert!(fanout >= 4);
        Self {
            target,
            fanout,
            store: Store::default(),
            selected: None,
        }
    }

    fn publish_cas(
        &mut self,
        expected_generation: u64,
        additions: &[Entry],
    ) -> Result<Work, &'static str> {
        let actual = self.selected.as_ref().map_or(0, |(_, root)| root.generation);
        if expected_generation != actual {
            return Err("stale repository root");
        }
        self.publish(additions)
    }

    fn publish(&mut self, additions: &[Entry]) -> Result<Work, &'static str> {
        if additions.is_empty() {
            return Err("empty publication");
        }
        let mut additions = additions.to_vec();
        additions.sort_unstable_by_key(commit_key);
        if additions.windows(2).any(|pair| commit_key(&pair[0]) >= commit_key(&pair[1])) {
            return Err("duplicate commit coordinate");
        }

        let mut work = Work::default();
        let directory = if let Some((_, root)) = self.selected.clone() {
            let last_summary = self.rightmost_pack(&root.directory, &mut work)?;
            let mut entries = self.load_pack(&last_summary, &mut work)?.entries;
            if commit_key(additions.first().unwrap()) <= commit_key(entries.last().unwrap()) {
                return Err("non-append publication");
            }
            entries.extend(additions);
            let replacements = entries
                .chunks(self.target)
                .map(|chunk| self.store_pack(chunk.to_vec(), &mut work))
                .collect::<Result<Vec<_>, _>>()?;
            self.replace_rightmost(&root.directory, replacements, &mut work)?
        } else {
            let packs = additions
                .chunks(self.target)
                .map(|chunk| self.store_pack(chunk.to_vec(), &mut work))
                .collect::<Result<Vec<_>, _>>()?;
            self.bulk_directory(packs, &mut work)?
        };

        let root = RepositoryRoot {
            generation: self.selected.as_ref().map_or(1, |(_, root)| root.generation + 1),
            target_entries: self.target as u16,
            entry_set_digest: directory.entry_set_digest,
            directory,
        };
        let bytes = encode_root(&root);
        let id = digest(ROOT_DOMAIN, &bytes);
        work.writes += 1;
        work.write_bytes += bytes.len() as u64;
        self.selected = Some((id, root));
        Ok(work)
    }

    fn store_pack(&mut self, entries: Vec<Entry>, work: &mut Work) -> Result<Summary, &'static str> {
        if entries.is_empty() || entries.len() > self.target {
            return Err("invalid pack size");
        }
        let pack = canonical_pack(entries)?;
        let bytes = encode_pack(&pack);
        let id = digest(PACK_DOMAIN, &bytes);
        if let Some(prior) = self.store.packs.get(&id) {
            if prior != &bytes {
                return Err("pack identity collision");
            }
        } else {
            work.writes += 1;
            work.write_bytes += bytes.len() as u64;
            self.store.packs.insert(id, bytes);
        }
        Ok(pack_summary(&pack, id))
    }

    fn store_node(&mut self, node: Node, work: &mut Work) -> Result<Summary, &'static str> {
        validate_node(&node, self.fanout)?;
        let bytes = encode_node(&node);
        let id = digest(NODE_DOMAIN, &bytes);
        if let Some(prior) = self.store.nodes.get(&id) {
            if prior != &bytes {
                return Err("node identity collision");
            }
        } else {
            work.writes += 1;
            work.write_bytes += bytes.len() as u64;
            self.store.nodes.insert(id, bytes);
        }
        Ok(node_summary(&node, id))
    }

    fn load_pack(&self, expected: &Summary, work: &mut Work) -> Result<Pack, &'static str> {
        let bytes = self.store.packs.get(&expected.id).ok_or("missing pack")?;
        work.reads += 1;
        work.read_bytes += bytes.len() as u64;
        if digest(PACK_DOMAIN, bytes) != expected.id {
            return Err("pack substitution");
        }
        let pack = decode_pack(bytes)?;
        if pack_summary(&pack, expected.id) != *expected {
            return Err("pack bounds mismatch");
        }
        Ok(pack)
    }

    fn load_node(&self, expected: &Summary, work: &mut Work) -> Result<Node, &'static str> {
        let bytes = self.store.nodes.get(&expected.id).ok_or("missing directory node")?;
        work.reads += 1;
        work.read_bytes += bytes.len() as u64;
        self.decode_expected_node(expected, bytes)
    }

    fn decode_expected_node(&self, expected: &Summary, bytes: &[u8]) -> Result<Node, &'static str> {
        if digest(NODE_DOMAIN, bytes) != expected.id {
            return Err("directory substitution");
        }
        let node = decode_node(bytes)?;
        validate_node(&node, self.fanout)?;
        if node_summary(&node, expected.id) != *expected {
            return Err("directory bounds mismatch");
        }
        Ok(node)
    }

    fn bulk_directory(
        &mut self,
        mut packs: Vec<Summary>,
        work: &mut Work,
    ) -> Result<Summary, &'static str> {
        packs.sort_unstable_by(|left, right| left.commit_min.cmp(&right.commit_min));
        validate_summary_order(&packs)?;
        let mut level = packs
            .chunks(self.fanout)
            .map(|chunk| self.store_node(Node::Leaf(chunk.to_vec()), work))
            .collect::<Result<Vec<_>, _>>()?;
        while level.len() > 1 {
            level = level
                .chunks(self.fanout)
                .map(|chunk| self.store_node(Node::Internal(chunk.to_vec()), work))
                .collect::<Result<Vec<_>, _>>()?;
        }
        level.pop().ok_or("empty directory")
    }

    fn rightmost_pack(&self, root: &Summary, work: &mut Work) -> Result<Summary, &'static str> {
        let node = self.load_node(root, work)?;
        match node {
            Node::Leaf(packs) => packs.last().cloned().ok_or("empty leaf"),
            Node::Internal(children) => self.rightmost_pack(children.last().ok_or("empty internal")?, work),
        }
    }

    fn replace_rightmost(
        &mut self,
        root: &Summary,
        replacements: Vec<Summary>,
        work: &mut Work,
    ) -> Result<Summary, &'static str> {
        let nodes = self.replace_rightmost_inner(root, replacements, work)?;
        if nodes.len() == 1 {
            return Ok(nodes[0].clone());
        }
        self.store_node(Node::Internal(nodes), work)
    }

    fn replace_rightmost_inner(
        &mut self,
        root: &Summary,
        replacements: Vec<Summary>,
        work: &mut Work,
    ) -> Result<Vec<Summary>, &'static str> {
        let bytes = self.store.nodes.get(&root.id).ok_or("missing directory node")?.clone();
        let node = self.decode_expected_node(root, &bytes)?;
        let entries = match node {
            Node::Leaf(mut packs) => {
                packs.pop().ok_or("empty leaf")?;
                packs.extend(replacements);
                packs
            }
            Node::Internal(mut children) => {
                let child = children.pop().ok_or("empty internal")?;
                children.extend(self.replace_rightmost_inner(&child, replacements, work)?);
                children
            }
        };
        entries
            .chunks(self.fanout)
            .map(|chunk| {
                let node = if self.store.packs.contains_key(&chunk[0].id) {
                    Node::Leaf(chunk.to_vec())
                } else {
                    Node::Internal(chunk.to_vec())
                };
                self.store_node(node, work)
            })
            .collect()
    }

    fn query(&self, route: Route, first: &[u8], last: &[u8]) -> Result<(usize, Work), &'static str> {
        let (_, root) = self.selected.as_ref().ok_or("missing selected root")?;
        let mut work = Work::default();
        let mut packs = Vec::new();
        self.find_packs(&root.directory, route, first, last, &mut packs, &mut work)?;
        let mut matches = 0;
        for summary in packs {
            let pack = self.load_pack(&summary, &mut work)?;
            matches += match route {
                Route::Commit => pack
                    .entries
                    .iter()
                    .filter(|entry| in_range(&commit_key(entry), first, last))
                    .count(),
                Route::Change => pack
                    .change_order
                    .iter()
                    .filter(|ordinal| in_range(&change_key(&pack.entries[**ordinal as usize]), first, last))
                    .count(),
                Route::Schema => pack
                    .schema_order
                    .iter()
                    .filter(|ordinal| in_range(&schema_key(&pack.entries[**ordinal as usize]), first, last))
                    .count(),
            };
        }
        Ok((matches, work))
    }

    fn find_packs(
        &self,
        root: &Summary,
        route: Route,
        first: &[u8],
        last: &[u8],
        packs: &mut Vec<Summary>,
        work: &mut Work,
    ) -> Result<(), &'static str> {
        let node = self.load_node(root, work)?;
        for child in node_children(&node) {
            let (min, max) = route_bounds(child, route);
            if max.as_slice() < first || min.as_slice() > last {
                continue;
            }
            if self.store.packs.contains_key(&child.id) {
                packs.push(child.clone());
            } else {
                self.find_packs(child, route, first, last, packs, work)?;
            }
        }
        Ok(())
    }

    fn full_verify(&self) -> Result<(), &'static str> {
        let (root_id, root) = self.selected.as_ref().ok_or("missing selected root")?;
        if digest(ROOT_DOMAIN, &encode_root(root)) != *root_id {
            return Err("repository root substitution");
        }
        if root.target_entries as usize != self.target
            || root.entry_set_digest != root.directory.entry_set_digest
        {
            return Err("repository geometry/digest mismatch");
        }
        let mut seen_nodes = HashSet::new();
        let mut seen_packs = HashSet::new();
        let mut entry_ids = BTreeSet::new();
        self.verify_node(
            &root.directory,
            &mut seen_nodes,
            &mut seen_packs,
            &mut entry_ids,
        )?;
        if entry_ids.len() as u64 != root.directory.entries {
            return Err("entry set completeness mismatch");
        }
        Ok(())
    }

    fn verify_node(
        &self,
        root: &Summary,
        seen_nodes: &mut HashSet<Id>,
        seen_packs: &mut HashSet<Id>,
        entry_ids: &mut BTreeSet<Id>,
    ) -> Result<(), &'static str> {
        if !seen_nodes.insert(root.id) {
            return Err("directory cycle/duplicate node");
        }
        let bytes = self.store.nodes.get(&root.id).ok_or("missing directory node")?;
        let node = self.decode_expected_node(root, bytes)?;
        for child in node_children(&node) {
            if self.store.packs.contains_key(&child.id) {
                if !seen_packs.insert(child.id) {
                    return Err("duplicate pack");
                }
                let pack_bytes = self.store.packs.get(&child.id).ok_or("missing pack")?;
                if digest(PACK_DOMAIN, pack_bytes) != child.id {
                    return Err("pack substitution");
                }
                let pack = decode_pack(pack_bytes)?;
                if pack_summary(&pack, child.id) != *child {
                    return Err("pack summary mismatch");
                }
                for entry in &pack.entries {
                    if !entry_ids.insert(entry_id(entry)) {
                        return Err("duplicate entry");
                    }
                }
            } else {
                self.verify_node(child, seen_nodes, seen_packs, entry_ids)?;
            }
        }
        Ok(())
    }

    fn settled_bytes(&self) -> u64 {
        let Some((_, root)) = &self.selected else { return 0 };
        let mut seen = HashSet::new();
        let mut total = encode_root(root).len() as u64;
        self.reachable_bytes(&root.directory, &mut seen, &mut total);
        total
    }

    fn reachable_bytes(&self, root: &Summary, seen: &mut HashSet<Id>, total: &mut u64) {
        if !seen.insert(root.id) {
            return;
        }
        if let Some(bytes) = self.store.packs.get(&root.id) {
            *total += bytes.len() as u64;
            return;
        }
        let bytes = &self.store.nodes[&root.id];
        *total += bytes.len() as u64;
        let node = decode_node(bytes).unwrap();
        for child in node_children(&node) {
            self.reachable_bytes(child, seen, total);
        }
    }
}

fn canonical_pack(entries: Vec<Entry>) -> Result<Pack, &'static str> {
    if entries.is_empty()
        || entries.windows(2).any(|pair| commit_key(&pair[0]) >= commit_key(&pair[1]))
    {
        return Err("noncanonical pack entries");
    }
    let mut change_order = (0..entries.len() as u16).collect::<Vec<_>>();
    change_order.sort_unstable_by_key(|ordinal| change_key(&entries[*ordinal as usize]));
    let mut schema_order = (0..entries.len() as u16).collect::<Vec<_>>();
    schema_order.sort_unstable_by_key(|ordinal| schema_key(&entries[*ordinal as usize]));
    if change_order
        .windows(2)
        .any(|pair| change_key(&entries[pair[0] as usize]) >= change_key(&entries[pair[1] as usize]))
        || schema_order
            .windows(2)
            .any(|pair| schema_key(&entries[pair[0] as usize]) >= schema_key(&entries[pair[1] as usize]))
    {
        return Err("duplicate secondary coordinate");
    }
    Ok(Pack {
        entries,
        change_order,
        schema_order,
    })
}

fn pack_summary(pack: &Pack, id: Id) -> Summary {
    let commits = pack.entries.iter().map(commit_key).collect::<Vec<_>>();
    let changes = pack.entries.iter().map(change_key).collect::<Vec<_>>();
    let schemas = pack.entries.iter().map(schema_key).collect::<Vec<_>>();
    let owners = pack.entries.iter().map(owner_key).collect::<Vec<_>>();
    let ids = pack.entries.iter().map(entry_id).collect::<Vec<_>>();
    Summary {
        id,
        packs: 1,
        entries: pack.entries.len() as u64,
        commit_min: commits.iter().min().unwrap().clone(),
        commit_max: commits.iter().max().unwrap().clone(),
        change_min: changes.iter().min().unwrap().clone(),
        change_max: changes.iter().max().unwrap().clone(),
        schema_min: schemas.iter().min().unwrap().clone(),
        schema_max: schemas.iter().max().unwrap().clone(),
        owner_min: owners.iter().min().unwrap().clone(),
        owner_max: owners.iter().max().unwrap().clone(),
        entry_set_digest: digest_ids(ids),
    }
}

fn node_summary(node: &Node, id: Id) -> Summary {
    let children = node_children(node);
    Summary {
        id,
        packs: children.iter().map(|child| child.packs).sum(),
        entries: children.iter().map(|child| child.entries).sum(),
        commit_min: children.first().unwrap().commit_min.clone(),
        commit_max: children.last().unwrap().commit_max.clone(),
        change_min: children.iter().map(|child| &child.change_min).min().unwrap().clone(),
        change_max: children.iter().map(|child| &child.change_max).max().unwrap().clone(),
        schema_min: children.iter().map(|child| &child.schema_min).min().unwrap().clone(),
        schema_max: children.iter().map(|child| &child.schema_max).max().unwrap().clone(),
        owner_min: children.iter().map(|child| &child.owner_min).min().unwrap().clone(),
        owner_max: children.iter().map(|child| &child.owner_max).max().unwrap().clone(),
        entry_set_digest: digest_child_sets(children),
    }
}

fn validate_node(node: &Node, fanout: usize) -> Result<(), &'static str> {
    let children = node_children(node);
    if children.is_empty() || children.len() > fanout {
        return Err("invalid directory fanout");
    }
    validate_summary_order(children)
}

fn validate_summary_order(children: &[Summary]) -> Result<(), &'static str> {
    if children.windows(2).any(|pair| pair[0].commit_max >= pair[1].commit_min) {
        return Err("directory overlap/order");
    }
    Ok(())
}

fn node_children(node: &Node) -> &[Summary] {
    match node {
        Node::Leaf(children) | Node::Internal(children) => children,
    }
}

fn route_bounds(summary: &Summary, route: Route) -> (&Vec<u8>, &Vec<u8>) {
    match route {
        Route::Commit => (&summary.commit_min, &summary.commit_max),
        Route::Change => (&summary.change_min, &summary.change_max),
        Route::Schema => (&summary.schema_min, &summary.schema_max),
    }
}

fn in_range(key: &[u8], first: &[u8], last: &[u8]) -> bool {
    first <= key && key <= last
}

fn digest_ids(ids: impl IntoIterator<Item = Id>) -> Id {
    let mut framed = Vec::new();
    for id in ids {
        framed.extend_from_slice(&id);
    }
    digest(ENTRY_SET_DOMAIN, &framed)
}

fn digest_child_sets(children: &[Summary]) -> Id {
    let mut framed = Vec::new();
    for child in children {
        framed.extend_from_slice(&child.entries.to_be_bytes());
        framed.extend_from_slice(&child.entry_set_digest);
    }
    digest(ENTRY_SET_DOMAIN, &framed)
}

fn digest(domain: &str, bytes: &[u8]) -> Id {
    let mut hasher = blake3::Hasher::new_derive_key(domain);
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

fn entry_id(entry: &Entry) -> Id {
    digest(ENTRY_DOMAIN, &encode_entry(entry))
}

fn owner_key(entry: &Entry) -> Vec<u8> {
    let mut out = vec![entry.owner_domain];
    out.extend_from_slice(&entry.owner_id);
    out
}

fn commit_key(entry: &Entry) -> Vec<u8> {
    let mut out = entry.commit_id.to_vec();
    out.push(entry.owner_domain);
    out.extend_from_slice(&entry.segment_ordinal.to_be_bytes());
    out
}

fn change_key(entry: &Entry) -> Vec<u8> {
    let mut out = entry.first_change_id.to_vec();
    out.push(entry.owner_domain);
    out.extend_from_slice(&entry.segment_ordinal.to_be_bytes());
    out
}

fn schema_key(entry: &Entry) -> Vec<u8> {
    let mut out = Vec::new();
    put_bytes(&mut out, &entry.schema);
    put_bytes(&mut out, &entry.first_key);
    out.extend_from_slice(&entry.owner_id);
    out.extend_from_slice(&entry.segment_ordinal.to_be_bytes());
    out
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

fn take_bytes<'a>(input: &mut &'a [u8]) -> Result<&'a [u8], &'static str> {
    let len = u32::from_be_bytes(input.get(..4).ok_or("truncated length")?.try_into().unwrap()) as usize;
    *input = &input[4..];
    let value = input.get(..len).ok_or("truncated bytes")?;
    *input = &input[len..];
    Ok(value)
}

fn encode_entry(entry: &Entry) -> Vec<u8> {
    let mut out = vec![entry.owner_domain];
    out.extend_from_slice(&entry.owner_id);
    out.extend_from_slice(&entry.commit_id);
    out.extend_from_slice(&entry.generation.to_be_bytes());
    out.extend_from_slice(&entry.segment_ordinal.to_be_bytes());
    out.extend_from_slice(&entry.first_change_id);
    out.extend_from_slice(&entry.last_change_id);
    put_bytes(&mut out, &entry.schema);
    put_bytes(&mut out, &entry.first_key);
    put_bytes(&mut out, &entry.last_key);
    out.extend_from_slice(&entry.segment_space.to_be_bytes());
    put_bytes(&mut out, &entry.segment_key);
    out.extend_from_slice(&entry.segment_digest);
    out.extend_from_slice(&entry.member_count.to_be_bytes());
    out.extend_from_slice(&entry.member_ids_digest);
    out
}

fn decode_entry(input: &mut &[u8]) -> Result<Entry, &'static str> {
    let owner_domain = *input.first().ok_or("truncated owner domain")?;
    *input = &input[1..];
    let owner_id = input.get(..16).ok_or("truncated owner")?.try_into().unwrap();
    *input = &input[16..];
    let commit_id = input.get(..16).ok_or("truncated commit")?.try_into().unwrap();
    *input = &input[16..];
    let generation = u64::from_be_bytes(input.get(..8).ok_or("truncated generation")?.try_into().unwrap());
    *input = &input[8..];
    let segment_ordinal = u32::from_be_bytes(input.get(..4).ok_or("truncated ordinal")?.try_into().unwrap());
    *input = &input[4..];
    let first_change_id = input.get(..16).ok_or("truncated first change")?.try_into().unwrap();
    *input = &input[16..];
    let last_change_id = input.get(..16).ok_or("truncated last change")?.try_into().unwrap();
    *input = &input[16..];
    let schema = take_bytes(input)?.to_vec();
    let first_key = take_bytes(input)?.to_vec();
    let last_key = take_bytes(input)?.to_vec();
    let segment_space = u32::from_be_bytes(input.get(..4).ok_or("truncated space")?.try_into().unwrap());
    *input = &input[4..];
    let segment_key = take_bytes(input)?.to_vec();
    let segment_digest = input.get(..32).ok_or("truncated segment digest")?.try_into().unwrap();
    *input = &input[32..];
    let member_count = u32::from_be_bytes(input.get(..4).ok_or("truncated member count")?.try_into().unwrap());
    *input = &input[4..];
    let member_ids_digest = input.get(..32).ok_or("truncated member digest")?.try_into().unwrap();
    *input = &input[32..];
    Ok(Entry { owner_domain, owner_id, commit_id, generation, segment_ordinal, first_change_id, last_change_id, schema, first_key, last_key, segment_space, segment_key, segment_digest, member_count, member_ids_digest })
}

fn encode_pack(pack: &Pack) -> Vec<u8> {
    let mut out = vec![1];
    out.extend_from_slice(&(pack.entries.len() as u16).to_be_bytes());
    for entry in &pack.entries {
        put_bytes(&mut out, &encode_entry(entry));
    }
    for order in [&pack.change_order, &pack.schema_order] {
        out.extend_from_slice(&(order.len() as u16).to_be_bytes());
        for ordinal in order {
            out.extend_from_slice(&ordinal.to_be_bytes());
        }
    }
    out
}

fn decode_pack(mut input: &[u8]) -> Result<Pack, &'static str> {
    if input.first() != Some(&1) {
        return Err("wrong pack version");
    }
    input = &input[1..];
    let count = u16::from_be_bytes(input.get(..2).ok_or("truncated pack count")?.try_into().unwrap()) as usize;
    input = &input[2..];
    if count == 0 {
        return Err("empty pack");
    }
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let mut encoded = take_bytes(&mut input)?;
        let entry = decode_entry(&mut encoded)?;
        if !encoded.is_empty() {
            return Err("trailing entry bytes");
        }
        entries.push(entry);
    }
    let mut orders = Vec::new();
    for _ in 0..2 {
        let order_count = u16::from_be_bytes(input.get(..2).ok_or("truncated order count")?.try_into().unwrap()) as usize;
        input = &input[2..];
        if order_count != count {
            return Err("offset count mismatch");
        }
        let mut order = Vec::with_capacity(count);
        for _ in 0..count {
            let ordinal = u16::from_be_bytes(input.get(..2).ok_or("truncated offset")?.try_into().unwrap());
            input = &input[2..];
            if ordinal as usize >= count {
                return Err("offset outside pack");
            }
            order.push(ordinal);
        }
        if order.iter().copied().collect::<BTreeSet<_>>().len() != count {
            return Err("offset alias/omission");
        }
        orders.push(order);
    }
    if !input.is_empty() {
        return Err("trailing pack bytes");
    }
    let canonical = canonical_pack(entries)?;
    if canonical.change_order != orders[0] || canonical.schema_order != orders[1] {
        return Err("noncanonical secondary offsets");
    }
    Ok(canonical)
}

fn encode_summary(out: &mut Vec<u8>, summary: &Summary) {
    out.extend_from_slice(&summary.id);
    out.extend_from_slice(&summary.packs.to_be_bytes());
    out.extend_from_slice(&summary.entries.to_be_bytes());
    for value in [&summary.commit_min, &summary.commit_max, &summary.change_min, &summary.change_max, &summary.schema_min, &summary.schema_max, &summary.owner_min, &summary.owner_max] {
        put_bytes(out, value);
    }
    out.extend_from_slice(&summary.entry_set_digest);
}

fn decode_summary(input: &mut &[u8]) -> Result<Summary, &'static str> {
    let id = input.get(..32).ok_or("truncated summary id")?.try_into().unwrap();
    *input = &input[32..];
    let packs = u64::from_be_bytes(input.get(..8).ok_or("truncated pack count")?.try_into().unwrap());
    *input = &input[8..];
    let entries = u64::from_be_bytes(input.get(..8).ok_or("truncated entry count")?.try_into().unwrap());
    *input = &input[8..];
    let commit_min = take_bytes(input)?.to_vec(); let commit_max = take_bytes(input)?.to_vec();
    let change_min = take_bytes(input)?.to_vec(); let change_max = take_bytes(input)?.to_vec();
    let schema_min = take_bytes(input)?.to_vec(); let schema_max = take_bytes(input)?.to_vec();
    let owner_min = take_bytes(input)?.to_vec(); let owner_max = take_bytes(input)?.to_vec();
    let entry_set_digest = input.get(..32).ok_or("truncated entry set digest")?.try_into().unwrap();
    *input = &input[32..];
    Ok(Summary { id, packs, entries, commit_min, commit_max, change_min, change_max, schema_min, schema_max, owner_min, owner_max, entry_set_digest })
}

fn encode_node(node: &Node) -> Vec<u8> {
    let (tag, children) = match node { Node::Leaf(children) => (1, children), Node::Internal(children) => (2, children) };
    let mut out = vec![tag];
    out.extend_from_slice(&(children.len() as u16).to_be_bytes());
    for child in children { encode_summary(&mut out, child); }
    out
}

fn decode_node(mut input: &[u8]) -> Result<Node, &'static str> {
    let tag = *input.first().ok_or("truncated node tag")?;
    input = &input[1..];
    let count = u16::from_be_bytes(input.get(..2).ok_or("truncated node count")?.try_into().unwrap()) as usize;
    input = &input[2..];
    let mut children = Vec::with_capacity(count);
    for _ in 0..count { children.push(decode_summary(&mut input)?); }
    if !input.is_empty() { return Err("trailing node bytes") }
    match tag { 1 => Ok(Node::Leaf(children)), 2 => Ok(Node::Internal(children)), _ => Err("wrong node tag") }
}

fn encode_root(root: &RepositoryRoot) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&root.generation.to_be_bytes());
    out.extend_from_slice(&root.target_entries.to_be_bytes());
    encode_summary(&mut out, &root.directory);
    out.extend_from_slice(&root.entry_set_digest);
    out
}

fn fixture(index: u64) -> Entry {
    let mut id = [0; 16]; id[8..].copy_from_slice(&index.to_be_bytes());
    let schema = format!("schema-{}", index % 16).into_bytes();
    let key = format!("schema-{}/key-{index:012}", index % 16).into_bytes();
    Entry {
        owner_domain: (index % 4) as u8 + 1,
        owner_id: id,
        commit_id: id,
        generation: index + 1,
        segment_ordinal: 0,
        first_change_id: id,
        last_change_id: id,
        schema,
        first_key: key.clone(),
        last_key: key,
        segment_space: 0x40004,
        segment_key: id.to_vec(),
        segment_digest: digest("segment", &id),
        member_count: 64,
        member_ids_digest: digest("members", &id),
    }
}

#[test]
fn commit_segment_pack_crossover_and_corruption() {
    for target in [64_usize, 128, 256] {
        for history in [1_000_u64, 10_000, 50_000] {
            let initial = (0..history).map(fixture).collect::<Vec<_>>();
            let flat_bytes = initial.iter().map(|entry| encode_entry(entry).len() as u64).sum::<u64>();
            let mut directory = Directory::new(target, 64);
            let build = directory.publish_cas(0, &initial).unwrap();
            directory.full_verify().unwrap();
            let settled = directory.settled_bytes();
            let exact_key = change_key(initial.last().unwrap());
            let (exact_rows, exact) = directory.query(Route::Change, &exact_key, &exact_key).unwrap();
            assert_eq!(exact_rows, 1);
            let range_first = change_key(&initial[(history - 51) as usize]);
            let (range_rows, range) = directory.query(Route::Change, &range_first, &exact_key).unwrap();
            assert_eq!(range_rows, 51);
            let commit_exact_key = commit_key(initial.last().unwrap());
            assert_eq!(directory.query(Route::Commit, &commit_exact_key, &commit_exact_key).unwrap().0, 1);
            let schema_exact_key = schema_key(initial.last().unwrap());
            assert_eq!(directory.query(Route::Schema, &schema_exact_key, &schema_exact_key).unwrap().0, 1);
            let schema = format!("schema-{}", (history - 1) % 16).into_bytes();
            let mut schema_first = Vec::new();
            put_bytes(&mut schema_first, &schema);
            schema_first.extend_from_slice(&0_u32.to_be_bytes());
            let mut schema_last = Vec::new();
            put_bytes(&mut schema_last, &schema);
            schema_last.extend_from_slice(&[0xff; 64]);
            let (schema_rows, schema_range) = directory
                .query(Route::Schema, &schema_first, &schema_last)
                .unwrap();
            assert_eq!(
                schema_rows,
                initial.iter().filter(|entry| entry.schema == schema).count()
            );
            let mut next = history;
            for delta in [1_u64, 10, (history / 100).max(1)] {
                let additions = (next..next + delta).map(fixture).collect::<Vec<_>>();
                let generation = directory.selected.as_ref().unwrap().1.generation;
                let update = directory.publish_cas(generation, &additions).unwrap();
                directory.full_verify().unwrap();
                eprintln!("HISTORY_PACK target={target} H={history} D={delta} build_writes={} build_bytes={} settled_bytes={settled} flat_bytes={flat_bytes} update_reads={} update_read_bytes={} update_writes={} update_write_bytes={} exact_reads={} exact_bytes={} range51_reads={} range51_bytes={} schema_rows={} schema_reads={} schema_bytes={}", build.writes, build.write_bytes, update.reads, update.read_bytes, update.writes, update.write_bytes, exact.reads, exact.read_bytes, range.reads, range.read_bytes, schema_rows, schema_range.reads, schema_range.read_bytes);
                next += delta;
            }
        }
    }

    let initial = (0..128).map(fixture).collect::<Vec<_>>();
    let mut reversed = initial.clone(); reversed.reverse();
    let mut forward = Directory::new(64, 64); forward.publish_cas(0, &initial).unwrap();
    let mut reverse = Directory::new(64, 64); reverse.publish_cas(0, &reversed).unwrap();
    assert_eq!(forward.selected, reverse.selected);

    let selected = forward.selected.clone();
    assert_eq!(forward.publish_cas(0, &[fixture(1_000)]), Err("stale repository root"));
    assert_eq!(forward.selected, selected);
    let mut staged = forward.clone(); staged.publish_cas(1, &[fixture(1_000)]).unwrap();
    assert!(forward.full_verify().is_ok()); assert!(staged.full_verify().is_ok());

    let pack_id = *forward.store.packs.keys().next().unwrap();
    let original = forward.store.packs[&pack_id].clone();
    forward.store.packs.get_mut(&pack_id).unwrap()[5] ^= 1;
    assert!(forward.full_verify().is_err());
    forward.store.packs.insert(pack_id, original.clone());
    let removed = forward.store.packs.remove(&pack_id).unwrap();
    assert_eq!(forward.full_verify(), Err("missing directory node"));
    forward.store.packs.insert(pack_id, removed);
    let mut malformed = decode_pack(&original).unwrap();
    malformed.change_order[1] = malformed.change_order[0];
    assert_eq!(decode_pack(&encode_pack(&malformed)), Err("offset alias/omission"));
    let mut noncanonical_offsets = decode_pack(&original).unwrap();
    noncanonical_offsets.schema_order.swap(0, 1);
    assert_eq!(
        decode_pack(&encode_pack(&noncanonical_offsets)),
        Err("noncanonical secondary offsets")
    );
    let duplicate = fixture(2_000);
    assert_eq!(
        canonical_pack(vec![duplicate.clone(), duplicate]),
        Err("noncanonical pack entries")
    );
    let mut wrong_owner = decode_pack(&original).unwrap();
    wrong_owner.entries[0].owner_id[0] ^= 1;
    forward.store.packs.insert(pack_id, encode_pack(&wrong_owner));
    assert_eq!(forward.full_verify(), Err("pack substitution"));
    forward.store.packs.insert(pack_id, original);
    assert!(forward.full_verify().is_ok());
}
