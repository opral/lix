//! Test-only model of a canonical authenticated common history-entry set.

use std::collections::{BTreeSet, HashMap, HashSet};

type Id = [u8; 32];

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u8)]
enum Domain {
    EntrySet = 1,
    CommitRoute = 2,
    ChangeRoute = 3,
    OwnerRoute = 4,
    OwnerSet = 5,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Summary {
    id: Id,
    count: u64,
    min: Vec<u8>,
    max: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Node {
    key: Vec<u8>,
    value: Vec<u8>,
    priority: Id,
    left: Option<Summary>,
    right: Option<Summary>,
}

#[derive(Clone, Debug)]
struct Entry {
    owner_domain: u8,
    owner_id: [u8; 16],
    commit_id: [u8; 16],
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
    nodes: HashMap<(Domain, Id), Vec<u8>>,
    entries: HashMap<Id, Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RepositoryRoot {
    generation: u64,
    entry_set: Option<Summary>,
    commit_route: Option<Summary>,
    change_route: Option<Summary>,
    owner_route: Option<Summary>,
    entry_count: u64,
    owner_count: u64,
}

#[derive(Clone)]
struct Accumulator {
    store: Store,
    selected: Option<(Id, RepositoryRoot)>,
    owner_sets: HashMap<Vec<u8>, Option<Summary>>,
}

impl Accumulator {
    fn new() -> Self {
        Self {
            store: Store::default(),
            selected: None,
            owner_sets: HashMap::new(),
        }
    }

    fn publish(&mut self, entries: &[Entry]) -> Work {
        let prior = self.selected.as_ref().map(|(_, root)| root.clone());
        let mut root = prior.clone().unwrap_or(RepositoryRoot {
            generation: 0,
            entry_set: None,
            commit_route: None,
            change_route: None,
            owner_route: None,
            entry_count: 0,
            owner_count: 0,
        });
        let mut work = Work::default();
        for (entry_index, entry) in entries.iter().enumerate() {
            let bytes = encode_entry(entry);
            let entry_id = digest("entry", &bytes);
            if self.store.entries.insert(entry_id, bytes.clone()).is_some() {
                panic!("duplicate entry identity");
            }
            work.writes += 1;
            work.write_bytes += bytes.len() as u64;
            root.entry_set = self.put(
                Domain::EntrySet,
                root.entry_set,
                entry_id.to_vec(),
                entry_id.to_vec(),
                false,
                &mut work,
            );
            root.commit_route = self.put(
                Domain::CommitRoute,
                root.commit_route,
                commit_key(entry),
                entry_id.to_vec(),
                false,
                &mut work,
            );
            root.change_route = self.put(
                Domain::ChangeRoute,
                root.change_route,
                change_key(entry),
                entry_id.to_vec(),
                false,
                &mut work,
            );

            let owner_key = owner_key(entry);
            let owner_root = self.owner_sets.get(&owner_key).cloned().flatten();
            let owner_root = self.put(
                Domain::OwnerSet,
                owner_root,
                entry_id.to_vec(),
                entry_id.to_vec(),
                false,
                &mut work,
            );
            let owner_root = owner_root.expect("one owner member has a root");
            let new_owner = self.owner_sets.insert(owner_key.clone(), Some(owner_root.clone())).is_none();
            root.owner_route = self.put(
                Domain::OwnerRoute,
                root.owner_route,
                owner_key,
                encode_owner_inventory(&owner_root),
                true,
                &mut work,
            );
            root.entry_count += 1;
            root.owner_count += u64::from(new_owner);

            // Model physical GC at bounded maintenance opportunities so the
            // crossover counts immutable writes without retaining obsolete
            // path-copy versions forever in process memory.
            if entry_index % 4_096 == 4_095 {
                self.prune_nodes(&root);
            }
        }
        root.generation += 1;
        assert_eq!(root.entry_set.as_ref().map_or(0, |root| root.count), root.entry_count);
        assert_eq!(root.commit_route.as_ref().map_or(0, |root| root.count), root.entry_count);
        assert_eq!(root.change_route.as_ref().map_or(0, |root| root.count), root.entry_count);
        assert_eq!(root.owner_route.as_ref().map_or(0, |root| root.count), root.owner_count);
        let bytes = encode_repository_root(&root);
        let id = digest("repository-root", &bytes);
        work.writes += 1;
        work.write_bytes += bytes.len() as u64;
        self.selected = Some((id, root));
        work
    }

    fn publish_cas(
        &mut self,
        expected_generation: u64,
        entries: &[Entry],
    ) -> Result<Work, &'static str> {
        let actual_generation = self.selected.as_ref().map_or(0, |(_, root)| root.generation);
        if actual_generation != expected_generation {
            return Err("stale repository root");
        }
        Ok(self.publish(entries))
    }

    fn put(
        &mut self,
        domain: Domain,
        root: Option<Summary>,
        key: Vec<u8>,
        value: Vec<u8>,
        replace: bool,
        work: &mut Work,
    ) -> Option<Summary> {
        match root {
            None => Some(self.store_node(domain, Node {
                priority: priority(domain, &key),
                key,
                value,
                left: None,
                right: None,
            }, work)),
            Some(summary) => {
                let node = self.load_node(domain, &summary, work);
                if key == node.key {
                    assert!(replace, "duplicate route key");
                    return Some(self.store_node(domain, Node { value, ..node }, work));
                }
                let candidate_priority = priority(domain, &key);
                if candidate_priority < node.priority {
                    let (left, right) = self.split(domain, Some(summary), &key, work);
                    return Some(self.store_node(domain, Node {
                        key,
                        value,
                        priority: candidate_priority,
                        left,
                        right,
                    }, work));
                }
                if key < node.key {
                    let left = self.put(domain, node.left.clone(), key, value, replace, work);
                    Some(self.store_node(domain, Node { left, ..node }, work))
                } else {
                    let right = self.put(domain, node.right.clone(), key, value, replace, work);
                    Some(self.store_node(domain, Node { right, ..node }, work))
                }
            }
        }
    }

    fn split(
        &mut self,
        domain: Domain,
        root: Option<Summary>,
        key: &[u8],
        work: &mut Work,
    ) -> (Option<Summary>, Option<Summary>) {
        let Some(summary) = root else { return (None, None) };
        let node = self.load_node(domain, &summary, work);
        if node.key.as_slice() < key {
            let (middle, right) = self.split(domain, node.right.clone(), key, work);
            let left = Some(self.store_node(domain, Node { right: middle, ..node }, work));
            (left, right)
        } else {
            let (left, middle) = self.split(domain, node.left.clone(), key, work);
            let right = Some(self.store_node(domain, Node { left: middle, ..node }, work));
            (left, right)
        }
    }

    fn store_node(&mut self, domain: Domain, node: Node, work: &mut Work) -> Summary {
        validate_local(domain, &node).expect("canonical node");
        let bytes = encode_node(&node);
        let id = node_id(domain, &bytes);
        if let Some(existing) = self.store.nodes.get(&(domain, id)) {
            assert_eq!(existing, &bytes);
        } else {
            work.writes += 1;
            work.write_bytes += bytes.len() as u64;
            self.store.nodes.insert((domain, id), bytes);
        }
        summary(&node, id)
    }

    fn load_node(&self, domain: Domain, expected: &Summary, work: &mut Work) -> Node {
        let bytes = self.store.nodes.get(&(domain, expected.id)).expect("missing authenticated node");
        work.reads += 1;
        work.read_bytes += bytes.len() as u64;
        assert_eq!(node_id(domain, bytes), expected.id, "substituted node");
        let node = decode_node(bytes).expect("malformed node");
        validate_local(domain, &node).expect("invalid node");
        assert_eq!(summary(&node, expected.id), *expected, "forged child summary");
        node
    }

    fn exact(&self, domain: Domain, mut root: Option<Summary>, key: &[u8]) -> (Option<Vec<u8>>, Work) {
        let mut work = Work::default();
        while let Some(summary) = root {
            let node = self.load_node(domain, &summary, &mut work);
            match key.cmp(node.key.as_slice()) {
                std::cmp::Ordering::Less => root = node.left,
                std::cmp::Ordering::Greater => root = node.right,
                std::cmp::Ordering::Equal => return (Some(node.value), work),
            }
        }
        (None, work)
    }

    fn verify_entry(&self, entry_id: Id, owner: &[u8], commit: &[u8], change: &[u8]) -> Work {
        let root = &self.selected.as_ref().expect("selected root").1;
        let mut work = Work::default();
        let (commit_value, commit_work) = self.exact(Domain::CommitRoute, root.commit_route.clone(), commit);
        work += commit_work;
        let (change_value, change_work) = self.exact(Domain::ChangeRoute, root.change_route.clone(), change);
        work += change_work;
        let (set_value, set_work) = self.exact(Domain::EntrySet, root.entry_set.clone(), &entry_id);
        work += set_work;
        let (owner_value, owner_work) = self.exact(Domain::OwnerRoute, root.owner_route.clone(), owner);
        work += owner_work;
        let owner_value = owner_value.expect("owner inventory");
        let owner_root = decode_owner_inventory(&owner_value).expect("owner inventory encoding");
        let (owner_member, owner_set_work) = self.exact(Domain::OwnerSet, Some(owner_root), &entry_id);
        work += owner_set_work;
        assert_eq!(commit_value.as_deref(), Some(entry_id.as_slice()));
        assert_eq!(change_value.as_deref(), Some(entry_id.as_slice()));
        assert_eq!(set_value.as_deref(), Some(entry_id.as_slice()));
        assert_eq!(owner_member.as_deref(), Some(entry_id.as_slice()));
        let bytes = self.store.entries.get(&entry_id).expect("entry payload");
        assert_eq!(digest("entry", bytes), entry_id);
        work.reads += 1;
        work.read_bytes += bytes.len() as u64;
        work
    }

    fn full_verify(&self) -> Result<(), &'static str> {
        let (selected_id, root) = self.selected.as_ref().ok_or("missing repository root")?;
        if digest("repository-root", &encode_repository_root(root)) != *selected_id {
            return Err("repository root substitution");
        }
        let entry_ids = self.collect(Domain::EntrySet, root.entry_set.as_ref())?;
        let commit_ids = self.collect(Domain::CommitRoute, root.commit_route.as_ref())?;
        let change_ids = self.collect(Domain::ChangeRoute, root.change_route.as_ref())?;
        if entry_ids.len() as u64 != root.entry_count
            || commit_ids.len() != entry_ids.len()
            || change_ids.len() != entry_ids.len()
            || commit_ids.values().cloned().collect::<BTreeSet<_>>() != entry_ids.values().cloned().collect()
            || change_ids.values().cloned().collect::<BTreeSet<_>>() != entry_ids.values().cloned().collect()
        {
            return Err("route entry-set mismatch");
        }
        for (id, value) in entry_ids {
            let id_array: Id = id.as_slice().try_into().map_err(|_| "invalid entry id")?;
            if id != value || self.store.entries.get(&id_array).is_none() {
                return Err("entry set payload mismatch");
            }
        }
        let owners = self.collect(Domain::OwnerRoute, root.owner_route.as_ref())?;
        if owners.len() as u64 != root.owner_count {
            return Err("owner count mismatch");
        }
        let mut owner_members = BTreeSet::new();
        let mut owner_member_count = 0_usize;
        for value in owners.values() {
            let owner_root = decode_owner_inventory(value)?;
            let members = self.collect(Domain::OwnerSet, Some(&owner_root))?;
            owner_member_count += members.len();
            owner_members.extend(members.into_values());
        }
        if owner_member_count != root.entry_count as usize
            || owner_members != commit_ids.into_values().collect()
        {
            return Err("owner inventory completeness mismatch");
        }
        Ok(())
    }

    fn collect(&self, domain: Domain, root: Option<&Summary>) -> Result<HashMap<Vec<u8>, Vec<u8>>, &'static str> {
        fn walk(
            store: &Store,
            domain: Domain,
            summary: &Summary,
            out: &mut HashMap<Vec<u8>, Vec<u8>>,
        ) -> Result<Node, &'static str> {
            let bytes = store.nodes.get(&(domain, summary.id)).ok_or("missing node")?;
            if node_id(domain, bytes) != summary.id { return Err("node substitution") }
            let node = decode_node(bytes)?;
            validate_local(domain, &node)?;
            if crate::tracked_state::history_accumulator_model::summary(&node, summary.id) != *summary { return Err("child summary mismatch") }
            if let Some(left) = &node.left {
                let child = walk(store, domain, left, out)?;
                if child.priority < node.priority { return Err("left heap order") }
            }
            if out.insert(node.key.clone(), node.value.clone()).is_some() { return Err("duplicate key") }
            if let Some(right) = &node.right {
                let child = walk(store, domain, right, out)?;
                if child.priority < node.priority { return Err("right heap order") }
            }
            Ok(node)
        }
        let mut out = HashMap::new();
        if let Some(root) = root { walk(&self.store, domain, root, &mut out)?; }
        Ok(out)
    }

    fn prune_nodes(&mut self, root: &RepositoryRoot) {
        fn mark(store: &Store, domain: Domain, root: Option<&Summary>, live: &mut HashSet<(Domain, Id)>) {
            let Some(root) = root else { return };
            if !live.insert((domain, root.id)) { return; }
            let node = decode_node(&store.nodes[&(domain, root.id)]).expect("reachable node");
            mark(store, domain, node.left.as_ref(), live);
            mark(store, domain, node.right.as_ref(), live);
        }

        let mut live = HashSet::new();
        mark(&self.store, Domain::EntrySet, root.entry_set.as_ref(), &mut live);
        mark(&self.store, Domain::CommitRoute, root.commit_route.as_ref(), &mut live);
        mark(&self.store, Domain::ChangeRoute, root.change_route.as_ref(), &mut live);
        mark(&self.store, Domain::OwnerRoute, root.owner_route.as_ref(), &mut live);
        for owner_root in self.owner_sets.values() {
            mark(&self.store, Domain::OwnerSet, owner_root.as_ref(), &mut live);
        }
        self.store.nodes.retain(|key, _| live.contains(key));
    }

    fn settled_bytes(&self) -> u64 {
        let Some((_, root)) = &self.selected else { return 0 };
        let mut seen = HashSet::new();
        let mut bytes = self.store.entries.values().map(|bytes| bytes.len() as u64).sum::<u64>();
        let mut roots = vec![
            (Domain::EntrySet, root.entry_set.clone()),
            (Domain::CommitRoute, root.commit_route.clone()),
            (Domain::ChangeRoute, root.change_route.clone()),
            (Domain::OwnerRoute, root.owner_route.clone()),
        ];
        roots.extend(self.owner_sets.values().cloned().map(|root| (Domain::OwnerSet, root)));
        for (domain, root) in roots {
            self.reachable_bytes(domain, root.as_ref(), &mut seen, &mut bytes);
        }
        bytes + encode_repository_root(root).len() as u64
    }

    fn reachable_bytes(&self, domain: Domain, root: Option<&Summary>, seen: &mut HashSet<(Domain, Id)>, bytes: &mut u64) {
        let Some(root) = root else { return };
        if !seen.insert((domain, root.id)) { return; }
        let encoded = &self.store.nodes[&(domain, root.id)];
        *bytes += encoded.len() as u64;
        let node = decode_node(encoded).unwrap();
        self.reachable_bytes(domain, node.left.as_ref(), seen, bytes);
        self.reachable_bytes(domain, node.right.as_ref(), seen, bytes);
    }
}

fn priority(domain: Domain, key: &[u8]) -> Id {
    let mut bytes = vec![domain as u8];
    bytes.extend_from_slice(key);
    digest("treap-priority", &bytes)
}

fn node_id(domain: Domain, bytes: &[u8]) -> Id {
    let mut framed = vec![domain as u8];
    framed.extend_from_slice(bytes);
    digest("treap-node", &framed)
}

fn digest(context: &str, bytes: &[u8]) -> Id {
    let mut hasher = blake3::Hasher::new_derive_key(context);
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

fn summary(node: &Node, id: Id) -> Summary {
    Summary {
        id,
        count: 1 + node.left.as_ref().map_or(0, |left| left.count) + node.right.as_ref().map_or(0, |right| right.count),
        min: node.left.as_ref().map_or_else(|| node.key.clone(), |left| left.min.clone()),
        max: node.right.as_ref().map_or_else(|| node.key.clone(), |right| right.max.clone()),
    }
}

fn validate_local(domain: Domain, node: &Node) -> Result<(), &'static str> {
    if node.priority != priority(domain, &node.key) { return Err("priority mismatch") }
    if node.left.as_ref().is_some_and(|left| left.max >= node.key || left.count == 0) { return Err("left order/count") }
    if node.right.as_ref().is_some_and(|right| right.min <= node.key || right.count == 0) { return Err("right order/count") }
    Ok(())
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) { out.extend_from_slice(&(bytes.len() as u32).to_be_bytes()); out.extend_from_slice(bytes); }
fn take_bytes<'a>(input: &mut &'a [u8]) -> Result<&'a [u8], &'static str> {
    let len = u32::from_be_bytes(input.get(..4).ok_or("truncated length")?.try_into().unwrap()) as usize;
    *input = &input[4..]; let bytes = input.get(..len).ok_or("truncated bytes")?; *input = &input[len..]; Ok(bytes)
}

fn encode_summary(out: &mut Vec<u8>, summary: &Option<Summary>) {
    match summary { None => out.push(0), Some(summary) => { out.push(1); out.extend_from_slice(&summary.id); out.extend_from_slice(&summary.count.to_be_bytes()); put_bytes(out, &summary.min); put_bytes(out, &summary.max); } }
}

fn decode_summary(input: &mut &[u8]) -> Result<Option<Summary>, &'static str> {
    let tag = *input.first().ok_or("truncated summary")?; *input = &input[1..];
    if tag == 0 { return Ok(None) } if tag != 1 { return Err("invalid summary tag") }
    let id = input.get(..32).ok_or("truncated summary id")?.try_into().unwrap(); *input = &input[32..];
    let count = u64::from_be_bytes(input.get(..8).ok_or("truncated summary count")?.try_into().unwrap()); *input = &input[8..];
    let min = take_bytes(input)?.to_vec(); let max = take_bytes(input)?.to_vec(); Ok(Some(Summary { id, count, min, max }))
}

fn encode_node(node: &Node) -> Vec<u8> {
    let mut out = Vec::new(); put_bytes(&mut out, &node.key); put_bytes(&mut out, &node.value); out.extend_from_slice(&node.priority); encode_summary(&mut out, &node.left); encode_summary(&mut out, &node.right); out
}

fn decode_node(mut input: &[u8]) -> Result<Node, &'static str> {
    let key = take_bytes(&mut input)?.to_vec(); let value = take_bytes(&mut input)?.to_vec();
    let priority = input.get(..32).ok_or("truncated priority")?.try_into().unwrap(); input = &input[32..];
    let left = decode_summary(&mut input)?; let right = decode_summary(&mut input)?;
    if !input.is_empty() { return Err("trailing node bytes") }
    Ok(Node { key, value, priority, left, right })
}

fn encode_entry(entry: &Entry) -> Vec<u8> {
    let mut out = vec![entry.owner_domain]; out.extend_from_slice(&entry.owner_id); out.extend_from_slice(&entry.commit_id); out.extend_from_slice(&entry.segment_ordinal.to_be_bytes()); out.extend_from_slice(&entry.first_change_id); out.extend_from_slice(&entry.last_change_id); put_bytes(&mut out, &entry.schema); put_bytes(&mut out, &entry.first_key); put_bytes(&mut out, &entry.last_key); out.extend_from_slice(&entry.segment_space.to_be_bytes()); put_bytes(&mut out, &entry.segment_key); out.extend_from_slice(&entry.segment_digest); out.extend_from_slice(&entry.member_count.to_be_bytes()); out.extend_from_slice(&entry.member_ids_digest); out
}

fn owner_key(entry: &Entry) -> Vec<u8> { let mut out = vec![entry.owner_domain]; out.extend_from_slice(&entry.owner_id); out }
fn commit_key(entry: &Entry) -> Vec<u8> { let mut out = vec![entry.owner_domain]; out.extend_from_slice(&entry.commit_id); out.extend_from_slice(&entry.segment_ordinal.to_be_bytes()); out }
fn change_key(entry: &Entry) -> Vec<u8> { let mut out = vec![entry.owner_domain]; out.extend_from_slice(&entry.first_change_id); out.extend_from_slice(&entry.segment_ordinal.to_be_bytes()); out }
fn encode_owner_inventory(root: &Summary) -> Vec<u8> { let mut out = Vec::new(); encode_summary(&mut out, &Some(root.clone())); out }
fn decode_owner_inventory(mut bytes: &[u8]) -> Result<Summary, &'static str> { let root = decode_summary(&mut bytes)?.ok_or("missing owner root")?; if !bytes.is_empty() { return Err("trailing owner inventory") } Ok(root) }

fn encode_repository_root(root: &RepositoryRoot) -> Vec<u8> {
    let mut out = Vec::new(); out.extend_from_slice(&root.generation.to_be_bytes()); encode_summary(&mut out, &root.entry_set); encode_summary(&mut out, &root.commit_route); encode_summary(&mut out, &root.change_route); encode_summary(&mut out, &root.owner_route); out.extend_from_slice(&root.entry_count.to_be_bytes()); out.extend_from_slice(&root.owner_count.to_be_bytes()); out
}

fn fixture(index: u64) -> Entry {
    let mut id = [0; 16]; id[8..].copy_from_slice(&index.to_be_bytes());
    let key = format!("key-{index:012}").into_bytes();
    Entry { owner_domain: 1, owner_id: id, commit_id: id, segment_ordinal: 0, first_change_id: id, last_change_id: id, schema: b"schema".to_vec(), first_key: key.clone(), last_key: key, segment_space: 0x40004, segment_key: id.to_vec(), segment_digest: digest("segment", &id), member_count: 64, member_ids_digest: digest("members", &id) }
}

#[test]
fn accumulator_crossover_and_authority_controls() {
    for history in [1_000_u64, 10_000, 50_000] {
        let mut acc = Accumulator::new();
        let build = acc.publish(&(0..history).map(fixture).collect::<Vec<_>>());
        acc.full_verify().unwrap();
        let settled = acc.settled_bytes();
        let mut next = history;
        for delta in [1_u64, 10, (history / 100).max(1)] {
            let additions = (next..next + delta).map(fixture).collect::<Vec<_>>();
            let update = acc.publish(&additions);
            acc.full_verify().unwrap();
            let target = additions.last().unwrap();
            let entry_id = digest("entry", &encode_entry(target));
            let proof = acc.verify_entry(entry_id, &owner_key(target), &commit_key(target), &change_key(target));
            let flat = (0..history).map(|index| encode_entry(&fixture(index)).len() as u64).sum::<u64>();
            eprintln!("HISTORY_ACCUM H={history} D={delta} build_writes={} build_bytes={} settled_bytes={settled} update_reads={} update_read_bytes={} update_writes={} update_write_bytes={} proof_reads={} proof_bytes={} flat_rewrite_bytes={flat}", build.writes, build.write_bytes, update.reads, update.read_bytes, update.writes, update.write_bytes, proof.reads, proof.read_bytes);
            next += delta;
        }
    }

    let canonical = (0..128).map(fixture).collect::<Vec<_>>();
    let mut reversed = canonical.clone();
    reversed.reverse();
    let mut forward_acc = Accumulator::new();
    forward_acc.publish(&canonical);
    let mut reverse_acc = Accumulator::new();
    reverse_acc.publish(&reversed);
    assert_eq!(forward_acc.selected, reverse_acc.selected);

    let mut acc = forward_acc;
    let selected = acc.selected.clone();
    let root = selected.as_ref().unwrap().1.entry_set.clone().unwrap();
    let original = acc.store.nodes[&(Domain::EntrySet, root.id)].clone();
    acc.store.nodes.get_mut(&(Domain::EntrySet, root.id)).unwrap()[0] ^= 1;
    assert!(acc.full_verify().is_err());
    acc.store.nodes.insert((Domain::EntrySet, root.id), original);
    assert!(acc.full_verify().is_ok());
    assert_eq!(acc.selected.as_ref().unwrap().0, selected.unwrap().0);

    let before_stale_cas = acc.selected.clone();
    assert_eq!(acc.publish_cas(0, &[fixture(1_000)]), Err("stale repository root"));
    assert_eq!(acc.selected, before_stale_cas);

    let mut crash_staging = acc.clone();
    crash_staging.publish_cas(1, &[fixture(1_000)]).unwrap();
    assert_ne!(crash_staging.selected, acc.selected);
    assert!(crash_staging.full_verify().is_ok());
    assert!(acc.full_verify().is_ok());

    let owner_root = acc.owner_sets.values().next().unwrap().clone().unwrap();
    let owner_node = acc.store.nodes.remove(&(Domain::OwnerSet, owner_root.id)).unwrap();
    assert_eq!(acc.full_verify(), Err("missing node"));
    acc.store.nodes.insert((Domain::OwnerSet, owner_root.id), owner_node);
    assert!(acc.full_verify().is_ok());
}
