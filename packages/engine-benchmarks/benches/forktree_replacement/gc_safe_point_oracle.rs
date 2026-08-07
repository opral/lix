use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Instant;

type ObjectId = u64;

#[derive(Clone)]
struct Object {
    generation: u64,
    bytes: u64,
    edges: Vec<ObjectId>,
}

#[derive(Default)]
struct Graph {
    objects: BTreeMap<ObjectId, Object>,
}

#[derive(Default)]
struct Closure {
    ids: BTreeSet<ObjectId>,
    bytes: u64,
    object_reads: u64,
    edge_reads: u64,
}

#[derive(Default)]
struct Attribution {
    closure_ids: usize,
    unique_ids: usize,
    unique_bytes: u64,
    object_reads: u64,
    edge_reads: u64,
}

struct Fixture {
    graph: Graph,
    current_root: ObjectId,
    reader_root: ObjectId,
    child_root: ObjectId,
    upload_root: ObjectId,
    reader_only_ids: Vec<ObjectId>,
    child_only_ids: Vec<ObjectId>,
    upload_only_ids: Vec<ObjectId>,
    abandoned_old_ids: Vec<ObjectId>,
    abandoned_new_ids: Vec<ObjectId>,
}

impl Graph {
    fn insert(&mut self, id: ObjectId, generation: u64, bytes: u64, edges: Vec<ObjectId>) {
        assert!(
            self.objects
                .insert(
                    id,
                    Object {
                        generation,
                        bytes,
                        edges,
                    },
                )
                .is_none(),
            "duplicate object"
        );
    }

    fn closure(&self, roots: &[ObjectId]) -> Result<Closure, String> {
        let mut result = Closure::default();
        let mut queue = VecDeque::from(roots.to_vec());
        while let Some(id) = queue.pop_front() {
            if !result.ids.insert(id) {
                continue;
            }
            let object = self
                .objects
                .get(&id)
                .ok_or_else(|| format!("missing selected object {id}"))?;
            result.bytes += object.bytes;
            result.object_reads += 1;
            result.edge_reads += object.edges.len() as u64;
            queue.extend(object.edges.iter().copied());
        }
        Ok(result)
    }

    fn delete_unmarked_before(
        &mut self,
        marked: &BTreeSet<ObjectId>,
        low_watermark: u64,
    ) -> (Vec<ObjectId>, Vec<ObjectId>, u64) {
        let mut deleted = Vec::new();
        let mut deferred = Vec::new();
        let mut deleted_bytes = 0;
        for (&id, object) in &self.objects {
            if marked.contains(&id) {
                continue;
            }
            if object.generation < low_watermark {
                deleted.push(id);
                deleted_bytes += object.bytes;
            } else {
                deferred.push(id);
            }
        }
        for id in &deleted {
            self.objects.remove(id);
        }
        (deleted, deferred, deleted_bytes)
    }
}

fn add_attribution(
    graph: &Graph,
    roots: &[ObjectId],
    already_owned: &mut BTreeSet<ObjectId>,
) -> Attribution {
    let closure = graph
        .closure(roots)
        .expect("authenticated selected closure");
    let unique = closure
        .ids
        .difference(already_owned)
        .copied()
        .collect::<Vec<_>>();
    let unique_bytes = unique
        .iter()
        .map(|id| graph.objects.get(id).expect("attributed object").bytes)
        .sum();
    already_owned.extend(closure.ids.iter().copied());
    Attribution {
        closure_ids: closure.ids.len(),
        unique_ids: unique.len(),
        unique_bytes,
        object_reads: closure.object_reads,
        edge_reads: closure.edge_reads,
    }
}

fn build_fixture(scale: usize) -> Fixture {
    let mut graph = Graph::default();
    let mut next = 1_u64;
    let mut allocate = || {
        let id = next;
        next += 1;
        id
    };

    let current_ids = (0..scale)
        .map(|_| {
            let id = allocate();
            graph.insert(id, 8, 128, Vec::new());
            id
        })
        .collect::<Vec<_>>();
    let current_root = allocate();
    graph.insert(current_root, 8, 96, current_ids.clone());

    let reader_only_ids = (0..scale / 4)
        .map(|_| {
            let id = allocate();
            graph.insert(id, 3, 144, Vec::new());
            id
        })
        .collect::<Vec<_>>();
    let reader_root = allocate();
    let mut reader_edges = current_ids[..scale / 2].to_vec();
    reader_edges.extend(reader_only_ids.iter().copied());
    graph.insert(reader_root, 3, 96, reader_edges);

    let child_only_ids = (0..scale / 10)
        .map(|_| {
            let id = allocate();
            graph.insert(id, 7, 160, Vec::new());
            id
        })
        .collect::<Vec<_>>();
    let child_root = allocate();
    let mut child_edges = vec![current_root];
    child_edges.extend(child_only_ids.iter().copied());
    graph.insert(child_root, 7, 96, child_edges);

    let upload_only_ids = (0..scale / 8)
        .map(|_| {
            let id = allocate();
            graph.insert(id, 6, 256, Vec::new());
            id
        })
        .collect::<Vec<_>>();
    let upload_root = allocate();
    graph.insert(upload_root, 6, 128, upload_only_ids.clone());

    let abandoned_old_ids = (0..scale / 20)
        .map(|_| {
            let id = allocate();
            graph.insert(id, 2, 192, Vec::new());
            id
        })
        .collect::<Vec<_>>();
    let abandoned_new_ids = (0..scale / 20)
        .map(|_| {
            let id = allocate();
            graph.insert(id, 5, 192, Vec::new());
            id
        })
        .collect::<Vec<_>>();

    Fixture {
        graph,
        current_root,
        reader_root,
        child_root,
        upload_root,
        reader_only_ids,
        child_only_ids,
        upload_only_ids,
        abandoned_old_ids,
        abandoned_new_ids,
    }
}

fn run(scale: usize) {
    let started = Instant::now();
    let mut fixture = build_fixture(scale);
    let mut owned = BTreeSet::new();
    let current = add_attribution(&fixture.graph, &[fixture.current_root], &mut owned);
    let reader = add_attribution(&fixture.graph, &[fixture.reader_root], &mut owned);
    let branch = add_attribution(&fixture.graph, &[fixture.child_root], &mut owned);
    let upload = add_attribution(&fixture.graph, &[fixture.upload_root], &mut owned);

    assert_eq!(reader.unique_ids, fixture.reader_only_ids.len() + 1);
    assert_eq!(branch.unique_ids, fixture.child_only_ids.len() + 1);
    assert_eq!(upload.unique_ids, fixture.upload_only_ids.len() + 1);

    let active_roots = [
        fixture.current_root,
        fixture.reader_root,
        fixture.child_root,
        fixture.upload_root,
    ];
    let marked = fixture
        .graph
        .closure(&active_roots)
        .expect("mark active selector and reader roots")
        .ids;
    let pinned_low_watermark = 4;
    assert!(
        5 >= pinned_low_watermark,
        "generation 5 compaction is fenced"
    );
    let (deleted_while_pinned, deferred, first_deleted_bytes) = fixture
        .graph
        .delete_unmarked_before(&marked, pinned_low_watermark);
    assert_eq!(deleted_while_pinned.len(), fixture.abandoned_old_ids.len());
    assert_eq!(deferred, fixture.abandoned_new_ids);
    fixture
        .graph
        .closure(&[fixture.reader_root])
        .expect("pinned historical root survives sweep and compaction");

    let advanced_low_watermark = 10;
    assert!(advanced_low_watermark > pinned_low_watermark);
    let current_only = fixture
        .graph
        .closure(&[fixture.current_root])
        .expect("mark current root after final release")
        .ids;
    let (deleted_after_advance, final_deferred, second_deleted_bytes) = fixture
        .graph
        .delete_unmarked_before(&current_only, advanced_low_watermark);
    assert!(final_deferred.is_empty());
    assert!(deleted_after_advance.contains(&fixture.reader_root));
    assert!(deleted_after_advance.contains(&fixture.child_root));
    assert!(deleted_after_advance.contains(&fixture.upload_root));
    fixture
        .graph
        .closure(&[fixture.current_root])
        .expect("current root survives final-reference reclamation");

    println!(
        "safe_point_result,scale={scale},current_unique_ids={},current_unique_bytes={},reader_closure_ids={},reader_unique_ids={},reader_unique_bytes={},reader_work_objects={},reader_work_edges={},branch_closure_ids={},branch_unique_ids={},branch_unique_bytes={},branch_work_objects={},branch_work_edges={},upload_closure_ids={},upload_unique_ids={},upload_unique_bytes={},upload_work_objects={},upload_work_edges={},abandoned_old_ids={},abandoned_new_ids={},pinned_low_watermark={pinned_low_watermark},pinned_deleted_ids={},pinned_deleted_bytes={first_deleted_bytes},pinned_deferred_ids={},advanced_low_watermark={advanced_low_watermark},released_deleted_ids={},released_deleted_bytes={second_deleted_bytes},wall_us={:.3}",
        current.unique_ids,
        current.unique_bytes,
        reader.closure_ids,
        reader.unique_ids,
        reader.unique_bytes,
        reader.object_reads,
        reader.edge_reads,
        branch.closure_ids,
        branch.unique_ids,
        branch.unique_bytes,
        branch.object_reads,
        branch.edge_reads,
        upload.closure_ids,
        upload.unique_ids,
        upload.unique_bytes,
        upload.object_reads,
        upload.edge_reads,
        fixture.abandoned_old_ids.len(),
        fixture.abandoned_new_ids.len(),
        deleted_while_pinned.len(),
        deferred.len(),
        deleted_after_advance.len(),
        started.elapsed().as_secs_f64() * 1_000_000.0,
    );
}

fn main() {
    let scale = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "1000".to_owned())
        .parse::<usize>()
        .expect("scale is usize");
    assert!(scale >= 20 && scale % 40 == 0);
    run(scale);
}
