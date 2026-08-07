//! Standalone DAG oracle for the non-runnable Stage-2 compiler wave.

use std::collections::{BTreeMap, BTreeSet};

const NODES: &[&str] = &[
    "G0", "M0", "R0", "R1", "R2", "R3", "R4", "R5", "R6", "R7", "W0", "W1", "W2", "W3", "W4", "W5",
    "D0", "D1", "D2", "C0", "C1",
];

const EDGES: &[(&str, &str)] = &[
    ("G0", "M0"),
    ("M0", "R0"),
    ("R0", "R1"),
    ("R0", "R2"),
    ("R0", "R3"),
    ("R0", "R4"),
    ("R0", "R5"),
    ("R0", "R6"),
    ("R0", "R7"),
    // Writer-last: every reader/consumer family must move first.
    ("R1", "W0"),
    ("R2", "W0"),
    ("R3", "W0"),
    ("R4", "W0"),
    ("R5", "W0"),
    ("R6", "W0"),
    ("R7", "W0"),
    // Install the owner-local conflict keys and read-only GC-generation fence
    // before any logical publication/root writer or sweep can run.
    ("W0", "W4"),
    ("W4", "W1"),
    ("W4", "W2"),
    ("W4", "W3"),
    // Sweep starts only after state/catalog, selector/root, and
    // blob/upload/plugin publication can all add every authoritative root
    // under that fence.
    ("W1", "W5"),
    ("W2", "W5"),
    ("W3", "W5"),
    // Working-diff writers disappear only after root-diff readers and
    // state/selector publication are authoritative.
    ("R4", "D0"),
    ("W1", "D0"),
    ("W2", "D0"),
    ("D0", "D1"),
    ("W1", "D1"),
    ("W2", "D1"),
    ("W3", "D1"),
    ("W4", "D1"),
    ("W5", "D1"),
    ("D1", "D2"),
    ("D2", "C0"),
    ("C0", "C1"),
];

fn main() {
    let nodes = NODES.iter().copied().collect::<BTreeSet<_>>();
    assert_eq!(nodes.len(), NODES.len(), "duplicate dependency node");

    let mut indegree = NODES
        .iter()
        .map(|node| (*node, 0_usize))
        .collect::<BTreeMap<_, _>>();
    let mut outgoing = BTreeMap::<&str, Vec<&str>>::new();
    for (before, after) in EDGES {
        assert!(nodes.contains(before), "unknown predecessor {before}");
        assert!(nodes.contains(after), "unknown successor {after}");
        *indegree.get_mut(after).expect("known successor") += 1;
        outgoing.entry(before).or_default().push(after);
    }

    let mut ready = indegree
        .iter()
        .filter_map(|(node, degree)| (*degree == 0).then_some(*node))
        .collect::<BTreeSet<_>>();
    let mut order = Vec::new();
    while let Some(node) = ready.pop_first() {
        order.push(node);
        for successor in outgoing.get(node).into_iter().flatten() {
            let degree = indegree.get_mut(successor).expect("known successor");
            *degree -= 1;
            if *degree == 0 {
                ready.insert(successor);
            }
        }
    }
    assert_eq!(
        order.len(),
        NODES.len(),
        "dependency graph contains a cycle"
    );

    let position = order
        .iter()
        .enumerate()
        .map(|(index, node)| (*node, index))
        .collect::<BTreeMap<_, _>>();
    for reader in ["R1", "R2", "R3", "R4", "R5", "R6", "R7"] {
        assert!(
            position[reader] < position["W0"],
            "writer started before {reader}"
        );
    }
    for delete in ["D0", "D1", "D2"] {
        assert!(
            position[delete] < position["C1"],
            "compile precedes {delete}"
        );
    }
    for publication in ["W1", "W2", "W3"] {
        assert!(
            position["W4"] < position[publication],
            "{publication} precedes the owner-local fence"
        );
        assert!(
            position[publication] < position["W5"],
            "sweep precedes publication/root prerequisite {publication}"
        );
    }
    assert!(position["D2"] < position["C0"]);
    assert!(position["C0"] < position["C1"]);

    println!(
        "forktree-stage2-compiler-dependency-oracle PASS nodes={} edges={} order={}",
        NODES.len(),
        EDGES.len(),
        order.join(",")
    );
}
