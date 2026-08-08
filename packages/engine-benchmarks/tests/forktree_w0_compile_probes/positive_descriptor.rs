// Acceptance-only sketch. Compile this against the candidate's public/internal
// boundary harness, not against the pre-cut baseline.
use lix::forktree::{CoherentView, ObjectId};

fn accepts_opaque_forktree_domain(_view: Option<CoherentView<()>>, _id: ObjectId) {}

fn main() {
    accepts_opaque_forktree_domain(None, ObjectId::from_bytes([0; 32]));
}
