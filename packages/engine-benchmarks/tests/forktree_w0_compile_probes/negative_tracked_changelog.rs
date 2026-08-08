// MUST fail with E0599: the public storage-adapter read trait has no legacy
// tracked/changelog loaders.
use lix::storage_adapter::StorageAdapterRead;

fn probe<R: StorageAdapterRead>(read: &R) {
    let _ = read.load_commit_state_manifest();
    let _ = read.load_tracked_state();
    let _ = read.load_branch_head_control();
}

fn main() {}
