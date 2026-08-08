// MUST fail: chronology/catalog authority is not a legacy physical reader.
use lix::changelog::{CHANGE_SPACE, COMMIT_SPACE};
use lix::tracked_state::TrackedStateStoreReader;

fn main() {
    let _ = (CHANGE_SPACE, COMMIT_SPACE);
    let _ = std::mem::size_of::<TrackedStateStoreReader<()>>();
}
