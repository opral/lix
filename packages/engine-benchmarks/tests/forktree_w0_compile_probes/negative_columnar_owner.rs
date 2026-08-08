// MUST fail: the deleted columnar owner/physical space is not an adapter API.
use lix::columnar_row_group::ColumnarRowGroup;
use lix::live_state::EntityColumnarWriteSets;

fn main() {
    let _ = (std::mem::size_of::<ColumnarRowGroup>(), std::mem::size_of::<EntityColumnarWriteSets>());
}
