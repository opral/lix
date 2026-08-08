// MUST fail type-checking: the deleted binary-CAS owner is not a consumer API.
use lix::binary_cas::{BinaryCasContext, BinaryCasSpace};

fn main() {
    let _ = BinaryCasContext::new();
    let _ = BinaryCasSpace::manifest();
}
