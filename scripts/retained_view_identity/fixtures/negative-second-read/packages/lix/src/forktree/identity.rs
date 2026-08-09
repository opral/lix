struct ViewIdentity {
    storage_read_epoch: u64,
    repository: [u8; 32],
    global_selector: [u8; 32],
    global_root: [u8; 32],
    branch_selector: [u8; 32],
    branch_root: [u8; 32],
    snapshot_commit: [u8; 32],
}
struct ViewToken([u8; 32]);
struct RawControlRead<'a> { view: &'a ViewIdentity, token: ViewToken }
struct PackedRead<'a> { view: &'a ViewIdentity, token: ViewToken }

fn build_readers(view: &ViewIdentity) {
    let raw = RawControlRead { view, token: ViewToken([0; 32]) };
    let packed = PackedRead { view, token: ViewToken([0; 32]) };
    let _ = (raw.token, packed.token);
}

fn packed_read(view: &ViewIdentity) {
    let _second = begin_read(view);
}
fn begin_read(_view: &ViewIdentity) -> ViewToken { ViewToken([1; 32]) }
