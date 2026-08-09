struct ViewIdentity {
    storage_read_epoch: u64,
    repository: [u8; 32],
    global_selector: [u8; 32],
    global_root: [u8; 32],
    branch_selector: [u8; 32],
    snapshot_commit: [u8; 32],
}
struct ViewToken([u8; 32]);
struct RawControlRead<'a> { view: &'a ViewIdentity, token: ViewToken }
struct PackedRead<'a> { view: &'a ViewIdentity, token: ViewToken }
