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
struct RetainedView { identity: ViewIdentity, token: ViewToken }
struct RawControlRead<'a> { view: &'a RetainedView, token: ViewToken }
struct PackedRead<'a> { view: &'a RetainedView, token: ViewToken }
struct HistoryRead<'a> { view: &'a RetainedView, token: ViewToken }

fn build_readers(view: &RetainedView) {
    let raw = RawControlRead { view, token: view.token };
    let packed = PackedRead { view, token: view.token };
    let history = HistoryRead { view, token: view.token };
    let _ = (raw.token, packed.token, history.token, view.identity.repository);
}

fn install_pack_index(proof: Result<ViewToken, DomainError>) -> Result<(), DomainError> {
    let validated = validate_proof(proof)?;
    let _installed_index = validated;
    Ok(())
}

fn validate_proof(proof: Result<ViewToken, DomainError>) -> Result<ViewToken, DomainError> { proof }

enum Domain { Known, Unknown }
enum DomainError { UnknownDomain }
fn check_domain(domain: Domain) -> Result<(), DomainError> {
    match domain { Domain::Known => Ok(()), Domain::Unknown => Err(DomainError::UnknownDomain) }
}
