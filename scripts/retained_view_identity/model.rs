//! Standalone, dependency-free model for the retained-view identity contract.
//!
//! This is intentionally not a production implementation.  A candidate must
//! bind the same logical fields to its own authenticated read token; wrapper
//! addresses and concrete reader types are not part of identity.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ViewIdentity {
    storage_read_epoch: u64,
    repository: [u8; 32],
    global_selector: [u8; 32],
    global_root: [u8; 32],
    branch_selector: [u8; 32],
    branch_root: [u8; 32],
    snapshot_commit: [u8; 32],
}

impl ViewIdentity {
    fn seed() -> Self {
        Self {
            storage_read_epoch: 7,
            repository: [0x10; 32],
            global_selector: [0x20; 32],
            global_root: [0x30; 32],
            branch_selector: [0x40; 32],
            branch_root: [0x50; 32],
            snapshot_commit: [0x60; 32],
        }
    }

    fn changed(&self, field: usize) -> Self {
        let mut changed = *self;
        match field {
            0 => changed.storage_read_epoch += 1,
            1 => changed.repository[0] ^= 1,
            2 => changed.global_selector[0] ^= 1,
            3 => changed.global_root[0] ^= 1,
            4 => changed.branch_selector[0] ^= 1,
            5 => changed.branch_root[0] ^= 1,
            6 => changed.snapshot_commit[0] ^= 1,
            _ => panic!("unknown identity field"),
        }
        changed
    }

    fn canonical_bytes(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + 32 * 6);
        bytes.extend_from_slice(&self.storage_read_epoch.to_be_bytes());
        bytes.extend_from_slice(&self.repository);
        bytes.extend_from_slice(&self.global_selector);
        bytes.extend_from_slice(&self.global_root);
        bytes.extend_from_slice(&self.branch_selector);
        bytes.extend_from_slice(&self.branch_root);
        bytes.extend_from_slice(&self.snapshot_commit);
        bytes
    }

    fn token(self) -> ViewToken {
        // This stable mixer is sufficient for the pure model.  It is not a
        // cryptographic replacement for the production authenticated digest.
        let mut output = [0u8; 32];
        for (index, byte) in self.canonical_bytes().into_iter().enumerate() {
            let lane = index % output.len();
            output[lane] = output[lane]
                .wrapping_add(byte)
                .rotate_left((index % 8) as u32)
                ^ (index as u8).wrapping_mul(17);
            let next = (lane * 7 + 3) % output.len();
            output[next] = output[next].wrapping_add(output[lane] ^ byte);
        }
        ViewToken(output)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ViewToken([u8; 32]);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Domain {
    RawControl,
    PackedHot,
    History,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Reject {
    UnknownDomain,
    ViewMismatch,
    FailedProof,
    IndexNotInstalled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AuthenticatedView {
    identity: ViewIdentity,
    token: ViewToken,
    domain: Domain,
}

fn authenticate(identity: ViewIdentity, domain: Domain) -> Result<AuthenticatedView, Reject> {
    if domain == Domain::Unknown {
        return Err(Reject::UnknownDomain);
    }
    Ok(AuthenticatedView {
        token: identity.token(),
        identity,
        domain,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReaderKind {
    Raw,
    Packed,
    History,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReaderHandle {
    token: ViewToken,
    kind: ReaderKind,
}

impl ReaderHandle {
    fn from_view(view: AuthenticatedView, kind: ReaderKind) -> Self {
        Self {
            token: view.token,
            kind,
        }
    }
}

#[derive(Debug, Default)]
struct OperationIndex {
    token: Option<ViewToken>,
    decoded_packs: usize,
    closure_proofs: usize,
    member_hits: usize,
}

impl OperationIndex {
    fn install(
        &mut self,
        expected: ViewToken,
        proof: Result<AuthenticatedView, Reject>,
    ) -> Result<(), Reject> {
        self.token = None;
        self.decoded_packs += 1;
        self.closure_proofs += 1;
        let proof = proof.map_err(|_| Reject::FailedProof)?;
        if proof.token != expected {
            return Err(Reject::ViewMismatch);
        }
        self.token = Some(proof.token);
        Ok(())
    }

    fn lookup(&mut self, reader: ReaderHandle) -> Result<(), Reject> {
        let Some(token) = self.token else {
            return Err(Reject::IndexNotInstalled);
        };
        if token != reader.token {
            return Err(Reject::ViewMismatch);
        }
        match reader.kind {
            ReaderKind::Raw | ReaderKind::Packed | ReaderKind::History => {
                self.member_hits += 1;
                Ok(())
            }
        }
    }
}

fn assert_same_view(readers: [ReaderHandle; 3]) -> Result<(), Reject> {
    if readers[0].token == readers[1].token && readers[1].token == readers[2].token {
        Ok(())
    } else {
        Err(Reject::ViewMismatch)
    }
}

fn main() {
    let identity = ViewIdentity::seed();
    let raw = authenticate(identity, Domain::RawControl).expect("raw proof");
    let packed = authenticate(identity, Domain::PackedHot).expect("packed proof");
    let history = authenticate(identity, Domain::History).expect("history proof");

    // Independent wrappers over one retained view share the token.  No
    // pointer/address equality is consulted.
    let readers = [
        ReaderHandle::from_view(raw, ReaderKind::Raw),
        ReaderHandle::from_view(packed, ReaderKind::Packed),
        ReaderHandle::from_view(history, ReaderKind::History),
    ];
    assert_same_view(readers).expect("raw/packed/history same-view binding");

    let mut index = OperationIndex::default();
    index
        .install(raw.token, Ok(raw))
        .expect("one authenticated index install");
    for reader in readers {
        index.lookup(reader).expect("same-view member hit");
    }
    assert_eq!(index.decoded_packs, 1);
    assert_eq!(index.closure_proofs, 1);
    assert_eq!(index.member_hits, 3);

    // Every identity component is part of the token.  A new read epoch,
    // repository, selector, root, branch, or snapshot must not reuse it.
    for field in 0..=6 {
        let other = authenticate(identity.changed(field), Domain::PackedHot)
            .expect("changed identity remains structurally authentic");
        assert_eq!(index.lookup(ReaderHandle::from_view(other, ReaderKind::Packed)), Err(Reject::ViewMismatch));
    }

    // A fresh wrapper with the same logical fields is accepted, including a
    // reopened reader only when the authenticated view identity is unchanged.
    let same_view_wrapper = authenticate(identity, Domain::PackedHot).expect("same view");
    index
        .lookup(ReaderHandle::from_view(same_view_wrapper, ReaderKind::Packed))
        .expect("wrapper identity is not pointer identity");
    assert_eq!(index.member_hits, 4);

    let mut rejected = OperationIndex::default();
    assert_eq!(
        rejected.install(identity.token(), authenticate(identity, Domain::Unknown)),
        Err(Reject::FailedProof)
    );
    assert_eq!(rejected.member_hits, 0);
    assert!(rejected.token.is_none());
    assert_eq!(rejected.lookup(readers[0]), Err(Reject::IndexNotInstalled));

    let wrong_root = authenticate(identity.changed(3), Domain::PackedHot).expect("wrong root");
    assert_eq!(
        rejected.install(identity.token(), Ok(wrong_root)),
        Err(Reject::ViewMismatch)
    );
    assert!(rejected.token.is_none());

    // The field is read here so the model explicitly documents that a
    // token's identity is the authenticated selector/root tuple, not a
    // wrapper pointer or a domain fallback.
    assert_eq!(raw.identity, identity);
    assert_eq!(raw.domain, Domain::RawControl);
    println!(
        "retained_view_identity_model=GREEN same_view=3 cross_view_rejections=7 failed_install_no_index=true"
    );
}
