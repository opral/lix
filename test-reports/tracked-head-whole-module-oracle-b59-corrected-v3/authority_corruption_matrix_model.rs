//! TEST/REPORT-ONLY discriminator for blocked v2.
//! Every authority domain is mutated through every corruption class and each
//! failure must consume exactly one retained read/view and no durable work.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Domain {
    GlobalSelector,
    BranchSelector,
    StateRoot,
    CatalogRoot,
    CheckpointRoot,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AuthoritySlot {
    GlobalSelector,
    BranchSelector,
    StateRoot,
    CatalogRoot,
    CheckpointRoot,
}
impl AuthoritySlot {
    const ALL: [Self; 5] = [
        Self::GlobalSelector,
        Self::BranchSelector,
        Self::StateRoot,
        Self::CatalogRoot,
        Self::CheckpointRoot,
    ];
    const fn domain(self) -> Domain {
        match self {
            Self::GlobalSelector => Domain::GlobalSelector,
            Self::BranchSelector => Domain::BranchSelector,
            Self::StateRoot => Domain::StateRoot,
            Self::CatalogRoot => Domain::CatalogRoot,
            Self::CheckpointRoot => Domain::CheckpointRoot,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Corruption {
    Malformed,
    Missing,
    WrongKind,
    IdentitySubstitution,
}
impl Corruption {
    const ALL: [Self; 4] = [
        Self::Malformed,
        Self::Missing,
        Self::WrongKind,
        Self::IdentitySubstitution,
    ];
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReadError {
    Malformed,
    Missing,
    WrongKind,
    IdentitySubstitution,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AuthenticatedRef {
    domain: Domain,
    id: u64,
    bytes: Vec<u8>,
    present: bool,
}
impl AuthenticatedRef {
    fn new(domain: Domain, nonce: u64) -> Self {
        let bytes = format!("{domain:?}:v3:{nonce}").into_bytes();
        let id = checksum(&bytes);
        Self {
            domain,
            id,
            bytes,
            present: true,
        }
    }
    fn validate(&self, expected_domain: Domain, expected_id: u64) -> Result<(), ReadError> {
        if !self.present {
            return Err(ReadError::Missing);
        }
        if self.domain != expected_domain {
            return Err(ReadError::WrongKind);
        }
        if self.bytes.is_empty()
            || !self
                .bytes
                .starts_with(format!("{expected_domain:?}").as_bytes())
        {
            return Err(ReadError::Malformed);
        }
        if checksum(&self.bytes) != self.id || self.id != expected_id {
            return Err(ReadError::IdentitySubstitution);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Counters {
    retained_reads: u8,
    retained_views: u8,
    plans: u8,
    prepared_writes: u8,
    commits: u8,
    selector_rotations: u8,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CoherentView {
    epoch: u64,
    view_id: u64,
}

struct Store {
    global_selector: AuthenticatedRef,
    branch_selector: AuthenticatedRef,
    state_root: AuthenticatedRef,
    catalog_root: AuthenticatedRef,
    checkpoint_root: AuthenticatedRef,
    expected: [u64; 5],
    epoch: u64,
    counters: Counters,
}
impl Store {
    fn new() -> Self {
        let global = AuthenticatedRef::new(Domain::GlobalSelector, 1);
        let branch = AuthenticatedRef::new(Domain::BranchSelector, 2);
        let state = AuthenticatedRef::new(Domain::StateRoot, 3);
        let catalog = AuthenticatedRef::new(Domain::CatalogRoot, 4);
        let checkpoint = AuthenticatedRef::new(Domain::CheckpointRoot, 5);
        Self {
            expected: [global.id, branch.id, state.id, catalog.id, checkpoint.id],
            global_selector: global,
            branch_selector: branch,
            state_root: state,
            catalog_root: catalog,
            checkpoint_root: checkpoint,
            epoch: 0,
            counters: Counters::default(),
        }
    }
    fn authority(&self, slot: AuthoritySlot) -> &AuthenticatedRef {
        match slot {
            AuthoritySlot::GlobalSelector => &self.global_selector,
            AuthoritySlot::BranchSelector => &self.branch_selector,
            AuthoritySlot::StateRoot => &self.state_root,
            AuthoritySlot::CatalogRoot => &self.catalog_root,
            AuthoritySlot::CheckpointRoot => &self.checkpoint_root,
        }
    }
    fn authority_mut(&mut self, slot: AuthoritySlot) -> &mut AuthenticatedRef {
        match slot {
            AuthoritySlot::GlobalSelector => &mut self.global_selector,
            AuthoritySlot::BranchSelector => &mut self.branch_selector,
            AuthoritySlot::StateRoot => &mut self.state_root,
            AuthoritySlot::CatalogRoot => &mut self.catalog_root,
            AuthoritySlot::CheckpointRoot => &mut self.checkpoint_root,
        }
    }
    fn expected_id(&self, slot: AuthoritySlot) -> u64 {
        self.expected[match slot {
            AuthoritySlot::GlobalSelector => 0,
            AuthoritySlot::BranchSelector => 1,
            AuthoritySlot::StateRoot => 2,
            AuthoritySlot::CatalogRoot => 3,
            AuthoritySlot::CheckpointRoot => 4,
        }]
    }
    fn corrupt(&mut self, slot: AuthoritySlot, kind: Corruption) {
        match kind {
            Corruption::Malformed => self.authority_mut(slot).bytes = b"not-canonical".to_vec(),
            Corruption::Missing => self.authority_mut(slot).present = false,
            Corruption::WrongKind => {
                let wrong = match slot.domain() {
                    Domain::GlobalSelector => Domain::BranchSelector,
                    Domain::BranchSelector => Domain::GlobalSelector,
                    Domain::StateRoot => Domain::CatalogRoot,
                    Domain::CatalogRoot => Domain::StateRoot,
                    Domain::CheckpointRoot => Domain::CatalogRoot,
                };
                *self.authority_mut(slot) = AuthenticatedRef::new(wrong, 99);
            }
            Corruption::IdentitySubstitution => self.authority_mut(slot).id ^= 1,
        }
    }
    fn view_id(&self) -> u64 {
        AuthoritySlot::ALL.iter().fold(self.epoch, |value, slot| {
            value.rotate_left(7) ^ self.authority(*slot).id
        })
    }
    fn open_coherent_view(&mut self) -> Result<CoherentView, ReadError> {
        // One retained StorageRead/view is acquired before every authority is authenticated.
        self.counters.retained_reads += 1;
        self.counters.retained_views += 1;
        for slot in AuthoritySlot::ALL {
            self.authority(slot)
                .validate(slot.domain(), self.expected_id(slot))?;
        }
        Ok(CoherentView {
            epoch: self.epoch,
            view_id: self.view_id(),
        })
    }
}

fn checksum(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |value, byte| {
        value
            .wrapping_mul(0x1000_0000_01b3)
            .wrapping_add(u64::from(*byte))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn expected_counters() -> Counters {
        Counters {
            retained_reads: 1,
            retained_views: 1,
            plans: 0,
            prepared_writes: 0,
            commits: 0,
            selector_rotations: 0,
        }
    }

    #[test]
    fn every_authority_domain_and_corruption_class_fails_closed_before_work() {
        let mut cases = 0;
        for slot in AuthoritySlot::ALL {
            for kind in Corruption::ALL {
                let mut store = Store::new();
                store.corrupt(slot, kind);
                assert_eq!(
                    store.open_coherent_view(),
                    Err(match kind {
                        Corruption::Malformed => ReadError::Malformed,
                        Corruption::Missing => ReadError::Missing,
                        Corruption::WrongKind => ReadError::WrongKind,
                        Corruption::IdentitySubstitution => ReadError::IdentitySubstitution,
                    })
                );
                assert_eq!(
                    store.counters,
                    expected_counters(),
                    "slot={slot:?} kind={kind:?}"
                );
                cases += 1;
            }
        }
        assert_eq!(cases, 5 * 4);
    }

    #[test]
    fn healthy_view_is_one_read_and_zero_durable_work() {
        let mut store = Store::new();
        let view = store.open_coherent_view().expect("healthy authorities");
        assert_eq!(view.epoch, 0);
        assert_eq!(view.view_id, store.view_id());
        assert_eq!(store.counters, expected_counters());
    }
}
