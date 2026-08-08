//! Pure test/report-only corruption discriminator for the b59 OLAP contract.
//! No production storage or adapter behavior is represented here.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Domain {
    GlobalSelector,
    BranchSelector,
    StateRoot,
    CatalogRoot,
    CheckpointRoot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Target {
    GlobalSelector,
    BranchSelector,
    StateRoot,
    CatalogRoot,
    CheckpointRoot,
}

impl Target {
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
enum Failure {
    MalformedGlobalSelector,
    MissingGlobalSelector,
    WrongGlobalSelectorKind,
    GlobalSelectorIdentityMismatch,
    MalformedBranchSelector,
    MissingBranchSelector,
    WrongBranchSelectorKind,
    BranchSelectorIdentityMismatch,
    MalformedStateRoot,
    MissingStateRoot,
    WrongStateRootKind,
    StateRootIdentityMismatch,
    MalformedCatalogRoot,
    MissingCatalogRoot,
    WrongCatalogRootKind,
    CatalogRootIdentityMismatch,
    MalformedCheckpointRoot,
    MissingCheckpointRoot,
    WrongCheckpointRootKind,
    CheckpointRootIdentityMismatch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Outcome {
    Valid,
    ValidAbsence,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Counters {
    retained_reads: u8,
    retained_views: u8,
    writes: u8,
    publications: u8,
    selector_cas: u8,
    epoch_cas: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Authority {
    domain: Domain,
    id: u64,
    bytes: Vec<u8>,
    present: bool,
}

impl Authority {
    fn new(domain: Domain, nonce: u64) -> Self {
        let bytes = format!("{domain:?}:v1:{nonce}").into_bytes();
        let id = checksum(&bytes);
        Self {
            domain,
            id,
            bytes,
            present: true,
        }
    }

    fn validate(&self, expected_domain: Domain, expected_id: u64) -> Result<(), Corruption> {
        if !self.present {
            return Err(Corruption::Missing);
        }
        if self.domain != expected_domain {
            return Err(Corruption::WrongKind);
        }
        if self.bytes.is_empty()
            || !self
                .bytes
                .starts_with(format!("{expected_domain:?}").as_bytes())
        {
            return Err(Corruption::Malformed);
        }
        if checksum(&self.bytes) != self.id || self.id != expected_id {
            return Err(Corruption::IdentitySubstitution);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Store {
    global_selector: Authority,
    branch_selector: Authority,
    state_root: Authority,
    catalog_root: Authority,
    checkpoint_root: Authority,
    expected_ids: [u64; 5],
    optional_object_present: bool,
    counters: Counters,
}

impl Store {
    fn new() -> Self {
        let global_selector = Authority::new(Domain::GlobalSelector, 1);
        let branch_selector = Authority::new(Domain::BranchSelector, 2);
        let state_root = Authority::new(Domain::StateRoot, 3);
        let catalog_root = Authority::new(Domain::CatalogRoot, 4);
        let checkpoint_root = Authority::new(Domain::CheckpointRoot, 5);
        Self {
            expected_ids: [
                global_selector.id,
                branch_selector.id,
                state_root.id,
                catalog_root.id,
                checkpoint_root.id,
            ],
            global_selector,
            branch_selector,
            state_root,
            catalog_root,
            checkpoint_root,
            optional_object_present: false,
            counters: Counters::default(),
        }
    }

    fn authority(&self, target: Target) -> &Authority {
        match target {
            Target::GlobalSelector => &self.global_selector,
            Target::BranchSelector => &self.branch_selector,
            Target::StateRoot => &self.state_root,
            Target::CatalogRoot => &self.catalog_root,
            Target::CheckpointRoot => &self.checkpoint_root,
        }
    }

    fn authority_mut(&mut self, target: Target) -> &mut Authority {
        match target {
            Target::GlobalSelector => &mut self.global_selector,
            Target::BranchSelector => &mut self.branch_selector,
            Target::StateRoot => &mut self.state_root,
            Target::CatalogRoot => &mut self.catalog_root,
            Target::CheckpointRoot => &mut self.checkpoint_root,
        }
    }

    fn expected_id(&self, target: Target) -> u64 {
        self.expected_ids[target as usize]
    }

    fn fingerprint(&self) -> u64 {
        [
            &self.global_selector,
            &self.branch_selector,
            &self.state_root,
            &self.catalog_root,
            &self.checkpoint_root,
        ]
        .into_iter()
        .fold(0u64, |value, authority| {
            let mut next = value ^ authority.id;
            next = checksum_with_seed(&authority.bytes, next);
            next ^ u64::from(authority.present)
        })
    }

    fn corrupt(&mut self, target: Target, corruption: Corruption) {
        let authority = self.authority_mut(target);
        match corruption {
            Corruption::Malformed => authority.bytes = b"not-canonical".to_vec(),
            Corruption::Missing => authority.present = false,
            Corruption::WrongKind => {
                let wrong_domain = match target {
                    Target::GlobalSelector => Domain::BranchSelector,
                    Target::BranchSelector => Domain::GlobalSelector,
                    Target::StateRoot => Domain::CatalogRoot,
                    Target::CatalogRoot => Domain::StateRoot,
                    Target::CheckpointRoot => Domain::CatalogRoot,
                };
                *authority = Authority::new(wrong_domain, 99);
            }
            Corruption::IdentitySubstitution => authority.id ^= 1,
        }
    }

    fn open_view(&mut self) -> Result<Outcome, Failure> {
        self.counters.retained_reads += 1;
        self.counters.retained_views += 1;
        for target in Target::ALL {
            let result = self
                .authority(target)
                .validate(target.domain(), self.expected_id(target));
            if let Err(corruption) = result {
                return Err(failure(target, corruption));
            }
        }
        Ok(Outcome::Valid)
    }

    fn read_optional_object(&self) -> Outcome {
        if self.optional_object_present {
            Outcome::Valid
        } else {
            Outcome::ValidAbsence
        }
    }
}

fn failure(target: Target, corruption: Corruption) -> Failure {
    match (target, corruption) {
        (Target::GlobalSelector, Corruption::Malformed) => Failure::MalformedGlobalSelector,
        (Target::GlobalSelector, Corruption::Missing) => Failure::MissingGlobalSelector,
        (Target::GlobalSelector, Corruption::WrongKind) => Failure::WrongGlobalSelectorKind,
        (Target::GlobalSelector, Corruption::IdentitySubstitution) => {
            Failure::GlobalSelectorIdentityMismatch
        }
        (Target::BranchSelector, Corruption::Malformed) => Failure::MalformedBranchSelector,
        (Target::BranchSelector, Corruption::Missing) => Failure::MissingBranchSelector,
        (Target::BranchSelector, Corruption::WrongKind) => Failure::WrongBranchSelectorKind,
        (Target::BranchSelector, Corruption::IdentitySubstitution) => {
            Failure::BranchSelectorIdentityMismatch
        }
        (Target::StateRoot, Corruption::Malformed) => Failure::MalformedStateRoot,
        (Target::StateRoot, Corruption::Missing) => Failure::MissingStateRoot,
        (Target::StateRoot, Corruption::WrongKind) => Failure::WrongStateRootKind,
        (Target::StateRoot, Corruption::IdentitySubstitution) => Failure::StateRootIdentityMismatch,
        (Target::CatalogRoot, Corruption::Malformed) => Failure::MalformedCatalogRoot,
        (Target::CatalogRoot, Corruption::Missing) => Failure::MissingCatalogRoot,
        (Target::CatalogRoot, Corruption::WrongKind) => Failure::WrongCatalogRootKind,
        (Target::CatalogRoot, Corruption::IdentitySubstitution) => {
            Failure::CatalogRootIdentityMismatch
        }
        (Target::CheckpointRoot, Corruption::Malformed) => Failure::MalformedCheckpointRoot,
        (Target::CheckpointRoot, Corruption::Missing) => Failure::MissingCheckpointRoot,
        (Target::CheckpointRoot, Corruption::WrongKind) => Failure::WrongCheckpointRootKind,
        (Target::CheckpointRoot, Corruption::IdentitySubstitution) => {
            Failure::CheckpointRootIdentityMismatch
        }
    }
}

fn checksum(bytes: &[u8]) -> u64 {
    checksum_with_seed(bytes, 0xcbf29ce484222325)
}

fn checksum_with_seed(bytes: &[u8], seed: u64) -> u64 {
    bytes.iter().fold(seed, |value, byte| {
        value
            .wrapping_mul(0x100000001b3)
            .wrapping_add(u64::from(*byte))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const ZERO_DURABLE_WORK: Counters = Counters {
        retained_reads: 1,
        retained_views: 1,
        writes: 0,
        publications: 0,
        selector_cas: 0,
        epoch_cas: 0,
    };

    #[test]
    fn every_named_corruption_is_typed_and_atomic() {
        let mut cases = 0;
        for target in Target::ALL {
            for corruption in Corruption::ALL {
                let mut store = Store::new();
                store.corrupt(target, corruption);
                let before = store.fingerprint();
                let error = store.open_view().expect_err("corruption must fail closed");
                assert_eq!(error, failure(target, corruption));
                assert_eq!(store.fingerprint(), before);
                assert_eq!(store.counters, ZERO_DURABLE_WORK);
                cases += 1;
            }
        }
        assert_eq!(cases, 20);
    }

    #[test]
    fn valid_absence_is_not_missing_and_has_no_durable_work() {
        let mut store = Store::new();
        let before = store.fingerprint();
        assert_eq!(store.open_view(), Ok(Outcome::Valid));
        assert_eq!(store.read_optional_object(), Outcome::ValidAbsence);
        assert_eq!(store.fingerprint(), before);
        assert_eq!(store.counters, ZERO_DURABLE_WORK);
    }
}
