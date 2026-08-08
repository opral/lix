//! Pure v3 discriminator for every selected selector/catalog/root domain.
//!
//! This is an executable specification only. It does not import Lix or expose
//! any production storage API. Every corruption is applied to exactly one
//! authenticated fixture before one coherent open.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Domain {
    GlobalSelector,
    BranchSelector,
    StateRoot,
    CommitCatalog,
    ChangeCatalog,
    CheckpointRoot,
}

const DOMAINS: [Domain; 6] = [
    Domain::GlobalSelector,
    Domain::BranchSelector,
    Domain::StateRoot,
    Domain::CommitCatalog,
    Domain::ChangeCatalog,
    Domain::CheckpointRoot,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Corruption {
    Malformed,
    Missing,
    WrongKind,
    IdentitySubstitution,
}

const CORRUPTIONS: [Corruption; 4] = [
    Corruption::Malformed,
    Corruption::Missing,
    Corruption::WrongKind,
    Corruption::IdentitySubstitution,
];

#[derive(Clone, Debug, Eq, PartialEq)]
struct AuthenticatedRef {
    domain: Domain,
    object_id: u64,
    canonical: Vec<u8>,
    present: bool,
}

impl AuthenticatedRef {
    fn new(domain: Domain, nonce: u64) -> Self {
        let canonical = format!("{}:v1:{}", domain_name(domain), nonce).into_bytes();
        let object_id = checksum(&canonical);
        Self {
            domain,
            object_id,
            canonical,
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
        if !self
            .canonical
            .starts_with(domain_name(expected_domain).as_bytes())
        {
            return Err(Corruption::Malformed);
        }
        if checksum(&self.canonical) != self.object_id || self.object_id != expected_id {
            return Err(Corruption::IdentitySubstitution);
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

impl Counters {
    fn durable_zero(self) -> bool {
        self.plans == 0
            && self.prepared_writes == 0
            && self.commits == 0
            && self.selector_rotations == 0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct View {
    view_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Store {
    refs: [AuthenticatedRef; 6],
    expected_ids: [u64; 6],
    epoch: u64,
    counters: Counters,
}

impl Store {
    fn new() -> Self {
        let refs = [
            AuthenticatedRef::new(Domain::GlobalSelector, 11),
            AuthenticatedRef::new(Domain::BranchSelector, 22),
            AuthenticatedRef::new(Domain::StateRoot, 33),
            AuthenticatedRef::new(Domain::CommitCatalog, 44),
            AuthenticatedRef::new(Domain::ChangeCatalog, 55),
            AuthenticatedRef::new(Domain::CheckpointRoot, 66),
        ];
        let expected_ids = refs.each_ref().map(|reference| reference.object_id);
        Self {
            refs,
            expected_ids,
            epoch: 7,
            counters: Counters::default(),
        }
    }

    fn open_coherent_view(&mut self) -> Result<View, Corruption> {
        self.counters.retained_reads += 1;
        self.counters.retained_views += 1;
        for (index, domain) in DOMAINS.into_iter().enumerate() {
            self.refs[index].validate(domain, self.expected_ids[index])?;
        }
        Ok(View {
            view_id: self.view_id(),
        })
    }

    fn publish(&mut self, view: View) -> Result<(), &'static str> {
        if view.view_id != self.view_id() {
            return Err("stale coherent view");
        }
        self.counters.plans += 1;
        self.counters.prepared_writes += 1;
        self.counters.commits += 1;
        self.counters.selector_rotations += 1;
        Ok(())
    }

    fn view_id(&self) -> u64 {
        self.refs.iter().fold(self.epoch, |value, reference| {
            value.rotate_left(9) ^ reference.object_id
        })
    }

    fn corrupt(&mut self, domain: Domain, kind: Corruption) {
        let index = domain_index(domain);
        match kind {
            Corruption::Malformed => self.refs[index].canonical = b"not-canonical".to_vec(),
            Corruption::Missing => self.refs[index].present = false,
            Corruption::WrongKind => {
                let substitute = match domain {
                    Domain::GlobalSelector => Domain::BranchSelector,
                    Domain::BranchSelector => Domain::GlobalSelector,
                    Domain::StateRoot => Domain::CommitCatalog,
                    Domain::CommitCatalog => Domain::StateRoot,
                    Domain::ChangeCatalog => Domain::CheckpointRoot,
                    Domain::CheckpointRoot => Domain::ChangeCatalog,
                };
                self.refs[index] = AuthenticatedRef::new(substitute, 999);
            }
            Corruption::IdentitySubstitution => self.refs[index].object_id ^= 1,
        }
    }

    fn identity_substitute_without_corruption(&mut self, domain: Domain) {
        self.refs[domain_index(domain)] = AuthenticatedRef::new(domain, 1001);
    }
}

fn domain_index(domain: Domain) -> usize {
    match domain {
        Domain::GlobalSelector => 0,
        Domain::BranchSelector => 1,
        Domain::StateRoot => 2,
        Domain::CommitCatalog => 3,
        Domain::ChangeCatalog => 4,
        Domain::CheckpointRoot => 5,
    }
}

fn domain_name(domain: Domain) -> &'static str {
    match domain {
        Domain::GlobalSelector => "global-selector",
        Domain::BranchSelector => "branch-selector",
        Domain::StateRoot => "state-root",
        Domain::CommitCatalog => "commit-catalog",
        Domain::ChangeCatalog => "change-catalog",
        Domain::CheckpointRoot => "checkpoint-root",
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

    #[test]
    fn every_domain_and_corruption_is_one_read_then_zero_durable_work() {
        for domain in DOMAINS {
            for corruption in CORRUPTIONS {
                let mut store = Store::new();
                let before = store.counters;
                store.corrupt(domain, corruption);
                assert_eq!(store.open_coherent_view(), Err(corruption));
                assert_eq!(store.counters.retained_reads, before.retained_reads + 1);
                assert_eq!(store.counters.retained_views, before.retained_views + 1);
                assert!(store.counters.durable_zero());
            }
        }
    }

    #[test]
    fn valid_view_binds_all_six_domains() {
        let mut store = Store::new();
        let view = store.open_coherent_view().expect("valid selected closure");
        assert_eq!(store.counters.retained_reads, 1);
        assert_eq!(store.counters.retained_views, 1);
        store.publish(view).expect("one publication");
        assert_eq!(store.counters.plans, 1);
        assert_eq!(store.counters.prepared_writes, 1);
        assert_eq!(store.counters.commits, 1);
        assert_eq!(store.counters.selector_rotations, 1);
    }

    #[test]
    fn any_domain_replacement_invalidates_the_pinned_view_without_writes() {
        for domain in DOMAINS {
            let mut store = Store::new();
            let view = store.open_coherent_view().expect("valid selected closure");
            store.identity_substitute_without_corruption(domain);
            assert_eq!(store.publish(view), Err("stale coherent view"));
            assert!(store.counters.durable_zero());
            assert_eq!(store.counters.retained_reads, 1);
            assert_eq!(store.counters.retained_views, 1);
        }
    }
}

fn main() {
    let mut failures = 0_u64;
    let mut stale_failures = 0_u64;
    let mut digest = 0xcbf2_9ce4_8422_2325_u64;
    for domain in DOMAINS {
        for corruption in CORRUPTIONS {
            let mut store = Store::new();
            store.corrupt(domain, corruption);
            let result = store.open_coherent_view();
            let passed = result.is_err()
                && store.counters.retained_reads == 1
                && store.counters.retained_views == 1
                && store.counters.durable_zero();
            if !passed {
                failures += 1;
            }
            let record = format!("{}:{corruption:?}:{passed}", domain_name(domain));
            digest = checksum_with_seed(digest, record.as_bytes());
        }
    }
    for domain in DOMAINS {
        let mut store = Store::new();
        let view = store.open_coherent_view().expect("valid selected closure");
        store.identity_substitute_without_corruption(domain);
        if store.publish(view) != Err("stale coherent view") || !store.counters.durable_zero() {
            stale_failures += 1;
        }
    }
    let total = (DOMAINS.len() * CORRUPTIONS.len()) as u64;
    println!(
        "v3_cases={} passed={} failures={} stale_view_cases=6 stale_view_failures={} retained_read_per_case=1 durable_work_per_failure=0 digest={digest:016x}",
        total,
        total - failures,
        failures,
        stale_failures
    );
    assert_eq!(failures, 0);
    assert_eq!(stale_failures, 0);
}

fn checksum_with_seed(seed: u64, bytes: &[u8]) -> u64 {
    bytes.iter().fold(seed, |value, byte| {
        value
            .rotate_left(5)
            .wrapping_mul(0x1000_0000_01b3)
            .wrapping_add(u64::from(*byte))
    })
}
