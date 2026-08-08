#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StorageRead(u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CoherentView {
    read_id: u64,
    epoch: u64,
    generation: u64,
    root: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PreparedPublication {
    read: StorageRead,
    view: CoherentView,
    owner: u64,
    next_root: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Failure {
    ReadAlias,
    Owner,
    Epoch,
    Generation,
    DualAuthority,
    Fallback,
}

struct SelectorPlane {
    read: StorageRead,
    epoch: u64,
    generation: u64,
    root: u64,
    owner: u64,
    writes: u32,
    dual_authority: bool,
    fallback: bool,
}

impl PreparedPublication {
    fn from_branch_view(read: StorageRead, view: CoherentView, next_root: u64, owner: u64) -> Self {
        Self {
            read,
            view,
            owner,
            next_root,
        }
    }
}

impl SelectorPlane {
    fn new(read: StorageRead) -> Self {
        Self {
            read,
            epoch: 4,
            generation: 9,
            root: 10,
            owner: 7,
            writes: 0,
            dual_authority: false,
            fallback: false,
        }
    }

    fn view(&self) -> CoherentView {
        CoherentView {
            read_id: self.read.0,
            epoch: self.epoch,
            generation: self.generation,
            root: self.root,
        }
    }

    fn compare_and_swap(
        &mut self,
        read: StorageRead,
        prepared: PreparedPublication,
    ) -> Result<(), Failure> {
        if self.dual_authority {
            return Err(Failure::DualAuthority);
        }
        if self.fallback {
            return Err(Failure::Fallback);
        }
        if read != self.read || prepared.read != self.read || prepared.view.read_id != read.0 {
            return Err(Failure::ReadAlias);
        }
        if prepared.owner != self.owner {
            return Err(Failure::Owner);
        }
        if prepared.view.epoch != self.epoch {
            return Err(Failure::Epoch);
        }
        if prepared.view.generation != self.generation {
            return Err(Failure::Generation);
        }
        if prepared.view.root != self.root {
            return Err(Failure::Epoch);
        }
        self.root = prepared.next_root;
        self.epoch += 1;
        self.generation += 1;
        self.writes += 1;
        Ok(())
    }
}

#[test]
fn positive_same_operation_read_and_owner_epoch_generation_cas() {
    let read = StorageRead(1);
    let mut plane = SelectorPlane::new(read);
    let view = plane.view();
    let prepared = PreparedPublication::from_branch_view(read, view, 11, 7);
    assert_eq!(plane.compare_and_swap(read, prepared), Ok(()));
    assert_eq!(
        (plane.root, plane.epoch, plane.generation, plane.writes),
        (11, 5, 10, 1)
    );
}

#[test]
fn mismatched_read_alias_is_rejected() {
    let read = StorageRead(1);
    let other = StorageRead(2);
    let mut plane = SelectorPlane::new(read);
    let prepared = PreparedPublication::from_branch_view(read, plane.view(), 11, 7);
    assert_eq!(
        plane.compare_and_swap(other, prepared),
        Err(Failure::ReadAlias)
    );
}

#[test]
fn fresh_read_for_preparation_is_rejected() {
    let read = StorageRead(1);
    let fresh = StorageRead(2);
    let mut plane = SelectorPlane::new(read);
    let fresh_view = CoherentView {
        read_id: fresh.0,
        ..plane.view()
    };
    let prepared = PreparedPublication::from_branch_view(fresh, fresh_view, 11, 7);
    assert_eq!(
        plane.compare_and_swap(read, prepared),
        Err(Failure::ReadAlias)
    );
}

#[test]
fn owner_epoch_and_generation_mismatches_are_rejected() {
    let read = StorageRead(1);
    let mut plane = SelectorPlane::new(read);
    let mut view = plane.view();
    let prepared = PreparedPublication::from_branch_view(read, view, 11, 8);
    assert_eq!(plane.compare_and_swap(read, prepared), Err(Failure::Owner));
    view.epoch += 1;
    let prepared = PreparedPublication::from_branch_view(read, view, 11, 7);
    assert_eq!(plane.compare_and_swap(read, prepared), Err(Failure::Epoch));
    view.epoch = plane.epoch;
    view.generation += 1;
    let prepared = PreparedPublication::from_branch_view(read, view, 11, 7);
    assert_eq!(
        plane.compare_and_swap(read, prepared),
        Err(Failure::Generation)
    );
}

#[test]
fn dual_authority_and_fallback_controls_are_explicit_failures() {
    let read = StorageRead(1);
    let view = SelectorPlane::new(read).view();
    let prepared = PreparedPublication::from_branch_view(read, view, 11, 7);

    let mut dual_authority = SelectorPlane::new(read);
    dual_authority.dual_authority = true;
    assert_eq!(
        dual_authority.compare_and_swap(read, prepared),
        Err(Failure::DualAuthority)
    );

    let mut fallback = SelectorPlane::new(read);
    fallback.fallback = true;
    assert_eq!(
        fallback.compare_and_swap(read, prepared),
        Err(Failure::Fallback)
    );
}
