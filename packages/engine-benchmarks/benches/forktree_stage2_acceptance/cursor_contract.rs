//! Executable model of the storage-cursor contract Stage 2 is allowed to use.
//!
//! This is deliberately not an alternate storage implementation. It binds the
//! ownership, cancellation, restart, and authentication semantics expected of
//! the independently owned cursor hard cut while Stage 1 remains unchanged.

use std::ops::Bound;

const REVIEWED_CURSOR_HEAD: &str = "770d73c17afd4d3a569b31820696fe28b65e25d3";
const CONTRACT: &[&str] = &[
    "one borrowing cursor belongs to one immutable StorageRead",
    "ascending pages are bounded, strictly ordered, and snapshot coherent",
    "reverse scan is deterministically Unsupported(ReverseScan)",
    "cancellation after page polling permanently poisons the cursor",
    "cancellation before first poll leaves the cursor untouched",
    "dropping StorageRead ends its historical view and invalidates its cursor",
    "fresh-view restart uses Bound::Excluded(authenticated_key) without a reader lease",
    "continuations never authorize reads writes or garbage collection",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CursorError {
    InvalidCursor,
    ReverseScanUnsupported,
    InvalidRestart,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PageRequest {
    generation: u64,
    polled: bool,
}

struct ReadView<'a> {
    snapshot: &'a [u32],
}

impl<'a> ReadView<'a> {
    fn cursor(&'a self, lower: Bound<u32>) -> Result<ModelCursor<'a>, CursorError> {
        let position = match lower {
            Bound::Unbounded => 0,
            Bound::Included(key) => self.snapshot.partition_point(|candidate| *candidate < key),
            Bound::Excluded(key) => self.snapshot.partition_point(|candidate| *candidate <= key),
        };
        Ok(ModelCursor {
            view: self,
            position,
            generation: 0,
            pending: false,
            poisoned: false,
        })
    }

    fn authenticated_restart(
        &'a self,
        token: &RestartToken,
    ) -> Result<ModelCursor<'a>, CursorError> {
        token.authenticate()?;
        self.cursor(Bound::Excluded(token.key))
    }
}

struct ModelCursor<'a> {
    view: &'a ReadView<'a>,
    position: usize,
    generation: u64,
    pending: bool,
    poisoned: bool,
}

impl ModelCursor<'_> {
    fn reverse_page(&mut self) -> Result<Vec<u32>, CursorError> {
        Err(CursorError::ReverseScanUnsupported)
    }

    fn start_page(&mut self) -> Result<PageRequest, CursorError> {
        if self.poisoned || self.pending {
            return Err(CursorError::InvalidCursor);
        }
        self.generation += 1;
        self.pending = true;
        Ok(PageRequest {
            generation: self.generation,
            polled: false,
        })
    }

    fn poll(&mut self, request: &mut PageRequest) -> Result<(), CursorError> {
        self.validate(request)?;
        request.polled = true;
        Ok(())
    }

    fn cancel(&mut self, request: PageRequest) -> Result<(), CursorError> {
        self.validate(&request)?;
        self.pending = false;
        if request.polled {
            self.poisoned = true;
        }
        Ok(())
    }

    fn complete(
        &mut self,
        request: PageRequest,
        limit_rows: usize,
    ) -> Result<Vec<u32>, CursorError> {
        self.validate(&request)?;
        if !request.polled {
            return Err(CursorError::InvalidCursor);
        }
        self.pending = false;
        let end = self
            .position
            .saturating_add(limit_rows.min(1_024))
            .min(self.view.snapshot.len());
        let page = self.view.snapshot[self.position..end].to_vec();
        self.position = end;
        Ok(page)
    }

    fn validate(&self, request: &PageRequest) -> Result<(), CursorError> {
        if self.poisoned || !self.pending || request.generation != self.generation {
            return Err(CursorError::InvalidCursor);
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct RestartToken {
    key: u32,
    authentication: [u8; 32],
}

impl RestartToken {
    fn new(key: u32) -> Self {
        let authentication = restart_authentication(key);
        Self {
            key,
            authentication,
        }
    }

    fn authenticate(&self) -> Result<(), CursorError> {
        if self.authentication != restart_authentication(self.key) {
            return Err(CursorError::InvalidRestart);
        }
        Ok(())
    }
}

fn restart_authentication(key: u32) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"lix.storage.authenticated.key.v1\0");
    hasher.update(&key.to_be_bytes());
    *hasher.finalize().as_bytes()
}

pub fn run() {
    let rows = [1, 2, 3, 4, 5];
    let view = ReadView { snapshot: &rows };

    let mut healthy = view.cursor(Bound::Unbounded).expect("open cursor");
    let mut request = healthy.start_page().expect("start first page");
    healthy.poll(&mut request).expect("poll first page");
    assert_eq!(healthy.complete(request, 2).expect("first page"), [1, 2]);
    let mut request = healthy.start_page().expect("start second page");
    healthy.poll(&mut request).expect("poll second page");
    assert_eq!(
        healthy.complete(request, 1_000_000).expect("bounded page"),
        [3, 4, 5]
    );
    assert_eq!(
        healthy.reverse_page(),
        Err(CursorError::ReverseScanUnsupported)
    );

    let mut unpolled = view.cursor(Bound::Unbounded).expect("open unpolled cursor");
    let request = unpolled.start_page().expect("start unpolled page");
    unpolled.cancel(request).expect("cancel before poll");
    let mut request = unpolled.start_page().expect("restart unpolled cursor");
    unpolled.poll(&mut request).expect("poll after safe cancel");
    assert_eq!(
        unpolled
            .complete(request, 1)
            .expect("page after safe cancel"),
        [1]
    );

    let mut poisoned = view.cursor(Bound::Unbounded).expect("open poison cursor");
    let mut request = poisoned.start_page().expect("start poison page");
    poisoned.poll(&mut request).expect("poll poison page");
    poisoned.cancel(request).expect("cancel polled page");
    assert_eq!(poisoned.start_page(), Err(CursorError::InvalidCursor));

    let token = RestartToken::new(2);
    let fresh_rows = [0, 1, 2, 3, 4, 5, 6];
    let fresh_view = ReadView {
        snapshot: &fresh_rows,
    };
    let mut restarted = fresh_view
        .authenticated_restart(&token)
        .expect("authenticated fresh-view restart");
    let mut request = restarted.start_page().expect("start restarted page");
    restarted.poll(&mut request).expect("poll restarted page");
    assert_eq!(
        restarted.complete(request, 2).expect("restarted page"),
        [3, 4]
    );
    let mut stale = token.clone();
    stale.key = 3;
    assert!(matches!(
        fresh_view.authenticated_restart(&stale),
        Err(CursorError::InvalidRestart)
    ));

    let mut contract = blake3::Hasher::new();
    for clause in CONTRACT {
        contract.update(clause.as_bytes());
        contract.update(b"\0");
    }
    println!(
        "cursor_contract reviewed_head={} clauses={} digest={} forward=true bounded=true reverse_unsupported=true cancel_unpolled_safe=true cancel_polled_poisoned=true authenticated_exclusive_restart=true restart_view=fresh identical_historical_view_after_drop=false reader_lease_required=false borrow_lifetime=true",
        REVIEWED_CURSOR_HEAD,
        CONTRACT.len(),
        contract.finalize().to_hex(),
    );
}
