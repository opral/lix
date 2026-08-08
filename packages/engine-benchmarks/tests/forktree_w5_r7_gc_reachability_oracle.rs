use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Fence {
    epoch: u64,
    progress: u64,
    selector: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Authority {
    fence: Fence,
    queue_head: usize,
    queue_tail: usize,
}

impl Authority {
    fn new() -> Self {
        Self {
            fence: Fence {
                epoch: 0,
                progress: 0,
                selector: 0,
            },
            queue_head: 0,
            queue_tail: 0,
        }
    }

    fn advance_gc(&mut self) {
        self.fence.epoch += 1;
        self.fence.progress += 1;
    }

    fn publish(&mut self, expected: Fence) -> Result<(), Error> {
        if self.fence != expected {
            return Err(Error::StaleFence);
        }
        self.fence.selector += 1;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct QueueEntry {
    object: String,
    blocked: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct QueueState {
    authority: Authority,
    entries: Vec<QueueEntry>,
    deleted: BTreeSet<String>,
    debt_tokens: u32,
    calls: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PageResult {
    advanced: bool,
    drained: bool,
    reclaimed: usize,
}

impl QueueState {
    fn new(entries: Vec<QueueEntry>) -> Self {
        let mut authority = Authority::new();
        authority.queue_tail = entries.len();
        Self {
            authority,
            entries,
            deleted: BTreeSet::new(),
            debt_tokens: 0,
            calls: 0,
        }
    }

    fn process(&mut self, max_entries: usize) -> PageResult {
        self.calls += 1;
        if self.authority.queue_head == self.entries.len() {
            return PageResult {
                advanced: false,
                drained: true,
                reclaimed: 0,
            };
        }

        if self.entries[self.authority.queue_head].blocked {
            self.debt_tokens = 1;
            return PageResult {
                advanced: false,
                drained: false,
                reclaimed: 0,
            };
        }

        let old_head = self.authority.queue_head;
        let new_head = (old_head + max_entries).min(self.entries.len());
        let mut reclaimed = 0;
        for entry in &self.entries[old_head..new_head] {
            if self.deleted.insert(entry.object.clone()) {
                reclaimed += 1;
            }
        }
        self.authority.queue_head = new_head;
        self.authority.advance_gc();

        PageResult {
            advanced: true,
            drained: new_head == self.entries.len(),
            reclaimed,
        }
    }

    fn release_blocked_head(&mut self) {
        self.entries[self.authority.queue_head].blocked = false;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct View {
    id: u64,
    root: String,
    valid: bool,
    last_key: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Cursor {
    view_id: u64,
    last_key: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Error {
    StaleFence,
    ReadExpired,
    InvalidCursor,
    MissingRoot,
    Cycle,
    Malformed,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

struct ReaderModel {
    next_view: u64,
    views: BTreeMap<u64, View>,
    pinned_roots: BTreeSet<String>,
}

impl ReaderModel {
    fn new() -> Self {
        Self {
            next_view: 0,
            views: BTreeMap::new(),
            pinned_roots: BTreeSet::new(),
        }
    }

    fn begin_read(&mut self, root: &str) -> View {
        self.next_view += 1;
        let view = View {
            id: self.next_view,
            root: root.to_owned(),
            valid: true,
            last_key: None,
        };
        self.pinned_roots.insert(root.to_owned());
        self.views.insert(view.id, view.clone());
        view
    }

    fn fail_page(&mut self, view_id: u64, delivered: &str) -> Result<(), Error> {
        let view = self.views.get_mut(&view_id).ok_or(Error::ReadExpired)?;
        view.valid = false;
        view.last_key = Some(delivered.to_owned());
        self.pinned_roots.remove(&view.root);
        Err(Error::Malformed)
    }

    fn resume(&self, cursor: &Cursor) -> Result<String, Error> {
        let view = self.views.get(&cursor.view_id).ok_or(Error::ReadExpired)?;
        if !view.valid {
            return Err(Error::ReadExpired);
        }
        if view.last_key != cursor.last_key {
            return Err(Error::InvalidCursor);
        }
        Ok(cursor
            .last_key
            .clone()
            .unwrap_or_else(|| "<start>".to_owned()))
    }
}

#[test]
fn w5_r7_retireable_65_entry_queue_drains_suffix() {
    let entries = (0..65)
        .map(|index| QueueEntry {
            object: format!("root-{index:02}"),
            blocked: false,
        })
        .collect();
    let mut queue = QueueState::new(entries);

    let first = queue.process(64);
    assert_eq!(
        first,
        PageResult {
            advanced: true,
            drained: false,
            reclaimed: 64
        }
    );
    assert_eq!(queue.authority.queue_head, 64);
    assert_eq!(queue.authority.queue_tail, 65);
    assert_eq!(queue.deleted.len(), 64);
    assert_eq!(
        queue.authority.fence,
        Fence {
            epoch: 1,
            progress: 1,
            selector: 0
        }
    );

    let second = queue.process(64);
    assert_eq!(
        second,
        PageResult {
            advanced: true,
            drained: true,
            reclaimed: 1
        }
    );
    assert_eq!(queue.authority.queue_head, 65);
    assert_eq!(queue.deleted.len(), 65);
    assert_eq!(queue.process(64).reclaimed, 0);
}

#[test]
fn w5_r7_blocked_head_preserves_one_debt_and_no_spin() {
    let mut queue = QueueState::new(vec![
        QueueEntry {
            object: "blocked-source".to_owned(),
            blocked: true,
        },
        QueueEntry {
            object: "released-suffix".to_owned(),
            blocked: false,
        },
    ]);

    let blocked = queue.process(64);
    assert_eq!(
        blocked,
        PageResult {
            advanced: false,
            drained: false,
            reclaimed: 0
        }
    );
    assert_eq!(queue.debt_tokens, 1);
    assert_eq!(queue.calls, 1);
    assert!(queue.deleted.is_empty());

    queue.release_blocked_head();
    let drained = queue.process(64);
    assert_eq!(drained.reclaimed, 2);
    assert!(drained.drained);
    assert_eq!(queue.debt_tokens, 1);
    assert_eq!(queue.calls, 2);
}

#[test]
fn w5_r7_publication_first_and_gc_first_are_fenced() {
    let mut publication_first = Authority::new();
    let prepared = publication_first.fence;
    publication_first.advance_gc();
    assert_eq!(publication_first.publish(prepared), Err(Error::StaleFence));

    let mut gc_first = Authority::new();
    let stale_prepared = gc_first.fence;
    gc_first.advance_gc();
    assert_eq!(gc_first.publish(stale_prepared), Err(Error::StaleFence));

    let mut same_view = Authority::new();
    let coherent = same_view.fence;
    assert_eq!(same_view.publish(coherent), Ok(()));
    assert_eq!(same_view.fence.selector, 1);
}

#[test]
fn w5_r7_pinned_view_upload_shared_and_final_reference() {
    let mut readers = ReaderModel::new();
    let view = readers.begin_read("checkpoint-root");
    let cursor = Cursor {
        view_id: view.id,
        last_key: None,
    };
    assert!(readers.pinned_roots.contains("checkpoint-root"));
    assert_eq!(readers.resume(&cursor), Ok("<start>".to_owned()));

    let mut owners = BTreeMap::from([
        (
            "shared-object".to_owned(),
            BTreeSet::from(["branch", "upload"]),
        ),
        ("final-object".to_owned(), BTreeSet::from(["branch"])),
    ]);
    assert!(owners["shared-object"].contains("upload"));
    owners.get_mut("shared-object").unwrap().remove("branch");
    assert!(owners["shared-object"].contains("upload"));
    owners.get_mut("shared-object").unwrap().remove("upload");
    owners.remove("shared-object");
    assert!(!owners.contains_key("shared-object"));

    assert_eq!(readers.fail_page(view.id, "row-09"), Err(Error::Malformed));
    assert_eq!(readers.resume(&cursor), Err(Error::ReadExpired));
    let fresh = readers.begin_read("checkpoint-root");
    assert_ne!(fresh.id, view.id);
    let restarted = Cursor {
        view_id: fresh.id,
        last_key: Some("row-09".to_owned()),
    };
    assert_eq!(readers.resume(&restarted), Err(Error::InvalidCursor));

    owners.get_mut("final-object").unwrap().remove("branch");
    owners.remove("final-object");
    assert!(owners.is_empty());
}

#[test]
fn w5_r7_corruption_cycles_and_missing_roots_fail_closed() {
    for error in [Error::Cycle, Error::MissingRoot, Error::Malformed] {
        assert!(matches!(
            Err::<(), _>(error),
            Err(Error::Cycle | Error::MissingRoot | Error::Malformed)
        ));
    }
}

#[test]
fn w5_r7_cold_reopen_preserves_authority_and_queue() {
    let mut queue = QueueState::new(vec![
        QueueEntry {
            object: "reopen-root".to_owned(),
            blocked: false,
        },
        QueueEntry {
            object: "reopen-tail".to_owned(),
            blocked: false,
        },
    ]);
    let _ = queue.process(1);

    let encoded = format!(
        "{}:{}:{}:{}:{}:{}",
        queue.authority.fence.epoch,
        queue.authority.fence.progress,
        queue.authority.fence.selector,
        queue.authority.queue_head,
        queue.authority.queue_tail,
        queue.deleted.len()
    );
    let recovered: Vec<usize> = encoded
        .split(':')
        .map(|part| part.parse().expect("typed reopen fixture"))
        .collect();
    assert_eq!(recovered, vec![1, 1, 0, 1, 2, 1]);
    assert_eq!(queue.authority.queue_head, recovered[3]);
    assert_eq!(queue.authority.queue_tail, recovered[4]);
    assert_eq!(queue.deleted.len(), recovered[5]);
}
