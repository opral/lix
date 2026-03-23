use crate::live_tracked_state::codec::DecodedNode;
use crate::live_tracked_state::types::{LiveTrackedRootId, LIVE_TRACKED_HASH_BYTES};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub(crate) struct LiveTrackedNodeCache {
    capacity: usize,
    state: Mutex<LiveTrackedNodeCacheState>,
}

#[derive(Debug, Default)]
struct LiveTrackedNodeCacheState {
    map: HashMap<[u8; LIVE_TRACKED_HASH_BYTES], Arc<DecodedNode>>,
    order: VecDeque<[u8; LIVE_TRACKED_HASH_BYTES]>,
}

impl LiveTrackedNodeCache {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            state: Mutex::new(LiveTrackedNodeCacheState::default()),
        }
    }

    pub(crate) fn get(&self, hash: &[u8; LIVE_TRACKED_HASH_BYTES]) -> Option<Arc<DecodedNode>> {
        let mut state = self.state.lock().ok()?;
        let value = state.map.get(hash).cloned();
        if value.is_some() {
            state.order.push_back(*hash);
        }
        value
    }

    pub(crate) fn insert(
        &self,
        hash: [u8; LIVE_TRACKED_HASH_BYTES],
        node: Arc<DecodedNode>,
    ) -> Arc<DecodedNode> {
        let mut state = self
            .state
            .lock()
            .expect("live tracked node cache mutex should not be poisoned");
        if let Some(existing) = state.map.get(&hash).cloned() {
            state.order.push_back(hash);
            return existing;
        }

        state.map.insert(hash, Arc::clone(&node));
        state.order.push_back(hash);
        while state.map.len() > self.capacity {
            let Some(candidate) = state.order.pop_front() else {
                break;
            };
            if state.order.iter().any(|existing| existing == &candidate) {
                continue;
            }
            state.map.remove(&candidate);
        }
        node
    }

    pub(crate) fn clear(&self) {
        let mut state = self
            .state
            .lock()
            .expect("live tracked node cache mutex should not be poisoned");
        state.map.clear();
        state.order.clear();
    }

    #[allow(dead_code)]
    pub(crate) fn contains_root(&self, root_id: &LiveTrackedRootId) -> bool {
        self.state
            .lock()
            .expect("live tracked node cache mutex should not be poisoned")
            .map
            .contains_key(root_id.as_bytes())
    }
}
