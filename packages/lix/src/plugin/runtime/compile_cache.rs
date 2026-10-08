//! Shared bounded singleflight and weighted-LRU machinery for compiled WASM.
//!
//! Source-byte accounting is an admission proxy for retained input and
//! initializer copies. It does not measure opaque compiled memory, and it
//! cannot guarantee that cancellation stops backend work already detached by a
//! runtime.

use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use lru::LruCache;
use tokio::sync::OnceCell;

use crate::common::LixError;

#[derive(Clone, Copy, Debug)]
pub(crate) struct CompileCacheLimits {
    pub(crate) ready_entries: usize,
    pub(crate) ready_source_bytes: u64,
    pub(crate) max_in_flight_groups: usize,
    pub(crate) max_in_flight_callers: usize,
    pub(crate) max_in_flight_source_bytes: u64,
}

impl CompileCacheLimits {
    pub(crate) const ENGINE: Self = Self {
        ready_entries: 16,
        ready_source_bytes: 64 * 1024 * 1024,
        max_in_flight_groups: 8,
        max_in_flight_callers: 32,
        max_in_flight_source_bytes: 64 * 1024 * 1024,
    };
}

struct ReadyValue<V> {
    source_bytes: u64,
    value: V,
}

struct Flight<V> {
    result: OnceCell<Result<V, LixError>>,
    /// The input weight used if a successful result enters the ready cache.
    ready_source_bytes: u64,
    /// One initializer's additional source copy, shared by the whole group.
    initializer_copy_bytes: u64,
}

struct FlightEntry<V> {
    flight: Arc<Flight<V>>,
    waiters: usize,
    initializer_copy_reserved: bool,
}

struct CacheState<K, V> {
    ready: LruCache<K, ReadyValue<V>>,
    ready_source_bytes: u64,
    in_flight: HashMap<K, FlightEntry<V>>,
    in_flight_source_bytes: u64,
    in_flight_callers: usize,
}

struct CacheInner<K, V> {
    state: Mutex<CacheState<K, V>>,
    limits: CompileCacheLimits,
}

/// A bounded ready cache with fail-fast singleflight admission.
///
/// Every admitted caller reserves its original source bytes and one caller
/// slot. A new flight also reserves the initializer's extra source-copy bytes.
/// Duplicate-key callers share one initializer, while distinct-key bursts are
/// rejected once the flight, caller, or byte limits are full.
pub(crate) struct BoundedCompileCache<K, V> {
    inner: Arc<CacheInner<K, V>>,
}

impl<K, V> Clone for BoundedCompileCache<K, V> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K, V> BoundedCompileCache<K, V>
where
    K: Clone + Eq + Hash,
    V: Clone + Send + Sync + 'static,
{
    pub(crate) fn new(limits: CompileCacheLimits) -> Self {
        assert!(
            limits.ready_entries > 0,
            "compile cache must retain entries"
        );
        assert!(
            limits.max_in_flight_groups > 0,
            "compile cache must admit an in-flight group"
        );
        assert!(
            limits.max_in_flight_callers > 0,
            "compile cache must admit a caller"
        );
        Self {
            inner: Arc::new(CacheInner {
                state: Mutex::new(CacheState {
                    ready: LruCache::new(
                        NonZeroUsize::new(limits.ready_entries)
                            .expect("ready capacity was checked above"),
                    ),
                    ready_source_bytes: 0,
                    in_flight: HashMap::new(),
                    in_flight_source_bytes: 0,
                    in_flight_callers: 0,
                }),
                limits,
            }),
        }
    }

    pub(crate) fn cached(&self, key: &K) -> Result<Option<V>, LixError> {
        let mut state = self.lock()?;
        Ok(state.ready.get(key).map(|entry| entry.value.clone()))
    }

    pub(crate) async fn get_or_compile<F, Fut>(
        &self,
        key: K,
        caller_source_bytes: u64,
        initializer_copy_bytes: u64,
        compile: F,
    ) -> Result<V, LixError>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<V, LixError>> + Send,
    {
        let waiter = match self.reserve(key, caller_source_bytes, initializer_copy_bytes)? {
            Reservation::Ready(value) => return Ok(value),
            Reservation::InFlight(waiter) => waiter,
        };

        // OnceCell runs one initializer at a time. If that future is cancelled,
        // it resets and a remaining waiter can initialize the same bounded
        // group with its own closure.
        let result = waiter
            .flight
            .result
            .get_or_init(move || compile())
            .await
            .clone();
        self.finish_flight(&waiter.key, &waiter.flight, &result)?;
        result
    }

    fn reserve(
        &self,
        key: K,
        caller_source_bytes: u64,
        initializer_copy_bytes: u64,
    ) -> Result<Reservation<K, V>, LixError> {
        let mut state = self.lock()?;
        if let Some(ready) = state.ready.get(&key) {
            return Ok(Reservation::Ready(ready.value.clone()));
        }
        if state.in_flight_callers >= self.inner.limits.max_in_flight_callers {
            return Err(admission_error("compiled WASM caller limit reached"));
        }

        if state.in_flight.contains_key(&key) {
            if state
                .in_flight_source_bytes
                .saturating_add(caller_source_bytes)
                > self.inner.limits.max_in_flight_source_bytes
            {
                return Err(admission_error(
                    "compiled WASM source admission budget is exhausted",
                ));
            }
            let flight = {
                let entry = state
                    .in_flight
                    .get_mut(&key)
                    .expect("in-flight entry was checked above");
                if entry.flight.ready_source_bytes != caller_source_bytes
                    || entry.flight.initializer_copy_bytes != initializer_copy_bytes
                {
                    return Err(admission_error(
                        "compiled WASM source accounting differs for the same cache key",
                    ));
                }
                entry.waiters += 1;
                Arc::clone(&entry.flight)
            };
            state.in_flight_callers += 1;
            state.in_flight_source_bytes = state
                .in_flight_source_bytes
                .saturating_add(caller_source_bytes);
            return Ok(Reservation::InFlight(FlightWaiter {
                inner: Arc::clone(&self.inner),
                key,
                flight,
                caller_source_bytes,
            }));
        }

        if state.in_flight.len() >= self.inner.limits.max_in_flight_groups {
            return Err(admission_error(
                "too many distinct compiled WASM inputs are active",
            ));
        }
        let retained_bytes = caller_source_bytes.saturating_add(initializer_copy_bytes);
        // A single oversized input may make progress only while it is the sole
        // admitted compile. Its ready value will still be skipped if it also
        // exceeds the ready-cache budget.
        let exclusive_oversize = state.in_flight.is_empty()
            && state.in_flight_callers == 0
            && retained_bytes > self.inner.limits.max_in_flight_source_bytes;
        if !exclusive_oversize
            && state.in_flight_source_bytes.saturating_add(retained_bytes)
                > self.inner.limits.max_in_flight_source_bytes
        {
            return Err(admission_error(
                "compiled WASM source admission budget is exhausted",
            ));
        }

        let flight = Arc::new(Flight {
            result: OnceCell::new(),
            ready_source_bytes: caller_source_bytes,
            initializer_copy_bytes,
        });
        state.in_flight.insert(
            key.clone(),
            FlightEntry {
                flight: Arc::clone(&flight),
                waiters: 1,
                initializer_copy_reserved: true,
            },
        );
        state.in_flight_callers += 1;
        state.in_flight_source_bytes = state.in_flight_source_bytes.saturating_add(retained_bytes);
        Ok(Reservation::InFlight(FlightWaiter {
            inner: Arc::clone(&self.inner),
            key,
            flight,
            caller_source_bytes,
        }))
    }

    fn finish_flight(
        &self,
        key: &K,
        flight: &Arc<Flight<V>>,
        result: &Result<V, LixError>,
    ) -> Result<(), LixError> {
        let mut state = self.lock()?;
        let owns_entry = state
            .in_flight
            .get(key)
            .is_some_and(|entry| Arc::ptr_eq(&entry.flight, flight));
        if !owns_entry {
            return Ok(());
        }
        let release_initializer_copy = state
            .in_flight
            .get_mut(key)
            .filter(|entry| Arc::ptr_eq(&entry.flight, flight))
            .is_some_and(|entry| std::mem::replace(&mut entry.initializer_copy_reserved, false));
        if release_initializer_copy {
            state.in_flight_source_bytes = state
                .in_flight_source_bytes
                .saturating_sub(flight.initializer_copy_bytes);
        }
        if let Ok(value) = result {
            state.in_flight.remove(key);
            self.insert_ready(
                &mut state,
                key.clone(),
                flight.ready_source_bytes,
                value.clone(),
            );
        }
        Ok(())
    }

    fn insert_ready(&self, state: &mut CacheState<K, V>, key: K, source_bytes: u64, value: V) {
        if source_bytes > self.inner.limits.ready_source_bytes {
            return;
        }
        if let Some((_, previous)) = state.ready.pop_entry(&key) {
            state.ready_source_bytes = state
                .ready_source_bytes
                .saturating_sub(previous.source_bytes);
        }
        while state.ready.len() >= self.inner.limits.ready_entries
            || state.ready_source_bytes.saturating_add(source_bytes)
                > self.inner.limits.ready_source_bytes
        {
            let Some((_, evicted)) = state.ready.pop_lru() else {
                break;
            };
            state.ready_source_bytes = state
                .ready_source_bytes
                .saturating_sub(evicted.source_bytes);
        }
        state.ready.put(
            key,
            ReadyValue {
                source_bytes,
                value,
            },
        );
        state.ready_source_bytes = state.ready_source_bytes.saturating_add(source_bytes);
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, CacheState<K, V>>, LixError> {
        self.inner.state.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "compiled WASM cache lock poisoned",
            )
        })
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> CompileCacheSnapshot {
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        CompileCacheSnapshot {
            ready_entries: state.ready.len(),
            ready_source_bytes: state.ready_source_bytes,
            in_flight_groups: state.in_flight.len(),
            in_flight_source_bytes: state.in_flight_source_bytes,
            in_flight_callers: state.in_flight_callers,
        }
    }
}

enum Reservation<K: Eq + Hash, V> {
    Ready(V),
    InFlight(FlightWaiter<K, V>),
}

struct FlightWaiter<K: Eq + Hash, V> {
    inner: Arc<CacheInner<K, V>>,
    key: K,
    flight: Arc<Flight<V>>,
    caller_source_bytes: u64,
}

impl<K, V> Drop for FlightWaiter<K, V>
where
    K: Eq + Hash,
{
    fn drop(&mut self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.in_flight_callers = state.in_flight_callers.saturating_sub(1);
        state.in_flight_source_bytes = state
            .in_flight_source_bytes
            .saturating_sub(self.caller_source_bytes);
        let (remove_flight, release_initializer_copy) = state
            .in_flight
            .get_mut(&self.key)
            .filter(|entry| Arc::ptr_eq(&entry.flight, &self.flight))
            .map(|entry| {
                entry.waiters = entry.waiters.saturating_sub(1);
                let release_initializer_copy = if entry.waiters == 0 {
                    std::mem::replace(&mut entry.initializer_copy_reserved, false)
                } else {
                    false
                };
                (entry.waiters == 0, release_initializer_copy)
            })
            .unwrap_or((false, false));
        if remove_flight {
            state.in_flight.remove(&self.key);
        }
        if release_initializer_copy {
            state.in_flight_source_bytes = state
                .in_flight_source_bytes
                .saturating_sub(self.flight.initializer_copy_bytes);
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct CompileCacheSnapshot {
    pub(crate) ready_entries: usize,
    pub(crate) ready_source_bytes: u64,
    pub(crate) in_flight_groups: usize,
    pub(crate) in_flight_source_bytes: u64,
    pub(crate) in_flight_callers: usize,
}

fn admission_error(message: &str) -> LixError {
    LixError::new(LixError::CODE_PLUGIN_RESOURCE_LIMIT, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    fn small_limits() -> CompileCacheLimits {
        CompileCacheLimits {
            ready_entries: 3,
            ready_source_bytes: 8,
            max_in_flight_groups: 2,
            max_in_flight_callers: 2,
            max_in_flight_source_bytes: 12,
        }
    }

    async fn wait_for_callers<K, V>(cache: &BoundedCompileCache<K, V>, expected: usize)
    where
        K: Clone + Eq + Hash,
        V: Clone + Send + Sync + 'static,
    {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if cache.snapshot().in_flight_callers == expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("callers should reach the admitted flight");
    }

    #[tokio::test]
    async fn same_key_is_singleflight_and_ready_values_are_weighted_lru() {
        let cache = BoundedCompileCache::new(small_limits());
        let calls = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let first_cache = cache.clone();
        let first_calls = Arc::clone(&calls);
        let first_gate = Arc::clone(&gate);
        let first = tokio::spawn(async move {
            first_cache
                .get_or_compile(1_u8, 3, 3, move || async move {
                    first_calls.fetch_add(1, Ordering::SeqCst);
                    first_gate.acquire().await.unwrap().forget();
                    Ok(String::from("one"))
                })
                .await
        });
        wait_for_callers(&cache, 1).await;
        let second_cache = cache.clone();
        let second_calls = Arc::clone(&calls);
        let second_gate = Arc::clone(&gate);
        let second = tokio::spawn(async move {
            second_cache
                .get_or_compile(1_u8, 3, 3, move || async move {
                    second_calls.fetch_add(1, Ordering::SeqCst);
                    second_gate.acquire().await.unwrap().forget();
                    Ok(String::from("one"))
                })
                .await
        });
        wait_for_callers(&cache, 2).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        gate.add_permits(1);
        assert_eq!(first.await.unwrap().unwrap(), "one");
        assert_eq!(second.await.unwrap().unwrap(), "one");
        assert_eq!(cache.snapshot().in_flight_callers, 0);

        cache
            .get_or_compile(2_u8, 4, 0, || async { Ok(String::from("two")) })
            .await
            .unwrap();
        let active_value = cache.cached(&1).unwrap().expect("first value is cached");
        assert_eq!(active_value, "one");
        cache
            .get_or_compile(3_u8, 2, 0, || async { Ok(String::from("three")) })
            .await
            .unwrap();
        assert!(cache.cached(&2).unwrap().is_none());
        assert!(cache.cached(&1).unwrap().is_some());
        assert_eq!(cache.snapshot().ready_source_bytes, 5);

        assert!(cache.cached(&3).unwrap().is_some());
        cache
            .get_or_compile(4_u8, 4, 0, || async { Ok(String::from("four")) })
            .await
            .unwrap();
        assert!(cache.cached(&1).unwrap().is_none());
        assert!(cache.cached(&3).unwrap().is_some());
        assert_eq!(active_value, "one", "eviction preserves cloned handles");
        assert_eq!(cache.snapshot().ready_source_bytes, 6);

        cache
            .get_or_compile(5_u8, 9, 0, || async { Ok(String::from("oversize")) })
            .await
            .unwrap();
        assert!(cache.cached(&5).unwrap().is_none());
        assert_eq!(cache.snapshot().ready_source_bytes, 6);
    }

    #[tokio::test]
    async fn errors_retry_and_admission_bounds_count_groups_callers_and_bytes() {
        let cache = BoundedCompileCache::new(small_limits());
        let failed_calls = AtomicUsize::new(0);
        for _ in 0..2 {
            assert!(
                cache
                    .get_or_compile(1_u8, 2, 2, || async {
                        failed_calls.fetch_add(1, Ordering::SeqCst);
                        Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "compile failed",
                        ))
                    })
                    .await
                    .is_err()
            );
        }
        assert_eq!(failed_calls.load(Ordering::SeqCst), 2);
        assert!(cache.cached(&1).unwrap().is_none(), "errors are not cached");

        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let cache_for_first = cache.clone();
        let gate_for_first = Arc::clone(&gate);
        let first = tokio::spawn(async move {
            cache_for_first
                .get_or_compile(2_u8, 4, 2, move || async move {
                    gate_for_first.acquire().await.unwrap().forget();
                    Ok(2_u8)
                })
                .await
        });
        wait_for_callers(&cache, 1).await;
        let byte_error = cache
            .get_or_compile(2_u8, 7, 2, || async { Ok(2_u8) })
            .await
            .expect_err("aggregate source budget must reject a joined caller");
        assert_eq!(byte_error.code, LixError::CODE_PLUGIN_RESOURCE_LIMIT);
        let joined_cache = cache.clone();
        let joined_gate = Arc::clone(&gate);
        let joined = tokio::spawn(async move {
            joined_cache
                .get_or_compile(2_u8, 4, 2, move || async move {
                    joined_gate.acquire().await.unwrap().forget();
                    Ok(2_u8)
                })
                .await
        });
        wait_for_callers(&cache, 2).await;
        let caller_error = cache
            .get_or_compile(2_u8, 0, 2, || async { Ok(2_u8) })
            .await
            .expect_err("caller limit must reject excess joined calls");
        assert_eq!(caller_error.code, LixError::CODE_PLUGIN_RESOURCE_LIMIT);
        gate.add_permits(1);
        assert_eq!(first.await.unwrap().unwrap(), 2);
        assert_eq!(joined.await.unwrap().unwrap(), 2);
        assert_eq!(cache.snapshot().in_flight_callers, 0);

        let group_cache = BoundedCompileCache::new(CompileCacheLimits {
            max_in_flight_groups: 1,
            max_in_flight_callers: 3,
            ..small_limits()
        });
        let group_gate = Arc::new(tokio::sync::Semaphore::new(0));
        let first_group_cache = group_cache.clone();
        let first_group_gate = Arc::clone(&group_gate);
        let group_task = tokio::spawn(async move {
            first_group_cache
                .get_or_compile(6_u8, 1, 0, move || async move {
                    first_group_gate.acquire().await.unwrap().forget();
                    Ok(6_u8)
                })
                .await
        });
        wait_for_callers(&group_cache, 1).await;
        let group_error = group_cache
            .get_or_compile(7_u8, 1, 0, || async { Ok(7_u8) })
            .await
            .expect_err("distinct group limit must reject without queuing");
        assert_eq!(group_error.code, LixError::CODE_PLUGIN_RESOURCE_LIMIT);
        group_gate.add_permits(1);
        assert_eq!(group_task.await.unwrap().unwrap(), 6);
    }

    #[tokio::test]
    async fn cancellation_keeps_joined_group_retryable_and_releases_final_waiter() {
        let cache = BoundedCompileCache::new(small_limits());
        let starts = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let first_cache = cache.clone();
        let first_starts = Arc::clone(&starts);
        let first_release = Arc::clone(&release);
        let first = tokio::spawn(async move {
            first_cache
                .get_or_compile(9_u8, 1, 1, move || async move {
                    first_starts.add_permits(1);
                    first_release.acquire().await.unwrap().forget();
                    Ok(9_u8)
                })
                .await
        });
        starts.acquire().await.unwrap().forget();
        let second_cache = cache.clone();
        let second_starts = Arc::clone(&starts);
        let second_release = Arc::clone(&release);
        let second = tokio::spawn(async move {
            second_cache
                .get_or_compile(9_u8, 1, 1, move || async move {
                    second_starts.add_permits(1);
                    second_release.acquire().await.unwrap().forget();
                    Ok(9_u8)
                })
                .await
        });
        wait_for_callers(&cache, 2).await;
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        starts.acquire().await.unwrap().forget();
        assert_eq!(cache.snapshot().in_flight_groups, 1);
        assert_eq!(cache.snapshot().in_flight_source_bytes, 2);
        release.add_permits(1);
        assert_eq!(second.await.unwrap().unwrap(), 9);
        assert_eq!(cache.snapshot().in_flight_groups, 0);
        assert_eq!(cache.snapshot().in_flight_callers, 0);

        let final_waiter = cache.clone();
        let task = tokio::spawn(async move {
            final_waiter
                .get_or_compile(10_u8, 1, 1, || async {
                    std::future::pending::<Result<u8, LixError>>().await
                })
                .await
        });
        wait_for_callers(&cache, 1).await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let snapshot = cache.snapshot();
        assert_eq!(snapshot.in_flight_groups, 0);
        assert_eq!(snapshot.in_flight_callers, 0);
        assert_eq!(snapshot.in_flight_source_bytes, 0);
        assert_eq!(
            cache
                .get_or_compile(10_u8, 1, 0, || async { Ok(10) })
                .await
                .unwrap(),
            10
        );
    }
}
