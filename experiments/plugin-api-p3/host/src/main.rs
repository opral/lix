use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use std::env;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll, Waker};
use std::time::{Duration, Instant};
use wasmtime::component::{
    Access, Accessor, Component, FutureConsumer, FutureReader, HasData, Linker, Resource,
    ResourceAny, ResourceTable, Source, StreamConsumer, StreamReader, StreamResult,
};
use wasmtime::{Config, Engine, ResourceLimiter, Store, StoreContextMut};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

#[derive(Debug)]
pub struct HostByteSource {
    bytes: Bytes,
    counters: Arc<Mutex<SourceCounters>>,
    stream_short_by: usize,
    stream_terminal_error: bool,
}

mod bindings {
    pub use super::HostByteSource;

    wasmtime::component::bindgen!({
        path: "../wit",
        world: "plugin",
        imports: { default: store | trappable },
        exports: { default: async | store },
        with: {
            "lix:plugin-p3-candidate/host.byte-source": HostByteSource,
        },
    });
}

use bindings::Plugin;
use bindings::exports::lix::plugin_p3_candidate::api;
use bindings::lix::plugin_p3_candidate::host;

const TARGET_BYTES: usize = 10 * 1024 * 1024;
const MIB: usize = 1024 * 1024;
const DEFAULT_GUEST_LINEAR_MEMORY_LIMIT_MIB: usize = 64;
const DEFAULT_MAX_ENTITY_SUMMARIES: usize = 1_000_000;
const DEFAULT_HOT_WARMUPS: usize = 2_400;
const DEFAULT_HOT_SAMPLES: usize = 24_000;
const PROPERTY_VALUE_BYTES: usize = 240;
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Clone, Copy, Debug)]
struct Settings {
    cold_warmups: usize,
    cold_samples: usize,
    warm_warmups: usize,
    warm_samples: usize,
    hot_warmups: usize,
    hot_samples: usize,
    hot_print_raw: bool,
    guest_linear_memory_limit_bytes: usize,
    max_entity_summaries: usize,
}

impl Settings {
    fn from_env() -> Result<Self> {
        let guest_linear_memory_limit_mib = env_usize(
            "LIX_P3_GUEST_LINEAR_MEMORY_LIMIT_MIB",
            DEFAULT_GUEST_LINEAR_MEMORY_LIMIT_MIB,
        )?;
        let guest_linear_memory_limit_bytes = guest_linear_memory_limit_mib
            .checked_mul(MIB)
            .context("LIX_P3_GUEST_LINEAR_MEMORY_LIMIT_MIB is too large")?;
        Ok(Self {
            cold_warmups: env_usize("LIX_P3_COLD_WARMUPS", 3)?,
            cold_samples: env_usize("LIX_P3_COLD_SAMPLES", 20)?,
            warm_warmups: env_usize("LIX_P3_WARM_WARMUPS", 10)?,
            warm_samples: env_usize("LIX_P3_WARM_SAMPLES", 100)?,
            hot_warmups: env_usize("LIX_P3_HOT_WARMUPS", DEFAULT_HOT_WARMUPS)?,
            hot_samples: env_usize("LIX_P3_HOT_SAMPLES", DEFAULT_HOT_SAMPLES)?,
            hot_print_raw: env_bool("LIX_P3_HOT_PRINT_RAW", false)?,
            guest_linear_memory_limit_bytes,
            max_entity_summaries: env_usize(
                "LIX_P3_MAX_ENTITY_SUMMARIES",
                DEFAULT_MAX_ENTITY_SUMMARIES,
            )?,
        })
    }
}

fn env_bool(name: &str, default: bool) -> Result<bool> {
    match env::var(name) {
        Ok(value) => match value.as_str() {
            "0" | "false" => Ok(false),
            "1" | "true" => Ok(true),
            _ => bail!("{name} must be one of: 0, 1, false, true"),
        },
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("read {name}")),
    }
}

fn env_usize(name: &str, default: usize) -> Result<usize> {
    match env::var(name) {
        Ok(value) => {
            let parsed = value
                .parse::<usize>()
                .with_context(|| format!("{name} must be a positive integer"))?;
            if parsed == 0 {
                bail!("{name} must be positive");
            }
            Ok(parsed)
        }
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("read {name}")),
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct SourceCounters {
    fork_calls: u64,
    len_calls: u64,
    read_calls: u64,
    read_bytes: u64,
    stream_calls: u64,
    stream_bytes: u64,
    stream_emitted_bytes: u64,
    drop_calls: u64,
}

impl SourceCounters {
    fn add_assign(&mut self, other: &Self) {
        self.fork_calls += other.fork_calls;
        self.len_calls += other.len_calls;
        self.read_calls += other.read_calls;
        self.read_bytes += other.read_bytes;
        self.stream_calls += other.stream_calls;
        self.stream_bytes += other.stream_bytes;
        self.stream_emitted_bytes += other.stream_emitted_bytes;
        self.drop_calls += other.drop_calls;
    }
}

struct State {
    ctx: WasiCtx,
    table: ResourceTable,
    /// Largest accepted size of any one guest linear memory.
    peak_guest_linear_bytes: usize,
    guest_linear_memory_limit_bytes: usize,
    max_entity_summaries: usize,
}

impl State {
    fn new(guest_linear_memory_limit_bytes: usize, max_entity_summaries: usize) -> Self {
        Self {
            ctx: WasiCtx::default(),
            table: ResourceTable::default(),
            peak_guest_linear_bytes: 0,
            guest_linear_memory_limit_bytes,
            max_entity_summaries,
        }
    }
}

impl WasiView for State {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.ctx,
            table: &mut self.table,
        }
    }
}

impl ResourceLimiter for State {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        if desired > self.guest_linear_memory_limit_bytes {
            return Ok(false);
        }
        self.peak_guest_linear_bytes = self.peak_guest_linear_bytes.max(desired);
        Ok(true)
    }

    fn table_growing(
        &mut self,
        _current: usize,
        _desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        Ok(true)
    }
}

struct Host;

impl HasData for Host {
    type Data<'a> = &'a mut State;
}

impl host::Host for &mut State {}
impl host::HostByteSource for &mut State {}

fn read_source(
    state: &mut State,
    source: Resource<HostByteSource>,
    offset: u64,
    length: u32,
) -> wasmtime::Result<Result<Vec<u8>, host::SourceError>> {
    let (bytes, counters) = {
        let source = state.table.get(&source)?;
        (source.bytes.clone(), Arc::clone(&source.counters))
    };
    let start = match usize::try_from(offset) {
        Ok(start) if start <= bytes.len() => start,
        _ => return Ok(Err(host::SourceError::InvalidRange)),
    };
    let requested = usize::try_from(length).expect("u32 fits usize");
    let Some(end) = start.checked_add(requested) else {
        return Ok(Err(host::SourceError::InvalidRange));
    };
    if end > bytes.len() {
        return Ok(Err(host::SourceError::InvalidRange));
    }
    let result = bytes.slice(start..end).to_vec();
    let mut counters = counters.lock().expect("source counters mutex poisoned");
    counters.read_calls += 1;
    counters.read_bytes += u64::from(length);
    Ok(Ok(result))
}

impl host::HostByteSourceWithStore<State> for Host {
    fn fork(
        mut store: Access<State, Self>,
        source: Resource<HostByteSource>,
    ) -> wasmtime::Result<Resource<HostByteSource>> {
        let (bytes, counters, stream_short_by, stream_terminal_error) = {
            let state = store.get();
            let source = state.table.get(&source)?;
            (
                source.bytes.clone(),
                Arc::clone(&source.counters),
                source.stream_short_by,
                source.stream_terminal_error,
            )
        };
        counters
            .lock()
            .expect("source counters mutex poisoned")
            .fork_calls += 1;
        Ok(store.get().table.push(HostByteSource {
            bytes,
            counters,
            stream_short_by,
            stream_terminal_error,
        })?)
    }

    fn len(
        mut store: Access<State, Self>,
        source: Resource<HostByteSource>,
    ) -> wasmtime::Result<u64> {
        let (len, counters) = {
            let state = store.get();
            let source = state.table.get(&source)?;
            (
                u64::try_from(source.bytes.len()).expect("usize fits u64"),
                Arc::clone(&source.counters),
            )
        };
        counters
            .lock()
            .expect("source counters mutex poisoned")
            .len_calls += 1;
        Ok(len)
    }

    fn read(
        mut store: Access<State, Self>,
        source: Resource<HostByteSource>,
        offset: u64,
        length: u32,
    ) -> wasmtime::Result<Result<Vec<u8>, host::SourceError>> {
        read_source(store.get(), source, offset, length)
    }

    async fn read_async(
        accessor: &Accessor<State, Self>,
        source: Resource<HostByteSource>,
        offset: u64,
        length: u32,
    ) -> wasmtime::Result<Result<Vec<u8>, host::SourceError>> {
        accessor.with(|mut store| read_source(store.get(), source, offset, length))
    }

    fn read_stream(
        mut store: Access<State, Self>,
        source: Resource<HostByteSource>,
        offset: u64,
        length: u64,
    ) -> wasmtime::Result<Result<host::SourceByteStream, host::SourceError>> {
        let (bytes, counters, stream_short_by, stream_terminal_error) = {
            let state = store.get();
            let source = state.table.get(&source)?;
            (
                source.bytes.clone(),
                Arc::clone(&source.counters),
                source.stream_short_by,
                source.stream_terminal_error,
            )
        };
        let start = match usize::try_from(offset) {
            Ok(start) if start <= bytes.len() => start,
            _ => return Ok(Err(host::SourceError::InvalidRange)),
        };
        let requested = match usize::try_from(length) {
            Ok(length) => length,
            Err(_) => return Ok(Err(host::SourceError::InvalidRange)),
        };
        let Some(end) = start.checked_add(requested) else {
            return Ok(Err(host::SourceError::InvalidRange));
        };
        if end > bytes.len() {
            return Ok(Err(host::SourceError::InvalidRange));
        }
        let emitted_end = end.saturating_sub(stream_short_by.min(requested));
        {
            let mut counters = counters.lock().expect("source counters mutex poisoned");
            counters.stream_calls += 1;
            counters.stream_bytes += length;
            counters.stream_emitted_bytes +=
                u64::try_from(emitted_end - start).expect("usize fits u64");
        }
        let data = StreamReader::new(&mut store, bytes.slice(start..emitted_end))?;
        let done = FutureReader::new(&mut store, async move {
            let result = if stream_terminal_error {
                Err(host::SourceError::Unavailable(
                    "injected terminal failure".to_owned(),
                ))
            } else {
                Ok(())
            };
            Ok::<_, wasmtime::Error>(result)
        })?;
        Ok(Ok(host::SourceByteStream { data, done }))
    }

    fn drop(
        mut store: Access<State, Self>,
        source: Resource<HostByteSource>,
    ) -> wasmtime::Result<()> {
        let source = store.get().table.delete(source)?;
        source
            .counters
            .lock()
            .expect("source counters mutex poisoned")
            .drop_calls += 1;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ExpectedEntity {
    entity_id: u64,
    start: u64,
    length: u32,
    hash: u64,
}

#[derive(Debug)]
struct Fixture {
    before: Bytes,
    after: Bytes,
    before_entities: Vec<ExpectedEntity>,
    after_entities: Vec<ExpectedEntity>,
    changed_index: usize,
    original_member: Bytes,
    changed_member: Bytes,
    edit_offset: u64,
    removed: u8,
    inserted: u8,
}

fn json_fixture() -> Fixture {
    let mut bytes = Vec::with_capacity(TARGET_BYTES + PROPERTY_VALUE_BYTES + 64);
    let mut entities = Vec::new();
    let mut value_offsets = Vec::new();
    bytes.push(b'{');
    let mut index = 0_u64;
    while bytes.len() + 1 < TARGET_BYTES {
        if index != 0 {
            bytes.push(b',');
        }
        let start = bytes.len();
        let key = format!("property-{index:08}");
        bytes.push(b'"');
        bytes.extend_from_slice(key.as_bytes());
        bytes.extend_from_slice(b"\":\"");
        let value_start = bytes.len();
        for position in 0..PROPERTY_VALUE_BYTES {
            let value =
                b'a' + u8::try_from((index as usize + position * 7) % 26).expect("mod 26 fits");
            bytes.push(value);
        }
        bytes.push(b'"');
        let end = bytes.len();
        let member = &bytes[start..end];
        entities.push(ExpectedEntity {
            entity_id: fnv1a(key.as_bytes()),
            start: u64::try_from(start).expect("usize fits u64"),
            length: u32::try_from(end - start).expect("small property fits u32"),
            hash: fnv1a(member),
        });
        value_offsets.push(value_start);
        index += 1;
    }
    bytes.push(b'}');

    let changed_index = entities.len() / 2;
    let edit_position = value_offsets[changed_index] + PROPERTY_VALUE_BYTES / 2;
    let mut after = bytes.clone();
    let removed = after[edit_position];
    let inserted = if after[edit_position] == b'z' {
        b'y'
    } else {
        after[edit_position] + 1
    };
    after[edit_position] = inserted;
    let entity = &entities[changed_index];
    let member_start = usize::try_from(entity.start).expect("fixture offset fits usize");
    let member_end = member_start + usize::try_from(entity.length).expect("length fits usize");
    let original_member = Bytes::copy_from_slice(&bytes[member_start..member_end]);
    let changed_member = Bytes::copy_from_slice(&after[member_start..member_end]);
    let mut after_entities = entities.clone();
    after_entities[changed_index].hash = fnv1a(&after[member_start..member_end]);

    Fixture {
        before: Bytes::from(bytes),
        after: Bytes::from(after),
        before_entities: entities,
        after_entities,
        changed_index,
        original_member,
        changed_member,
        edit_offset: u64::try_from(edit_position).expect("usize fits u64"),
        removed,
        inserted,
    }
}

fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(FNV_OFFSET, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}

struct SignalState<T> {
    value: Option<T>,
    closed: bool,
    waker: Option<Waker>,
}

struct SignalSender<T> {
    state: Arc<Mutex<SignalState<T>>>,
    sent: bool,
}

struct SignalReceiver<T> {
    state: Arc<Mutex<SignalState<T>>>,
}

fn signal<T>() -> (SignalSender<T>, SignalReceiver<T>) {
    let state = Arc::new(Mutex::new(SignalState {
        value: None,
        closed: false,
        waker: None,
    }));
    (
        SignalSender {
            state: Arc::clone(&state),
            sent: false,
        },
        SignalReceiver { state },
    )
}

impl<T> SignalSender<T> {
    fn send(mut self, value: T) {
        let waker = {
            let mut state = self.state.lock().expect("signal mutex poisoned");
            state.value = Some(value);
            state.closed = true;
            state.waker.take()
        };
        self.sent = true;
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<T> Drop for SignalSender<T> {
    fn drop(&mut self) {
        if self.sent {
            return;
        }
        let waker = {
            let mut state = self.state.lock().expect("signal mutex poisoned");
            state.closed = true;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<T> Future for SignalReceiver<T> {
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let mut state = self.state.lock().expect("signal mutex poisoned");
        if let Some(value) = state.value.take() {
            Poll::Ready(Ok(value))
        } else if state.closed {
            Poll::Ready(Err(anyhow!(
                "component stream/future closed without a value"
            )))
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

struct SummaryCollector {
    items: Vec<api::EntitySummary>,
    max_items: usize,
    sender: Option<SignalSender<Vec<api::EntitySummary>>>,
}

impl StreamConsumer<State> for SummaryCollector {
    type Item = api::EntitySummary;

    fn poll_consume(
        mut self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
        mut store: StoreContextMut<State>,
        mut source: Source<'_, Self::Item>,
        finish: bool,
    ) -> Poll<wasmtime::Result<StreamResult>> {
        let remaining = source.remaining(&mut store);
        if remaining == 0 {
            return Poll::Ready(Ok(if finish {
                StreamResult::Cancelled
            } else {
                StreamResult::Completed
            }));
        }
        let Some(total) = self.items.len().checked_add(remaining) else {
            return Poll::Ready(Err(wasmtime::Error::msg("entity output count overflow")));
        };
        if total > self.max_items {
            return Poll::Ready(Err(wasmtime::Error::msg(format!(
                "entity output exceeded host limit: {total} > {}",
                self.max_items
            ))));
        }
        if let Err(error) = self.items.try_reserve(remaining) {
            return Poll::Ready(Err(wasmtime::Error::msg(format!(
                "reserve entity output: {error}"
            ))));
        }
        source.read(&mut store, &mut self.items)?;
        Poll::Ready(Ok(StreamResult::Completed))
    }
}

impl Drop for SummaryCollector {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            sender.send(std::mem::take(&mut self.items));
        }
    }
}

struct ChangeCollector {
    items: Vec<api::EntityChange>,
    max_items: usize,
    sender: Option<SignalSender<Vec<api::EntityChange>>>,
}

impl StreamConsumer<State> for ChangeCollector {
    type Item = api::EntityChange;

    fn poll_consume(
        mut self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
        mut store: StoreContextMut<State>,
        mut source: Source<'_, Self::Item>,
        finish: bool,
    ) -> Poll<wasmtime::Result<StreamResult>> {
        let remaining = source.remaining(&mut store);
        if remaining == 0 {
            return Poll::Ready(Ok(if finish {
                StreamResult::Cancelled
            } else {
                StreamResult::Completed
            }));
        }
        let Some(total) = self.items.len().checked_add(remaining) else {
            return Poll::Ready(Err(wasmtime::Error::msg("change output count overflow")));
        };
        if total > self.max_items {
            return Poll::Ready(Err(wasmtime::Error::msg(format!(
                "change output exceeded host limit: {total} > {}",
                self.max_items
            ))));
        }
        if let Err(error) = self.items.try_reserve(remaining) {
            return Poll::Ready(Err(wasmtime::Error::msg(format!(
                "reserve change output: {error}"
            ))));
        }
        source.read(&mut store, &mut self.items)?;
        Poll::Ready(Ok(StreamResult::Completed))
    }
}

impl Drop for ChangeCollector {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            sender.send(std::mem::take(&mut self.items));
        }
    }
}

struct CancelCollector;

impl StreamConsumer<State> for CancelCollector {
    type Item = api::EntitySummary;

    fn poll_consume(
        self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
        _store: StoreContextMut<State>,
        _source: Source<'_, Self::Item>,
        _finish: bool,
    ) -> Poll<wasmtime::Result<StreamResult>> {
        Poll::Ready(Ok(StreamResult::Dropped))
    }
}

struct TerminalCollector {
    sender: Option<SignalSender<Result<(), api::PluginError>>>,
}

impl FutureConsumer<State> for TerminalCollector {
    type Item = Result<(), api::PluginError>;

    fn poll_consume(
        mut self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
        mut store: StoreContextMut<State>,
        mut source: Source<'_, Self::Item>,
        finish: bool,
    ) -> Poll<wasmtime::Result<()>> {
        let mut value = None;
        source.read(&mut store, &mut value)?;
        if let Some(value) = value {
            if let Some(sender) = self.sender.take() {
                sender.send(value);
            }
        } else if finish {
            self.sender.take();
        } else {
            return Poll::Pending;
        }
        Poll::Ready(Ok(()))
    }
}

async fn drain_entities(
    accessor: &Accessor<State>,
    stream: api::EntitySummaryStream,
    max_items: usize,
) -> Result<(u64, Vec<api::EntitySummary>)> {
    let declared_count = stream.count;
    let declared_count_usize = usize::try_from(declared_count)
        .context("declared entity count does not fit the host address space")?;
    if declared_count_usize > max_items {
        bail!("declared entity count exceeded host limit: {declared_count_usize} > {max_items}");
    }
    let (items_sender, items_receiver) = signal();
    let (done_sender, done_receiver) = signal();
    accessor.with(|mut store| -> wasmtime::Result<()> {
        stream.items.pipe(
            &mut store,
            SummaryCollector {
                items: Vec::with_capacity(declared_count_usize.min(4096)),
                max_items,
                sender: Some(items_sender),
            },
        )?;
        stream.done.pipe(
            &mut store,
            TerminalCollector {
                sender: Some(done_sender),
            },
        )?;
        wasmtime::Result::Ok(())
    })?;
    let (items, done) = tokio::join!(items_receiver, done_receiver);
    let items = items?;
    match done? {
        Ok(()) if items.len() == declared_count_usize => Ok((declared_count, items)),
        Ok(()) => bail!(
            "entity stream count mismatch: declared {declared_count}, received {}",
            items.len()
        ),
        Err(error) => bail!("entity stream terminal error: {error:?}"),
    }
}

async fn drain_changes(
    accessor: &Accessor<State>,
    stream: api::EntityChangeStream,
    max_items: usize,
) -> Result<Vec<api::EntityChange>> {
    let declared_count = stream.count;
    let declared_count_usize = usize::try_from(declared_count)
        .context("declared change count does not fit the host address space")?;
    if declared_count_usize > max_items {
        bail!("declared change count exceeded host limit: {declared_count_usize} > {max_items}");
    }
    let (items_sender, items_receiver) = signal();
    let (done_sender, done_receiver) = signal();
    accessor.with(|mut store| -> wasmtime::Result<()> {
        stream.items.pipe(
            &mut store,
            ChangeCollector {
                items: Vec::with_capacity(declared_count_usize.min(16)),
                max_items,
                sender: Some(items_sender),
            },
        )?;
        stream.done.pipe(
            &mut store,
            TerminalCollector {
                sender: Some(done_sender),
            },
        )?;
        Ok(())
    })?;
    let (items, done) = tokio::join!(items_receiver, done_receiver);
    let items = items?;
    match done? {
        Ok(()) if items.len() == declared_count_usize => Ok(items),
        Ok(()) => bail!(
            "change stream count mismatch: declared {declared_count}, received {}",
            items.len()
        ),
        Err(error) => bail!("change stream terminal error: {error:?}"),
    }
}

struct Opened {
    document: ResourceAny,
    declared_count: u64,
    summaries: Vec<api::EntitySummary>,
}

struct Case {
    plugin: Plugin,
    store: Store<State>,
}

impl Case {
    async fn new(
        engine: &Engine,
        component: &Component,
        linker: &Linker<State>,
        guest_linear_memory_limit_bytes: usize,
        max_entity_summaries: usize,
    ) -> Result<Self> {
        let mut store = Store::new(
            engine,
            State::new(guest_linear_memory_limit_bytes, max_entity_summaries),
        );
        store.limiter(|state| state);
        let plugin = Plugin::instantiate_async(&mut store, component, linker)
            .await
            .map_err(|error| anyhow!("instantiate candidate component: {error:?}"))?;
        Ok(Self { plugin, store })
    }

    fn push_source(
        &mut self,
        bytes: Bytes,
    ) -> Result<(Resource<HostByteSource>, Arc<Mutex<SourceCounters>>)> {
        let counters = Arc::new(Mutex::new(SourceCounters::default()));
        let resource = self.store.data_mut().table.push(HostByteSource {
            bytes,
            counters: Arc::clone(&counters),
            stream_short_by: 0,
            stream_terminal_error: false,
        })?;
        Ok((resource, counters))
    }

    fn push_short_stream_source(
        &mut self,
        bytes: Bytes,
        short_by: usize,
    ) -> Result<(Resource<HostByteSource>, Arc<Mutex<SourceCounters>>)> {
        if short_by == 0 || short_by > bytes.len() {
            bail!("short stream fault must omit between one byte and the complete source");
        }
        let counters = Arc::new(Mutex::new(SourceCounters::default()));
        let resource = self.store.data_mut().table.push(HostByteSource {
            bytes,
            counters: Arc::clone(&counters),
            stream_short_by: short_by,
            stream_terminal_error: false,
        })?;
        Ok((resource, counters))
    }

    fn push_terminal_error_source(
        &mut self,
        bytes: Bytes,
    ) -> Result<(Resource<HostByteSource>, Arc<Mutex<SourceCounters>>)> {
        let counters = Arc::new(Mutex::new(SourceCounters::default()));
        let resource = self.store.data_mut().table.push(HostByteSource {
            bytes,
            counters: Arc::clone(&counters),
            stream_short_by: 0,
            stream_terminal_error: true,
        })?;
        Ok((resource, counters))
    }

    async fn open_list(&mut self, bytes: Vec<u8>) -> Result<Opened> {
        let api = self.plugin.lix_plugin_p3_candidate_api().clone();
        let max_items = self.store.data().max_entity_summaries;
        self.store
            .run_concurrent(async move |accessor| {
                let result = api.call_open_list(accessor, bytes).await?;
                let result =
                    result.map_err(|error| anyhow!("open-list plugin error: {error:?}"))?;
                let (declared_count, summaries) =
                    drain_entities(accessor, result.entities, max_items).await?;
                Result::<Opened>::Ok(Opened {
                    document: result.document,
                    declared_count,
                    summaries,
                })
            })
            .await?
    }

    async fn open_stream(&mut self, source: Resource<HostByteSource>) -> Result<Opened> {
        let api = self.plugin.lix_plugin_p3_candidate_api().clone();
        let max_items = self.store.data().max_entity_summaries;
        self.store
            .run_concurrent(async move |accessor| {
                let result = api.call_open_stream(accessor, source).await?;
                let result =
                    result.map_err(|error| anyhow!("open-stream plugin error: {error:?}"))?;
                let (declared_count, summaries) =
                    drain_entities(accessor, result.entities, max_items).await?;
                Result::<Opened>::Ok(Opened {
                    document: result.document,
                    declared_count,
                    summaries,
                })
            })
            .await?
    }

    async fn open_list_cancel_output(&mut self, bytes: Vec<u8>) -> Result<ResourceAny> {
        let api = self.plugin.lix_plugin_p3_candidate_api().clone();
        self.store
            .run_concurrent(async move |accessor| {
                let result = api.call_open_list(accessor, bytes).await?;
                let result =
                    result.map_err(|error| anyhow!("open-list plugin error: {error:?}"))?;
                let (done_sender, done_receiver) = signal();
                accessor.with(|mut store| -> wasmtime::Result<()> {
                    result.entities.items.pipe(&mut store, CancelCollector)?;
                    result.entities.done.pipe(
                        &mut store,
                        TerminalCollector {
                            sender: Some(done_sender),
                        },
                    )?;
                    Ok(())
                })?;
                match done_receiver.await? {
                    Err(api::PluginError::Cancelled) => Ok(result.document),
                    other => bail!("cancelled output terminal mismatch: {other:?}"),
                }
            })
            .await?
    }

    async fn stats(&mut self, document: ResourceAny) -> Result<api::DocumentStats> {
        let api = self.plugin.lix_plugin_p3_candidate_api().clone();
        self.store
            .run_concurrent(async move |accessor| {
                api.document().call_stats(accessor, document).await
            })
            .await?
            .map_err(Into::into)
    }

    async fn fork(&mut self, document: ResourceAny) -> Result<ResourceAny> {
        let api = self.plugin.lix_plugin_p3_candidate_api().clone();
        self.store
            .run_concurrent(async move |accessor| {
                api.document().call_fork(accessor, document).await
            })
            .await?
            .map_err(Into::into)
    }

    async fn file_changed(
        &mut self,
        document: ResourceAny,
        before: Resource<HostByteSource>,
        after: Resource<HostByteSource>,
        edit_offset: u64,
        inserted: u8,
    ) -> Result<api::FileTransition> {
        self.file_changed_sync_inline(
            document,
            before,
            after,
            vec![api::InputSplice {
                offset: edit_offset,
                delete_len: 1,
                insert: vec![inserted],
            }],
        )
        .await
    }

    async fn file_changed_sync_inline(
        &mut self,
        document: ResourceAny,
        before: Resource<HostByteSource>,
        after: Resource<HostByteSource>,
        edits: Vec<api::InputSplice>,
    ) -> Result<api::FileTransition> {
        let api = self.plugin.lix_plugin_p3_candidate_api().clone();
        self.store
            .run_concurrent(async move |accessor| {
                let result = api
                    .document()
                    .call_file_changed(accessor, document, before, after, edits)
                    .await?;
                result.map_err(|error| anyhow!("file-changed plugin error: {error:?}"))
            })
            .await?
    }

    async fn file_changed_async_inline(
        &mut self,
        document: ResourceAny,
        before: Resource<HostByteSource>,
        after: Resource<HostByteSource>,
        edits: Vec<api::InputSplice>,
    ) -> Result<api::FileTransition> {
        let api = self.plugin.lix_plugin_p3_candidate_api().clone();
        self.store
            .run_concurrent(async move |accessor| {
                let result = api
                    .document()
                    .call_file_changed_async_inline(accessor, document, before, after, edits)
                    .await?;
                result.map_err(|error| anyhow!("async-inline plugin error: {error:?}"))
            })
            .await?
    }

    async fn file_changed_async_read_inline(
        &mut self,
        document: ResourceAny,
        before: Resource<HostByteSource>,
        after: Resource<HostByteSource>,
        edits: Vec<api::InputSplice>,
    ) -> Result<api::FileTransition> {
        let api = self.plugin.lix_plugin_p3_candidate_api().clone();
        self.store
            .run_concurrent(async move |accessor| {
                let result = api
                    .document()
                    .call_file_changed_async_read_inline(accessor, document, before, after, edits)
                    .await?;
                result.map_err(|error| anyhow!("async-read-inline plugin error: {error:?}"))
            })
            .await?
    }

    async fn file_changed_async_stream(
        &mut self,
        document: ResourceAny,
        before: Resource<HostByteSource>,
        after: Resource<HostByteSource>,
        edits: Vec<api::InputSplice>,
    ) -> Result<api::FileTransition> {
        let api = self.plugin.lix_plugin_p3_candidate_api().clone();
        let max_items = self.store.data().max_entity_summaries;
        self.store
            .run_concurrent(async move |accessor| {
                let result = api
                    .document()
                    .call_file_changed_async_stream(accessor, document, before, after, edits)
                    .await?;
                let result =
                    result.map_err(|error| anyhow!("async-stream plugin error: {error:?}"))?;
                let changes = drain_changes(accessor, result.changes, max_items).await?;
                Ok(api::FileTransition {
                    document: result.document,
                    changes,
                })
            })
            .await?
    }

    async fn drop_document(&mut self, document: ResourceAny) -> Result<()> {
        document.resource_drop_async(&mut self.store).await?;
        Ok(())
    }

    fn peak_guest_linear_bytes(&self) -> usize {
        self.store.data().peak_guest_linear_bytes
    }

    fn assert_memory_limit(&self) -> Result<()> {
        let peak = self.peak_guest_linear_bytes();
        let limit = self.store.data().guest_linear_memory_limit_bytes;
        if peak > limit {
            bail!("guest linear-memory peak exceeded configured limit: {peak} > {limit}");
        }
        Ok(())
    }

    fn assert_host_table_empty(&self) -> Result<()> {
        if !self.store.data().table.is_empty() {
            bail!("host resource table retained an entry after hot-path cleanup");
        }
        Ok(())
    }
}

fn validate_opened(opened: &Opened, expected: &[ExpectedEntity]) -> Result<()> {
    let expected_count = u64::try_from(expected.len()).expect("usize fits u64");
    if opened.declared_count != expected_count {
        bail!(
            "declared entity count mismatch: got {}, expected {expected_count}",
            opened.declared_count
        );
    }
    if opened.summaries.len() != expected.len() {
        bail!(
            "drained entity count mismatch: got {}, expected {}",
            opened.summaries.len(),
            expected.len()
        );
    }
    for (index, (actual, expected)) in opened.summaries.iter().zip(expected).enumerate() {
        if actual.entity_id != expected.entity_id
            || actual.start != expected.start
            || actual.length != expected.length
            || actual.hash != expected.hash
        {
            bail!("entity summary {index} mismatch: got {actual:?}, expected {expected:?}");
        }
    }
    Ok(())
}

fn validate_stats(
    stats: &api::DocumentStats,
    bytes: usize,
    entities: usize,
    revision: u64,
) -> Result<()> {
    let expected_bytes = u64::try_from(bytes).expect("usize fits u64");
    let expected_entities = u64::try_from(entities).expect("usize fits u64");
    if stats.byte_length != expected_bytes
        || stats.entity_count != expected_entities
        || stats.revision != revision
    {
        bail!(
            "document stats mismatch: got {stats:?}, expected byte_length={expected_bytes}, entity_count={expected_entities}, revision={revision}"
        );
    }
    Ok(())
}

fn validate_transition(
    transition: &api::FileTransition,
    expected: &ExpectedEntity,
    expected_snapshot: &[u8],
) -> Result<()> {
    let [change] = transition.changes.as_slice() else {
        bail!(
            "sparse transition returned {} changes instead of one",
            transition.changes.len()
        );
    };
    if change.entity_id != expected.entity_id || change.snapshot != expected_snapshot {
        bail!(
            "sparse transition change mismatch: id={}, bytes={}",
            change.entity_id,
            change.snapshot.len()
        );
    }
    Ok(())
}

fn transition_direction(
    fixture: &Fixture,
    currently_after: bool,
) -> (&Bytes, &Bytes, u8, &ExpectedEntity, &Bytes) {
    if currently_after {
        (
            &fixture.after,
            &fixture.before,
            fixture.removed,
            &fixture.before_entities[fixture.changed_index],
            &fixture.original_member,
        )
    } else {
        (
            &fixture.before,
            &fixture.after,
            fixture.inserted,
            &fixture.after_entities[fixture.changed_index],
            &fixture.changed_member,
        )
    }
}

fn stream_open_counters(bytes: usize) -> SourceCounters {
    let bytes = u64::try_from(bytes).expect("usize fits u64");
    SourceCounters {
        len_calls: 1,
        stream_calls: 1,
        stream_bytes: bytes,
        stream_emitted_bytes: bytes,
        drop_calls: 1,
        ..SourceCounters::default()
    }
}

fn warm_source_counters(member_bytes: u32) -> SourceCounters {
    SourceCounters {
        len_calls: 1,
        read_calls: 1,
        read_bytes: u64::from(member_bytes),
        drop_calls: 1,
        ..SourceCounters::default()
    }
}

fn snapshot_counters(counters: &Arc<Mutex<SourceCounters>>) -> SourceCounters {
    counters
        .lock()
        .expect("source counters mutex poisoned")
        .clone()
}

fn assert_counters(
    label: &str,
    counters: &Arc<Mutex<SourceCounters>>,
    expected: &SourceCounters,
) -> Result<SourceCounters> {
    let actual = snapshot_counters(counters);
    if &actual != expected {
        bail!("{label} source counters mismatch: got {actual:?}, expected {expected:?}");
    }
    Ok(actual)
}

#[derive(Clone, Copy, Debug)]
struct TimingSummary {
    p50: Duration,
    p95: Duration,
    min: Duration,
    max: Duration,
}

const HOT_VARIANT_COUNT: usize = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
enum HotVariant {
    SyncInline = 0,
    AsyncInline = 1,
    AsyncReadInline = 2,
    AsyncStream = 3,
}

impl HotVariant {
    const fn index(self) -> usize {
        self as usize
    }

    const fn label(self) -> &'static str {
        match self {
            Self::SyncInline => "sync-export-sync-read-inline",
            Self::AsyncInline => "async-export-sync-read-inline",
            Self::AsyncReadInline => "async-export-async-read-inline",
            Self::AsyncStream => "async-export-sync-read-stream",
        }
    }

    const fn code(self) -> char {
        match self {
            Self::SyncInline => 'S',
            Self::AsyncInline => 'A',
            Self::AsyncReadInline => 'R',
            Self::AsyncStream => 'T',
        }
    }
}

const HOT_VARIANTS: [HotVariant; HOT_VARIANT_COUNT] = [
    HotVariant::SyncInline,
    HotVariant::AsyncInline,
    HotVariant::AsyncReadInline,
    HotVariant::AsyncStream,
];

const HOT_ORDERS: [[HotVariant; HOT_VARIANT_COUNT]; 24] = [
    [
        HotVariant::SyncInline,
        HotVariant::AsyncInline,
        HotVariant::AsyncReadInline,
        HotVariant::AsyncStream,
    ],
    [
        HotVariant::SyncInline,
        HotVariant::AsyncInline,
        HotVariant::AsyncStream,
        HotVariant::AsyncReadInline,
    ],
    [
        HotVariant::SyncInline,
        HotVariant::AsyncReadInline,
        HotVariant::AsyncInline,
        HotVariant::AsyncStream,
    ],
    [
        HotVariant::SyncInline,
        HotVariant::AsyncReadInline,
        HotVariant::AsyncStream,
        HotVariant::AsyncInline,
    ],
    [
        HotVariant::SyncInline,
        HotVariant::AsyncStream,
        HotVariant::AsyncInline,
        HotVariant::AsyncReadInline,
    ],
    [
        HotVariant::SyncInline,
        HotVariant::AsyncStream,
        HotVariant::AsyncReadInline,
        HotVariant::AsyncInline,
    ],
    [
        HotVariant::AsyncInline,
        HotVariant::SyncInline,
        HotVariant::AsyncReadInline,
        HotVariant::AsyncStream,
    ],
    [
        HotVariant::AsyncInline,
        HotVariant::SyncInline,
        HotVariant::AsyncStream,
        HotVariant::AsyncReadInline,
    ],
    [
        HotVariant::AsyncInline,
        HotVariant::AsyncReadInline,
        HotVariant::SyncInline,
        HotVariant::AsyncStream,
    ],
    [
        HotVariant::AsyncInline,
        HotVariant::AsyncReadInline,
        HotVariant::AsyncStream,
        HotVariant::SyncInline,
    ],
    [
        HotVariant::AsyncInline,
        HotVariant::AsyncStream,
        HotVariant::SyncInline,
        HotVariant::AsyncReadInline,
    ],
    [
        HotVariant::AsyncInline,
        HotVariant::AsyncStream,
        HotVariant::AsyncReadInline,
        HotVariant::SyncInline,
    ],
    [
        HotVariant::AsyncReadInline,
        HotVariant::SyncInline,
        HotVariant::AsyncInline,
        HotVariant::AsyncStream,
    ],
    [
        HotVariant::AsyncReadInline,
        HotVariant::SyncInline,
        HotVariant::AsyncStream,
        HotVariant::AsyncInline,
    ],
    [
        HotVariant::AsyncReadInline,
        HotVariant::AsyncInline,
        HotVariant::SyncInline,
        HotVariant::AsyncStream,
    ],
    [
        HotVariant::AsyncReadInline,
        HotVariant::AsyncInline,
        HotVariant::AsyncStream,
        HotVariant::SyncInline,
    ],
    [
        HotVariant::AsyncReadInline,
        HotVariant::AsyncStream,
        HotVariant::SyncInline,
        HotVariant::AsyncInline,
    ],
    [
        HotVariant::AsyncReadInline,
        HotVariant::AsyncStream,
        HotVariant::AsyncInline,
        HotVariant::SyncInline,
    ],
    [
        HotVariant::AsyncStream,
        HotVariant::SyncInline,
        HotVariant::AsyncInline,
        HotVariant::AsyncReadInline,
    ],
    [
        HotVariant::AsyncStream,
        HotVariant::SyncInline,
        HotVariant::AsyncReadInline,
        HotVariant::AsyncInline,
    ],
    [
        HotVariant::AsyncStream,
        HotVariant::AsyncInline,
        HotVariant::SyncInline,
        HotVariant::AsyncReadInline,
    ],
    [
        HotVariant::AsyncStream,
        HotVariant::AsyncInline,
        HotVariant::AsyncReadInline,
        HotVariant::SyncInline,
    ],
    [
        HotVariant::AsyncStream,
        HotVariant::AsyncReadInline,
        HotVariant::SyncInline,
        HotVariant::AsyncInline,
    ],
    [
        HotVariant::AsyncStream,
        HotVariant::AsyncReadInline,
        HotVariant::AsyncInline,
        HotVariant::SyncInline,
    ],
];

#[derive(Clone, Copy, Debug)]
struct PairedDeltaSummary {
    p50_ns: i128,
    p95_ns: i128,
    mean_ns: f64,
    mean_ci95_low_ns: f64,
    mean_ci95_high_ns: f64,
    candidate_slower_percent: f64,
}

fn paired_delta_summary(baseline: &[Duration], candidate: &[Duration]) -> PairedDeltaSummary {
    assert_eq!(baseline.len(), candidate.len());
    let mut deltas: Vec<i128> = baseline
        .iter()
        .zip(candidate)
        .map(|(baseline, candidate)| {
            i128::try_from(candidate.as_nanos()).expect("duration fits i128")
                - i128::try_from(baseline.as_nanos()).expect("duration fits i128")
        })
        .collect();
    deltas.sort_unstable();
    let count = deltas.len();
    let mean_ns = deltas.iter().map(|value| *value as f64).sum::<f64>() / count as f64;
    let variance = deltas
        .iter()
        .map(|value| {
            let difference = *value as f64 - mean_ns;
            difference * difference
        })
        .sum::<f64>()
        / (count.saturating_sub(1).max(1)) as f64;
    let margin = 1.96 * (variance / count as f64).sqrt();
    let slower = deltas.iter().filter(|value| **value > 0).count();
    PairedDeltaSummary {
        p50_ns: deltas[(count - 1) * 50 / 100],
        p95_ns: deltas[(count - 1) * 95 / 100],
        mean_ns,
        mean_ci95_low_ns: mean_ns - margin,
        mean_ci95_high_ns: mean_ns + margin,
        candidate_slower_percent: slower as f64 * 100.0 / count as f64,
    }
}

fn summarize(samples: &[Duration]) -> TimingSummary {
    let mut samples = samples.to_vec();
    samples.sort_unstable();
    TimingSummary {
        p50: samples[(samples.len() - 1) * 50 / 100],
        p95: samples[(samples.len() - 1) * 95 / 100],
        min: samples[0],
        max: *samples.last().expect("samples is non-empty"),
    }
}

fn print_samples(phase: &str, transport: &str, samples: &[Duration]) {
    for (index, duration) in samples.iter().enumerate() {
        println!(
            "sample\tphase={phase}\ttransport={transport}\tindex={index}\telapsed_ms={:.6}",
            duration.as_secs_f64() * 1000.0,
        );
    }
}

fn print_result(
    phase: &str,
    transport: &str,
    samples: usize,
    timing: TimingSummary,
    peak_guest_bytes: usize,
    counters: &SourceCounters,
) {
    println!(
        "result\tphase={phase}\ttransport={transport}\tsamples={samples}\tp50_ms={:.6}\tp95_ms={:.6}\tmin_ms={:.6}\tmax_ms={:.6}\tguest_linear_high_water_mib={:.3}\tsource_forks={}\tsource_lens={}\tsource_reads={}\tsource_read_bytes={}\tsource_streams={}\tsource_stream_bytes={}\tsource_stream_emitted_bytes={}\tsource_drops={}",
        timing.p50.as_secs_f64() * 1000.0,
        timing.p95.as_secs_f64() * 1000.0,
        timing.min.as_secs_f64() * 1000.0,
        timing.max.as_secs_f64() * 1000.0,
        peak_guest_bytes as f64 / (1024.0 * 1024.0),
        counters.fork_calls,
        counters.len_calls,
        counters.read_calls,
        counters.read_bytes,
        counters.stream_calls,
        counters.stream_bytes,
        counters.stream_emitted_bytes,
        counters.drop_calls,
    );
}

struct PreparedHotCall {
    before: Resource<HostByteSource>,
    after: Resource<HostByteSource>,
    before_counters: Arc<Mutex<SourceCounters>>,
    after_counters: Arc<Mutex<SourceCounters>>,
    edits: Vec<api::InputSplice>,
}

struct CompletedHotCall {
    duration: Duration,
    transition: api::FileTransition,
    before_counters: Arc<Mutex<SourceCounters>>,
    after_counters: Arc<Mutex<SourceCounters>>,
}

fn prepare_hot_call(case: &mut Case, fixture: &Fixture) -> Result<PreparedHotCall> {
    let (before, before_counters) = case.push_source(fixture.before.clone())?;
    let (after, after_counters) = case.push_source(fixture.after.clone())?;
    Ok(PreparedHotCall {
        before,
        after,
        before_counters,
        after_counters,
        edits: vec![api::InputSplice {
            offset: fixture.edit_offset,
            delete_len: 1,
            insert: vec![fixture.inserted],
        }],
    })
}

async fn execute_hot_call(
    case: &mut Case,
    document: ResourceAny,
    variant: HotVariant,
    prepared: PreparedHotCall,
) -> Result<CompletedHotCall> {
    let PreparedHotCall {
        before,
        after,
        before_counters,
        after_counters,
        edits,
    } = prepared;
    let (duration, transition) = match variant {
        HotVariant::SyncInline => {
            let started = Instant::now();
            let transition = case
                .file_changed_sync_inline(document, before, after, edits)
                .await?;
            (started.elapsed(), transition)
        }
        HotVariant::AsyncInline => {
            let started = Instant::now();
            let transition = case
                .file_changed_async_inline(document, before, after, edits)
                .await?;
            (started.elapsed(), transition)
        }
        HotVariant::AsyncReadInline => {
            let started = Instant::now();
            let transition = case
                .file_changed_async_read_inline(document, before, after, edits)
                .await?;
            (started.elapsed(), transition)
        }
        HotVariant::AsyncStream => {
            let started = Instant::now();
            let transition = case
                .file_changed_async_stream(document, before, after, edits)
                .await?;
            (started.elapsed(), transition)
        }
    };
    Ok(CompletedHotCall {
        duration,
        transition,
        before_counters,
        after_counters,
    })
}

async fn run_hot_round(
    case: &mut Case,
    document: ResourceAny,
    fixture: &Fixture,
    order: &[HotVariant; HOT_VARIANT_COUNT],
) -> Result<[Duration; HOT_VARIANT_COUNT]> {
    let mut prepared: [Option<PreparedHotCall>; HOT_VARIANT_COUNT] = std::array::from_fn(|_| None);
    for variant in HOT_VARIANTS {
        prepared[variant.index()] = Some(prepare_hot_call(case, fixture)?);
    }
    let mut completed: [Option<CompletedHotCall>; HOT_VARIANT_COUNT] =
        std::array::from_fn(|_| None);

    for &variant in order {
        let call = prepared[variant.index()]
            .take()
            .expect("each hot variant appears once");
        completed[variant.index()] = Some(execute_hot_call(case, document, variant, call).await?);
    }

    let expected_counters =
        warm_source_counters(fixture.before_entities[fixture.changed_index].length);
    let mut durations = [Duration::ZERO; HOT_VARIANT_COUNT];
    for &variant in order.iter().rev() {
        let call = completed[variant.index()]
            .take()
            .expect("each hot variant completed once");
        durations[variant.index()] = call.duration;
        validate_transition(
            &call.transition,
            &fixture.after_entities[fixture.changed_index],
            &fixture.changed_member,
        )?;
        assert_counters(
            "hot comparison before",
            &call.before_counters,
            &expected_counters,
        )?;
        assert_counters(
            "hot comparison after",
            &call.after_counters,
            &expected_counters,
        )?;
        let stats = case.stats(call.transition.document).await?;
        validate_stats(&stats, fixture.after.len(), fixture.after_entities.len(), 1)?;
        case.drop_document(call.transition.document).await?;
    }
    let base_stats = case.stats(document).await?;
    validate_stats(
        &base_stats,
        fixture.before.len(),
        fixture.before_entities.len(),
        0,
    )?;
    case.assert_host_table_empty()?;
    Ok(durations)
}

fn duration_delta_ns(candidate: Duration, baseline: Duration) -> i128 {
    i128::try_from(candidate.as_nanos()).expect("duration fits i128")
        - i128::try_from(baseline.as_nanos()).expect("duration fits i128")
}

fn print_hot_arm(variant: HotVariant, samples: &[Duration]) {
    let summary = summarize(samples);
    println!(
        "hot-result\tvariant={}\tsamples={}\tp50_ns={}\tp95_ns={}\tmin_ns={}\tmax_ns={}",
        variant.label(),
        samples.len(),
        summary.p50.as_nanos(),
        summary.p95.as_nanos(),
        summary.min.as_nanos(),
        summary.max.as_nanos(),
    );
}

fn print_hot_blocks(samples: &[Vec<Duration>; HOT_VARIANT_COUNT]) {
    let block_size = samples[0].len().min(240);
    for start in (0..samples[0].len()).step_by(block_size) {
        let end = (start + block_size).min(samples[0].len());
        let sync = summarize(&samples[HotVariant::SyncInline.index()][start..end]);
        let async_inline = summarize(&samples[HotVariant::AsyncInline.index()][start..end]);
        let async_read = summarize(&samples[HotVariant::AsyncReadInline.index()][start..end]);
        let async_stream = summarize(&samples[HotVariant::AsyncStream.index()][start..end]);
        println!(
            "hot-block\tindex={}\tstart={start}\tend={end}\tsync_p50_ns={}\tasync_inline_p50_ns={}\tasync_read_p50_ns={}\tasync_stream_p50_ns={}\tasync_minus_sync_p50_ns={}\tasync_read_minus_async_p50_ns={}\tstream_minus_async_p50_ns={}",
            start / block_size,
            sync.p50.as_nanos(),
            async_inline.p50.as_nanos(),
            async_read.p50.as_nanos(),
            async_stream.p50.as_nanos(),
            duration_delta_ns(async_inline.p50, sync.p50),
            duration_delta_ns(async_read.p50, async_inline.p50),
            duration_delta_ns(async_stream.p50, async_inline.p50),
        );
    }
}

fn hot_conservative_gate(label: &str, baseline: &[Duration], candidate: &[Duration]) -> bool {
    let baseline_summary = summarize(baseline);
    let candidate_summary = summarize(candidate);
    let delta = paired_delta_summary(baseline, candidate);
    let p50_delta_ns = duration_delta_ns(candidate_summary.p50, baseline_summary.p50);
    let p95_delta_ns = duration_delta_ns(candidate_summary.p95, baseline_summary.p95);
    let p50_slowdown_percent =
        (candidate_summary.p50.as_nanos() as f64 / baseline_summary.p50.as_nanos() as f64 - 1.0)
            * 100.0;
    let p95_slowdown_percent =
        (candidate_summary.p95.as_nanos() as f64 / baseline_summary.p95.as_nanos() as f64 - 1.0)
            * 100.0;
    let p50_margin_ns = (baseline_summary.p50.as_nanos() as f64 * 0.10).min(500.0);
    let p95_margin_ns = (baseline_summary.p95.as_nanos() as f64 * 0.15).min(1_000.0);
    let point_gate_pass =
        p50_delta_ns as f64 <= p50_margin_ns && p95_delta_ns as f64 <= p95_margin_ns;
    // Samples within one process are serial and can be autocorrelated. Keep
    // this IID interval as a conservative diagnostic, not the retained
    // cross-process inference.
    let iid_ci_guard_pass = delta.mean_ci95_high_ns <= p50_margin_ns;
    let conservative_gate_pass = point_gate_pass && iid_ci_guard_pass;
    println!(
        "hot-comparison\tname={label}\tp50_delta_ns={p50_delta_ns}\tp95_delta_ns={p95_delta_ns}\tp50_slowdown_percent={p50_slowdown_percent:.3}\tp95_slowdown_percent={p95_slowdown_percent:.3}\tpaired_p50_delta_ns={}\tpaired_p95_delta_ns={}\tpaired_mean_delta_ns={:.3}\tiid_paired_mean_ci95_low_ns={:.3}\tiid_paired_mean_ci95_high_ns={:.3}\tcandidate_slower_percent={:.3}\tp50_margin_ns={p50_margin_ns:.3}\tp95_margin_ns={p95_margin_ns:.3}\tpoint_gate_pass={point_gate_pass}\tiid_ci_guard_pass={iid_ci_guard_pass}\tconservative_gate_pass={conservative_gate_pass}",
        delta.p50_ns,
        delta.p95_ns,
        delta.mean_ns,
        delta.mean_ci95_low_ns,
        delta.mean_ci95_high_ns,
        delta.candidate_slower_percent,
    );
    conservative_gate_pass
}

async fn benchmark_hot_abi(
    engine: &Engine,
    component: &Component,
    linker: &Linker<State>,
    fixture: &Fixture,
    settings: Settings,
) -> Result<()> {
    let mut case = Case::new(
        engine,
        component,
        linker,
        settings.guest_linear_memory_limit_bytes,
        settings.max_entity_summaries,
    )
    .await?;
    let opened = case.open_list(fixture.before.to_vec()).await?;
    validate_opened(&opened, &fixture.before_entities)?;
    let document = opened.document;
    let stats = case.stats(document).await?;
    validate_stats(
        &stats,
        fixture.before.len(),
        fixture.before_entities.len(),
        0,
    )?;

    for round in 0..settings.hot_warmups {
        let order = &HOT_ORDERS[round % HOT_ORDERS.len()];
        let _ = run_hot_round(&mut case, document, fixture, order).await?;
    }

    let mut samples: [Vec<Duration>; HOT_VARIANT_COUNT] =
        std::array::from_fn(|_| Vec::with_capacity(settings.hot_samples));
    for round in 0..settings.hot_samples {
        let order = &HOT_ORDERS[round % HOT_ORDERS.len()];
        let durations = run_hot_round(&mut case, document, fixture, order).await?;
        for variant in HOT_VARIANTS {
            samples[variant.index()].push(durations[variant.index()]);
        }
        if settings.hot_print_raw {
            println!(
                "hot-sample\tindex={round}\torder={}{}{}{}\tsync_ns={}\tasync_inline_ns={}\tasync_read_ns={}\tasync_stream_ns={}",
                order[0].code(),
                order[1].code(),
                order[2].code(),
                order[3].code(),
                durations[HotVariant::SyncInline.index()].as_nanos(),
                durations[HotVariant::AsyncInline.index()].as_nanos(),
                durations[HotVariant::AsyncReadInline.index()].as_nanos(),
                durations[HotVariant::AsyncStream.index()].as_nanos(),
            );
        }
    }

    for variant in HOT_VARIANTS {
        print_hot_arm(variant, &samples[variant.index()]);
    }
    print_hot_blocks(&samples);
    let async_export_conservative_gate_pass = hot_conservative_gate(
        "async-export-over-sync-export",
        &samples[HotVariant::SyncInline.index()],
        &samples[HotVariant::AsyncInline.index()],
    );
    let async_read_conservative_gate_pass = hot_conservative_gate(
        "async-read-over-sync-read",
        &samples[HotVariant::AsyncInline.index()],
        &samples[HotVariant::AsyncReadInline.index()],
    );
    let async_stream_conservative_gate_pass = hot_conservative_gate(
        "async-stream-over-inline-output",
        &samples[HotVariant::AsyncInline.index()],
        &samples[HotVariant::AsyncStream.index()],
    );
    println!(
        "hot-decision\tscope=ready-262-byte-random-reads-one-change\tasync_export_conservative_gate_pass={async_export_conservative_gate_pass}\tasync_read_conservative_gate_pass={async_read_conservative_gate_pass}\tasync_stream_conservative_gate_pass={async_stream_conservative_gate_pass}\tstream_payload_bytes={}\tstream_memory_comparison=not-measured",
        fixture.changed_member.len(),
    );
    case.drop_document(document).await?;
    case.assert_host_table_empty()?;
    case.assert_memory_limit()?;
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum ColdTransport {
    List,
    BytesStream,
}

impl ColdTransport {
    const fn label(self) -> &'static str {
        match self {
            Self::List => "list-u8",
            Self::BytesStream => "bytes-stream-u8",
        }
    }
}

async fn one_open(
    case: &mut Case,
    transport: ColdTransport,
    bytes: &Bytes,
) -> Result<(Opened, Option<Arc<Mutex<SourceCounters>>>)> {
    match transport {
        ColdTransport::List => Ok((case.open_list(bytes.to_vec()).await?, None)),
        ColdTransport::BytesStream => {
            let (source, counters) = case.push_source(bytes.clone())?;
            Ok((case.open_stream(source).await?, Some(counters)))
        }
    }
}

async fn benchmark_cold(
    engine: &Engine,
    component: &Component,
    linker: &Linker<State>,
    fixture: &Fixture,
    settings: Settings,
    transport: ColdTransport,
) -> Result<TimingSummary> {
    let mut case = Case::new(
        engine,
        component,
        linker,
        settings.guest_linear_memory_limit_bytes,
        settings.max_entity_summaries,
    )
    .await?;
    let expected_stream_counters = stream_open_counters(fixture.before.len());
    for _ in 0..settings.cold_warmups {
        let (opened, counters) = one_open(&mut case, transport, &fixture.before).await?;
        validate_opened(&opened, &fixture.before_entities)?;
        if let Some(counters) = counters {
            assert_counters("cold warmup", &counters, &expected_stream_counters)?;
        }
        case.drop_document(opened.document).await?;
    }

    let mut durations = Vec::with_capacity(settings.cold_samples);
    let mut total_counters = SourceCounters::default();
    for _ in 0..settings.cold_samples {
        let prepared_list =
            matches!(transport, ColdTransport::List).then(|| fixture.before.to_vec());
        let prepared_stream = if matches!(transport, ColdTransport::BytesStream) {
            let (source, counters) = case.push_source(fixture.before.clone())?;
            Some((source, counters))
        } else {
            None
        };
        let start = Instant::now();
        let (opened, counters) = match transport {
            ColdTransport::List => (
                case.open_list(prepared_list.expect("list input prepared"))
                    .await?,
                None,
            ),
            ColdTransport::BytesStream => {
                let (source, counters) = prepared_stream.expect("stream input prepared");
                (case.open_stream(source).await?, Some(counters))
            }
        };
        durations.push(start.elapsed());
        validate_opened(&opened, &fixture.before_entities)?;
        if let Some(counters) = counters {
            let counters = assert_counters("cold sample", &counters, &expected_stream_counters)?;
            total_counters.add_assign(&counters);
        }
        case.drop_document(opened.document).await?;
    }
    let timing = summarize(&durations);
    print_samples("cold-open", transport.label(), &durations);
    print_result(
        "cold-open",
        transport.label(),
        settings.cold_samples,
        timing,
        case.peak_guest_linear_bytes(),
        &total_counters,
    );
    case.assert_memory_limit()?;
    Ok(timing)
}

async fn benchmark_stateless_full_update(
    engine: &Engine,
    component: &Component,
    linker: &Linker<State>,
    fixture: &Fixture,
    settings: Settings,
) -> Result<TimingSummary> {
    let mut case = Case::new(
        engine,
        component,
        linker,
        settings.guest_linear_memory_limit_bytes,
        settings.max_entity_summaries,
    )
    .await?;
    for _ in 0..settings.cold_warmups {
        let opened = case.open_list(fixture.after.to_vec()).await?;
        validate_opened(&opened, &fixture.after_entities)?;
        case.drop_document(opened.document).await?;
    }
    let mut durations = Vec::with_capacity(settings.cold_samples);
    for _ in 0..settings.cold_samples {
        let input = fixture.after.to_vec();
        let start = Instant::now();
        let opened = case.open_list(input).await?;
        durations.push(start.elapsed());
        validate_opened(&opened, &fixture.after_entities)?;
        case.drop_document(opened.document).await?;
    }
    let timing = summarize(&durations);
    print_samples("warm-update-stateless-full", "open-list-u8", &durations);
    print_result(
        "warm-update-stateless-full",
        "open-list-u8",
        settings.cold_samples,
        timing,
        case.peak_guest_linear_bytes(),
        &SourceCounters::default(),
    );
    case.assert_memory_limit()?;
    Ok(timing)
}

async fn benchmark_persistent_warm(
    engine: &Engine,
    component: &Component,
    linker: &Linker<State>,
    fixture: &Fixture,
    settings: Settings,
    opened_via: ColdTransport,
) -> Result<TimingSummary> {
    let mut case = Case::new(
        engine,
        component,
        linker,
        settings.guest_linear_memory_limit_bytes,
        settings.max_entity_summaries,
    )
    .await?;
    let (opened, open_counters) = one_open(&mut case, opened_via, &fixture.before).await?;
    validate_opened(&opened, &fixture.before_entities)?;
    if let Some(counters) = open_counters {
        assert_counters(
            "persistent setup",
            &counters,
            &stream_open_counters(fixture.before.len()),
        )?;
    }
    let stats = case.stats(opened.document).await?;
    validate_stats(
        &stats,
        fixture.before.len(),
        fixture.before_entities.len(),
        0,
    )?;
    let mut document = opened.document;
    let member_bytes = fixture.before_entities[fixture.changed_index].length;
    let expected_source_counters = warm_source_counters(member_bytes);

    let fork = case.fork(document).await?;
    let fork_stats = case.stats(fork).await?;
    validate_stats(
        &fork_stats,
        fixture.before.len(),
        fixture.before_entities.len(),
        0,
    )?;
    let (before, before_counters) = case.push_source(fixture.before.clone())?;
    let (after, after_counters) = case.push_source(fixture.after.clone())?;
    let successor = case
        .file_changed(
            document,
            before,
            after,
            fixture.edit_offset,
            fixture.inserted,
        )
        .await?;
    validate_transition(
        &successor,
        &fixture.after_entities[fixture.changed_index],
        &fixture.changed_member,
    )?;
    assert_counters(
        "immutability check before",
        &before_counters,
        &expected_source_counters,
    )?;
    assert_counters(
        "immutability check after",
        &after_counters,
        &expected_source_counters,
    )?;
    let successor_stats = case.stats(successor.document).await?;
    validate_stats(
        &successor_stats,
        fixture.after.len(),
        fixture.after_entities.len(),
        1,
    )?;
    let original_stats = case.stats(document).await?;
    validate_stats(
        &original_stats,
        fixture.before.len(),
        fixture.before_entities.len(),
        0,
    )?;
    let fork_stats_after = case.stats(fork).await?;
    validate_stats(
        &fork_stats_after,
        fixture.before.len(),
        fixture.before_entities.len(),
        0,
    )?;
    case.drop_document(successor.document).await?;
    case.drop_document(fork).await?;
    println!(
        "check\tname=immutable-fork-successor\tstatus=ok\ttransport={}",
        opened_via.label(),
    );

    let mut currently_after = false;
    let mut revision = 0_u64;
    for _ in 0..settings.warm_warmups {
        let (before_bytes, after_bytes, inserted, expected, expected_snapshot) =
            transition_direction(fixture, currently_after);
        let (before, before_counters) = case.push_source(before_bytes.clone())?;
        let (after, after_counters) = case.push_source(after_bytes.clone())?;
        let previous = document;
        let transition = case
            .file_changed(previous, before, after, fixture.edit_offset, inserted)
            .await?;
        validate_transition(&transition, expected, expected_snapshot)?;
        assert_counters("warmup before", &before_counters, &expected_source_counters)?;
        assert_counters("warmup after", &after_counters, &expected_source_counters)?;
        document = transition.document;
        case.drop_document(previous).await?;
        currently_after = !currently_after;
        revision = revision.wrapping_add(1);
    }

    let mut durations = Vec::with_capacity(settings.warm_samples);
    let mut total_counters = SourceCounters::default();
    for _ in 0..settings.warm_samples {
        let (before_bytes, after_bytes, inserted, expected, expected_snapshot) =
            transition_direction(fixture, currently_after);
        let (before, before_counters) = case.push_source(before_bytes.clone())?;
        let (after, after_counters) = case.push_source(after_bytes.clone())?;
        let previous = document;
        let start = Instant::now();
        let transition = case
            .file_changed(previous, before, after, fixture.edit_offset, inserted)
            .await?;
        durations.push(start.elapsed());
        validate_transition(&transition, expected, expected_snapshot)?;
        let before = assert_counters("sample before", &before_counters, &expected_source_counters)?;
        let after = assert_counters("sample after", &after_counters, &expected_source_counters)?;
        total_counters.add_assign(&before);
        total_counters.add_assign(&after);
        document = transition.document;
        case.drop_document(previous).await?;
        currently_after = !currently_after;
        revision = revision.wrapping_add(1);
    }
    let final_entities = if currently_after {
        &fixture.after_entities
    } else {
        &fixture.before_entities
    };
    let final_stats = case.stats(document).await?;
    validate_stats(
        &final_stats,
        fixture.before.len(),
        final_entities.len(),
        revision,
    )?;
    let timing = summarize(&durations);
    let warm_transport = match opened_via {
        ColdTransport::List => "document-opened-list",
        ColdTransport::BytesStream => "document-opened-bytes-stream",
    };
    print_samples(
        "warm-update-persistent-sparse-sequential",
        warm_transport,
        &durations,
    );
    print_result(
        "warm-update-persistent-sparse-sequential",
        warm_transport,
        settings.warm_samples,
        timing,
        case.peak_guest_linear_bytes(),
        &total_counters,
    );
    case.drop_document(document).await?;
    case.assert_memory_limit()?;
    Ok(timing)
}

async fn validate_fail_closed_short_stream(
    engine: &Engine,
    component: &Component,
    linker: &Linker<State>,
    fixture: &Fixture,
    guest_linear_memory_limit_bytes: usize,
    max_entity_summaries: usize,
) -> Result<()> {
    let mut case = Case::new(
        engine,
        component,
        linker,
        guest_linear_memory_limit_bytes,
        max_entity_summaries,
    )
    .await?;
    let (source, counters) = case.push_short_stream_source(fixture.before.clone(), 1)?;
    let started = Instant::now();
    let error = match case.open_stream(source).await {
        Ok(opened) => {
            case.drop_document(opened.document).await?;
            bail!("truncated source stream produced a partial document");
        }
        Err(error) => error,
    };
    let message = format!("{error:#}");
    if !message.contains("source stream length mismatch") {
        bail!("truncated source failed for an unexpected reason: {message}");
    }
    let bytes = u64::try_from(fixture.before.len()).expect("usize fits u64");
    let expected = SourceCounters {
        len_calls: 1,
        stream_calls: 1,
        stream_bytes: bytes,
        stream_emitted_bytes: bytes - 1,
        drop_calls: 1,
        ..SourceCounters::default()
    };
    assert_counters("fail-closed short stream", &counters, &expected)?;
    case.assert_memory_limit()?;
    println!(
        "check\tname=fail-closed-short-stream\tstatus=ok\tadvertised_bytes={bytes}\temitted_bytes={}\telapsed_ms={:.3}",
        bytes - 1,
        started.elapsed().as_secs_f64() * 1000.0,
    );
    Ok(())
}

async fn validate_fail_closed_terminal_error(
    engine: &Engine,
    component: &Component,
    linker: &Linker<State>,
    fixture: &Fixture,
    settings: Settings,
) -> Result<()> {
    let mut case = Case::new(
        engine,
        component,
        linker,
        settings.guest_linear_memory_limit_bytes,
        settings.max_entity_summaries,
    )
    .await?;
    let (source, counters) = case.push_terminal_error_source(fixture.before.clone())?;
    let started = Instant::now();
    let error = match case.open_stream(source).await {
        Ok(opened) => {
            case.drop_document(opened.document).await?;
            bail!("terminal source error produced a partial document");
        }
        Err(error) => error,
    };
    let message = format!("{error:#}");
    if !message.contains("injected terminal failure") {
        bail!("terminal source error failed for an unexpected reason: {message}");
    }
    let expected = stream_open_counters(fixture.before.len());
    assert_counters("fail-closed terminal error", &counters, &expected)?;
    case.assert_memory_limit()?;
    println!(
        "check\tname=fail-closed-terminal-error\tstatus=ok\temitted_bytes={}\telapsed_ms={:.3}",
        fixture.before.len(),
        started.elapsed().as_secs_f64() * 1000.0,
    );
    Ok(())
}

async fn validate_output_cancellation(
    engine: &Engine,
    component: &Component,
    linker: &Linker<State>,
    fixture: &Fixture,
    settings: Settings,
) -> Result<()> {
    let mut case = Case::new(
        engine,
        component,
        linker,
        settings.guest_linear_memory_limit_bytes,
        settings.max_entity_summaries,
    )
    .await?;
    let started = Instant::now();
    let document = case
        .open_list_cancel_output(fixture.before.to_vec())
        .await?;
    let stats = case.stats(document).await?;
    validate_stats(
        &stats,
        fixture.before.len(),
        fixture.before_entities.len(),
        0,
    )?;
    case.drop_document(document).await?;
    case.assert_memory_limit()?;
    println!(
        "check\tname=output-cancellation-terminal\tstatus=ok\telapsed_ms={:.3}",
        started.elapsed().as_secs_f64() * 1000.0,
    );
    Ok(())
}

async fn validate_output_limit(
    engine: &Engine,
    component: &Component,
    linker: &Linker<State>,
    fixture: &Fixture,
    guest_linear_memory_limit_bytes: usize,
) -> Result<()> {
    let max_items = fixture
        .before_entities
        .len()
        .checked_sub(1)
        .context("fixture must contain at least one entity")?;
    let mut case = Case::new(
        engine,
        component,
        linker,
        guest_linear_memory_limit_bytes,
        max_items,
    )
    .await?;
    let started = Instant::now();
    let error = match case.open_list(fixture.before.to_vec()).await {
        Ok(opened) => {
            case.drop_document(opened.document).await?;
            bail!("entity output above the host limit was accepted");
        }
        Err(error) => error,
    };
    let message = format!("{error:#}");
    if !message.contains("declared entity count exceeded host limit") {
        bail!("entity output limit failed for an unexpected reason: {message}");
    }
    case.assert_memory_limit()?;
    println!(
        "check\tname=host-entity-output-limit\tstatus=ok\tdeclared={}\tlimit={max_items}\telapsed_ms={:.3}",
        fixture.before_entities.len(),
        started.elapsed().as_secs_f64() * 1000.0,
    );
    Ok(())
}

fn print_comparison(label: &str, baseline: TimingSummary, candidate: TimingSummary) {
    println!(
        "comparison\tname={label}\tp50_speedup={:.3}\tp95_speedup={:.3}",
        baseline.p50.as_secs_f64() / candidate.p50.as_secs_f64(),
        baseline.p95.as_secs_f64() / candidate.p95.as_secs_f64(),
    );
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let component_path = env::args()
        .nth(1)
        .context("usage: plugin-api-p3-host <guest-component.wasm>")?;
    if !Path::new(&component_path).exists() {
        bail!("guest component does not exist: {component_path}");
    }
    let settings = Settings::from_env()?;
    let fixture = json_fixture();
    if fixture.before.len() < TARGET_BYTES {
        bail!("fixture is smaller than the 10 MiB target");
    }

    let mut config = Config::new();
    config.wasm_component_model_async(true);
    let engine = Engine::new(&config)?;
    let component = Component::from_file(&engine, &component_path)
        .map_err(|error| anyhow!("compile component {component_path}: {error:?}"))?;

    let mut linker = Linker::new(&engine);
    wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
    wasmtime_wasi::p3::add_to_linker(&mut linker)?;
    Plugin::add_to_linker::<State, Host>(&mut linker, |state| state)?;

    println!(
        "config\twasmtime=47.0.2\tfixture=json-top-level-object\tbytes={}\tentities={}\tchanged_entity={}\tchanged_member_bytes={}\tguest_per_linear_memory_limit_mib={}\tmax_entity_summaries={}\tcold_warmups={}\tcold_samples={}\twarm_warmups={}\twarm_samples={}\thot_warmups={}\thot_samples={}\thot_print_raw={}\tguest_component_bytes={}",
        fixture.before.len(),
        fixture.before_entities.len(),
        fixture.changed_index,
        fixture.before_entities[fixture.changed_index].length,
        settings.guest_linear_memory_limit_bytes / MIB,
        settings.max_entity_summaries,
        settings.cold_warmups,
        settings.cold_samples,
        settings.warm_warmups,
        settings.warm_samples,
        settings.hot_warmups,
        settings.hot_samples,
        settings.hot_print_raw,
        std::fs::metadata(&component_path)?.len(),
    );

    validate_fail_closed_short_stream(
        &engine,
        &component,
        &linker,
        &fixture,
        settings.guest_linear_memory_limit_bytes,
        settings.max_entity_summaries,
    )
    .await?;
    validate_fail_closed_terminal_error(&engine, &component, &linker, &fixture, settings).await?;
    validate_output_cancellation(&engine, &component, &linker, &fixture, settings).await?;
    validate_output_limit(
        &engine,
        &component,
        &linker,
        &fixture,
        settings.guest_linear_memory_limit_bytes,
    )
    .await?;

    let cold_list = benchmark_cold(
        &engine,
        &component,
        &linker,
        &fixture,
        settings,
        ColdTransport::List,
    )
    .await?;
    let cold_stream = benchmark_cold(
        &engine,
        &component,
        &linker,
        &fixture,
        settings,
        ColdTransport::BytesStream,
    )
    .await?;
    print_comparison("cold-list-over-bytes-stream", cold_list, cold_stream);

    let stateless =
        benchmark_stateless_full_update(&engine, &component, &linker, &fixture, settings).await?;
    let warm_list = benchmark_persistent_warm(
        &engine,
        &component,
        &linker,
        &fixture,
        settings,
        ColdTransport::List,
    )
    .await?;
    let warm_stream = benchmark_persistent_warm(
        &engine,
        &component,
        &linker,
        &fixture,
        settings,
        ColdTransport::BytesStream,
    )
    .await?;
    print_comparison(
        "stateless-full-over-persistent-list-opened",
        stateless,
        warm_list,
    );
    print_comparison(
        "stateless-full-over-persistent-stream-opened",
        stateless,
        warm_stream,
    );
    benchmark_hot_abi(&engine, &component, &linker, &fixture, settings).await?;
    Ok(())
}
