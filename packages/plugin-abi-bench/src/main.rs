use std::cmp;
use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use wasmtime::component::{Component, Linker};
use wasmtime::{Config, Engine, ResourceLimiter, Store};
use wasmtime_wasi::{
    ResourceTable, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView, p2::add_to_linker_sync,
};

mod rich_bindings {
    wasmtime::component::bindgen!({
        path: "wit/rich",
        world: "plugin",
    });
}

mod packed_bindings {
    wasmtime::component::bindgen!({
        path: "wit/packed",
        world: "plugin",
    });
}

use rich_bindings::exports::lix::abi_bench_rich::api::{EntityState, File};

const RICH_COMPONENT_BYTES: &[u8] =
    include_bytes!(env!("CARGO_CDYLIB_FILE_LIX_PLUGIN_ABI_BENCH_RICH_GUEST"));
const PACKED_COMPONENT_BYTES: &[u8] =
    include_bytes!(env!("CARGO_CDYLIB_FILE_LIX_PLUGIN_ABI_BENCH_PACKED_GUEST"));

const HEADER_LEN: usize = 32;
const RECORD_LEN: usize = 32;
const PACKED_VERSION: u16 = 1;
const TIMING_MEMORY_LIMIT: usize = 512 * 1024 * 1024;
const PRODUCTION_MEMORY_LIMIT: usize = 64 * 1024 * 1024;
const DEFAULT_SIZES: &[usize] = &[100 * 1024, 1024 * 1024, 5 * 1024 * 1024, 10 * 1024 * 1024];
const DEFAULT_DENSITIES: &[usize] = &[48, 1024];

type BenchResult<T> = Result<T, String>;

fn main() {
    if let Err(error) = run() {
        eprintln!("plugin ABI benchmark failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> BenchResult<()> {
    let sizes = env_usize_list("LIX_ABI_BENCH_SIZES", DEFAULT_SIZES)?;
    let densities = env_usize_list("LIX_ABI_BENCH_DENSITIES", DEFAULT_DENSITIES)?;
    let runtime = Runtime::new()?;

    if let Ok(profile) = env::var("LIX_ABI_BENCH_PROFILE") {
        return run_profile(&runtime, &profile, sizes[0], densities[0]);
    }

    let sampling = Sampling {
        min_samples: env_usize("LIX_ABI_BENCH_MIN_SAMPLES", 9)?,
        max_samples: env_usize("LIX_ABI_BENCH_MAX_SAMPLES", 31)?,
        target: Duration::from_millis(env_usize("LIX_ABI_BENCH_TARGET_MS", 1_500)? as u64),
        warmups: env_usize("LIX_ABI_BENCH_WARMUPS", 2)?,
    };
    if sampling.min_samples == 0 || sampling.min_samples > sampling.max_samples {
        return Err("sample bounds must satisfy 0 < min <= max".to_string());
    }

    println!(
        "config\twasmtime=45\ttiming_limit_bytes={TIMING_MEMORY_LIMIT}\tproduction_limit_bytes={PRODUCTION_MEMORY_LIMIT}\trich_component_bytes={}\tpacked_component_bytes={}\tmin_samples={}\tmax_samples={}\ttarget_ms={}\twarmups={}",
        RICH_COMPONENT_BYTES.len(),
        PACKED_COMPONENT_BYTES.len(),
        sampling.min_samples,
        sampling.max_samples,
        sampling.target.as_millis(),
        sampling.warmups,
    );
    println!(
        "result\tabi\toperation\tlogical_bytes\tdensity\tentities\tinput_bytes\toutput_bytes\tsamples\tp50_us\tp95_us\tpeak_linear_bytes\tlimit64\tstatus"
    );

    for &logical_bytes in &sizes {
        let file_copy = copy_floor(logical_bytes, sampling)?;
        print_result(
            "memcpy",
            Operation::FileRoundTrip,
            logical_bytes,
            0,
            0,
            logical_bytes,
            0,
            &file_copy,
            0,
            "n/a",
            "ok",
        );

        for &density in &densities {
            let data = DataSet::new(logical_bytes, density)?;
            println!(
                "dataset\tlogical_bytes={}\tdensity={}\tentities={}\trich_state_content_bytes={}\tpacked_full_bytes={}\tpacked_state_bytes={}\tpacked_file_bytes={}",
                logical_bytes,
                density,
                data.entities.len(),
                data.rich_state_content_bytes,
                data.packed_full.len(),
                data.packed_state.len(),
                data.packed_file.len(),
            );

            let packed_copy = copy_floor(data.packed_full.len(), sampling)?;
            print_result(
                "memcpy",
                Operation::DetectEmpty,
                logical_bytes,
                density,
                data.entities.len(),
                data.packed_full.len(),
                0,
                &packed_copy,
                0,
                "n/a",
                "ok",
            );

            for operation in Operation::ALL {
                run_rich_case(&runtime, &data, operation, sampling)?;
                run_packed_case(&runtime, &data, operation, sampling)?;
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct Sampling {
    min_samples: usize,
    max_samples: usize,
    target: Duration,
    warmups: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Operation {
    DetectEmpty,
    EntityRoundTrip,
    FileRoundTrip,
}

impl Operation {
    const ALL: [Self; 3] = [
        Self::DetectEmpty,
        Self::EntityRoundTrip,
        Self::FileRoundTrip,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::DetectEmpty => "detect-empty",
            Self::EntityRoundTrip => "entity-round-trip",
            Self::FileRoundTrip => "file-round-trip",
        }
    }
}

#[derive(Debug)]
struct DataSet {
    entities: Vec<EntityState>,
    file: File,
    empty_file: File,
    rich_state_content_bytes: usize,
    packed_full: Vec<u8>,
    packed_state: Vec<u8>,
    packed_file: Vec<u8>,
}

impl DataSet {
    fn new(logical_bytes: usize, density: usize) -> BenchResult<Self> {
        if density == 0 {
            return Err("entity density must be non-zero".to_string());
        }
        let count = logical_bytes.div_ceil(density);
        let mut entities = Vec::with_capacity(count);
        let mut remaining = logical_bytes;
        for index in 0..count {
            let snapshot_len = cmp::min(density, remaining);
            remaining -= snapshot_len;
            let metadata = (index % 2 == 0).then(|| format!("order-{index:010}"));
            entities.push(EntityState {
                entity_pk: vec![format!("row-{index:012x}")],
                schema_key: "lix:bench-row@1".to_string(),
                snapshot_content: "x".repeat(snapshot_len),
                metadata,
            });
        }
        let file_data = vec![b'x'; logical_bytes];
        let rich_state_content_bytes = entity_content_bytes(&entities);
        let packed_full = pack_arena(&entities, &file_data, 0)?;
        let packed_state = pack_arena(&entities, &[], 0)?;
        let packed_file = pack_arena(&[], &file_data, 1)?;
        Ok(Self {
            entities,
            file: File {
                filename: Some("benchmark.csv".to_string()),
                data: file_data,
            },
            empty_file: File {
                filename: None,
                data: Vec::new(),
            },
            rich_state_content_bytes,
            packed_full,
            packed_state,
            packed_file,
        })
    }

    fn rich_io_bytes(&self, operation: Operation) -> (usize, usize) {
        match operation {
            Operation::DetectEmpty => (
                self.rich_state_content_bytes
                    + self.file.data.len()
                    + self.file.filename.as_ref().map_or(0, String::len),
                0,
            ),
            Operation::EntityRoundTrip => {
                (self.rich_state_content_bytes, self.rich_state_content_bytes)
            }
            Operation::FileRoundTrip => (self.file.data.len(), self.file.data.len()),
        }
    }

    fn packed_input(&self, operation: Operation) -> &Vec<u8> {
        match operation {
            Operation::DetectEmpty => &self.packed_full,
            Operation::EntityRoundTrip => &self.packed_state,
            Operation::FileRoundTrip => &self.packed_file,
        }
    }

    fn packed_output_bytes(&self, operation: Operation) -> usize {
        match operation {
            Operation::DetectEmpty => 0,
            Operation::EntityRoundTrip => self.packed_state.len(),
            Operation::FileRoundTrip => self.packed_file.len(),
        }
    }
}

fn entity_content_bytes(entities: &[EntityState]) -> usize {
    entities
        .iter()
        .map(|entity| {
            entity.entity_pk.iter().map(String::len).sum::<usize>()
                + entity.schema_key.len()
                + entity.snapshot_content.len()
                + entity.metadata.as_ref().map_or(0, String::len)
        })
        .sum()
}

fn pack_arena(entities: &[EntityState], file: &[u8], kind: u16) -> BenchResult<Vec<u8>> {
    let records_bytes = entities
        .len()
        .checked_mul(RECORD_LEN)
        .and_then(|bytes| bytes.checked_add(HEADER_LEN))
        .ok_or_else(|| "packed arena directory overflow".to_string())?;
    let estimated = records_bytes
        .checked_add(entity_content_bytes(entities))
        .and_then(|bytes| bytes.checked_add(entities.len() * 8))
        .and_then(|bytes| bytes.checked_add(file.len()))
        .ok_or_else(|| "packed arena capacity overflow".to_string())?;
    let mut arena = vec![0_u8; records_bytes];
    arena.reserve(estimated.saturating_sub(records_bytes));

    for (index, entity) in entities.iter().enumerate() {
        let record_offset = HEADER_LEN + index * RECORD_LEN;
        let pk_table_offset = checked_u32(arena.len(), "primary-key table offset")?;
        let pk_count = checked_u32(entity.entity_pk.len(), "primary-key item count")?;
        let table_len = entity
            .entity_pk
            .len()
            .checked_mul(8)
            .ok_or_else(|| "primary-key table length overflow".to_string())?;
        arena.resize(arena.len() + table_len, 0);
        for (pk_index, value) in entity.entity_pk.iter().enumerate() {
            let span = append_bytes(&mut arena, value.as_bytes())?;
            let span_offset = pk_table_offset as usize + pk_index * 8;
            put_pair(&mut arena, span_offset, span);
        }
        let schema = append_bytes(&mut arena, entity.schema_key.as_bytes())?;
        let snapshot = append_bytes(&mut arena, entity.snapshot_content.as_bytes())?;
        let metadata = match &entity.metadata {
            Some(value) => append_bytes(&mut arena, value.as_bytes())?,
            None => (0, u32::MAX),
        };
        put_pair(&mut arena, record_offset, (pk_table_offset, pk_count));
        put_pair(&mut arena, record_offset + 8, schema);
        put_pair(&mut arena, record_offset + 16, snapshot);
        put_pair(&mut arena, record_offset + 24, metadata);
    }

    let file_offset = checked_u32(arena.len(), "file offset")?;
    let file_len = checked_u32(file.len(), "file length")?;
    arena.extend_from_slice(file);
    let total_len = checked_u32(arena.len(), "total arena length")?;

    arena[..4].copy_from_slice(b"LPK1");
    put_u16(&mut arena, 4, PACKED_VERSION);
    put_u16(&mut arena, 6, kind);
    put_u32(&mut arena, 8, checked_u32(entities.len(), "entity count")?);
    put_u32(&mut arena, 12, 32_u32);
    put_u32(&mut arena, 16, file_offset);
    put_u32(&mut arena, 20, file_len);
    put_u32(&mut arena, 24, total_len);
    put_u32(&mut arena, 28, 0);
    Ok(arena)
}

fn append_bytes(arena: &mut Vec<u8>, bytes: &[u8]) -> BenchResult<(u32, u32)> {
    let offset = checked_u32(arena.len(), "field offset")?;
    let len = checked_u32(bytes.len(), "field length")?;
    arena.extend_from_slice(bytes);
    Ok((offset, len))
}

fn checked_u32(value: usize, label: &str) -> BenchResult<u32> {
    u32::try_from(value).map_err(|_| format!("{label} exceeds packed v1's u32 range"))
}

fn put_pair(bytes: &mut [u8], offset: usize, pair: (u32, u32)) {
    put_u32(bytes, offset, pair.0);
    put_u32(bytes, offset + 4, pair.1);
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

#[derive(Debug)]
struct PeakLimiter {
    limit: usize,
    aggregate_current: usize,
    peak_allowed: usize,
    max_requested: usize,
    denied: bool,
}

impl PeakLimiter {
    const fn new(limit: usize) -> Self {
        Self {
            limit,
            aggregate_current: 0,
            peak_allowed: 0,
            max_requested: 0,
            denied: false,
        }
    }
}

impl ResourceLimiter for PeakLimiter {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        let growth = desired.saturating_sub(current);
        let requested_aggregate = self.aggregate_current.saturating_add(growth);
        self.max_requested = self.max_requested.max(requested_aggregate);
        let allowed =
            requested_aggregate <= self.limit && maximum.is_none_or(|maximum| desired <= maximum);
        if allowed {
            self.aggregate_current = requested_aggregate;
            self.peak_allowed = self.peak_allowed.max(requested_aggregate);
        } else {
            self.denied = true;
        }
        Ok(allowed)
    }

    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        Ok(maximum.is_none_or(|maximum| desired <= maximum))
    }
}

struct HostState {
    ctx: WasiCtx,
    table: ResourceTable,
    limiter: PeakLimiter,
}

impl HostState {
    fn new(memory_limit: usize) -> Self {
        Self {
            ctx: WasiCtxBuilder::new().build(),
            table: ResourceTable::new(),
            limiter: PeakLimiter::new(memory_limit),
        }
    }
}

impl WasiView for HostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.ctx,
            table: &mut self.table,
        }
    }
}

struct Runtime {
    engine: Engine,
    rich: Component,
    packed: Component,
}

impl Runtime {
    fn new() -> BenchResult<Self> {
        let mut config = Config::new();
        config.wasm_component_model(true);
        let engine = Engine::new(&config).map_err(|error| error.to_string())?;
        let rich = Component::new(&engine, RICH_COMPONENT_BYTES)
            .map_err(|error| format!("compile rich component: {error}"))?;
        let packed = Component::new(&engine, PACKED_COMPONENT_BYTES)
            .map_err(|error| format!("compile packed component: {error}"))?;
        Ok(Self {
            engine,
            rich,
            packed,
        })
    }

    fn linker(&self) -> BenchResult<Linker<HostState>> {
        let mut linker = Linker::new(&self.engine);
        add_to_linker_sync(&mut linker).map_err(|error| error.to_string())?;
        Ok(linker)
    }
}

struct RichRunner {
    store: Store<HostState>,
    bindings: rich_bindings::Plugin,
}

impl RichRunner {
    fn new(runtime: &Runtime, memory_limit: usize) -> BenchResult<Self> {
        let mut store = Store::new(&runtime.engine, HostState::new(memory_limit));
        store.limiter(|state| &mut state.limiter);
        let bindings =
            rich_bindings::Plugin::instantiate(&mut store, &runtime.rich, &runtime.linker()?)
                .map_err(|error| format!("instantiate rich component: {error}"))?;
        Ok(Self { store, bindings })
    }

    fn call(&mut self, data: &DataSet, operation: Operation) -> BenchResult<()> {
        let api = self.bindings.lix_abi_bench_rich_api();
        match operation {
            Operation::DetectEmpty => {
                let output = api
                    .call_detect_empty(&mut self.store, &data.entities, &data.file)
                    .map_err(|error| error.to_string())?
                    .map_err(|error| format!("rich guest error: {error}"))?;
                black_box(output.len());
            }
            Operation::EntityRoundTrip => {
                let output = api
                    .call_entity_round_trip(&mut self.store, &data.entities, &data.empty_file)
                    .map_err(|error| error.to_string())?
                    .map_err(|error| format!("rich guest error: {error}"))?;
                if output.len() != data.entities.len() {
                    return Err("rich entity round-trip returned the wrong count".to_string());
                }
                black_box(output.first().map(|change| change.schema_key.len()));
            }
            Operation::FileRoundTrip => {
                let output = api
                    .call_file_round_trip(&mut self.store, &[], &data.file)
                    .map_err(|error| error.to_string())?
                    .map_err(|error| format!("rich guest error: {error}"))?;
                if output.len() != data.file.data.len() {
                    return Err("rich file round-trip returned the wrong length".to_string());
                }
                black_box(output.first().copied());
            }
        }
        Ok(())
    }

    fn peak(&self) -> usize {
        self.store.data().limiter.peak_allowed
    }

    fn limiter_status(&self) -> String {
        let limiter = &self.store.data().limiter;
        format!(
            "peak={} max_requested={} denied={}",
            limiter.peak_allowed, limiter.max_requested, limiter.denied
        )
    }
}

struct PackedRunner {
    store: Store<HostState>,
    bindings: packed_bindings::Plugin,
}

impl PackedRunner {
    fn new(runtime: &Runtime, memory_limit: usize) -> BenchResult<Self> {
        let mut store = Store::new(&runtime.engine, HostState::new(memory_limit));
        store.limiter(|state| &mut state.limiter);
        let bindings =
            packed_bindings::Plugin::instantiate(&mut store, &runtime.packed, &runtime.linker()?)
                .map_err(|error| format!("instantiate packed component: {error}"))?;
        Ok(Self { store, bindings })
    }

    fn call(&mut self, data: &DataSet, operation: Operation) -> BenchResult<()> {
        let api = self.bindings.lix_abi_bench_packed_api();
        let input = data.packed_input(operation);
        let output = match operation {
            Operation::DetectEmpty => api.call_detect_empty(&mut self.store, input),
            Operation::EntityRoundTrip => api.call_entity_round_trip(&mut self.store, input),
            Operation::FileRoundTrip => api.call_file_round_trip(&mut self.store, input),
        }
        .map_err(|error| error.to_string())?
        .map_err(|error| format!("packed guest error: {error}"))?;
        if output.len() != data.packed_output_bytes(operation) {
            return Err("packed operation returned the wrong length".to_string());
        }
        black_box(output.first().copied());
        Ok(())
    }

    fn peak(&self) -> usize {
        self.store.data().limiter.peak_allowed
    }

    fn limiter_status(&self) -> String {
        let limiter = &self.store.data().limiter;
        format!(
            "peak={} max_requested={} denied={}",
            limiter.peak_allowed, limiter.max_requested, limiter.denied
        )
    }
}

#[derive(Debug)]
struct Measurements {
    samples: Vec<Duration>,
}

impl Measurements {
    fn p50(&self) -> Duration {
        percentile(&self.samples, 50)
    }

    fn p95(&self) -> Duration {
        percentile(&self.samples, 95)
    }
}

fn measure(
    sampling: Sampling,
    mut call: impl FnMut() -> BenchResult<()>,
) -> BenchResult<Measurements> {
    let started = Instant::now();
    call()?;
    let probe = started.elapsed().max(Duration::from_nanos(1));
    for _ in 1..sampling.warmups {
        call()?;
    }
    let target_samples = (sampling.target.as_nanos() / probe.as_nanos()) as usize;
    let count = target_samples.clamp(sampling.min_samples, sampling.max_samples);
    let mut samples = Vec::with_capacity(count);
    for _ in 0..count {
        let started = Instant::now();
        call()?;
        samples.push(started.elapsed());
    }
    samples.sort_unstable();
    Ok(Measurements { samples })
}

fn copy_floor(bytes: usize, sampling: Sampling) -> BenchResult<Measurements> {
    let source = vec![b'x'; bytes];
    let mut destination = vec![0_u8; bytes];
    measure(sampling, || {
        destination.copy_from_slice(black_box(&source));
        black_box(destination.first().copied());
        Ok(())
    })
}

fn percentile(samples: &[Duration], percentile: usize) -> Duration {
    let index = (samples.len() * percentile)
        .div_ceil(100)
        .saturating_sub(1)
        .min(samples.len() - 1);
    samples[index]
}

fn run_rich_case(
    runtime: &Runtime,
    data: &DataSet,
    operation: Operation,
    sampling: Sampling,
) -> BenchResult<()> {
    let mut runner = RichRunner::new(runtime, TIMING_MEMORY_LIMIT)?;
    let measured = measure(sampling, || runner.call(data, operation));
    let peak = runner.peak();
    let production = production_probe_rich(runtime, data, operation);
    let (input_bytes, output_bytes) = data.rich_io_bytes(operation);
    match measured {
        Ok(measured) => print_result(
            "rich",
            operation,
            data.file.data.len(),
            data.entities
                .first()
                .map_or(0, |entity| entity.snapshot_content.len()),
            data.entities.len(),
            input_bytes,
            output_bytes,
            &measured,
            peak,
            production.as_deref().unwrap_or("pass"),
            "ok",
        ),
        Err(error) => print_failure(
            "rich",
            operation,
            data,
            input_bytes,
            output_bytes,
            peak,
            &format!("{}; {}", one_line(&error), runner.limiter_status()),
            production.as_deref().unwrap_or("pass"),
        ),
    }
    Ok(())
}

fn run_packed_case(
    runtime: &Runtime,
    data: &DataSet,
    operation: Operation,
    sampling: Sampling,
) -> BenchResult<()> {
    let mut runner = PackedRunner::new(runtime, TIMING_MEMORY_LIMIT)?;
    let measured = measure(sampling, || runner.call(data, operation));
    let peak = runner.peak();
    let production = production_probe_packed(runtime, data, operation);
    let input_bytes = data.packed_input(operation).len();
    let output_bytes = data.packed_output_bytes(operation);
    match measured {
        Ok(measured) => print_result(
            "packed",
            operation,
            data.file.data.len(),
            data.entities
                .first()
                .map_or(0, |entity| entity.snapshot_content.len()),
            data.entities.len(),
            input_bytes,
            output_bytes,
            &measured,
            peak,
            production.as_deref().unwrap_or("pass"),
            "ok",
        ),
        Err(error) => print_failure(
            "packed",
            operation,
            data,
            input_bytes,
            output_bytes,
            peak,
            &format!("{}; {}", one_line(&error), runner.limiter_status()),
            production.as_deref().unwrap_or("pass"),
        ),
    }
    Ok(())
}

fn production_probe_rich(
    runtime: &Runtime,
    data: &DataSet,
    operation: Operation,
) -> Option<String> {
    match RichRunner::new(runtime, PRODUCTION_MEMORY_LIMIT) {
        Ok(mut runner) => match runner.call(data, operation) {
            Ok(()) => None,
            Err(error) => Some(format!(
                "fail:{}:{}",
                one_line(&error),
                runner.limiter_status()
            )),
        },
        Err(error) => Some(format!("instantiate-fail:{}", one_line(&error))),
    }
}

fn production_probe_packed(
    runtime: &Runtime,
    data: &DataSet,
    operation: Operation,
) -> Option<String> {
    match PackedRunner::new(runtime, PRODUCTION_MEMORY_LIMIT) {
        Ok(mut runner) => match runner.call(data, operation) {
            Ok(()) => None,
            Err(error) => Some(format!(
                "fail:{}:{}",
                one_line(&error),
                runner.limiter_status()
            )),
        },
        Err(error) => Some(format!("instantiate-fail:{}", one_line(&error))),
    }
}

fn print_result(
    abi: &str,
    operation: Operation,
    logical_bytes: usize,
    density: usize,
    entities: usize,
    input_bytes: usize,
    output_bytes: usize,
    measured: &Measurements,
    peak: usize,
    limit64: &str,
    status: &str,
) {
    println!(
        "result\t{abi}\t{}\t{logical_bytes}\t{density}\t{entities}\t{input_bytes}\t{output_bytes}\t{}\t{}\t{}\t{peak}\t{}\t{}",
        operation.name(),
        measured.samples.len(),
        measured.p50().as_secs_f64() * 1_000_000.0,
        measured.p95().as_secs_f64() * 1_000_000.0,
        one_line(limit64),
        one_line(status),
    );
}

fn print_failure(
    abi: &str,
    operation: Operation,
    data: &DataSet,
    input_bytes: usize,
    output_bytes: usize,
    peak: usize,
    status: &str,
    limit64: &str,
) {
    println!(
        "result\t{abi}\t{}\t{}\t{}\t{}\t{input_bytes}\t{output_bytes}\t0\t0\t0\t{peak}\t{}\t{}",
        operation.name(),
        data.file.data.len(),
        data.entities
            .first()
            .map_or(0, |entity| entity.snapshot_content.len()),
        data.entities.len(),
        one_line(limit64),
        one_line(status),
    );
}

fn one_line(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            '\t' | '\n' | '\r' => ' ',
            _ => character,
        })
        .collect()
}

fn run_profile(
    runtime: &Runtime,
    profile: &str,
    logical_bytes: usize,
    density: usize,
) -> BenchResult<()> {
    let (abi, operation_name) = profile
        .split_once('-')
        .ok_or_else(|| "profile must start with rich- or packed-".to_string())?;
    let operation = match operation_name {
        "detect-empty" => Operation::DetectEmpty,
        "entity-round-trip" => Operation::EntityRoundTrip,
        "file-round-trip" => Operation::FileRoundTrip,
        _ => return Err(format!("unknown profile operation {operation_name:?}")),
    };
    let iterations = env_usize("LIX_ABI_BENCH_PROFILE_ITERATIONS", 1_000)?;
    let data = DataSet::new(logical_bytes, density)?;
    let started = Instant::now();
    match abi {
        "rich" => {
            let mut runner = RichRunner::new(runtime, TIMING_MEMORY_LIMIT)?;
            for _ in 0..iterations {
                runner.call(&data, operation)?;
            }
            println!(
                "profile\tabi=rich\toperation={}\titerations={iterations}\telapsed_ms={}\tpeak_linear_bytes={}",
                operation.name(),
                started.elapsed().as_millis(),
                runner.peak()
            );
        }
        "packed" => {
            let mut runner = PackedRunner::new(runtime, TIMING_MEMORY_LIMIT)?;
            for _ in 0..iterations {
                runner.call(&data, operation)?;
            }
            println!(
                "profile\tabi=packed\toperation={}\titerations={iterations}\telapsed_ms={}\tpeak_linear_bytes={}",
                operation.name(),
                started.elapsed().as_millis(),
                runner.peak()
            );
        }
        _ => return Err(format!("unknown profile ABI {abi:?}")),
    }
    Ok(())
}

fn env_usize(name: &str, default: usize) -> BenchResult<usize> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|error| format!("invalid {name}={value:?}: {error}")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(format!("invalid {name}: {error}")),
    }
}

fn env_usize_list(name: &str, default: &[usize]) -> BenchResult<Vec<usize>> {
    let Ok(value) = env::var(name) else {
        return Ok(default.to_vec());
    };
    let values = value
        .split(',')
        .map(|part| {
            part.parse::<usize>()
                .map_err(|error| format!("invalid {name} item {part:?}: {error}"))
        })
        .collect::<BenchResult<Vec<_>>>()?;
    if values.is_empty() || values.contains(&0) {
        return Err(format!("{name} must contain non-zero integers"));
    }
    Ok(values)
}
