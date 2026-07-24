use std::cmp::Ordering;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use serde::Serialize;
use wasmtime::{Caller, Engine, Extern, Linker, Memory, Module, Store, TypedFunc};

const OP_INIT_PERSISTENT: i32 = 0;
const OP_STATELESS_V1: i32 = 1;
const OP_FULL_FILE_PERSISTENT: i32 = 2;
const OP_SPLICE_PERSISTENT: i32 = 3;
const OP_CHECKPOINT_REDUCER: i32 = 4;
const OP_SNAPSHOT: i32 = 5;
const OP_CREATE_CHECKPOINT: i32 = 7;
const OP_STATELESS_V1_CHECKPOINT: i32 = 8;
const OP_HOST_CONTEXT_FINE: i32 = 9;
const OP_HOST_CONTEXT_BATCHED: i32 = 10;
const OP_INIT_HYBRID_INDEX: i32 = 11;
const OP_HYBRID_INDEX_EDIT: i32 = 12;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Format {
    Csv,
    Markdown,
    Json,
    Excalidraw,
    Text,
}

impl Format {
    const ALL: [Self; 5] = [
        Self::Csv,
        Self::Markdown,
        Self::Json,
        Self::Excalidraw,
        Self::Text,
    ];

    fn code(self) -> u8 {
        match self {
            Self::Csv => 0,
            Self::Markdown => 1,
            Self::Json => 2,
            Self::Excalidraw => 3,
            Self::Text => 4,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Markdown => "markdown",
            Self::Json => "json",
            Self::Excalidraw => "excalidraw",
            Self::Text => "text",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|format| format.name() == value)
    }
}

#[derive(Clone, Debug)]
struct Options {
    wasm: PathBuf,
    sizes_kib: Vec<usize>,
    formats: Vec<Format>,
    iterations: usize,
    warmups: usize,
    output: Option<PathBuf>,
}

impl Options {
    fn parse() -> Result<Self> {
        let mut wasm = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../target/wasm32-unknown-unknown/release/plugin_api_v2_guest.wasm");
        let mut sizes_kib = vec![100, 1_024, 10_240];
        let mut formats = Format::ALL.to_vec();
        let mut iterations = 15;
        let mut warmups = 2;
        let mut output = None;
        let mut arguments = env::args().skip(1);
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--wasm" => wasm = PathBuf::from(required_value(&mut arguments, "--wasm")?),
                "--sizes-kib" => {
                    sizes_kib = required_value(&mut arguments, "--sizes-kib")?
                        .split(',')
                        .map(|value| value.parse().context("invalid --sizes-kib value"))
                        .collect::<Result<_>>()?;
                }
                "--formats" => {
                    formats = required_value(&mut arguments, "--formats")?
                        .split(',')
                        .map(|value| {
                            Format::parse(value)
                                .ok_or_else(|| anyhow!("unknown format in --formats: {value}"))
                        })
                        .collect::<Result<_>>()?;
                }
                "--iterations" => {
                    iterations = required_value(&mut arguments, "--iterations")?
                        .parse()
                        .context("invalid --iterations value")?;
                }
                "--warmups" => {
                    warmups = required_value(&mut arguments, "--warmups")?
                        .parse()
                        .context("invalid --warmups value")?;
                }
                "--output" => {
                    output = Some(PathBuf::from(required_value(&mut arguments, "--output")?));
                }
                "--quick" => {
                    sizes_kib = vec![100];
                    iterations = 5;
                    warmups = 1;
                }
                "--help" | "-h" => {
                    println!(
                        "plugin-api-v2-host [--wasm PATH] [--sizes-kib 100,1024,10240] \
                         [--formats csv,markdown,json,excalidraw,text] [--iterations N] \
                         [--warmups N] [--output PATH] [--quick]"
                    );
                    std::process::exit(0);
                }
                _ => bail!("unknown argument: {argument}"),
            }
        }
        if iterations == 0 {
            bail!("--iterations must be greater than zero");
        }
        Ok(Self {
            wasm,
            sizes_kib,
            formats,
            iterations,
            warmups,
            output,
        })
    }
}

fn required_value(arguments: &mut impl Iterator<Item = String>, option: &str) -> Result<String> {
    arguments
        .next()
        .ok_or_else(|| anyhow!("{option} requires a value"))
}

#[derive(Debug)]
struct Fixture {
    format: Format,
    original: Vec<u8>,
    variants: [Vec<u8>; 2],
    target_edit_offset: usize,
    entity_count: usize,
    target_entity_index: usize,
}

impl Fixture {
    fn generate(format: Format, target_bytes: usize) -> Self {
        let entity_count = entity_count(format, target_bytes);
        let approximate_overhead = match format {
            Format::Csv => 14,
            Format::Markdown => 21,
            Format::Json => 22,
            Format::Excalidraw => 87,
            Format::Text => 9,
        };
        let payload_width = target_bytes
            .checked_div(entity_count.max(1))
            .unwrap_or(1)
            .saturating_sub(approximate_overhead)
            .max(8);
        let target_entity_index = entity_count / 2;
        let mut original = Vec::with_capacity(target_bytes + 1_024);
        let mut target_edit_offset = None;

        match format {
            Format::Csv => {
                for index in 0..entity_count {
                    append(&mut original, &format!("{index:06},"));
                    append_payload(
                        &mut original,
                        payload_width,
                        index,
                        target_entity_index,
                        &mut target_edit_offset,
                    );
                    append(&mut original, ",tail\n");
                }
            }
            Format::Markdown => {
                for index in 0..entity_count {
                    append(&mut original, &format!("Paragraph {index:06} "));
                    append_payload(
                        &mut original,
                        payload_width,
                        index,
                        target_entity_index,
                        &mut target_edit_offset,
                    );
                    append(&mut original, "\n\n");
                }
            }
            Format::Json => {
                append(&mut original, "{\n");
                for index in 0..entity_count {
                    append(&mut original, &format!("  \"k{index:06}\": \""));
                    append_payload(
                        &mut original,
                        payload_width,
                        index,
                        target_entity_index,
                        &mut target_edit_offset,
                    );
                    append(
                        &mut original,
                        if index + 1 == entity_count {
                            "\"\n"
                        } else {
                            "\",\n"
                        },
                    );
                }
                append(&mut original, "}\n");
            }
            Format::Excalidraw => {
                append(&mut original, "{\"type\":\"excalidraw\",\"elements\":[\n");
                for index in 0..entity_count {
                    append(
                        &mut original,
                        &format!(
                            "{{\"id\":\"e{index:06}\",\"type\":\"rectangle\",\"x\":{index},\"y\":{index},\"customData\":\""
                        ),
                    );
                    append_payload(
                        &mut original,
                        payload_width,
                        index,
                        target_entity_index,
                        &mut target_edit_offset,
                    );
                    append(
                        &mut original,
                        if index + 1 == entity_count {
                            "\"}\n"
                        } else {
                            "\"},\n"
                        },
                    );
                }
                append(&mut original, "],\"appState\":{},\"files\":{}}\n");
            }
            Format::Text => {
                for index in 0..entity_count {
                    append(&mut original, &format!("{index:06} "));
                    append_payload(
                        &mut original,
                        payload_width,
                        index,
                        target_entity_index,
                        &mut target_edit_offset,
                    );
                    append(&mut original, "\n");
                }
            }
        }

        let target_edit_offset = target_edit_offset.expect("target payload was emitted");
        if matches!(format, Format::Json | Format::Excalidraw) {
            serde_json::from_slice::<serde_json::Value>(&original)
                .expect("generated structured fixture must be valid JSON");
        }
        let mut first = original.clone();
        let mut second = original.clone();
        first[target_edit_offset] = b'b';
        second.splice(
            target_edit_offset..target_edit_offset + 1,
            std::iter::repeat_n(b'c', 17),
        );
        Self {
            format,
            original,
            variants: [first, second],
            target_edit_offset,
            entity_count,
            target_entity_index,
        }
    }

    fn splice_request(&self, call_index: usize) -> Vec<u8> {
        if call_index.is_multiple_of(2) {
            let delete_len = if call_index == 0 { 1 } else { 17 };
            encode_splice(self.target_edit_offset, delete_len, b"b")
        } else {
            encode_splice(self.target_edit_offset, 1, &[b'c'; 17])
        }
    }
}

fn entity_count(format: Format, target_bytes: usize) -> usize {
    let large_scale = target_bytes >= 8 * 1_024 * 1_024;
    if large_scale {
        return match format {
            Format::Csv | Format::Text => 200_000,
            Format::Markdown | Format::Json | Format::Excalidraw => 50_000,
        };
    }
    let nominal_width = match format {
        Format::Csv | Format::Text => 52,
        Format::Markdown => 128,
        Format::Json => 200,
        Format::Excalidraw => 220,
    };
    (target_bytes / nominal_width).max(10)
}

fn append(output: &mut Vec<u8>, value: &str) {
    output.extend_from_slice(value.as_bytes());
}

fn append_payload(
    output: &mut Vec<u8>,
    width: usize,
    index: usize,
    target_index: usize,
    target_edit_offset: &mut Option<usize>,
) {
    let midpoint = width / 2;
    for offset in 0..width {
        if index == target_index && offset == midpoint {
            *target_edit_offset = Some(output.len());
        }
        output.push(b'a');
    }
}

fn encode_splice(start: usize, delete_len: usize, insert: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(16 + insert.len());
    output.extend_from_slice(&(start as u64).to_le_bytes());
    output.extend_from_slice(&(delete_len as u64).to_le_bytes());
    output.extend_from_slice(insert);
    output
}

#[derive(Clone, Debug)]
struct ContextEntity {
    id: u64,
    start: usize,
    end: usize,
}

#[derive(Debug)]
struct HostContext {
    before: Arc<[u8]>,
    after: Arc<[u8]>,
    entities: Arc<[ContextEntity]>,
    target_entity_index: usize,
    original_file_len: usize,
    import_calls: usize,
    import_bytes: usize,
}

impl Default for HostContext {
    fn default() -> Self {
        Self {
            before: Arc::from([]),
            after: Arc::from([]),
            entities: Arc::from([]),
            target_entity_index: 0,
            original_file_len: 0,
            import_calls: 0,
            import_bytes: 0,
        }
    }
}

impl HostContext {
    fn source(&self, view: i32) -> Option<&[u8]> {
        match view {
            0 => Some(&self.before),
            1 => Some(&self.after),
            _ => None,
        }
    }

    fn source_delta(&self, view: i32) -> Option<isize> {
        let source_len = self.source(view)?.len();
        Some(source_len as isize - self.original_file_len as isize)
    }

    fn entity_bounds(&self, view: i32, index: usize) -> Option<(usize, usize)> {
        let entity = self.entities.get(index)?;
        let delta = self.source_delta(view)?;
        if index < self.target_entity_index {
            Some((entity.start, entity.end))
        } else if index == self.target_entity_index {
            Some((entity.start, shift_usize(entity.end, delta)?))
        } else {
            Some((
                shift_usize(entity.start, delta)?,
                shift_usize(entity.end, delta)?,
            ))
        }
    }

    fn entity_at_offset(&self, offset: usize) -> Option<usize> {
        let candidate = self
            .entities
            .partition_point(|entity| entity.start <= offset)
            .checked_sub(1)?;
        let (start, end) = self.entity_bounds(0, candidate)?;
        (offset >= start && offset <= end).then_some(candidate)
    }

    fn note_import(&mut self, bytes: usize) {
        self.import_calls += 1;
        self.import_bytes += bytes;
    }
}

fn shift_usize(value: usize, delta: isize) -> Option<usize> {
    usize::try_from(isize::try_from(value).ok()?.checked_add(delta)?).ok()
}

struct WasmGuest {
    store: Store<HostContext>,
    memory: Memory,
    alloc: TypedFunc<i32, i32>,
    dealloc: TypedFunc<(i32, i32), ()>,
    run: TypedFunc<(i32, i32, i32), i64>,
}

#[derive(Debug)]
struct Invocation {
    output: Vec<u8>,
    elapsed: Duration,
    memory_bytes: usize,
}

impl WasmGuest {
    fn instantiate(engine: &Engine, module: &Module) -> Result<Self> {
        let mut store = Store::new(engine, HostContext::default());
        let mut linker = Linker::new(engine);
        install_context_imports(&mut linker)?;
        let instance = linker
            .instantiate(&mut store, module)
            .map_err(wasmtime_error)?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .context("guest did not export memory")?;
        let alloc = instance
            .get_typed_func(&mut store, "alloc")
            .map_err(wasmtime_error)?;
        let dealloc = instance
            .get_typed_func(&mut store, "dealloc")
            .map_err(wasmtime_error)?;
        let run = instance
            .get_typed_func(&mut store, "run")
            .map_err(wasmtime_error)?;
        Ok(Self {
            store,
            memory,
            alloc,
            dealloc,
            run,
        })
    }

    fn set_context(
        &mut self,
        before: Arc<[u8]>,
        after: Arc<[u8]>,
        entities: Arc<[ContextEntity]>,
        target_entity_index: usize,
        original_file_len: usize,
    ) {
        let context = self.store.data_mut();
        context.before = before;
        context.after = after;
        context.entities = entities;
        context.target_entity_index = target_entity_index;
        context.original_file_len = original_file_len;
    }

    fn reset_import_stats(&mut self) {
        let context = self.store.data_mut();
        context.import_calls = 0;
        context.import_bytes = 0;
    }

    fn import_stats(&self) -> (usize, usize) {
        let context = self.store.data();
        (context.import_calls, context.import_bytes)
    }

    fn invoke(&mut self, operation: i32, input: &[u8]) -> Result<Invocation> {
        let input_len = i32::try_from(input.len()).context("input exceeds wasm32 ABI")?;
        let started = Instant::now();
        let pointer = self
            .alloc
            .call(&mut self.store, input_len)
            .map_err(wasmtime_error)?;
        self.memory.write(
            &mut self.store,
            usize::try_from(pointer).context("negative guest pointer")?,
            input,
        )?;
        let packed = self
            .run
            .call(&mut self.store, (operation, pointer, input_len))
            .map_err(wasmtime_error)? as u64;
        let output_pointer = usize::try_from(packed >> 32).expect("wasm32 output pointer");
        let output_len =
            usize::try_from(packed & u64::from(u32::MAX)).expect("wasm32 output length");
        let mut output = vec![0_u8; output_len];
        self.memory.read(&self.store, output_pointer, &mut output)?;
        self.dealloc
            .call(
                &mut self.store,
                (
                    i32::try_from(output_pointer).context("output pointer exceeds i32")?,
                    i32::try_from(output_len).context("output length exceeds i32")?,
                ),
            )
            .map_err(wasmtime_error)?;
        let elapsed = started.elapsed();
        let memory_bytes = self.memory.data_size(&self.store);
        Ok(Invocation {
            output,
            elapsed,
            memory_bytes,
        })
    }
}

fn install_context_imports(linker: &mut Linker<HostContext>) -> Result<()> {
    linker
        .func_wrap(
            "lix_context",
            "context_entity_at_offset",
            |mut caller: Caller<'_, HostContext>, offset: i32| -> i32 {
                let resolved = usize::try_from(offset)
                    .ok()
                    .and_then(|offset| caller.data().entity_at_offset(offset))
                    .and_then(|index| i32::try_from(index).ok())
                    .unwrap_or(-1);
                caller.data_mut().note_import(4);
                resolved
            },
        )
        .map_err(wasmtime_error)?;
    linker
        .func_wrap(
            "lix_context",
            "context_entity_count",
            |mut caller: Caller<'_, HostContext>| -> i32 {
                let count = i32::try_from(caller.data().entities.len()).unwrap_or(-1);
                caller.data_mut().note_import(4);
                count
            },
        )
        .map_err(wasmtime_error)?;
    linker
        .func_wrap(
            "lix_context",
            "context_entity_id",
            |mut caller: Caller<'_, HostContext>, index: i32| -> i64 {
                let id = usize::try_from(index)
                    .ok()
                    .and_then(|index| caller.data().entities.get(index))
                    .map(|entity| entity.id as i64)
                    .unwrap_or(-1);
                caller.data_mut().note_import(8);
                id
            },
        )
        .map_err(wasmtime_error)?;
    for (name, select_end) in [
        ("context_entity_start", false),
        ("context_entity_end", true),
    ] {
        linker
            .func_wrap(
                "lix_context",
                name,
                move |mut caller: Caller<'_, HostContext>, view: i32, index: i32| -> i32 {
                    let bound = usize::try_from(index)
                        .ok()
                        .and_then(|index| caller.data().entity_bounds(view, index))
                        .map(|(start, end)| if select_end { end } else { start })
                        .and_then(|bound| i32::try_from(bound).ok())
                        .unwrap_or(-1);
                    caller.data_mut().note_import(4);
                    bound
                },
            )
            .map_err(wasmtime_error)?;
    }
    linker
        .func_wrap(
            "lix_context",
            "context_source_byte",
            |mut caller: Caller<'_, HostContext>, view: i32, offset: i32| -> i32 {
                let byte = usize::try_from(offset)
                    .ok()
                    .and_then(|offset| caller.data().source(view)?.get(offset).copied())
                    .map(i32::from)
                    .unwrap_or(-1);
                caller.data_mut().note_import(1);
                byte
            },
        )
        .map_err(wasmtime_error)?;
    linker
        .func_wrap(
            "lix_context",
            "context_source_len",
            |mut caller: Caller<'_, HostContext>, view: i32| -> i32 {
                let len = caller
                    .data()
                    .source(view)
                    .and_then(|source| i32::try_from(source.len()).ok())
                    .unwrap_or(-1);
                caller.data_mut().note_import(4);
                len
            },
        )
        .map_err(wasmtime_error)?;
    linker
        .func_wrap(
            "lix_context",
            "context_source_read",
            |mut caller: Caller<'_, HostContext>,
             view: i32,
             offset: i32,
             pointer: i32,
             len: i32|
             -> i32 {
                let requested = (|| {
                    let offset = usize::try_from(offset).ok()?;
                    let pointer = usize::try_from(pointer).ok()?;
                    let len = usize::try_from(len).ok()?;
                    let end = offset.checked_add(len)?;
                    let bytes = caller.data().source(view)?.get(offset..end)?.to_vec();
                    let memory = caller.get_export("memory").and_then(Extern::into_memory)?;
                    memory.write(&mut caller, pointer, &bytes).ok()?;
                    Some(len)
                })();
                let bytes = requested.unwrap_or(0);
                caller.data_mut().note_import(bytes);
                requested
                    .and_then(|len| i32::try_from(len).ok())
                    .unwrap_or(-1)
            },
        )
        .map_err(wasmtime_error)?;
    Ok(())
}

#[derive(Debug, Serialize)]
struct Report {
    schema_version: u32,
    guest_target: &'static str,
    wasmtime_version: &'static str,
    host_architecture: &'static str,
    host_operating_system: &'static str,
    guest_module_bytes: usize,
    timing_scope: &'static str,
    fixture_scope: &'static str,
    iterations: usize,
    warmups: usize,
    records: Vec<Record>,
}

#[derive(Debug, Serialize)]
struct Record {
    format: Format,
    requested_kib: usize,
    actual_file_bytes: usize,
    entity_count: usize,
    candidate: &'static str,
    api_input: &'static str,
    p50_ms: f64,
    p95_ms: f64,
    speedup_over_v1_p50: f64,
    initialization_ms: f64,
    average_boundary_input_bytes: usize,
    average_boundary_output_bytes: usize,
    average_host_import_calls: usize,
    average_host_import_bytes: usize,
    retained_checkpoint_bytes: usize,
    host_private_index_bytes: usize,
    guest_retained_index_bytes: usize,
    peak_guest_linear_memory_bytes: usize,
    peak_growth_after_initialization_bytes: usize,
    exceeds_64_mib_guest_limit: bool,
    stable_id_preserved: bool,
    exact_result_verified: bool,
}

#[derive(Clone, Copy, Debug)]
enum Candidate {
    StatelessV1,
    PersistentFullFile,
    PersistentSplice,
    HybridGuestIndex,
    CopiedCheckpoint,
    HostContextFine,
    HostContextBatched,
}

impl Candidate {
    const ALL: [Self; 7] = [
        Self::StatelessV1,
        Self::PersistentFullFile,
        Self::PersistentSplice,
        Self::HybridGuestIndex,
        Self::CopiedCheckpoint,
        Self::HostContextFine,
        Self::HostContextBatched,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::StatelessV1 => "v1_stateless_full_state_and_file",
            Self::PersistentFullFile => "a_persistent_full_file",
            Self::PersistentSplice => "b_persistent_splice",
            Self::HybridGuestIndex => "b2_guest_index_host_source",
            Self::CopiedCheckpoint => "c_copied_checkpoint_reducer",
            Self::HostContextFine => "d_host_context_fine_imports",
            Self::HostContextBatched => "d_host_context_batched_ranges",
        }
    }

    fn api_input(self) -> &'static str {
        match self {
            Self::StatelessV1 => "checkpoint + full new file; returns 32-byte outcome",
            Self::PersistentFullFile => "full new file; returns 32-byte outcome",
            Self::PersistentSplice => "byte splice; returns 32-byte outcome",
            Self::HybridGuestIndex => {
                "byte splice + two host source ranges; returns exact changed entity"
            }
            Self::CopiedCheckpoint => "checkpoint + byte splice; returns checkpoint",
            Self::HostContextFine => {
                "byte splice + point KV and per-byte source imports; returns 32-byte outcome"
            }
            Self::HostContextBatched => {
                "byte splice + point KV and source-range imports; returns 32-byte outcome"
            }
        }
    }

    fn uses_host_sources(self) -> bool {
        matches!(
            self,
            Self::HybridGuestIndex | Self::HostContextFine | Self::HostContextBatched
        )
    }

    fn uses_host_semantic_index(self) -> bool {
        matches!(self, Self::HostContextFine | Self::HostContextBatched)
    }
}

fn main() -> Result<()> {
    let options = Options::parse()?;
    let wasm = fs::read(&options.wasm)
        .with_context(|| format!("failed to read guest module at {}", options.wasm.display()))?;
    let engine = Engine::default();
    let module = Module::new(&engine, &wasm).map_err(wasmtime_error)?;
    let mut records = Vec::new();

    for requested_kib in &options.sizes_kib {
        for format in &options.formats {
            let fixture = Fixture::generate(*format, requested_kib * 1_024);
            eprintln!(
                "benchmarking {} at {:.2} MiB ({} entities)",
                format.name(),
                fixture.original.len() as f64 / (1024.0 * 1024.0),
                fixture.entity_count
            );
            let start = records.len();
            for candidate in Candidate::ALL {
                records.push(benchmark_candidate(
                    &engine,
                    &module,
                    &fixture,
                    *requested_kib,
                    candidate,
                    options.warmups,
                    options.iterations,
                )?);
            }
            let v1_p50 = records[start].p50_ms;
            for record in &mut records[start..] {
                record.speedup_over_v1_p50 = v1_p50 / record.p50_ms;
            }
        }
    }

    let report = Report {
        schema_version: 1,
        guest_target: "wasm32-unknown-unknown",
        wasmtime_version: "45",
        host_architecture: env::consts::ARCH,
        host_operating_system: env::consts::OS,
        guest_module_bytes: wasm.len(),
        timing_scope: "guest alloc + host-to-guest copy + Wasm execution + guest-to-host copy + guest dealloc",
        fixture_scope: "synthetic valid CSV/JSON/Excalidraw plus Markdown/text; localized value alternates between 1 and 17 bytes",
        iterations: options.iterations,
        warmups: options.warmups,
        records,
    };
    let json = serde_json::to_string_pretty(&report)?;
    if let Some(output) = options.output {
        if let Some(parent) = output.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        fs::write(&output, json)
            .with_context(|| format!("failed to write {}", output.display()))?;
        eprintln!(
            "wrote {} benchmark records to {}",
            report.records.len(),
            output.display()
        );
    } else {
        println!("{json}");
    }
    Ok(())
}

fn benchmark_candidate(
    engine: &Engine,
    module: &Module,
    fixture: &Fixture,
    requested_kib: usize,
    candidate: Candidate,
    warmups: usize,
    iterations: usize,
) -> Result<Record> {
    let mut guest = WasmGuest::instantiate(engine, module)?;
    let initial_input = with_format(fixture.format, &fixture.original);
    let mut stateless_checkpoints = None;
    let (mut checkpoint, initialization_ms) = match candidate {
        Candidate::PersistentFullFile | Candidate::PersistentSplice => {
            let initialization = guest.invoke(OP_INIT_PERSISTENT, &initial_input)?;
            (
                guest.invoke(OP_SNAPSHOT, &[])?.output,
                milliseconds(initialization.elapsed),
            )
        }
        Candidate::StatelessV1 => {
            let mut setup_guest = WasmGuest::instantiate(engine, module)?;
            let initialization = setup_guest.invoke(OP_CREATE_CHECKPOINT, &initial_input)?;
            let base = initialization.output;
            let first = setup_guest
                .invoke(
                    OP_CREATE_CHECKPOINT,
                    &with_format(fixture.format, &fixture.variants[0]),
                )?
                .output;
            let second = setup_guest
                .invoke(
                    OP_CREATE_CHECKPOINT,
                    &with_format(fixture.format, &fixture.variants[1]),
                )?
                .output;
            let initialization_ms = milliseconds(initialization.elapsed);
            stateless_checkpoints = Some([base.clone(), first, second]);
            (base, initialization_ms)
        }
        Candidate::HybridGuestIndex => {
            let mut setup_guest = WasmGuest::instantiate(engine, module)?;
            let checkpoint = setup_guest
                .invoke(OP_CREATE_CHECKPOINT, &initial_input)?
                .output;
            let source: Arc<[u8]> = Arc::from(fixture.original.clone().into_boxed_slice());
            guest.set_context(
                source.clone(),
                source,
                Arc::from([]),
                fixture.target_entity_index,
                fixture.original.len(),
            );
            let initialization = guest.invoke(OP_INIT_HYBRID_INDEX, &[fixture.format.code()])?;
            (checkpoint, milliseconds(initialization.elapsed))
        }
        Candidate::CopiedCheckpoint
        | Candidate::HostContextFine
        | Candidate::HostContextBatched => {
            let mut setup_guest = WasmGuest::instantiate(engine, module)?;
            let initialization = setup_guest.invoke(OP_CREATE_CHECKPOINT, &initial_input)?;
            (initialization.output, milliseconds(initialization.elapsed))
        }
    };
    let retained_checkpoint_bytes = if candidate.uses_host_sources() {
        0
    } else {
        checkpoint.len()
    };
    let initial_id = checkpoint_entity_id(&checkpoint, fixture.target_entity_index)?;
    let entity_count = checkpoint_entity_count(&checkpoint)?;
    let context_entities: Arc<[ContextEntity]> =
        Arc::from(checkpoint_entities(&checkpoint)?.into_boxed_slice());
    let empty_context_entities: Arc<[ContextEntity]> = Arc::from([]);
    let sources: [Arc<[u8]>; 3] = [
        Arc::from(fixture.original.clone().into_boxed_slice()),
        Arc::from(fixture.variants[0].clone().into_boxed_slice()),
        Arc::from(fixture.variants[1].clone().into_boxed_slice()),
    ];
    let initialized_memory = guest.memory.data_size(&guest.store);
    let mut peak_memory = initialized_memory;
    let mut durations = Vec::with_capacity(iterations);
    let mut input_bytes = 0_usize;
    let mut output_bytes = 0_usize;
    let mut total_calls = 0_usize;
    let mut last_output = Vec::new();

    for measured_index in 0..(warmups + iterations) {
        if measured_index == warmups {
            guest.reset_import_stats();
        }
        let variant = measured_index % 2;
        if candidate.uses_host_sources() {
            let before_source = if measured_index == 0 {
                sources[0].clone()
            } else {
                sources[1 + (measured_index - 1) % 2].clone()
            };
            guest.set_context(
                before_source,
                sources[1 + variant].clone(),
                if candidate.uses_host_semantic_index() {
                    context_entities.clone()
                } else {
                    empty_context_entities.clone()
                },
                fixture.target_entity_index,
                fixture.original.len(),
            );
        }
        let request = match candidate {
            Candidate::StatelessV1 => encode_state_request(
                checkpoint_before_stateless_call(
                    stateless_checkpoints.as_ref().expect("v1 checkpoints"),
                    measured_index,
                ),
                &fixture.variants[variant],
            ),
            Candidate::PersistentFullFile => fixture.variants[variant].clone(),
            Candidate::PersistentSplice => fixture.splice_request(measured_index),
            Candidate::HybridGuestIndex => fixture.splice_request(measured_index),
            Candidate::CopiedCheckpoint => {
                encode_state_request(&checkpoint, &fixture.splice_request(measured_index))
            }
            Candidate::HostContextFine | Candidate::HostContextBatched => {
                fixture.splice_request(measured_index)
            }
        };
        let operation = match candidate {
            Candidate::StatelessV1 => OP_STATELESS_V1,
            Candidate::PersistentFullFile => OP_FULL_FILE_PERSISTENT,
            Candidate::PersistentSplice => OP_SPLICE_PERSISTENT,
            Candidate::HybridGuestIndex => OP_HYBRID_INDEX_EDIT,
            Candidate::CopiedCheckpoint => OP_CHECKPOINT_REDUCER,
            Candidate::HostContextFine => OP_HOST_CONTEXT_FINE,
            Candidate::HostContextBatched => OP_HOST_CONTEXT_BATCHED,
        };
        let invocation = guest.invoke(operation, &request)?;
        peak_memory = peak_memory.max(invocation.memory_bytes);
        if matches!(candidate, Candidate::CopiedCheckpoint) {
            checkpoint = invocation.output.clone();
        }
        if candidate.uses_host_sources() {
            last_output = invocation.output.clone();
        }
        if measured_index >= warmups {
            durations.push(invocation.elapsed);
            input_bytes += request.len();
            output_bytes += invocation.output.len();
            total_calls += 1;
        }
    }

    let final_variant = (warmups + iterations - 1) % 2;
    let (stable_id_preserved, exact_result_verified) = match candidate {
        Candidate::PersistentFullFile | Candidate::PersistentSplice => {
            checkpoint = guest.invoke(OP_SNAPSHOT, &[])?.output;
            validate_checkpoint_result(&checkpoint, fixture, final_variant, initial_id)?
        }
        Candidate::StatelessV1 => {
            let previous = checkpoint_before_stateless_call(
                stateless_checkpoints.as_ref().expect("v1 checkpoints"),
                warmups + iterations - 1,
            );
            let request = encode_state_request(previous, &fixture.variants[final_variant]);
            checkpoint = guest.invoke(OP_STATELESS_V1_CHECKPOINT, &request)?.output;
            validate_checkpoint_result(&checkpoint, fixture, final_variant, initial_id)?
        }
        Candidate::CopiedCheckpoint => {
            validate_checkpoint_result(&checkpoint, fixture, final_variant, initial_id)?
        }
        Candidate::HybridGuestIndex => validate_hybrid_result(
            &last_output,
            &context_entities,
            fixture,
            final_variant,
            initial_id,
        )?,
        Candidate::HostContextFine | Candidate::HostContextBatched => validate_host_context_result(
            &last_output,
            &context_entities,
            fixture,
            final_variant,
            initial_id,
        )?,
    };
    if !stable_id_preserved || !exact_result_verified {
        bail!(
            "{} {} failed correctness: stable_id={stable_id_preserved}, exact={exact_result_verified}",
            fixture.format.name(),
            candidate.name()
        );
    }

    let (host_import_calls, host_import_bytes) = guest.import_stats();
    let host_private_index_bytes = if candidate.uses_host_semantic_index() {
        context_entities.len() * std::mem::size_of::<ContextEntity>()
    } else {
        0
    };
    let guest_retained_index_bytes = if matches!(candidate, Candidate::HybridGuestIndex) {
        entity_count * 24
    } else {
        0
    };
    Ok(Record {
        format: fixture.format,
        requested_kib,
        actual_file_bytes: fixture.original.len(),
        entity_count,
        candidate: candidate.name(),
        api_input: candidate.api_input(),
        p50_ms: milliseconds(percentile(&durations, 0.50)),
        p95_ms: milliseconds(percentile(&durations, 0.95)),
        speedup_over_v1_p50: 0.0,
        initialization_ms,
        average_boundary_input_bytes: input_bytes / total_calls,
        average_boundary_output_bytes: output_bytes / total_calls,
        average_host_import_calls: host_import_calls / total_calls,
        average_host_import_bytes: host_import_bytes / total_calls,
        retained_checkpoint_bytes,
        host_private_index_bytes,
        guest_retained_index_bytes,
        peak_guest_linear_memory_bytes: peak_memory,
        peak_growth_after_initialization_bytes: peak_memory.saturating_sub(initialized_memory),
        exceeds_64_mib_guest_limit: peak_memory > 64 * 1_024 * 1_024,
        stable_id_preserved,
        exact_result_verified,
    })
}

fn checkpoint_before_stateless_call(checkpoints: &[Vec<u8>; 3], call_index: usize) -> &[u8] {
    if call_index == 0 {
        &checkpoints[0]
    } else {
        &checkpoints[1 + (call_index - 1) % 2]
    }
}

fn validate_checkpoint_result(
    checkpoint: &[u8],
    fixture: &Fixture,
    final_variant: usize,
    initial_id: u64,
) -> Result<(bool, bool)> {
    let final_id = checkpoint_entity_id(checkpoint, fixture.target_entity_index)?;
    let last_changed_id = checkpoint_last_changed_id(checkpoint)?;
    let final_bytes = checkpoint_file(checkpoint)?;
    Ok((
        initial_id == final_id && last_changed_id == initial_id,
        final_bytes == fixture.variants[final_variant],
    ))
}

fn validate_host_context_result(
    outcome: &[u8],
    entities: &[ContextEntity],
    fixture: &Fixture,
    final_variant: usize,
    initial_id: u64,
) -> Result<(bool, bool)> {
    if outcome.len() != 32 {
        bail!(
            "host-context outcome was {} bytes, expected 32",
            outcome.len()
        );
    }
    let reported_count = usize::try_from(read_u64(outcome, 0)?).context("outcome count")?;
    let reported_id = read_u64(outcome, 8)?;
    let reported_hash = read_u64(outcome, 16)?;
    let reported_changed = read_u64(outcome, 24)? == 1;
    let entity = entities
        .get(fixture.target_entity_index)
        .context("host-context target entity")?;
    let delta = fixture.variants[final_variant].len() as isize - fixture.original.len() as isize;
    let end = shift_usize(entity.end, delta).context("host-context entity end")?;
    let expected_hash = fnv1a(&fixture.variants[final_variant][entity.start..end]);
    Ok((
        reported_count == entities.len() && reported_id == initial_id,
        reported_changed && reported_hash == expected_hash,
    ))
}

fn validate_hybrid_result(
    outcome: &[u8],
    entities: &[ContextEntity],
    fixture: &Fixture,
    final_variant: usize,
    initial_id: u64,
) -> Result<(bool, bool)> {
    if outcome.len() < 24 {
        bail!(
            "hybrid outcome was {} bytes, expected at least 24",
            outcome.len()
        );
    }
    let reported_count = usize::try_from(read_u64(outcome, 0)?).context("outcome count")?;
    let reported_id = read_u64(outcome, 8)?;
    let reported_len = usize::try_from(read_u64(outcome, 16)?).context("outcome length")?;
    let reported_bytes = outcome.get(24..).context("hybrid outcome bytes")?;
    let entity = entities
        .get(fixture.target_entity_index)
        .context("hybrid target entity")?;
    let delta = fixture.variants[final_variant].len() as isize - fixture.original.len() as isize;
    let end = shift_usize(entity.end, delta).context("hybrid entity end")?;
    let expected_bytes = &fixture.variants[final_variant][entity.start..end];
    Ok((
        reported_count == entities.len() && reported_id == initial_id,
        reported_len == expected_bytes.len() && reported_bytes == expected_bytes,
    ))
}

fn with_format(format: Format, bytes: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(1 + bytes.len());
    output.push(format.code());
    output.extend_from_slice(bytes);
    output
}

fn encode_state_request(checkpoint: &[u8], update: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(8 + checkpoint.len() + update.len());
    output.extend_from_slice(&(checkpoint.len() as u64).to_le_bytes());
    output.extend_from_slice(checkpoint);
    output.extend_from_slice(update);
    output
}

fn checkpoint_file(checkpoint: &[u8]) -> Result<&[u8]> {
    validate_checkpoint(checkpoint)?;
    let file_len = usize::try_from(read_u64(checkpoint, 40)?).context("file length")?;
    checkpoint
        .get(56..56 + file_len)
        .context("checkpoint file payload")
}

fn checkpoint_last_changed_id(checkpoint: &[u8]) -> Result<u64> {
    validate_checkpoint(checkpoint)?;
    read_u64(checkpoint, 24)
}

fn checkpoint_entity_count(checkpoint: &[u8]) -> Result<usize> {
    validate_checkpoint(checkpoint)?;
    usize::try_from(read_u64(checkpoint, 48)?).context("entity count")
}

fn checkpoint_entity_id(checkpoint: &[u8], entity_index: usize) -> Result<u64> {
    let file_len = checkpoint_file(checkpoint)?.len();
    let entity_count = checkpoint_entity_count(checkpoint)?;
    if entity_index >= entity_count {
        bail!("target entity {entity_index} is outside {entity_count} entities");
    }
    read_u64(checkpoint, 56 + file_len + entity_index * 16)
}

fn checkpoint_entities(checkpoint: &[u8]) -> Result<Vec<ContextEntity>> {
    let file_len = checkpoint_file(checkpoint)?.len();
    let entity_count = checkpoint_entity_count(checkpoint)?;
    let records = checkpoint
        .get(56 + file_len..)
        .context("checkpoint entity records")?;
    if records.len() != entity_count * 16 {
        bail!("checkpoint entity record length mismatch");
    }
    records
        .chunks_exact(16)
        .map(|record| {
            Ok(ContextEntity {
                id: read_u64(record, 0)?,
                start: u32::from_le_bytes(record[8..12].try_into().expect("entity start")) as usize,
                end: u32::from_le_bytes(record[12..16].try_into().expect("entity end")) as usize,
            })
        })
        .collect()
}

fn validate_checkpoint(checkpoint: &[u8]) -> Result<()> {
    if checkpoint.get(0..8) != Some(b"LIXAPIV2".as_slice()) {
        bail!("invalid checkpoint magic");
    }
    Ok(())
}

fn read_u64(input: &[u8], offset: usize) -> Result<u64> {
    let bytes: [u8; 8] = input
        .get(offset..offset + 8)
        .context("missing u64 field")?
        .try_into()
        .expect("eight-byte slice");
    Ok(u64::from_le_bytes(bytes))
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(FNV_OFFSET, |mut hash, byte| {
        hash ^= u64::from(*byte);
        hash.wrapping_mul(FNV_PRIME)
    })
}

fn percentile(values: &[Duration], quantile: f64) -> Duration {
    let mut values = values.to_vec();
    values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    let rank = ((values.len() as f64 * quantile).ceil() as usize)
        .saturating_sub(1)
        .min(values.len() - 1);
    values[rank]
}

fn milliseconds(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn wasmtime_error(error: wasmtime::Error) -> anyhow::Error {
    anyhow!("Wasmtime error: {error:?}")
}

#[allow(dead_code)]
fn _path_is_used_for_help_text(path: &Path) -> &Path {
    path
}
