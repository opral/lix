use anyhow::{Context, Result, anyhow, bail};
use std::fmt::Write as _;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use wasmtime::{
    Config, Engine, Instance, Memory, Module, OptLevel, Store, StoreLimits, StoreLimitsBuilder,
    TypedFunc,
};

const MIB: usize = 1024 * 1024;
const PATCH_MAGIC: u64 = 0x4c49_585f_4353_5631;
const PATCH_HEADER_LEN: usize = 64;

fn from_wasmtime<T>(result: std::result::Result<T, wasmtime::Error>) -> Result<T> {
    result.map_err(|error| anyhow!(error.to_string()))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Architecture {
    Stateless,
    Persistent,
}

#[derive(Debug)]
struct Options {
    memory_mib: usize,
    sizes_mib: Vec<usize>,
    iterations_1mib: usize,
    iterations_10mib: usize,
    architectures: Vec<Architecture>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            memory_mib: 64,
            sizes_mib: vec![1, 10],
            iterations_1mib: 80,
            iterations_10mib: 40,
            architectures: vec![Architecture::Stateless, Architecture::Persistent],
        }
    }
}

impl Options {
    fn parse() -> Result<Self> {
        let mut options = Self::default();
        let mut arguments = std::env::args().skip(1);
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--memory-mib" => {
                    options.memory_mib = arguments
                        .next()
                        .context("--memory-mib needs a value")?
                        .parse()
                        .context("invalid --memory-mib")?;
                }
                "--size-mib" => {
                    let value = arguments.next().context("--size-mib needs a value")?;
                    options.sizes_mib = match value.as_str() {
                        "all" => vec![1, 10],
                        "1" => vec![1],
                        "10" => vec![10],
                        _ => bail!("--size-mib must be 1, 10, or all"),
                    };
                }
                "--iterations-1mib" => {
                    options.iterations_1mib = arguments
                        .next()
                        .context("--iterations-1mib needs a value")?
                        .parse()
                        .context("invalid --iterations-1mib")?;
                }
                "--iterations-10mib" => {
                    options.iterations_10mib = arguments
                        .next()
                        .context("--iterations-10mib needs a value")?
                        .parse()
                        .context("invalid --iterations-10mib")?;
                }
                "--architecture" => {
                    let value = arguments.next().context("--architecture needs a value")?;
                    options.architectures = match value.as_str() {
                        "both" => vec![Architecture::Stateless, Architecture::Persistent],
                        "stateless" => vec![Architecture::Stateless],
                        "persistent" => vec![Architecture::Persistent],
                        _ => bail!("--architecture must be both, stateless, or persistent"),
                    };
                }
                "--help" | "-h" => {
                    println!(
                        "persistent-csv-wasm-bench\n\
                         \n\
                         --memory-mib N          Wasm linear-memory ceiling (default 64)\n\
                         --size-mib 1|10|all     Dataset selection (default all)\n\
                         --iterations-1mib N     Measured warm calls (default 80)\n\
                         --iterations-10mib N    Measured warm calls (default 40)\n\
                         --architecture stateless|persistent|both"
                    );
                    std::process::exit(0);
                }
                _ => bail!("unknown argument: {argument}"),
            }
        }
        Ok(options)
    }

    fn iterations(&self, size_mib: usize) -> usize {
        match size_mib {
            1 => self.iterations_1mib,
            10 => self.iterations_10mib,
            _ => unreachable!(),
        }
    }
}

#[derive(Debug)]
struct HostState {
    limits: StoreLimits,
}

struct Guest {
    store: Store<HostState>,
    memory: Memory,
    alloc: TypedFunc<u32, u32>,
    dealloc: TypedFunc<(u32, u32), ()>,
    stateless: TypedFunc<(u32, u32, u32, u32), u32>,
    hydrate: TypedFunc<(u32, u32), u32>,
    apply_splice: TypedFunc<(u32, u32, u32, u32), u32>,
    result_pointer: TypedFunc<(), u32>,
    result_length: TypedFunc<(), u32>,
    changed_rows: TypedFunc<(), u64>,
    changed_cells: TypedFunc<(), u64>,
    logical_document_bytes: TypedFunc<(), u64>,
}

impl Guest {
    fn instantiate(
        engine: &Engine,
        module: &Module,
        memory_limit: usize,
    ) -> Result<(Self, Duration)> {
        let limits = StoreLimitsBuilder::new().memory_size(memory_limit).build();
        let mut store = Store::new(engine, HostState { limits });
        store.limiter(|state| &mut state.limits);
        let started = Instant::now();
        let instance =
            from_wasmtime(Instance::new(&mut store, module, &[])).context("instantiate guest")?;
        let elapsed = started.elapsed();
        let memory = instance
            .get_memory(&mut store, "memory")
            .context("guest did not export memory")?;

        macro_rules! function {
            ($name:literal) => {
                from_wasmtime(instance.get_typed_func(&mut store, $name))
                    .with_context(|| format!("missing guest export {}", $name))?
            };
        }

        Ok((
            Self {
                alloc: function!("guest_alloc"),
                dealloc: function!("guest_dealloc"),
                stateless: function!("stateless_diff_and_render"),
                hydrate: function!("hydrate"),
                apply_splice: function!("apply_splice"),
                result_pointer: function!("result_pointer"),
                result_length: function!("result_length"),
                changed_rows: function!("changed_rows"),
                changed_cells: function!("changed_cells"),
                logical_document_bytes: function!("logical_document_bytes"),
                store,
                memory,
            },
            elapsed,
        ))
    }

    fn put(&mut self, bytes: &[u8]) -> Result<u32> {
        if bytes.is_empty() {
            return Ok(0);
        }
        let length = u32::try_from(bytes.len()).context("guest input larger than u32")?;
        let pointer = from_wasmtime(self.alloc.call(&mut self.store, length))?;
        self.memory
            .write(&mut self.store, pointer as usize, bytes)
            .context("copy input into guest memory")?;
        Ok(pointer)
    }

    fn free(&mut self, pointer: u32, length: usize) -> Result<()> {
        if length == 0 {
            return Ok(());
        }
        from_wasmtime(self.dealloc.call(&mut self.store, (pointer, length as u32)))?;
        Ok(())
    }

    fn result(&mut self) -> Result<Vec<u8>> {
        let pointer = from_wasmtime(self.result_pointer.call(&mut self.store, ()))?;
        let length = from_wasmtime(self.result_length.call(&mut self.store, ()))? as usize;
        let mut result = vec![0u8; length];
        self.memory
            .read(&self.store, pointer as usize, &mut result)
            .context("copy result out of guest memory")?;
        Ok(result)
    }

    fn stateless_call(&mut self, old: &[u8], new: &[u8]) -> Result<Vec<u8>> {
        let old_pointer = self.put(old)?;
        let new_pointer = self.put(new)?;
        let status = from_wasmtime(self.stateless.call(
            &mut self.store,
            (old_pointer, old.len() as u32, new_pointer, new.len() as u32),
        ))?;
        if status != 0 {
            bail!("stateless guest returned status {status}");
        }
        let result = self.result()?;
        self.free(new_pointer, new.len())?;
        self.free(old_pointer, old.len())?;
        Ok(result)
    }

    fn hydrate_call(&mut self, input: &[u8]) -> Result<()> {
        let pointer = self.put(input)?;
        let status = from_wasmtime(
            self.hydrate
                .call(&mut self.store, (pointer, input.len() as u32)),
        )?;
        if status != 0 {
            bail!("hydrate guest returned status {status}");
        }
        self.free(pointer, input.len())?;
        Ok(())
    }

    fn splice_call(&mut self, splice: &Splice<'_>) -> Result<ReturnedPatch> {
        let insert_pointer = self.put(splice.insert)?;
        let status = from_wasmtime(self.apply_splice.call(
            &mut self.store,
            (
                splice.offset as u32,
                splice.delete_length as u32,
                insert_pointer,
                splice.insert.len() as u32,
            ),
        ))?;
        if status != 0 {
            bail!("apply_splice guest returned status {status}");
        }
        let result = self.result()?;
        self.free(insert_pointer, splice.insert.len())?;
        ReturnedPatch::parse(&result)
    }

    fn changes(&mut self) -> Result<(u64, u64)> {
        Ok((
            from_wasmtime(self.changed_rows.call(&mut self.store, ()))?,
            from_wasmtime(self.changed_cells.call(&mut self.store, ()))?,
        ))
    }

    fn linear_memory_bytes(&self) -> usize {
        self.memory.data_size(&self.store)
    }

    fn logical_bytes(&mut self) -> Result<u64> {
        from_wasmtime(self.logical_document_bytes.call(&mut self.store, ()))
    }
}

#[derive(Debug)]
struct Dataset {
    base: Vec<u8>,
    edited: Vec<u8>,
    rows: usize,
    edit_offset: usize,
}

fn make_dataset(target_bytes: usize) -> Dataset {
    let mut csv = String::with_capacity(target_bytes + 256);
    csv.push_str("id,user,company,note,state,score\n");
    let mut row_ranges = Vec::new();
    let mut row = 0usize;
    while csv.len() < target_bytes {
        let start = csv.len();
        writeln!(
            &mut csv,
            "{row:08},user_{row:08},company_{company:04},\"memo, \"\"quoted\"\" row {row:08}\",ACTIVE_A,{score:08}",
            company = row % 10_000,
            score = row.wrapping_mul(7919) % 100_000_000,
        )
        .unwrap();
        row_ranges.push(start..csv.len());
        row += 1;
    }

    let base = csv.into_bytes();
    let chosen = &row_ranges[row_ranges.len() / 2];
    let marker_offset = base[chosen.clone()]
        .windows(b"ACTIVE_A".len())
        .position(|window| window == b"ACTIVE_A")
        .expect("generated edit marker")
        + chosen.start;
    let edit_offset = marker_offset + b"ACTIVE_".len();
    let mut edited = base.clone();
    edited[edit_offset] = b'B';
    Dataset {
        base,
        edited,
        rows: row_ranges.len() + 1,
        edit_offset,
    }
}

#[derive(Debug)]
struct Splice<'a> {
    offset: usize,
    delete_length: usize,
    insert: &'a [u8],
}

fn common_splice<'a>(old: &[u8], new: &'a [u8]) -> Splice<'a> {
    let prefix = old
        .iter()
        .zip(new)
        .take_while(|(left, right)| left == right)
        .count();
    let maximum_suffix = old.len().min(new.len()) - prefix;
    let suffix = old[old.len() - maximum_suffix..]
        .iter()
        .rev()
        .zip(new[new.len() - maximum_suffix..].iter().rev())
        .take_while(|(left, right)| left == right)
        .count();
    Splice {
        offset: prefix,
        delete_length: old.len() - prefix - suffix,
        insert: &new[prefix..new.len() - suffix],
    }
}

#[derive(Debug)]
struct ReturnedPatch {
    offset: usize,
    delete_length: usize,
    insert: Vec<u8>,
    row_index: usize,
    changed_cells: usize,
}

impl ReturnedPatch {
    fn parse(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < PATCH_HEADER_LEN {
            bail!("short guest patch: {} bytes", bytes.len());
        }
        let word = |index: usize| {
            u64::from_le_bytes(
                bytes[index * 8..index * 8 + 8]
                    .try_into()
                    .expect("eight-byte patch word"),
            )
        };
        if word(0) != PATCH_MAGIC {
            bail!("invalid patch magic");
        }
        let insert_length = word(3) as usize;
        if bytes.len() != PATCH_HEADER_LEN + insert_length {
            bail!("patch payload length mismatch");
        }
        Ok(Self {
            offset: word(1) as usize,
            delete_length: word(2) as usize,
            insert: bytes[PATCH_HEADER_LEN..].to_vec(),
            row_index: word(4) as usize,
            changed_cells: word(7) as usize,
        })
    }
}

fn materialize(old: &[u8], patch: &ReturnedPatch) -> Result<Vec<u8>> {
    let delete_end = patch
        .offset
        .checked_add(patch.delete_length)
        .context("patch overflow")?;
    if delete_end > old.len() {
        bail!("patch outside old document");
    }
    let mut output = Vec::with_capacity(old.len() - patch.delete_length + patch.insert.len());
    output.extend_from_slice(&old[..patch.offset]);
    output.extend_from_slice(&patch.insert);
    output.extend_from_slice(&old[delete_end..]);
    Ok(output)
}

#[derive(Debug)]
struct Distribution {
    count: usize,
    mean_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    min_ms: f64,
    max_ms: f64,
}

impl Distribution {
    fn from_durations(durations: &[Duration]) -> Self {
        assert!(!durations.is_empty());
        let mut values = durations
            .iter()
            .map(|duration| duration.as_secs_f64() * 1000.0)
            .collect::<Vec<_>>();
        values.sort_by(f64::total_cmp);
        let percentile = |fraction: f64| {
            let rank = ((values.len() as f64 * fraction).ceil() as usize)
                .saturating_sub(1)
                .min(values.len() - 1);
            values[rank]
        };
        Self {
            count: values.len(),
            mean_ms: values.iter().sum::<f64>() / values.len() as f64,
            p50_ms: percentile(0.50),
            p95_ms: percentile(0.95),
            min_ms: values[0],
            max_ms: *values.last().unwrap(),
        }
    }
}

fn print_distribution(label: &str, distribution: &Distribution) {
    println!(
        "METRIC name={label} n={} mean_ms={:.6} p50_ms={:.6} p95_ms={:.6} min_ms={:.6} max_ms={:.6}",
        distribution.count,
        distribution.mean_ms,
        distribution.p50_ms,
        distribution.p95_ms,
        distribution.min_ms,
        distribution.max_ms,
    );
}

fn bench_stateless(
    engine: &Engine,
    module: &Module,
    dataset: &Dataset,
    iterations: usize,
    memory_limit: usize,
) -> Result<()> {
    let (mut guest, instantiate) = Guest::instantiate(engine, module, memory_limit)?;
    let initial_memory = guest.linear_memory_bytes();

    for iteration in 0..4 {
        let (old, new) = if iteration % 2 == 0 {
            (&dataset.base, &dataset.edited)
        } else {
            (&dataset.edited, &dataset.base)
        };
        let output = guest.stateless_call(old, new)?;
        if output != *new {
            bail!("stateless warmup output mismatch");
        }
    }

    let mut calls = Vec::with_capacity(iterations);
    for iteration in 0..iterations {
        let (old, new) = if iteration % 2 == 0 {
            (&dataset.base, &dataset.edited)
        } else {
            (&dataset.edited, &dataset.base)
        };
        let started = Instant::now();
        let output = guest.stateless_call(old, new)?;
        calls.push(started.elapsed());
        if output != *new {
            bail!("stateless output mismatch at iteration {iteration}");
        }
        if guest.changes()? != (1, 1) {
            bail!("stateless semantic identification was not one row/one cell");
        }
    }

    println!(
        "ARCH architecture=stateless instantiate_ms={:.6} initial_linear_bytes={} peak_linear_bytes={} logical_retained_bytes={} bytes_in_per_edit={} bytes_out_per_edit={}",
        instantiate.as_secs_f64() * 1000.0,
        initial_memory,
        guest.linear_memory_bytes(),
        guest.logical_bytes()?,
        dataset.base.len() + dataset.edited.len(),
        dataset.edited.len(),
    );
    print_distribution(
        "stateless_wasm_boundary",
        &Distribution::from_durations(&calls),
    );
    Ok(())
}

fn bench_persistent(
    engine: &Engine,
    module: &Module,
    dataset: &Dataset,
    iterations: usize,
    memory_limit: usize,
) -> Result<()> {
    let (mut guest, instantiate) = Guest::instantiate(engine, module, memory_limit)?;
    let initial_memory = guest.linear_memory_bytes();
    let hydrate_started = Instant::now();
    guest.hydrate_call(&dataset.base)?;
    let hydrate = hydrate_started.elapsed();
    let hydrated_memory = guest.linear_memory_bytes();
    let hydrated_logical = guest.logical_bytes()?;

    let mut current_is_base = true;
    for _ in 0..6 {
        let (old, new) = if current_is_base {
            (&dataset.base, &dataset.edited)
        } else {
            (&dataset.edited, &dataset.base)
        };
        let splice = common_splice(old, new);
        let patch = guest.splice_call(&splice)?;
        let output = materialize(old, &patch)?;
        if output != *new {
            bail!("persistent warmup output mismatch");
        }
        current_is_base = !current_is_base;
    }

    let mut diff_times = Vec::with_capacity(iterations);
    let mut wasm_times = Vec::with_capacity(iterations);
    let mut materialize_times = Vec::with_capacity(iterations);
    let mut total_times = Vec::with_capacity(iterations);
    let mut bytes_in = 0usize;
    let mut bytes_out = 0usize;
    let mut patch_length = None;
    for iteration in 0..iterations {
        let (old, new) = if current_is_base {
            (&dataset.base, &dataset.edited)
        } else {
            (&dataset.edited, &dataset.base)
        };
        let total_started = Instant::now();
        let diff_started = Instant::now();
        let splice = common_splice(old, new);
        diff_times.push(diff_started.elapsed());
        if splice.offset != dataset.edit_offset
            || splice.delete_length != 1
            || splice.insert.len() != 1
        {
            bail!("unexpected one-byte splice: {splice:?}");
        }
        let wasm_started = Instant::now();
        let patch = guest.splice_call(&splice)?;
        wasm_times.push(wasm_started.elapsed());
        let materialize_started = Instant::now();
        let output = materialize(old, &patch)?;
        materialize_times.push(materialize_started.elapsed());
        total_times.push(total_started.elapsed());

        if output != *new {
            bail!("persistent output mismatch at iteration {iteration}");
        }
        if patch.changed_cells != 1 || guest.changes()? != (1, 1) {
            bail!("persistent semantic identification was not one row/one cell");
        }
        if patch.row_index == 0 || patch.row_index >= dataset.rows {
            bail!("guest reported invalid changed row index");
        }
        patch_length = Some(PATCH_HEADER_LEN + patch.insert.len());
        bytes_in += splice.insert.len();
        bytes_out += PATCH_HEADER_LEN + patch.insert.len();
        current_is_base = !current_is_base;
    }

    println!(
        "ARCH architecture=persistent instantiate_ms={:.6} initial_linear_bytes={} hydrate_ms={:.6} hydrated_linear_bytes={} peak_linear_bytes={} hydrated_logical_bytes={} final_logical_bytes={} hydrate_bytes_in={} bytes_in_per_edit={:.2} bytes_out_per_edit={:.2} patch_bytes={}",
        instantiate.as_secs_f64() * 1000.0,
        initial_memory,
        hydrate.as_secs_f64() * 1000.0,
        hydrated_memory,
        guest.linear_memory_bytes(),
        hydrated_logical,
        guest.logical_bytes()?,
        dataset.base.len(),
        bytes_in as f64 / iterations as f64,
        bytes_out as f64 / iterations as f64,
        patch_length.unwrap_or(0),
    );
    print_distribution(
        "persistent_host_prefix_suffix",
        &Distribution::from_durations(&diff_times),
    );
    print_distribution(
        "persistent_wasm_boundary",
        &Distribution::from_durations(&wasm_times),
    );
    print_distribution(
        "persistent_host_materialize",
        &Distribution::from_durations(&materialize_times),
    );
    print_distribution(
        "persistent_end_to_end",
        &Distribution::from_durations(&total_times),
    );
    Ok(())
}

fn guest_wasm_path() -> PathBuf {
    if let Some(target) = option_env!("CARGO_TARGET_DIR") {
        return PathBuf::from(target)
            .join("wasm32-unknown-unknown/release/csv_persistent_guest.wasm");
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/wasm32-unknown-unknown/release/csv_persistent_guest.wasm")
}

fn main() -> Result<()> {
    let options = Options::parse()?;
    let wasm_path = guest_wasm_path();
    let wasm = std::fs::read(&wasm_path).with_context(|| {
        format!(
            "read {}; first run `cargo build --manifest-path guest/Cargo.toml --target wasm32-unknown-unknown --release`",
            wasm_path.display()
        )
    })?;

    let mut config = Config::new();
    config.cranelift_opt_level(OptLevel::Speed);
    config.debug_info(false);
    let engine_started = Instant::now();
    let engine = from_wasmtime(Engine::new(&config))?;
    let engine_time = engine_started.elapsed();
    let compile_started = Instant::now();
    let module =
        from_wasmtime(Module::from_binary(&engine, &wasm)).context("compile core Wasm module")?;
    let compile_time = compile_started.elapsed();
    println!(
        "CONFIG wasmtime=45.0.3 wasm_bytes={} memory_limit_mib={} engine_ms={:.6} compile_ms={:.6}",
        wasm.len(),
        options.memory_mib,
        engine_time.as_secs_f64() * 1000.0,
        compile_time.as_secs_f64() * 1000.0,
    );

    for size_mib in &options.sizes_mib {
        let dataset = make_dataset(size_mib * MIB);
        let iterations = options.iterations(*size_mib);
        println!(
            "DATASET requested_mib={} bytes={} rows={} edit_offset={} iterations={}",
            size_mib,
            dataset.base.len(),
            dataset.rows,
            dataset.edit_offset,
            iterations,
        );
        for architecture in &options.architectures {
            let outcome = match architecture {
                Architecture::Stateless => bench_stateless(
                    &engine,
                    &module,
                    &dataset,
                    iterations,
                    options.memory_mib * MIB,
                ),
                Architecture::Persistent => bench_persistent(
                    &engine,
                    &module,
                    &dataset,
                    iterations,
                    options.memory_mib * MIB,
                ),
            };
            if let Err(error) = outcome {
                println!(
                    "ARCH_ERROR architecture={architecture:?} requested_mib={} error={:?}",
                    size_mib,
                    anyhow!(error)
                );
            }
        }
    }
    Ok(())
}
