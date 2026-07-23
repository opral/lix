use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use std::env;
use std::path::Path;
use std::time::{Duration, Instant};
use wasmtime::component::{Component, Linker, ResourceTable, StreamReader};
use wasmtime::{Config, Engine, ResourceLimiter, Store};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

mod bindings {
    wasmtime::component::bindgen!({
        path: "../wit",
        world: "guest",
        exports: {
            "lix:p3-stream-probe/transfer.consume-list": async,
        },
    });
}

use bindings::Guest;

const SIZES: [usize; 2] = [1024 * 1024, 10 * 1024 * 1024];
const WARMUPS: usize = 8;
const SAMPLES: usize = 40;

#[derive(Clone, Copy, Debug)]
enum Transport {
    List,
    StreamVec64K,
    StreamBytes8K,
    StreamBytes64K,
    StreamBytes1M,
}

enum PreparedInput {
    List(Vec<u8>),
    StreamVec(Vec<u8>),
    StreamBytes(Bytes),
}

impl Transport {
    const ALL: [Self; 5] = [
        Self::List,
        Self::StreamVec64K,
        Self::StreamBytes8K,
        Self::StreamBytes64K,
        Self::StreamBytes1M,
    ];

    const fn label(self) -> &'static str {
        match self {
            Self::List => "list-u8",
            Self::StreamVec64K => "stream-u8-vec-64k",
            Self::StreamBytes8K => "stream-u8-bytes-8k",
            Self::StreamBytes64K => "stream-u8-bytes-64k",
            Self::StreamBytes1M => "stream-u8-bytes-1m",
        }
    }

    const fn chunk_bytes(self) -> u32 {
        match self {
            Self::List => 0,
            Self::StreamBytes8K => 8 * 1024,
            Self::StreamVec64K | Self::StreamBytes64K => 64 * 1024,
            Self::StreamBytes1M => 1024 * 1024,
        }
    }
}

#[derive(Default)]
struct State {
    ctx: WasiCtx,
    table: ResourceTable,
    peak_guest_linear_bytes: usize,
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

struct Case {
    guest: Guest,
    store: Store<State>,
}

impl Case {
    async fn new(engine: &Engine, component: &Component, linker: &Linker<State>) -> Result<Self> {
        let mut store = Store::new(engine, State::default());
        store.limiter(|state| state);
        let guest = Guest::instantiate_async(&mut store, component, linker)
            .await
            .map_err(|error| anyhow!("instantiate probe component: {error:?}"))?;
        Ok(Self { guest, store })
    }

    async fn call(
        &mut self,
        transport: Transport,
        input: PreparedInput,
        checksum: bool,
        expected: (u64, u64),
    ) -> Result<(u64, u64)> {
        let result = match (transport, input) {
            (Transport::List, PreparedInput::List(input)) => {
                self.guest
                    .lix_p3_stream_probe_transfer()
                    .call_consume_list(&mut self.store, &input, checksum)
                    .await?
            }
            (Transport::StreamVec64K, PreparedInput::StreamVec(input)) => {
                let stream = StreamReader::new(&mut self.store, input)?;
                self.store
                    .run_concurrent(async |accessor| {
                        self.guest
                            .lix_p3_stream_probe_transfer()
                            .call_consume_stream(
                                accessor,
                                stream,
                                checksum,
                                transport.chunk_bytes(),
                            )
                            .await
                    })
                    .await??
            }
            (
                Transport::StreamBytes8K | Transport::StreamBytes64K | Transport::StreamBytes1M,
                PreparedInput::StreamBytes(input),
            ) => {
                let stream = StreamReader::new(&mut self.store, input)?;
                self.store
                    .run_concurrent(async |accessor| {
                        self.guest
                            .lix_p3_stream_probe_transfer()
                            .call_consume_stream(
                                accessor,
                                stream,
                                checksum,
                                transport.chunk_bytes(),
                            )
                            .await
                    })
                    .await??
            }
            _ => unreachable!("prepared input must match transport"),
        };

        if result != expected {
            bail!(
                "incorrect guest result for {}: got {result:?}, expected {expected:?}",
                transport.label()
            );
        }
        Ok(result)
    }
}

#[derive(Debug)]
struct Summary {
    p50: Duration,
    p95: Duration,
    min: Duration,
    max: Duration,
}

fn summarize(mut samples: Vec<Duration>) -> Summary {
    samples.sort_unstable();
    let p50 = samples[(samples.len() - 1) * 50 / 100];
    let p95 = samples[(samples.len() - 1) * 95 / 100];
    Summary {
        p50,
        p95,
        min: samples[0],
        max: *samples.last().expect("samples is non-empty"),
    }
}

fn fixture(size: usize) -> Bytes {
    let mut data = Vec::with_capacity(size);
    for index in 0..size {
        data.push((index.wrapping_mul(131).wrapping_add(17) & 0xff) as u8);
    }
    Bytes::from(data)
}

fn prepare_input(transport: Transport, input: &Bytes) -> PreparedInput {
    match transport {
        Transport::List => PreparedInput::List(input.to_vec()),
        Transport::StreamVec64K => PreparedInput::StreamVec(input.to_vec()),
        Transport::StreamBytes8K | Transport::StreamBytes64K | Transport::StreamBytes1M => {
            PreparedInput::StreamBytes(input.clone())
        }
    }
}

async fn run_case(
    engine: &Engine,
    component: &Component,
    linker: &Linker<State>,
    size: usize,
    transport: Transport,
    checksum: bool,
) -> Result<()> {
    let input = fixture(size);
    let expected = (
        input.len() as u64,
        if checksum {
            input
                .iter()
                .fold(0_u64, |sum, byte| sum.wrapping_add(u64::from(*byte)))
        } else {
            0
        },
    );
    let mut case = Case::new(engine, component, linker).await?;

    for _ in 0..WARMUPS {
        let prepared = prepare_input(transport, &input);
        case.call(transport, prepared, checksum, expected).await?;
    }

    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        // Input ownership setup is outside the timer for both list and stream.
        // The timer measures canonical transfer, guest draining, and optional scan.
        let prepared = prepare_input(transport, &input);
        let start = Instant::now();
        case.call(transport, prepared, checksum, expected).await?;
        samples.push(start.elapsed());
    }

    let summary = summarize(samples);
    println!(
        "result\tbytes={size}\twork={}\ttransport={}\tp50_ms={:.3}\tp95_ms={:.3}\tmin_ms={:.3}\tmax_ms={:.3}\tguest_peak_mib={:.3}",
        if checksum { "checksum" } else { "count" },
        transport.label(),
        summary.p50.as_secs_f64() * 1000.0,
        summary.p95.as_secs_f64() * 1000.0,
        summary.min.as_secs_f64() * 1000.0,
        summary.max.as_secs_f64() * 1000.0,
        case.store.data().peak_guest_linear_bytes as f64 / (1024.0 * 1024.0),
    );
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let component_path = env::args()
        .nth(1)
        .context("usage: p3-stream-probe-host <guest-component.wasm>")?;
    if !Path::new(&component_path).exists() {
        bail!("guest component does not exist: {component_path}");
    }

    let mut config = Config::new();
    config.wasm_component_model_async(true);
    let engine = Engine::new(&config)?;
    let component = Component::from_file(&engine, &component_path)
        .map_err(|error| anyhow!("compile component {component_path}: {error:?}"))?;

    let mut linker = Linker::new(&engine);
    wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
    wasmtime_wasi::p3::add_to_linker(&mut linker)?;

    println!(
        "config\twasmtime=47.0.2\twarmups={WARMUPS}\tsamples={SAMPLES}\tguest_component_bytes={}",
        std::fs::metadata(&component_path)?.len()
    );

    for size in SIZES {
        for checksum in [false, true] {
            for transport in Transport::ALL {
                run_case(&engine, &component, &linker, size, transport, checksum).await?;
            }
        }
    }

    Ok(())
}
