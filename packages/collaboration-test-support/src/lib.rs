//! Private, transport-neutral real-time collaboration benchmark driver.
//!
//! Local and protocol clients implement the same phase adapter so workload
//! semantics, arrival scheduling, percentile calculation, and gates cannot
//! drift between benchmark surfaces.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::Serialize;

#[derive(Debug, Clone, Copy)]
pub struct CapacityConfig {
    pub clients: usize,
    pub operations: usize,
    pub wave_size: usize,
    pub conflict_wave_interval: usize,
    pub arrival_interval: Duration,
    pub convergence_gate: Duration,
}

impl CapacityConfig {
    pub fn validate(self) {
        assert!(self.clients > 0, "capacity workload needs clients");
        assert!(self.wave_size > 0, "capacity workload needs a wave size");
        assert!(
            self.operations >= self.wave_size && self.operations.is_multiple_of(self.wave_size),
            "operations must contain complete waves"
        );
        assert!(
            self.conflict_wave_interval > 0,
            "conflict wave interval must be non-zero"
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaveEdit {
    pub operation: usize,
    pub slot: usize,
    pub token: String,
    pub intentionally_overlapping: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WavePlan {
    pub index: usize,
    pub edits: Vec<WaveEdit>,
    pub marker: String,
}

impl WavePlan {
    pub fn expected_tokens(&self) -> impl Iterator<Item = &str> {
        self.edits
            .iter()
            .filter(|edit| !edit.intentionally_overlapping)
            .map(|edit| edit.token.as_str())
    }
}

pub fn wave_plans(config: CapacityConfig) -> Vec<WavePlan> {
    config.validate();
    (0..config.operations / config.wave_size)
        .map(|wave| {
            let edits = (0..config.wave_size)
                .map(|participant| {
                    let operation = wave * config.wave_size + participant;
                    let intentionally_overlapping =
                        wave.is_multiple_of(config.conflict_wave_interval) && participant < 2;
                    WaveEdit {
                        operation,
                        slot: if intentionally_overlapping {
                            0
                        } else {
                            operation + 1
                        },
                        token: format!("wave-{wave}-client-{operation}"),
                        intentionally_overlapping,
                    }
                })
                .collect::<Vec<_>>();
            let marker = edits
                .last()
                .expect("validated wave is non-empty")
                .token
                .clone();
            WavePlan {
                index: wave,
                edits,
                marker,
            }
        })
        .collect()
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct CapacityResourceCounters {
    pub waves_staged: u64,
    pub transactions_staged: u64,
    pub transactions_committed: u64,
    pub convergence_deliveries: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct CapacityReport {
    pub schema: &'static str,
    pub backend: String,
    pub format: String,
    pub clients: usize,
    pub operations: usize,
    pub wave_size: usize,
    pub arrival_ms: f64,
    pub overlap_percent: f64,
    pub service_p50_ms: f64,
    pub service_p95_ms: f64,
    pub convergence_p50_ms: f64,
    pub convergence_p95_ms: f64,
    pub convergence_p99_ms: f64,
    pub wave_p95_ms: f64,
    pub schedule_lag_p95_ms: f64,
    pub total_ms: f64,
    pub resources: CapacityResourceCounters,
    pub backend_resources: BTreeMap<String, u64>,
}

impl CapacityReport {
    pub fn emit_json(&self) {
        println!(
            "{}",
            serde_json::to_string(self).expect("capacity report should serialize")
        );
    }
}

#[async_trait(?Send)]
pub trait CollaborationCapacityBackend {
    type StagedWave;

    fn backend_name(&self) -> &'static str;

    fn format_name(&self) -> &'static str;

    async fn read_base(&self) -> Vec<u8>;

    async fn stage_wave(&mut self, wave: &WavePlan, base: &[u8]) -> Self::StagedWave;

    async fn commit_wave(&mut self, staged: Self::StagedWave) -> Vec<Duration>;

    async fn await_convergence(&mut self, marker: &[u8], wave_started: Instant) -> Vec<Duration>;

    async fn assert_final_state(&self, expected_tokens: &[String]);

    fn resource_counters(&self) -> BTreeMap<String, u64> {
        BTreeMap::new()
    }
}

pub async fn run_capacity_workload<B>(backend: &mut B, config: CapacityConfig) -> CapacityReport
where
    B: CollaborationCapacityBackend,
{
    let plans = wave_plans(config);
    let overlap_count = plans
        .iter()
        .flat_map(|wave| &wave.edits)
        .filter(|edit| edit.intentionally_overlapping)
        .count();
    let expected_tokens = plans
        .iter()
        .flat_map(WavePlan::expected_tokens)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let mut service_latencies = Vec::with_capacity(config.operations);
    let mut convergence_latencies =
        Vec::with_capacity(config.clients * config.operations / config.wave_size);
    let mut wave_latencies = Vec::with_capacity(plans.len());
    let mut schedule_lags = Vec::with_capacity(plans.len());
    let mut resources = CapacityResourceCounters::default();
    let run_started = Instant::now();
    let schedule_origin = run_started + config.arrival_interval;

    for wave in &plans {
        let base = backend.read_base().await;
        let staged = backend.stage_wave(wave, &base).await;
        resources.waves_staged += 1;
        resources.transactions_staged += wave.edits.len() as u64;

        let wave_started = schedule_origin
            + config
                .arrival_interval
                .saturating_mul(u32::try_from(wave.index).expect("wave index fits u32"));
        tokio::time::sleep_until(tokio::time::Instant::from_std(wave_started)).await;
        schedule_lags.push(wave_started.elapsed());

        let services = backend.commit_wave(staged).await;
        assert_eq!(
            services.len(),
            wave.edits.len(),
            "backend must report one service sample per commit"
        );
        resources.transactions_committed += services.len() as u64;
        service_latencies.extend(services);

        let convergences = backend
            .await_convergence(wave.marker.as_bytes(), wave_started)
            .await;
        assert_eq!(
            convergences.len(),
            config.clients,
            "backend must report one convergence sample per client"
        );
        resources.convergence_deliveries += convergences.len() as u64;
        wave_latencies.push(
            convergences
                .iter()
                .copied()
                .max()
                .expect("capacity workload has clients"),
        );
        convergence_latencies.extend(convergences);
    }

    backend.assert_final_state(&expected_tokens).await;
    service_latencies.sort_unstable();
    convergence_latencies.sort_unstable();
    wave_latencies.sort_unstable();
    schedule_lags.sort_unstable();
    let convergence_p95 = percentile(&convergence_latencies, 95);
    assert!(
        convergence_p95 < config.convergence_gate,
        "{} {}-client commit-to-convergence p95 {:.3} ms exceeded {:.3} ms",
        backend.backend_name(),
        config.clients,
        millis(convergence_p95),
        millis(config.convergence_gate),
    );

    CapacityReport {
        schema: "lix.collaboration-capacity.v1",
        backend: backend.backend_name().to_owned(),
        format: backend.format_name().to_owned(),
        clients: config.clients,
        operations: config.operations,
        wave_size: config.wave_size,
        arrival_ms: millis(config.arrival_interval),
        overlap_percent: overlap_count as f64 / config.operations as f64 * 100.0,
        service_p50_ms: millis(percentile(&service_latencies, 50)),
        service_p95_ms: millis(percentile(&service_latencies, 95)),
        convergence_p50_ms: millis(percentile(&convergence_latencies, 50)),
        convergence_p95_ms: millis(convergence_p95),
        convergence_p99_ms: millis(percentile(&convergence_latencies, 99)),
        wave_p95_ms: millis(percentile(&wave_latencies, 95)),
        schedule_lag_p95_ms: millis(percentile(&schedule_lags, 95)),
        total_ms: millis(run_started.elapsed()),
        resources,
        backend_resources: backend.resource_counters(),
    }
}

fn percentile(samples: &[Duration], percentile: usize) -> Duration {
    assert!(!samples.is_empty());
    samples[(samples.len() * percentile).div_ceil(100).saturating_sub(1)]
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_workload_has_exactly_ten_percent_overlap() {
        let config = CapacityConfig {
            clients: 100,
            operations: 100,
            wave_size: 5,
            conflict_wave_interval: 4,
            arrival_interval: Duration::from_millis(50),
            convergence_gate: Duration::from_millis(100),
        };
        let plans = wave_plans(config);
        assert_eq!(plans.len(), 20);
        assert_eq!(
            plans
                .iter()
                .flat_map(|wave| &wave.edits)
                .filter(|edit| edit.intentionally_overlapping)
                .count(),
            10
        );
        assert_eq!(plans.iter().flat_map(WavePlan::expected_tokens).count(), 90);
    }
}
