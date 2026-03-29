use std::collections::BTreeSet;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OptimizerPassMetadata {
    pub(crate) name: &'static str,
    pub(crate) order: u32,
    pub(crate) description: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OptimizerPassRegistry {
    pub(crate) name: &'static str,
    pub(crate) passes: &'static [OptimizerPassMetadata],
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct OptimizerPassSettings {
    disabled_passes: BTreeSet<String>,
}

impl OptimizerPassSettings {
    pub(crate) fn is_enabled(&self, pass_name: &str) -> bool {
        !self.disabled_passes.contains(pass_name)
    }

    #[cfg(test)]
    pub(crate) fn with_disabled_passes<I, S>(pass_names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            disabled_passes: pass_names.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct OptimizerPassOutcome {
    pub(crate) changed: bool,
    pub(crate) diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OptimizerPassTrace {
    pub(crate) name: &'static str,
    pub(crate) order: u32,
    pub(crate) description: &'static str,
    pub(crate) enabled: bool,
    pub(crate) changed: bool,
    pub(crate) duration_us: u64,
    pub(crate) diagnostics: Vec<String>,
}

pub(crate) fn run_infallible_pass<F>(
    metadata: OptimizerPassMetadata,
    settings: &OptimizerPassSettings,
    pass: F,
) -> OptimizerPassTrace
where
    F: FnOnce() -> OptimizerPassOutcome,
{
    if !settings.is_enabled(metadata.name) {
        return skipped_trace(metadata);
    }

    let started_at = Instant::now();
    let outcome = pass();
    trace_from_outcome(metadata, outcome, started_at.elapsed().as_micros())
}

pub(crate) fn run_fallible_pass<E, F>(
    metadata: OptimizerPassMetadata,
    settings: &OptimizerPassSettings,
    pass: F,
) -> Result<OptimizerPassTrace, E>
where
    F: FnOnce() -> Result<OptimizerPassOutcome, E>,
{
    if !settings.is_enabled(metadata.name) {
        return Ok(skipped_trace(metadata));
    }

    let started_at = Instant::now();
    let outcome = pass()?;
    Ok(trace_from_outcome(
        metadata,
        outcome,
        started_at.elapsed().as_micros(),
    ))
}

fn skipped_trace(metadata: OptimizerPassMetadata) -> OptimizerPassTrace {
    OptimizerPassTrace {
        name: metadata.name,
        order: metadata.order,
        description: metadata.description,
        enabled: false,
        changed: false,
        duration_us: 0,
        diagnostics: vec!["pass disabled".to_string()],
    }
}

fn trace_from_outcome(
    metadata: OptimizerPassMetadata,
    outcome: OptimizerPassOutcome,
    duration_us: u128,
) -> OptimizerPassTrace {
    OptimizerPassTrace {
        name: metadata.name,
        order: metadata.order,
        description: metadata.description,
        enabled: true,
        changed: outcome.changed,
        duration_us: duration_us.min(u128::from(u64::MAX)) as u64,
        diagnostics: outcome.diagnostics,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        run_infallible_pass, OptimizerPassMetadata, OptimizerPassOutcome, OptimizerPassSettings,
    };

    #[test]
    fn disabled_passes_emit_skipped_traces() {
        let trace = run_infallible_pass(
            OptimizerPassMetadata {
                name: "public-read.test",
                order: 10,
                description: "test pass",
            },
            &OptimizerPassSettings::with_disabled_passes(["public-read.test"]),
            || OptimizerPassOutcome {
                changed: true,
                diagnostics: vec!["should not run".to_string()],
            },
        );

        assert!(!trace.enabled);
        assert!(!trace.changed);
        assert_eq!(trace.diagnostics, vec!["pass disabled".to_string()]);
    }
}
