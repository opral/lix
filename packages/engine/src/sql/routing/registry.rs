use std::collections::BTreeSet;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RoutingPassMetadata {
    pub(crate) name: &'static str,
    pub(crate) order: u32,
    pub(crate) description: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RoutingPassRegistry {
    pub(crate) name: &'static str,
    pub(crate) passes: &'static [RoutingPassMetadata],
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RoutingPassSettings {
    disabled_passes: BTreeSet<String>,
}

impl RoutingPassSettings {
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
pub(crate) struct RoutingPassOutcome {
    pub(crate) changed: bool,
    pub(crate) diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct RoutingPassTrace {
    pub(crate) name: &'static str,
    pub(crate) order: u32,
    pub(crate) description: &'static str,
    pub(crate) enabled: bool,
    pub(crate) changed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) duration_us: Option<u64>,
    pub(crate) diagnostics: Vec<String>,
}

pub(crate) fn run_infallible_pass<F>(
    metadata: RoutingPassMetadata,
    settings: &RoutingPassSettings,
    pass: F,
) -> RoutingPassTrace
where
    F: FnOnce() -> RoutingPassOutcome,
{
    if !settings.is_enabled(metadata.name) {
        return skipped_trace(metadata);
    }

    let started_at = Instant::now();
    let outcome = pass();
    trace_from_outcome(metadata, outcome, started_at.elapsed().as_micros())
}

pub(crate) fn run_fallible_pass<E, F>(
    metadata: RoutingPassMetadata,
    settings: &RoutingPassSettings,
    pass: F,
) -> Result<RoutingPassTrace, E>
where
    F: FnOnce() -> Result<RoutingPassOutcome, E>,
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

fn skipped_trace(metadata: RoutingPassMetadata) -> RoutingPassTrace {
    RoutingPassTrace {
        name: metadata.name,
        order: metadata.order,
        description: metadata.description,
        enabled: false,
        changed: false,
        duration_us: None,
        diagnostics: vec!["pass disabled".to_string()],
    }
}

fn trace_from_outcome(
    metadata: RoutingPassMetadata,
    outcome: RoutingPassOutcome,
    duration_us: u128,
) -> RoutingPassTrace {
    RoutingPassTrace {
        name: metadata.name,
        order: metadata.order,
        description: metadata.description,
        enabled: true,
        changed: outcome.changed,
        duration_us: Some(duration_us.min(u128::from(u64::MAX)) as u64),
        diagnostics: outcome.diagnostics,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        run_infallible_pass, RoutingPassMetadata, RoutingPassOutcome, RoutingPassSettings,
    };

    #[test]
    fn disabled_passes_emit_skipped_traces() {
        let trace = run_infallible_pass(
            RoutingPassMetadata {
                name: "public-read.test",
                order: 10,
                description: "test pass",
            },
            &RoutingPassSettings::with_disabled_passes(["public-read.test"]),
            || RoutingPassOutcome {
                changed: true,
                diagnostics: vec!["should not run".to_string()],
            },
        );

        assert!(!trace.enabled);
        assert!(!trace.changed);
        assert_eq!(trace.duration_us, None);
        assert_eq!(trace.diagnostics, vec!["pass disabled".to_string()]);
    }
}
