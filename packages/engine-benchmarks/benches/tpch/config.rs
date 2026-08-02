use std::time::Duration;

#[derive(Clone, Copy, Debug)]
pub(crate) struct Config {
    pub(crate) scale_factor: f64,
    pub(crate) samples: usize,
}

impl Config {
    pub(crate) fn from_env() -> Self {
        let scale_factor = env_parse("LIX_TPCH_SCALE_FACTOR", 0.01_f64);
        let samples = env_parse("LIX_TPCH_SAMPLES", 5_usize);
        assert!(
            scale_factor.is_finite() && scale_factor >= 0.001,
            "LIX_TPCH_SCALE_FACTOR must be finite and at least 0.001"
        );
        assert!(samples > 0, "LIX_TPCH_SAMPLES must be positive");
        Self {
            scale_factor,
            samples,
        }
    }
}

fn env_parse<T>(name: &str, default: T) -> T
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    std::env::var(name).map_or(default, |value| {
        value
            .parse()
            .unwrap_or_else(|error| panic!("invalid {name}={value:?}: {error}"))
    })
}

pub(crate) fn median(samples: &[Duration]) -> Duration {
    percentile(samples, 0.5)
}

pub(crate) fn p90(samples: &[Duration]) -> Duration {
    percentile(samples, 0.9)
}

pub(crate) fn median_absolute_deviation(samples: &[Duration]) -> Duration {
    let median = median(samples);
    let deviations = samples
        .iter()
        .map(|sample| sample.abs_diff(median))
        .collect::<Vec<_>>();
    self::median(&deviations)
}

fn percentile(samples: &[Duration], percentile: f64) -> Duration {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let index = ((sorted.len() - 1) as f64 * percentile).ceil() as usize;
    sorted[index]
}
