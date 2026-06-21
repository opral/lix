use std::sync::OnceLock;

const FASTCDC_MIN_CHUNK_BYTES: usize = 16 * 1024;
const FASTCDC_AVG_CHUNK_BYTES: usize = 64 * 1024;
const FASTCDC_MAX_CHUNK_BYTES: usize = 256 * 1024;
const SINGLE_CHUNK_FAST_PATH_MAX_BYTES: usize = 64 * 1024;

// Experiment-only runtime overrides for filesystem backend benchmarking.
const FASTCDC_MIN_CHUNK_BYTES_ENV: &str = "LIX_EXPERIMENT_FASTCDC_MIN_BYTES";
const FASTCDC_AVG_CHUNK_BYTES_ENV: &str = "LIX_EXPERIMENT_FASTCDC_AVG_BYTES";
const FASTCDC_MAX_CHUNK_BYTES_ENV: &str = "LIX_EXPERIMENT_FASTCDC_MAX_BYTES";
const SINGLE_CHUNK_FAST_PATH_MAX_BYTES_ENV: &str = "LIX_EXPERIMENT_FASTCDC_SINGLE_BYTES";

static FASTCDC_CONFIG: OnceLock<FastCdcConfig> = OnceLock::new();

#[derive(Clone, Copy)]
struct FastCdcConfig {
    min_chunk_bytes: usize,
    avg_chunk_bytes: usize,
    max_chunk_bytes: usize,
    single_chunk_fast_path_max_bytes: usize,
}

#[expect(clippy::cast_possible_truncation)]
pub(crate) fn fastcdc_chunk_ranges(data: &[u8]) -> Vec<(usize, usize)> {
    if data.is_empty() {
        return Vec::new();
    }
    let config = fastcdc_config();
    if data.len() <= config.single_chunk_fast_path_max_bytes {
        return vec![(0, data.len())];
    }

    fastcdc::v2020::FastCDC::new(
        data,
        config.min_chunk_bytes as u32,
        config.avg_chunk_bytes as u32,
        config.max_chunk_bytes as u32,
    )
    .map(|chunk| {
        let start = chunk.offset;
        let end = start + chunk.length;
        (start, end)
    })
    .collect()
}

fn fastcdc_config() -> FastCdcConfig {
    *FASTCDC_CONFIG.get_or_init(load_fastcdc_config)
}

fn load_fastcdc_config() -> FastCdcConfig {
    let config = FastCdcConfig {
        min_chunk_bytes: env_usize(FASTCDC_MIN_CHUNK_BYTES_ENV).unwrap_or(FASTCDC_MIN_CHUNK_BYTES),
        avg_chunk_bytes: env_usize(FASTCDC_AVG_CHUNK_BYTES_ENV).unwrap_or(FASTCDC_AVG_CHUNK_BYTES),
        max_chunk_bytes: env_usize(FASTCDC_MAX_CHUNK_BYTES_ENV).unwrap_or(FASTCDC_MAX_CHUNK_BYTES),
        single_chunk_fast_path_max_bytes: env_usize(SINGLE_CHUNK_FAST_PATH_MAX_BYTES_ENV)
            .unwrap_or(SINGLE_CHUNK_FAST_PATH_MAX_BYTES),
    };
    assert!(
        config.min_chunk_bytes > 0
            && config.min_chunk_bytes <= config.avg_chunk_bytes
            && config.avg_chunk_bytes <= config.max_chunk_bytes
            && config.max_chunk_bytes <= u32::MAX as usize,
        "invalid FastCDC experiment chunk sizes: min={}, avg={}, max={}",
        config.min_chunk_bytes,
        config.avg_chunk_bytes,
        config.max_chunk_bytes
    );
    assert!(
        config.single_chunk_fast_path_max_bytes > 0
            && config.single_chunk_fast_path_max_bytes <= u32::MAX as usize,
        "invalid FastCDC experiment single-chunk fast path size: {}",
        config.single_chunk_fast_path_max_bytes
    );
    config
}

#[cfg(not(target_family = "wasm"))]
fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok().map(|value| {
        parse_size(&value).unwrap_or_else(|| panic!("{name} must be a positive byte size"))
    })
}

#[cfg(target_family = "wasm")]
fn env_usize(_name: &str) -> Option<usize> {
    None
}

fn parse_size(value: &str) -> Option<usize> {
    let trimmed = value.trim();
    let last_digit = trimmed.rfind(|ch: char| ch.is_ascii_digit())?;
    let (digits, suffix) = trimmed.split_at(last_digit + 1);
    let base = digits.parse::<usize>().ok()?;
    let multiplier = match suffix.to_ascii_lowercase().as_str() {
        "" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024 * 1024,
        _ => return None,
    };
    base.checked_mul(multiplier)
}
