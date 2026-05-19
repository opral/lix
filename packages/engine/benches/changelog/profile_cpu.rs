use std::hint::black_box;
use std::time::{Duration, Instant};

use lix_engine::changelog::bench as changelog_bench;
use lix_engine::LixError;

fn main() {
    let mut args = std::env::args().skip(1);
    let op = args.next().unwrap_or_else(|| "all".to_string());
    let seconds = args
        .next()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(15);

    run(&op, Duration::from_secs(seconds)).expect("run changelog CPU profile workload");
}

fn run(op: &str, duration: Duration) -> Result<(), LixError> {
    let segment = changelog_bench::segment_1c_1000ch()?;
    let encoded = changelog_bench::encode_bench_segment(&segment)?;
    let deadline = Instant::now() + duration;
    let mut iterations = 0u64;

    while Instant::now() < deadline {
        match op {
            "decode" => {
                black_box(changelog_bench::decode_bench_segment(&encoded)?);
            }
            "validate" => {
                black_box(changelog_bench::validate_bench_segment_shape(&segment)?);
            }
            "index" => {
                black_box(changelog_bench::build_decoded_segment_index(&segment)?);
            }
            "all" => {
                black_box(changelog_bench::decode_bench_segment(&encoded)?);
                black_box(changelog_bench::validate_bench_segment_shape(&segment)?);
                black_box(changelog_bench::build_decoded_segment_index(&segment)?);
            }
            _ => {
                return Err(LixError::unknown(format!(
                    "unknown changelog CPU profile op '{op}', expected decode, validate, index, or all"
                )));
            }
        }
        iterations += 1;
    }

    eprintln!(
        "changelog_cpu_profile op={op} duration_ms={} iterations={iterations}",
        duration.as_millis()
    );
    Ok(())
}
