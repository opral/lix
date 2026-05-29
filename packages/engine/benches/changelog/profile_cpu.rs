use std::hint::black_box;
use std::time::{Duration, Instant};

use lix_engine::LixError;
use lix_engine::changelog::bench as changelog_bench;

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
    let append = changelog_bench::append_1c_1000ch()?;
    let encoded = changelog_bench::encode_bench_append(&append)?;
    let deadline = Instant::now() + duration;
    let mut iterations = 0u64;

    while Instant::now() < deadline {
        match op {
            "decode" => {
                black_box(changelog_bench::decode_bench_append(&encoded)?);
            }
            "validate" => {
                black_box(changelog_bench::validate_bench_append_shape(&append)?);
            }
            "index" => {
                black_box(changelog_bench::build_decoded_append_index(&append)?);
            }
            "all" => {
                black_box(changelog_bench::decode_bench_append(&encoded)?);
                black_box(changelog_bench::validate_bench_append_shape(&append)?);
                black_box(changelog_bench::build_decoded_append_index(&append)?);
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
