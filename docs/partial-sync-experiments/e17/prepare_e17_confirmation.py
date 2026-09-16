from pathlib import Path
import hashlib
p=Path('/root/repos/history-sync-research');s=(p/'e17-warm-diagnostic-harness.rs').read_text();s=s[:s.index('\nfn attribution()')]
s=s.replace('        let _ = attribution();\n','').replace('        let counters = attribution();\n','').replace(',"counters":counters','').replace('"trace_enabled":lix::storage_bench::root_replay_trace_enabled(),','"instrumented":false,')
s=s.replace('    let mut warm_samples = Vec::new();','''    let warmup_ms: u64 = std::env::var("LIX_PROFILE_CPU_WARMUP_MS").unwrap_or("0".into()).parse().unwrap();
    let warmup = Instant::now();
    let mut state = 1_u64;
    while warmup.elapsed().as_millis() < u128::from(warmup_ms) {
        for _ in 0..10000 { state = std::hint::black_box(state.wrapping_mul(6364136223846793005).wrapping_add(1)); }
    }
    let mut warm_samples = Vec::new();''')
s=s.replace('"warm_samples":warm_samples,','"cpu_warmup_ms":warmup_ms,"warm_samples":warm_samples,')
f=p/'e17-confirmation-harness.rs';f.write_text(s);print(hashlib.sha256(f.read_bytes()).hexdigest())
