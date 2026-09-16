from pathlib import Path
import hashlib
p=Path('/root/repos/history-sync-research');s=(p/'e17-warm-diagnostic-harness.rs').read_text()
s=s.replace('    let mut warm_samples = Vec::new();','''    let warmup_ms: u64 = std::env::var("LIX_PROFILE_CPU_WARMUP_MS").unwrap_or("0".into()).parse().unwrap();
    let warmup = Instant::now();
    let mut state = 1_u64;
    while warmup.elapsed().as_millis() < u128::from(warmup_ms) {
        for _ in 0..10000 { state = std::hint::black_box(state.wrapping_mul(6364136223846793005).wrapping_add(1)); }
    }
    let mut control = PerfControl::open();
    if let Some(c) = control.as_mut() { c.command("enable"); }
    let mut warm_samples = Vec::new();''')
s=s.replace('    let warm_us = warm_samples[0]["us"].as_u64().unwrap();','''    if let Some(c) = control.as_mut() { c.command("disable"); }
    let warm_us = warm_samples[0]["us"].as_u64().unwrap();''')
s=s.replace('"warm_samples":warm_samples,','"cpu_warmup_ms":warmup_ms,"warm_samples":warm_samples,')
s+='''
struct PerfControl { control: std::fs::File, ack: std::io::BufReader<std::fs::File> }
impl PerfControl {
    fn open() -> Option<Self> {
        let ctl = std::env::var("LIX_PROFILE_PERF_CTL").ok()?;
        let ack = std::env::var("LIX_PROFILE_PERF_ACK").unwrap();
        Some(Self { control: std::fs::OpenOptions::new().read(true).write(true).open(ctl).unwrap(),
            ack: std::io::BufReader::new(std::fs::OpenOptions::new().read(true).write(true).open(ack).unwrap()) })
    }
    fn command(&mut self, command: &str) {
        use std::io::{Write, BufRead};
        writeln!(self.control, "{command}").unwrap();self.control.flush().unwrap();
        let mut response=String::new();self.ack.read_line(&mut response).unwrap();assert_eq!(response.trim(),"ack");
    }
}
'''
f=p/'e17-perf-harness.rs';f.write_text(s);print(hashlib.sha256(f.read_bytes()).hexdigest())
