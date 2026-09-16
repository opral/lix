from pathlib import Path
import hashlib
p=Path('/root/repos/history-sync-research');s=(p/'e16-deep-controls-harness.rs').read_text()
start=s.index('    probe.offline.store(true, Ordering::Release);\n    let before = probe.snapshot();\n    let started = Instant::now();\n    let warm = replica.execute(&sql, &params).await.unwrap();')
end=s.index('    probe.offline.store(false, Ordering::Release);',start)
s=s[:start]+'''    probe.offline.store(true, Ordering::Release);
    let idle_ms: u64 = std::env::var("LIX_PROFILE_WARM_IDLE_MS").unwrap_or("0".into()).parse().unwrap();
    let repeats: usize = std::env::var("LIX_PROFILE_WARM_REPEATS").unwrap_or("20".into()).parse().unwrap();
    let mut warm_samples = Vec::new();
    for iteration in 0..repeats {
        if idle_ms > 0 { tokio::time::sleep(Duration::from_millis(idle_ms)).await; }
        let _ = attribution();
        let before = probe.snapshot();
        let started = Instant::now();
        let warm = replica.execute(&sql, &params).await.unwrap();
        let elapsed_us = started.elapsed().as_micros();
        let counters = attribution();
        assert_eq!(warm.rows(), expected.rows());
        let network = diff(&probe.snapshot(), &before);
        assert_eq!(native_requests(&network), 0, "covered query must be local offline");
        warm_samples.push(json!({"iteration":iteration,"us":elapsed_us,"counters":counters,"network":network}));
    }
    let warm_us = warm_samples[0]["us"].as_u64().unwrap();
'''+s[end:]
s=s.replace('json!({"schema":"lix.partial-history.v1",','json!({"warm_samples":warm_samples,"warm_idle_ms":idle_ms,"trace_enabled":lix::storage_bench::root_replay_trace_enabled(),"schema":"lix.partial-history.e17-diagnostic",')
s+='''
fn attribution() -> serde_json::Value {
    use lix::storage_bench as b;
    let c = b::take_root_replay_cost_attribution();
    let r = b::take_root_replay_accounting();
    let plan = b::take_plan_load_attribution();
    fn cost(c: b::RootReplayCostBucket) -> serde_json::Value {
        json!({"ns":c.total_nanos,"bytes":c.total_bytes,"count":c.total_count,"replay_ns":c.replay_nanos})
    }
    json!({"read":cost(c.storage_read),"decode":cost(c.decode),"hash":cost(c.hash),"encode":cost(c.encode),
      "root_cache":b::take_root_base_batch_cache_accounting(),"scan_arms":b::take_tracked_scan_branch_accounting(),
      "replay":{"boundaries":r.boundaries,"loaded":r.plans_loaded,"staged":r.plans_staged,"probes":r.available_root_probes,"hits":r.available_root_hits,"load_ns":r.plan_load_nanos,"stage_ns":r.stage_nanos},
      "plan":{"plans":plan.plans,"members":plan.members_decoded,"bytes":plan.member_payload_bytes,
        "phases":plan.phases.iter().enumerate().map(|(i,v)|json!({"name":b::PLAN_LOAD_PHASE_NAMES[i],"wall_ns":v.wall_nanos,"io_ns":v.io_nanos,"calls":v.read_calls,"keys":v.read_keys,"bytes":v.read_bytes,"entries":v.entries})).collect::<Vec<_>>()}})
}
'''
f=p/'e17-warm-diagnostic-harness.rs';f.write_text(s);print(hashlib.sha256(f.read_bytes()).hexdigest())
