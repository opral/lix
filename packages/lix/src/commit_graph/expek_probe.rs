//! expEK MEASUREMENT INSTRUMENT — NOT FOR MERGE.
//!
//! Answers one question the per-scope digest census cannot: of the commits the
//! scope digest still has to load, how many carry **no member whose entity pk
//! the request asked for**? Those are exactly the commits an entity-keyed
//! `(schema_key, entity_pk)` token set could prune, so this counts the ceiling
//! of that artifact instead of modelling it.
//!
//! The probe sits **inside** the membership route (it observes the loaded
//! member vector, one layer below the traversal), so a zero here means "no
//! commits are entity-prunable", not "the instrument missed the lane".

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

#[derive(Default, Debug, Clone, Copy)]
pub(crate) struct ProjectionRow {
    /// Scope digest already proved absence — no delta load.
    pub(crate) pruned_scope: u64,
    /// Scope digest could not prune; the delta was loaded.
    pub(crate) loaded: u64,
    /// Of `loaded`: request pins entity pks and NO loaded member matched one.
    /// An exact per-entity membership test would have pruned this commit.
    pub(crate) loaded_entity_absent: u64,
    /// Of `loaded`: at least one loaded member matched a requested entity pk.
    pub(crate) loaded_entity_present: u64,
    /// Of `loaded`: the request pins no entity pk at all, so an entity-keyed
    /// artifact is structurally unable to help this projection.
    pub(crate) loaded_no_entity_filter: u64,
    /// Request placed no schema constraint.
    pub(crate) unconstrained: u64,
    /// Total members returned by the schema-filtered delta loads.
    pub(crate) members_loaded: u64,
    /// Distinct entity pks seen across those members (per commit, summed).
    pub(crate) distinct_entities_loaded: u64,
}

type Table = BTreeMap<String, ProjectionRow>;

fn table() -> &'static Mutex<Table> {
    static TABLE: OnceLock<Mutex<Table>> = OnceLock::new();
    TABLE.get_or_init(|| Mutex::new(BTreeMap::new()))
}

pub(crate) fn label(schema_keys: &[String]) -> String {
    if schema_keys.is_empty() {
        "<unconstrained>".to_string()
    } else {
        schema_keys.join("+")
    }
}

pub(crate) fn with_row(schema_keys: &[String], update: impl FnOnce(&mut ProjectionRow)) {
    if !enabled() {
        return;
    }
    let mut table = table().lock().expect("expEK probe table is not poisoned");
    update(table.entry(label(schema_keys)).or_default());
}

pub(crate) fn enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("LIX_EXPEK_PROBE").is_ok())
}

/// Write-side cardinality histogram: how many delta members, and how many
/// distinct scopes, each commit publishes. `scope_count == u64::MAX` marks an
/// opaque commit (the scope set was not enumerable).
static WRITES: OnceLock<Mutex<Vec<(u64, u64)>>> = OnceLock::new();

pub(crate) fn record_commit_write(member_count: u64, scope_count: u64) {
    if !enabled() {
        return;
    }
    WRITES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .expect("expEK write table is not poisoned")
        .push((member_count, scope_count));
}

/// Prints one digest line for the whole write population, with the case count,
/// rather than one line per commit (a transcript of 2000 lines is exactly what
/// the runbook says not to diff).
pub(crate) fn print_write_summary(tag: &str) {
    if !enabled() {
        return;
    }
    let writes = std::mem::take(
        &mut *WRITES
            .get_or_init(|| Mutex::new(Vec::new()))
            .lock()
            .expect("expEK write table is not poisoned"),
    );
    if writes.is_empty() {
        eprintln!("expek_writes tag={tag} commits=0");
        return;
    }
    let mut members = writes.iter().map(|(m, _)| *m).collect::<Vec<_>>();
    members.sort_unstable();
    let opaque = writes.iter().filter(|(_, s)| *s == u64::MAX).count();
    let mut scopes = writes
        .iter()
        .filter(|(_, s)| *s != u64::MAX)
        .map(|(_, s)| *s)
        .collect::<Vec<_>>();
    scopes.sort_unstable();
    let pick = |sorted: &[u64], q: f64| -> u64 {
        if sorted.is_empty() {
            return 0;
        }
        sorted[((sorted.len() - 1) as f64 * q).round() as usize]
    };
    eprintln!(
        "expek_writes tag={tag} commits={} members_p50={} members_p95={} members_max={} \
         members_sum={} scopes_p50={} scopes_p95={} scopes_max={} opaque={}",
        writes.len(),
        pick(&members, 0.50),
        pick(&members, 0.95),
        members.last().copied().unwrap_or(0),
        members.iter().sum::<u64>(),
        pick(&scopes, 0.50),
        pick(&scopes, 0.95),
        scopes.last().copied().unwrap_or(0),
        opaque,
    );
}

/// Drains and prints the table. One line per projection, with the case count on
/// the line so a lost line is detectable.
pub(crate) fn drain_and_print(tag: &str) {
    if !enabled() {
        return;
    }
    print_write_summary(tag);
    let drained = std::mem::take(&mut *table().lock().expect("expEK probe table is not poisoned"));
    for (projection, row) in drained {
        eprintln!(
            "expek_projection tag={tag} projection={projection} \
             pruned_scope={} loaded={} loaded_entity_absent={} loaded_entity_present={} \
             loaded_no_entity_filter={} unconstrained={} members_loaded={} distinct_entities_loaded={}",
            row.pruned_scope,
            row.loaded,
            row.loaded_entity_absent,
            row.loaded_entity_present,
            row.loaded_no_entity_filter,
            row.unconstrained,
            row.members_loaded,
            row.distinct_entities_loaded,
        );
    }
}
