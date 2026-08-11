# Does `untracked` need a second plane?

Design analysis of the untracked state lane in lix: history, current physical
reality, measured cost, and a recommendation across three candidate designs.

Machine: `ryzen-9950x-IV` (32 cores, 186 GB). Tree: `claude-3-optimizations` @ `43de7b54d`.

---

## 0. Answer first

**Recommendation: C — remove the untracked *lane*, keep nothing of it but the
public filter. Adopt the shape of PR #1273, rebased onto `claude-3-optimizations`.**

The owner's design B ("give untracked rows a real `change_id`/`commit_id` and
keep only a marker") is, on inspection, *not a different design from C* — it is
C plus a boolean column that nobody needs, because in lix a row that carries a
real `change_id` and `commit_id` and lives in the one tracked plane **is a
tracked row**. The two things B wants to preserve are already available without
a second plane:

- *"don't show me untracked rows"* — this is a `WHERE` predicate. The marker is
  already a public, readable column today (`registry.rs:597`, `:628`), and the
  diff surface is already SQL (`docs/diff-commands.md`), so the filtering can be
  expressed at the query. But note what B is actually asking for: a per-row
  "this is ephemeral" tag. That tag does **not** need engine support at all —
  it is a schema-level concern (`ui_state` rows are the ones you filter out),
  and lix already lets a plugin/app name its own schema keys.
- *history GC for fast-mutating state* — this is checkpoint/retention policy on
  the tracked plane, keyed by schema, not a second storage lane.

The measured evidence below shows the second plane's cost is not mainly read
latency (it is cheap when idle, because the fast path short-circuits) — it is
**write amplification, disabled fast paths, lost features, and ~5,100 net lines
of production engine code carrying dual authority through the hottest files in
the repo.**

---

<!--MEASUREMENTS-->

