# Main-native hot-index key-only experiment (qualified NO-CUT)

## Identity and question

- Baseline: `d2c634b2aeb780aff46013ec04902fcbb5c6f846`
- Baseline tree: `d321745bf83a7e7358b038880ad40004fc888ee5`
- Workload: production `tracked_state_crud` SQL session, 10,000 rows, setup excluded,
  ten samples per operation, RocksDB and SlateDB.
- Candidate: keep canonical hot rows unchanged, but make the derived declared-column
  hot index key-only. Decode the already canonical native `RowPk` suffix from the
  index key instead of reading and parsing the duplicated JSON PK value.

The candidate was built and measured, then removed from the source tree after the
NO-CUT verdict. Its release binary is preserved by SHA-256 below.

## Median latency

| adapter | operation | d2c baseline | key-only candidate | candidate / baseline |
|---|---:|---:|---:|---:|
| RocksDB | exact point | 1.190678 ms | 1.282639 ms | 1.0772 |
| RocksDB | indexed OLAP filter | 1.803710 ms | 1.771828 ms | 0.9823 |
| RocksDB | update one | 0.805213 ms | 0.904809 ms | 1.1237 |
| SlateDB | exact point | 1.263765 ms | 1.253754 ms | 0.9921 |
| SlateDB | indexed OLAP filter | 1.889651 ms | 1.906099 ms | 1.0087 |
| SlateDB | update one | 0.952410 ms | 0.949564 ms | 0.9970 |

The only intended beneficiary, indexed filtering, improved 1.77% on RocksDB and
regressed 0.87% on SlateDB. Point is structurally unaffected and exposes the run's
noise/guardrail: RocksDB regressed 7.72%. Update regressed 12.37% on RocksDB and
was neutral on SlateDB.

## Adapter and byte attribution

The candidate does not change adapter call cardinality: the path still performs
one witness point read, the same candidate range scan/page sequence, and the same
canonical exact-row resolution. It changes the index scan projection from
`FullValue` to `KeyOnly` and changes each derived index put from a duplicated JSON
PK value to an empty value. Thus returned and written derived-index value bytes
fall from the exact sum of encoded JSON PK lengths to zero; index keys and all
canonical-row bytes are unchanged. The frozen runner did not expose per-space
adapter byte counters, so no fabricated aggregate byte total is claimed. This is
diagnostic only: eliminating those bytes did not produce meaningful wall-clock
improvement on both engines.

## Correctness and authority

Every timed operation retained its benchmark assertions and completed normally.
The candidate did not alter canonical row authority, query semantics, or storage
view ownership. Native key decoding rejected prefix escape, malformed typed value,
invalid terminators, truncated PKs, and trailing key bytes. No compatibility
decoder, fallback, second index authority, or ordinary-row JSON representation was
added.

## Verdict

**QUALIFIED NO-CUT.** The derived index's duplicated JSON PK value is untidy, but
it is not a material end-to-end owner. An adapter/layout cut must improve both
RocksDB and SlateDB; this one does not clear even 10%, and its RocksDB update
guardrail is materially red. Do not land it.

## Reproduction

Release binaries:

- baseline: `17c33e9bae9e999368dc9723e2f5fee0fee91bb4944256af4539cd91af8e5deb`
- candidate: `10465dacc44c5d4216fd731a820e03b440144a088e2524afc300128460433d1f`

Common environment (substitute operation and storage):

```sh
LIX_TRACKED_STATE_CRUD_PROFILE=1 \
LIX_TRACKED_STATE_CRUD_PROFILE_LAYER=sql_session \
LIX_TRACKED_STATE_CRUD_PROFILE_STORAGE=rocksdb \
LIX_TRACKED_STATE_CRUD_PROFILE_ROW_COUNT=10000 \
LIX_TRACKED_STATE_CRUD_PROFILE_SAMPLES=10 \
LIX_TRACKED_STATE_CRUD_PROFILE_OP=read_one \
/tmp/tracked_state_crud-d2c-base
```

Operations were `read_one`, `update_one`, and `read_all` with
`LIX_TRACKED_STATE_CRUD_PROFILE_READ_SHAPE=olap_filter` for the indexed filter.
Raw logs live under `/root/repos/evidence/main-native-index-d2c/raw`.
