# EXP-KEY-ORDER-15

Status: **QUALIFIED WIN (model); production cut pending**.

## Anchors and authority

- Parent: `5089b964d5e9b0143656c5278e525db9100e2b61`
  (`origin/codex/schema-forktree-unified-dc4-347`).
- No later native-carrier ref was visible when the model was started.
- Both layouts store identical Schema-v1 tuple bodies in one canonical,
  content-addressed C2 page directory. There is no second index, cache,
  fallback, JSON encoding, or synthetic value path.
- Old identity order: schema, file owner, typed PK, scope.
- Candidate identity order: schema, typed PK, file owner, scope.
- Pages authenticate layout, identity count, full typed key, physical position,
  tuple body, object hash, and root position.

## Decisive matrix

Repeated 20-sample paired RocksDB/SlateDB crossover: integer and composite PK,
N=1K/10K/100K, D=1 and 1%, H=10 (24 layout pairs).

| Operation | candidate/old median | worst repeated pair |
| --- | ---: | ---: |
| typed exact PK p50 | 0.292x | 0.348x |
| typed exact PK p95 | 0.294x | 0.532x |
| typed PK range100 p50 | 0.638x | 0.662x |
| typed PK range100 p95 | 0.639x | 0.670x |
| update p50 | 0.988x | targeted tail resolved below |
| readback p50 | 0.973x | 1.038x |
| diff p50 | 0.982x | 0.998x |
| settled bytes | 0.994x | 1.001x |

The initial H=10 tails contained two sequential-order outliers. Reversed-order,
100-sample targeted runs removed them: update p50/p95 ranged 0.793/0.719x to
1.020/0.919x; readback ranged 0.969/0.963x to 0.980/0.776x. Exact and range
remained 0.248-0.321x and 0.599-0.651x.

One sparse-diff key set touched 82 candidate versus 76 old leaves. An
eight-seed Slate sweep showed this is symmetric page-occupancy variation, not
a systematic regression: diff candidate/old mean/median were 0.984/0.980x
p50 and 0.982/0.984x p95. Worst seed was 1.067x p50 and 1.048x p95; other
seeds favored the candidate as strongly as 0.886x.

Physical explanation: same-PK rows across four file owners require four old
pages but one candidate page in the representative exact cell. Across the
matrix, candidate exact reads used median 0.400x object keys, 0.340x bytes and
0.250x decoded rows; range100 used 0.667x keys, 0.669x bytes and 0.636x rows.

## Correctness

All 48 focused cells and all 8 seed-sweep pairs passed identical logical
digests, authenticated missing/wrong-child/root-substitution controls, and
cold reopen. Codec controls reject wrong layout/count/owner binding,
noncanonical key order, malformed/truncated pages and adjacent-prefix escape;
they preserve local-over-global and untracked-over-tracked Value/NULL/tombstone
visibility.

## Evidence

- focused repeated raw log SHA-256:
  `b54153840549b64984b14aa85419a09376751c342073dbab1901507b4d4336bb`
- reversed-order targeted raw log SHA-256:
  `72a03278d5a0d408e96d18793d2203ac44dc8a66e41ba02a84b748f2b69ec55d`
- eight-seed diff raw log SHA-256:
  `bccc13b324c6dd20b9bc9dda86bd510502581be566e4cc643328214ea5bd5cb9`
- release binary SHA-256:
  `6a0cb6345b93a0f71cd3a6b78a9526a57d95a2e362d50ec4caa8a5046235bce6`

## Decision

The OLTP-first gate passes: important typed exact/range paths improve far more
than 5%; mutation, full readback, diff and settled bytes have no systematic
critical regression. The global consecutive no-win streak resets from 16/20
to **0/20**. A production compiler-first hard cut must still delete the old
codec/readers/writers and independently prove the integrated semantics.
