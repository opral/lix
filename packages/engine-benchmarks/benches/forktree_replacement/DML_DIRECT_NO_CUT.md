# ForkTree SQL DML direct-read experiment — terminal NO-CUT

Verdict: **BLOCKER / NO-CUT**. The exact SQL result digest remains correct,
but the transaction-scoped physical target cannot remove the model bridge's
full snapshot without either multiplying broad scans or independently
interpreting DataFusion filters. The latter would be a second SQL authority.

## Causal classification

The original SlateDB 1K one-transaction result was 5,239.224 us versus
4,285.660 us for current Lix (+22.251%). Its phase attribution was:

- authenticated ForkTree full-row load/decode: 946 us;
- unchanged Lix binder/write executor: 2,798 us;
- one ForkTree selector publication: 281 us.

The 946 us `O(N)` model snapshot is therefore the dominant excess term. Its
perfect elimination is 18.056% of model wall and projects 4,293.224 us, only
0.177% above current Lix. The SQL semantic owner itself remains `O(R + E)`.
The diagnostic bridge is `O(N + R log_B N + E)`.

The smallest honest correction forwarded Lix's transaction-owned structured
scan/exact-read requests to the authenticated ForkTree and retained only a
normal staged postimage overlay. Its proposed complexity was
`O(Q log_B N + R log_B N + E)`, where `Q` is the number of identities selected
by Lix; legitimate broad scans remain `O(N)`.

The focused gate proved that `Q` is not available at this boundary. The 18
statement workload issued 15 provider-level broad scans and zero exact/point
reads. Primary-key predicates remain DataFusion row filters above the physical
request. Reinterpreting those filters in the model adapter would duplicate the
Lix binder/provider authority, while changing the cursor/storage or Stage-2
production boundaries was explicitly out of scope.

## Focused rejected gate

SlateDB, 1,000 setup rows, one 18-statement transaction, setup excluded:

| Axis | Original ForkTree bridge | Direct-read prototype | Delta |
|---|---:|---:|---:|
| wall | 5,239.224 us | 17,140.733 us | +227.162% |
| CPU | 5,249.535 us | 17,129.652 us | +226.308% |
| allocated bytes | 7,975,593 | 35,611,184 | +346.503% |
| allocation calls | 63,437 | 246,062 | +287.884% |
| physical read objects | 37 | 543 | +1,367.568% |
| physical read bytes | 20,008 | 292,295 | +1,360.891% |
| physical write objects | 1 | 1 | 0% |
| physical write bytes | 3,089 | 3,089 | 0% |
| settled disk | 29,722 B | 29,722 B | 0% |

Both paths produced result digest
`93ca58c1bbfe93ab2d99e323e317b9e0b2441291be25fe64397cb7fdfa88c41e`,
ended with 1,005 live rows, and published six authenticated objects / 2,993
object bytes in one root transition. The rejected prototype made 571 logical
read transactions and 604 key gets because each broad request reopened the
authenticated tree.

## Slate write amplification

The original current/ForkTree one-transaction physical writes were
2 objects / 1,662 B versus 1 object / 3,089 B: ForkTree reduced request/object
count by 50% but increased physical bytes by 85.861% (+1,427 B). This is the
immutable six-object path/root postimage encoded into one Slate table write,
not the full-snapshot read term. Logical write bytes were lower for ForkTree
(3,245 B versus 6,575 B), and settled disk was 76.710% lower (29,722 B versus
127,613 B). Eliminating the +1,427 B would require a distinct Stage-2 object
packing/publication cut; it cannot be changed in this model adapter and has no
measured >10% wall ceiling.

No RocksDB, 10K, or 50K matrix was run after the smallest Slate gate failed.
The implementer contract remains: Stage 2 must place the accepted authenticated
range/point iterator below the existing Lix provider so predicates are lowered
once by Lix and the physical target receives identities/ranges directly. It
must retain one atomic selector publication and one SQL binder/write executor.
