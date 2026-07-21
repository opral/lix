---
type: minor
---

Reduced tracked-state write amplification with the v3 tree codec.

Tracked-state keys now use a prefix-friendly ordered encoding. Leaf nodes
front-code keys, dictionary repeated commit and state metadata, and compact
equal or small timestamps; internal nodes front-code child boundaries. This is
a hard storage-format cut; repositories created by older engine versions must
be recreated.

Ordinary tracked-state diffs now retain hash-guided sparse traversal: they bind
commit-root first-parent metadata and point-validate every changed row's change
record. Winner reachability, inherited timestamps, and whole-root coverage
are checked against staged chunks by the full audit in the explicit tracked-state
rebuild path before publication, instead of scanning all unchanged rows on every
merge. A hierarchical Merkle zipper also preserves subtree skipping when an
insert shifts chunk boundaries or changes the root height.
