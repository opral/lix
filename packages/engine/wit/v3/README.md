# Lix Plugin API v3 prototype

v3 is a hard cut. It deliberately has no guest-owned `document` resource and
no v2 adapter. A plugin invocation receives a capability to one immutable,
content-addressed host root and returns the host-created successor transaction
alongside sparse semantic or byte output.

The root contains three independently paged arenas:

1. exact accepted file bytes, represented as a rope so insertions do not
   renumber or copy the unchanged suffix;
2. complete durable entities keyed by `(schema_key, entity_pk)`;
3. opaque plugin-specific state keyed and versioned by plugin generation.

The host pre-seeds a transaction with the verified candidate file bytes.
Plugins read only needed file ranges, entity keys, and state pages, then replace
only affected state keys. The host drains and validates the sparse output
cursor, applies it to the entity arena, and publishes one successor root.
Any trap, deadline, invalid output, or dropped transaction leaves the previous
root unchanged.

Branching aliases a root. Eviction drops decoded page caches, not roots.
Upgrades receive the previous root under the old generation and must explicitly
produce a new-generation transaction. Merge resolution is performed over
durable entity keys before a single renderer transition; opaque state is a
rebuildable acceleration structure and never merge authority.
