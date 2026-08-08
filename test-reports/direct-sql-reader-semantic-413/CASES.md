# Direct SQL entity-reader semantic discriminator

Anchor: `413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d`.

This is test/report-only acceptance material. It does not define a second
storage owner and does not authorize restoring the old row reader. The
canonical ForkTree read must serve every accepted case through one
operation-owned `CoherentView`/`StorageAdapterRead` and one semantic scan.

## Required cases

1. **Absent identity** — a missing entity key produces no row; it must not be
   synthesized as a NULL row or treated as a tombstone.
2. **SQL NULL** — an authenticated `StateCell::Null` produces one row with a
   present identity and SQL NULL payload.
3. **Deletion tombstone** — an authenticated `StateCell::Tombstone` follows
   the explicit tombstone policy, retaining deletion state when requested and
   remaining absent by default.
4. **Tracked + untracked** — one retained view composes tracked and untracked
   current rows, preserving each row's retention/owner marker and identity.
5. **Global/local replacement** — a branch value replaces the equal global
   key; a branch tombstone suppresses the global value when tombstones are
   excluded, and is returned with deletion state when explicitly requested.
6. **Ordering and LIMIT** — filter by typed `EntityPk` and schema/file identity
   before applying LIMIT; order is `(EntityPk, file_id)` and remains stable
   across tracked/untracked and global/branch rows.
7. **Snapshot projection** — projected bytes preserve present identity,
   payload NULL, and deletion marker as separate states; `None` bytes alone
   are insufficient evidence.
8. **Primary-key projection** — PK output preserves typed ordering and does not
   turn absent/tombstoned identities into ordinary live keys.
9. **Malformed authority** — malformed selector/root/state key/value, wrong
   branch/domain, duplicate logical key, or substituted global/local row fails
   closed before output or LIMIT.
10. **No semantic hiding** — the canonical path must not return capability
    rejection for a supported retention/tombstone case merely to invoke the
    old materialized-row reader. The old `scan_entity_rows` path and physical
    fallback are forbidden.

## Required source proof

- `ForkTreeReadFacade::scan_entity_rows` is absent.
- Snapshot and PK projections each invoke exactly one
  `LiveStateReader::scan_batch`.
- The same operation-owned read/view supplies selector, global root, branch
  root, tracked rows, and untracked rows.
- Overlay precedence, typed key order, filter-before-LIMIT, and tombstone
  precedence are authenticated by the canonical ForkTree serving code.
- The terminal projection carries deletion/null state separately; a
  `Vec<Option<Bytes>>` projection cannot certify this contract.
- Decode, domain, duplicate, and identity errors propagate before partial
  output; no fallback or compatibility reader handles malformed data.

The 413 head is expected to be RED because its new terminal type is
`Vec<Option<Bytes>>`, it removed the old eligibility guard without adding a
deletion marker, and its ForkTree reader selects tracked or untracked mode
instead of composing the complete retention overlay.
