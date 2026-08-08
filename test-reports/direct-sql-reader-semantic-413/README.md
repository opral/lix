# Direct SQL entity-reader semantic oracle

Test/report-only successor anchored at the immutable production candidate
`413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d`.

Run from this directory:

```sh
bash verify_entity_semantics.sh /root/repos/lix-direct-reader-successor-413 candidate
```

The expected result on the anchor is a deliberate RED. The oracle does not
compile or modify production sources. It distinguishes a real semantic
correction from the prior workaround of rejecting tombstone/retention cases
and falling back to the old row path.
