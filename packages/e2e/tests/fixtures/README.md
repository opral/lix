# Snapshot fixtures

`v72_partial_checkpoints.lixsnap` contains state authored on Lix v71 and
migrated/partial-checkpointed by Lix `4816fdba5`. It was converted from the
previously tracked deterministic `LIXMEM` fixture to `LIXSNAP` on 2026-08-27.

The fixture reproduces the sync-shaped path where a sparse replica hydrates a
partial-checkpoint anchor and must still resolve the checkpoint's file and
directory state. Its SHA-256 is
`634eefb12a96bbb656214d5f203fb2f0dbd0fc552379754e3c86eb9cb99b6f70`.

Keep provenance outside the binary payload. Regenerate the fixture only as an
explicit evidence migration and update this note and its regression test.
