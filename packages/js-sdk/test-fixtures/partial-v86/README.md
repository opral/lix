# Released v86 partial OPFS fixture

This fixture was generated on 2026-10-12 with the published npm packages
`@lix-js/sdk@0.19.0` and `@lix-js/storage-opfs@0.19.0`. Their compatibility
metadata reports storage format 86, server protocol 12 and sync protocol 28.
Package tarball SHA-256 hashes and the full disposable server revision are
recorded in `manifest.json`.

The published browser SDK created a real partial replica in OPFS against a
disposable v86 authority. The authority held a cached key/value, a custom table
with a unique `slug` column, and `/released-cache.bin` containing `[1, 2, 3]`.
The browser hydrated those rows and its repository identity, prepared offline
editing, then disconnected its HTTP transport. It authored both an indexed
`pending-note` row and the `browser-pending` key locally. These edits were never
uploaded. After the SDK closed, its released direct OPFS backend read the
physical records without invoking the current engine or changing format markers.

`opfs-records.bin` freezes those 100 records, including two resident declared
index entries and the original v86 active epoch. The fixture contains only
synthetic data and a disposable loopback authority locator; no authentication
headers are serialized. Canonical `exportSnapshot()` deliberately omits the
derived index plane and epoch control, so a canonical snapshot cannot reproduce
this migration failure.

The binary framing is `LIXOPFS1`, a big-endian u32 record count, and records of
three big-endian u32 fields (space ID, key length, value length), followed by the
key and value bytes. The fixture worker replays these exact records into fresh
OPFS storage. Normal SDK opening then performs the upgrade, with no configured
authority or network prerequisite.

The regression lives in `src/partial-preservation.browser.test.ts`, included by
the normal browser suite and `scripts/ci-test-browser.mjs`. Against the deployed
pre-fix SDK artifact it fails with `LIX_MIGRATION_PRESERVATION_FAILED` while
opening the local candidate. The fixture must remain frozen when current codecs
change. Regenerate it only as a deliberate evidence update, retaining the old
fixture and documenting the new producer and hashes.
