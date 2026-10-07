---
type: minor
target: lix
---
Recover and hydrate repositories through shared, byte-bounded dependency pages and content batches. Sealed read operations retain an immutable leased basis, stage content privately, validate the complete dependency proof on the client, and publish only validated dependencies. Interrupted operations release their scratch data, and reopening a replica reclaims abandoned work before admitting new reads.

Sync protocol 31 and physical repository format 87 require matching current clients and authorities. Storage providers must support byte-admitted exact reads, ordered point-read prefixes, and scan pages. Supported old physical repositories migrate before opening; SlateDB adds a resumable length-indexed layout while retaining the original generation and its immutable content.
