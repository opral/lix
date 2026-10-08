---
type: minor
target: lix
---
Recover and hydrate repositories through shared, byte-bounded dependency pages and content batches. Sealed read operations retain an immutable leased basis, stage content privately, validate the complete dependency proof on the client, and publish only validated dependencies. Interrupted operations release their scratch data, and reopening a replica reclaims abandoned work before admitting new reads. The authority reclaims inactive retained responses under pressure, allowing new reads after client termination without waiting for cached responses to expire.

Matching clients and authorities must use the protocol and storage-format versions advertised by the SDK's compatibility metadata. Storage providers must support byte-admitted exact reads, ordered point-read prefixes, and scan pages. Supported old physical repositories migrate before opening; SlateDB adds a resumable length-indexed layout while retaining the original generation and its immutable content.
