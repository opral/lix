---
type: minor
---

Added a native binary file-upsert route for remote Lix clients.

`POST /lix/v1/file/upsert` accepts an `application/octet-stream` file body and
uses Lix's existing transactional file-write path without JSON base64 encoding
or SQL planning. Clients can discover the capability in the handshake response
as `binaryFileUpsert`.
