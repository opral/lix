---
type: minor
---

Added a native binary file-read route for remote Lix clients.

`GET /lix/v1/file` returns raw file bytes without SQL planning or JSON/base64
encoding. Clients can discover the capability in the handshake response as
`binaryFileRead`; `Lix-File-Found` distinguishes a missing file from a present
empty file.
