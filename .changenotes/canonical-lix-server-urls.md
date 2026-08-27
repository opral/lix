---
type: minor
---

Standardized remote Lix URLs and added streamed snapshot export to the server protocol.

Remote clients now connect with an immutable `https://host/lix/{uuid}` locator, while raw HTTP clients use `/lix/v1/{uuid}/...`. The previous host-specific URL plus appended `/lix/v1` shape is no longer supported.
