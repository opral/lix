---
type: minor
---

Finish the pre-0.12 terminology hard cut. The JavaScript protocol entry point is
now `@lix-js/sdk/server-protocol`, its public constants and wire types use
`ServerProtocol` names, and invalid wire data reports
`LIX_SERVER_PROTOCOL_ERROR`. The previous entry point and names have no
compatibility aliases. SDK documentation and examples now use repository
terminology throughout.
