---
type: patch
---

Reopen previously admitted browser replicas after a cold offline reload without granting cached credentials a remote lease. Persist only a credential digest and verified local routing identity scoped to the physical store, authority and protocol epochs; remote access still requires fresh admission.

Handle bodyless HTTP responses (204, 205 and 304) without constructing an invalid response stream, fixing remote session cleanup through wrapped browser transports.
