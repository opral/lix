---
type: minor
---

Moved legacy working-diff repair out of application and protocol session opening and into the explicit offline repository migration. Repository format v72 guarantees current working-diff epochs before the engine opens, so successful handshakes allocate session state without repairing repository storage.
