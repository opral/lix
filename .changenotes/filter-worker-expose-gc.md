---
type: patch
---

Fixed Node.js worker startup when the host process uses worker-incompatible runtime flags such as `--expose-gc`.

The JavaScript SDK no longer forwards host runtime flags to its self-contained worker.
