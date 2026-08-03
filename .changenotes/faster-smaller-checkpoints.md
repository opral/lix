---
type: patch
---

Made checkpoints faster and smaller, especially for repositories with long histories.

Checkpoints now write incremental changes instead of copying complete current state and history, while repository maintenance remains linear as history grows.
