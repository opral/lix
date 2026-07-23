---
type: minor
---

Added checkpoints for compacting automatic commit history into deliberate milestones.

The SDK can create checkpoints, while read-only SQL surfaces expose checkpoint history and the net working changes on the active branch or across branches. Older automatic-commit intervals are garbage collected after one recovery interval.
