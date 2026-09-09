---
type: patch
---

Fixed filesystem imports failing with "plugin observation is unknown or evicted" after edits through another Lix session.

Read batches that combine file contents with directory or metadata queries now refresh the observations for the file bytes actually returned. Aggregate queries still do not authorize overwriting unseen file contents.
