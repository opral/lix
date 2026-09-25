---
type: minor
---

Checkpoints can carry a title and Zettel comment for historical context through `lix_create_checkpoint(title, comment)`.

Both arguments accept `NULL`. When either is present, `lix_log().conversation_id` identifies the checkpoint's conversation so applications can join to its title and comments. The previous zero- and one-argument checkpoint calls are removed.
