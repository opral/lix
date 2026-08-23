---
type: patch
---

OPFS-backed Lix repositories now batch point reads through SQLite and suppress delayed retry replays after an owner request completes. This keeps checkpointing and other bounded foreground operations responsive while multiple browser clients bootstrap the same repository.
