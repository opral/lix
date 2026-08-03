---
type: minor
---

Renamed the `lix_file`, `lix_file_by_branch`, and `lix_file_history` binary payload column from `data` to `content`. Native file read and write APIs now use `content` names as well; the former `data` surface is not supported.
