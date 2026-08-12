---
type: minor
---

File and entity history queries no longer slow down as a repository's commit history grows deeper.

Every commit now records which kinds of data it changed, so a history query can skip past commits that cannot contain the answer instead of opening each one. This changes the on-disk commit format: repositories written by older versions must be recreated with this release.
