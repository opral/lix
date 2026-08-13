---
type: minor
---

File and entity history queries no longer slow down as much as a repository's commit history grows deeper.

Every commit now records which kinds of data it changed, so a history query can skip past commits that cannot contain the answer instead of opening each one. This changes the stored commit format: repositories created by an earlier version are rejected at open with "recreate the repository" and must be recreated with this release.
