---
type: patch
---

Fixed two data-loss bugs: silent row loss on repositories with more than 1024 plugin-backed files, and a rebuild that could fail part-way and leave a branch unreadable.

On a repository with more than 1024 plugin-backed files, reads could return fewer rows than exist, and creating a branch could permanently drop the certified manifests of files past the 1024th — the new branch was missing those files with no error and no warning. Separately, rebuilding a branch's tracked state across a deep history could fail with a duplicate-mutation error, leaving the repository unreadable in that mode; that failure was fail-closed, so it never returned incorrect data.
