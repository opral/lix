---
type: patch
---

Fixed repeated plugin archive installs creating duplicate history.

Installing the same plugin archive again now leaves commits and changes unchanged, which also keeps filesystem-backed workspaces idle after bundled plugins have been materialized.
