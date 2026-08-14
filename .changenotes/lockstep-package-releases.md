---
type: patch
---

Release `lix`, its Rust storage adapters, `@lix-js/sdk`, and JavaScript storage adapters at one lockstep version from one workflow.

The release workflow now publishes crates.io packages in dependency order, waits for both Rust and npm publishing before creating the GitHub release, and safely resumes partial releases by verifying immutable registry checksums.
