Shared Rust source for the JavaScript bindings. The SDK compiles the WASM
backend; the filesystem package compiles the native backend. Shared session,
serialization, telemetry, and component-host code stays in one place. This
directory is not a standalone Cargo package.

The filesystem crate's build script enables `lix_filesystem_native` and supplies
the platform linker configuration. The SDK's host builds run shared Rust tests;
they do not compile or link the N-API backend.
