## Optimizing Perf & Storage

- The read and write entrypoint is the vtable. Only optimize the vtable's query performance
  - Avoid optimizing by bypassing the vtable for direct access to underlying tables. That can lead to drift.
