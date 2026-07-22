---
type: patch
---

Native installed-plugin calls now have bounded execution time and Wasmtime resources.

Plugin instantiation, change detection, and rendering receive a fresh five-second guest deadline, alongside bounded memory, instance, and table allocation.
