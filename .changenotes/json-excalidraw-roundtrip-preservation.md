---
type: patch
---

Improved JSON and Excalidraw data and formatting preservation when alternating file and row edits.

JSON retains number spelling and string escapes when rebuilding files from rows, and inserted array items remain writable after later insertions. Excalidraw preserves unchanged element and embedded-file formatting, including unknown fields. Rebuilt plugin indexes discard stale offsets so subsequent edits address the correct bytes.
