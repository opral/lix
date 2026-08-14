---
type: patch
---

Binary files now reuse storage across edits that shift bytes, such as rewriting an MP4's metadata, trimming a clip, or splicing audio.

The binary content store previously cut chunks at fixed 1 MiB offsets, so inserting or removing bytes anywhere in a file renamed every chunk after that point and the whole file was stored again. Chunk boundaries are now chosen from the content itself, so the boundaries resynchronise just past the edit and the rest of the file keeps the storage it already had. Chunk sizes still average 1 MiB, so file layout, seek behaviour, and row counts are unchanged.
