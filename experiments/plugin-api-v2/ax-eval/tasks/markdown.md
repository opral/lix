# Markdown plugin task

Implement a small but executable Markdown plugin against the assigned candidate
facade. Put the submission and its tests in the assigned workspace; do not edit
the candidate SDK.

The supported subset is ATX headings and paragraphs separated by blank lines.
Each block has a stable ID and order value. A paragraph and heading with the
same logical content are compatible kinds and may retain identity.

Tests must prove all of the following:

1. Opening `# Title\n\nSame\n\nSame\n` against three existing block IDs
   preserves both duplicate paragraph IDs.
2. Editing only the second duplicate emits exactly one upsert for its existing
   ID.
3. Moving a paragraph before the heading preserves its ID.
4. Changing a paragraph into a heading preserves its compatible ID.
5. Copying a paragraph allocates exactly one retry-stable ID rather than
   aliasing the original.
6. Deleting a block tombstones only the acknowledged matched ID.
7. Applying one committed block change produces exact Markdown bytes.
8. A failed or discarded transition does not mutate the accepted base view for
   APIs that retain document or checkpoint state.

Keep the implementation proportional to this task; a full CommonMark parser is
not required. Run its tests and leave the workspace in a state an independent
judge can rerun.

