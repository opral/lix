# Text plugin task

Implement a small but executable text plugin against the assigned candidate
facade. Put the submission and its tests in the assigned workspace; do not edit
the candidate SDK.

The supported format is UTF-8 text split into logical lines. A terminal newline
is represented by a final empty line. Each line entity has a stable ID and an
order value in its snapshot.

Tests must prove all of the following:

1. Opening `alpha\nbeta\nalpha\n` against four existing line IDs (the fourth
   identifies the terminal empty line) preserves all four IDs.
2. Changing only `beta` to `BETA` emits exactly one upsert for the same ID.
3. Deleting the final duplicate `alpha` tombstones that non-empty line's ID,
   not the first duplicate's ID or the terminal empty line's ID.
4. Inserting a line allocates exactly one retry-stable ID.
5. Reordering two existing lines preserves both IDs.
6. Applying one committed entity change produces the exact expected bytes.
7. A failed or discarded transition does not change the accepted base view for
   APIs that retain document or checkpoint state.

Keep the implementation proportional to this task. Run its tests and leave the
workspace in a state an independent judge can rerun.
