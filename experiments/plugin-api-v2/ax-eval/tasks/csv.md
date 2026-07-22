# CSV plugin task

Implement a small but executable CSV plugin against the assigned candidate
facade. Put the submission and its tests in the assigned workspace; do not edit
the candidate SDK.

The supported subset is UTF-8 comma-separated records with RFC-style doubled
quotes, quoted newlines, LF or CRLF record endings, and stable row IDs. Row
order is part of rendering but not the ID.

Tests must prove all of the following:

1. Opening `name,note\na,"one\ntwo"\nb,same\nb,same\n` against existing row IDs
   preserves every ID, including both identical `b,same` rows.
2. Editing one cell in only the second duplicate emits exactly one upsert for
   that duplicate's existing ID.
3. Inserting a row allocates exactly one retry-stable ID.
4. Reordering existing rows preserves their IDs.
5. Removing one duplicate tombstones only its matched ID.
6. A quoted multiline edit is parsed as one row rather than several lines.
7. Applying one committed row change produces exact CSV bytes and does not
   silently change the configured line ending.
8. A failed or discarded transition does not mutate the accepted base view for
   APIs that retain document or checkpoint state.

Keep the implementation proportional to this task. Run its tests and leave the
workspace in a state an independent judge can rerun.

