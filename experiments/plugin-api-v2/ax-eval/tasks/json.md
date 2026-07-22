# JSON plugin task

Implement a small but executable JSON plugin against the assigned candidate
facade. Put the submission and its tests in the assigned workspace; do not edit
the candidate SDK.

The supported subset is UTF-8 JSON objects, arrays, strings, booleans, null, and
numbers. Object slots use a stable identity derived from their stable parent and
decoded key. Array elements use opaque stable IDs plus order values; numeric
JSON Pointers are locators, not entity IDs.

Tests must prove all of the following:

1. Opening `{"a":0,"rows":[{"name":"x"},{"name":"x"}]}` against existing
   identities preserves both duplicate array-item IDs.
2. Changing only `a` emits exactly one upsert for the existing object-slot ID.
3. Inserting an element at array index zero allocates one retry-stable ID and
   preserves every prior array-element ID.
4. Moving an array element changes order without changing its ID.
5. Deleting a container emits tombstones for all acknowledged descendant IDs.
6. Keys containing `/` and `~` remain distinct and render correctly.
7. Applying one committed leaf change produces valid exact semantic JSON; key
   whitespace/order may be canonicalized consistently.
8. A failed or discarded transition does not mutate the accepted base view for
   APIs that retain document or checkpoint state.

Do not use an array index or full JSON Pointer as an array element's primary
key. Keep the implementation proportional to this task. Run its tests and
leave the workspace in a state an independent judge can rerun.

