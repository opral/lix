# Excalidraw plugin task

Implement a small but executable Excalidraw JSON plugin against the assigned
candidate facade. Put the submission and its tests in the assigned workspace;
do not edit the candidate SDK.

The supported subset has a top-level `type`, `elements`, `appState`, and `files`.
Elements use their native `id` as stable identity and their fractional `index`
for order. Lix entity deletion is distinct from Excalidraw's domain-level
`isDeleted` property.

Tests must prove all of the following:

1. Opening a scene with two existing element IDs preserves those native IDs.
2. Changing one element's `x` coordinate emits exactly one upsert for that ID.
3. Reordering elements by `index` preserves both IDs.
4. Copying an element with a new native ID creates one new entity.
5. Setting `isDeleted: true` is an element upsert, not a Lix tombstone; removing
   the element from an acknowledged file is a tombstone.
6. A binding edit that changes references on two elements is emitted atomically
   as one change set.
7. Changing one element does not copy an unchanged base64 file asset through
   the semantic change output.
8. Applying committed element changes produces valid scene JSON with exact
   native IDs and references.
9. A failed or discarded transition does not mutate the accepted base view for
   APIs that retain document or checkpoint state.

Keep the implementation proportional to this task. Run its tests and leave the
workspace in a state an independent judge can rerun.

