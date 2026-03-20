/**
 * Returns the difference between two versions for the version_change table.
 *
 * The difference is the set of changes that exist in version `a` but not in version `b`.
 * Modeled after https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Set/difference
 *
 * @example
 *   ```ts
 *   await lix.db.selectFrom("version_change")
 *     .where(versionChangeInDifference(a: versionA, b: versionB))
 *     .selectAll()
 *     .execute();
 *   ```
 */
export function versionChangeInDifference(a, b) {
    return (eb) => eb("version_change.change_id", "in", (subquery) => subquery
        .selectFrom("version_change as A")
        .leftJoin("version_change as B", (join) => join
        .onRef("A.change_id", "=", "B.change_id")
        .on("B.version_id", "=", b.id))
        .where("A.version_id", "=", a.id)
        .where("B.change_id", "is", null)
        .select("A.change_id"));
}
//# sourceMappingURL=version-change-in-difference.js.map