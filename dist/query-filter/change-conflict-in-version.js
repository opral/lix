/**
 * Filters if a conflict is in the given version.
 *
 * @example
 *   ```ts
 *   const conflicts = await lix.db.selectFrom("change_conflict")
 *      .where(changeConflictInVersion(currentVersion))
 *      .selectAll()
 *      .execute();
 *   ```
 */
export function changeConflictInVersion(version) {
    return (eb) => eb("change_conflict.id", "in", (subquery) => subquery
        .selectFrom("version_change_conflict")
        .select("change_conflict_id")
        .where("version_change_conflict.version_id", "=", version.id));
}
//# sourceMappingURL=change-conflict-in-version.js.map