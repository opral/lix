/**
 * Closes the lix.
 */
export async function closeLix(args) {
    await args.lix.db.destroy();
}
//# sourceMappingURL=close-lix.js.map