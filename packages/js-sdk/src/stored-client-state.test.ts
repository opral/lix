import { expect, test } from "vitest";
import { openStoredClientState } from "./client-state.js";
import type { LixSnapshotStorage } from "./types.js";

test("stored client state replaces an unsupported legacy snapshot", async () => {
	let snapshot: Uint8Array | undefined = new TextEncoder().encode(
		"SQLite format 3",
	);
	const storage: LixSnapshotStorage = {
		load: async () => snapshot,
		save: async (_namespace, nextSnapshot) => {
			snapshot = nextSnapshot.slice();
		},
	};

	const first = await openStoredClientState({
		storage,
		namespace: "remote:https://example.com/workspace",
	});
	expect(first.get("legacy")).toBeUndefined();
	await first.set("atelier", { focusedPanel: "right" });
	await first.close();

	const reopened = await openStoredClientState({
		storage,
		namespace: "remote:https://example.com/workspace",
	});
	expect(reopened.get("atelier")).toEqual({ focusedPanel: "right" });
	await reopened.close();
});
