import { expect, test, vi } from "vitest";
import { openStoredClientState } from "./client-state.js";
import type { LixSnapshotStorage } from "./types.js";

test("stored client state persists an absent snapshot and survives reopen", async () => {
	let snapshot: Uint8Array | undefined;
	const storage: LixSnapshotStorage = {
		load: async () => snapshot?.slice(),
		save: async (_namespace, nextSnapshot) => {
			snapshot = nextSnapshot.slice();
		},
	};

	const first = await openStoredClientState({
		storage,
		namespace: "remote:https://example.com/workspace",
	});
	expect(first.get("atelier")).toBeUndefined();
	await first.set("atelier", { focusedPanel: "right" });
	await first.close();
	expect(new TextDecoder().decode(snapshot).startsWith("lix-client-state-v1\n"))
		.toBe(true);

	const reopened = await openStoredClientState({
		storage,
		namespace: "remote:https://example.com/workspace",
	});
	expect(reopened.get("atelier")).toEqual({ focusedPanel: "right" });
	await reopened.close();
});

test("stored client state rejects a missing v1 header without resetting storage", async () => {
	const original = new TextEncoder().encode(
		'[["preserved",{"focusedPanel":"right"}]]',
	);
	const snapshot = original.slice();
	const save = vi.fn(async () => undefined);
	const storage: LixSnapshotStorage = {
		load: async () => snapshot,
		save,
	};

	await expect(
		openStoredClientState({
			storage,
			namespace: "remote:https://example.com/workspace",
		}),
	).rejects.toThrow("Stored Lix client state header is invalid");
	expect(save).not.toHaveBeenCalled();
	expect(snapshot).toEqual(original);
});

test("stored client state rejects a corrupt v1 header without resetting storage", async () => {
	const corrupt = new TextEncoder().encode(
		'lix-client-state-v1\n[["preserved",true]]',
	);
	corrupt[4] ^= 0xff;
	const original = corrupt.slice();
	const snapshot = corrupt.slice();
	const save = vi.fn(async () => undefined);
	const storage: LixSnapshotStorage = {
		load: async () => snapshot,
		save,
	};

	await expect(
		openStoredClientState({
			storage,
			namespace: "remote:https://example.com/workspace",
		}),
	).rejects.toThrow("Stored Lix client state header is invalid");
	expect(save).not.toHaveBeenCalled();
	expect(snapshot).toEqual(original);
});

test("stored client state rejects invalid utf-8 without resetting storage", async () => {
	const prefix = new TextEncoder().encode('lix-client-state-v1\n[["key","');
	const suffix = new TextEncoder().encode('"]]');
	const snapshot = new Uint8Array(prefix.length + 1 + suffix.length);
	snapshot.set(prefix);
	snapshot[prefix.length] = 0xff;
	snapshot.set(suffix, prefix.length + 1);
	const original = snapshot.slice();
	const save = vi.fn(async () => undefined);
	const storage: LixSnapshotStorage = {
		load: async () => snapshot,
		save,
	};

	await expect(
		openStoredClientState({
			storage,
			namespace: "remote:https://example.com/workspace",
		}),
	).rejects.toThrow("Stored Lix client state is invalid");
	expect(save).not.toHaveBeenCalled();
	expect(snapshot).toEqual(original);
});
