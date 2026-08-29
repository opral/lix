import { expect, test, vi } from "vitest";
import type { BindingExecuteResult, LixBinding } from "./binding-types.js";
import { authoritativeHotBinding } from "./authoritative-hot-binding.js";

const result = (value: string): BindingExecuteResult => ({
	columns: [{ name: "value", type: "text" }],
	rows: [[{ kind: "text", value }]],
	rowsAffected: 0,
	notices: [],
});

const coordinate = (
	head: string,
	checkpoint = "checkpoint",
): BindingExecuteResult => ({
	columns: [
		{ name: "head_commit_id", type: "text" },
		{ name: "checkpoint_commit_id", type: "text" },
	],
	rows: [
		[
			{ kind: "text", value: head },
			{ kind: "text", value: checkpoint },
		],
	],
	rowsAffected: 0,
	notices: [],
});

function authorityRequired(kind: "history" | "mutation") {
	return Object.assign(new Error(`${kind} belongs to authority`), {
		name: "LixError",
		code: "LIX_AUTHORITY_EXECUTION_REQUIRED",
		details: { executionKind: kind },
	});
}

test("keeps hot reads local and routes history without hydrating the replica", async () => {
	const localExecute = vi.fn(async (sql: string) => {
		if (sql.includes("lix_history")) throw authorityRequired("history");
		return result("local-hot");
	});
	const authorityExecute = vi.fn(async () => result("authority-history"));
	const binding = authoritativeHotBinding(
		{ execute: localExecute } as unknown as LixBinding,
		{ execute: authorityExecute } as unknown as LixBinding,
	);

	await expect(binding.execute("SELECT * FROM lix_file", [])).resolves.toEqual(
		result("local-hot"),
	);
	await expect(
		binding.execute("SELECT * FROM lix_history('lix_file')", []),
	).resolves.toEqual(result("authority-history"));
	expect(authorityExecute).toHaveBeenCalledOnce();
});

test("does not release an authority mutation before its certified hot coordinate installs", async () => {
	let localCoordinateReads = 0;
	const localExecute = vi.fn(async (sql: string) => {
		if (sql.startsWith("UPDATE")) throw authorityRequired("mutation");
		localCoordinateReads += 1;
		return coordinate(localCoordinateReads < 2 ? "old-head" : "new-head");
	});
	const authorityExecute = vi.fn(async (sql: string) =>
		sql.startsWith("UPDATE") ? result("updated") : coordinate("new-head"),
	);
	const binding = authoritativeHotBinding(
		{ execute: localExecute } as unknown as LixBinding,
		{ execute: authorityExecute } as unknown as LixBinding,
	);

	await expect(
		binding.execute("UPDATE lix_file SET path = '/b' WHERE path = '/a'", []),
	).resolves.toEqual(result("updated"));
	expect(localCoordinateReads).toBe(2);
	expect(authorityExecute).toHaveBeenCalledTimes(3);
});
