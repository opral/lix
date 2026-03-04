import { expect, test } from "vitest";
import init, {
	openLix as openLixWasm,
	resolveEngineWasmModuleOrPath,
	Value,
} from "./engine-wasm/index.js";
import { createWasmSqliteBackend } from "./backend/wasm-sqlite.js";
import { createNodeWasmRuntime } from "./wasm-runtime/node.js";
import type { LixRuntimeValue } from "./types.js";

type CanonicalValue =
	| { kind: "null"; value: null }
	| { kind: "bool"; value: boolean }
	| { kind: "int"; value: number }
	| { kind: "float"; value: number }
	| { kind: "text"; value: string }
	| { kind: "blob"; base64: string };

const DISALLOWED_NON_CANONICAL_KINDS = new Set([
	"Null",
	"Bool",
	"Boolean",
	"Integer",
	"Real",
	"Text",
	"Blob",
]);

function assertCanonicalValue(value: unknown): asserts value is CanonicalValue {
	expect(value).toBeTypeOf("object");
	expect(value).not.toBeNull();
	const kind = (value as { kind?: unknown }).kind;
	expect(typeof kind).toBe("string");
	expect(DISALLOWED_NON_CANONICAL_KINDS.has(String(kind))).toBe(false);
	expect(["null", "bool", "int", "float", "text", "blob"]).toContain(kind);
}

function decodeCanonicalToRuntime(value: CanonicalValue): LixRuntimeValue {
	switch (value.kind) {
		case "null":
			return null;
		case "bool":
		case "int":
		case "float":
		case "text":
			return value.value;
		case "blob":
			return Value.from(value).asBlob() ?? new Uint8Array();
	}
}

function encodeRuntimeToCanonical(value: LixRuntimeValue): CanonicalValue {
	if (value === null || value === undefined) {
		return { kind: "null", value: null };
	}
	if (typeof value === "boolean") {
		return { kind: "bool", value };
	}
	if (typeof value === "number") {
		return Number.isInteger(value)
			? { kind: "int", value }
			: { kind: "float", value };
	}
	if (typeof value === "string") {
		return { kind: "text", value };
	}
	if (value instanceof Uint8Array) {
		return Value.blob(value).toJSON() as CanonicalValue;
	}
	throw new Error(`unsupported runtime value: ${String(value)}`);
}

async function createCanonicalBoundaryLix() {
	const moduleOrPath = await resolveEngineWasmModuleOrPath();
	await init({ module_or_path: moduleOrPath });

	const runtimeBackend = await createWasmSqliteBackend();
	const beginTransaction = runtimeBackend.beginTransaction;
	if (typeof beginTransaction !== "function") {
		throw new Error("runtime backend beginTransaction() is required for this test");
	}
	const exportSnapshot = runtimeBackend.exportSnapshot;
	if (typeof exportSnapshot !== "function") {
		throw new Error("runtime backend exportSnapshot() is required for this test");
	}
	const backend = {
		dialect: "sqlite" as const,
		async execute(sql: string, params: CanonicalValue[]) {
			const runtimeParams = params.map(decodeCanonicalToRuntime);
			const result = await runtimeBackend.execute(sql, runtimeParams);
			return {
				rows: result.rows.map((row) => row.map(encodeRuntimeToCanonical)),
				columns: result.columns,
			};
		},
		async beginTransaction() {
			const tx = await beginTransaction();
			return {
				dialect: "sqlite" as const,
				async execute(sql: string, params: CanonicalValue[]) {
					const runtimeParams = params.map(decodeCanonicalToRuntime);
					const result = await tx.execute(sql, runtimeParams);
					return {
						rows: result.rows.map((row) =>
							row.map(encodeRuntimeToCanonical),
						),
						columns: result.columns,
					};
				},
				async commit() {
					await tx.commit();
				},
				async rollback() {
					await tx.rollback();
				},
			};
		},
		async exportSnapshot() {
			return exportSnapshot();
		},
	};

	const lix = await openLixWasm(backend as any, createNodeWasmRuntime(), undefined);
	return { lix, runtimeBackend };
}

test("execute emits canonical wire value kinds only", async () => {
	const { lix, runtimeBackend } = await createCanonicalBoundaryLix();
	try {
		const result = (await lix.execute(
			"SELECT 1 AS i, 1.5 AS f, 'abc' AS t, X'0102' AS b, NULL AS n",
			[],
			undefined,
		)) as { rows: unknown[][] };
		const row = result.rows[0]!;
		expect(row).toHaveLength(5);
		for (const cell of row) {
			assertCanonicalValue(cell);
		}
		expect((row[0] as CanonicalValue).kind).toBe("int");
		expect((row[1] as CanonicalValue).kind).toBe("float");
		expect((row[2] as CanonicalValue).kind).toBe("text");
		expect((row[3] as CanonicalValue).kind).toBe("blob");
		expect((row[4] as CanonicalValue).kind).toBe("null");
	} finally {
		try {
			lix.free();
		} finally {
			if (typeof runtimeBackend.close === "function") {
				await runtimeBackend.close();
			}
		}
	}
});

test("beginTransaction.execute emits canonical wire value kinds only", async () => {
	const { lix, runtimeBackend } = await createCanonicalBoundaryLix();
	try {
		const tx = await lix.beginTransaction(undefined);
		try {
			const result = (await tx.execute(
				"SELECT 2 AS i, 'x' AS t",
				[],
			)) as { rows: unknown[][] };
			const row = result.rows[0]!;
			for (const cell of row) {
				assertCanonicalValue(cell);
			}
			expect((row[0] as CanonicalValue).kind).toBe("int");
			expect((row[1] as CanonicalValue).kind).toBe("text");
		} finally {
			await tx.rollback();
		}
	} finally {
		try {
			lix.free();
		} finally {
			if (typeof runtimeBackend.close === "function") {
				await runtimeBackend.close();
			}
		}
	}
});

test("observe emits canonical wire value kinds only", async () => {
	const { lix, runtimeBackend } = await createCanonicalBoundaryLix();
	try {
		const events = lix.observe({
			sql: "SELECT 3 AS i, 'obs' AS t",
			params: [],
		});

		const first = (await events.next()) as
			| {
					rows?: { rows: unknown[][] };
			  }
			| undefined;
		expect(first).toBeDefined();
		const row = first!.rows!.rows[0]!;
		for (const cell of row) {
			assertCanonicalValue(cell);
		}
		expect((row[0] as CanonicalValue).kind).toBe("int");
		expect((row[1] as CanonicalValue).kind).toBe("text");
		events.close();
	} finally {
		try {
			lix.free();
		} finally {
			if (typeof runtimeBackend.close === "function") {
				await runtimeBackend.close();
			}
		}
	}
});
