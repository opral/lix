import { expect, test } from "vitest";
import { createWasmSqliteBackend } from "./backend/wasm-sqlite.js";
import { isLixError, type LixError } from "./engine-wasm/index.js";
import { openLix } from "./open-lix.js";

test("engine errors cross the wasm boundary as structured LixError instances", async () => {
	const backend = await createWasmSqliteBackend();

	let thrown: unknown;
	try {
		await openLix({ backend });
	} catch (err) {
		thrown = err;
	}

	expect(thrown).toBeInstanceOf(Error);
	expect(isLixError(thrown)).toBe(true);
	const err = thrown as LixError;
	expect(err.code).toBe("LIX_ERROR_NOT_INITIALIZED");
	expect(typeof err.message).toBe("string");
	expect(err.message.length).toBeGreaterThan(0);
	// No engine call attaches a hint to this error yet (Plan 401 populates
	// hints at specific sites). The field is optional on the type, so its
	// absence is the expected current state.
	expect(err.hint).toBeUndefined();
});

test("isLixError recognizes errors carrying a hint field", () => {
	const synthetic = Object.assign(new Error("boom"), {
		code: "LIX_ERROR_UNSUPPORTED_WRITE_EXPRESSION",
		hint: "use lix_json('...') instead",
	});

	expect(isLixError(synthetic)).toBe(true);
	expect((synthetic as LixError).hint).toBe("use lix_json('...') instead");
});

test("isLixError rejects plain errors and non-LIX codes", () => {
	expect(isLixError(new Error("plain"))).toBe(false);
	expect(isLixError({ code: "LIX_ERROR_FOO", message: "x" })).toBe(false); // not Error instance
	expect(
		isLixError(Object.assign(new Error("x"), { code: "OTHER_ERROR" })),
	).toBe(false);
	expect(isLixError(null)).toBe(false);
	expect(isLixError(undefined)).toBe(false);
});
