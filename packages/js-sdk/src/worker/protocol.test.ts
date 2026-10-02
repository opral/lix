import { expect, test } from "vitest";
import { deserializeWorkerError, serializeWorkerError } from "./protocol.js";

test("worker errors preserve safe native source origin and bounded causes", () => {
	const nativeError = Object.assign(new Error("table lookup failed"), {
		name: "LixError",
		code: "LIX_TABLE_NOT_FOUND",
		rustOrigin: {
			kind: "source_location",
			file: "packages/lix/src/sql2/error.rs",
			line: 72,
			column: 9,
		},
		rustStacktraceStatus: "not_captured",
	});
	const applicationError = new Error("SQL request failed", {
		cause: nativeError,
	});

	const restored = deserializeWorkerError(serializeWorkerError(applicationError));
	const restoredCause = restored.cause as Error & {
		code?: string;
		rustOrigin?: unknown;
		rustStacktraceStatus?: unknown;
	};

	expect(restored.message).toBe("SQL request failed");
	expect(restoredCause.code).toBe("LIX_TABLE_NOT_FOUND");
	expect(restoredCause.rustOrigin).toEqual({
		kind: "source_location",
		file: "packages/lix/src/sql2/error.rs",
		line: 72,
		column: 9,
	});
	expect(restoredCause.rustStacktraceStatus).toBe("not_captured");
});

test("worker error serialization omits unsafe or malformed Rust origins", () => {
	const error = Object.assign(new Error("native failure"), {
		rustOrigin: {
			kind: "source_location",
			file: "/home/runner/private/packages/lix/src/error.rs",
			line: 12,
			column: 2,
		},
		rustStacktraceStatus: "captured",
	});

	const restored = deserializeWorkerError(serializeWorkerError(error)) as Error & {
		rustOrigin?: unknown;
		rustStacktraceStatus?: unknown;
	};

	expect(restored.rustOrigin).toBeUndefined();
	expect(restored.rustStacktraceStatus).toBeUndefined();
});
