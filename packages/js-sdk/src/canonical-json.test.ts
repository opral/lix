import { expect, test } from "vitest";

import {
	canonicalizeJsonValue,
	encodeCanonicalJson,
	parseCanonicalJson,
	type LixJsonValue,
} from "./canonical-json.js";

test("encodeCanonicalJson sorts object keys recursively", () => {
	const value: LixJsonValue = {
		z: { b: 2, a: 1 },
		a: [{ d: 4, c: 3 }],
	};

	expect(encodeCanonicalJson(value)).toBe(
		'{"a":[{"c":3,"d":4}],"z":{"a":1,"b":2}}',
	);
});

test("parseCanonicalJson preserves scalar and array payloads", () => {
	expect(parseCanonicalJson('"hello"')).toBe("hello");
	expect(parseCanonicalJson("[3,2,1]")).toEqual([3, 2, 1]);
});

test("parseCanonicalJson rejects invalid JSON text", () => {
	expect(() => parseCanonicalJson("{not-json}", "snapshot_content")).toThrow(
		/snapshot_content must be valid canonical JSON text/,
	);
});

test("canonicalizeJsonValue leaves array order intact", () => {
	expect(canonicalizeJsonValue([{ b: 2, a: 1 }, "x", 1])).toEqual([
		{ a: 1, b: 2 },
		"x",
		1,
	]);
});
