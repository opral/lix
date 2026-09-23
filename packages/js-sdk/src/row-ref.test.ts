import { readFileSync } from "node:fs";
import { expect, test } from "vitest";
import { decodeRowRef, type RowRefParts } from "./index.js";

const fixtures = JSON.parse(
	readFileSync(new URL("../../lix/tests/fixtures/row_ref_v2.json", import.meta.url), "utf8"),
) as Array<{ name: string; ref: string; parts: RowRefParts }>;

test.each(fixtures)("decodes Rust-generated $name reference", ({ ref, parts }) => {
	expect(decodeRowRef(ref)).toEqual(parts);
});

test("rejects malformed and noncanonical references", () => {
	const valid = fixtures[0].ref;
	for (const ref of [
		"plain text",
		valid.replace("v2:", "v1:"),
		`${valid}=`,
		`${valid}!`,
		`${valid}A`,
	]) {
		expect(() => decodeRowRef(ref)).toThrow(TypeError);
	}
	// Mutating the final sextet changes only unused bits of this one-byte tail.
	const scoped = fixtures.find((fixture) => fixture.name === "scoped")!.ref;
	const payload = scoped.slice("lix_row_ref:v2:".length);
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
	const last = alphabet.indexOf(payload.at(-1)!);
	if (payload.length % 4 === 2) {
		const altered = alphabet[(last & 0b110000) | ((last + 1) & 0b001111)];
		expect(() => decodeRowRef(scoped.slice(0, -1) + altered)).toThrow(TypeError);
	}
});

test("rejects truncated payloads and unknown component tags", () => {
	const valid = fixtures.find((fixture) => fixture.name === "negative")!.ref;
	const payload = valid.slice("lix_row_ref:v2:".length);
	const binary = atob(payload.replace(/-/g, "+").replace(/_/g, "/"));
	const encode = (value: string) =>
		"lix_row_ref:v2:" + btoa(value).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
	expect(() => decodeRowRef(encode(binary.slice(0, -1)))).toThrow(TypeError);
	const bad = binary.slice(0, -9) + String.fromCharCode(255) + binary.slice(-8);
	expect(() => decodeRowRef(encode(bad))).toThrow(TypeError);
});
