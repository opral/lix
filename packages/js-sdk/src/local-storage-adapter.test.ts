import { describe, expect, test } from "vitest";
import { LocalStorage, type WebStorageLike } from "./local-storage-adapter.js";

class MemoryWebStorage implements WebStorageLike {
	readonly values = new Map<string, string>();

	getItem(key: string): string | null {
		return this.values.get(key) ?? null;
	}

	setItem(key: string, value: string): void {
		this.values.set(key, value);
	}
}

describe("LocalStorage snapshot adapter", () => {
	test("round-trips opaque snapshot bytes", async () => {
		const webStorage = new MemoryWebStorage();
		const storage = new LocalStorage({ storage: webStorage });
		const snapshot = new Uint8Array([0, 1, 2, 127, 128, 255]);

		expect(await storage.load("workspace-a")).toBeUndefined();
		await storage.save("workspace-a", snapshot);

		expect(await storage.load("workspace-a")).toEqual(snapshot);
		expect(webStorage.values.size).toBe(1);
	});

	test("isolates namespaces and encodes namespace characters in keys", async () => {
		const webStorage = new MemoryWebStorage();
		const storage = new LocalStorage({
			storage: webStorage,
			prefix: "test-lix",
		});

		await storage.save("https://example.com/acme/a", new Uint8Array([1]));
		await storage.save("https://example.com/acme/b", new Uint8Array([2]));

		expect(await storage.load("https://example.com/acme/a")).toEqual(
			new Uint8Array([1]),
		);
		expect(await storage.load("https://example.com/acme/b")).toEqual(
			new Uint8Array([2]),
		);
		expect([...webStorage.values.keys()]).toEqual([
			"test-lix:https%3A%2F%2Fexample.com%2Facme%2Fa",
			"test-lix:https%3A%2F%2Fexample.com%2Facme%2Fb",
		]);
	});

	test("rejects malformed and unsupported stored records", async () => {
		const webStorage = new MemoryWebStorage();
		const storage = new LocalStorage({
			storage: webStorage,
			prefix: "test-lix",
		});
		const key = "test-lix:broken";

		webStorage.setItem(key, "not-json");
		await expect(storage.load("broken")).rejects.toThrow("not valid JSON");

		webStorage.setItem(
			key,
			JSON.stringify({ format: "lix-snapshot", version: 2, data: "AQ==" }),
		);
		await expect(storage.load("broken")).rejects.toThrow(
			"unsupported format or version",
		);

		webStorage.setItem(
			key,
			JSON.stringify({ format: "lix-snapshot", version: 1, data: "***=" }),
		);
		await expect(storage.load("broken")).rejects.toThrow("invalid base64");
	});

	test("propagates storage write failures", async () => {
		const storage = new LocalStorage({
			storage: {
				getItem: () => null,
				setItem: () => {
					throw new Error("quota exceeded");
				},
			},
		});

		await expect(
			storage.save("workspace", new Uint8Array([1])),
		).rejects.toThrow("quota exceeded");
	});

	test("validates adapter options, namespaces, and snapshots", async () => {
		expect(() => new LocalStorage({ prefix: "" } as never)).toThrow(
			"prefix must be a non-empty string",
		);
		const storage = new LocalStorage({ storage: new MemoryWebStorage() });
		await expect(storage.load("")).rejects.toThrow(
			"namespace must be a non-empty string",
		);
		await expect(storage.save("workspace", [1, 2] as never)).rejects.toThrow(
			"snapshot must be a Uint8Array",
		);
	});
});
