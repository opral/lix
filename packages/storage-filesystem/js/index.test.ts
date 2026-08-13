import { describe, expect, test } from "vitest";
import { FilesystemStorage } from "./index.js";

	describe("FilesystemStorage", () => {
	test("mirrors the Rust new(path) defaults", () => {
		const storage = new FilesystemStorage({ path: "./repository" });

		expect(storage.path).toBe("./repository");
		expect(storage.syncAllFiles).toBe(true);
		expect(storage.lixStorage.config).toEqual({
			kind: "filesystem",
			path: "./repository",
			syncAllFiles: true,
		});
	});

	test("rejects an empty path", () => {
		expect(
			() => new FilesystemStorage({ path: "" }),
		).toThrow("FilesystemStorage requires a non-empty path");
	});

	test("requires openLix before synchronization controls", async () => {
		const storage = new FilesystemStorage({ path: "./repository" });

		await expect(storage.syncDiskToLix()).rejects.toMatchObject({
			code: "LIX_FILESYSTEM_STORAGE_NOT_OPEN",
		});
	});
});
