import { expect, test, vi } from "vitest";
import { openLix } from "./remote.js";

test("the remote-only entrypoint opens the same Lix facade", async () => {
	const remoteFetch = vi.fn(async () =>
		Response.json({ protocolVersion: 1, activeBranchId: "main-id" }),
	);
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/@acme/workspace",
			fetch: remoteFetch,
		},
	});

	expect(await lix.activeBranchId()).toBe("main-id");
	expect(remoteFetch).toHaveBeenCalledTimes(2);
	await lix.close();
});

test("the remote-only entrypoint requires a server", async () => {
	await expect(openLix({} as never)).rejects.toThrow("requires a remote server");
});
