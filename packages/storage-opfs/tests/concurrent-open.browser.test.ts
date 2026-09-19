import { OpfsStorage as RpcTestStorage } from "./rpc-test-storage.js";
import { expect, test } from "vitest";
import { openLix } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";
test("concurrent cold opens share one physical OPFS owner", async () => {
	const relays = [0, 1, 2].map(
		() =>
			new Worker(new URL("./.generated/owner.js", import.meta.url), {
				type: "module",
			}),
	);
	try {
		for (let attempt = 0; attempt < 5; attempt++) {
			const name=`concurrent-cold-${crypto.randomUUID()}`;
			const results=await Promise.allSettled([0,1,2,3].map(()=>openLix({storage:new OpfsStorage({name})})));
			try {
				expect(
					results.map((result) => result.status),
					`attempt ${attempt}: ${results.map((r) => (r.status === "rejected" ? String(r.reason) : "ready")).join(", ")}`,
				).toEqual(Array(4).fill("fulfilled"));
			} finally {
				await Promise.all(
					results.flatMap((result) =>
						result.status === "fulfilled" ? [result.value.close()] : [],
					),
				);
			}
		}
	} finally {
		relays.forEach((worker) => worker.terminate());
	}
}, 120000);
test("a losing cold initializer admits the completed winning seed", async () => {
	const name=`seed-race-${crypto.randomUUID()}`;
	const base = new RpcTestStorage({ name }).lixStorage;
	const gate=new BroadcastChannel(`${name}-gate`);
	const paused = new Promise<void>((resolve) => {
		gate.onmessage = (event) => {
			if (event.data === "seed-paused") resolve();
		};
	});
	const slow = openLix({
		storage: {
			lixStorage: {
				version: 3,
				moduleUrl: new URL("./concurrent-seed-provider.ts", import.meta.url)
					.href,
				options: {
					moduleUrl: base.moduleUrl,
					providerOptions: base.options,
					gate: `${name}-gate`,
				},
			},
		},
	});
	// Observe the failure immediately while retaining its result for assertions.
	const slowResult = slow.then(
		(value) => ({ value }),
		(error) => ({ error }),
	);
	let winner;
	try {
		await paused;
		winner = await openLix({ storage: new RpcTestStorage({ name }) });
		await winner.execute("INSERT INTO lix_key_value (key,value) VALUES ('winner-only','preserved')");
		gate.postMessage("resume");
		const result = await slowResult;
		if ("error" in result) throw result.error;
		expect(await result.value.activeBranchId()).toBe(await winner.activeBranchId());
		expect((await result.value.execute("SELECT value FROM lix_key_value WHERE key = 'winner-only'")).rows).toEqual([{value:"preserved"}]);
	} finally {
		gate.postMessage("resume");
		const result = await slowResult;
		await Promise.all([
			winner?.close(),
			...("value" in result ? [result.value.close()] : []),
		]);
		gate.close();
	}
}, 30000);
