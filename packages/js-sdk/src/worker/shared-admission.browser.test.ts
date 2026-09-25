import { expect, test } from "vitest";
import { LixWorkerClient } from "./client.js";
import { createRepositoryConnection } from "./factory.browser.js";
import { networkFetch } from "../http-transport.js";

test.runIf(import.meta.env.LIX_ADMISSION_REGRESSION === true)(
	"dedicated repository owner and HTTP admission survive rotation, reject account drift and keep offline local work",
	async () => {
		const name = "admission-regression-" + crypto.randomUUID();
		const physicalScope = `lix:opfs:${name}`;
		const url = `${location.origin}/lix/00000000-0000-7000-8000-000000000004`;
		const clients: LixWorkerClient[] = [];
		let admissions = 0;
		async function attach(initial: string, drop = false) {
			let token = initial;
			const connection = createRepositoryConnection(physicalScope);
			const client = new LixWorkerClient(connection,false);
			clients.push(client);
			client.beginLease(undefined, undefined, {
				url,
				headers: () => [["Authorization", token]],
				fetch: async (input, init) => {
					if (String(input).endsWith("/admission")) admissions++;
					const headers = new Headers(init?.headers);
					if(drop) headers.set("x-test-drop", "yes");
					return networkFetch(input,{...init,headers});
				},
			});
			// The binding is stubbed, but the real repository host still requires the
			// provider's physical identity for durable local admission proofs.
			await client.request({
				kind: "open",
				storage: {
					kind: "jsStorage",
					moduleUrl: new URL(
						"../../admission-regression-binding.ts",
						import.meta.url,
					).href,
					options: { sharedEngineKey: physicalScope },
				},
				telemetryEnabled: false,
				progressEnabled: false,
				server:{url,headers:[],dynamicHeaders:true},
			});
			return {
				client,
				token(value: string) {
					token = value;
				},
				execute(sql: string) {
					return client.request<any>({ kind: "execute", sql, params: [] });
				},
			};
		}
		try {
			const first = await attach("Bearer initial");
			const admissionAtOpen = admissions;
			for (let i = 0; i < 5; i++) {
				await expect(first.execute("cancel")).rejects.toMatchObject({
					name: "AbortError",
				});
				expect((await first.execute("remote")).rows).toEqual([
					["Bearer initial"],
				]);
			}
			expect(admissions).toBe(admissionAtOpen);
			const same = await attach("Bearer initial");
			const rotated = await attach("Bearer rotated");
			expect(admissions).toBe(3);
			await expect(attach("Bearer other")).rejects.toMatchObject({
				code: "LIX_SHARED_ENGINE_IDENTITY_MISMATCH",
			});
			await first.execute("write:durable-local-work");
			first.token("Bearer offline-refresh");
			expect((await first.execute("remote")).rows).toEqual([
				["Bearer initial"],
			]);
			expect((await first.execute("read")).rows).toEqual([
				["durable-local-work"],
			]);
			first.token("Bearer same-account-refreshed");
			expect((await first.execute("remote")).rows).toEqual([
				["Bearer same-account-refreshed"],
			]);
			await same.client.terminate();
			await rotated.client.terminate();
			first.token("Bearer offline-refresh");
			await expect(first.execute("remote")).rejects.toMatchObject({
				code: "LIX_IDENTITY_UNVERIFIED_OFFLINE",
			});
			expect((await first.execute("read")).rows).toEqual([
				["durable-local-work"],
			]);
			const cached = await attach("Bearer initial", true);
			expect((await cached.execute("read")).rows).toEqual([
				["durable-local-work"],
			]);
			await expect(attach("Bearer offline-new")).rejects.toMatchObject({
				code: "LIX_IDENTITY_UNVERIFIED_OFFLINE",
			});
			await expect(attach("Bearer denied")).rejects.toMatchObject({
				code: "LIX_ADMISSION_AUTH_REJECTED",
			});
		} finally {for(const client of clients) await client.terminate();}
	},
	30000,
);

test.runIf(import.meta.env.LIX_ADMISSION_REGRESSION === true)(
	"dedicated repository owner retains Rust authority progress and report from admission",
	async () => {
		const physicalScope = `lix:opfs:admission-upgrade-${crypto.randomUUID()}`;
		const url = `${location.origin}/lix/00000000-0000-7000-8000-000000000004`;
		const client = new LixWorkerClient(
			createRepositoryConnection(physicalScope),
			false,
		);
		const events: import("../types.js").LixOpenProgress[] = [];
		client.beginLease(
			undefined,
			undefined,
			{
				url,
				headers: [["Authorization", `Bearer upgrading-${crypto.randomUUID()}`]],
			},
			(event) => events.push(event),
		);
		try {
			const report = await client.request<import("../types.js").LixOpenReport>({
				kind: "open",
				storage: {
					kind: "jsStorage",
					moduleUrl: new URL(
						"../../admission-regression-binding.ts",
						import.meta.url,
					).href,
					options: { sharedEngineKey: physicalScope },
				},
				telemetryEnabled: false,
				progressEnabled: true,
				server:{url,headers:[],dynamicHeaders:true},
			});
			expect(report.migrations).toEqual([
				{ scope: "authority", fromFormat: 80, toFormat: 83 },
			]);
			expect(
				events.some(
					(event) => event.scope === "authority" && event.phase === "migrating",
				),
			).toBe(true);
			expect(
				events.some(
					(event) => event.scope === "authority" && event.phase === "complete",
				),
			).toBe(true);
		} finally {
			await client.terminate();
		}
	},
	30_000,
);
