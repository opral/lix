import { afterEach, expect, test } from "vitest";
import { openLix, type Lix } from "@lix-js/sdk";
import { qb } from "../src/index.js";

const encoder = new TextEncoder();
let lix: Lix | undefined;

afterEach(async () => {
	if (lix) {
		await lix.close();
		lix = undefined;
	}
});

test("qb(lix).transaction works with openLix()", async () => {
	lix = await openLix();

	await qb(lix)
		.transaction()
		.execute(async (trx) => {
			await trx
				.insertInto("lix_file")
				.values({
					path: "/tx-basic.md",
					data: encoder.encode("ok"),
				})
				.execute();
		});

	const row = await qb(lix)
		.selectFrom("lix_file")
		.where("path", "=", "/tx-basic.md")
		.select(["path"])
		.executeTakeFirst();
	expect(row?.path).toBe("/tx-basic.md");
});

test("qb(lix) serializes concurrent transactions on one Lix instance", async () => {
	lix = await openLix();
	const wait = (ms: number) =>
		new Promise<void>((resolve) => setTimeout(resolve, ms));

	const txA = qb(lix)
		.transaction()
		.execute(async (trx) => {
			await trx
				.insertInto("lix_file")
				.values({
					path: "/tx-concurrent-a.md",
					data: encoder.encode("A"),
				})
				.execute();
			await wait(30);
		});

	const txB = qb(lix)
		.transaction()
		.execute(async (trx) => {
			await trx
				.insertInto("lix_file")
				.values({
					path: "/tx-concurrent-b.md",
					data: encoder.encode("B"),
				})
				.execute();
		});

	await Promise.all([txA, txB]);

	const rows = await qb(lix)
		.selectFrom("lix_file")
		.where("path", "in", ["/tx-concurrent-a.md", "/tx-concurrent-b.md"])
		.select(["path"])
		.execute();
	expect(rows.map((row) => row.path).sort()).toEqual([
		"/tx-concurrent-a.md",
		"/tx-concurrent-b.md",
	]);
});
