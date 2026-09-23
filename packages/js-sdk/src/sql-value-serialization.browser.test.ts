import { expect, test } from "vitest";

test("serializes SQL integers and JSONB numbers as JavaScript numbers", async () => {
	const { openLix, Value } = await import("@lix-js/sdk");
	const lix = await openLix();
	try {
		const integers = await lix.execute(
			"SELECT 9007199254740991::BIGINT AS upper, -9007199254740991::BIGINT AS lower",
		);
		expect(integers.rows[0]).toEqual({
			upper: Number.MAX_SAFE_INTEGER,
			lower: Number.MIN_SAFE_INTEGER,
		});

		const json = {
			safe: Number.MAX_SAFE_INTEGER,
			decimal: 1.25,
			nested: [2, { ok: true }],
			nullish: null,
		};
		const jsonb = await lix.execute("SELECT $1 AS payload", [json]);
		expect(jsonb.rows[0]?.payload).toEqual(json);

		const real = await lix.execute("SELECT $1 AS value", [Value.real(1e20)]);
		expect(real.rows[0]?.value).toBe(1e20);

		await expect(
			lix.execute("SELECT 9007199254740992::BIGINT AS value"),
		).rejects.toThrow(/safe integer range/);
	} finally {
		await lix.close();
	}
});
