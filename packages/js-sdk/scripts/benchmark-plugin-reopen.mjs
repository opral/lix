import { performance } from "node:perf_hooks";

import { bundledPluginArchives, openLix } from "../dist/index.js";

const warmupIterations = Number(process.env.WARMUP_ITERATIONS ?? 2);
const measuredIterations = Number(process.env.MEASURED_ITERATIONS ?? 12);
const archives = await bundledPluginArchives();
const csvPlugin = archives.find((plugin) => plugin.key === "plugin_csv_v2");
if (!csvPlugin) throw new Error("expected bundled CSV plugin");

for (let index = 0; index < warmupIterations; index += 1) {
	await runCycle(index);
}

const durations = [];
for (let index = 0; index < measuredIterations; index += 1) {
	const start = performance.now();
	await runCycle(index);
	durations.push(performance.now() - start);
}

console.log(
	JSON.stringify(
		{
			warmupIterations,
			measuredIterations,
			p50Ms: percentile(durations, 0.5),
			p95Ms: percentile(durations, 0.95),
			minMs: Math.min(...durations),
			maxMs: Math.max(...durations),
			durationsMs: durations,
		},
		null,
		2,
	),
);

async function runCycle(index) {
	const lix = await openLix();
	try {
		await lix.execute(
			"INSERT INTO lix_file (path, data) VALUES ($1, $2)",
			[`/.lix/plugins/${csvPlugin.key}.lixplugin`, csvPlugin.archiveBytes],
		);
		await lix.execute(
			"INSERT INTO lix_file (path, data) VALUES ($1, $2)",
			[
				`/benchmark-${index}.csv`,
				new TextEncoder().encode("name,age\nAda,36\nGrace,37\n"),
			],
		);
		const result = await lix.execute("SELECT count(*) AS count FROM csv_v2_row");
		if (result.rows[0]?.get("count") !== 3) {
			throw new Error("CSV plugin benchmark returned unexpected rows");
		}
	} finally {
		await lix.close();
	}
}

function percentile(values, quantile) {
	const sorted = [...values].sort((left, right) => left - right);
	return sorted[Math.ceil(sorted.length * quantile) - 1];
}
