/** Read-path profile; synthetic fixtures never touch a hosted repository. */
import { performance } from "node:perf_hooks";
import { openLix } from "../dist/index.js";

const sizes = (process.env.PROFILE_FILE_COUNTS ?? "16,1600")
	.split(",")
	.map(Number);
const samples = Number(process.env.PROFILE_SAMPLES ?? 5);
if (
	sizes.some((n) => !Number.isSafeInteger(n) || n < 1 || n > 100000) ||
	!Number.isSafeInteger(samples) ||
	samples < 1 ||
	samples > 100
)
	throw new Error("Invalid profile dimensions");
const results = [];
for (const files of sizes) {
	const lix = await openLix();
	try {
		for (let offset = 0; offset < files; offset += 100) {
			const values = Array.from(
				{ length: Math.min(100, files - offset) },
				(_, i) => `('/fixture/f${offset + i}.txt',$1)`,
			).join(",");
			await lix.execute(`INSERT INTO lix_file(path,content) VALUES ${values}`, [
				new TextEncoder().encode("unrelated"),
			]);
		}
		const path = "/fixture/selected.csv";
		const content = new TextEncoder().encode(
			"name,value\n" + "example,42\n".repeat(9600),
		);
		const inserted = await lix.execute(
			"INSERT INTO lix_file(path,content) VALUES ($1,$2) RETURNING id",
			[path, content],
		);
		const id = inserted.rows[0].id;
		const queries = [
			["metadata", "SELECT id,path FROM lix_file WHERE id=$1", [id]],
			[
				"prepared",
				"SELECT id,path,lixcol_change_id,lix_active_branch_commit_id() AS commit_id,octet_length(content) AS size,content FROM lix_file WHERE path=$1",
				[path],
			],
			[
				"editor",
				"SELECT id,path,content,lixcol_metadata FROM lix_file WHERE id=$1 LIMIT 1",
				[id],
			],
		];
		const timings = [];
		for (let sample = 0; sample < samples; sample++)
			for (const [phase, sql, params] of queries) {
				const start = performance.now();
				const result = await lix.execute(sql, params);
				if (result.rows.length !== 1)
					throw new Error("Expected exactly one selected file");
				timings.push({
					sample,
					phase,
					ms: Number((performance.now() - start).toFixed(3)),
				});
			}
		results.push({ files, selectedBytes: content.length, timings });
	} finally {
		await lix.close();
	}
}
console.log(
	JSON.stringify(
		{
			profile: "document-open-synthetic-memory",
			note: "First query after fixture creation and subsequent cached reads; excludes HTTP, OPFS, and rendering.",
			results,
		},
		null,
		2,
	),
);
