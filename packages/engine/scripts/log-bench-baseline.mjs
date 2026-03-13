import { mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "../../..");
const engineRoot = path.resolve(__dirname, "..");
const resultsDir = path.join(engineRoot, "results");
const outputPath = path.join(
	resultsDir,
	"lix-file-recursive-update.bench.json",
);

const benchCommand = [
	"cargo",
	"bench",
	"-p",
	"lix_engine",
	"--bench",
	"lix_file_recursive_update",
];

async function main() {
	await mkdir(resultsDir, { recursive: true });

	await runCommand(benchCommand[0], benchCommand.slice(1), { cwd: repoRoot });
	const gitCommit = (
		await runCommand("git", ["rev-parse", "HEAD"], { cwd: repoRoot, capture: true })
	).trim();

	const criterionRoot = path.join(
		repoRoot,
		"target",
		"criterion",
		"lix_file",
		"update_existing_row_deep_history",
	);

	const baselines = await collectEstimates(criterionRoot);
	if (baselines.length === 0) {
		throw new Error(
			`no Criterion estimates found under ${criterionRoot}; benchmark output layout may have changed`,
		);
	}

	const report = {
		bench: "lix_file_recursive_update",
		generated_at: new Date().toISOString(),
		git_commit: gitCommit,
		command: benchCommand.join(" "),
		results: baselines.sort((left, right) => left.depth - right.depth),
	};

	await writeFile(outputPath, `${JSON.stringify(report, null, 2)}\n`);
	console.log(`[bench] wrote baseline to ${path.relative(repoRoot, outputPath)}`);
}

async function collectEstimates(criterionRoot) {
	const entries = await readdir(criterionRoot, { withFileTypes: true });
	const out = [];

	for (const entry of entries) {
		if (!entry.isDirectory()) {
			continue;
		}
		const depth = Number(entry.name);
		if (!Number.isFinite(depth)) {
			continue;
		}
		const estimatesPath = path.join(
			criterionRoot,
			entry.name,
			"new",
			"estimates.json",
		);
		const estimates = JSON.parse(await readFile(estimatesPath, "utf8"));
		out.push({
			group: "lix_file",
			benchmark: "update_existing_row_deep_history",
			depth,
			unit: "ms",
			mean_ms: nsToMs(estimates.mean.point_estimate),
			mean_ci_lower_ms: nsToMs(estimates.mean.confidence_interval.lower_bound),
			mean_ci_upper_ms: nsToMs(estimates.mean.confidence_interval.upper_bound),
			median_ms: nsToMs(estimates.median.point_estimate),
			median_ci_lower_ms: nsToMs(estimates.median.confidence_interval.lower_bound),
			median_ci_upper_ms: nsToMs(estimates.median.confidence_interval.upper_bound),
			std_dev_ms: nsToMs(estimates.std_dev.point_estimate),
		});
	}

	return out;
}

function nsToMs(value) {
	return Number((value / 1_000_000).toFixed(3));
}

function runCommand(cmd, args, options = {}) {
	return new Promise((resolve, reject) => {
		const child = spawn(cmd, args, {
			cwd: options.cwd,
			stdio: options.capture ? ["ignore", "pipe", "inherit"] : "inherit",
			env: process.env,
		});

		let stdout = "";
		if (options.capture) {
			child.stdout.on("data", (chunk) => {
				stdout += chunk;
			});
		}

		child.on("error", reject);
		child.on("close", (code) => {
			if (code === 0) {
				resolve(stdout);
				return;
			}
			reject(new Error(`${cmd} ${args.join(" ")} failed with exit code ${code}`));
		});
	});
}

main().catch((error) => {
	console.error(`[bench] baseline logging failed: ${error.message}`);
	process.exitCode = 1;
});
