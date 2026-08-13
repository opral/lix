#!/usr/bin/env node

import { createHash } from "node:crypto";
import { execFileSync, spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import process from "node:process";
import { setTimeout as delay } from "node:timers/promises";

const [crateName, expectedVersion] = process.argv.slice(2);
if (!crateName || !expectedVersion) {
	console.error("Usage: node scripts/publish-crate.mjs <crate> <expected-version>");
	process.exit(2);
}

const metadata = JSON.parse(
	execFileSync("cargo", ["metadata", "--format-version", "1", "--no-deps"], {
		encoding: "utf8",
	}),
);
const cargoPackage = metadata.packages.find((candidate) => candidate.name === crateName);
if (!cargoPackage) throw new Error(`${crateName} is not in the Cargo workspace`);
if (cargoPackage.version !== expectedVersion) {
	throw new Error(
		`${crateName}@${cargoPackage.version} does not match release version ${expectedVersion}`,
	);
}

const workspacePackageNames = new Set(
	metadata.packages.map((candidate) => candidate.name),
);
const waitsForLockstepDependency = cargoPackage.dependencies.some(
	(dependency) =>
		dependency.path !== null &&
		workspacePackageNames.has(dependency.name) &&
		dependency.req === `=${expectedVersion}`,
);

let packaged = false;
const packageAttempts = waitsForLockstepDependency ? 36 : 1;
for (let attempt = 1; attempt <= packageAttempts; attempt += 1) {
	const packageResult = spawnSync(
		"cargo",
		["package", "--locked", "-p", crateName],
		{ encoding: "utf8" },
	);
	if (packageResult.error) throw packageResult.error;
	if (packageResult.status === 0) {
		process.stdout.write(packageResult.stdout);
		process.stderr.write(packageResult.stderr);
		packaged = true;
		break;
	}

	if (attempt === packageAttempts) {
		process.stdout.write(packageResult.stdout);
		process.stderr.write(packageResult.stderr);
		throw new Error(
			`cargo package failed for ${crateName}@${expectedVersion} with status ${packageResult.status}`,
		);
	}

	console.warn(
		`${crateName}@${expectedVersion} is not packageable through Cargo's registry index yet; retrying in 5 seconds (${attempt}/${packageAttempts})`,
	);
	await delay(5_000);
}
if (!packaged) throw new Error(`cargo package did not run for ${crateName}`);

const cratePath = join(
	metadata.target_directory,
	"package",
	`${crateName}-${expectedVersion}.crate`,
);
const localChecksum = createHash("sha256")
	.update(readFileSync(cratePath))
	.digest("hex");

async function registryChecksum() {
	const response = await fetch(
		`https://crates.io/api/v1/crates/${encodeURIComponent(crateName)}/${encodeURIComponent(expectedVersion)}`,
		{
			headers: {
				"User-Agent": "opral-lix-release-workflow (https://github.com/opral/lix)",
			},
		},
	);
	if (response.status === 404) return undefined;
	if (!response.ok) {
		throw new Error(
			`crates.io lookup for ${crateName}@${expectedVersion} failed: HTTP ${response.status} ${await response.text()}`,
		);
	}
	return (await response.json()).version.checksum;
}

function acceptMatchingPublishedChecksum(remoteChecksum) {
	if (remoteChecksum === undefined) return false;
	if (remoteChecksum !== localChecksum) {
		throw new Error(
			`${crateName}@${expectedVersion} exists with different contents:\n` +
				`registry: ${remoteChecksum}\nlocal:    ${localChecksum}`,
		);
	}
	console.log(`${crateName}@${expectedVersion} is already published with identical contents.`);
	return true;
}

if (!acceptMatchingPublishedChecksum(await registryChecksum())) {
	const publish = spawnSync(
		"cargo",
		["publish", "--locked", "--no-verify", "-p", crateName],
		{ stdio: "inherit" },
	);
	if (publish.error) throw publish.error;

	// A publish can reach crates.io even when Cargo reports a late failure.
	// Check once before failing; a workflow retry will safely verify the same
	// immutable checksum if the API has not caught up yet.
	if (publish.status !== 0) {
		if (acceptMatchingPublishedChecksum(await registryChecksum())) {
			process.exit(0);
		}
		throw new Error(
			`cargo publish failed for ${crateName}@${expectedVersion} with status ${publish.status}`,
		);
	}

	let published = false;
	for (let attempt = 0; attempt < 24; attempt += 1) {
		const remoteChecksum = await registryChecksum();
		if (remoteChecksum !== undefined) {
			acceptMatchingPublishedChecksum(remoteChecksum);
			published = true;
			break;
		}
		await delay(5_000);
	}
	if (!published) {
		throw new Error(
			`${crateName}@${expectedVersion} was uploaded but did not appear on crates.io`,
		);
	}
}
