#!/usr/bin/env node

import { execFileSync } from "node:child_process";

const CRATES_IO_PACKAGES = new Set([
	"lix",
	"lix-storage-filesystem",
	"lix-storage-rocksdb",
	"lix-storage-slatedb",
]);

const metadata = JSON.parse(
	execFileSync(
		"cargo",
		["metadata", "--format-version", "1", "--no-deps"],
		{ encoding: "utf8" },
	),
);

const failures = [];
for (const cargoPackage of metadata.packages) {
	const shouldPublish = CRATES_IO_PACKAGES.has(cargoPackage.name);
	const publishesToCratesIo = cargoPackage.publish === null;
	const publishesElsewhere =
		Array.isArray(cargoPackage.publish) && cargoPackage.publish.length > 0;

	if (shouldPublish && !publishesToCratesIo) {
		failures.push(
			`${cargoPackage.name} must remain publishable to crates.io (remove package.publish)`,
		);
	}
	if (!shouldPublish && (publishesToCratesIo || publishesElsewhere)) {
		failures.push(
			`${cargoPackage.name} must set package.publish = false`,
		);
	}
}

for (const packageName of CRATES_IO_PACKAGES) {
	if (!metadata.packages.some((cargoPackage) => cargoPackage.name === packageName)) {
		failures.push(`${packageName} is missing from the Cargo workspace`);
	}
}

if (failures.length > 0) {
	console.error("Invalid Cargo publish surface:");
	for (const failure of failures) console.error(`- ${failure}`);
	process.exit(1);
}

const published = metadata.packages
	.filter((cargoPackage) => CRATES_IO_PACKAGES.has(cargoPackage.name))
	.map((cargoPackage) => `${cargoPackage.name}@${cargoPackage.version}`)
	.sort();
console.log(`Validated crates.io publish surface: ${published.join(", ")}`);
