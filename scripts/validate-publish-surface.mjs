#!/usr/bin/env node

import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
	JS_SDK_NATIVE_PACKAGES,
	PUBLIC_NPM_PACKAGE_PATHS,
} from "./release.mjs";

const CRATES_IO_PACKAGES = new Set([
	"lix",
	"lix-plugin-bindings-column-merger",
	"lix-plugin-bindings-combined",
	"lix-plugin-bindings-file-projection",
	"lix-schema",
	"lix-storage-filesystem",
	"lix-storage-rocksdb",
	"lix-storage-slatedb",
]);
const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const rootCargoToml = readFileSync(join(root, "Cargo.toml"), "utf8");
const workspaceVersion = rootCargoToml.match(
	/\[workspace\.package\][\s\S]*?\nversion\s*=\s*"([^"]+)"/,
)?.[1];
if (!workspaceVersion) throw new Error("Cargo.toml has no workspace package version");

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
	if (shouldPublish && cargoPackage.version !== workspaceVersion) {
		failures.push(
			`${cargoPackage.name}@${cargoPackage.version} must match workspace version ${workspaceVersion}`,
		);
	}
	if (shouldPublish) {
		const manifest = readFileSync(cargoPackage.manifest_path, "utf8");
		const packageSection = manifest.match(/\[package\]([\s\S]*?)(?=\n\[|$)/)?.[1];
		if (!packageSection || !/^version\.workspace\s*=\s*true\s*$/m.test(packageSection)) {
			failures.push(`${cargoPackage.name} must inherit version.workspace = true`);
		}
	}
	if (!shouldPublish && (publishesToCratesIo || publishesElsewhere)) {
		failures.push(
			`${cargoPackage.name} must set package.publish = false`,
		);
	}
}

for (const dependency of [
	"lix",
	"lix_schema",
	"lix_storage_filesystem",
	"lix_storage_rocksdb",
	"lix_storage_slatedb",
]) {
	const line = rootCargoToml.match(
		new RegExp(`^${dependency}\\s*=\\s*\\{[^}]+\\}$`, "m"),
	)?.[0];
	if (!line?.includes(`version = "=${workspaceVersion}"`)) {
		failures.push(`${dependency} must use exact workspace dependency version =${workspaceVersion}`);
	}
}

for (const packagePath of PUBLIC_NPM_PACKAGE_PATHS) {
	const npmPackage = JSON.parse(
		readFileSync(join(root, packagePath, "package.json"), "utf8"),
	);
	if (npmPackage.version !== workspaceVersion) {
		failures.push(
			`${npmPackage.name}@${npmPackage.version} must match workspace version ${workspaceVersion}`,
		);
	}
	if (
		npmPackage.name === "@lix-js/sdk" &&
		JS_SDK_NATIVE_PACKAGES.some(
			(name) => npmPackage.optionalDependencies?.[name] !== workspaceVersion,
		)
	) {
		failures.push(`${npmPackage.name} native packages must pin ${workspaceVersion}`);
	}
	if (
		npmPackage.name === "@lix-js/storage-filesystem" &&
		npmPackage.peerDependencies?.["@lix-js/sdk"] !== workspaceVersion
	) {
		failures.push(
			`${npmPackage.name} must pin @lix-js/sdk to ${workspaceVersion}`,
		);
	}
	const packageLock = JSON.parse(
		readFileSync(join(root, packagePath, "package-lock.json"), "utf8"),
	);
	if (
		packageLock.version !== workspaceVersion ||
		packageLock.packages?.[""]?.version !== workspaceVersion
	) {
		failures.push(`${npmPackage.name} package lock must match ${workspaceVersion}`);
	}
}

for (const packageName of CRATES_IO_PACKAGES) {
	if (!metadata.packages.some((cargoPackage) => cargoPackage.name === packageName)) {
		failures.push(`${packageName} is missing from the Cargo workspace`);
	}
}

if (failures.length > 0) {
	console.error("Invalid lockstep publish surface:");
	for (const failure of failures) console.error(`- ${failure}`);
	process.exit(1);
}

const published = metadata.packages
	.filter((cargoPackage) => CRATES_IO_PACKAGES.has(cargoPackage.name))
	.map((cargoPackage) => `${cargoPackage.name}@${cargoPackage.version}`)
	.sort();
const publishedNpm = PUBLIC_NPM_PACKAGE_PATHS.map((packagePath) => {
	const npmPackage = JSON.parse(
		readFileSync(join(root, packagePath, "package.json"), "utf8"),
	);
	return `${npmPackage.name}@${npmPackage.version}`;
}).sort();
console.log(
	`Validated lockstep publish surface: ${[...published, ...publishedNpm].join(", ")}`,
);
