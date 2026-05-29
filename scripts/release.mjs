import { existsSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { execFileSync } from "node:child_process";

export const CHANGE_TYPES = ["major", "minor", "patch"];
export const CHANGE_SCOPES = ["engine", "lix-sdk", "js-sdk", "cli"];
export const JS_SDK_NATIVE_PACKAGES = [
	"@lix-js/sdk-darwin-arm64",
	"@lix-js/sdk-linux-arm64",
	"@lix-js/sdk-linux-x64",
	"@lix-js/sdk-win32-x64",
];

export function readText(root, path) {
	return readFileSync(join(root, path), "utf8");
}

export function writeText(root, path, text) {
	writeFileSync(join(root, path), text);
}

export function readJson(root, path) {
	return JSON.parse(readText(root, path));
}

export function writeJson(root, path, value) {
	writeText(root, path, `${JSON.stringify(value, null, "\t")}\n`);
}

export function currentVersion(root) {
	const match = readText(root, "Cargo.toml").match(
		/\[workspace\.package\][\s\S]*?\nversion\s*=\s*"([^"]+)"/,
	);
	if (!match) {
		throw new Error("Could not find [workspace.package].version in Cargo.toml");
	}
	return match[1];
}

export function bumpVersion(version, type) {
	const match = version.match(/^(\d+)\.(\d+)\.(\d+)(?:-.+)?$/);
	if (!match) {
		throw new Error(`Unsupported version format: ${version}`);
	}
	const major = Number(match[1]);
	const minor = Number(match[2]);
	const patch = Number(match[3]);
	if (type === "major") return `${major + 1}.0.0`;
	if (type === "minor") return `${major}.${minor + 1}.0`;
	if (type === "patch") return `${major}.${minor}.${patch + 1}`;
	throw new Error(`Unsupported change type: ${type}`);
}

export function changeFiles(root) {
	const dir = join(root, ".changes");
	if (!existsSync(dir)) return [];
	return readdirSync(dir)
		.filter((file) => file.endsWith(".md"))
		.map((file) => `.changes/${file}`)
		.sort();
}

export function parseChange(root, path) {
	const text = readText(root, path).trim();
	const match = text.match(/^---\n([\s\S]*?)\n---\n([\s\S]+)$/);
	if (!match) {
		throw new Error(`${path}: expected frontmatter followed by a changelog body`);
	}
	const metadata = Object.fromEntries(
		match[1]
			.split("\n")
			.map((line) => line.trim())
			.filter(Boolean)
			.map((line) => {
				const separator = line.indexOf(":");
				if (separator === -1) throw new Error(`${path}: invalid frontmatter line "${line}"`);
				return [line.slice(0, separator).trim(), line.slice(separator + 1).trim()];
			}),
	);
	const type = metadata.type;
	const scope = metadata.scope;
	const body = match[2].trim().replace(/\s+/g, " ");
	if (!CHANGE_TYPES.includes(type)) {
		throw new Error(`${path}: type must be one of ${CHANGE_TYPES.join(", ")}`);
	}
	if (!CHANGE_SCOPES.includes(scope)) {
		throw new Error(`${path}: scope must be one of ${CHANGE_SCOPES.join(", ")}`);
	}
	if (!body) {
		throw new Error(`${path}: changelog body must not be empty`);
	}
	return { path, type, scope, body };
}

export function loadChanges(root) {
	return changeFiles(root).map((path) => parseChange(root, path));
}

export function highestChangeType(changes) {
	if (changes.some((change) => change.type === "major")) return "major";
	if (changes.some((change) => change.type === "minor")) return "minor";
	if (changes.some((change) => change.type === "patch")) return "patch";
	return null;
}

export function changelogEntry(version, date, changes) {
	const labels = { major: "Major", minor: "Minor", patch: "Patch" };
	let entry = `## ${version} - ${date}\n`;
	for (const type of CHANGE_TYPES) {
		const typed = changes.filter((change) => change.type === type);
		if (typed.length === 0) continue;
		entry += `\n### ${labels[type]}\n\n`;
		for (const change of typed) {
			entry += `- ${change.scope}: ${change.body}\n`;
		}
	}
	return `${entry}\n`;
}

export function updateCargoToml(root, version) {
	let text = readText(root, "Cargo.toml");
	text = text.replace(
		/(\[workspace\.package\][\s\S]*?\nversion\s*=\s*")[^"]+(")/,
		`$1${version}$2`,
	);
	text = text.replace(
		/(lix_engine\s*=\s*\{\s*path\s*=\s*"packages\/engine",\s*version\s*=\s*")[^"]+(")/,
		`$1${version}$2`,
	);
	text = text.replace(
		/(lix_sdk\s*=\s*\{\s*path\s*=\s*"packages\/rs-sdk",\s*version\s*=\s*")[^"]+(")/,
		`$1${version}$2`,
	);
	writeText(root, "Cargo.toml", text);
}

export function updatePackageVersion(root, version) {
	const packageJsonPath = "packages/js-sdk/package.json";
	const lockPath = "packages/js-sdk/package-lock.json";
	const packageJson = readJson(root, packageJsonPath);
	packageJson.version = version;
	packageJson.optionalDependencies = Object.fromEntries(
		JS_SDK_NATIVE_PACKAGES.map((packageName) => [packageName, version]),
	);
	writeJson(root, packageJsonPath, packageJson);

	const lock = readJson(root, lockPath);
	lock.version = version;
	if (lock.packages?.[""]) {
		lock.packages[""].version = version;
		lock.packages[""].optionalDependencies = Object.fromEntries(
			JS_SDK_NATIVE_PACKAGES.map((packageName) => [packageName, version]),
		);
	}
	writeJson(root, lockPath, lock);
}

export function updateChangelog(root, version, date, changes) {
	const path = "CHANGELOG.md";
	const existing = existsSync(join(root, path)) ? readText(root, path).trimEnd() : "# Changelog\n";
	const entry = changelogEntry(version, date, changes).trimEnd();
	const next =
		existing.trim() === "# Changelog"
			? `# Changelog\n\n${entry}\n`
			: `${existing.replace(/^# Changelog\n*/, `# Changelog\n\n${entry}\n\n`)}\n`;
	writeText(root, path, next);
}

export function prepareRelease(root, { date = new Date().toISOString().slice(0, 10) } = {}) {
	const changes = loadChanges(root);
	if (changes.length === 0) {
		return null;
	}
	const type = highestChangeType(changes);
	const version = bumpVersion(currentVersion(root), type);
	updateCargoToml(root, version);
	updatePackageVersion(root, version);
	updateChangelog(root, version, date, changes);
	for (const change of changes) {
		rmSync(join(root, change.path));
	}
	execFileSync("cargo", ["update", "-p", "lix_cli", "-p", "lix_engine", "-p", "lix_js_sdk", "-p", "lix_sdk"], {
		cwd: root,
		stdio: "inherit",
	});
	return { version, type, changes };
}

export function releaseTagForHead(root) {
	const message = execFileSync("git", ["log", "-1", "--pretty=%B"], {
		cwd: root,
		encoding: "utf8",
	}).trim();
	const match = message.match(/Release v(\d+\.\d+\.\d+)/);
	if (!match) return null;
	const version = currentVersion(root);
	if (version !== match[1]) {
		throw new Error(`Release commit says ${match[1]}, but Cargo.toml says ${version}`);
	}
	return `v${version}`;
}
