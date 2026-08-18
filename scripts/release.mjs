import {
	existsSync,
	readdirSync,
	readFileSync,
	renameSync,
	rmSync,
	unlinkSync,
	writeFileSync,
} from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { execFileSync } from "node:child_process";

export const CHANGE_TYPES = ["minor", "patch"];
export const JS_SDK_NATIVE_PACKAGES = [
	"@lix-js/sdk-darwin-arm64",
	"@lix-js/sdk-linux-arm64",
	"@lix-js/sdk-linux-x64",
	"@lix-js/sdk-win32-x64",
];
export const PUBLIC_NPM_PACKAGE_PATHS = [
	"packages/js-sdk",
	"packages/storage-filesystem",
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
	const dir = join(root, ".changenotes");
	if (!existsSync(dir)) return [];
	return readdirSync(dir)
		.filter((file) => file.endsWith(".md") && file !== "README.md")
		.map((file) => `.changenotes/${file}`)
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
	const bodyParagraphs = changeBodyParagraphs(match[2]);
	if (!CHANGE_TYPES.includes(type)) {
		throw new Error(`${path}: type must be one of ${CHANGE_TYPES.join(", ")}`);
	}
	if (bodyParagraphs.length === 0) {
		throw new Error(`${path}: changelog body must not be empty`);
	}
	return {
		path,
		type,
		body: bodyParagraphs.join("\n\n"),
		summary: bodyParagraphs[0],
		details: bodyParagraphs.slice(1),
	};
}

export function loadChanges(root) {
	return changeFiles(root).map((path) => parseChange(root, path));
}

export function highestChangeType(changes) {
	if (changes.some((change) => change.type === "minor")) return "minor";
	if (changes.some((change) => change.type === "patch")) return "patch";
	return null;
}

export function changelogEntry(version, date, changes) {
	const labels = { minor: "Minor", patch: "Patch" };
	let entry = `## ${version} - ${date}\n`;
	for (const type of CHANGE_TYPES) {
		const typed = changes.filter((change) => change.type === type);
		if (typed.length === 0) continue;
		entry += `\n### ${labels[type]}\n\n`;
		for (const change of typed) {
			entry += changelogListItem(change);
		}
	}
	return `${entry}\n`;
}

function changeBodyParagraphs(body) {
	const paragraphs = [];
	let lines = [];
	let inFence = false;
	for (const line of body.trim().replace(/\r\n/g, "\n").split("\n")) {
		if (line.trimStart().startsWith("```")) {
			inFence = !inFence;
			lines.push(line);
			continue;
		}
		if (!inFence && line.trim() === "") {
			pushBodyParagraph(paragraphs, lines);
			lines = [];
			continue;
		}
		lines.push(line);
	}
	pushBodyParagraph(paragraphs, lines);
	return paragraphs;
}

function pushBodyParagraph(paragraphs, lines) {
	if (lines.length === 0) return;
	const hasFence = lines.some((line) => line.trimStart().startsWith("```"));
	const paragraph = hasFence
		? lines.join("\n").trim()
		: lines
				.map((line) => line.trim())
				.filter(Boolean)
				.join(" ");
	if (paragraph) paragraphs.push(paragraph);
}

function changelogListItem(change) {
	const paragraphs = change.summary ? [change.summary, ...(change.details ?? [])] : changeBodyParagraphs(change.body);
	const [summary, ...details] = paragraphs;
	let item = `- ${summary}\n`;
	for (const detail of details) {
		item += `\n${indentChangelogDetail(detail)}\n`;
	}
	return item;
}

function indentChangelogDetail(detail) {
	return detail
		.split("\n")
		.map((line) => `  ${line}`)
		.join("\n");
}

export function updateCargoToml(root, version, { renameManifest = renameSync } = {}) {
	const manifests = readCargoManifests(root);
	const rootManifestPath = resolve(root, "Cargo.toml");
	const rootManifest = manifests.get(rootManifestPath);
	if (rootManifest === undefined) {
		throw new Error("Could not find workspace Cargo.toml");
	}
	const planned = new Map(manifests);
	planned.set(
		rootManifestPath,
		rootManifest.replace(
			/(\[workspace\.package\][\s\S]*?\nversion\s*=\s*")[^"]+(")/,
			`$1${version}$2`,
		),
	);

	const lockstepManifests = workspaceVersionedPackageManifests(manifests);
	for (const [manifestPath] of manifests) {
		planned.set(
			manifestPath,
			updateVersionedPathDependencyRequirements(
				manifestPath,
				planned.get(manifestPath),
				lockstepManifests,
				version,
			),
		);
	}
	assertCargoLockstepVersions(root, planned, version);

	const changed = [...planned].filter(
		([manifestPath, text]) => text !== manifests.get(manifestPath),
	);
	const staged = changed.map(([manifestPath, text], index) => ({
		manifestPath,
		stagedPath: join(dirname(manifestPath), `.Cargo.toml.release-${process.pid}-${index}`),
		text,
	}));
	try {
		for (const { stagedPath, text } of staged) writeFileSync(stagedPath, text);
		for (const { manifestPath, stagedPath } of staged) {
			renameManifest(stagedPath, manifestPath);
		}
	} catch (error) {
		for (const [manifestPath] of changed) {
			writeFileSync(manifestPath, manifests.get(manifestPath));
		}
		throw error;
	} finally {
		for (const { stagedPath } of staged) {
			if (existsSync(stagedPath)) unlinkSync(stagedPath);
		}
	}
}

export function validateCargoLockstepVersions(root, version = currentVersion(root)) {
	assertCargoLockstepVersions(root, readCargoManifests(root), version);
}

function readCargoManifests(root) {
	const manifests = new Map();
	const visit = (directory) => {
		for (const entry of readdirSync(directory, { withFileTypes: true }).sort((a, b) =>
			a.name.localeCompare(b.name),
		)) {
			if (entry.isDirectory()) {
				if ([".git", "node_modules", "target"].includes(entry.name)) continue;
				visit(join(directory, entry.name));
			} else if (entry.isFile() && entry.name === "Cargo.toml") {
				const manifestPath = resolve(directory, entry.name);
				manifests.set(manifestPath, readFileSync(manifestPath, "utf8"));
			}
		}
	};
	visit(resolve(root));
	return manifests;
}

function workspaceVersionedPackageManifests(manifests) {
	return new Set(
		[...manifests]
			.filter(([, text]) => {
				const packageSection = text.match(/\[package\]([\s\S]*?)(?=\n\[|$)/)?.[1];
				return packageSection && /^version\.workspace\s*=\s*true\s*$/m.test(packageSection);
			})
			.map(([manifestPath]) => manifestPath),
	);
}

function updateVersionedPathDependencyRequirements(
	manifestPath,
	text,
	lockstepManifests,
	version,
) {
	const replacements = versionedPathDependencies(manifestPath, text)
		.filter((dependency) => lockstepManifests.has(dependency.targetManifest))
		.map((dependency) => ({
			start: dependency.versionStart,
			end: dependency.versionEnd,
			value: `=${version}`,
		}))
		.sort((a, b) => b.start - a.start);
	let next = text;
	for (const replacement of replacements) {
		next = `${next.slice(0, replacement.start)}${replacement.value}${next.slice(replacement.end)}`;
	}
	return next;
}

function assertCargoLockstepVersions(root, manifests, version) {
	const lockstepManifests = workspaceVersionedPackageManifests(manifests);
	const expected = `=${version}`;
	const mismatches = [];
	for (const [manifestPath, text] of manifests) {
		for (const dependency of versionedPathDependencies(manifestPath, text)) {
			if (!lockstepManifests.has(dependency.targetManifest)) continue;
			if (dependency.version === expected) continue;
			mismatches.push(
				`${relative(root, manifestPath)}: ${dependency.name} requires ${dependency.version}, expected ${expected}`,
			);
		}
	}
	if (mismatches.length > 0) {
		mismatches.sort();
		throw new Error(`Cargo lockstep path dependency mismatches:\n${mismatches.join("\n")}`);
	}
}

function versionedPathDependencies(manifestPath, text) {
	const dependencies = [];
	const sections = [...text.matchAll(/^\[([^\]\n]+)\][ \t]*(?:#.*)?$/gm)];
	for (let index = 0; index < sections.length; index += 1) {
		const section = sections[index];
		const sectionName = section[1];
		const bodyStart = section.index + section[0].length;
		const bodyEnd = sections[index + 1]?.index ?? text.length;
		if (isDependencyListSection(sectionName)) {
			const body = text.slice(bodyStart, bodyEnd);
			for (const declaration of inlineDependencyDeclarations(body, bodyStart)) {
				pushVersionedPathDependency(
					dependencies,
					manifestPath,
					declaration.name,
					text,
					declaration.start,
					declaration.end,
				);
			}
			continue;
		}
		const dependencyName = dependencySubtableName(sectionName);
		if (!dependencyName) continue;
		pushVersionedPathDependency(
			dependencies,
			manifestPath,
			dependencyName,
			text,
			bodyStart,
			bodyEnd,
		);
	}
	return dependencies;
}

function isDependencyListSection(sectionName) {
	return /^(?:(?:workspace\.)?(?:dev-|build-)?dependencies|target\..+\.(?:dev-|build-)?dependencies)$/.test(
		sectionName,
	);
}

function dependencySubtableName(sectionName) {
	return sectionName.match(
		/^(?:(?:workspace\.)?(?:dev-|build-)?dependencies|target\..+\.(?:dev-|build-)?dependencies)\.([A-Za-z0-9_-]+)$/,
	)?.[1];
}

function inlineDependencyDeclarations(body, bodyOffset) {
	const declarations = [];
	for (const match of body.matchAll(/^[ \t]*(?:"([^"]+)"|([A-Za-z0-9_-]+))\s*=\s*\{/gm)) {
		const tableStart = bodyOffset + match.index + match[0].lastIndexOf("{");
		const tableEnd = inlineTableEnd(body, match.index + match[0].lastIndexOf("{"));
		if (tableEnd === null) {
			throw new Error(`Unterminated inline Cargo dependency table for ${match[1] ?? match[2]}`);
		}
		declarations.push({
			name: match[1] ?? match[2],
			start: tableStart,
			end: bodyOffset + tableEnd,
		});
	}
	return declarations;
}

function inlineTableEnd(text, start) {
	let depth = 0;
	let quote = null;
	let escaped = false;
	let comment = false;
	for (let index = start; index < text.length; index += 1) {
		const character = text[index];
		if (comment) {
			if (character === "\n") comment = false;
			continue;
		}
		if (quote) {
			if (quote === '"' && escaped) {
				escaped = false;
				continue;
			}
			if (quote === '"' && character === "\\") {
				escaped = true;
				continue;
			}
			if (character === quote) quote = null;
			continue;
		}
		if (character === "#") {
			comment = true;
			continue;
		}
		if (character === '"' || character === "'") {
			quote = character;
			continue;
		}
		if (character === "{") depth += 1;
		if (character === "}" && --depth === 0) return index + 1;
	}
	return null;
}

function pushVersionedPathDependency(
	dependencies,
	manifestPath,
	name,
	text,
	start,
	end,
) {
	const declaration = text.slice(start, end);
	const dependencyPath = declaration.match(/\bpath\s*=\s*"([^"]+)"/)?.[1];
	const versionMatch = /\bversion\s*=\s*"([^"]+)"/.exec(declaration);
	if (!dependencyPath || !versionMatch) return;
	const valueOffset = versionMatch.index + versionMatch[0].lastIndexOf(versionMatch[1]);
	dependencies.push({
		name,
		version: versionMatch[1],
		versionStart: start + valueOffset,
		versionEnd: start + valueOffset + versionMatch[1].length,
		targetManifest: resolve(dirname(manifestPath), dependencyPath, "Cargo.toml"),
	});
}

export function updatePackageVersion(root, version) {
	for (const packagePath of PUBLIC_NPM_PACKAGE_PATHS) {
		const packageJsonPath = `${packagePath}/package.json`;
		const lockPath = `${packagePath}/package-lock.json`;
		const packageJson = readJson(root, packageJsonPath);
		packageJson.version = version;

		if (packageJson.name === "@lix-js/sdk") {
			packageJson.optionalDependencies = Object.fromEntries(
				JS_SDK_NATIVE_PACKAGES.map((packageName) => [packageName, version]),
			);
		}
		if (packageJson.name === "@lix-js/storage-filesystem") {
			packageJson.peerDependencies["@lix-js/sdk"] = version;
		}
		writeJson(root, packageJsonPath, packageJson);

		const lock = readJson(root, lockPath);
		lock.version = version;
		if (lock.packages?.[""]) {
			lock.packages[""].version = version;
			lock.packages[""].optionalDependencies = packageJson.optionalDependencies;
			lock.packages[""].peerDependencies = packageJson.peerDependencies;
		}
		if (packageJson.name === "@lix-js/sdk") {
			for (const packageName of JS_SDK_NATIVE_PACKAGES) {
				const lockedPackage = lock.packages?.[`node_modules/${packageName}`];
				if (!lockedPackage) continue;
				const unscopedName = packageName.split("/").at(-1);
				lockedPackage.version = version;
				lockedPackage.resolved = `https://registry.npmjs.org/${packageName}/-/${unscopedName}-${version}.tgz`;
				delete lockedPackage.integrity;
			}
		}
		writeJson(root, lockPath, lock);
	}
}

export function updateCargoLockfiles(root, { runCargo = execFileSync } = {}) {
	for (const manifestPath of ["Cargo.toml", "tooling/Cargo.toml"]) {
		if (!existsSync(join(root, manifestPath))) continue;
		runCargo(
			"cargo",
			["update", "--workspace", "--manifest-path", manifestPath],
			{
				cwd: root,
				stdio: "inherit",
			},
		);
	}
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
	validateCargoLockstepVersions(root, version);
	updatePackageVersion(root, version);
	updateChangelog(root, version, date, changes);
	for (const change of changes) {
		rmSync(join(root, change.path));
	}
	updateCargoLockfiles(root);
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
