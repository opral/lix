import { execFileSync, spawnSync } from "node:child_process";
import { appendFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

// Keep this hash compatible with LixRay's content-tag lookup.
export function serverInputHash(cwd = process.cwd()) {
	const tree = (...args) => execFileSync("git", ["ls-tree", ...args], { cwd });
	const input = Buffer.concat([
		tree(
			"HEAD",
			"--",
			".cargo",
			".dockerignore",
			"Cargo.lock",
			"Cargo.toml",
			"rust-toolchain.toml",
		),
		tree(
			"-r",
			"HEAD",
			"--",
			"packages/lix",
			"packages/lix-schema",
			"packages/server",
			"packages/storage-slatedb",
			"plugins",
		),
	]);
	return execFileSync("git", ["hash-object", "--stdin"], {
		cwd,
		input,
		encoding: "utf8",
	}).trim();
}

export function existingDigest(image, run = spawnSync) {
	const result = run(
		"docker",
		[
			"buildx",
			"imagetools",
			"inspect",
			image,
			"--format",
			"{{json .Manifest.Digest}}",
		],
		{ encoding: "utf8" },
	);
	if (result.error) throw result.error;
	if (result.status !== 0) {
		// Authentication/registry failures are not cache misses. Avoid paying for
		// a build that cannot be published when the registry is unavailable.
		if (/manifest unknown|not found|name unknown/i.test(result.stderr ?? ""))
			return "";
		throw new Error(`Cannot inspect ${image}: ${result.stderr}`);
	}
	const digest = JSON.parse(result.stdout.trim());
	if (!/^sha256:[a-f0-9]{64}$/.test(digest))
		throw new Error("Invalid image digest");
	return digest;
}

if (
	process.argv[1] &&
	import.meta.url === pathToFileURL(process.argv[1]).href
) {
	const tag = `ghcr.io/opral/lix-server:content-${serverInputHash()}`;
	const digest = existingDigest(tag);
	appendFileSync(process.env.GITHUB_OUTPUT, `tag=${tag}\ndigest=${digest}\n`);
	console.log(
		digest
			? `Reuse ${tag}@${digest}; no server compilation needed.`
			: `No image for ${tag}; build from source.`,
	);
}
