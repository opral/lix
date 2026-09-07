import { execFileSync } from "node:child_process";
import { appendFileSync, readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

// Only the SDK's handwritten TypeScript is known not to affect the Rust-only
// suites. Both SDK jobs still build Rust and run their native/browser tests.
// Unknown paths (including fixtures, manifests and build scripts) run full CI.
export function isSdkOnlyChange(paths) {
	const sdkSource = (path) =>
		/^packages\/js-sdk\/src\/.+\.ts$/.test(path) &&
		!path.startsWith("packages/js-sdk/src/wasm/");
	return (
		paths.some(sdkSource) &&
		paths.every(
			(path) => sdkSource(path) || /^\.changenotes\/[^/]+\.md$/.test(path),
		)
	);
}

export function selectRustScope({ eventName, event, cwd = process.cwd() }) {
	if (eventName !== "pull_request") return true;
	try {
		const git = (...args) =>
			execFileSync("git", args, {
				cwd,
				encoding: "utf8",
				stdio: ["ignore", "pipe", "pipe"],
				maxBuffer: 16 * 1024 * 1024,
			});
		const [head, base, prHead, ...extra] = git(
			"rev-list",
			"--parents",
			"-n",
			"1",
			"HEAD",
		)
			.trim()
			.split(/\s+/);
		// Diff the actual tested merge against its first parent, including base
		// updates. Missing/shallow history or a head checkout must run full CI.
		if (
			!base ||
			!prHead ||
			extra.length ||
			prHead !== event?.pull_request?.head?.sha
		) {
			return true;
		}
		// Disable rename detection so moving a Rust input into an allowed path
		// still exposes the deleted Rust path. NUL delimiters preserve filenames.
		const paths = git(
			"diff",
			"--name-only",
			"--no-renames",
			"-z",
			base,
			head,
			"--",
		)
			.split("\0")
			.filter(Boolean);
		return !isSdkOnlyChange(paths);
	} catch {
		return true;
	}
}

if (
	process.argv[1] &&
	import.meta.url === pathToFileURL(process.argv[1]).href
) {
	let rust = true;
	try {
		rust = selectRustScope({
			eventName: process.env.GITHUB_EVENT_NAME,
			event: JSON.parse(readFileSync(process.env.GITHUB_EVENT_PATH, "utf8")),
		});
	} catch {
		// An unreadable event must never suppress the Rust checks.
	}
	appendFileSync(process.env.GITHUB_OUTPUT, `rust=${rust}\n`);
	console.log(
		rust
			? "Run all Rust checks (Rust/unknown inputs, push, dispatch, or uncertain diff)."
			: "SDK TypeScript-only PR: run both SDK suites; skip the Rust-only jobs.",
	);
}
