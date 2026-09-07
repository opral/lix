// Kept small and pure so the release policy is covered without building Lix.
export function frozenReleaseCandidates(prs, repository) {
	return prs.filter(pr => pr.state === "open" && !pr.draft &&
		pr.base.ref === "main" && pr.head.repo?.full_name === repository &&
		/^release\/v\d+\.\d+\.\d+$/.test(pr.head.ref));
}

export async function checkReleaseFreeze({ github, context, core }) {
	const prs = await github.paginate(github.rest.pulls.list, {
		...context.repo, state: "open", base: "main", per_page: 100,
	});
	const frozen = frozenReleaseCandidates(prs, `${context.repo.owner}/${context.repo.repo}`);
	core.setOutput("frozen", frozen.length > 0 ? "true" : "false");
	if (frozen.length) core.info(`Release candidate frozen: ${frozen.map(pr => `#${pr.number}`).join(", ")}. Return it to draft and dispatch Release PR to refresh.`);
}

export function assertReleaseReady({ needs, eventName, event, currentPr }) {
	const requireSuccess = name => {
		if (needs[name]?.result !== "success") throw new Error(`${name} did not pass (${needs[name]?.result ?? "missing"})`);
	};
	if (eventName === "pull_request") {
		if (event.pull_request.draft || !currentPr || currentPr.draft || currentPr.state !== "open") {
			throw new Error("Not an open, ready candidate; mark ready and run full CI.");
		}
		if (currentPr.head.sha !== event.pull_request.head.sha) {
			throw new Error("The candidate changed; validate its current head commit.");
		}
		const testedBase = event.pull_request.base;
		const currentBase = currentPr.base;
		if (!testedBase?.sha || !currentBase?.sha ||
			currentBase.sha !== testedBase.sha || currentBase.ref !== testedBase.ref) {
			throw new Error("The base branch changed or is unknown; refresh the candidate and run new CI.");
		}
	}
	requireSuccess("merge-reuse");
	// Only push runs can reuse exact-tree evidence verified by merge-reuse.
	if (needs["merge-reuse"].outputs?.reuse === "true") {
		if (eventName !== "push") throw new Error("PR candidates must run validation.");
		requireSuccess("promote-browser-sdk");
		return;
	}
	for (const name of ["changelog", "cargo-config", "js-sdk-test"]) requireSuccess(name);
	// Retain the existing intentional Rust-only skip for SDK TypeScript changes.
	if (needs.changelog.outputs?.rust === "false") {
		if (needs.cargo?.result !== "skipped") throw new Error("Unexpected Rust scope result");
	} else requireSuccess("cargo");
	if (eventName === "pull_request") {
		requireSuccess("preview-artifact-changes");
		const server = needs["preview-artifact-changes"].outputs?.server;
		if (server === "true") requireSuccess("preview-server-image");
		else if (server !== "false" || needs["preview-server-image"]?.result !== "skipped") {
			throw new Error("Unexpected server preview scope result");
		}
	}
}
