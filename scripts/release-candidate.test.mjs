import assert from "node:assert/strict";
import test from "node:test";
import { assertReleaseReady, checkReleaseFreeze, frozenReleaseCandidates } from "./release-candidate.mjs";

const pr = (overrides = {}) => ({
	number: 1669, state: "open", draft: false, base: { ref: "main" },
	head: { ref: "release/v0.15.0", sha: "candidate", repo: { full_name: "opral/lix" } },
	...overrides,
});

function validation() {
	return {
		eventName: "pull_request", event: { pull_request: pr() }, currentPr: pr(),
		needs: {
			"merge-reuse": { result: "success", outputs: { reuse: "false" } },
			"promote-browser-sdk": { result: "skipped" },
			changelog: { result: "success", outputs: { rust: "true" } },
			"cargo-config": { result: "success" }, cargo: { result: "success" },
			"js-sdk-test": { result: "success" },
			"preview-artifact-changes": { result: "success", outputs: { server: "true" } },
			"preview-server-image": { result: "success" },
		},
	};
}

test("any ready release candidate freezes updates, even across version bumps", async () => {
	const ready = pr();
	const inputs = [ready, pr({ draft: true }), pr({ state: "closed" }),
		pr({ base: { ref: "other" } }), pr({ head: { ref: "fix/foo" } }),
		pr({ head: { ...ready.head, repo: { full_name: "fork/lix" } } })];
	assert.deepEqual(frozenReleaseCandidates(inputs, "opral/lix"), [ready]);
	const outputs = {};
	await checkReleaseFreeze({
		github: { rest: { pulls: { list() {} } }, paginate: async () => inputs },
		context: { repo: { owner: "opral", repo: "lix" } },
		core: { setOutput: (key, value) => { outputs[key] = value; }, info() {} },
	});
	assert.equal(outputs.frozen, "true");
	assert.deepEqual(frozenReleaseCandidates([pr({ draft: true })], "opral/lix"), []);
});

test("a validated unchanged ready candidate passes", () => {
	assert.doesNotThrow(() => assertReleaseReady(validation()));
});

test("failures, cancellations, skipped or missing required jobs never pass", () => {
	for (const name of ["merge-reuse", "changelog", "cargo-config", "cargo", "js-sdk-test", "preview-artifact-changes", "preview-server-image"]) {
		for (const result of ["failure", "cancelled", "skipped", undefined]) {
			const input = validation();
			input.needs[name].result = result;
			assert.throws(() => assertReleaseReady(input), /did not pass/);
		}
	}
});

test("draft reruns cannot certify a now-ready PR; changed or withdrawn candidates fail", () => {
	const draftRerun = validation();
	draftRerun.event.pull_request.draft = true;
	assert.throws(() => assertReleaseReady(draftRerun), /ready candidate/);
	for (const change of [{ draft: true }, { state: "closed" }, { head: { sha: "new-head" } }]) {
		const input = validation();
		Object.assign(input.currentPr, change);
		assert.throws(() => assertReleaseReady(input));
	}
});

test("existing explicit SDK-only and unchanged-server scopes remain allowed", () => {
	const input = validation();
	input.needs.changelog.outputs.rust = "false";
	input.needs.cargo.result = "skipped";
	input.needs["preview-artifact-changes"].outputs.server = "false";
	input.needs["preview-server-image"].result = "skipped";
	assert.doesNotThrow(() => assertReleaseReady(input));
});

test("verified exact-tree reuse is push-only and requires artifact promotion", () => {
	const input = validation();
	input.needs["merge-reuse"].outputs.reuse = "true";
	assert.throws(() => assertReleaseReady(input), /PR candidates/);
	input.eventName = "push";
	assert.throws(() => assertReleaseReady(input), /promote-browser-sdk/);
	input.needs["promote-browser-sdk"].result = "success";
	assert.doesNotThrow(() => assertReleaseReady(input));
});

test("unknown or inconsistent preview scope fails closed", () => {
	const input = validation();
	delete input.needs["preview-artifact-changes"].outputs.server;
	assert.throws(() => assertReleaseReady(input), /preview scope/);
	input.needs["preview-artifact-changes"].outputs.server = "false";
	input.needs["preview-server-image"].result = "failure";
	assert.throws(() => assertReleaseReady(input), /preview scope/);
});
