import assert from "node:assert/strict";
import test from "node:test";
import { assertArtifactRequest } from "./ci-artifact-request.mjs";
const sha = "a".repeat(40);
const request = { artifactsOnly: true, eventName: "workflow_dispatch", requestedRevision: sha, workflowRevision: sha };
test("explicit dispatch accepts an exact ref, including a draft PR branch", () => {
	assert.doesNotThrow(() => assertArtifactRequest(request));
});
for (const revision of [undefined, "", "main", "a".repeat(7), "A".repeat(40)]) {
	test(`rejects a missing or mutable revision: ${revision}`, () => {
		assert.throws(() => assertArtifactRequest({ ...request, requestedRevision: revision }));
	});
}
test("a branch advancing before dispatch cannot produce misleading provenance", () => {
	assert.throws(() => assertArtifactRequest({ ...request, workflowRevision: "b".repeat(40) }), /no longer matches/);
});
test("artifact mode cannot run on implicit PR/push events", () => {
	for (const eventName of ["push", "pull_request"]) {
		assert.throws(() => assertArtifactRequest({ ...request, eventName }), /explicit/);
	}
});
test("ordinary validation retains its existing behavior and needs no source input", () => {
	for (const eventName of ["push", "pull_request", "workflow_dispatch"]) {
		assert.doesNotThrow(() => assertArtifactRequest({ artifactsOnly: false, eventName }));
	}
});
