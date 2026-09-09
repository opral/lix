/** An artifact dispatch must build the exact source attached to its run. */
export function assertArtifactRequest({ artifactsOnly, eventName, requestedRevision, workflowRevision }) {
	if (!artifactsOnly) return;
	if (eventName !== "workflow_dispatch") {
		throw new Error("Artifact-only builds require explicit workflow_dispatch.");
	}
	if (!/^[0-9a-f]{40}$/.test(requestedRevision ?? "")) {
		throw new Error("Artifact-only builds require source_revision as a full lowercase commit SHA.");
	}
	if (requestedRevision !== workflowRevision) {
		throw new Error("The dispatched ref no longer matches source_revision. Dispatch the intended branch/tag again with its exact current SHA.");
	}
}
