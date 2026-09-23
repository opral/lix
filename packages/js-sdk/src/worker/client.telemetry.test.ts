import { expect, test } from "vitest";
import type { LixBinding, TelemetryParentContext } from "../binding-types.js";
import { wrapTelemetryParentBinding } from "./client.js";

test("first observation frame retains its creation parent across async setup", async () => {
	const creationParent = {
		traceparent: "00-11111111111111111111111111111111-1111111111111111-01",
	};
	const laterParent = {
		traceparent: "00-22222222222222222222222222222222-2222222222222222-01",
	};
	let activeParent: TelemetryParentContext | undefined = creationParent;
	const parents: Array<TelemetryParentContext | undefined> = [];
	const binding = {
		setTelemetryParent(parent: TelemetryParentContext | undefined) {
			parents.push(parent);
		},
		async observe() {
			return {
				setTelemetryParent(parent: TelemetryParentContext | undefined) {
					parents.push(parent);
				},
				async next() {
					return undefined;
				},
				close() {},
			};
		},
	} as unknown as LixBinding;
	const wrapped = wrapTelemetryParentBinding(binding, () => activeParent);
	const events = await wrapped.observe("SELECT 1", []);

	activeParent = undefined;
	await events.next();
	expect(parents.at(-1)).toEqual(creationParent);

	activeParent = laterParent;
	await events.next();
	expect(parents.at(-1)).toEqual(laterParent);

	activeParent = undefined;
	await events.next();
	expect(parents.at(-1)).toBeUndefined();
});
