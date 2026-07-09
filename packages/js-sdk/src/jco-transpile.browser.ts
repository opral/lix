// This module is copied from the pinned JCO package by build:jco-browser. Using
// its browser-native generator avoids pulling JCO's CLI-only Node imports and
// optional minifier into application bundles.
// @ts-expect-error The generated module is copied into dist after tsc runs.
import { $init, generate } from "./jco/js-component-bindgen-component.js";

type TranspileOptions = {
	name?: string;
	emitTypescriptDeclarations?: boolean;
	instantiation?: "async" | "sync";
	nodejsCompat?: boolean;
	base64Cutoff?: number;
};

type TranspileResult = {
	files: Record<string, Uint8Array>;
	imports: string[];
	exports: [string, "function" | "instance"][];
};

export async function transpileBytes(
	component: Uint8Array,
	options: TranspileOptions = {},
): Promise<TranspileResult> {
	await $init;
	const generated = generate(component, {
		name: options.name ?? "component",
		noTypescript: options.emitTypescriptDeclarations === false,
		instantiation: options.instantiation
			? { tag: options.instantiation }
			: undefined,
		noNodejsCompat: options.nodejsCompat === false,
		base64Cutoff: options.base64Cutoff ?? 5000,
	});
	return {
		files: Object.fromEntries(generated.files),
		imports: generated.imports,
		exports: generated.exports,
	};
}
