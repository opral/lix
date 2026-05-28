import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";

const nativePath = fileURLToPath(new URL("../lix_js_sdk.node", import.meta.url));
const native = { exports: {} as Record<string, any> };
try {
	if (!existsSync(nativePath)) {
		throw new Error(`Native addon not found at ${nativePath}`);
	}
	process.dlopen(native, nativePath);
} catch (cause) {
	const error = new Error(
		`Failed to load @lix-js/sdk native addon for ${process.platform}-${process.arch}. ` +
			"This package currently requires a matching Node native binary. " +
			"Run `pnpm --filter @lix-js/sdk build` for local development, or install a release that includes your platform binary.",
	) as Error & { cause?: unknown };
	error.cause = cause;
	throw error;
}

export const addon = native.exports;
