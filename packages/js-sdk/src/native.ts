import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const localNativePath = fileURLToPath(new URL("../lix_js_sdk.node", import.meta.url));

const nativePackages = {
	"linux-x64": "@lix-js/sdk-linux-x64",
	"linux-arm64": "@lix-js/sdk-linux-arm64",
	"darwin-arm64": "@lix-js/sdk-darwin-arm64",
	"win32-x64": "@lix-js/sdk-win32-x64",
} as const;

function nativePackageName() {
	const key = `${process.platform}-${process.arch}` as keyof typeof nativePackages;
	return nativePackages[key];
}

function resolveNativePath() {
	if (existsSync(localNativePath)) {
		return localNativePath;
	}
	const packageName = nativePackageName();
	let packageResolutionError: unknown;
	if (packageName) {
		try {
			return require.resolve(packageName);
		} catch (error) {
			packageResolutionError = error;
		}
	}
	if (!packageName) {
		throw new Error(`Unsupported platform ${process.platform}-${process.arch}`);
	}
	throw packageResolutionError;
}

const native = { exports: {} as Record<string, any> };
try {
	const nativePath = resolveNativePath();
	process.dlopen(native, nativePath);
} catch (cause) {
	const error = new Error(
		`Failed to load @lix-js/sdk native addon for ${process.platform}-${process.arch}. ` +
			"This package requires the matching optional native binary package. " +
			"Run `npm run build` from packages/js-sdk for local development, or install a release that includes your platform binary.",
	) as Error & { cause?: unknown };
	error.cause = cause;
	throw error;
}

export const addon = native.exports;
