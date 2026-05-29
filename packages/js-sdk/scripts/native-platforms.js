export const nativePlatforms = [
	{ suffix: "linux-x64", os: "linux", cpu: "x64" },
	{ suffix: "linux-arm64", os: "linux", cpu: "arm64" },
	{ suffix: "darwin-arm64", os: "darwin", cpu: "arm64" },
	{ suffix: "win32-x64", os: "win32", cpu: "x64" },
];

export function nativePackageName(suffix) {
	return `@lix-js/sdk-${suffix}`;
}

export function nativePlatformForCurrentProcess() {
	return nativePlatforms.find(
		(platform) => platform.os === process.platform && platform.cpu === process.arch,
	);
}

export function nativePlatformForSuffix(suffix) {
	return nativePlatforms.find((platform) => platform.suffix === suffix);
}
