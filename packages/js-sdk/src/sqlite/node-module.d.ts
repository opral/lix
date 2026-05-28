declare module "node:module" {
	export function createRequire(filename: string): (id: string) => unknown;
}
