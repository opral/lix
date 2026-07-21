import { WASIShim } from "@bytecodealliance/preview2-shim/instantiation";
import { transpileBytes } from "#jco-transpile";

type PluginRuntimeOperation =
	| "initComponent"
	| "detectChanges"
	| "render"
	| "closeComponent";

export type PluginRuntimeRequest = {
	operation: PluginRuntimeOperation;
	componentId?: number;
	componentBytes?: Uint8Array;
	maxMemoryBytes?: string;
	maxFuel?: string;
	timeoutMs?: string;
	state?: PluginEntityState[];
	file?: PluginFile;
};

export type PluginRuntimeResponse = {
	componentId?: number;
	changes?: PluginDetectedChange[];
	bytes?: Uint8Array;
	errorMessage?: string;
};

type PluginFile = {
	filename?: string;
	data: Uint8Array;
};

type PluginEntityState = {
	entityPk: string[];
	schemaKey: string;
	snapshotContent: string;
	metadata?: string;
};

type PluginDetectedChange = {
	entityPk: string[];
	schemaKey: string;
	snapshotContent?: string;
	metadata?: string;
};

type PluginApi = {
	detectChanges(
		state: PluginEntityState[],
		file: PluginFile,
	): PluginDetectedChange[];
	render(state: PluginEntityState[]): Uint8Array;
};

type WebAssemblyHost = {
	compile(bytes: ArrayBuffer): Promise<object>;
};

const webAssembly = (
	globalThis as typeof globalThis & { WebAssembly: WebAssemblyHost }
).WebAssembly;

type InstantiationModule = {
	instantiate(
		getCoreModule: (path: string) => Promise<object>,
		imports: Record<string, unknown>,
	): Promise<{ api?: PluginApi }>;
};

type PreparedComponent = {
	generatedModule: InstantiationModule;
	getCoreModule(path: string): Promise<object>;
};

const PREPARED_COMPONENT_CACHE_SIZE = 8;
const preparedComponents = new Map<string, PreparedComponent>();
const componentPreparations = new Map<string, Promise<PreparedComponent>>();

class WasmPluginRuntime {
	private nextComponentId = 1;
	private readonly components = new Map<number, PluginApi>();

	readonly dispatch = async (
		request: PluginRuntimeRequest,
	): Promise<PluginRuntimeResponse> => {
		try {
			switch (request.operation) {
				case "initComponent":
					return await this.initComponent(request);
				case "detectChanges":
					return this.detectChanges(request);
				case "render":
					return this.render(request);
				case "closeComponent":
					return this.closeComponent(request);
			}
		} catch (cause) {
			return {
				errorMessage: `Lix plugin runtime ${request.operation} failed: ${errorMessage(cause)}`,
			};
		}
	};

	private async initComponent(
		request: PluginRuntimeRequest,
	): Promise<PluginRuntimeResponse> {
		if (!request.componentBytes) {
			throw new TypeError("initComponent requires componentBytes");
		}
		const componentId = this.nextComponentId;
		this.nextComponentId += 1;
		const prepared = await prepareComponent(request.componentBytes);
		const wasi = new WASIShim({
			sandbox: {
				preopens: {},
				env: {},
				args: ["lix-plugin"],
				enableNetwork: false,
			},
		});
		const instance = await prepared.generatedModule.instantiate(
			prepared.getCoreModule,
			wasi.getImportObject() as Record<string, unknown>,
		);
		if (!instance.api) {
			throw new Error("component does not export lix:plugin/api");
		}
		this.components.set(componentId, instance.api);
		return { componentId };
	}

	private detectChanges(request: PluginRuntimeRequest): PluginRuntimeResponse {
		const component = this.requiredComponent(request.componentId);
		if (!request.file) {
			throw new TypeError("detectChanges requires file");
		}
		return {
			changes: component.detectChanges(request.state ?? [], request.file),
		};
	}

	private render(request: PluginRuntimeRequest): PluginRuntimeResponse {
		const component = this.requiredComponent(request.componentId);
		return { bytes: component.render(request.state ?? []) };
	}

	private closeComponent(request: PluginRuntimeRequest): PluginRuntimeResponse {
		if (request.componentId !== undefined) {
			this.components.delete(request.componentId);
		}
		return {};
	}

	private requiredComponent(componentId: number | undefined): PluginApi {
		if (componentId === undefined) {
			throw new TypeError("plugin runtime operation requires componentId");
		}
		const component = this.components.get(componentId);
		if (!component) {
			throw new Error(`unknown plugin component ${componentId}`);
		}
		return component;
	}
}

export function createPluginRuntimeDispatch(): WasmPluginRuntime["dispatch"] {
	return new WasmPluginRuntime().dispatch;
}

async function prepareComponent(
	componentBytes: Uint8Array,
): Promise<PreparedComponent> {
	const cacheKey = await contentHash(componentBytes);
	const cached = preparedComponents.get(cacheKey);
	if (cached) {
		preparedComponents.delete(cacheKey);
		preparedComponents.set(cacheKey, cached);
		return cached;
	}

	let preparation = componentPreparations.get(cacheKey);
	if (!preparation) {
		preparation = transpileAndCompile(componentBytes).then((prepared) => {
			preparedComponents.set(cacheKey, prepared);
			while (preparedComponents.size > PREPARED_COMPONENT_CACHE_SIZE) {
				const oldest = preparedComponents.keys().next().value;
				if (oldest === undefined) break;
				preparedComponents.delete(oldest);
			}
			return prepared;
		});
		componentPreparations.set(cacheKey, preparation);
	}

	try {
		return await preparation;
	} finally {
		if (componentPreparations.get(cacheKey) === preparation) {
			componentPreparations.delete(cacheKey);
		}
	}
}

async function transpileAndCompile(
	componentBytes: Uint8Array,
): Promise<PreparedComponent> {
	const name = "lix_plugin";
	const transpiled = await transpileBytes(componentBytes, {
		name,
		emitTypescriptDeclarations: false,
		instantiation: "async",
		nodejsCompat: false,
	});
	const files = new Map(Object.entries(transpiled.files));
	const moduleSource = requiredFile(files, `${name}.js`);
	const moduleUrl = `data:text/javascript;base64,${bytesToBase64(moduleSource)}`;
	const generatedModule = (await import(
		/* @vite-ignore */ moduleUrl
	)) as InstantiationModule;
	const compiledCoreModules = new Map<string, Promise<object>>();

	return {
		generatedModule,
		async getCoreModule(path) {
			let compilation = compiledCoreModules.get(path);
			if (!compilation) {
				compilation = webAssembly
					.compile(copyArrayBuffer(requiredFile(files, path)))
					.catch((cause) => {
						compiledCoreModules.delete(path);
						throw cause;
					});
				compiledCoreModules.set(path, compilation);
			}
			return await compilation;
		},
	};
}

async function contentHash(bytes: Uint8Array): Promise<string> {
	const digest = await crypto.subtle.digest("SHA-256", copyArrayBuffer(bytes));
	return Array.from(new Uint8Array(digest), (byte) =>
		byte.toString(16).padStart(2, "0"),
	).join("");
}

function bytesToBase64(bytes: Uint8Array): string {
	let binary = "";
	const chunkSize = 0x8000;
	for (let index = 0; index < bytes.length; index += chunkSize) {
		binary += String.fromCharCode(...bytes.subarray(index, index + chunkSize));
	}
	return btoa(binary);
}

function requiredFile(
	files: ReadonlyMap<string, Uint8Array>,
	path: string,
): Uint8Array {
	const file = files.get(path);
	if (!file) {
		throw new Error(`JCO output is missing ${path}`);
	}
	return file;
}

function copyArrayBuffer(bytes: Uint8Array): ArrayBuffer {
	const copy = new Uint8Array(bytes.byteLength);
	copy.set(bytes);
	return copy.buffer;
}

function errorMessage(cause: unknown): string {
	if (cause instanceof Error) {
		return cause.message;
	}
	if (cause && typeof cause === "object") {
		const tag = "tag" in cause ? String(cause.tag) : undefined;
		const value = "val" in cause ? String(cause.val) : undefined;
		if (tag || value) {
			return [tag, value].filter(Boolean).join(": ");
		}
	}
	return String(cause);
}
