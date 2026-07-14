export type LixJsError = Error & {
	code?: string;
	details?: unknown;
	hint?: string;
};

export function invalidArgument(
	operation: string,
	argument: string,
	expected: string,
	actual: string,
	receiver = "lix",
) {
	const error = new Error(
		`${receiver}.${operation}() expected ${argument} to be a ${expected}`,
	) as LixJsError;
	error.name = "LixError";
	error.code = "LIX_INVALID_ARGUMENT";
	error.details = { operation, argument, expected, actual };
	return error;
}

export function invalidParam(
	index: number,
	message: string,
	actual: string,
): LixJsError {
	const error = new Error(message) as LixJsError;
	error.name = "LixError";
	error.code = "LIX_INVALID_PARAM";
	error.details = {
		operation: "execute",
		parameter_index: index + 1,
		argument: `params[${index}]`,
		actual,
	};
	return error;
}

export function localFilesystemNotOpen(operation: string): LixJsError {
	const error = new Error(
		`LocalFilesystem.${operation}() requires the storage to be opened with openLix() first`,
	) as LixJsError;
	error.name = "LixError";
	error.code = "LIX_LOCAL_FILESYSTEM_NOT_OPEN";
	error.details = { operation };
	return error;
}

export function localFilesystemAlreadyOpen(): LixJsError {
	const error = new Error(
		"openLix() LocalFilesystem is already open; close the existing Lix or create a new LocalFilesystem",
	) as LixJsError;
	error.name = "LixError";
	error.code = "LIX_LOCAL_FILESYSTEM_IN_USE";
	return error;
}
