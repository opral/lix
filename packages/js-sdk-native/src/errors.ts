export type LixJsError = Error & {
	code?: string;
	details?: unknown;
	hint?: string;
};

export function withLixError<T>(fn: () => T): T {
	try {
		return fn();
	} catch (error) {
		throw error;
	}
}

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
