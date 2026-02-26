import { useContext, useEffect, useState, use } from "react";
import type { Lix } from "@lix-js/sdk";
import { LixContext } from "../provider.js";

// Map to cache promises by query key
const queryPromiseCache = new Map<string, Promise<any>>();
const lixInstanceIds = new WeakMap<object, number>();
let nextLixInstanceId = 1;

interface UseQueryOptions {
	subscribe?: boolean;
}

// Query factory receives a lix instance and returns a compilable+executable query.
interface QueryLike<TRow> {
	compile(): {
		sql: string;
		parameters: ReadonlyArray<unknown>;
	};
	execute(): Promise<TRow[]>;
}

type QueryFactory<TRow> = (lix: Lix) => QueryLike<TRow>;

/**
 * Subscribe to a live query using React 19 Suspense.
 *
 * The hook suspends on first render and re-suspends whenever its SQL changes,
 * so wrap consuming components with React Suspense and an ErrorBoundary.
 *
 * @param query - Factory function that creates a compiled+executable query object. Preferred shape: `(lix) => qb(lix).selectFrom(...)`.
 * @param options - Optional configuration
 * @param options.subscribe - Whether to subscribe to live updates (default: true)
 *
 * @example
 * // Basic list
 * function KeyValueList() {
 *   const keyValues = useQuery((lix) =>
 *     qb(lix).selectFrom('lix_key_value')
 *       .where('key', 'like', 'example_%')
 *       .selectAll()
 *   );
 *   return (
 *     <ul>
 *       {keyValues.map(item => (
 *         <li key={item.key}>{item.key}: {item.value}</li>
 *       ))}
 *     </ul>
 *   );
 * }
 *
 * @example
 * // With Suspense + ErrorBoundary
 * import { Suspense } from 'react';
 * import { ErrorBoundary } from 'react-error-boundary';
 *
 * function App() {
 *   return (
 *     <Suspense fallback={<div>Loading…</div>}>
 *       <ErrorBoundary fallbackRender={() => <div>Failed to load.</div>}>
 *         <KeyValueList />
 *       </ErrorBoundary>
 *     </Suspense>
 *   );
 * }
 *
 * @example
 * // One-time query without live updates
 * const config = useQuery(
 *   (lix) => qb(lix).selectFrom('config').selectAll(),
 *   { subscribe: false }
 * );
 */
export function useQuery<TRow>(
	query: QueryFactory<TRow>,
	options: UseQueryOptions = {},
): TRow[] {
	const lix = useContext(LixContext);
	if (!lix) throw new Error("useQuery must be used inside <LixProvider>.");

	const { subscribe = true } = options;
	const builder = query(lix);
	const compiled = builder.compile();
	const observeQuery = {
		sql: compiled.sql,
		params: [...compiled.parameters],
	};
	const cacheKey =
		`${getLixInstanceId(lix)}:${subscribe ? "sub" : "once"}:` +
		`${compiled.sql}:${JSON.stringify(compiled.parameters)}`;

	// Get or create promise. Cache key includes parameters so different queries
	// resolve independently while reuse avoids duplicating in-flight requests.
	const cached = queryPromiseCache.get(cacheKey) as Promise<TRow[]> | undefined;
	const promise: Promise<TRow[]> =
		cached ??
		(() => {
			const p = builder.execute() as Promise<TRow[]>;
			queryPromiseCache.set(cacheKey, p);
			return p;
		})();

	// Use the promise (suspends on first render)
	const initialRows = use(promise);

	// Local state for updates
	const [rows, setRows] = useState(initialRows);
	useEffect(() => {
		setRows(initialRows);
	}, [cacheKey]);

	// Subscribe for ongoing updates (only if subscribe is true)
	useEffect(() => {
		if (!subscribe) return;

		let cancelled = false;
		const events = lix.observe(observeQuery);

		const observeLoop = async () => {
			try {
				while (!cancelled) {
					const event = await events.next();
					if (cancelled || event === undefined) {
						break;
					}
					const nextRows = (await query(lix).execute()) as TRow[];
					if (cancelled) {
						return;
					}
					setRows(nextRows);
				}
			} catch (err) {
				if (cancelled) {
					return;
				}
				// Clear promise to allow retry
				queryPromiseCache.delete(cacheKey);
				// Surface error to ErrorBoundary
				setRows(() => {
					throw err instanceof Error ? err : new Error(String(err));
				});
			}
		};

		void observeLoop();
		return () => {
			cancelled = true;
			events.close();
		};
	}, [cacheKey, subscribe, lix]);

	if (!subscribe) {
		return initialRows;
	}

	return rows;
}

function getLixInstanceId(lix: Lix): number {
	const asObject = lix as object;
	const cached = lixInstanceIds.get(asObject);
	if (cached !== undefined) {
		return cached;
	}
	const next = nextLixInstanceId++;
	lixInstanceIds.set(asObject, next);
	return next;
}

/* ------------------------------------------------------------------------- */
/* Optional single-row helper                                                */
/* ------------------------------------------------------------------------- */

/**
 * Subscribe to a live query and return only the first result inside React.
 * Equivalent to calling `.executeTakeFirst()` on a Kysely query.
 *
 * @example
 * ```tsx
 * function ExampleComponent({ itemId }: { itemId: string }) {
 *   const item = useQueryTakeFirst((lix) =>
 *     qb(lix).selectFrom('lix_key_value')
 *       .where('key', '=', `example_${itemId}`)
 *       .selectAll()
 *   );
 *
 *   // No loading/error states needed - Suspense and ErrorBoundary handle them
 *   if (!item) {
 *     return <div>Item not found</div>;
 *   }
 *
 *   return <div>Value: {item.value}</div>;
 * }
 *
 * // Wrap with Suspense and ErrorBoundary:
 * <Suspense fallback={<div>Loading...</div>}>
 *   <ErrorBoundary fallback={<div>Error occurred</div>}>
 *     <ExampleComponent itemId="123" />
 *   </ErrorBoundary>
 * </Suspense>
 * ```
 */
export const useQueryTakeFirst = <TResult>(
	query: QueryFactory<TResult>,
	options: UseQueryOptions = {},
): TResult | undefined => {
	const rows = useQuery<TResult>(query, options);
	return rows[0] as TResult | undefined;
};

/**
 * Subscribe to a live query and return only the first result inside React.
 * Throws an error if no result is found.
 *
 * @param query - Factory function that creates a Kysely SelectQueryBuilder
 *
 * @throws Error if no result is found
 *
 * @example
 * ```tsx
 * function ExampleDetail({ itemId }: { itemId: string }) {
 *   const item = useQueryTakeFirstOrThrow((lix) =>
 *     qb(lix).selectFrom('lix_key_value')
 *       .where('key', '=', `example_${itemId}`)
 *       .selectAll()
 *   );
 *
 *   // No need to check for undefined - will throw to ErrorBoundary if not found
 *   return <div>Value: {item.value}</div>;
 * }
 *
 * // Wrap with Suspense and ErrorBoundary:
 * <Suspense fallback={<div>Loading...</div>}>
 *   <ErrorBoundary fallback={<div>Item not found</div>}>
 *     <ExampleDetail itemId="123" />
 *   </ErrorBoundary>
 * </Suspense>
 * ```
 */
export const useQueryTakeFirstOrThrow = <TResult>(
	query: QueryFactory<TResult>,
	options: UseQueryOptions = {},
): TResult => {
	const data = useQueryTakeFirst(query, options);
	if (data === undefined) throw new Error("No result found");
	return data;
};
