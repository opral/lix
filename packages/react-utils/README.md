# @lix-js/react-utils

React 19 hooks and helpers for building reactive UIs on top of the Lix SDK. These utilities wire Kysely queries to React Suspense and subscribe to live database updates.

- React 19 Suspense-first data fetching
- Live updates via Lix.observe(query)
- Minimal API surface: `LixProvider`, `useLix`, `useQuery`, `useQueryTakeFirst`, `useQueryTakeFirstOrThrow`

## Installation

```bash
npm i @lix-js/react-utils
```

## Requirements

- React 19 (these hooks use `use()` and Suspense)
- Lix SDK instance provided via context

## Quick start

Wrap your app with `LixProvider` and pass a Lix instance.

```tsx
import { createRoot } from "react-dom/client";
import { LixProvider } from "@lix-js/react-utils";
import { openLix } from "@lix-js/sdk";

async function bootstrap() {
	const lix = await openLix({});
	const root = createRoot(document.getElementById("root")!);
	root.render(
		<LixProvider lix={lix}>
			<App />
		</LixProvider>,
	);
}

bootstrap();
```

## useQuery

Subscribe to a live query using React Suspense. The callback receives `lix` and must return a compilable/executable query (for example `qb(lix).selectFrom(...)`).

```tsx
import { Suspense } from "react";
import { ErrorBoundary } from "react-error-boundary";
import { useQuery } from "@lix-js/react-utils";
import { qb } from "@lix-js/kysely";

function KeyValueList() {
	const rows = useQuery((lix) =>
		qb(lix).selectFrom("key_value").where("key", "like", "demo_%").selectAll(),
	);

	return (
		<ul>
			{rows.map((r) => (
				<li key={r.key}>
					{r.key}: {r.value}
				</li>
			))}
		</ul>
	);
}

export function Page() {
	return (
		<Suspense fallback={<div>Loading…</div>}>
			<ErrorBoundary fallbackRender={() => <div>Failed to load.</div>}>
				<KeyValueList />
			</ErrorBoundary>
		</Suspense>
	);
}
```

Options

```tsx
// One-time execution (no live updates)
const rows = useQuery((lix) => qb(lix).selectFrom("config").selectAll(), {
	subscribe: false,
});
```

### Behavior

- Suspends on first render until the underlying query resolves.
- Re-suspends if the compiled SQL or params of the query change.
- Subscribes to live updates when `subscribe !== false` and updates state on emissions.
- On subscription error, clears the cached promise and throws to the nearest ErrorBoundary.

## Single-row helpers

When you want just one row:

```tsx
import {
	useQueryTakeFirst,
	useQueryTakeFirstOrThrow,
} from "@lix-js/react-utils";
import { qb } from "@lix-js/kysely";

// First row or undefined
const file = useQueryTakeFirst((lix) =>
	qb(lix).selectFrom("file").select(["id", "path"]).where("id", "=", fileId),
);

// First row or throw (suspends, then throws to ErrorBoundary if not found)
const activeVersion = useQueryTakeFirstOrThrow((lix) =>
	qb(lix)
		.selectFrom("active_version")
		.innerJoin("version", "version.id", "active_version.version_id")
		.selectAll("version"),
);
```

## Query Builder Integration

`react-utils` does not construct query builders for you. Pass any query object that implements `compile()` and `execute()`. In practice, most apps use `qb(lix)` from `@lix-js/kysely`.

## Synchronizing external state updates (rich text editors, etc.)

When building experiences like rich text editors, dashboards, or collaborative views, you often need to synchronize external changes while avoiding feedback loops from your own writes. Lix provides a simple pattern for this using a “writer key” and commit events.

See the guide for the pattern, pitfalls, and a decision matrix:

- https://lix.dev/guide/writer-key

## Provider and context

```tsx
import { LixProvider, useLix } from "@lix-js/react-utils";

function NeedsLix() {
	const lix = useLix(); // same instance passed to LixProvider
	// …
}
```

## FAQ

- Why does the callback receive `lix` directly?
  - The hook is query-builder agnostic. You can wrap `lix` however you want (for example `qb(lix)`), and react-utils only needs the compiled SQL + execute behavior.

- Can I do imperative fetching?
  - Yes, you can call `qb(lix)` directly in event handlers. `useQuery` is for declarative, Suspense-friendly reads.

## TypeScript tips

- `useQuery<TRow>(...)` infers the row shape from your Kysely selection. You can also provide an explicit generic to guide inference if needed.

## License

Apache-2.0
