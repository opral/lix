import { type ReactNode } from "react";
import { getGithubStars } from "../github-stars-cache";
import { GitHubIcon, LixLogo } from "./header";

const githubUrl = "https://github.com/opral/lix";
const pythonIssueUrl = "https://github.com/opral/lix/issues/370";
const rustIssueUrl = "https://github.com/opral/lix/issues/371";
const goIssueUrl = "https://github.com/opral/lix/issues/373";
const npmUrl = "https://www.npmjs.com/package/@lix-js/sdk";

const jsHeroSample = `import { openLix, SqliteBackend } from "@lix-js/sdk";

const lix = await openLix({
  backend: new SqliteBackend({ path: "app.lix" }),
});

const main = await lix.activeBranchId();

await lix.fs.writeFile("/orders.xlsx", bytes);

const original = await lix.fs.readFile("/orders.xlsx");

const draft = await lix.createBranch({ name: "Explore" });
await lix.switchBranch({ branchId: draft.id });

await lix.fs.writeFile("/orders.xlsx", draftBytes);

await lix.switchBranch({ branchId: main });

const merge = await lix.mergeBranch({
  sourceBranchId: draft.id,
});

const changes = await lix.execute(
  "SELECT schema_key, count(*) AS count FROM lix_change GROUP BY schema_key"
);`;

const stats = [
  ["Just a Library", "no daemon, no protocol, no remote"],
  ["ACID", "across state, blobs, and history"],
  ["semantic diffs", "per-entity, not byte-level"],
  ["SQL API", "history is a query"],
];

const overhead = [
  "repository directories on disk",
  "working trees + checkout per worker",
  "locks & packfiles",
  "garbage collection",
  "LFS for blobs",
  "process calls & shell-outs",
  "protocol servers",
  "two-phase commits with the rest of your data",
];

const features = [
  {
    tag: "01",
    title: "Importable library.",
    body: "Import, open, run. Programmatic and in-process - no daemon, no protocol, no remote. In-memory by default; bring your own SQLite, Postgres, S3, or Cloudflare backend if needed.",
    code: `import { openLix, SqliteBackend } from "@lix-js/sdk";

const lix = await openLix({
  backend: new SqliteBackend({ path: "app.lix" }),
});`,
  },
  {
    tag: "02",
    title: "ACID transactions.",
    body: "One transaction covers your data, the blobs it references, and the change row that records the edit. No two-phase commit between three systems.",
    code: `await lix.transaction(async (tx) => {
  await tx.fs.writeFile("/spec.docx", body);
  await tx.fs.writeFile("/spec.png", img);
});`,
  },
  {
    tag: "03",
    title: "Parallel sessions. No worktrees.",
    body: "Each agent gets its own session and version without Git-style multi-checkout worktrees. Lix commits through the backend transaction layer, not per-agent repo directories or shell-out coordination.",
    code: `const agent1 = await lix.create_session("copy");
const agent2 = await lix.create_session("pricing");
const agent3 = await lix.create_session("qa");

await agent1.fs.writeFile("/landing.md", copyDraft);
await agent2.fs.writeFile("/plans.json", priceModel);
await agent3.fs.writeFile("/checks/report.json", testRun);

await agent1.commit();
await agent2.commit();
await agent3.commit();`,
  },
  {
    tag: "04",
    title: "SQL interface.",
    body: "Agents can answer complex questions in one query instead of rereading whole files. Burn fewer tokens, get faster responses, and ground reviews in structured changes - not raw bytes.",
    visual: "sql-query",
  },
  {
    tag: "05",
    title: "Bring your own backend if needed.",
    body: "Start in memory, then plug Lix into the infrastructure your app already runs: SQLite, Postgres, S3 object storage, Cloudflare storage, or your own backend adapter.",
    visual: "backend",
    code: `const lix = await openLix({
  backend: new SqliteBackend({ path: "app.lix" }),
});`,
  },
];

const patterns = [
  {
    kind: "agents/",
    title: "AI agent filesystems",
    body: "Isolated workspaces, branchable explore steps, semantic change history, and rollback when a run goes sideways.",
    stack: ["openLix()", "branch()", "session()", "diff()"],
  },
  {
    kind: "db/",
    title: "VCS for Postgres & SQLite",
    body: "Time-travel and branchable schemas on top of an existing database. Reviewable migrations. Diffable rows.",
    stack: ["postgres()", "diff(rows)", "merge()"],
  },
  {
    kind: "apps/",
    title: "Apps with version control",
    body: "Add branches, review, rollback, and history to editors, CMSs, design tools, internal ops apps, and AI-native products.",
    stack: ["branch()", "history()", "revert()"],
  },
  {
    kind: "review/",
    title: "Review for machine changes",
    body: "Surface what an agent actually changed at the entity level. Approve, request edits, or revert by symbol - not by patch.",
    stack: ["diff()", "history.entity()", "revert()"],
  },
];

const shipped = [
  "Importable SDK",
  "ACID transactions across state, blobs, and history",
  "Parallel sessions and versions",
  "Semantic changes via SQL",
  "Pluggable backend interface",
];

const roadmap07 = ["CLI for creating, inspecting, and scripting Lix files"];

const roadmap08 = ["File plugin API for DOCX, XLSX, CAD, PDF, and code"];

const roadmap09 = ["Merge conflicts as first-class citizens"];

const roadmap10 = ["Working changes and checkpointing"];

function cn(...classes: Array<string | false | undefined>) {
  return classes.filter(Boolean).join(" ");
}

function highlightCode(code: string) {
  const pattern =
    /(`[^`]*`|"[^"]*"|'[^']*'|\bORDER BY\b|\bGROUP BY\b|\b(?:import|from|const|let|await|async|return|use|SELECT|FROM|JOIN|WHERE|AND|AS|DESC|INSERT|INTO|VALUES|UPDATE|SET)\b|\b(?:openLix|createBackend|SqliteBackend|sqlite|execute|transaction|create_session|createBranch|switchBranch|mergeBranchPreview|mergeBranch|activeBranchId|branch|diff|history|entity|write|writeFile|readFile|commit|file|fs|lix_json_get_text)\b|\b(?:lix_change|lix_file|xlsx_row)\b)/g;

  return code.split(pattern).map((part, index) => {
    if (!part) return null;

    let className: string | undefined;
    if (/^(`[^`]*`|"[^"]*"|'[^']*')$/.test(part)) {
      className = "text-[#058a3e]";
    } else if (
      /^(import|from|const|let|await|async|return|use|SELECT|FROM|JOIN|WHERE|AND|AS|DESC|ORDER BY|GROUP BY|INSERT|INTO|VALUES|UPDATE|SET)$/.test(
        part,
      )
    ) {
      className = "text-[#6d28d9]";
    } else if (
      /^(openLix|createBackend|SqliteBackend|sqlite|execute|transaction|create_session|createBranch|switchBranch|mergeBranchPreview|mergeBranch|activeBranchId|branch|diff|history|entity|write|writeFile|readFile|commit|file|fs|lix_json_get_text)$/.test(
        part,
      )
    ) {
      className = "text-[#066f86]";
    } else if (/^(lix_change|lix_file|xlsx_row)$/.test(part)) {
      className = "text-[#066f86]";
    }

    return className ? (
      <span className={className} key={`${part}-${index}`}>
        {part}
      </span>
    ) : (
      part
    );
  });
}

const fontSans = "font-[Geist,ui-sans-serif,system-ui,sans-serif]";
const fontMono = "font-[Geist_Mono,ui-monospace,JetBrains_Mono,monospace]";
const sectionClass =
  "mx-auto w-full max-w-[1280px] border-b border-[#e7e6e1] px-6 py-16 md:px-8 lg:px-14 lg:py-[88px]";
const displayClass =
  "font-medium leading-[0.98] tracking-[-0.035em] text-[#0a0a0a] [text-wrap:balance]";
const eyebrowClass = cn(
  fontMono,
  "text-xs font-medium uppercase tracking-[0.04em] text-[#6b6b66]",
);
const sectionTitleClass = cn(
  displayClass,
  "mt-4 text-[clamp(36px,4.1vw,48px)]",
);
const focusClass =
  "focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-[3px] focus-visible:outline-[#08b5d6]";

function RustIcon() {
  return (
    <svg
      width="16"
      height="16"
      viewBox="0 0 224 224"
      fill="currentColor"
      aria-hidden="true"
    >
      <path
        fill="#ce422b"
        d="M218.46 109.358l-9.062-5.614c-.076-.882-.162-1.762-.258-2.642l7.803-7.265a3.107 3.107 0 00.933-2.89 3.093 3.093 0 00-1.967-2.312l-9.97-3.715c-.25-.863-.512-1.72-.781-2.58l6.214-8.628a3.114 3.114 0 00-.592-4.263 3.134 3.134 0 00-1.431-.637l-10.507-1.709a80.869 80.869 0 00-1.263-2.353l4.417-9.7a3.12 3.12 0 00-.243-3.035 3.106 3.106 0 00-2.705-1.385l-10.671.372a85.152 85.152 0 00-1.685-2.044l2.456-10.381a3.125 3.125 0 00-3.762-3.763l-10.384 2.456a88.996 88.996 0 00-2.047-1.684l.373-10.671a3.11 3.11 0 00-1.385-2.704 3.127 3.127 0 00-3.034-.246l-9.681 4.417c-.782-.429-1.567-.854-2.353-1.265l-1.713-10.506a3.098 3.098 0 00-1.887-2.373 3.108 3.108 0 00-3.014.35l-8.628 6.213c-.85-.27-1.703-.53-2.56-.778l-3.716-9.97a3.111 3.111 0 00-2.311-1.97 3.134 3.134 0 00-2.89.933l-7.266 7.802a93.746 93.746 0 00-2.643-.258l-5.614-9.082A3.125 3.125 0 00111.97 4c-1.09 0-2.085.56-2.642 1.478l-5.615 9.081a93.32 93.32 0 00-2.642.259l-7.266-7.802a3.13 3.13 0 00-2.89-.933 3.106 3.106 0 00-2.312 1.97l-3.715 9.97c-.857.247-1.71.506-2.56.778L73.7 12.588a3.101 3.101 0 00-3.014-.35A3.127 3.127 0 0068.8 14.61l-1.713 10.506c-.79.41-1.575.832-2.353 1.265l-9.681-4.417a3.125 3.125 0 00-4.42 2.95l.372 10.67c-.69.553-1.373 1.115-2.048 1.685l-10.383-2.456a3.143 3.143 0 00-2.93.832 3.124 3.124 0 00-.833 2.93l2.436 10.383a93.897 93.897 0 00-1.68 2.043l-10.672-.372a3.138 3.138 0 00-2.704 1.385 3.126 3.126 0 00-.246 3.035l4.418 9.7c-.43.779-.855 1.563-1.266 2.353l-10.507 1.71a3.097 3.097 0 00-2.373 1.886 3.117 3.117 0 00.35 3.013l6.214 8.628a89.12 89.12 0 00-.78 2.58l-9.97 3.715a3.117 3.117 0 00-1.035 5.202l7.803 7.265c-.098.879-.184 1.76-.258 2.642l-9.062 5.614A3.122 3.122 0 004 112.021c0 1.092.56 2.084 1.478 2.642l9.062 5.614c.074.882.16 1.762.258 2.642l-7.803 7.265a3.117 3.117 0 001.034 5.201l9.97 3.716a110 110 0 00.78 2.58l-6.212 8.627a3.112 3.112 0 00.6 4.27c.419.33.916.547 1.443.63l10.507 1.709c.407.792.83 1.576 1.265 2.353l-4.417 9.68a3.126 3.126 0 002.95 4.42l10.65-.374c.553.69 1.115 1.372 1.685 2.047l-2.435 10.383a3.09 3.09 0 00.831 2.91 3.117 3.117 0 002.931.83l10.384-2.436a82.268 82.268 0 002.047 1.68l-.371 10.671a3.11 3.11 0 001.385 2.704 3.125 3.125 0 003.034.241l9.681-4.416c.779.432 1.563.854 2.353 1.265l1.713 10.505a3.147 3.147 0 001.887 2.395 3.111 3.111 0 003.014-.349l8.628-6.213c.853.271 1.71.535 2.58.783l3.716 9.969a3.112 3.112 0 002.312 1.967 3.112 3.112 0 002.89-.933l7.266-7.802c.877.101 1.761.186 2.642.264l5.615 9.061a3.12 3.12 0 002.642 1.478 3.165 3.165 0 002.663-1.478l5.614-9.061c.884-.078 1.765-.163 2.643-.264l7.265 7.802a3.106 3.106 0 002.89.933 3.105 3.105 0 002.312-1.967l3.716-9.969c.863-.248 1.719-.512 2.58-.783l8.629 6.213a3.12 3.12 0 004.9-2.045l1.713-10.506c.793-.411 1.577-.838 2.353-1.265l9.681 4.416a3.13 3.13 0 003.035-.241 3.126 3.126 0 001.385-2.704l-.372-10.671a81.794 81.794 0 002.046-1.68l10.383 2.436a3.123 3.123 0 003.763-3.74l-2.436-10.382a84.588 84.588 0 001.68-2.048l10.672.374a3.104 3.104 0 002.704-1.385 3.118 3.118 0 00.244-3.035l-4.417-9.68c.43-.779.852-1.563 1.263-2.353l10.507-1.709a3.08 3.08 0 002.373-1.886 3.11 3.11 0 00-.35-3.014l-6.214-8.627c.272-.857.532-1.717.781-2.58l9.97-3.716a3.109 3.109 0 001.967-2.311 3.107 3.107 0 00-.933-2.89l-7.803-7.265c.096-.88.182-1.761.258-2.642l9.062-5.614a3.11 3.11 0 001.478-2.642 3.157 3.157 0 00-1.476-2.663h-.064zm-60.687 75.337c-3.468-.747-5.656-4.169-4.913-7.637a6.412 6.412 0 017.617-4.933c3.468.741 5.676 4.169 4.933 7.637a6.414 6.414 0 01-7.617 4.933h-.02zm-3.076-20.847c-3.158-.677-6.275 1.334-6.936 4.5l-3.22 15.026c-9.929 4.5-21.055 7.018-32.614 7.018-11.89 0-23.12-2.622-33.234-7.328l-3.22-15.026c-.677-3.158-3.778-5.18-6.936-4.499l-13.273 2.848a80.222 80.222 0 01-6.853-8.091h64.61c.731 0 1.218-.132 1.218-.797v-22.91c0-.665-.487-.797-1.218-.797H94.133v-14.469h20.415c1.864 0 9.97.533 12.551 10.898.811 3.179 2.601 13.54 3.818 16.863 1.214 3.715 6.152 11.146 11.415 11.146h32.202c.365 0 .755-.041 1.166-.116a80.56 80.56 0 01-7.307 8.587l-13.583-2.911-.113.058zm-89.38 20.537a6.407 6.407 0 01-7.617-4.933c-.74-3.467 1.462-6.894 4.934-7.637a6.417 6.417 0 017.617 4.933c.74 3.468-1.464 6.894-4.934 7.637zm-24.564-99.28a6.438 6.438 0 01-3.261 8.484c-3.241 1.438-7.019-.025-8.464-3.261-1.445-3.237.025-7.039 3.262-8.483a6.416 6.416 0 018.463 3.26zM33.22 102.94l13.83-6.15c2.952-1.311 4.294-4.769 2.972-7.72l-2.848-6.44H58.36v50.362h-22.5a79.158 79.158 0 01-3.014-21.672c0-2.869.155-5.697.452-8.483l-.08.103zm60.687-4.892v-14.86h26.629c1.376 0 9.722 1.59 9.722 7.822 0 5.18-6.399 7.038-11.663 7.038h-24.77.082zm96.811 13.375c0 1.973-.072 3.922-.216 5.862h-8.113c-.811 0-1.137.532-1.137 1.327v3.715c0 8.752-4.934 10.671-9.268 11.146-4.129.464-8.691-1.726-9.248-4.252-2.436-13.684-6.482-16.595-12.881-21.672 7.948-5.036 16.204-12.487 16.204-22.498 0-10.753-7.369-17.523-12.385-20.847-7.059-4.644-14.862-5.572-16.968-5.572H52.899c11.374-12.673 26.835-21.673 44.174-24.975l9.887 10.361a5.849 5.849 0 008.278.19l11.064-10.568c23.119 4.314 42.729 18.721 54.082 38.598l-7.576 17.09c-1.306 2.951.027 6.419 2.973 7.72l14.573 6.48c.255 2.607.383 5.224.384 7.843l-.021.052zM106.912 24.94a6.398 6.398 0 019.062.209 6.437 6.437 0 01-.213 9.082 6.396 6.396 0 01-9.062-.21 6.436 6.436 0 01.213-9.083v.002zm75.137 60.476a6.402 6.402 0 018.463-3.26 6.425 6.425 0 013.261 8.482 6.402 6.402 0 01-8.463 3.261 6.425 6.425 0 01-3.261-8.483z"
      />
    </svg>
  );
}

function JsIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" aria-hidden="true">
      <rect x="2" y="2" width="20" height="20" rx="3" fill="#F7DF1E" />
      <text
        x="12"
        y="17"
        textAnchor="middle"
        fontSize="11"
        fontWeight="800"
        fill="#0a0a0a"
        fontFamily="Geist Mono, monospace"
      >
        JS
      </text>
    </svg>
  );
}

function PyIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" aria-hidden="true">
      <path
        d="M12 2.5c-3.2 0-3 1.5-3 1.5v2H12v.7H6.5s-2.2-.2-2.2 3.4 1.9 3.5 1.9 3.5h2v-1.8s-.1-2 1.9-2h3.7s1.9 0 1.9-1.8V4s.3-1.5-3.7-1.5zM10.2 3.5a.85.85 0 1 1 0 1.7.85.85 0 0 1 0-1.7z"
        fill="#3776AB"
      />
      <path
        d="M12 21.5c3.2 0 3-1.5 3-1.5v-2H12v-.7h5.5s2.2.2 2.2-3.4-1.9-3.5-1.9-3.5h-2v1.8s.1 2-1.9 2H10.2s-1.9 0-1.9 1.8V20s-.3 1.5 3.7 1.5zm1.8-1a.85.85 0 1 1 0-1.7.85.85 0 0 1 0 1.7z"
        fill="#FFD43B"
      />
    </svg>
  );
}

function GoIcon() {
  return (
    <svg width="18" height="16" viewBox="0 0 28 24" aria-hidden="true">
      <g fill="#00ADD8">
        <rect x="2" y="9" width="3.4" height="1.6" rx="0.5" />
        <rect x="2" y="13" width="6" height="1.6" rx="0.5" />
        <text
          x="14"
          y="17"
          fontFamily="Geist Mono, monospace"
          fontWeight="700"
          fontSize="11"
        >
          go
        </text>
      </g>
    </svg>
  );
}

function StarCount() {
  const githubStars = getGithubStars("opral/lix");
  if (githubStars === null) return <span>Star on GitHub</span>;
  return (
    <span>
      Star on GitHub{" "}
      <span className="text-[#6b6b66]">{githubStars.toLocaleString()}</span>
    </span>
  );
}

function Button({
  href,
  children,
  variant = "solid",
}: {
  href: string;
  children: ReactNode;
  variant?: "solid" | "ghost";
}) {
  return (
    <a
      className={cn(
        fontMono,
        focusClass,
        "inline-flex min-h-11 items-center justify-center gap-2 rounded-md border px-[18px] text-[13px] font-medium leading-none transition-colors",
        "max-md:w-full max-md:min-w-0",
        variant === "ghost"
          ? "border-[#e7e6e1] bg-transparent text-[#0a0a0a] [-webkit-text-fill-color:#0a0a0a] hover:border-[#0a0a0a]"
          : "border-[#0a0a0a] bg-[#0a0a0a] text-[#fafaf7] [-webkit-text-fill-color:#fafaf7] hover:bg-[#1a1a1a]",
      )}
      href={href}
      target={href.startsWith("http") ? "_blank" : undefined}
      rel={href.startsWith("http") ? "noopener noreferrer" : undefined}
    >
      {children}
    </a>
  );
}

function CodeBlock({
  children,
  small = false,
}: {
  children: ReactNode;
  small?: boolean;
}) {
  return (
    <pre
      className={cn(
        fontMono,
        "m-0 overflow-x-auto border border-[#e7e6e1] bg-[#f5f4ee] text-[#0a0a0a] tracking-normal [tab-size:2]",
        small
          ? "p-4 text-[12.5px] leading-[1.65]"
          : "px-6 py-[22px] text-[13.5px] leading-[1.65]",
      )}
    >
      <code className="font-[inherit] whitespace-pre">
        {typeof children === "string" ? highlightCode(children) : children}
      </code>
    </pre>
  );
}

function SemanticSqlVisual() {
  const jsStrong = "text-[#0a0a0a]";
  const jsKeyword = "text-[#6d28d9]";
  const jsMethod = "text-[#066f86]";
  const jsString = "text-[#058a3e]";

  return (
    <div>
      <CodeBlock small>
        {
          <>
            <span className={jsKeyword}>const</span>
            {` rows = `}
            <span className={jsKeyword}>await</span>
            {` `}
            <span className={jsStrong}>lix</span>.
            <span className={jsMethod}>execute</span>
            {`(
  `}
            <span className={jsString}>{`\`
  SELECT
    f.path,
    lix_json_get_text(c.entity_pk, 0) AS row_id,
    c.snapshot_content AS change
  FROM lix_change AS c
  JOIN lix_file AS f
    ON f.id = c.file_id
  WHERE c.schema_key = 'xlsx_row'
    AND f.path = '/orders.xlsx'
  ORDER BY c.created_at DESC;
  \``}</span>
            {`
`}
            <span className={jsStrong}>);</span>
          </>
        }
      </CodeBlock>
    </div>
  );
}

function AgentQuestionCard() {
  return (
    <div className="mt-5 max-w-[460px]">
      <div className="flex items-start gap-3">
        <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full border border-[#e7e6e1] bg-[#f5f4ee]">
          <img
            alt=""
            aria-hidden="true"
            className="h-4 w-4"
            loading="lazy"
            src="https://cdn.simpleicons.org/claude/D97757"
          />
        </div>
        <div className="min-w-0 rounded-[10px] border border-[#e7e6e1] bg-[#f5f4ee] px-3.5 py-3">
          <p className="m-0 text-[14px] leading-normal text-[#0a0a0a]">
            Which orders changed status in this branch?
          </p>
          <div
            className={cn(
              fontMono,
              "mt-2 flex items-center gap-2 text-[12px] text-[#6b6b66]",
            )}
          >
            <span className="h-1.5 w-1.5 rounded-full bg-[#d97757]" />
            <span>Executing SQL</span>
          </div>
        </div>
      </div>
    </div>
  );
}

function BackendLogos() {
  const backends = [
    { name: "SQLite", src: "https://cdn.simpleicons.org/sqlite/003B57" },
    { name: "Postgres", src: "https://cdn.simpleicons.org/postgresql/4169E1" },
    {
      name: "S3",
      src: "https://cdn.worldvectorlogo.com/logos/amazon-s3-simple-storage-service.svg",
    },
    {
      name: "Cloudflare Workers",
      src: "https://cdn.simpleicons.org/cloudflareworkers/F38020",
    },
    { name: "Supabase", src: "https://cdn.simpleicons.org/supabase/3FCF8E" },
  ];

  return (
    <div
      className={cn(
        fontMono,
        "mt-5 flex max-w-[460px] flex-wrap items-center gap-x-5 gap-y-3 text-[12px] text-[#2b2b2b]",
      )}
    >
      {backends.map((backend) => (
        <div className="inline-flex items-center gap-2" key={backend.name}>
          <img
            alt=""
            aria-hidden="true"
            className="h-5 w-5 shrink-0"
            loading="lazy"
            src={backend.src}
          />
          <span>{backend.name}</span>
        </div>
      ))}
    </div>
  );
}

function FeatureVisual({ feature }: { feature: (typeof features)[number] }) {
  if ("visual" in feature && feature.visual === "sql-query") {
    return <SemanticSqlVisual />;
  }
  if ("code" in feature) {
    return <CodeBlock small>{feature.code}</CodeBlock>;
  }
  return null;
}

function SectionHeader({
  eyebrow,
  title,
  className = "",
}: {
  eyebrow: string;
  title: ReactNode;
  className?: string;
}) {
  return (
    <div className={className}>
      <div className={eyebrowClass}>{eyebrow}</div>
      <h2 className={sectionTitleClass}>{title}</h2>
    </div>
  );
}

function LangTabs() {
  const tabs = [
    {
      key: "js",
      label: "JavaScript",
      Icon: JsIcon,
      stable: true,
      href: undefined,
    },
    {
      key: "rust",
      label: "Rust",
      Icon: RustIcon,
      stable: false,
      href: rustIssueUrl,
    },
    {
      key: "python",
      label: "Python",
      Icon: PyIcon,
      stable: false,
      href: pythonIssueUrl,
    },
    { key: "go", label: "Go", Icon: GoIcon, stable: false, href: goIssueUrl },
  ] as const;

  return (
    <div className="mt-10 max-w-[1100px] overflow-hidden rounded-[10px] border border-[#e7e6e1] bg-[#fafaf7]">
      <div className="flex items-center justify-between gap-[18px] border-b border-[#e7e6e1] bg-[#f3f2ec] max-md:flex-col max-md:items-stretch max-md:gap-0">
        <div className="flex overflow-x-auto [scrollbar-width:none] [&::-webkit-scrollbar]:hidden">
          {tabs.map(({ key, label, Icon, stable, href }) => {
            const active = key === "js";
            const className = cn(
              fontMono,
              focusClass,
              "relative inline-flex items-center gap-2 whitespace-nowrap px-4 py-3 text-[13px] text-[#2b2b2b]",
              active &&
                "after:absolute after:bottom-0 after:left-3 after:right-3 after:h-0.5 after:bg-[#066f86] after:content-['']",
            );
            if (href) {
              return (
                <a
                  className={className}
                  href={href}
                  key={key}
                  rel="noopener noreferrer"
                  target="_blank"
                  title="Track support on GitHub"
                >
                  <Icon />
                  <span>{label}</span>
                </a>
              );
            }
            return (
              <button
                className={cn(
                  className,
                  "cursor-default border-0 bg-transparent",
                )}
                key={key}
                type="button"
                title={stable ? label : "Track support on GitHub"}
              >
                <Icon />
                <span>{label}</span>
              </button>
            );
          })}
        </div>
        <div
          className={cn(
            fontMono,
            "shrink-0 pr-[18px] text-[11.5px] text-[#6b6b66] max-md:border-t max-md:border-[#e7e6e1] max-md:px-4 max-md:py-2.5",
          )}
        >
          @lix-js/sdk · v0.6.0
        </div>
      </div>
      <div className="[&_pre]:border-0">
        <CodeBlock>{jsHeroSample}</CodeBlock>
      </div>
    </div>
  );
}

export function V2Hero() {
  return (
    <section className={cn(sectionClass, "pt-16 lg:pt-20 lg:pb-16")}>
      <a
        className={cn(
          fontMono,
          focusClass,
          "mb-6 inline-flex max-w-full flex-wrap items-center gap-x-2.5 gap-y-1 rounded-full border border-[#9be7f4] bg-[#ecfbfd] px-3 py-2 text-xs font-medium uppercase tracking-[0.04em] text-[#066f86] transition-colors hover:border-[#08b5d6] hover:bg-[#dff8fc] hover:text-[#034e61]",
        )}
        href="/blog/introducing-lix"
      >
        <span className="h-1.5 w-1.5 shrink-0 rounded-full bg-[#08b5d6]" />
        <span>Lix v0.6 SDK was released</span>
        <span className="text-[#034e61]">Read the announcement →</span>
      </a>
      <h1
        className={cn(
          displayClass,
          "max-w-[1040px] text-[clamp(42px,10.5vw,54px)] md:text-[clamp(50px,5.25vw,66px)] [&_br]:max-md:hidden",
        )}
      >
        An embeddable version
        <br /> control system for AI agents.
      </h1>
      <p className="mt-6 max-w-[760px] text-[clamp(18px,1.8vw,20px)] leading-[1.35] text-[#2b2b2b]">
        Give agents branches, checkpoints, semantic diffs, and rollback without
        dealing with Git internals.
      </p>
      <div className="mt-7 flex flex-wrap items-center gap-3 max-md:items-stretch">
        <Button href={npmUrl}>
          <span className="!text-[#08b5d6] [-webkit-text-fill-color:#08b5d6]">
            $
          </span>
          {" npm install @lix-js/sdk"}
        </Button>
        <Button href={githubUrl} variant="ghost">
          <GitHubIcon className="h-[17px] w-[17px]" />
          <StarCount />
        </Button>
      </div>
      <LangTabs />
    </section>
  );
}

function StatsStrip() {
  return (
    <section className="border-y border-[#e7e6e1]">
      <div className="mx-auto grid w-full max-w-[1280px] grid-cols-2 px-6 md:px-8 lg:grid-cols-4 lg:px-14">
        {stats.map(([label, sub]) => (
          <div
            className={cn(
              fontMono,
              "min-w-0 border-r border-[#e7e6e1] px-4 py-6 max-lg:[&:nth-child(2)]:border-r-0 max-lg:[&:nth-child(-n+2)]:border-b lg:last:border-r-0",
            )}
            key={label}
          >
            <div className="text-lg text-[#0a0a0a]">{label}</div>
            <span className="mt-1 block text-xs leading-[1.35] text-[#6b6b66]">
              {sub}
            </span>
          </div>
        ))}
      </div>
    </section>
  );
}

function WhyBuilt() {
  return (
    <section
      className={cn(
        sectionClass,
        "grid gap-10 lg:grid-cols-[260px_minmax(0,1fr)] lg:gap-16",
      )}
    >
      <SectionHeader
        eyebrow="§01 / why we built it"
        title={
          <>
            Git wasn't designed
            <br /> to be embedded.
          </>
        }
      />
      <div className="max-w-[740px]">
        <p className="mb-[22px] text-[19px] leading-[1.55] text-[#2b2b2b]">
          AI agents are creating an explosion in version-control demand:
          isolated workspaces, branchable explore steps, reviewable changes,
          rollback. They need versioning as{" "}
          <em className={cn(fontMono, "not-italic")}>infrastructure</em>, not as
          a CLI.
        </p>
        <p className="mb-[22px] text-[19px] leading-[1.55] text-[#2b2b2b]">
          Teams reach for Git - and end up managing a sidecar that was never
          meant to live inside their app:
        </p>
        <div className="mb-[22px] grid max-w-[720px] grid-cols-1 gap-2.5 md:grid-cols-2">
          {overhead.map((item) => (
            <div
              className={cn(
                fontMono,
                "flex min-w-0 gap-2 rounded-md border border-[#e7e6e1] px-3.5 py-2.5 text-xs leading-[1.4] text-[#2b2b2b]",
              )}
              key={item}
            >
              <span className="text-[#6b6b66]">-</span>
              {item}
            </div>
          ))}
        </div>
        <p className="text-[17px] leading-[1.55] text-[#0a0a0a]">
          <strong className="font-medium">
            Lix is built the other way around.
          </strong>{" "}
          Sessions, not worktrees. A SQL engine you embed, with semantic diffs
          and branches as first-class state - backed by infrastructure you
          already run.
        </p>
      </div>
    </section>
  );
}

function Inside() {
  return (
    <section className={sectionClass}>
      <SectionHeader
        eyebrow="§02 / what's inside v0.6"
        title="What you get with Lix."
      />
      <div className="mt-12 border-t border-[#e7e6e1]">
        {features.map((feature) => (
          <article
            className="grid grid-cols-1 gap-4 border-b border-[#e7e6e1] py-[30px] md:grid-cols-[48px_minmax(0,1fr)] md:gap-6 lg:grid-cols-[60px_minmax(0,1fr)_minmax(0,1.1fr)] lg:gap-10"
            key={feature.tag}
          >
            <div className={cn(fontMono, "pt-1.5 text-xs text-[#6b6b66]")}>
              {feature.tag}
            </div>
            <div>
              <h3 className="m-0 text-[22px] font-medium tracking-[-0.02em] text-[#0a0a0a]">
                {feature.title}
              </h3>
              <p className="mt-2.5 max-w-[460px] text-[15.5px] leading-[1.6] text-[#2b2b2b]">
                {feature.body}
              </p>
              {"visual" in feature && feature.visual === "sql-query" ? (
                <AgentQuestionCard />
              ) : null}
              {"visual" in feature && feature.visual === "backend" ? (
                <BackendLogos />
              ) : null}
            </div>
            <div className="md:col-start-2 lg:col-start-auto">
              <FeatureVisual feature={feature} />
            </div>
          </article>
        ))}
      </div>
    </section>
  );
}

function Patterns() {
  return (
    <section className={sectionClass}>
      <div className="mb-12 block gap-8 md:flex md:items-end md:justify-between">
        <SectionHeader
          eyebrow="§03 / what teams build with lix"
          title="What you can build."
        />
        <p
          className={cn(
            fontMono,
            "mt-[18px] max-w-[280px] text-left text-xs leading-normal text-[#6b6b66] md:mt-0 md:text-right",
          )}
        >
          v0.6 ships the engine.
          <br /> new shapes land in every release.
        </p>
      </div>
      <div className="grid grid-cols-1 border-l border-t border-[#e7e6e1] md:grid-cols-2">
        {patterns.map((pattern) => (
          <article
            className="min-w-0 border-b border-r border-[#e7e6e1] px-6 py-[30px] md:p-8 md:pb-9"
            key={pattern.kind}
          >
            <div className={cn(fontMono, "text-xs text-[#066f86]")}>
              {pattern.kind}
            </div>
            <h3 className="mt-3 text-[26px] font-medium tracking-[-0.02em] text-[#0a0a0a]">
              {pattern.title}
            </h3>
            <p className="mt-3 max-w-[480px] text-base leading-[1.55] text-[#2b2b2b]">
              {pattern.body}
            </p>
            <div className={cn(fontMono, "mt-[18px] flex flex-wrap gap-2")}>
              {pattern.stack.map((item) => (
                <span
                  className="inline-flex min-h-[26px] items-center rounded-full border border-[#e7e6e1] px-2.5 text-[11.5px] text-[#2b2b2b]"
                  key={item}
                >
                  {item}
                </span>
              ))}
            </div>
          </article>
        ))}
      </div>
    </section>
  );
}

function Roadmap() {
  return (
    <section
      className={cn(
        sectionClass,
        "grid gap-10 lg:grid-cols-[260px_minmax(0,1fr)] lg:gap-16",
      )}
    >
      <div>
        <SectionHeader eyebrow="§04 / roadmap" title="Roadmap." />
      </div>
      <div className="relative pl-8">
        <div
          className="absolute bottom-3 left-[9px] top-3 w-px bg-[#d7d5ce]"
          aria-hidden="true"
        />
        <RoadmapMilestone
          title="now / v0.6"
          subtitle="Lix SDK"
          items={shipped}
          shipped
        />
        <RoadmapMilestone title="v0.7" subtitle="Lix CLI" items={roadmap07} />
        <RoadmapMilestone
          title="v0.8"
          subtitle="file plugin API"
          items={roadmap08}
        />
        <RoadmapMilestone
          title="v0.9"
          subtitle="merge conflicts"
          items={roadmap09}
        />
        <RoadmapMilestone
          title="v0.10"
          subtitle="working changes"
          items={roadmap10}
        />
      </div>
    </section>
  );
}

function RoadmapMilestone({
  title,
  subtitle,
  items,
  shipped = false,
}: {
  title: string;
  subtitle: string;
  items: string[];
  shipped?: boolean;
}) {
  return (
    <div className="relative border-b border-[#e7e6e1] py-6 first:pt-0 last:border-b-0 last:pb-0">
      <span
        className={cn(
          "absolute -left-[28px] top-7 h-5 w-5 rounded-full border-2 bg-[#fbfaf7]",
          shipped ? "border-[#08b5d6]" : "border-[#a9a69d]",
        )}
        aria-hidden="true"
      />
      <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
        <h3
          className={cn(
            fontMono,
            "m-0 text-[11.5px] font-medium uppercase tracking-[0.04em]",
            shipped ? "text-[#066f86]" : "text-[#6b6b66]",
          )}
        >
          {title}
        </h3>
        <p className={cn(fontMono, "m-0 text-[11.5px] text-[#6b6b66]")}>
          {subtitle}
        </p>
      </div>
      <ul className="mt-3.5 grid list-none gap-2.5 p-0 md:grid-cols-2">
        {items.map((item) => (
          <li
            className={cn(
              "flex gap-2 text-[14.5px] leading-[1.55]",
              shipped ? "text-[#0a0a0a]" : "text-[#2b2b2b]",
            )}
            key={item}
          >
            <span className={shipped ? "text-[#066f86]" : "text-[#6b6b66]"}>
              {shipped ? "✓" : "○"}
            </span>
            {item}
          </li>
        ))}
      </ul>
    </div>
  );
}

function GetStarted() {
  return (
    <section
      className={cn(
        sectionClass,
        "grid items-center gap-10 py-16 lg:grid-cols-2 lg:gap-16 lg:py-24",
      )}
    >
      <div>
        <SectionHeader
          eyebrow="install"
          title={
            <span className="block max-w-[540px] text-[clamp(44px,5.2vw,60px)]">
              Get started
              <br /> in a minute.
            </span>
          }
        />
        <p className="mt-[18px] max-w-[460px] text-[17px] leading-normal text-[#2b2b2b]">
          Install. Open a Lix with the default in-memory backend. Write
          something, branch it, query its history. Plug in Postgres or S3 when
          you outgrow it.
        </p>
        <div className="mt-8 flex flex-wrap items-center gap-3 max-md:items-stretch">
          <Button href={npmUrl}>npm install @lix-js/sdk</Button>
          <Button href={githubUrl} variant="ghost">
            <GitHubIcon className="h-[17px] w-[17px]" />
            <StarCount />
          </Button>
          <a
            className={cn(
              fontMono,
              focusClass,
              "text-[13px] text-[#6b6b66] hover:text-[#0a0a0a] max-md:w-full",
            )}
            href="/docs/what-is-lix"
          >
            Read the docs →
          </a>
        </div>
        <div
          className={cn(
            fontMono,
            "mt-[18px] text-xs leading-normal text-[#6b6b66]",
          )}
        >
          MIT · TypeScript · Node, Deno, Bun, Tokio, edge runtimes.
        </div>
      </div>
      <CodeBlock>
        {
          <>
            <span className="text-[#6b6b66]"># 1 · install</span>
            {"\n"}$ npm install @lix-js/sdk{"\n\n"}
            <span className="text-[#6b6b66]"># 2 · open</span>
            {"\n"}
            <span className="text-[#b65aff]">import</span>
            {" { "}
            <span className="text-[#2b2b2b]">openLix</span>
            {" } "}
            <span className="text-[#b65aff]">from</span>{" "}
            <span className="text-[#058a3e]">"@lix-js/sdk"</span>;{"\n"}
            <span className="text-[#b65aff]">const</span> lix ={" "}
            <span className="text-[#b65aff]">await</span>{" "}
            <span className="text-[#066f86]">openLix</span>();{"\n\n"}
            <span className="text-[#6b6b66]"># 3 · write & commit</span>
            {"\n"}
            <span className="text-[#b65aff]">await</span> lix.
            <span className="text-[#066f86]">fs</span>.
            <span className="text-[#066f86]">writeFile</span>(
            <span className="text-[#058a3e]">"/hello.json"</span>, payload);
            {"\n\n"}
            <span className="text-[#6b6b66]"># 4 · branch & ask</span>
            {"\n"}
            <span className="text-[#b65aff]">const</span> b ={" "}
            <span className="text-[#b65aff]">await</span> lix.
            <span className="text-[#066f86]">branch</span>(
            <span className="text-[#058a3e]">"draft"</span>);{"\n"}
            <span className="text-[#b65aff]">const</span> diff ={" "}
            <span className="text-[#b65aff]">await</span> lix.
            <span className="text-[#066f86]">diff</span>({"{ "}from:{" "}
            <span className="text-[#058a3e]">"main"</span>, to: b {"}"});{"\n"}
          </>
        }
      </CodeBlock>
    </section>
  );
}

export function V2Footer() {
  return (
    <footer
      className={cn(
        fontMono,
        "mx-auto flex w-full max-w-[1280px] items-end justify-between gap-8 border-t border-[#e7e6e1] px-6 pb-12 pt-9 text-xs text-[#6b6b66] max-md:block lg:px-14 lg:pb-14",
      )}
    >
      <div>
        <LixLogo className="mb-2 h-auto w-[26px] text-[#08b5d6]" />
        <p>© 2026 Opral US Inc.</p>
      </div>
      <nav
        className="flex flex-wrap justify-end gap-6 text-[13px] max-md:mt-7 max-md:justify-start max-md:gap-4"
        aria-label="V2 footer navigation"
      >
        <a href="/docs/what-is-lix">docs</a>
        <a href={githubUrl} target="_blank" rel="noopener noreferrer">
          github
        </a>
        <a
          href="https://discord.gg/gdMPPWy57R"
          target="_blank"
          rel="noopener noreferrer"
        >
          discord
        </a>
        <a href="/blog">blog</a>
        <a href="/blog/april-2026-update">changelog</a>
      </nav>
    </footer>
  );
}

export function V2LandingPage() {
  return (
    <div
      className={cn(
        fontSans,
        "bg-[#fafaf7] text-[#0a0a0a] tracking-[-0.005em] [font-feature-settings:'ss01','ss02','cv11']",
      )}
    >
      <main>
        <V2Hero />
        <StatsStrip />
        <WhyBuilt />
        <Inside />
        <Patterns />
        <Roadmap />
        <GetStarted />
      </main>
      <V2Footer />
    </div>
  );
}
