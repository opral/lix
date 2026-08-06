import { useState } from "react";
import { getGithubStars } from "../github-stars-cache";
import { DownloadsChart, coarseWeeklyDownloads } from "./downloads-chart";
import { Footer } from "./footer";
import { Header } from "./header";

const INSTALL_COMMAND = "npm install @lix-js/sdk";

/**
 * Copyable `npm install` command button.
 *
 * @example
 * <CopyInstallButton background="white" />
 */
function CopyInstallButton({
  background = "white",
}: {
  background?: "white" | "paper";
}) {
  const [copied, setCopied] = useState(false);

  const copy = () => {
    if (navigator.clipboard) navigator.clipboard.writeText(INSTALL_COMMAND);
    setCopied(true);
    setTimeout(() => setCopied(false), 1600);
  };

  return (
    <button
      onClick={copy}
      className={`flex h-10 cursor-pointer items-center gap-3 rounded-lg border border-[#DDDBD3] px-3.5 font-mono text-[13.5px] text-ink transition-colors hover:border-cyan-bright ${
        background === "white"
          ? "bg-white shadow-[0_1px_2px_rgba(20,23,26,0.04)]"
          : "bg-paper"
      }`}
    >
      <span className="text-[#A8ACB2]">$</span>
      <span>{INSTALL_COMMAND}</span>
      <span className="ml-1.5 font-sans text-xs font-semibold uppercase tracking-[0.04em] text-cyan-deep">
        {copied ? "copied" : "copy"}
      </span>
    </button>
  );
}

const whatYouGet = [
  {
    title: "Works with any file format",
    description:
      "Companies produce DOCX, XLSX, and CAD, not just text. Plugins map any format to versioned entities.",
    icon: (
      <svg viewBox="0 0 80 56" className="block h-11 w-16" aria-hidden="true">
        <rect
          x="28"
          y="6"
          width="30"
          height="38"
          rx="3"
          fill="#FBFAF7"
          stroke="#C9C7BF"
          strokeWidth="1.5"
        />
        <rect
          x="24"
          y="10"
          width="30"
          height="38"
          rx="3"
          fill="#FBFAF7"
          stroke="#C9C7BF"
          strokeWidth="1.5"
        />
        <rect
          x="20"
          y="14"
          width="30"
          height="38"
          rx="3"
          fill="#FFFFFF"
          stroke="#8A8F96"
          strokeWidth="1.5"
        />
        <line
          x1="26"
          y1="24"
          x2="44"
          y2="24"
          stroke="#C9C7BF"
          strokeWidth="1.5"
          strokeLinecap="round"
        />
        <line
          x1="26"
          y1="31"
          x2="44"
          y2="31"
          stroke="#C9C7BF"
          strokeWidth="1.5"
          strokeLinecap="round"
        />
        <line
          x1="26"
          y1="38"
          x2="36"
          y2="38"
          stroke="#07B6D5"
          strokeWidth="1.5"
          strokeLinecap="round"
        />
      </svg>
    ),
  },
  {
    title: "Semantic changes",
    description:
      "A spreadsheet diff is a cell, not a byte blob. Review the clause, cell, or row that changed.",
    icon: (
      <svg viewBox="0 0 80 56" className="block h-11 w-16" aria-hidden="true">
        <rect
          x="16"
          y="10"
          width="14"
          height="10"
          rx="2"
          fill="none"
          stroke="#C9C7BF"
          strokeWidth="1.5"
        />
        <rect
          x="33"
          y="10"
          width="14"
          height="10"
          rx="2"
          fill="none"
          stroke="#C9C7BF"
          strokeWidth="1.5"
        />
        <rect
          x="50"
          y="10"
          width="14"
          height="10"
          rx="2"
          fill="none"
          stroke="#C9C7BF"
          strokeWidth="1.5"
        />
        <rect
          x="16"
          y="23"
          width="14"
          height="10"
          rx="2"
          fill="none"
          stroke="#C9C7BF"
          strokeWidth="1.5"
        />
        <rect
          x="33"
          y="23"
          width="14"
          height="10"
          rx="2"
          fill="rgba(7,182,213,0.14)"
          stroke="#07B6D5"
          strokeWidth="1.5"
        />
        <rect
          x="50"
          y="23"
          width="14"
          height="10"
          rx="2"
          fill="none"
          stroke="#C9C7BF"
          strokeWidth="1.5"
        />
        <rect
          x="16"
          y="36"
          width="14"
          height="10"
          rx="2"
          fill="none"
          stroke="#C9C7BF"
          strokeWidth="1.5"
        />
        <rect
          x="33"
          y="36"
          width="14"
          height="10"
          rx="2"
          fill="none"
          stroke="#C9C7BF"
          strokeWidth="1.5"
        />
        <rect
          x="50"
          y="36"
          width="14"
          height="10"
          rx="2"
          fill="none"
          stroke="#C9C7BF"
          strokeWidth="1.5"
        />
      </svg>
    ),
  },
  {
    title: "SQL and transactions",
    description:
      "File content, app data, and history live in an ACID OLTP database. Query millions of rows with SQL.",
    icon: (
      <svg viewBox="0 0 80 56" className="block h-11 w-16" aria-hidden="true">
        <ellipse
          cx="40"
          cy="15"
          rx="19"
          ry="6"
          fill="#FFFFFF"
          stroke="#8A8F96"
          strokeWidth="1.5"
        />
        <path
          d="M21 15 V41 C21 44.3 29.5 47 40 47 C50.5 47 59 44.3 59 41 V15"
          fill="none"
          stroke="#8A8F96"
          strokeWidth="1.5"
        />
        <path
          d="M21 28 C21 31.3 29.5 34 40 34 C50.5 34 59 31.3 59 28"
          fill="none"
          stroke="#C9C7BF"
          strokeWidth="1.5"
        />
        <line
          x1="33"
          y1="40"
          x2="47"
          y2="40"
          stroke="#07B6D5"
          strokeWidth="1.5"
          strokeLinecap="round"
        />
      </svg>
    ),
  },
  {
    title: "Real-time collaboration",
    description:
      "Companies work live, not in pull requests. People and agents share a repository and see changes as they happen.",
    icon: (
      <svg viewBox="0 0 80 56" className="block h-11 w-16" aria-hidden="true">
        <rect
          x="18"
          y="10"
          width="44"
          height="36"
          rx="4"
          fill="#FFFFFF"
          stroke="#8A8F96"
          strokeWidth="1.5"
        />
        <line
          x1="26"
          y1="20"
          x2="54"
          y2="20"
          stroke="#C9C7BF"
          strokeWidth="1.5"
          strokeLinecap="round"
        />
        <line
          x1="26"
          y1="28"
          x2="46"
          y2="28"
          stroke="#C9C7BF"
          strokeWidth="1.5"
          strokeLinecap="round"
        />
        <path d="M32 34 L32 42 L38 38.5 Z" fill="#07B6D5" />
        <path d="M50 24 L50 32 L56 28.5 Z" fill="#8A8F96" />
      </svg>
    ),
  },
  {
    title: "Checkpoints instead of commits",
    description:
      "Non-developers expect automatic saving, not commits. Lix records every change; a checkpoint marks a state you want to return to.",
    icon: (
      <svg viewBox="0 0 80 56" className="block h-11 w-16" aria-hidden="true">
        <line
          x1="12"
          y1="34"
          x2="68"
          y2="34"
          stroke="#C9C7BF"
          strokeWidth="1.5"
        />
        <circle cx="20" cy="34" r="2.5" fill="#C9C7BF" />
        <circle cx="32" cy="34" r="2.5" fill="#C9C7BF" />
        <circle cx="44" cy="34" r="2.5" fill="#C9C7BF" />
        <circle cx="56" cy="34" r="3.5" fill="#07B6D5" />
        <line
          x1="56"
          y1="30"
          x2="56"
          y2="16"
          stroke="#07B6D5"
          strokeWidth="1.5"
        />
        <path
          d="M56 16 L66 19 L56 22 Z"
          fill="rgba(7,182,213,0.14)"
          stroke="#07B6D5"
          strokeWidth="1.5"
          strokeLinejoin="round"
        />
      </svg>
    ),
  },
  {
    title: "Pluggable storage",
    description:
      "Lix runs on pluggable storage: in memory, on the local filesystem, or on S3. Git assumes a POSIX filesystem, which makes it hard to embed and scale.",
    icon: (
      <svg viewBox="0 0 80 56" className="block h-11 w-16" aria-hidden="true">
        <rect
          x="32"
          y="8"
          width="16"
          height="14"
          rx="3"
          fill="#FFFFFF"
          stroke="#8A8F96"
          strokeWidth="1.5"
        />
        <line
          x1="40"
          y1="22"
          x2="40"
          y2="32"
          stroke="#C9C7BF"
          strokeWidth="1.5"
          strokeDasharray="3 3"
        />
        <rect
          x="14"
          y="34"
          width="16"
          height="14"
          rx="3"
          fill="none"
          stroke="#C9C7BF"
          strokeWidth="1.5"
        />
        <rect
          x="32"
          y="34"
          width="16"
          height="14"
          rx="3"
          fill="rgba(7,182,213,0.14)"
          stroke="#07B6D5"
          strokeWidth="1.5"
        />
        <rect
          x="50"
          y="34"
          width="16"
          height="14"
          rx="3"
          fill="none"
          stroke="#C9C7BF"
          strokeWidth="1.5"
        />
      </svg>
    ),
  },
  {
    title: "Permissions",
    badge: "soon",
    description:
      "Finance, legal, and contractors need different access. Lix models permissions per file and group inside the repository.",
    icon: (
      <svg viewBox="0 0 80 56" className="block h-11 w-16" aria-hidden="true">
        <path
          d="M31 26 v-6 a9 9 0 0 1 18 0 v6"
          fill="none"
          stroke="#8A8F96"
          strokeWidth="1.5"
        />
        <rect
          x="26"
          y="26"
          width="28"
          height="22"
          rx="4"
          fill="#FFFFFF"
          stroke="#8A8F96"
          strokeWidth="1.5"
        />
        <circle cx="40" cy="35" r="3" fill="#07B6D5" />
        <line
          x1="40"
          y1="37"
          x2="40"
          y2="42"
          stroke="#07B6D5"
          strokeWidth="1.5"
          strokeLinecap="round"
        />
      </svg>
    ),
  },
];

/**
 * Landing page for lix.dev.
 *
 * @example
 * <LandingPage readmeHtml={html} />
 */
function LandingPage({ readmeHtml }: { readmeHtml?: string }) {
  const githubStars = getGithubStars("opral/lix");

  return (
    <div className="min-h-screen bg-paper text-ink">
      <Header width="narrow" />
      <main className="mx-auto w-full max-w-[1100px] px-8">
        {/* Hero */}
        <section className="max-w-[720px] pt-16">
          <p className="mb-4 font-mono text-xs uppercase tracking-[0.08em] text-ink-faint">
            Open source · MIT
          </p>
          <h1 className="text-balance text-[30px] font-bold leading-[1.1] tracking-[-0.03em] sm:text-[40px]">
            A version control system beyond code
          </h1>
          <div className="mt-4 flex max-w-[620px] flex-col gap-3">
            <p className="text-base leading-[1.6] text-ink-secondary">
              Agents and tools work with files. Applications need a database.
              Teams need version control.
            </p>
            <p className="text-base leading-[1.6] text-ink-secondary">
              Lix combines all three: normal files for tools, SQL rows for apps,
              and version control for every change.
            </p>
          </div>
          <div className="mt-6 flex flex-wrap items-center gap-3">
            <CopyInstallButton background="white" />
            <a
              href="/docs/what-is-lix"
              className="flex h-10 items-center rounded-lg bg-ink px-4 text-[13.5px] font-semibold text-paper transition-colors hover:text-paper"
            >
              Read the docs
            </a>
          </div>
        </section>

        {/* Stats */}
        <section className="mt-12 flex flex-wrap gap-10 border-y border-line py-5">
          <div className="flex flex-col gap-1">
            <span className="text-[22px] font-bold leading-none tracking-[-0.02em]">
              {coarseWeeklyDownloads()}
            </span>
            <span className="text-[13px] text-ink-muted">weekly downloads</span>
          </div>
          {githubStars !== null && (
            <a
              href="https://github.com/opral/lix"
              target="_blank"
              rel="noopener noreferrer"
              className="group flex flex-col gap-1"
            >
              <span className="text-[22px] font-bold leading-none tracking-[-0.02em] transition-colors group-hover:text-cyan-deep">
                {githubStars.toLocaleString("en-US")}
              </span>
              <span className="text-[13px] text-ink-muted">GitHub stars</span>
            </a>
          )}
        </section>

        {/* Adoption chart */}
        <DownloadsChart />

        {/* What you get */}
        <section className="pt-14">
          <h2 className="mb-1 text-[22px] font-bold tracking-[-0.02em]">
            What you get
          </h2>
          <p className="mt-3 max-w-[640px] text-[15px] leading-[1.6] text-ink-secondary">
            Putting more than code into a repository needs seven things a code
            VCS never had:
          </p>
          <div className="mt-5">
            {whatYouGet.map((item, index) => (
              <div
                key={item.title}
                className={`grid grid-cols-1 items-center gap-3 border-t border-line py-3 sm:grid-cols-[84px_210px_1fr] sm:gap-7 ${
                  index === whatYouGet.length - 1 ? "border-b" : ""
                }`}
              >
                {item.icon}
                <span className="flex flex-wrap items-center gap-2 text-[15px] font-semibold tracking-[-0.01em]">
                  {item.title}
                  {"badge" in item && (
                    <span className="rounded border border-line-strong px-1.5 py-0.5 font-mono text-[10px] font-normal uppercase tracking-[0.08em] text-ink-muted">
                      {item.badge}
                    </span>
                  )}
                </span>
                <span className="text-[15px] leading-[1.6] text-ink-secondary">
                  {item.description}
                </span>
              </div>
            ))}
          </div>
        </section>

        {/* README */}
        {readmeHtml && (
          <section className="pt-14">
            <div className="overflow-hidden rounded-xl border border-line bg-white">
              <div className="flex flex-wrap items-center justify-between gap-5 border-b border-line-soft bg-[#FDFCFA] px-7 py-2.5">
                <span className="font-mono text-xs text-ink-faint">
                  README.md · opral/lix
                </span>
                <a
                  href="https://github.com/opral/lix"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="flex items-center gap-1.5 font-mono text-xs text-ink-muted transition-colors hover:text-cyan-deep"
                >
                  <svg
                    viewBox="0 0 24 24"
                    fill="currentColor"
                    className="h-3.5 w-3.5"
                    aria-hidden="true"
                  >
                    <path d="M12 2a10 10 0 00-3.16 19.49c.5.09.68-.21.68-.47v-1.69c-2.78.6-3.37-1.34-3.37-1.34a2.64 2.64 0 00-1.1-1.46c-.9-.62.07-.6.07-.6a2.08 2.08 0 011.52 1 2.1 2.1 0 002.87.82 2.11 2.11 0 01.63-1.32c-2.22-.25-4.56-1.11-4.56-4.95a3.88 3.88 0 011-2.7 3.6 3.6 0 01.1-2.67s.84-.27 2.75 1a9.5 9.5 0 015 0c1.91-1.29 2.75-1 2.75-1a3.6 3.6 0 01.1 2.67 3.87 3.87 0 011 2.7c0 3.85-2.34 4.7-4.57 4.95a2.37 2.37 0 01.68 1.84v2.72c0 .27.18.57.69.47A10 10 0 0012 2z" />
                  </svg>
                  view on GitHub →
                </a>
              </div>
              <div className="px-6 pb-10 pt-8 sm:px-10">
                <article
                  className="markdown-wc-body"
                  dangerouslySetInnerHTML={{ __html: readmeHtml }}
                />
              </div>
            </div>
          </section>
        )}

        {/* CTA */}
        <section className="pb-16 pt-14">
          <div className="flex flex-wrap items-center justify-between gap-8 rounded-xl border border-line bg-white px-6 py-7 sm:px-10">
            <div className="flex flex-col gap-1.5">
              <h2 className="text-[19px] font-bold tracking-[-0.02em]">
                Start with the SDK
              </h2>
              <p className="text-[14.5px] text-ink-muted">
                MIT licensed. Runs in the browser and on the server.
              </p>
            </div>
            <CopyInstallButton background="paper" />
          </div>
        </section>
      </main>
      <Footer width="narrow" />
    </div>
  );
}

export default LandingPage;
