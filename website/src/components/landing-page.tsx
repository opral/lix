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
      className={`flex h-12 cursor-pointer items-center gap-3.5 rounded-lg border border-[#DDDBD3] px-4 font-mono text-[14.5px] text-ink transition-colors hover:border-cyan-bright ${
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
    title: "Keep normal files",
    description:
      "Existing tools and agents can keep reading and writing files on disk.",
    icon: (
      <svg viewBox="0 0 80 56" className="block h-14 w-20" aria-hidden="true">
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
    title: "Query everything with SQL",
    description:
      "Query file content, app data, and change history without rereading whole files.",
    icon: (
      <svg viewBox="0 0 80 56" className="block h-14 w-20" aria-hidden="true">
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
    title: "Track semantic changes",
    description:
      "Review the paragraph, CSV record, property, or app row that changed.",
    icon: (
      <svg viewBox="0 0 80 56" className="block h-14 w-20" aria-hidden="true">
        <line
          x1="16"
          y1="14"
          x2="64"
          y2="14"
          stroke="#C9C7BF"
          strokeWidth="2"
          strokeLinecap="round"
        />
        <line
          x1="16"
          y1="24"
          x2="52"
          y2="24"
          stroke="#C9C7BF"
          strokeWidth="2"
          strokeLinecap="round"
        />
        <line
          x1="16"
          y1="34"
          x2="30"
          y2="34"
          stroke="#C9C7BF"
          strokeWidth="2"
          strokeLinecap="round"
        />
        <line
          x1="35"
          y1="34"
          x2="52"
          y2="34"
          stroke="#07B6D5"
          strokeWidth="2"
          strokeLinecap="round"
        />
        <line
          x1="57"
          y1="34"
          x2="64"
          y2="34"
          stroke="#C9C7BF"
          strokeWidth="2"
          strokeLinecap="round"
        />
        <line
          x1="16"
          y1="44"
          x2="58"
          y2="44"
          stroke="#C9C7BF"
          strokeWidth="2"
          strokeLinecap="round"
        />
      </svg>
    ),
  },
  {
    title: "Branch and merge safely",
    description:
      "Give every user or agent an isolated repository, then review and merge its work.",
    icon: (
      <svg viewBox="0 0 80 56" className="block h-14 w-20" aria-hidden="true">
        <path
          d="M14 40 H66"
          fill="none"
          stroke="#C9C7BF"
          strokeWidth="2"
          strokeLinecap="round"
        />
        <path
          d="M26 40 C32 40 32 18 40 18 H46 C56 18 54 40 60 40"
          fill="none"
          stroke="#07B6D5"
          strokeWidth="2"
          strokeLinecap="round"
        />
        <circle cx="14" cy="40" r="3.5" fill="#8A8F96" />
        <circle cx="43" cy="18" r="3.5" fill="#07B6D5" />
        <circle cx="66" cy="40" r="3.5" fill="#8A8F96" />
      </svg>
    ),
  },
  {
    title: "Use ACID transactions",
    description:
      "Update files and rows together while Lix records their history.",
    icon: (
      <svg viewBox="0 0 80 56" className="block h-14 w-20" aria-hidden="true">
        <rect
          x="12"
          y="10"
          width="56"
          height="36"
          rx="6"
          fill="none"
          stroke="#C9C7BF"
          strokeWidth="1.5"
          strokeDasharray="4 4"
        />
        <rect
          x="20"
          y="19"
          width="18"
          height="18"
          rx="3"
          fill="#FFFFFF"
          stroke="#8A8F96"
          strokeWidth="1.5"
        />
        <rect
          x="44"
          y="19"
          width="18"
          height="18"
          rx="3"
          fill="#FFFFFF"
          stroke="#8A8F96"
          strokeWidth="1.5"
        />
        <line
          x1="38"
          y1="28"
          x2="44"
          y2="28"
          stroke="#07B6D5"
          strokeWidth="2"
          strokeLinecap="round"
        />
      </svg>
    ),
  },
  {
    title: "Run locally or remotely",
    description:
      "Embed Lix in an app or connect to a shared repository through the server protocol.",
    icon: (
      <svg viewBox="0 0 80 56" className="block h-14 w-20" aria-hidden="true">
        <rect
          x="12"
          y="20"
          width="16"
          height="16"
          rx="3"
          fill="#FFFFFF"
          stroke="#8A8F96"
          strokeWidth="1.5"
        />
        <rect
          x="52"
          y="20"
          width="16"
          height="16"
          rx="3"
          fill="#FFFFFF"
          stroke="#8A8F96"
          strokeWidth="1.5"
        />
        <line
          x1="28"
          y1="28"
          x2="52"
          y2="28"
          stroke="#C9C7BF"
          strokeWidth="1.5"
          strokeDasharray="3 4"
          strokeLinecap="round"
        />
        <circle cx="40" cy="28" r="3" fill="#07B6D5" />
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
        <section className="max-w-[820px] pt-[104px]">
          <p className="mb-[26px] font-mono text-[12.5px] uppercase tracking-[0.08em] text-ink-faint">
            Open source · MIT
          </p>
          <h1 className="text-balance text-[40px] font-bold leading-[1.04] tracking-[-0.035em] sm:text-[60px]">
            Database, filesystem and version control in one system
          </h1>
          <div className="mt-[30px] flex max-w-[660px] flex-col gap-[18px]">
            <p className="text-xl leading-[1.55] text-ink-secondary">
              Agents and tools work with files. Applications need a database.
              Teams need version control.
            </p>
            <p className="text-xl leading-[1.55] text-ink-secondary">
              Lix combines all three: normal files for tools, SQL rows for apps,
              and version control for every change.
            </p>
          </div>
          <div className="mt-10 flex flex-wrap items-center gap-3.5">
            <CopyInstallButton background="white" />
            <a
              href="/docs/what-is-lix"
              className="flex h-12 items-center rounded-lg bg-ink px-5 text-[14.5px] font-semibold text-paper transition-colors hover:text-paper"
            >
              Read the docs
            </a>
          </div>
        </section>

        {/* Stats */}
        <section className="mt-[84px] flex flex-wrap gap-14 border-y border-line py-7">
          <div className="flex flex-col gap-1.5">
            <span className="text-3xl font-bold leading-none tracking-[-0.02em]">
              {coarseWeeklyDownloads()}
            </span>
            <span className="text-sm text-ink-muted">weekly downloads</span>
          </div>
          {githubStars !== null && (
            <a
              href="https://github.com/opral/lix"
              target="_blank"
              rel="noopener noreferrer"
              className="group flex flex-col gap-1.5"
            >
              <span className="text-3xl font-bold leading-none tracking-[-0.02em] transition-colors group-hover:text-cyan-deep">
                {githubStars.toLocaleString("en-US")}
              </span>
              <span className="text-sm text-ink-muted">GitHub stars</span>
            </a>
          )}
        </section>

        {/* Adoption chart */}
        <DownloadsChart />

        {/* What you get */}
        <section className="pt-24">
          <h2 className="mb-1 text-[34px] font-bold tracking-[-0.028em]">
            What you get
          </h2>
          <div className="mt-9">
            {whatYouGet.map((item, index) => (
              <div
                key={item.title}
                className={`grid grid-cols-1 items-center gap-4 border-t border-line py-5 sm:grid-cols-[104px_224px_1fr] sm:gap-8 ${
                  index === whatYouGet.length - 1 ? "border-b" : ""
                }`}
              >
                {item.icon}
                <span className="text-[17px] font-semibold tracking-[-0.01em]">
                  {item.title}
                </span>
                <span className="text-[17px] leading-[1.6] text-ink-secondary">
                  {item.description}
                </span>
              </div>
            ))}
          </div>
        </section>

        {/* README */}
        {readmeHtml && (
          <section className="pt-[88px]">
            <div className="overflow-hidden rounded-xl border border-line bg-white">
              <div className="flex flex-wrap items-center justify-between gap-5 border-b border-line-soft bg-[#FDFCFA] px-7 py-3.5">
                <span className="font-mono text-[12.5px] text-ink-faint">
                  README.md · opral/lix
                </span>
                <a
                  href="https://github.com/opral/lix"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="font-mono text-[12.5px] text-ink-muted transition-colors hover:text-cyan-deep"
                >
                  view on GitHub →
                </a>
              </div>
              <div className="px-6 pb-12 pt-10 sm:px-12">
                <article
                  className="markdown-wc-body"
                  dangerouslySetInnerHTML={{ __html: readmeHtml }}
                />
              </div>
            </div>
          </section>
        )}

        {/* CTA */}
        <section className="pb-[104px] pt-24">
          <div className="flex flex-wrap items-center justify-between gap-10 rounded-xl border border-line bg-white px-6 py-11 sm:px-12">
            <div className="flex flex-col gap-2.5">
              <h2 className="text-[26px] font-bold tracking-[-0.025em]">
                Start with the SDK
              </h2>
              <p className="text-base text-ink-muted">
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
