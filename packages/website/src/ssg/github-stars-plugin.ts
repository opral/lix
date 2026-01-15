import fs from "node:fs/promises";
import { fileURLToPath } from "node:url";

export type GithubRepoMetrics = {
  stars: number;
  forks: number;
  openIssues: number;
  closedIssues: number;
  contributorCount: number;
};

type GithubCache = {
  generatedAt: string;
  data: Record<string, GithubRepoMetrics | null>;
};

const GITHUB_CACHE_TTL_MINUTES = 60;
const githubCachePath = fileURLToPath(
  new URL("../github_repo_data.gen.json", import.meta.url),
);
let didLogGithubToken = false;

export function githubStarsPlugin({ token }: { token?: string }) {
  return {
    name: "lix:github-data",
    async buildStart() {
      await ensureGithubCache(token);
    },
    async configureServer() {
      await ensureGithubCache(token);
    },
  };
}

async function ensureGithubCache(token?: string) {
  if (token && !didLogGithubToken) {
    console.info("Using LIX_WEBSITE_GITHUB_TOKEN for GitHub API requests.");
    didLogGithubToken = true;
  }
  const cached = await readGithubCache();
  if (cached && !isCacheExpired(cached)) return;

  const repos = new Set<string>(["opral/lix"]);

  const data: Record<string, GithubRepoMetrics | null> = {};
  for (const repo of repos) {
    const metrics = await fetchGithubRepoMetrics(repo, token);
    data[repo.toLowerCase()] = metrics;
  }

  const payload: GithubCache = {
    generatedAt: new Date().toISOString(),
    data,
  };

  await fs.writeFile(githubCachePath, JSON.stringify(payload, null, 2) + "\n");
}

async function readGithubCache(): Promise<GithubCache | null> {
  try {
    const raw = await fs.readFile(githubCachePath, "utf8");
    return JSON.parse(raw) as GithubCache;
  } catch {
    return null;
  }
}

function isCacheExpired(cache: GithubCache) {
  const generatedAt = Date.parse(cache.generatedAt);
  if (Number.isNaN(generatedAt)) return true;
  const ttlMs = GITHUB_CACHE_TTL_MINUTES * 60 * 1000;
  return Date.now() - generatedAt > ttlMs;
}

function getHeaders(token?: string) {
  return {
    Accept: "application/vnd.github+json",
    "User-Agent": "lix-website",
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
  };
}

async function fetchGithubRepoMetrics(
  repo: string,
  token?: string,
): Promise<GithubRepoMetrics | null> {
  try {
    const repoRes = await fetch(`https://api.github.com/repos/${repo}`, {
      headers: getHeaders(token),
    });

    if (!repoRes.ok) {
      console.warn(`GitHub repo fetch failed for ${repo}: ${repoRes.status}`);
      return null;
    }

    const repoData = (await repoRes.json()) as {
      stargazers_count?: number;
      forks_count?: number;
      open_issues_count?: number;
    };

    const openIssuesRes = await fetch(
      `https://api.github.com/search/issues?q=repo:${repo}+is:issue+is:open&per_page=1`,
      { headers: getHeaders(token) },
    );
    const closedIssuesRes = await fetch(
      `https://api.github.com/search/issues?q=repo:${repo}+is:issue+is:closed&per_page=1`,
      { headers: getHeaders(token) },
    );

    let openIssues = 0;
    if (openIssuesRes.ok) {
      const openData = (await openIssuesRes.json()) as {
        total_count?: number;
      };
      openIssues = openData.total_count ?? 0;
    }

    let closedIssues = 0;
    if (closedIssuesRes.ok) {
      const closedData = (await closedIssuesRes.json()) as {
        total_count?: number;
      };
      closedIssues = closedData.total_count ?? 0;
    }

    const contributorsRes = await fetch(
      `https://api.github.com/repos/${repo}/contributors?per_page=1&anon=1`,
      { headers: getHeaders(token) },
    );

    let contributorCount = 0;
    if (contributorsRes.ok) {
      const linkHeader = contributorsRes.headers.get("Link");
      if (linkHeader) {
        const lastMatch = linkHeader.match(/page=(\d+)>; rel="last"/);
        if (lastMatch) {
          contributorCount = parseInt(lastMatch[1], 10);
        }
      } else {
        const data = (await contributorsRes.json()) as unknown[];
        contributorCount = data.length;
      }
    }

    return {
      stars: repoData.stargazers_count ?? 0,
      forks: repoData.forks_count ?? 0,
      openIssues,
      closedIssues,
      contributorCount,
    };
  } catch (error) {
    console.warn(`GitHub fetch failed for ${repo}`, error);
    return null;
  }
}
