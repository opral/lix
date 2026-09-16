import { execFileSync } from "node:child_process";
import { appendFileSync, readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

// These independent site/content trees are not compiler inputs. Do not exempt
// arbitrary Markdown: packages/lix/src/init_readme.md is embedded in the engine.
export const isContentPath = path => /^(blog|docs|website)\//.test(path) && !/[\r\n]/.test(path);

export function selectContentScope({ eventName, event, cwd = process.cwd() }) {
  try {
    const git = (...args) => execFileSync("git", args, { cwd, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], maxBuffer: 16 * 1024 * 1024 });
    const head = git("rev-parse", "HEAD").trim();
    let base;
    if (eventName === "pull_request") {
      const parents = git("rev-list", "--parents", "-n", "1", "HEAD").trim().split(/\s+/);
      if (parents.length !== 3 || parents[2] !== event?.pull_request?.head?.sha) return false;
      base = parents[1];
    } else if (eventName === "push") {
      base = event?.before;
      if (!/^[a-f0-9]{40}$/.test(base ?? "") || /^0+$/.test(base) || event?.after !== head || event?.forced) return false;
      // A multi-commit push must include every commit, not just HEAD's parent.
      git("merge-base", "--is-ancestor", base, head);
    } else return false;
    const paths = git("diff", "--name-only", "--no-renames", "-z", base, head, "--").split("\0").filter(Boolean);
    return paths.length > 0 && paths.every(isContentPath);
  } catch { return false; }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const contentOnly = selectContentScope({ eventName: process.env.GITHUB_EVENT_NAME, event: JSON.parse(readFileSync(process.env.GITHUB_EVENT_PATH, "utf8")) });
  appendFileSync(process.env.GITHUB_OUTPUT, `content_only=${contentOnly}\n`);
  console.log(contentOnly ? "Content/site-only change: skip Rust, SDK and server builds; retain content checks and website validation." : "Code/unknown inputs or uncertain history: retain code validation.");
}
