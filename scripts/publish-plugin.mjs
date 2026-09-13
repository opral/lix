#!/usr/bin/env node
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { readFileSync, appendFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { PLUGIN_RELEASE_TARGETS, releaseTarget, releaseTag } from "./release.mjs";

const hash = bytes => createHash("sha256").update(bytes).digest("hex");
const git = (root, ...args) => execFileSync("git", args, { cwd: root, encoding: "utf8" }).trim();

export function pluginVersionAt(root, ref, target) {
  if (!PLUGIN_RELEASE_TARGETS.includes(target)) throw new Error(`Unknown plugin release target: ${target}`);
  const manifest = git(root, "show", `${ref}:${releaseTarget(target).path}/Cargo.toml`);
  const version = manifest.match(/\[package\][\s\S]*?\nversion\s*=\s*"(\d+\.\d+\.\d+)"/)?.[1];
  if (!version) throw new Error(`${target} must have an explicit release version at ${ref}`);
  return version;
}

export function selectPluginReleases(root, { sha, before, target }) {
  if (!/^[a-f0-9]{40}$/.test(sha)) throw new Error("Release source must be a full commit SHA");
  git(root, "merge-base", "--is-ancestor", sha, "origin/main");
  const targets = target ? [target] : PLUGIN_RELEASE_TARGETS;
  return targets.flatMap(key => {
    const version = pluginVersionAt(root, sha, key);
    {
      // The migration commit may replace workspace inheritance with an equal
      // explicit version; it must not accidentally publish every plugin.
      const path = releaseTarget(key).path;
      const old = git(root, "show", `${target ? `${sha}^` : before}:${path}/Cargo.toml`);
      let previous = old.match(/\[package\][\s\S]*?\nversion\s*=\s*"(\d+\.\d+\.\d+)"/)?.[1];
      if (!previous && /^version\.workspace\s*=\s*true$/m.test(old)) {
        const workspace = git(root, "show", `${target ? `${sha}^` : before}:Cargo.toml`);
        previous = workspace.match(/\[workspace\.package\][\s\S]*?\nversion\s*=\s*"(\d+\.\d+\.\d+)"/)?.[1];
      }
      if (!previous) throw new Error(`Cannot determine previous version for ${key}`);
      if (version === previous) {
        if (target) throw new Error("Manual retry source must be the commit that increased the plugin version");
        return [];
      }
      const a = version.split(".").map(Number), b = previous.split(".").map(Number);
      const changed = a.findIndex((value, index) => value !== b[index]);
      if (changed < 0 || a[changed] < b[changed]) throw new Error(`${key} release version must increase`);
    }
    return [{ target: key, version, sha, tag: releaseTag(key, version) }];
  });
}

async function absentOn404(call) {
  try { return await call(); } catch (error) { if (error.status === 404) return null; throw error; }
}

/** Publish only complete, content-verified plugin releases. Never replace assets. */
export async function publishPlugin({ github, owner, repo, target, version, sha, directory, root = process.cwd() }) {
  if (!PLUGIN_RELEASE_TARGETS.includes(target)) throw new Error(`Unknown plugin release target: ${target}`);
  const metadata = JSON.parse(readFileSync(join(directory, "release-metadata.json"), "utf8"));
  const fileName = `${target}.lixplugin`;
  if (metadata.key !== target || metadata.version !== version || metadata.fileName !== fileName || !Number.isInteger(metadata.apiMajor) || metadata.apiMajor < 1) {
    throw new Error("Plugin build metadata does not match release");
  }
  const archive = readFileSync(join(directory, fileName));
  const digest = hash(archive);
  if (metadata.sha256 !== digest) throw new Error("Plugin archive checksum does not match metadata");
  const checksum = Buffer.from(`${digest}  ${fileName}\n`);
  if (!readFileSync(join(directory, "SHA256SUMS")).equals(checksum)) throw new Error("Invalid SHA256SUMS");
  const assets = [{ name: fileName, bytes: archive, type: "application/zip" }, { name: "SHA256SUMS", bytes: checksum, type: "text/plain" }];
  const tag = releaseTag(target, version);
  const changelog = readFileSync(join(root, releaseTarget(target).path, "CHANGELOG.md"), "utf8");
  const heading = new RegExp(`^## \\[?${version.replaceAll(".", "\\.")}\\]?(?:\\s|$)`, "m");
  const match = heading.exec(changelog);
  if (!match) throw new Error(`Missing changelog entry for ${tag}`);
  const remainder = changelog.slice(match.index + match[0].length);
  const next = remainder.search(/^## /m);
  const notes = changelog.slice(match.index, next < 0 ? undefined : match.index + match[0].length + next).trim();
  const tagRef = await absentOn404(() => github.rest.git.getRef({ owner, repo, ref: `tags/${tag}` }));
  const existingTag = tagRef ? await github.rest.repos.getCommit({ owner, repo, ref: tag }) : null;
  if (existingTag && existingTag.data.sha !== sha) throw new Error(`${tag} points to a different source commit`);
  let release = await absentOn404(() => github.rest.repos.getReleaseByTag({ owner, repo, tag }));
  if (release && !existingTag) throw new Error("Existing release has no matching tag");
  if (!existingTag) await github.rest.git.createRef({ owner, repo, ref: `refs/tags/${tag}`, sha });
  if (!release) {
    release = await github.rest.repos.createRelease({ owner, repo, tag_name: tag, target_commitish: sha,
      name: `${target} v${version}`, body: `${notes}\n\nPlugin API: v${metadata.apiMajor} (${metadata.apiIdentity}).\nSource: ${sha}\n\nDownload \`${fileName}\` and verify it with \`SHA256SUMS\`.`,
      draft: true, make_latest: "false" });
  }
  const listed = await github.paginate(github.rest.repos.listReleaseAssets, { owner, repo, release_id: release.data.id, per_page: 100 });
  if (listed.some(asset => !assets.some(expected => expected.name === asset.name))) throw new Error("Release contains unexpected assets");
  for (const asset of assets) {
    const existing = listed.find(value => value.name === asset.name);
    if (existing) {
      const downloaded = await github.rest.repos.getReleaseAsset({ owner, repo, asset_id: existing.id, headers: { accept: "application/octet-stream" } });
      if (hash(Buffer.from(downloaded.data)) !== hash(asset.bytes)) throw new Error(`Existing ${asset.name} has different contents; refusing to overwrite`);
    } else {
      if (!release.data.draft) throw new Error(`Published release is missing ${asset.name}; refusing to modify it`);
      await github.rest.repos.uploadReleaseAsset({ owner, repo, release_id: release.data.id, name: asset.name,
        data: asset.bytes, headers: { "content-type": asset.type, "content-length": asset.bytes.length } });
    }
  }
  // Verify both uploaded bytes, including a resumed draft, before publication.
  const completed = await github.paginate(github.rest.repos.listReleaseAssets, { owner, repo, release_id: release.data.id, per_page: 100 });
  if (completed.length !== assets.length || new Set(completed.map(asset => asset.name)).size !== assets.length || completed.some(asset => !assets.some(expected => expected.name === asset.name))) {
    throw new Error("Completed release contains unexpected or duplicate assets");
  }
  for (const asset of assets) {
    const found = completed.find(value => value.name === asset.name);
    if (!found) throw new Error(`Missing uploaded asset ${asset.name}`);
    const response = await github.rest.repos.getReleaseAsset({ owner, repo, asset_id: found.id, headers: { accept: "application/octet-stream" } });
    if (hash(Buffer.from(response.data)) !== hash(asset.bytes)) throw new Error(`Uploaded ${asset.name} failed checksum verification`);
  }
  if (release.data.draft) await github.rest.repos.updateRelease({ owner, repo, release_id: release.data.id, draft: false, make_latest: "false" });
  return tag;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const selected = selectPluginReleases(process.cwd(), { sha: process.env.RELEASE_SHA, before: process.env.BEFORE_SHA, target: process.env.RELEASE_TARGET || undefined });
  const output = `matrix=${JSON.stringify({ include: selected })}\nhas_release=${selected.length > 0}\n`;
  process.stdout.write(output);
  if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, output);
}
