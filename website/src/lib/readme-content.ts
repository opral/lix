import { parse } from "@opral/markdown-wc";
import { normalizeMarkdownHtml } from "./markdown-html";
import readmeMarkdown from "../../../README.md?raw";

export async function loadReadmeContent() {
  const parsed = await parse(readmeMarkdown);
  return {
    html: normalizeMarkdownHtml(parsed.html)
      .replaceAll('src="./assets/', 'src="/assets/')
      .replaceAll('src="./website/public/assets/', 'src="/assets/'),
  };
}
