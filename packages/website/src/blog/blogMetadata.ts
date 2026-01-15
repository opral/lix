import { getMarkdownDescription, getMarkdownTitle } from "../lib/seo";

type BlogMetadataInput = {
  rawMarkdown: string;
  frontmatter?: Record<string, unknown>;
};

export function getBlogTitle({ rawMarkdown, frontmatter }: BlogMetadataInput) {
  return getMarkdownTitle({ rawMarkdown, frontmatter });
}

export function getBlogDescription({
  rawMarkdown,
  frontmatter,
}: BlogMetadataInput) {
  return getMarkdownDescription({ rawMarkdown, frontmatter });
}
