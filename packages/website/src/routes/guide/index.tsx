import { createFileRoute, redirect } from "@tanstack/react-router";

export const Route = createFileRoute("/guide/")({
  loader: () => {
    throw redirect({
      to: "/docs/$slugId",
      params: { slugId: "what-is-lix" },
    });
  },
});
