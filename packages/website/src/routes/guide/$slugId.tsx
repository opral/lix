import { createFileRoute, redirect } from "@tanstack/react-router";

export const Route = createFileRoute("/guide/$slugId")({
  loader: ({ params }) => {
    throw redirect({
      to: "/docs/$slugId",
      params: { slugId: params.slugId },
    });
  },
});
