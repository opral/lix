import { useState } from "react";
import { useRouterState } from "@tanstack/react-router";
import { getGithubStars } from "../github-stars-cache";
import { Footer } from "./footer";
import type { SyntheticEvent } from "react";

/**
 * Hand-drawn style illustration representing chat-like edits.
 *
 * @example
 * <CursorEditingIllustration />
 */
const CursorEditingIllustration = () => (
  <div
    className="inline-flex w-full max-w-[240px] flex-col gap-2 rounded-2xl border border-gray-200 bg-gray-50 px-4 py-4"
    aria-hidden="true"
  >
    <div className="h-3 w-full rounded bg-gray-200" />
    <div className="h-3 w-3/4 rounded bg-rose-200" />
    <div className="h-3 w-4/5 rounded bg-emerald-200" />
    <div className="h-3 w-1/2 rounded bg-gray-200" />
  </div>
);

/**
 * Illustration highlighting async branching workflows.
 *
 * @example
 * <AsyncWorkflowIllustration />
 */
const AsyncWorkflowIllustration = () => (
  <div
    className="inline-flex w-full max-w-[240px] flex-col gap-4 rounded-2xl border border-gray-200 bg-white px-4 py-4"
    aria-hidden="true"
  >
    <svg
      viewBox="0 0 187.75157590091476 120.55659158611115"
      className="h-32 w-full"
      xmlns="http://www.w3.org/2000/svg"
    >
      <defs>
        <symbol id="async-agent-avatar">
          <image
            href="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIyNCIgaGVpZ2h0PSIyNCIgdmlld0JveD0iMCAwIDI0IDI0IiBmaWxsPSJub25lIiBzdHJva2U9ImN1cnJlbnRDb2xvciIgc3Ryb2tlLXdpZHRoPSIyIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIGNsYXNzPSJsdWNpZGUgbHVjaWRlLWdpdC1tZXJnZS1pY29uIGx1Y2lkZS1naXQtbWVyZ2UiPjxjaXJjbGUgY3g9IjE4IiBjeT0iMTgiIHI9IjMiLz48Y2lyY2xlIGN4PSI2IiBjeT0iNiIgcj0iMyIvPjxwYXRoIGQ9Ik02IDIxVjlhOSA5IDAgMCAwIDkgOSIvPjwvc3ZnPg=="
            preserveAspectRatio="none"
            width="100%"
            height="100%"
          />
        </symbol>
        <symbol id="async-agent-checkpoint">
          <image
            href="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIxMDBwdCIgaGVpZ2h0PSIxMDBwdCIgdmVyc2lvbj0iMS4xIiB2aWV3Qm94PSIwIDAgMTAwIDEwMCI+CiA8Zz4KICA8cGF0aCBkPSJtNjAuMTI1IDc4Ljg4N2g2LjQyNTh2Ni45MDYyaC02LjQyNTh6Ii8+CiAgPHBhdGggZD0ibTM1LjU2MiA1NS43NWMzLjkxMDIgMCA3LjQzMzYtMi4zNTU1IDguOTI5Ny01Ljk2ODggMS40OTYxLTMuNjA5NCAwLjY3MTg3LTcuNzY5NS0yLjA5MzgtMTAuNTM1LTIuNzY1Ni0yLjc2MTctNi45MjE5LTMuNTg5OC0xMC41MzUtMi4wOTM4cy01Ljk2ODggNS4wMTk1LTUuOTY4OCA4LjkyOTdjMC4wMTE3MTkgNS4zMzU5IDQuMzMyIDkuNjU2MiA5LjY2OCA5LjY2OHoiLz4KICA8cGF0aCBkPSJtNTEuNjEzIDc4Ljg4N2g2LjQyNTh2Ni45MDYyaC02LjQyNTh6Ii8+CiAgPHBhdGggZD0ibTM0LjYwNSA3OC44ODdoNi40MjU4djYuOTA2MmgtNi40MjU4eiIvPgogIDxwYXRoIGQ9Im00My4xMTMgNzguODg3aDYuMzI0MnY2LjkwNjJoLTYuMzI0MnoiLz4KICA8cGF0aCBkPSJtMjUuODQ0IDc4Ljg4N2g2LjY4NzV2Ni45MDYyaC02LjY4NzV6Ii8+CiAgPHBhdGggZD0ibTY1LjQzOCAzNi40NTdjLTMuOTEwMiAwLjAxOTUzMS03LjQyNTggMi4zODY3LTguOTA2MiA2LjAwNzgtMS40ODA1IDMuNjE3Mi0wLjYzMjgxIDcuNzczNCAyLjE0MDYgMTAuNTIzIDIuNzc3MyAyLjc1MzkgNi45Mzc1IDMuNTYyNSAxMC41NDcgMi4wNTA4IDMuNjA1NS0xLjUxMTcgNS45NDUzLTUuMDQ2OSA1LjkyNTgtOC45NTctMC4wMjczNDMtNS4zMzU5LTQuMzcxMS05LjY0MDYtOS43MDctOS42MjV6Ii8+CiAgPHBhdGggZD0ibTczLjM1NSAyMS4wMmgtNDYuNzExYy02LjUwMzkgMC4wMDc4MTMtMTEuNzczIDUuMjc3My0xMS43ODEgMTEuNzgxdjU1LjQxOGMwLjAwNzgxMyA2LjUwMzkgNS4yNzczIDExLjc3NyAxMS43ODEgMTEuNzgxaDQ2LjcxMWM2LjUwMzktMC4wMDM5MDYgMTEuNzc3LTUuMjczNCAxMS43ODktMTEuNzgxdi01NS40MThjLTAuMDExNzE5LTYuNTAzOS01LjI4NTItMTEuNzczLTExLjc4OS0xMS43ODF6bS0yMi45MTggMzguNDU3djAuMDAzOTA3YzAuMzI4MTIgMCAwLjY0MDYyIDAuMTYwMTYgMC44MzIwMyAwLjQyNTc4bDYuODg2NyA5LjM3NWMwLjIxMDk0IDAuMzA4NTkgMC4yNDYwOSAwLjcwMzEyIDAuMDgyMDMxIDEuMDQzLTAuMTc5NjkgMC4zNDM3NS0wLjUzNTE2IDAuNTYyNS0wLjkyNTc4IDAuNTYyNWgtMTMuNzE5Yy0wLjM5MDYyIDAtMC43NDYwOS0wLjIxODc1LTAuOTI1NzgtMC41NjI1LTAuMTYwMTYtMC4zMzk4NC0wLjEyNS0wLjczODI4IDAuMDkzNzUtMS4wNDNsNi44NzUtOS4zNzVjMC4xOTUzMS0wLjI2OTUzIDAuNTA3ODEtMC40Mjk2OSAwLjg0Mzc1LTAuNDI1Nzh6bS0xNC44NzUtMjUuMTAyYzQuNzUgMC4wMTk1MzEgOS4wMjM0IDIuOTAyMyAxMC44MjQgNy4zMDA4IDEuNzk2OSA0LjM5NDUgMC43NzM0NCA5LjQ0NTMtMi42MDE2IDEyLjc4OS0zLjM3MTEgMy4zNDc3LTguNDI5NyA0LjMzMi0xMi44MTIgMi41LTQuMzgyOC0xLjgzNTktNy4yMzA1LTYuMTI4OS03LjIxMDktMTAuODgzIDAuMDMxMjUtNi40ODgzIDUuMzEyNS0xMS43MjMgMTEuODAxLTExLjcwN3ptNDEuNjY4IDUyLjQ0OWMwIDAuMjczNDQtMC4xMDkzOCAwLjUzOTA2LTAuMzA0NjkgMC43MzQzOC0wLjE5NTMxIDAuMTk1MzEtMC40NjA5NCAwLjMwNDY5LTAuNzM4MjggMC4zMDQ2OWgtNTEuMzg3Yy0wLjU3NDIyIDAtMS4wMzkxLTAuNDY0ODQtMS4wMzkxLTEuMDM5MXYtOC45ODA1YzAtMC41NzQyMiAwLjQ2NDg0LTEuMDQzIDEuMDM5MS0xLjA0M2g1MS4zODdjMC4yNzczNCAwIDAuNTQyOTcgMC4xMDkzOCAwLjczODI4IDAuMzA0NjkgMC4xOTUzMSAwLjE5NTMxIDAuMzA0NjkgMC40NjA5NCAwLjMwNDY5IDAuNzM4Mjh6bS0xMS43OTMtMjguOTkyYy00Ljc1LTAuMDE1NjI1LTkuMDE5NS0yLjg5MDYtMTAuODI0LTcuMjgxMi0xLjgwODYtNC4zOTA2LTAuNzkyOTctOS40Mzc1IDIuNTcwMy0xMi43ODkgMy4zNjcyLTMuMzUxNiA4LjQxOC00LjM1MTYgMTIuODAxLTIuNTMxMiA0LjM4NjcgMS44MjQyIDcuMjQ2MSA2LjEwNTUgNy4yNDYxIDEwLjg1Mi0wLjAwMzkwNyAzLjEyNS0xLjI1IDYuMTE3Mi0zLjQ2MDkgOC4zMjAzLTIuMjEwOSAyLjIwNy01LjIxMDkgMy40Mzc1LTguMzMyIDMuNDI5N3oiLz4KICA8cGF0aCBkPSJtNTAuNDggNjIuMjgxLTQuODM1OSA2LjYwNTVoOS42NTYyeiIvPgogIDxwYXRoIGQ9Im02OC42MjUgNzguODg3aDYuNDgwNXY2LjkwNjJoLTYuNDgwNXoiLz4KICA8cGF0aCBkPSJtOTEuOTQ5IDQ1LjIzOGMtMC41NzgxMiAwLTEuMDQzLTAuNDY0ODQtMS4wNDMtMS4wMzkxdi0zMy43ODFjMC0wLjU3ODEyIDAuNDY0ODQtMS4wNDMgMS4wNDMtMS4wNDMgMC41NzQyMiAwIDEuMDM5MSAwLjQ2NDg0IDEuMDM5MSAxLjA0M3YzMy43ODFjMCAwLjI3MzQ0LTAuMTA5MzggMC41MzkwNi0wLjMwNDY5IDAuNzM0MzgtMC4xOTUzMSAwLjE5NTMxLTAuNDYwOTQgMC4zMDQ2OS0wLjczNDM4IDAuMzA0Njl6Ii8+CiAgPHBhdGggZD0ibTk4LjgxMiA0OC44NDR2MTUuOTA2Yy0wLjAwMzkwNiAzLjE0MDYtMi41NDY5IDUuNjgzNi01LjY4NzUgNS42ODc1aC01Ljg5NDV2LTI3LjI4MWg1Ljg5NDVjMy4xNDA2IDAuMDAzOTA2IDUuNjgzNiAyLjU0NjkgNS42ODc1IDUuNjg3NXoiLz4KICA8cGF0aCBkPSJtMTIuNzgxIDQzLjE1NnYyNy4yODFoLTUuOTA2MmMtMy4xNDA2LTAuMDAzOTA2LTUuNjgzNi0yLjU0NjktNS42ODc1LTUuNjg3NXYtMTUuOTA2YzAuMDAzOTA2LTMuMTQwNiAyLjU0NjktNS42ODM2IDUuNjg3NS01LjY4NzV6Ii8+CiAgPHBhdGggZD0ibTguMDUwOCA0NS4yMzhjLTAuNTc0MjIgMC0xLjAzOTEtMC40NjQ4NC0xLjAzOTEtMS4wMzkxdi0zMy43ODFjMC0wLjU3ODEyIDAuNDY0ODQtMS4wNDMgMS4wMzkxLTEuMDQzIDAuNTc4MTIgMCAxLjA0MyAwLjQ2NDg0IDEuMDQzIDEuMDQzdjMzLjc4MWMwIDAuMjczNDQtMC4xMDkzOCAwLjUzOTA2LTAuMzA0NjkgMC43MzQzOC0wLjE5NTMxIDAuMTk1MzEtMC40NjA5NCAwLjMwNDY5LTAuNzM4MjggMC4zMDQ2OXoiLz4KICA8cGF0aCBkPSJtOTcuNjU2IDUuNzA3YzAgMi4zMDg2LTEuMzkwNiA0LjM5MDYtMy41MjM0IDUuMjczNC0yLjEzMjggMC44ODY3Mi00LjU4OTggMC4zOTg0NC02LjIyMjctMS4yMzQ0LTEuNjMyOC0xLjYzMjgtMi4xMjExLTQuMDg5OC0xLjIzNDQtNi4yMjI3IDAuODgyODEtMi4xMzI4IDIuOTY0OC0zLjUyMzQgNS4yNzM0LTMuNTIzNCAxLjUxMTcgMCAyLjk2NDggMC42MDE1NiA0LjAzNTIgMS42NzE5czEuNjcxOSAyLjUyMzQgMS42NzE5IDQuMDM1MnoiLz4KICA8cGF0aCBkPSJtMTMuNzYyIDUuNzA3YzAgMy4xNTIzLTIuNTU4NiA1LjcxMDktNS43MTA5IDUuNzEwOS0zLjE1MjMgMC01LjcwNy0yLjU1ODYtNS43MDctNS43MTA5IDAtMy4xNTIzIDIuNTU0Ny01LjcwNyA1LjcwNy01LjcwNyAzLjE1MjMgMCA1LjcxMDkgMi41NTQ3IDUuNzEwOSA1LjcwNyIvPgogPC9nPgo8L3N2Zz4="
            preserveAspectRatio="none"
            width="100%"
            height="100%"
          />
        </symbol>
        <style>{`@font-face { font-family: "Comic Shanns"; src: url(data:font/woff2;base64,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); }`}</style>
      </defs>
      <rect
        x="0"
        y="0"
        width="187.75157590091476"
        height="120.55659158611115"
        fill="#ffffff"
      />
      <g strokeLinecap="round">
        <g transform="translate(18.34830013564715 14.545933930658293) rotate(0 0.1316811308506658 48.00532882772643)">
          <path
            d="M0 0 C-0.83 28.87, -2.39 55.66, 0.26 96.01 M0 0 C0.15 31.79, -0.12 65.62, 0.26 96.01"
            stroke="#000000"
            strokeWidth="2"
            fill="none"
          />
        </g>
      </g>
      <g transform="translate(43.890187172653896 10.597529379081152) rotate(0 23.693984985351562 8.97739723540667)">
        <text
          x="0"
          y="12.56835612956935"
          fontFamily='"Comic Shanns", sans-serif'
          fontSize="14.363835576650688"
          fill="#1e1e1e"
        >
          merged
        </text>
      </g>
      <g transform="translate(56.89972778644915 46.25743279979929) rotate(0 10.294483472688626 10.29448347268817)">
        <use
          href="#async-agent-checkpoint"
          width="21"
          height="21"
          opacity="1"
        />
      </g>
      <g transform="translate(82.97563595950851 48.86803881109404) rotate(0 47.387969970703125 8.97739723540667)">
        <text
          x="0"
          y="12.56835612956935"
          fontFamily='"Comic Shanns", sans-serif'
          fontSize="14.363835576650688"
          fill="#1e1e1e"
        >
          fix spelling
        </text>
      </g>
      <g transform="translate(41.825091552154845 85.40411087068264) rotate(0 47.387969970703125 8.97739723540667)">
        <text
          x="0"
          y="12.56835612956935"
          fontFamily='"Comic Shanns", sans-serif'
          fontSize="14.363835576650688"
          fill="#1e1e1e"
        >
          add document
        </text>
      </g>
      <g strokeLinecap="round">
        <g transform="translate(20.029223717050627 91.05546746840923) rotate(0 11.894338706990311 -23.783246077665353)">
          <path
            d="M0 0 C3.87 -2.91, 19.27 -9.55, 23.23 -17.48 C27.2 -25.4, 23.7 -42.55, 23.79 -47.57 M0 0 C3.87 -2.91, 19.27 -9.55, 23.23 -17.48 C27.2 -25.4, 23.7 -42.55, 23.79 -47.57"
            stroke="#1e1e1e"
            strokeWidth="2"
            fill="none"
          />
        </g>
        <g transform="translate(42.75487941351457 42.50495784611758) rotate(0 -11.121422410754349 -9.447668749566219)">
          <path
            d="M0 0 C-6.53 -5.55, -13.06 -11.09, -22.24 -18.9 M0 0 C-6.44 -5.47, -12.89 -10.95, -22.24 -18.9"
            stroke="#1e1e1e"
            strokeWidth="2"
            fill="none"
          />
        </g>
      </g>
      <g transform="translate(10 10) rotate(0 10.54745795938743 10.54745795938743)">
        <path
          d="M21.09 10.55 C21.09 11.16, 21.04 11.78, 20.93 12.38 C20.83 12.98, 20.67 13.58, 20.46 14.15 C20.25 14.73, 19.99 15.29, 19.68 15.82 C19.38 16.35, 19.02 16.86, 18.63 17.33 C18.23 17.79, 17.79 18.23, 17.33 18.63 C16.86 19.02, 16.35 19.38, 15.82 19.68 C15.29 19.99, 14.73 20.25, 14.15 20.46 C13.58 20.67, 12.98 20.83, 12.38 20.93 C11.78 21.04, 11.16 21.09, 10.55 21.09 C9.94 21.09, 9.32 21.04, 8.72 20.93 C8.11 20.83, 7.51 20.67, 6.94 20.46 C6.37 20.25, 5.8 19.99, 5.27 19.68 C4.75 19.38, 4.24 19.02, 3.77 18.63 C3.3 18.23, 2.86 17.79, 2.47 17.33 C2.08 16.86, 1.72 16.35, 1.41 15.82 C1.11 15.29, 0.84 14.73, 0.64 14.15 C0.43 13.58, 0.27 12.98, 0.16 12.38 C0.05 11.78, 0 11.16, 0 10.55 C0 9.94, 0.05 9.32, 0.16 8.72 C0.27 8.11, 0.43 7.51, 0.64 6.94 C0.84 6.37, 1.11 5.8, 1.41 5.27 C1.72 4.75, 2.08 4.24, 2.47 3.77 C2.86 3.3, 3.3 2.86, 3.77 2.47 C4.24 2.08, 4.75 1.72, 5.27 1.41 C5.8 1.11, 6.37 0.84, 6.94 0.64 C7.51 0.43, 8.11 0.27, 8.72 0.16 C9.32 0.05, 9.94 0, 10.55 0 C11.16 0, 11.78 0.05, 12.38 0.16 C12.98 0.27, 13.58 0.43, 14.15 0.64 C14.73 0.84, 15.29 1.11, 15.82 1.41 C16.35 1.72, 16.86 2.08, 17.33 2.47 C17.79 2.86, 18.23 3.3, 18.63 3.77 C19.02 4.24, 19.38 4.75, 19.68 5.27 C19.99 5.8, 20.25 6.37, 20.46 6.94 C20.67 7.51, 20.83 8.11, 20.93 8.72 C21.04 9.32, 21.07 10.24, 21.09 10.55 C21.12 10.85, 21.12 10.24, 21.09 10.55"
          fill="#d0bfff"
        />
        <g transform="translate(3.785536291607059 2.192950567596654)">
          <use href="#async-agent-avatar" width="15" height="15" opacity="1" />
        </g>
      </g>
    </svg>
  </div>
);

/**
 * Illustration showing audit and approval checklists.
 *
 * @example
 * <AuditWorkflowIllustration />
 */
const AuditWorkflowIllustration = () => (
  <div
    className="inline-flex w-full max-w-[240px] flex-col gap-3 rounded-2xl border border-gray-200 bg-white px-4 py-4"
    aria-hidden="true"
  >
    <button className="relative flex items-center justify-center rounded border border-green-200 bg-green-50 px-4 py-2 text-sm font-medium text-green-700">
      <span className="absolute left-4 flex h-4 w-4 items-center justify-center">
        <svg
          xmlns="http://www.w3.org/2000/svg"
          className="h-4 w-4"
          viewBox="0 0 20 20"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.8"
          strokeLinecap="round"
          strokeLinejoin="round"
          aria-hidden="true"
        >
          <path d="M5 10l3 3 7-7" />
        </svg>
      </span>
      Approve
    </button>
    <button className="relative flex items-center justify-center rounded border border-yellow-200 bg-yellow-50 px-4 py-2 text-sm font-medium text-yellow-700">
      <span className="absolute left-4 flex h-4 w-4 items-center justify-center">
        <svg
          xmlns="http://www.w3.org/2000/svg"
          className="h-4 w-4"
          viewBox="0 0 20 20"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.8"
          strokeLinecap="round"
          strokeLinejoin="round"
          aria-hidden="true"
        >
          <path d="M6 6l8 8M14 6l-8 8" />
        </svg>
      </span>
      Request changes
    </button>
  </div>
);

/**
 * Lix logo used across the landing page.
 *
 * @example
 * <LixLogo className="h-6 w-6" />
 */
const LixLogo = ({ className = "" }) => (
  <svg
    width="30"
    height="22"
    viewBox="0 0 26 18"
    fill="currentColor"
    xmlns="http://www.w3.org/2000/svg"
    className={className}
  >
    <g id="Group 162">
      <path
        id="Vector"
        d="M14.7618 5.74842L16.9208 9.85984L22.3675 0.358398H25.7133L19.0723 11.6284L22.5712 17.5085H19.2407L16.9208 13.443L14.6393 17.5085H11.2705L14.7618 11.6284L11.393 5.74842H14.7618Z"
        fill="currentColor"
      />
      <path
        id="Vector_2"
        d="M6.16211 17.5081V5.74805H9.42368V17.5081H6.16211Z"
        fill="currentColor"
      />
      <path
        id="Vector_3"
        d="M3.52112 0.393555V17.6416H0.287109V0.393555H3.52112Z"
        fill="currentColor"
      />
      <path
        id="Rectangle 391"
        d="M6.21582 0.393555H14.8399V3.08856H6.21582V0.393555Z"
        fill="currentColor"
      />
    </g>
  </svg>
);

/**
 * GitHub mark icon used in the site header.
 *
 * @example
 * <GitHubIcon className="h-5 w-5" />
 */
const GitHubIcon = ({ className = "" }) => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    viewBox="0 0 24 24"
    fill="currentColor"
    className={className}
    aria-hidden="true"
  >
    <path d="M12 2a10 10 0 00-3.16 19.49c.5.09.68-.21.68-.47v-1.69c-2.78.6-3.37-1.34-3.37-1.34a2.64 2.64 0 00-1.1-1.46c-.9-.62.07-.6.07-.6a2.08 2.08 0 011.52 1 2.1 2.1 0 002.87.82 2.11 2.11 0 01.63-1.32c-2.22-.25-4.56-1.11-4.56-4.95a3.88 3.88 0 011-2.7 3.6 3.6 0 01.1-2.67s.84-.27 2.75 1a9.5 9.5 0 015 0c1.91-1.29 2.75-1 2.75-1a3.6 3.6 0 01.1 2.67 3.87 3.87 0 011 2.7c0 3.85-2.34 4.7-4.57 4.95a2.37 2.37 0 01.68 1.84v2.72c0 .27.18.57.69.47A10 10 0 0012 2z" />
  </svg>
);

/**
 * Discord icon used in the site header.
 *
 * @example
 * <DiscordIcon className="h-5 w-5" />
 */
const DiscordIcon = ({ className = "" }) => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    viewBox="0 0 71 55"
    fill="currentColor"
    className={className}
    aria-hidden="true"
  >
    <path d="M60.1045 4.8978C55.5792 2.8214 50.7265 1.2916 45.6527 0.41542C45.5603 0.39851 45.468 0.440769 45.4204 0.525289C44.7963 1.6353 44.105 3.0834 43.6209 4.2216C38.1637 3.4046 32.7345 3.4046 27.3892 4.2216C26.905 3.0581 26.1886 1.6353 25.5617 0.525289C25.5141 0.443589 25.4218 0.40133 25.3294 0.41542C20.2584 1.2888 15.4057 2.8186 10.8776 4.8978C10.8384 4.9147 10.8048 4.9429 10.7825 4.9793C1.57795 18.7309 -0.943561 32.1443 0.293408 45.3914C0.299005 45.4562 0.335386 45.5182 0.385761 45.5574C6.45866 50.0174 12.3413 52.7249 18.1147 54.5195C18.2071 54.5477 18.3052 54.5131 18.363 54.4376C19.7295 52.5728 20.9469 50.6063 21.9907 48.5383C22.0527 48.4172 21.9931 48.2735 21.8674 48.2259C19.9366 47.4931 18.0979 46.6 16.3292 45.5858C16.1893 45.5033 16.1789 45.3039 16.3116 45.2082C16.679 44.9293 17.0464 44.6391 17.4034 44.346C17.4654 44.2947 17.5534 44.2843 17.6228 44.3189C29.2558 49.8743 41.8354 49.8743 53.3179 44.3189C53.3873 44.2817 53.4753 44.292 53.5401 44.3433C53.8971 44.6364 54.2645 44.9293 54.6346 45.2082C54.7673 45.3039 54.7594 45.5033 54.6195 45.5858C52.8508 46.6197 51.0121 47.4931 49.0775 48.223C48.9518 48.2706 48.894 48.4172 48.956 48.5383C50.0198 50.6034 51.2372 52.5699 52.5872 54.4347C52.6414 54.5131 52.7423 54.5477 52.8347 54.5195C58.6464 52.7249 64.529 50.0174 70.6019 45.5574C70.6559 45.5182 70.6894 45.459 70.695 45.3942C72.1747 30.0791 68.2147 16.7757 60.1968 4.9821C60.1772 4.9429 60.1436 4.9147 60.1045 4.8978ZM23.7259 37.3253C20.2276 37.3253 17.3451 34.1136 17.3451 30.1693C17.3451 26.225 20.1717 23.0133 23.7259 23.0133C27.308 23.0133 30.1626 26.2532 30.1066 30.1693C30.1066 34.1136 27.28 37.3253 23.7259 37.3253ZM47.2012 37.3253C43.7029 37.3253 40.8203 34.1136 40.8203 30.1693C40.8203 26.225 43.6469 23.0133 47.2012 23.0133C50.7833 23.0133 53.6379 26.2532 53.5819 30.1693C53.5819 34.1136 50.7833 37.3253 47.2012 37.3253Z" />
  </svg>
);

/**
 * X (formerly Twitter) icon used in the site header.
 *
 * @example
 * <XIcon className="h-5 w-5" />
 */
const XIcon = ({ className = "" }) => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    viewBox="0 0 1200 1227"
    fill="currentColor"
    className={className}
    aria-hidden="true"
  >
    <path d="M714.163 519.284 1160.89 0h-105.86L667.137 450.887 357.328 0H0l468.492 681.821L0 1226.37h105.866l409.625-476.152 327.181 476.152H1200L714.137 519.284h.026ZM569.165 687.828l-47.468-67.894-377.686-540.24h162.604l304.797 435.991 47.468 67.894 396.2 566.721H892.476L569.165 687.854v-.026Z" />
  </svg>
);

/**
 * JavaScript icon for code tabs.
 */
const JsIcon = ({ className = "" }) => (
  <svg
    viewBox="0 0 24 24"
    className={className}
    fill="none"
    xmlns="http://www.w3.org/2000/svg"
  >
    <rect width="24" height="24" fill="#F7DF1E" rx="2" />
    <path
      d="M6 18l2-12h4l2 12h-2l-1-4h-4l-1 4H6z" // Placeholder path if text doesn't work well, but let's try text
      fill="none"
    />
    <text
      x="12"
      y="17"
      textAnchor="middle"
      fontSize="11"
      fontWeight="bold"
      fill="black"
      fontFamily="sans-serif"
    >
      JS
    </text>
  </svg>
);

/**
 * Python icon for code tabs.
 */
const PythonIcon = ({ className = "" }) => (
  <svg
    viewBox="16 16 32 32"
    className={className}
    fill="none"
    xmlns="http://www.w3.org/2000/svg"
  >
    <path
      fill="url(#python__a)"
      d="M31.885 16c-8.124 0-7.617 3.523-7.617 3.523l.01 3.65h7.752v1.095H21.197S16 23.678 16 31.876c0 8.196 4.537 7.906 4.537 7.906h2.708v-3.804s-.146-4.537 4.465-4.537h7.688s4.32.07 4.32-4.175v-7.019S40.374 16 31.885 16zm-4.275 2.454a1.394 1.394 0 1 1 0 2.79 1.393 1.393 0 0 1-1.395-1.395c0-.771.624-1.395 1.395-1.395z"
    />
    <path
      fill="url(#python__b)"
      d="M32.115 47.833c8.124 0 7.617-3.523 7.617-3.523l-.01-3.65H31.97v-1.095h10.832S48 40.155 48 31.958c0-8.197-4.537-7.906-4.537-7.906h-2.708v3.803s.146 4.537-4.465 4.537h-7.688s-4.32-.07-4.32 4.175v7.019s-.656 4.247 7.833 4.247zm4.275-2.454a1.393 1.393 0 0 1-1.395-1.395 1.394 1.394 0 1 1 1.395 1.395z"
    />
    <defs>
      <linearGradient
        id="python__a"
        x1="19.075"
        x2="34.898"
        y1="18.782"
        y2="34.658"
        gradientUnits="userSpaceOnUse"
      >
        <stop stopColor="#387EB8" />
        <stop offset="1" stopColor="#366994" />
      </linearGradient>
      <linearGradient
        id="python__b"
        x1="28.809"
        x2="45.803"
        y1="28.882"
        y2="45.163"
        gradientUnits="userSpaceOnUse"
      >
        <stop stopColor="#FFE052" />
        <stop offset="1" stopColor="#FFC331" />
      </linearGradient>
    </defs>
  </svg>
);

/**
 * Rust icon for code tabs.
 */
const RustIcon = ({ className = "" }) => (
  <svg
    viewBox="0 0 224 224"
    className={className}
    fill="currentColor"
    xmlns="http://www.w3.org/2000/svg"
  >
    <path
      fill="#000"
      d="M218.46 109.358l-9.062-5.614c-.076-.882-.162-1.762-.258-2.642l7.803-7.265a3.107 3.107 0 00.933-2.89 3.093 3.093 0 00-1.967-2.312l-9.97-3.715c-.25-.863-.512-1.72-.781-2.58l6.214-8.628a3.114 3.114 0 00-.592-4.263 3.134 3.134 0 00-1.431-.637l-10.507-1.709a80.869 80.869 0 00-1.263-2.353l4.417-9.7a3.12 3.12 0 00-.243-3.035 3.106 3.106 0 00-2.705-1.385l-10.671.372a85.152 85.152 0 00-1.685-2.044l2.456-10.381a3.125 3.125 0 00-3.762-3.763l-10.384 2.456a88.996 88.996 0 00-2.047-1.684l.373-10.671a3.11 3.11 0 00-1.385-2.704 3.127 3.127 0 00-3.034-.246l-9.681 4.417c-.782-.429-1.567-.854-2.353-1.265l-1.713-10.506a3.098 3.098 0 00-1.887-2.373 3.108 3.108 0 00-3.014.35l-8.628 6.213c-.85-.27-1.703-.53-2.56-.778l-3.716-9.97a3.111 3.111 0 00-2.311-1.97 3.134 3.134 0 00-2.89.933l-7.266 7.802a93.746 93.746 0 00-2.643-.258l-5.614-9.082A3.125 3.125 0 00111.97 4c-1.09 0-2.085.56-2.642 1.478l-5.615 9.081a93.32 93.32 0 00-2.642.259l-7.266-7.802a3.13 3.13 0 00-2.89-.933 3.106 3.106 0 00-2.312 1.97l-3.715 9.97c-.857.247-1.71.506-2.56.778L73.7 12.588a3.101 3.101 0 00-3.014-.35A3.127 3.127 0 0068.8 14.61l-1.713 10.506c-.79.41-1.575.832-2.353 1.265l-9.681-4.417a3.125 3.125 0 00-4.42 2.95l.372 10.67c-.69.553-1.373 1.115-2.048 1.685l-10.383-2.456a3.143 3.143 0 00-2.93.832 3.124 3.124 0 00-.833 2.93l2.436 10.383a93.897 93.897 0 00-1.68 2.043l-10.672-.372a3.138 3.138 0 00-2.704 1.385 3.126 3.126 0 00-.246 3.035l4.418 9.7c-.43.779-.855 1.563-1.266 2.353l-10.507 1.71a3.097 3.097 0 00-2.373 1.886 3.117 3.117 0 00.35 3.013l6.214 8.628a89.12 89.12 0 00-.78 2.58l-9.97 3.715a3.117 3.117 0 00-1.035 5.202l7.803 7.265c-.098.879-.184 1.76-.258 2.642l-9.062 5.614A3.122 3.122 0 004 112.021c0 1.092.56 2.084 1.478 2.642l9.062 5.614c.074.882.16 1.762.258 2.642l-7.803 7.265a3.117 3.117 0 001.034 5.201l9.97 3.716a110 110 0 00.78 2.58l-6.212 8.627a3.112 3.112 0 00.6 4.27c.419.33.916.547 1.443.63l10.507 1.709c.407.792.83 1.576 1.265 2.353l-4.417 9.68a3.126 3.126 0 002.95 4.42l10.65-.374c.553.69 1.115 1.372 1.685 2.047l-2.435 10.383a3.09 3.09 0 00.831 2.91 3.117 3.117 0 002.931.83l10.384-2.436a82.268 82.268 0 002.047 1.68l-.371 10.671a3.11 3.11 0 001.385 2.704 3.125 3.125 0 003.034.241l9.681-4.416c.779.432 1.563.854 2.353 1.265l1.713 10.505a3.147 3.147 0 001.887 2.395 3.111 3.111 0 003.014-.349l8.628-6.213c.853.271 1.71.535 2.58.783l3.716 9.969a3.112 3.112 0 002.312 1.967 3.112 3.112 0 002.89-.933l7.266-7.802c.877.101 1.761.186 2.642.264l5.615 9.061a3.12 3.12 0 002.642 1.478 3.165 3.165 0 002.663-1.478l5.614-9.061c.884-.078 1.765-.163 2.643-.264l7.265 7.802a3.106 3.106 0 002.89.933 3.105 3.105 0 002.312-1.967l3.716-9.969c.863-.248 1.719-.512 2.58-.783l8.629 6.213a3.12 3.12 0 004.9-2.045l1.713-10.506c.793-.411 1.577-.838 2.353-1.265l9.681 4.416a3.13 3.13 0 003.035-.241 3.126 3.126 0 001.385-2.704l-.372-10.671a81.794 81.794 0 002.046-1.68l10.383 2.436a3.123 3.123 0 003.763-3.74l-2.436-10.382a84.588 84.588 0 001.68-2.048l10.672.374a3.104 3.104 0 002.704-1.385 3.118 3.118 0 00.244-3.035l-4.417-9.68c.43-.779.852-1.563 1.263-2.353l10.507-1.709a3.08 3.08 0 002.373-1.886 3.11 3.11 0 00-.35-3.014l-6.214-8.627c.272-.857.532-1.717.781-2.58l9.97-3.716a3.109 3.109 0 001.967-2.311 3.107 3.107 0 00-.933-2.89l-7.803-7.265c.096-.88.182-1.761.258-2.642l9.062-5.614a3.11 3.11 0 001.478-2.642 3.157 3.157 0 00-1.476-2.663h-.064zm-60.687 75.337c-3.468-.747-5.656-4.169-4.913-7.637a6.412 6.412 0 017.617-4.933c3.468.741 5.676 4.169 4.933 7.637a6.414 6.414 0 01-7.617 4.933h-.02zm-3.076-20.847c-3.158-.677-6.275 1.334-6.936 4.5l-3.22 15.026c-9.929 4.5-21.055 7.018-32.614 7.018-11.89 0-23.12-2.622-33.234-7.328l-3.22-15.026c-.677-3.158-3.778-5.18-6.936-4.499l-13.273 2.848a80.222 80.222 0 01-6.853-8.091h64.61c.731 0 1.218-.132 1.218-.797v-22.91c0-.665-.487-.797-1.218-.797H94.133v-14.469h20.415c1.864 0 9.97.533 12.551 10.898.811 3.179 2.601 13.54 3.818 16.863 1.214 3.715 6.152 11.146 11.415 11.146h32.202c.365 0 .755-.041 1.166-.116a80.56 80.56 0 01-7.307 8.587l-13.583-2.911-.113.058zm-89.38 20.537a6.407 6.407 0 01-7.617-4.933c-.74-3.467 1.462-6.894 4.934-7.637a6.417 6.417 0 017.617 4.933c.74 3.468-1.464 6.894-4.934 7.637zm-24.564-99.28a6.438 6.438 0 01-3.261 8.484c-3.241 1.438-7.019-.025-8.464-3.261-1.445-3.237.025-7.039 3.262-8.483a6.416 6.416 0 018.463 3.26zM33.22 102.94l13.83-6.15c2.952-1.311 4.294-4.769 2.972-7.72l-2.848-6.44H58.36v50.362h-22.5a79.158 79.158 0 01-3.014-21.672c0-2.869.155-5.697.452-8.483l-.08.103zm60.687-4.892v-14.86h26.629c1.376 0 9.722 1.59 9.722 7.822 0 5.18-6.399 7.038-11.663 7.038h-24.77.082zm96.811 13.375c0 1.973-.072 3.922-.216 5.862h-8.113c-.811 0-1.137.532-1.137 1.327v3.715c0 8.752-4.934 10.671-9.268 11.146-4.129.464-8.691-1.726-9.248-4.252-2.436-13.684-6.482-16.595-12.881-21.672 7.948-5.036 16.204-12.487 16.204-22.498 0-10.753-7.369-17.523-12.385-20.847-7.059-4.644-14.862-5.572-16.968-5.572H52.899c11.374-12.673 26.835-21.673 44.174-24.975l9.887 10.361a5.849 5.849 0 008.278.19l11.064-10.568c23.119 4.314 42.729 18.721 54.082 38.598l-7.576 17.09c-1.306 2.951.027 6.419 2.973 7.72l14.573 6.48c.255 2.607.383 5.224.384 7.843l-.021.052zM106.912 24.94a6.398 6.398 0 019.062.209 6.437 6.437 0 01-.213 9.082 6.396 6.396 0 01-9.062-.21 6.436 6.436 0 01.213-9.083v.002zm75.137 60.476a6.402 6.402 0 018.463-3.26 6.425 6.425 0 013.261 8.482 6.402 6.402 0 01-8.463 3.261 6.425 6.425 0 01-3.261-8.483z"
    />
  </svg>
);

/**
 * Go icon for code tabs.
 */
const GoIcon = ({ className = "" }) => (
  <svg
    viewBox="0 0 207 180"
    className={className}
    xmlns="http://www.w3.org/2000/svg"
  >
    <g fill="#00ADD8" fillRule="evenodd" transform="translate(0, 51)">
      <path d="m16.2 24.1c-.4 0-.5-.2-.3-.5l2.1-2.7c.2-.3.7-.5 1.1-.5h35.7c.4 0 .5.3.3.6l-1.7 2.6c-.2.3-.7.6-1 .6z" />
      <path d="m1.1 33.3c-.4 0-.5-.2-.3-.5l2.1-2.7c.2-.3.7-.5 1.1-.5h45.6c.4 0 .6.3.5.6l-.8 2.4c-.1.4-.5.6-.9.6z" />
      <path d="m25.3 42.5c-.4 0-.5-.3-.3-.6l1.4-2.5c.2-.3.6-.6 1-.6h20c.4 0 .6.3.6.7l-.2 2.4c0 .4-.4.7-.7.7z" />
      <g transform="translate(55)">
        <path d="m74.1 22.3c-6.3 1.6-10.6 2.8-16.8 4.4-1.5.4-1.6.5-2.9-1-1.5-1.7-2.6-2.8-4.7-3.8-6.3-3.1-12.4-2.2-18.1 1.5-6.8 4.4-10.3 10.9-10.2 19 .1 8 5.6 14.6 13.5 15.7 6.8.9 12.5-1.5 17-6.6.9-1.1 1.7-2.3 2.7-3.7-3.6 0-8.1 0-19.3 0-2.1 0-2.6-1.3-1.9-3 1.3-3.1 3.7-8.3 5.1-10.9.3-.6 1-1.6 2.5-1.6h36.4c-.2 2.7-.2 5.4-.6 8.1-1.1 7.2-3.8 13.8-8.2 19.6-7.2 9.5-16.6 15.4-28.5 17-9.8 1.3-18.9-.6-26.9-6.6-7.4-5.6-11.6-13-12.7-22.2-1.3-10.9 1.9-20.7 8.5-29.3 7.1-9.3 16.5-15.2 28-17.3 9.4-1.7 18.4-.6 26.5 4.9 5.3 3.5 9.1 8.3 11.6 14.1.6.9.2 1.4-1 1.7z" />
        <path
          d="m107.2 77.6c-9.1-.2-17.4-2.8-24.4-8.8-5.9-5.1-9.6-11.6-10.8-19.3-1.8-11.3 1.3-21.3 8.1-30.2 7.3-9.6 16.1-14.6 28-16.7 10.2-1.8 19.8-.8 28.5 5.1 7.9 5.4 12.8 12.7 14.1 22.3 1.7 13.5-2.2 24.5-11.5 33.9-6.6 6.7-14.7 10.9-24 12.8-2.7.5-5.4.6-8 .9zm23.8-40.4c-.1-1.3-.1-2.3-.3-3.3-1.8-9.9-10.9-15.5-20.4-13.3-9.3 2.1-15.3 8-17.5 17.4-1.8 7.8 2 15.7 9.2 18.9 5.5 2.4 11 2.1 16.3-.6 7.9-4.1 12.2-10.5 12.7-19.1z"
          fillRule="nonzero"
        />
      </g>
    </g>
  </svg>
);

/**
 * Landing page for the Lix documentation site.
 *
 * @example
 * <LandingPage />
 */
function LandingPage() {
  const [activeFeature, setActiveFeature] = useState("filesystem");

  const getFeatureCode = (featureId: string) => {
    switch (featureId) {
      case "filesystem":
        return (
          <>
            <span className="text-gray-500">// 2) Write a file via SQL</span>
            <br />
            <span className="text-indigo-600">await</span>{" "}
            <span className="text-sky-700">lix</span>
            <span className="text-gray-900">.</span>
            <span className="text-sky-700">db</span>
            <br />
            {"  "}
            <span className="text-gray-900">.</span>
            <span className="text-sky-700">insertInto</span>
            <span className="text-gray-900">(</span>
            <span className="text-amber-600">"file"</span>
            <span className="text-gray-900">)</span>
            <br />
            {"  "}
            <span className="text-gray-900">.</span>
            <span className="text-sky-700">values</span>
            <span className="text-gray-900">({"{"}</span>
            <br />
            {"    "}
            <span className="text-sky-700">path</span>
            <span className="text-gray-900">:</span>{" "}
            <span className="text-amber-600">"/config.json"</span>
            <span className="text-gray-900">,</span>
            <br />
            {"    "}
            <span className="text-sky-700">data</span>
            <span className="text-gray-900">:</span>{" "}
            <span className="text-indigo-600">new</span>{" "}
            <span className="text-sky-700">TextEncoder</span>
            <span className="text-gray-900">().</span>
            <span className="text-sky-700">encode</span>
            <span className="text-gray-900">(</span>
            <span className="text-sky-700">JSON</span>
            <span className="text-gray-900">.</span>
            <span className="text-sky-700">stringify</span>
            <span className="text-gray-900">(</span>
            <span className="text-gray-900">{"{"}</span>{" "}
            <span className="text-sky-700">theme</span>
            <span className="text-gray-900">:</span>{" "}
            <span className="text-amber-600">"light"</span>{" "}
            <span className="text-gray-900">{"}"}</span>
            <span className="text-gray-900">)</span>
            <span className="text-gray-900">),</span>
            <br />
            <span className="text-gray-900">{"  })"}</span>
            <br />
            {"  "}
            <span className="text-gray-900">.</span>
            <span className="text-sky-700">execute</span>
            <span className="text-gray-900">();</span>
            <br />
            <br />
            <span className="text-gray-500">// 3) Update a file</span>
            <br />
            <span className="text-indigo-600">await</span>{" "}
            <span className="text-sky-700">lix</span>
            <span className="text-gray-900">.</span>
            <span className="text-sky-700">db</span>
            <br />
            {"  "}
            <span className="text-gray-900">.</span>
            <span className="text-sky-700">updateTable</span>
            <span className="text-gray-900">(</span>
            <span className="text-amber-600">"file"</span>
            <span className="text-gray-900">)</span>
            <br />
            {"  "}
            <span className="text-gray-900">.</span>
            <span className="text-sky-700">set</span>
            <span className="text-gray-900">({"{"}</span>{" "}
            <span className="text-sky-700">data</span>
            <span className="text-gray-900">:</span>{" "}
            <span className="text-amber-600">"..."</span>{" "}
            <span className="text-gray-900">{"}"})</span>
            <br />
            {"  "}
            <span className="text-gray-900">.</span>
            <span className="text-sky-700">where</span>
            <span className="text-gray-900">(</span>
            <span className="text-amber-600">"path"</span>
            <span className="text-gray-900">,</span>{" "}
            <span className="text-amber-600">"="</span>
            <span className="text-gray-900">,</span>{" "}
            <span className="text-amber-600">"/config.json"</span>
            <span className="text-gray-900">)</span>
            <br />
            {"  "}
            <span className="text-gray-900">.</span>
            <span className="text-sky-700">execute</span>
            <span className="text-gray-900">();</span>
          </>
        );
      case "history":
        return (
          <>
            <span className="text-gray-500">
              // Query file history at a specific checkpoint
            </span>
            <br />
            <span className="text-indigo-600">const</span>{" "}
            <span className="text-sky-700">history</span>{" "}
            <span className="text-indigo-600">=</span>{" "}
            <span className="text-indigo-600">await</span>{" "}
            <span className="text-sky-700">lix</span>
            <span className="text-gray-900">.</span>
            <span className="text-sky-700">db</span>
            <br />
            {"  "}
            <span className="text-gray-900">.</span>
            <span className="text-sky-700">selectFrom</span>
            <span className="text-gray-900">(</span>
            <span className="text-amber-600">"file_history"</span>
            <span className="text-gray-900">)</span>
            <br />
            {"  "}
            <span className="text-gray-900">.</span>
            <span className="text-sky-700">selectAll</span>
            <span className="text-gray-900">()</span>
            <br />
            {"  "}
            <span className="text-gray-900">.</span>
            <span className="text-sky-700">where</span>
            <span className="text-gray-900">(</span>
            <span className="text-amber-600">"path"</span>
            <span className="text-gray-900">,</span>{" "}
            <span className="text-amber-600">"="</span>
            <span className="text-gray-900">,</span>{" "}
            <span className="text-amber-600">"/config.json"</span>
            <span className="text-gray-900">)</span>
            <br />
            {"  "}
            <span className="text-gray-900">.</span>
            <span className="text-sky-700">where</span>
            <span className="text-gray-900">(</span>
            <span className="text-amber-600">"lixcol_commit_id"</span>
            <span className="text-gray-900">,</span>{" "}
            <span className="text-amber-600">"="</span>
            <span className="text-gray-900">,</span>{" "}
            <span className="text-sky-700">checkpoint</span>
            <span className="text-gray-900">.</span>
            <span className="text-sky-700">id</span>
            <span className="text-gray-900">)</span>
            <br />
            {"  "}
            <span className="text-gray-900">.</span>
            <span className="text-sky-700">execute</span>
            <span className="text-gray-900">();</span>
          </>
        );
      case "branching":
        return (
          <>
            <span className="text-gray-500">// Create a new version</span>
            <br />
            <span className="text-indigo-600">const</span>{" "}
            <span className="text-sky-700">v2</span>{" "}
            <span className="text-indigo-600">=</span>{" "}
            <span className="text-indigo-600">await</span>{" "}
            <span className="text-sky-700">createVersion</span>
            <span className="text-gray-900">({"{"}</span>
            <br />
            {"  "}
            <span className="text-sky-700">lix</span>
            <span className="text-gray-900">,</span>
            <br />
            {"  "}
            <span className="text-sky-700">name</span>
            <span className="text-gray-900">:</span>{" "}
            <span className="text-amber-600">"v2-draft"</span>
            <span className="text-gray-900">,</span>
            <br />
            <span className="text-gray-900">{"});"}</span>
            <br />
            <br />
            <span className="text-gray-500">
              // Update file in the new version
            </span>
            <br />
            <span className="text-indigo-600">await</span>{" "}
            <span className="text-sky-700">lix</span>
            <span className="text-gray-900">.</span>
            <span className="text-sky-700">db</span>
            <br />
            {"  "}
            <span className="text-gray-900">.</span>
            <span className="text-sky-700">updateTable</span>
            <span className="text-gray-900">(</span>
            <span className="text-amber-600">"file_by_version"</span>
            <span className="text-gray-900">)</span>
            <br />
            {"  "}
            <span className="text-gray-900">.</span>
            <span className="text-sky-700">set</span>
            <span className="text-gray-900">({"{"}</span>{" "}
            <span className="text-sky-700">data</span>
            <span className="text-gray-900">:</span>{" "}
            <span className="text-amber-600">"..."</span>{" "}
            <span className="text-gray-900">{"}"})</span>
            <br />
            {"  "}
            <span className="text-gray-900">.</span>
            <span className="text-sky-700">where</span>
            <span className="text-gray-900">(</span>
            <span className="text-amber-600">"lixcol_version_id"</span>
            <span className="text-gray-900">,</span>{" "}
            <span className="text-amber-600">"="</span>
            <span className="text-gray-900">,</span>{" "}
            <span className="text-sky-700">v2</span>
            <span className="text-gray-900">.</span>
            <span className="text-sky-700">id</span>
            <span className="text-gray-900">)</span>
            <br />
            {"  "}
            <span className="text-gray-900">.</span>
            <span className="text-sky-700">execute</span>
            <span className="text-gray-900">();</span>
          </>
        );
      case "diffs":
        return (
          <>
            <span className="text-gray-500">// Create versions</span>
            <br />
            <span className="text-indigo-600">const</span>{" "}
            <span className="text-sky-700">v1</span>{" "}
            <span className="text-indigo-600">=</span>{" "}
            <span className="text-indigo-600">await</span>{" "}
            <span className="text-sky-700">createVersion</span>
            <span className="text-gray-900">({"{"}</span>{" "}
            <span className="text-sky-700">name</span>
            <span className="text-gray-900">:</span>{" "}
            <span className="text-amber-600">"v1"</span>{" "}
            <span className="text-gray-900">{"}"})</span>
            <br />
            <span className="text-indigo-600">const</span>{" "}
            <span className="text-sky-700">v2</span>{" "}
            <span className="text-indigo-600">=</span>{" "}
            <span className="text-indigo-600">await</span>{" "}
            <span className="text-sky-700">createVersion</span>
            <span className="text-gray-900">({"{"}</span>{" "}
            <span className="text-sky-700">name</span>
            <span className="text-gray-900">:</span>{" "}
            <span className="text-amber-600">"v2"</span>{" "}
            <span className="text-gray-900">{"}"})</span>
            <br />
            <br />
            <span className="text-gray-500">// Compare versions</span>
            <br />
            <span className="text-indigo-600">const</span>{" "}
            <span className="text-sky-700">diff</span>{" "}
            <span className="text-indigo-600">=</span>{" "}
            <span className="text-indigo-600">await</span>{" "}
            <span className="text-sky-700">selectVersionDiff</span>
            <span className="text-gray-900">({"{"}</span>
            <br />
            {"  "}
            <span className="text-sky-700">lix</span>
            <span className="text-gray-900">,</span>
            <br />
            {"  "}
            <span className="text-sky-700">source</span>
            <span className="text-gray-900">:</span>{" "}
            <span className="text-sky-700">v1</span>
            <span className="text-gray-900">,</span>
            <br />
            {"  "}
            <span className="text-sky-700">target</span>
            <span className="text-gray-900">:</span>{" "}
            <span className="text-sky-700">v2</span>
            <span className="text-gray-900">,</span>
            <br />
            <span className="text-gray-900">{"})"}</span>
            <br />
            {"  "}
            <span className="text-gray-900">.</span>
            <span className="text-sky-700">execute</span>
            <span className="text-gray-900">();</span>
          </>
        );
      default:
        return null;
    }
  };

  const buildShowcases = [
    {
      id: "flashtype",
      title: "Flashtype – AI Markdown Editor",
      category: "AI content",
      description:
        "Generate documents with AI, capture history for every prompt, and ship safe revisions.",
      screenshot: "/flashtype.png",
      fallback: "Flashtype Demo",
      href: "https://flashtype.ai",
      ctaLabel: "Explore Flashtype →",
      creator: "Flashtype",
      creatorRole: "Public app",
      creatorInitials: "FT",
    },
    {
      id: "inlang",
      title: "Inlang – Software globalization ecosystem",
      category: "Localization platform",
      description:
        "Create, localize, and deliver product experiences with AI-assisted translation, review workflows, and Lix change control.",
      screenshot: "/inlang.png",
      fallback: "Inlang Preview",
      href: "https://inlang.com",
      ctaLabel: "Explore Inlang →",
      creator: "Inlang",
      creatorRole: "Product ecosystem",
      creatorInitials: "IL",
    },
    {
      id: "prosemirror",
      title: "ProseMirror / TipTap Plugin",
      category: "Real-time editors",
      description:
        "Collaborative publishing UI with branching proposals, inline reviews, and one-click merges.",
      screenshot: "/prosemirror.png",
      fallback: "ProseMirror Demo",
      href: "https://github.com/opral/lix/tree/main/packages/plugin-prosemirror",
      ctaLabel: "View code →",
      creator: "Studio Alva",
      creatorRole: "Product studio",
      creatorInitials: "SA",
    },
  ];

  const docsPath = "/docs";
  const pathname = useRouterState({
    select: (state) => state.location.pathname,
  });
  const githubStars = getGithubStars("opral/lix");

  const formatStars = (count: number) => {
    if (count >= 1000) {
      return `${(count / 1000).toFixed(1).replace(/\.0$/, "")}k`;
    }
    return count.toString();
  };

  const featureSpotlights = [
    {
      id: "review-everything",
      title: "Every change is tracked",
      description:
        "Lix tracks every change. Query the history and display diffs in your app.",
      Illustration: CursorEditingIllustration,
    },
    {
      id: "human-approval",
      title: "Users stay in control",
      description:
        "Change proposals let users review, accept, or reject changes.",
      Illustration: AuditWorkflowIllustration,
    },
    {
      id: "async-workflows",
      title: "Agents can experiment",
      description: (
        <>
          Agents work in isolated{" "}
          <a href={docsPath} className="underline hover:text-[#0891b2]">
            versions
          </a>{" "}
          without affecting user data.
        </>
      ),
      Illustration: AsyncWorkflowIllustration,
    },
  ];

  const createImageErrorHandler =
    (fallback: string) => (event: SyntheticEvent<HTMLImageElement>) => {
      const container = event.currentTarget.parentElement;
      if (!container) {
        return;
      }

      container.style.backgroundColor = "#0f172a";
      container.style.display = "flex";
      container.style.alignItems = "center";
      container.style.justifyContent = "center";
      container.innerHTML = `<div style="color: #cbd5f5; font-size: 14px; font-weight: 500;">${fallback}</div>`;
    };

  const navLinks = [
    { href: docsPath, label: "Docs" },
    { href: "/plugins/", label: "Plugins" },
    { href: "/blog/", label: "Blog" },
  ];

  const isActive = (href: string) =>
    href === "/"
      ? pathname === "/"
      : pathname === href || pathname.startsWith(`${href.replace(/\/$/, "")}/`);

  const socialLinks = [
    {
      href: "https://discord.gg/gdMPPWy57R",
      label: "Discord",
      Icon: DiscordIcon,
      sizeClass: "h-5 w-5",
    },
    {
      href: "https://x.com/lixCCS",
      label: "X",
      Icon: XIcon,
      sizeClass: "h-4 w-4",
    },
  ];

  return (
    <div className="font-sans text-gray-900 bg-white">
      <header className="sticky top-0 z-50 border-b border-gray-200 bg-white/80 backdrop-blur">
        <div className="mx-auto flex w-full max-w-[1440px] items-center justify-between pl-6 pr-6 py-3">
          <a
            href="/"
            className="flex items-center text-[#0891B2]"
            aria-label="lix home"
          >
            <LixLogo className="h-7 w-7" />
            <span className="sr-only">lix</span>
          </a>
          <div className="flex items-center gap-6">
            <nav className="hidden items-center gap-4 text-sm font-medium text-gray-700 sm:flex">
              {navLinks.map(({ href, label }) => (
                <a
                  key={href}
                  href={href}
                  className={
                    isActive(href)
                      ? href.startsWith("/plugins")
                        ? "px-2 py-1 text-[#0891B2] hover:text-[#0692B6]"
                        : "px-2 py-1 text-[#0891B2]"
                      : "px-2 py-1 transition-colors hover:text-[#0692B6]"
                  }
                  aria-current={isActive(href) ? "page" : undefined}
                >
                  {label}
                </a>
              ))}
            </nav>
            <div
              className="hidden h-4 w-px bg-gray-200 sm:block"
              aria-hidden="true"
            />
            <div className="flex items-center gap-3">
              {socialLinks.map(({ href, label, Icon, sizeClass }) => (
                <a
                  key={label}
                  href={href}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-gray-900 transition-colors hover:text-gray-900"
                  aria-label={label}
                >
                  <Icon className={sizeClass ?? "h-5 w-5"} />
                </a>
              ))}
              <div className="h-4 w-px bg-gray-200" aria-hidden="true" />
              <a
                href="https://github.com/opral/lix"
                target="_blank"
                rel="noopener noreferrer"
                className="group inline-flex items-center gap-1.5 text-sm font-medium text-gray-700 transition-colors hover:text-gray-700"
              >
                <GitHubIcon className="h-5 w-5" />
                GitHub
                {githubStars !== null && (
                  <span
                    className="inline-flex items-center gap-1 text-gray-500 transition-colors group-hover:text-gray-500"
                    title={`${githubStars.toLocaleString()} GitHub stars`}
                    aria-label={`${githubStars.toLocaleString()} GitHub stars`}
                  >
                    <span className="relative h-3.5 w-3.5" aria-hidden="true">
                      <svg
                        className="absolute inset-0 h-3.5 w-3.5 text-gray-400 group-hover:opacity-0 transition-opacity"
                        viewBox="0 0 24 24"
                        fill="none"
                        stroke="currentColor"
                        strokeWidth="2"
                        strokeLinecap="round"
                        strokeLinejoin="round"
                      >
                        <polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2" />
                      </svg>
                      <svg
                        className="absolute inset-0 h-3.5 w-3.5 text-yellow-500 opacity-0 group-hover:opacity-100 transition-opacity"
                        viewBox="0 0 16 16"
                        fill="currentColor"
                      >
                        <path d="M8 .25a.75.75 0 0 1 .673.418l1.882 3.815 4.21.612a.75.75 0 0 1 .416 1.279l-3.046 2.97.719 4.192a.75.75 0 0 1-1.088.791L8 12.347l-3.766 1.98a.75.75 0 0 1-1.088-.79l.72-4.194L.818 6.374a.75.75 0 0 1 .416-1.28l4.21-.611L7.327.668A.75.75 0 0 1 8 .25z" />
                      </svg>
                    </span>
                    <span>{formatStars(githubStars)}</span>
                  </span>
                )}
              </a>
            </div>
          </div>
        </div>
      </header>
      {/* Main content */}
      <main className="relative px-4 sm:px-6">
        {/* Hero Section - Simplified */}
        <section className="relative pt-20 pb-12 px-4 sm:px-6">
          <div className="relative max-w-4xl mx-auto text-center">
            {/* Beta Chip */}
            <a
              href="https://github.com/opral/lix/issues/374"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-2 mb-6 px-3 py-1.5 rounded-full bg-amber-50 border border-amber-200 text-sm text-amber-800 hover:bg-amber-100 transition-colors"
            >
              <span className="inline-block w-2 h-2 rounded-full bg-amber-400" />
              <span>
                <span className="font-medium">Lix is in beta</span> · Follow
                progress to v1.0
              </span>
              <svg
                className="w-3.5 h-3.5"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
                strokeWidth={2}
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  d="M9 5l7 7-7 7"
                />
              </svg>
            </a>
            <h1 className="text-gray-900 font-bold leading-[1.1] text-4xl sm:text-5xl md:text-6xl tracking-tight">
              Change control for
              <br />
              apps and <span className="text-[#0891b2]">AI agents</span>
            </h1>

            <p className="text-gray-500 text-lg sm:text-xl max-w-4xl mx-auto mt-8">
              Lix is an embeddable change control system that enables Git-like
              features for any file format.
            </p>

            {/* Trust signals - Large stat blocks with icons */}
            <div className="flex items-center justify-center gap-8 sm:gap-12 mt-12">
              <div className="flex flex-col items-center">
                <svg
                  className="w-5 h-5 text-gray-400 mb-1.5"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                  strokeWidth={1.5}
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5M16.5 12L12 16.5m0 0L7.5 12m4.5 4.5V3"
                  />
                </svg>
                <div className="text-2xl font-bold text-gray-900">50k+</div>
                <div className="text-sm text-gray-500 mt-1">
                  Weekly downloads
                </div>
              </div>
              <div className="w-px h-14 bg-gray-200" />
              <a
                href="https://github.com/opral/lix/graphs/contributors"
                target="_blank"
                rel="noopener noreferrer"
                className="flex flex-col items-center hover:opacity-70 transition-opacity"
              >
                <svg
                  className="w-5 h-5 text-gray-400 mb-1.5"
                  viewBox="0 0 24 24"
                  fill="currentColor"
                >
                  <path d="M12 2C6.477 2 2 6.477 2 12c0 4.42 2.865 8.166 6.839 9.489.5.092.682-.217.682-.48 0-.237-.008-.866-.013-1.7-2.782.603-3.369-1.34-3.369-1.34-.454-1.156-1.11-1.464-1.11-1.464-.908-.62.069-.608.069-.608 1.003.07 1.531 1.03 1.531 1.03.892 1.529 2.341 1.087 2.91.831.092-.645.35-1.087.636-1.337-2.22-.253-4.555-1.11-4.555-4.943 0-1.091.39-1.984 1.029-2.683-.103-.253-.446-1.27.098-2.647 0 0 .84-.268 2.75 1.026A9.578 9.578 0 0112 6.836c.85.004 1.705.114 2.504.336 1.909-1.294 2.747-1.026 2.747-1.026.546 1.377.203 2.394.1 2.647.64.699 1.028 1.592 1.028 2.683 0 3.842-2.339 4.687-4.566 4.935.359.309.678.919.678 1.852 0 1.336-.012 2.415-.012 2.743 0 .267.18.578.688.48C19.138 20.161 22 16.416 22 12c0-5.523-4.477-10-10-10z" />
                </svg>
                <div className="text-2xl font-bold text-gray-900">100+</div>
                <div className="text-sm text-gray-500 mt-1">Contributors</div>
              </a>
              <div className="w-px h-14 bg-gray-200" />
              <div className="flex flex-col items-center">
                <svg
                  className="w-5 h-5 text-gray-400 mb-1.5"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                  strokeWidth={1.5}
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    d="M3 6l3 1m0 0l-3 9a5.002 5.002 0 006.001 0M6 7l3 9M6 7l6-2m6 2l3-1m-3 1l-3 9a5.002 5.002 0 006.001 0M18 7l3 9m-3-9l-6-2m0-2v2m0 16V5m0 16H9m3 0h3"
                  />
                </svg>
                <div className="text-2xl font-bold text-gray-900">MIT</div>
                <div className="text-sm text-gray-500 mt-1">Open Source</div>
              </div>
            </div>

            <div className="flex flex-col sm:flex-row items-center justify-center gap-3 mt-8">
              <a
                href={docsPath}
                className="inline-flex items-center justify-center h-11 px-6 rounded-lg text-sm font-medium bg-[#0891b2] text-white hover:bg-[#0e7490] transition-colors"
              >
                Get started
                <svg
                  className="h-4 w-4 ml-2"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                  strokeWidth={2}
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    d="M14 5l7 7m0 0l-7 7m7-7H3"
                  />
                </svg>
              </a>
              <a
                href="https://github.com/opral/lix"
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex h-11 items-center justify-center gap-2 px-5 rounded-lg border border-gray-300 bg-white text-sm text-gray-800 transition-colors duration-200 hover:bg-gray-50"
              >
                <svg
                  className="w-5 h-5"
                  viewBox="0 0 24 24"
                  fill="currentColor"
                >
                  <path d="M12 2C6.477 2 2 6.477 2 12c0 4.42 2.865 8.166 6.839 9.489.5.092.682-.217.682-.48 0-.237-.008-.866-.013-1.7-2.782.603-3.369-1.34-3.369-1.34-.454-1.156-1.11-1.464-1.11-1.464-.908-.62.069-.608.069-.608 1.003.07 1.531 1.03 1.531 1.03.892 1.529 2.341 1.087 2.91.831.092-.645.35-1.087.636-1.337-2.22-.253-4.555-1.11-4.555-4.943 0-1.091.39-1.984 1.029-2.683-.103-.253-.446-1.27.098-2.647 0 0 .84-.268 2.75 1.026A9.578 9.578 0 0112 6.836c.85.004 1.705.114 2.504.336 1.909-1.294 2.747-1.026 2.747-1.026.546 1.377.203 2.394.1 2.647.64.699 1.028 1.592 1.028 2.683 0 3.842-2.339 4.687-4.566 4.935.359.309.678.919.678 1.852 0 1.336-.012 2.415-.012 2.743 0 .267.18.578.688.48C19.138 20.161 22 16.416 22 12c0-5.523-4.477-10-10-10z" />
                </svg>
                GitHub
              </a>
            </div>

            {/* Hero code snippet with language tabs */}
            <div className="mt-12 w-full max-w-2xl mx-auto">
              <div className="rounded-xl border border-gray-200 bg-white overflow-hidden">
                <div className="flex items-center px-4 border-b border-gray-200 bg-gray-50">
                  <div className="flex gap-6 text-sm">
                    <button className="flex items-center gap-2 text-gray-900 font-medium border-b-2 border-gray-900 py-3 px-1 cursor-pointer">
                      <JsIcon className="h-4 w-4" />
                      JavaScript
                    </button>
                    <a
                      href="https://github.com/opral/lix/issues/370"
                      target="_blank"
                      rel="noopener noreferrer"
                      className="flex items-center gap-2 text-gray-400 py-3 px-1 hover:text-gray-600 transition-colors cursor-pointer"
                    >
                      <PythonIcon className="h-4 w-4" />
                      Python
                    </a>
                    <a
                      href="https://github.com/opral/lix/issues/371"
                      target="_blank"
                      rel="noopener noreferrer"
                      className="flex items-center gap-2 text-gray-400 py-3 px-1 hover:text-gray-600 transition-colors cursor-pointer"
                    >
                      <RustIcon className="h-4 w-4" />
                      Rust
                    </a>
                    <a
                      href="https://github.com/opral/lix/issues/373"
                      target="_blank"
                      rel="noopener noreferrer"
                      className="flex items-center gap-2 text-gray-400 py-3 px-1 hover:text-gray-600 transition-colors cursor-pointer"
                    >
                      <GoIcon className="h-4 w-4" />
                      Go
                    </a>
                  </div>
                </div>
                <div className="p-5 text-sm leading-relaxed font-mono text-left overflow-x-auto whitespace-pre-wrap">
                  <span className="text-indigo-600">import</span>{" "}
                  <span className="text-gray-900">
                    {"{ openLix, InMemoryEnvironment }"}
                  </span>{" "}
                  <span className="text-indigo-600">from</span>{" "}
                  <span className="text-amber-600">"@lix-js/sdk"</span>
                  <span className="text-gray-900">;</span>
                  <br />
                  <span className="text-indigo-600">import</span>{" "}
                  <span className="text-gray-900">{"{ plugin "}</span>
                  <span className="text-indigo-600">as</span>
                  <span className="text-gray-900">{" json }"}</span>{" "}
                  <span className="text-indigo-600">from</span>{" "}
                  <span className="text-amber-600">"@lix-js/plugin-json"</span>
                  <span className="text-gray-900">;</span>
                  <br />
                  <br />
                  <span className="text-indigo-600">const</span>{" "}
                  <span className="text-gray-900">lix</span>{" "}
                  <span className="text-gray-900">= openLix</span>
                  <span className="text-gray-900">{"({"}</span>
                  <br />
                  <span className="text-sky-600">{"  environment"}</span>
                  <span className="text-gray-900">{": "}</span>
                  <span className="text-indigo-600">new</span>
                  <span className="text-gray-900">
                    {" InMemoryEnvironment(),"}
                  </span>
                  <br />
                  <span className="text-sky-600">{"  providePlugins"}</span>
                  <span className="text-gray-900">{": ["}</span>
                  <span className="text-gray-900">json</span>
                  <span className="text-gray-900">{"]"}</span>
                  <br />
                  <span className="text-gray-900">{"})"}</span>
                </div>
              </div>
            </div>
          </div>
        </section>

        {/* Value Props - Lightweight */}
        <section className="py-12 px-6 sm:px-12 md:px-16 bg-white">
          <div className="max-w-6xl mx-auto">
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-8 sm:gap-12">
              <div className="flex flex-col items-center sm:items-start gap-3">
                <svg
                  className="w-7 h-7 text-[#0891b2]"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                  strokeWidth={1.5}
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m2.25 0H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z"
                  />
                </svg>
                <div className="text-center sm:text-left">
                  <h3 className="text-lg font-semibold text-gray-900">
                    Any file format
                  </h3>
                  <p className="text-gray-600 text-base mt-2">
                    Lix can track changes in any file format like{" "}
                    <code className="text-sm bg-gray-100 px-1.5 py-0.5 rounded text-gray-800 font-medium">
                      .xlsx
                    </code>
                    ,{" "}
                    <code className="text-sm bg-gray-100 px-1.5 py-0.5 rounded text-gray-800 font-medium">
                      .pdf
                    </code>
                    ,{" "}
                    <code className="text-sm bg-gray-100 px-1.5 py-0.5 rounded text-gray-800 font-medium">
                      .json
                    </code>{" "}
                    etc. via plugins.
                  </p>
                </div>
              </div>
              <div className="flex flex-col items-center sm:items-start gap-3">
                <svg
                  className="w-7 h-7 text-[#0891b2]"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                  strokeWidth={1.5}
                >
                  <circle cx="18" cy="18" r="3" />
                  <circle cx="6" cy="6" r="3" />
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    d="M6 21V9a9 9 0 009 9"
                  />
                </svg>
                <div className="text-center sm:text-left">
                  <h3 className="text-lg font-semibold text-gray-900">
                    Diff, merge & branch
                  </h3>
                  <p className="text-gray-600 text-base mt-2">
                    Query history, compare versions, and manage changes via SQL.
                  </p>
                </div>
              </div>
              <div className="flex flex-col items-center sm:items-start gap-3">
                <svg
                  className="w-7 h-7 text-[#0891b2]"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                  strokeWidth={1.5}
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    d="M13.5 16.875h3.375m0 0h3.375m-3.375 0V13.5m0 3.375v3.375M6 10.5h2.25a2.25 2.25 0 002.25-2.25V6a2.25 2.25 0 00-2.25-2.25H6A2.25 2.25 0 003.75 6v2.25A2.25 2.25 0 006 10.5zm0 9.75h2.25A2.25 2.25 0 0010.5 18v-2.25a2.25 2.25 0 00-2.25-2.25H6a2.25 2.25 0 00-2.25 2.25V18A2.25 2.25 0 006 20.25zm9.75-9.75H18a2.25 2.25 0 002.25-2.25V6A2.25 2.25 0 0018 3.75h-2.25A2.25 2.25 0 0013.5 6v2.25a2.25 2.25 0 002.25 2.25z"
                  />
                </svg>
                <div className="text-center sm:text-left">
                  <h3 className="text-lg font-semibold text-gray-900">
                    Integrates with your stack
                  </h3>
                  <p className="text-gray-600 text-base mt-2">
                    Lix runs embedded in your app as a single SQLite file,
                    persistable anywhere e.g. local FS, S3, your database.
                  </p>
                </div>
              </div>
            </div>
          </div>
        </section>

        {/* What You Can Build Section */}
        <section className="py-16 px-6 sm:px-12 md:px-16 bg-white border-t border-gray-200">
          <div className="max-w-6xl mx-auto">
            <h2 className="flex flex-wrap items-center justify-center gap-2 text-center text-2xl sm:text-3xl font-bold text-gray-900">
              <span>What people build with</span>
              <span className="text-[#0692B6]">lix</span>
            </h2>
            <div className="mt-14 grid grid-cols-1 gap-10 sm:grid-cols-2 xl:grid-cols-3">
              {buildShowcases.map(
                ({ id, title, screenshot, fallback, href, ctaLabel }) => (
                  <a
                    key={id}
                    href={href}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="group flex flex-col overflow-hidden rounded-xl border border-gray-200 transition-transform duration-300 hover:-translate-y-2 hover:border-gray-300"
                  >
                    <div className="relative aspect-[16/9] overflow-hidden">
                      <img
                        src={screenshot}
                        alt={title}
                        className="h-full w-full object-cover transition-transform duration-500 group-hover:scale-[1.04]"
                        onError={createImageErrorHandler(fallback)}
                      />
                    </div>
                    <div className="flex items-center justify-between gap-4 px-5 py-4 text-gray-900">
                      <h3 className="text-lg font-semibold">{title}</h3>
                      <span className="relative inline-flex items-center text-sm font-medium text-[#0692B6]">
                        <span className="sr-only">{ctaLabel}</span>
                        <span
                          className="flex items-center gap-1 opacity-0 transition-opacity duration-200 group-hover:opacity-100"
                          style={{ color: "#047497" }}
                        >
                          Open
                          <span aria-hidden>→</span>
                        </span>
                      </span>
                    </div>
                  </a>
                ),
              )}
            </div>
          </div>
        </section>

        {/* Primary Features */}
        <section className="pt-10 pb-16 px-6 sm:px-12 md:px-16 bg-white border-t border-gray-200">
          <div className="max-w-6xl mx-auto">
            <div className="text-center mb-12">
              <h2 className="text-2xl sm:text-3xl font-bold text-gray-900">
                How lix works
              </h2>
            </div>
            <div className="grid grid-cols-1 gap-10 lg:grid-cols-[minmax(0,340px)_1fr]">
              {/* Left: Features List */}
              <div className="flex flex-col border-t border-gray-100">
                {[
                  {
                    id: "filesystem",
                    title: "Filesystem",
                    description:
                      "Store and manage files with SQL. Read, write, and organize files just like a traditional filesystem.",
                    icon: (
                      <svg
                        xmlns="http://www.w3.org/2000/svg"
                        className="h-5 w-5"
                        fill="none"
                        viewBox="0 0 24 24"
                        stroke="currentColor"
                        strokeWidth={2}
                      >
                        <path
                          strokeLinecap="round"
                          strokeLinejoin="round"
                          d="M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z"
                        />
                      </svg>
                    ),
                  },
                  {
                    id: "history",
                    title: "History",
                    description:
                      "Track every operation, not just snapshots. Know exactly what changed, when, and by whom.",
                    icon: (
                      <svg
                        xmlns="http://www.w3.org/2000/svg"
                        className="h-5 w-5"
                        fill="none"
                        viewBox="0 0 24 24"
                        stroke="currentColor"
                        strokeWidth={2}
                      >
                        <path
                          strokeLinecap="round"
                          strokeLinejoin="round"
                          d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"
                        />
                      </svg>
                    ),
                  },
                  {
                    id: "branching",
                    title: "Versions (Branching)",
                    description:
                      "Create named versions and branches. Experiment safely without affecting the main state.",
                    icon: (
                      <svg
                        xmlns="http://www.w3.org/2000/svg"
                        className="h-5 w-5"
                        fill="none"
                        viewBox="0 0 24 24"
                        stroke="currentColor"
                        strokeWidth={2}
                      >
                        <path
                          strokeLinecap="round"
                          strokeLinejoin="round"
                          d="M6 3v12"
                        />
                        <circle cx="18" cy="6" r="3" />
                        <circle cx="6" cy="18" r="3" />
                        <path
                          strokeLinecap="round"
                          strokeLinejoin="round"
                          d="M18 9a9 9 0 0 1-9 9"
                        />
                      </svg>
                    ),
                  },
                  {
                    id: "diffs",
                    title: "Diffs",
                    description:
                      "Compare any two points in time. See granular differences at the operation level.",
                    icon: (
                      <svg
                        xmlns="http://www.w3.org/2000/svg"
                        className="h-5 w-5"
                        fill="none"
                        viewBox="0 0 24 24"
                        stroke="currentColor"
                        strokeWidth={2}
                      >
                        <rect
                          x="3"
                          y="3"
                          width="18"
                          height="18"
                          rx="2"
                          ry="2"
                        />
                        <line x1="12" y1="3" x2="12" y2="21" />
                      </svg>
                    ),
                  },
                ].map((feature) => (
                  <button
                    key={feature.id}
                    onClick={() => setActiveFeature(feature.id)}
                    className={`group flex gap-4 p-6 text-left transition-all rounded-xl hover:bg-gray-50 cursor-pointer ${
                      activeFeature === feature.id ? "bg-gray-100" : "bg-white"
                    }`}
                  >
                    <div
                      className={`flex-shrink-0 mt-1 ${
                        activeFeature === feature.id
                          ? "text-gray-900"
                          : "text-gray-400 group-hover:text-gray-600"
                      }`}
                    >
                      {feature.icon}
                    </div>
                    <div className="flex flex-col gap-2">
                      <h3
                        className={`text-lg font-semibold ${
                          activeFeature === feature.id
                            ? "text-gray-900"
                            : "text-gray-900"
                        }`}
                      >
                        {feature.title}
                      </h3>
                      <p className="text-base text-gray-600 leading-relaxed">
                        {feature.description}
                      </p>
                    </div>
                  </button>
                ))}
                <a
                  href={docsPath}
                  className="inline-flex items-center gap-2 p-6 text-[#0692B6] font-medium hover:text-[#047497] transition-colors"
                >
                  Explore all features <span aria-hidden>→</span>
                </a>
              </div>

              {/* Right: Code Examples */}
              <div className="relative min-w-0 overflow-hidden rounded-xl border border-gray-200 bg-white flex flex-col">
                <div className="flex items-center px-4 border-b border-gray-200 bg-gray-50">
                  <div className="flex gap-6 text-sm">
                    <button className="flex items-center gap-2 text-gray-900 font-medium border-b-2 border-gray-900 py-3 px-1 cursor-pointer">
                      <JsIcon className="h-4 w-4" />
                      JavaScript
                    </button>
                    <a
                      href="https://github.com/opral/lix/issues/370"
                      target="_blank"
                      rel="noopener noreferrer"
                      className="flex items-center gap-2 text-gray-500 py-3 px-1 hover:text-gray-700 transition-colors cursor-pointer"
                    >
                      <PythonIcon className="h-4 w-4" />
                      Python
                    </a>
                    <a
                      href="https://github.com/opral/lix/issues/371"
                      target="_blank"
                      rel="noopener noreferrer"
                      className="flex items-center gap-2 text-gray-500 py-3 px-1 hover:text-gray-700 transition-colors cursor-pointer"
                    >
                      <RustIcon className="h-4 w-4" />
                      Rust
                    </a>
                    <a
                      href="https://github.com/opral/lix/issues/373"
                      target="_blank"
                      rel="noopener noreferrer"
                      className="flex items-center gap-2 text-gray-500 py-3 px-1 hover:text-gray-700 transition-colors cursor-pointer"
                    >
                      <GoIcon className="h-4 w-4" />
                      Go
                    </a>
                  </div>
                </div>
                <div className="flex-1 overflow-x-auto p-6 text-sm leading-7 font-mono whitespace-pre">
                  <div className="mb-6">
                    <span className="text-gray-500">// JavaScript SDK</span>
                    <br />
                    <span className="text-indigo-600">import</span>{" "}
                    <span className="text-gray-900">
                      {"{ openLix, selectWorkingDiff, InMemoryEnvironment }"}
                    </span>{" "}
                    <span className="text-indigo-600">from</span>{" "}
                    <span className="text-amber-600">"@lix-js/sdk"</span>
                    <span className="text-gray-900">;</span>
                    <br />
                    <span className="text-indigo-600">import</span>{" "}
                    <span className="text-gray-900">
                      {"{ plugin as json }"}
                    </span>{" "}
                    <span className="text-indigo-600">from</span>{" "}
                    <span className="text-amber-600">
                      "@lix-js/plugin-json"
                    </span>
                    <span className="text-gray-900">;</span>
                    <br />
                    <br />
                    <span className="text-gray-500">
                      // 1) Create an in-memory lix
                    </span>
                    <br />
                    <span className="text-indigo-600">const</span>{" "}
                    <span className="text-sky-700">lix</span>{" "}
                    <span className="text-indigo-600">=</span>{" "}
                    <span className="text-indigo-600">await</span>{" "}
                    <span className="text-sky-700">openLix</span>
                    <span className="text-gray-900">({"{"}</span>
                    <br />
                    {"    "}
                    <span className="text-sky-700">environment</span>
                    <span className="text-gray-900">:</span>{" "}
                    <span className="text-indigo-600">new</span>{" "}
                    <span className="text-sky-700">InMemoryEnvironment</span>
                    <span className="text-gray-900">(),</span>
                    <br />
                    {"    "}
                    <span className="text-sky-700">providePlugins</span>
                    <span className="text-gray-900">:</span>{" "}
                    <span className="text-gray-900">[</span>
                    <span className="text-sky-700">json</span>
                    <span className="text-gray-900">]</span>
                    <br />
                    <span className="text-gray-900">{"});"}</span>
                  </div>
                  <code>{getFeatureCode(activeFeature)}</code>
                </div>
              </div>
            </div>
          </div>
        </section>

        {/* Feature Spotlights */}
        <section className="py-12 px-6 sm:px-12 md:px-16 bg-white border-t border-gray-200">
          <div className="max-w-6xl mx-auto">
            <h2 className="text-center text-2xl sm:text-3xl font-bold text-gray-900">
              Lix enables the most powerful AI apps & agents
            </h2>
            <div className="mt-14 grid grid-cols-1 gap-10 sm:grid-cols-2 xl:grid-cols-3">
              {featureSpotlights.map(
                ({ id, title, description, Illustration }) => (
                  <div key={id} className="flex flex-col gap-6">
                    <div className="flex justify-center items-center h-40">
                      <Illustration />
                    </div>
                    <div>
                      <h3 className="text-lg font-semibold text-gray-900">
                        {title}
                      </h3>
                      <p className="mt-2 text-base text-gray-600">
                        {description}
                      </p>
                    </div>
                  </div>
                ),
              )}
            </div>
          </div>
        </section>

        <Footer />
      </main>
    </div>
  );
}

export default LandingPage;
