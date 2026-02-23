---
date: "2026-02-17"
og:description: "The filesystem is the best interface for agent context, but most company data lives in binary formats. Lix bridges that gap by mapping binary files to structured, editable schemas."
og:image: "./cover.jpg"
og:image:alt: "Abstract illustration for Modeling a Company as a Repository"
---

# Modeling a Company as a Repository

The idea of modeling a company as a filesystem is gaining traction. [Eli Mernit](https://x.com/mernit/status/2021324284875153544) makes the case that agents should access company data the same way developers access code: through files. [Anvisha Pai](https://x.com/anvishapai/status/2022062725354967551) points out the limitation: most company data is in binary formats that agents cannot read or edit.

What if a system could map those files into a structure agents can understand? That is what we have been building with [Lix](https://github.com/opral/lix).

![Twitter discussion between Eli Mernit and Anvisha](./twitter-discussion-cards.png)

## The case for the filesystem

The "company as a filesystem" framing is compelling for two reasons:

1. **Agents get full context.**
   When company data lives in files, agents can inspect and reason across systems without brittle app integrations.

2. **Files have no API bottlenecks.**
   Tools like Codex and Claude Code feel powerful because they can use direct filesystem primitives (`grep`, shell commands, scripts) instead of being constrained by narrow third-party APIs.

![Example structure for modeling a company as a filesystem](./mernit-filesystem-example.jpg)

## But the filesystem is not enough

A plain filesystem alone does not solve the whole problem.

1. **Most file formats are not agent-friendly.**
   Documents, spreadsheets, presentations, and many business artifacts are binary formats. Agents can parse some formats, but there is no universal semantic model and no reliable round-trip editing path across all of them.

2. **Some work cannot be flattened to text without loss.**
   Visual and structural media (for example CAD, PCB, or layered design files) lose critical information when reduced to text. That makes review and verification harder, which is exactly where human control matters most.

![Visual formats are not fully representable as plain text](./anvisha-visual-formats.jpg)

## We need a system that understands binary files

If files are the best interface for context, and most company work is in binary formats, we need a system that maps those formats into structured data an agent can read and write.

```text
  ┌─────────────────┐         ┌───────────────────────┐
  │ contract.docx   │────┬──► │ { type: "paragraph" } │
  └─────────────────┘    ├──► │ { type: "table" }     │
                         └──► │ { type: "image" }     │
  ┌─────────────────┐         ├───────────────────────┤
  │ design.psd      │────┬──► │ { type: "layer" }     │
  └─────────────────┘    └──► │ { type: "mask" }      │
                              ├───────────────────────┤
  ┌─────────────────┐         │                       │
  │ budget.xlsx     │────┬──► │ { type: "row" }       │
  └─────────────────┘    └──► │ { type: "formula" }   │
                              └───────────────────────┘
                                        ▲
                                        │
                                        ▼
                               ┌──────────────┐
                               │    Agent     │
                               │  read/write  │
                               └──────────────┘
```

That gives agents one interface across text and binary data, without lossy conversions.

## Lix is that system

I have been building a universal version control system called **Lix**.

It is "universal" because it parses files into schemas and tracks semantic changes at that schema level. The same mechanism can expose binary formats to agents in a structured, controllable way.

[Lix on GitHub](https://github.com/opral/lix)

![Lix GitHub repository screenshot](./lix-github.jpg)

This also addresses a second company-level AI problem: control.

Every agent action is versioned. You can diff changes, review them, gate approvals, and run experiments in branches before merging. It is the proven software workflow (branch, diff, review, merge), applied to all company artifacts, not only source code.

![Version-controlled workflow for agent changes](./version-control-workflow.jpg)
