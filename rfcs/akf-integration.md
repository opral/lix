# AKF Integration — Trust Metadata for Agent Version Control

## Overview

[AKF](https://github.com/HMAKT99/AKF) (Agent Knowledge Format) is the AI native file format.

Lix provides version control for AI agents. AKF provides trust metadata for the files those agents create. Together: every versioned file carries provenance.

## How it fits

When an AI agent creates or modifies a file tracked by Lix:

1. **Lix** versions the change (what changed, when, by which agent)
2. **AKF** stamps the file with trust metadata (trust score, source provenance, compliance)

The result: Lix tracks the version history, AKF tracks the trust history.

## Usage

```bash
# Agent generates a file
# Lix tracks the version
# AKF stamps trust metadata
akf stamp output.md --agent lix-agent --evidence "generated from primary sources"
akf inspect output.md  # Shows trust score, provenance, compliance
```

## Why

- Version control tracks WHAT changed
- AKF tracks HOW TRUSTWORTHY the change is
- EU AI Act Article 50 (Aug 2, 2026) requires transparency metadata
- Together: complete provenance for the agent era

## Install

```bash
pip install akf
```

## Links

- [akf.dev](https://akf.dev)
- [GitHub](https://github.com/HMAKT99/AKF)
