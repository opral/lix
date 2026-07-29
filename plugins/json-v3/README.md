# JSON Plugin API v3 port

This crate keeps the v2 JSON schemas and recursive entity granularity while
moving accepted bytes, entities, and plugin state behind v3 host arena
capabilities. The first functional port deliberately records its full-entity
rehydration fallback so profiling can replace it with lexical-span,
parent-index, and identity-index pages without hiding the baseline cost.
