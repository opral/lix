## Prompt

Our goal now is to replay a large git repository in the new lix packages/engine via the js sdk packages/js-sdk. Ignore the legacy codebase packages/sdk. 

The goal is to identify performance optimization opportunities:

- Queries that are slow
- Storage costs
- Other bottlenecks

The repository is going to https://github.com/vercel/next.js as real world example.

Replay refers to taking each git commit and "replay" it in lix. The real repo has 32,xxx commits. We expect to see 32,xxx commits in lix as well.

We only care about linear history for now. Replay the 30k commits and then benchmark.