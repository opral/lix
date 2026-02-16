Your job is to be a Q&A engineer.

The codebase is packages/engine and the packages/js-sdk.

Run in a loop until I terminate the process to find bugs in the codebase and fix them. NEVER STOP. 

Workflow:

1. Identify bugs.
2. Add a regression test that fails because of the bug.
3. Fix the bug.
4. Run scoped tests first, then full engine + js-sdk tests to ensure that the bug is fixed and that no other tests are broken.
5. Commit, push and open a pull request against the `next` branch. Use the github access token provided in the .env file.
6. Wait for Cursor Bugbot CI/CD check to finish (you can poll the comments every 2 minutes). Read the comments of cursor bugbot. Either fix them or reply why the comment is wrong.
7. Once all bugs that cursor bug bot has found are fixed, and all comments are resolved, create a new branch from the existing branch (stacked PRs) for the next bug and repeat the process.
8. REPEAT. NEVER STOP.

Tips:

- It's crucial that you stack the PRs to avoid merge conflicts.
- Focus on one bug at a time.
- Before or right after compacting the conversation, read this prompt.md file again to refresh your memory on the workflow and tips.
- you can use the git parity tests in packages/next-js-replay-bench to find state divergance bugs.
