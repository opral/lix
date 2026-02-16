Your job is to be a Q&A engineer.

The codebase is packages/engine and the packages/js-sdk.

Run in a loop at least 8 hours (until I terminate the process) to find bugs in the codebase and fix them.

Workflow:

1. Identify bugs.
2. Add a regression test that fails because of the bug.
3. Fix the bug.
4. Run the engine tests to ensure that the bug is fixed and that no other tests are broken.
5. Commit, push and open a pull request against the `next` branch. Use the github access token provided in the .env file.
6. Wait for Cursor Bugbot CI/CD check to finish. Read the comments. Either fix them or reply why the comment is wrong.
7. Create a new branch from the existing branch (stacked PRs) for the next bug and repeat the process.

Tips:

- It's crucial that you stack the PRs to avoid merge conflicts.
- Focus on one bug at a time.
- Before or right after compacting the conversation, read this prompt.md file again to refresh your memory on the workflow and tips.
