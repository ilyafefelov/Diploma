# Fix plan

## Completed in this release

- Consolidate Python and Node security updates.
- Restore clean-checkout test truth for the V13 fallback.
- Remove untracked local-document dependencies from repository tests.
- Make Windows line-ending architecture tests stable.
- Bundle dashboard icons locally and test that configuration.
- Clear dashboard lint/type errors without suppressions.
- Add PR/main CI for Python, Dagster, Compose, dashboard, and intro-video.
- Validate the entire release branch in a fresh environment.

## Merge/release sequence

1. Push the branch and open one reviewable PR.
2. Wait for all hosted checks and automated review comments.
3. Fix actionable findings and rerun the relevant gates.
4. Merge the exact green head SHA.
5. Confirm GitHub Pages deployment and live route/assets.
6. Re-audit Dependabot alerts; close only superseded PRs.
7. Publish a GitHub release tied to the merged SHA.

## Deferred backlog

| Priority | Work | Exit evidence |
|---|---|---|
| P1 | Document/install Windows C++ Build Tools or trusted `diffcp` wheel | Fresh Windows `uv sync` succeeds without copying from another venv |
| P1 | Re-audit hosted Dependabot state | No critical/high actionable alerts |
| P2 | Split the large Nuxt client chunk | Build report below agreed route budget |
| P2 | Reduce upstream warning volume | Warning baseline categorized and materially smaller |
| P2 | Add browser accessibility/visual CI | Stable tests for all public/operator routes |
| Gate | Materialize V13/DT inputs | Signed, validated packets; still no execution promotion by implication |
