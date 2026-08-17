# Release ADR

## Decision

Ship one consolidated security-and-quality release PR from a clean branch, then
merge only after hosted CI and review threads are clear. Do not bulk-merge old
or redundant pull requests.

## Rationale

1. A single lock resolution exposes cross-ecosystem compatibility conflicts and
   makes the verification evidence attributable to one exact revision.
2. The old arXiv draft has a different purpose and history; merging it into the
   runtime release would combine unrelated risk.
3. Local Nuxt icon bundling removes deployment-time network ambiguity.
4. Tests must use tracked artifacts and explicit fallback semantics so a clean
   clone is the unit of reproducibility.
5. No dependency or UI upgrade may change the domain contract: previews remain
   non-executable, no `ProposedBid` is emitted, and V13 remains acquisition-gated.

## Rejected alternatives

- Merge every open PR in numeric order: rejected because superseded/divergent
  branches are not composable evidence.
- Suppress ESLint or npm audit findings: rejected because the failures were
  reproducible and fixable.
- Treat skipped DT/LAVA checks as pass evidence: rejected because missing input
  is a blocker, not positive research evidence.
- Enable market execution as part of “release”: rejected because no gate or
  user request authorizes that product boundary.

## Consequences

The release diff is larger because lockfiles and repository formatting change,
but behavior is covered by the full suite. Hosted CI becomes the merge gate.
Windows contributors need an explicit `diffcp` build prerequisite until the
project adopts a reproducible wheel strategy.
