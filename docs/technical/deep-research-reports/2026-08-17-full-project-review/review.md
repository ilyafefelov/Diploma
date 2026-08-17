# Evidence-led review

## Scope and method

The review used a clean worktree from `origin/main`, not the user's dirty
working directories. It inspected open pull requests, review threads, GitHub
security alerts, releases, workflows, Python and Node dependency graphs,
Dagster definitions, FastAPI routes, Nuxt routes, tracked evidence documents,
and official external market sources.

## Findings

### R1 — Critical/high dependency exposure — resolved on branch

The live repository began with 99 Dependabot alerts, including critical or high
advisories affecting the Nuxt toolchain, serialization/tar transitive packages,
Torch/Python libraries, and several web dependencies. The release branch moves
the Python lock to current compatible security releases, including Torch 2.13,
Transformers 5.x, Cryptography 49, GitPython 3.1.59, Pillow 12.3, Starlette 1.6,
and setuptools 84. The dashboard moves to Nuxt 4.5, Nuxt UI 4.8, ECharts 6.1,
and a secure esbuild override. Both npm workspaces now report zero audit
findings. Hosted alert closure must be verified after merge.

### R2 — No pull-request CI — resolved on branch

Main previously had publishing/deployment workflows but no general PR gate. A
new `.github/workflows/ci.yml` validates Python, Dagster, Compose, dashboard,
and the intro-video package on pull requests and main pushes. A repository test
locks the required commands into the workflow contract.

### R3 — Stale tests depended on local/untracked evidence — resolved

Four clean-main tests failed although the runtime contract was internally
consistent. Two expected a populated V13 packet while the clean checkout
correctly returned `missing_v13_acquisition_packet`; two referenced untracked
local documents. Tests now assert the tracked fallback contract and tracked V13
boundary sources. Production response schemas and execution boundaries were not
changed.

### R4 — Dashboard quality drift — resolved, one performance warning deferred

The baseline dashboard had 172 lint errors, 195 warnings, two Windows-sensitive
tests, and unresolved icon-bundle warnings after dependency upgrades. The
release branch uses explicit public-artifact interfaces, repository formatting,
line-ending-stable assertions, local Lucide/Simple Icons bundles, and a bundling
architecture test. Lint, typecheck, all 261 tests, and static generation pass.
The largest client chunk remains about 722 kB minified; code splitting is a
P2 performance task, not a correctness blocker.

### R5 — Fresh Windows `diffcp` install is not self-contained — open P1

`diffcp==1.1.8` publishes a source distribution and documents a C++11 compiler
requirement. A fresh Windows sync fails without Visual C++/NMake. Verification
used the already compatible CPython 3.12 Windows extension from the established
project environment. The source lock is correct, but the setup documentation
must explicitly require Build Tools or provide a trusted internal wheel cache.

### R6 — Research inputs are absent in a clean checkout — expected blocker

The full verifier skips DT/LAVA readiness and margin-smoke materializations when
their V13 packet/candidate-frame inputs are unavailable. This is correct
fail-closed behavior. It prevents the absence of research evidence from being
misrepresented as model readiness or deployment.

### R7 — Pull-request inventory contains no safe “merge everything” set

The open set was almost entirely Dependabot PRs plus an old 73-commits-behind
draft arXiv PR. The draft is superseded by later released thesis evidence and
is too divergent to merge safely. Dependency PRs should close automatically or
be closed as superseded once this consolidated lock update lands.

## Verification snapshot

| Gate | Result |
|---|---|
| Ruff | Pass |
| mypy | 283 files, pass |
| pytest | 1,079 passed |
| Dagster | definitions load, pass |
| Compose | config validation, pass |
| Dashboard ESLint | 0 findings |
| Dashboard typecheck | Pass |
| Dashboard Vitest | 261 passed |
| Dashboard static generation | 13 routes, pass |
| Dashboard npm audit | 0 vulnerabilities |
| Intro-video typecheck/audit | Pass / 0 vulnerabilities |

The Python suite emits upstream Starlette/httpx and cvxpylayers/NumPy warnings.
They are not failures, but should be tracked before the next dependency epoch.
