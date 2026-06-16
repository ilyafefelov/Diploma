# Final Repository Review Checklist

Date: 2026-06-16

Use this checklist before the final GitHub repository submission and before the
live defense demo.

## 1. GitHub Hygiene

- Confirm `git status --short` contains only intentional final-submission
  changes.
- Keep unrelated thesis edits separate unless they are part of the final
  repository package.
- Confirm generated/runtime artifacts are not tracked:
  `outputs/`, `output/`, `analysis_outputs/`, `reports/`, `node_modules/`.
- Do not re-track local experiment packets unless they have been converted into
  small curated summaries under `docs/technical/final-evidence/`.
- Keep third-party paper PDFs out of Git; preserve source metadata in
  `docs/thesis/sources/README.md` and source-capture notes.
- Do not run line-ending normalization across the repository during final
  packaging.

## 2. README Review

- First screen states the product boundary: DAM/IDM hourly operator
  recommendation preview, not live trading.
- Quickstart works from a clean checkout with `uv sync --extra dev` and
  `npm -C dashboard install`.
- Demo path names `/operator`, `/defense`, and `/docs`.
- Evidence visuals render from tracked paths.
- API section links to `docs/technical/API_ENDPOINTS.md`.
- Claim-boundary table says no `ProposedBid`, no market payload, no production
  dispatch, and `market_execution_enabled=false`.

## 3. Dashboard Demo

- Open `/operator` and verify the first viewport shows:
  - `Operator Preview`;
  - the boundary strip: `Preview only`, `No ProposedBid`,
    `No market payload`, `Human review required`;
  - the `/defense` quick path.
- Select `client_003_dnipro_factory`.
- Show DAM latest official, then IDM as hourly read-model preview.
- Show target-date modes: latest official, today, tomorrow, day+2.
- Select `HF live safe-switch value-aligned shadow`.
- Show one non-HOLD case and one guarded HOLD/fallback case when available.
- End at `/defense` and explain V2+ headline evidence, DT/V2+ secondary
  evidence, and HF manual shadow/demo evidence.

## 4. API/Docs Consistency

- `GET /health` returns OK.
- `GET /dashboard/operator-recommendation` returns preview rows or a
  source-readiness blocker, not synthetic hidden prices.
- `GET /dashboard/shadow-recommendation-preview` rejects promotable/execution
  artifacts.
- FastAPI docs expose the same route families summarized in README.
- `docs/technical/API_ENDPOINTS.md` uses preview/read-model language.

## 5. Evidence Traceability

- Every headline number in README is present in
  `docs/technical/FINAL_METRICS_ATLAS.md`.
- The final evidence index links only to tracked files.
- Local-only data paths are described as source packets, not GitHub links.
- V13 is described as acquisition/source-readiness, not modeling success.
- DT/LAVA and HF are described as shadow/manual research evidence, not deployed
  controllers.

## 6. Final Verification

Run the lightweight final audit first:

```powershell
.\scripts\final_repo_audit.ps1 -SkipFullVerify -SkipSmoke
```

Then run the full verification when runtime permits:

```powershell
.\.venv\Scripts\Activate.ps1
.\scripts\verify.ps1
npm -C dashboard run typecheck
npm -C dashboard run test:unit
```

Optional browser smoke after the local stack is running:

```powershell
npm -C dashboard run smoke:hf-value-aligned
```

## 7. Final Commit Boundary

- Stage only intentional final-submission files.
- Keep generated-artifact deletions staged if the final package is removing them
  from Git.
- Do not include local `.env`, runtime logs, caches, or unreviewed thesis
  scratch outputs.
- Do not include tracked third-party source PDFs under `docs/thesis/sources/`
  or `docs/technical/papers/`.
- Re-run `git diff --check` immediately before commit.

