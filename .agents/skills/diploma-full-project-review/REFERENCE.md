# Diploma Full Project Review Reference

## Canonical report packet

Create a dated folder:

```text
docs/technical/deep-research-reports/<YYYY-MM-DD>-full-project-review/
```

Recommended files:

- `README.md` - packet index and verdict
- `review.md` - severity-ordered critical review
- `adr.md` - architectural decision record
- `prd.md` - academic MVP/product requirements
- `data-flow.md` - Mermaid data-flow and safety diagrams
- `experiments-atlas.md` - setup, pipeline, results, deliverables
- `dashboard-review.md` - visual/API review with screenshots
- `external-benchmark.md` - market, competitor, academic comparison
- `plain-language-review.md` - supervisor-friendly summary
- `fix-plan.md` - prioritized remediation plan
- `source-matrix.md` - commands, sources, file evidence
- `infographics.md` - reusable deterministic visual blocks
- `review-index.json` - machine-readable packet summary
- `assets/` - screenshots and generated/captured visuals

## Commands

Prefer the repo wrapper from an activated root virtualenv when doing full verification:

```powershell
.\.venv\Scripts\Activate.ps1
.\scripts\verify.ps1
```

If the wrapper is unavailable, use the closest repo-specific checks:

```powershell
uv run ruff check .
uv run mypy .
uv run pytest -p no:cacheprovider tests
uv run dg check defs
uv run dg list defs --json
docker compose config --quiet
```

Dashboard checks:

```powershell
cd dashboard
npm run typecheck
npm exec -- vitest run
```

V13 acquisition preflight:

```powershell
.\.venv\Scripts\python.exe scripts\preflight_ua_context_v13_acquisition_inputs.py --config configs\real_data_dfl_ua_context_v13_acquisition_week3.yaml --output .tmp_runtime\v13_acquisition_inputs_preflight.json
```

## Evidence paths to check

- `configs/market_rules_ua.yaml`
- `src/smart_arbitrage/market_rules.py`
- `src/smart_arbitrage/gatekeeper/schemas.py`
- `src/gatekeeper/schemas.py` for stale legacy duplicates
- `data/research_runs/`
- `docs/thesis/chapters/`
- `docs/thesis/appendices/`
- `docs/syllabus/`
- `dashboard/app/pages/operator.vue`
- `dashboard/app/pages/defense.vue`
- `dashboard/app/assets/css/operator-hud.css`
- `api/main.py`

## Experiment classification

Use these labels consistently:

- **Headline evidence**: promoted offline/read-model result with strict comparator and rolling robustness.
- **Negative evidence**: valid experiment that did not beat the frozen comparator.
- **Shadow evidence**: visible/readable diagnostics that do not change production/default behavior.
- **Smoke evidence**: CI/prototype contract validation, not scientific promotion.
- **Blocked gate**: source, governance, credential, or safety precondition not met.

## External comparison sources

Refresh live before finalizing:

- Ukrainian market rules: NEURC, OREE, Zakon Rada, Interfax or other primary/near-primary sources.
- Industry competitors: Tesla Autobidder, Fluence Mosaic, Wartsila GEMS, Habitat Energy, Arenko, Modo Energy when relevant.
- Academic work: decision-focused ESS arbitrage, predict-then-bid, NBEATSx EPF, TFT EPF, degradation-aware BESS optimization.

## Review checklist

- Findings are severity ordered.
- Every major claim has a repo artifact, command output, thesis paragraph, or external source.
- Stale docs are identified, not silently trusted.
- Current verification status is stated exactly, including failures.
- `market_execution_enabled=false` is preserved.
- V13, DT/LAVA, Poland, TFT, and DFL boundaries are not overstated.
- Dashboard review includes real screenshot or browser evidence.
- The fix plan distinguishes immediate, short-term, medium-term, and research-roadmap work.

