# Source Matrix

## Local Repo Evidence

| Area | Evidence | Path or command |
|---|---|---|
| V2+ headline packet | Mean regret, strict comparator, V2 comparator, rolling robustness | `data/research_runs/week3_official_global_panel_schedule_value_v2_plus_comparison/dfl_schedule_value_learner_v2_plus_comparison.md` |
| V13 blocked state | Missing DAM receipts and safe-switch rows, candidate generation not ready | `data/research_runs/week3_dfl_ua_context_acquisition_v13/dfl_ua_context_v13_acquisition_summary.json` |
| DT shadow | HF DT available, 97,431 research rows, 7,300 sequences, not promotable | `data/research_runs/week3_dt_research_shadow_current/dt_research_shadow_smoke_summary.json` |
| Direct DT shadow | HF DT trained directly on candidate-index/schedule-family teacher targets; ties the V13 fallback row, worse than strict/oracle, not promoted | `data/research_runs/week3_dt_direct_candidate_shadow_current/dt_research_shadow_smoke_summary.json`; `docs/technical/DT_DIRECT_CANDIDATE_SHADOW.md` |
| DT vs real V2+ shadow | Apples-to-apples DT check against the headline V2+ strict-row packet; DT loses to V2+ and strict, not promoted | `data/research_runs/week3_dt_v2_plus_apples_to_apples_current/dt_v2_plus_apples_to_apples_summary.json`; `docs/technical/DT_V2_PLUS_APPLES_TO_APPLES_SHADOW.md` |
| LAVA smoke | 8-instance NPZ smoke, promotion false, V13 blocked | `data/research_runs/week3_dt_lava_lava_npz_smoke_current/candidate_lava_margin_metrics.json` |
| Poland feature audit | 17 passing features, 7 null-blocked, positive not promoted | `data/research_runs/week3_poland_lag24_feature_audit_rolling_gate/poland_lag24_feature_audit_rolling_gate_summary.json` |
| Market rules | NEURC 621 regime and caps | `configs/market_rules_ua.yaml` |
| Canonical contracts | Active market caps and Pydantic contracts | `src/smart_arbitrage/gatekeeper/schemas.py` |
| Legacy duplicate risk | Excluded legacy path is explicitly marked obsolete; active contracts live under `smart_arbitrage.gatekeeper.schemas` | `src/gatekeeper/schemas.py` |
| Operator UI | Normal-flow schedule dock with post-fix desktop/mobile no-occlusion evidence | `dashboard/app/assets/css/operator-hud.css`; `assets/operator-dock-*.json` |
| API typing failures | Original review found shadow schedule helper Mypy failures; current closure Mypy passes and no longer shows `api/main.py` errors | `api/main.py` |
| Windows startup typing failures | Original platform lambda Mypy failures are fixed | `sitecustomize.py` |

## Commands Run

| Command | Result |
|---|---|
| `uv run ruff check .` | Passed |
| `uv run mypy .` | Passed, 247 source files |
| `uv run pytest -p no:cacheprovider tests` | Passed, 945 tests in 616.85s |
| `uv run dg check defs` | Passed |
| `uv run dg list defs --json` | Passed, 335 assets and 63 checks |
| `docker compose config --quiet` | Passed |
| `npm run typecheck` in `dashboard` | Passed |
| `npm exec -- vitest run` in `dashboard` | Passed, 66 tests |

## Online Thesis Source

- Google Docs thesis draft: `Draft.Thesis.2.goit.energy_arbitrage.Fefelov`
- URL: <https://docs.google.com/document/d/1jjja9ng99O-xCisijMUbPrEM-3UJi_hilwnFJY8nups/edit?tab=t.0#heading=h.2wogexmrudi8>

Reviewed thesis points:

- API is FastAPI read-model, not market execution.
- DFL is downstream decision-quality/regret/value research, not forecast-only.
- DT is an offline RL sequence-model roadmap, not a deployed controller.
- EU/Poland rows are governance-only or lagged exogenous context, not Ukrainian training targets.
- Credentialless MVP/DT shadow language preserves market-execution boundaries.

## External Sources

Market rules:

- <https://en.interfax.com.ua/news/economic/1161619.html>
- <https://www.oree.com.ua/index.php/web/7004>
- <https://zakon.rada.gov.ua/go/v0621874-26>

Industry:

- <https://www.tesla.com/en_sa/support/energy/tesla-software/autobidder>
- <https://fluenceenergy.com/mosaic-intelligent-bidding-software/>
- <https://storage.wartsila.com/technology/gems/>

Academic:

- <https://arxiv.org/abs/2505.01551>
- <https://arxiv.org/abs/2305.00362>
- <https://arxiv.org/abs/2104.05522>
- <https://ideas.repec.org/a/wly/jforec/v43y2024i5p1465-1491.html>
