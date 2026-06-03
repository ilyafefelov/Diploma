# Додаток. Розширений словник і технічні ідентифікатори

Цей додаток містить довші терміни, винесені з основної частини для безперервного читання. В основному документі короткий перелік скорочень лишається у front matter, а детальні технічні назви, endpoint names, asset keys, evidence flags і shadow-run identifiers розміщено після основних розділів.

## Evidence and gates

- `market_execution_enabled=false` - глобальна межа, яка забороняє трактувати preview як live trading.
- `dt_lava_ready=false` - V13 source-readiness не дозволяє DT/LAVA promotion.
- `permits_model_training=false` - source readiness недостатня для permitted model-training rows.
- `strict_similar_day` - frozen leakage-free baseline.
- `Schedule/Value Learner V2+` - headline schedule/value selector у main text.
- `hf_live_safe_switch_value_aligned_shadow` - manual shadow preview source, який ранжує LP-free schedules через HF scorer і deterministic gates.
- `guard_abstained_to_safe_fallback` - diagnostic flag, що означає повернення до HOLD/V2+ fallback через threshold, tail-risk або safety guard.
- `source_backed_price_context_available` - numeric readiness flag для перевірки, що DAM/IDM preview не використовує synthetic prices.
- `same_day_forecast_refresh` - source mode для same-day forecast rows, які не треба маскувати як pre-publication evidence.
- `request_fallback_materialized` - source mode, коли forecast rows матеріалізовано під час preview request і явно позначено в response.

## Long identifiers

- `data/research_runs/week3_official_global_panel_schedule_value_v2_plus_comparison/`
- `data/research_runs/week3_tft_quantile_365_full_negative_evidence/`
- `data/research_runs/week3_dt_direct_candidate_shadow_current/`
- `data/research_runs/hf_live_safe_switch_value_aligned_shadow_promotion_proof_2026_05_01_2026_06_01/`
- `data/research_runs/hf_value_aligned_forecast_readiness_2026-06-02/`
- `data/research_runs/hf_live_safe_switch_shadow_demo_evidence_2026_06_01/`
- `docs/thesis/chapters/assets/hf-value-aligned-shadow-flow.png`
- `docs/technical/DT_DIRECT_CANDIDATE_SHADOW.md`
- `docs/technical/DFL_CANDIDATE_VALUE_DFL_V3.md`
- `docs/technical/DFL_PLATEAU_BREAKER_V4.md`
- `docs/technical/DFL_POINT_IN_TIME_CONTEXT_REPAIR.md`

## Terms moved from front matter

AFE, AFL, ACER, ATB, CSV, CUDA, CVXPY, `cvxpylayers`, DOI, DST, EFC, ENTSO-E, FX, HF, IDM, JSON, LFP, LSTM, MCO, MILP, MLflow, MQTT, NBU, NPZ, NREL, OREE, OPSD, P2D, Pydantic, REST, RDS, SCMO, SDAC, SEI, SIDC, SOTA, SPO/SPO+, THieF, VAT, VSN, XAI.
