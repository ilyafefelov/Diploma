# docs/

Ця папка містить всю документацію проєкту: технічну, академічну та навчальну.

---

## 📁 Структура

```
docs/
├── technical/          # Технічна документація системи
│   ├── ARCHITECTURE_AND_DATA_FLOW.md
│   ├── API_ENDPOINTS.md
│   ├── BASELINE_FREEZE.md
│   ├── DFL_ACTION_CLASSIFIER_BASELINE.md
│   ├── DFL_CLASSIFIER_FAILURE_ANALYSIS.md
│   ├── DFL_DATA_RECOVERY_ROADMAP.md
│   ├── DFL_AFL_FORECAST_ERROR_AUDIT.md
│   ├── DFL_FORECAST_DECISION_LOSS_V1.md
│   ├── DFL_RESIDUAL_DT_RESEARCH_CHALLENGER.md
│   ├── DFL_SOURCE_SPECIFIC_RESEARCH_CHALLENGER.md
│   ├── DFL_PRODUCTION_PROMOTION_GATE.md
│   ├── DFL_REGIME_GATED_TFT_SELECTOR_V2.md
│   ├── DFL_FORECAST_AFL_HARDENING.md
│   ├── DFL_PROMOTION_GATE.md
│   ├── DFL_VECTOR_EVIDENCE_REGISTRY.md
│   ├── BASELINE_LP_AND_DATA_PIPELINE.md
│   ├── DFL_READINESS_GATE.md
│   ├── MANIFESTED_CALIBRATION_EVIDENCE_REGISTRY.md
│   ├── OFFLINE_DFL_EXPERIMENT.md
│   ├── OFFLINE_DFL_PANEL_EXPERIMENT.md
│   ├── RESEARCH_INTEGRATION_PLAN.md
│   ├── WEEK3_ARCHITECTURE_API_FOLLOWUP_REPORT.md
│   ├── OPERATOR_DEMO_READY.md
│   ├── assets/         # Діаграми та технічні зображення
│   └── papers/         # Технічні статті та архів ключових PDF
│
├── thesis/             # Матеріали дипломної роботи
│   ├── chapters/       # Розділи пояснювальної записки
│   │   ├── 01-project-overview.md
│   │   └── 02-literature-review.md
│   ├── sources/        # Локальний PDF-архів джерел
│   │   └── README.md   ← індекс статей для керівника
│   └── weekly-reports/ # Щотижневі звіти
│       ├── week1/      # Week 1 — submission-ready пакет
│       └── week2/
│
└── syllabus/           # Канонічні вимоги та дедлайни дипломного процесу
    ├── introduction/
    └── weekly reports/
```

---

## 🎯 Точки входу за роллю

| Роль | Куди дивитися |
|------|--------------|
| **Науковий керівник** | [thesis/weekly-reports/week1/supervisor-summary.md](thesis/weekly-reports/week1/supervisor-summary.md) · [thesis/weekly-reports/week1/report.md](thesis/weekly-reports/week1/report.md) · [Vercel live demo](https://dashboard-gilt-one-97.vercel.app/week1/interactive_report1) |
| **Академічна записка** | [thesis/chapters/01-project-overview.md](thesis/chapters/01-project-overview.md) · [thesis/chapters/02-literature-review.md](thesis/chapters/02-literature-review.md) |
| **Бібліографія / джерела** | [thesis/sources/README.md](thesis/sources/README.md) — індекс усіх PDF |
| **Архітектура і data flow** | [technical/ARCHITECTURE_AND_DATA_FLOW.md](technical/ARCHITECTURE_AND_DATA_FLOW.md) — інфографіка архітектури, потоків даних і safety boundary |
| **Технічна інтеграція** | [technical/API_ENDPOINTS.md](technical/API_ENDPOINTS.md) · [technical/API_READ_MODEL_FRESHNESS_AND_PERFORMANCE.md](technical/API_READ_MODEL_FRESHNESS_AND_PERFORMANCE.md) · [technical/MANIFESTED_CALIBRATION_EVIDENCE_REGISTRY.md](technical/MANIFESTED_CALIBRATION_EVIDENCE_REGISTRY.md) · [technical/DFL_VECTOR_EVIDENCE_REGISTRY.md](technical/DFL_VECTOR_EVIDENCE_REGISTRY.md) · [technical/DFL_READINESS_GATE.md](technical/DFL_READINESS_GATE.md) · [technical/DFL_PROMOTION_GATE.md](technical/DFL_PROMOTION_GATE.md) · [technical/DFL_PRODUCTION_PROMOTION_GATE.md](technical/DFL_PRODUCTION_PROMOTION_GATE.md) · [technical/DFL_REGIME_GATED_TFT_SELECTOR_V2.md](technical/DFL_REGIME_GATED_TFT_SELECTOR_V2.md) · [technical/DFL_ACTION_CLASSIFIER_BASELINE.md](technical/DFL_ACTION_CLASSIFIER_BASELINE.md) · [technical/DFL_CLASSIFIER_FAILURE_ANALYSIS.md](technical/DFL_CLASSIFIER_FAILURE_ANALYSIS.md) · [technical/DFL_DATA_RECOVERY_ROADMAP.md](technical/DFL_DATA_RECOVERY_ROADMAP.md) · [technical/DFL_AFL_FORECAST_ERROR_AUDIT.md](technical/DFL_AFL_FORECAST_ERROR_AUDIT.md) · [technical/DFL_FORECAST_DECISION_LOSS_V1.md](technical/DFL_FORECAST_DECISION_LOSS_V1.md) · [technical/DFL_RESIDUAL_DT_RESEARCH_CHALLENGER.md](technical/DFL_RESIDUAL_DT_RESEARCH_CHALLENGER.md) · [technical/DFL_SOURCE_SPECIFIC_RESEARCH_CHALLENGER.md](technical/DFL_SOURCE_SPECIFIC_RESEARCH_CHALLENGER.md) · [technical/DFL_FORECAST_AFL_HARDENING.md](technical/DFL_FORECAST_AFL_HARDENING.md) · [technical/AFE_TO_AFL_TO_DFL_ROADMAP.md](technical/AFE_TO_AFL_TO_DFL_ROADMAP.md) · [technical/AFE_SEMANTIC_EVENT_CONTEXT.md](technical/AFE_SEMANTIC_EVENT_CONTEXT.md) · [technical/OFFLINE_DFL_EXPERIMENT.md](technical/OFFLINE_DFL_EXPERIMENT.md) · [technical/OFFLINE_DFL_PANEL_EXPERIMENT.md](technical/OFFLINE_DFL_PANEL_EXPERIMENT.md) · [technical/WEEK3_ARCHITECTURE_API_FOLLOWUP_REPORT.md](technical/WEEK3_ARCHITECTURE_API_FOLLOWUP_REPORT.md) · [technical/BASELINE_FREEZE.md](technical/BASELINE_FREEZE.md) · [technical/BASELINE_LP_AND_DATA_PIPELINE.md](technical/BASELINE_LP_AND_DATA_PIPELINE.md) · [technical/OPERATOR_DEMO_READY.md](technical/OPERATOR_DEMO_READY.md) |
| **Research roadmap** | [technical/RESEARCH_INTEGRATION_PLAN.md](technical/RESEARCH_INTEGRATION_PLAN.md) · [technical/deep-research-reports/deep-research-report.md](technical/deep-research-reports/deep-research-report.md) · [technical/deep-research-reports/week3 research/README.md](<technical/deep-research-reports/week3 research/README.md>) |
| **Next slice plan** | [technical/NEXT_SLICE_PLAN_RESEARCH_GROUNDED_CALIBRATION_QA.md](technical/NEXT_SLICE_PLAN_RESEARCH_GROUNDED_CALIBRATION_QA.md) |
| **Інженер / розробник** | [../src/](../src/) · [../api/](../api/) · [../dashboard/](../dashboard/) |
| **Дедлайни та вимоги** | [syllabus/Покрокова інструкція...](syllabus/) |

---

## 📌 Поточний стан (May 2026)

- ✅ Week 3 accepted real-data benchmark — Dnipro factory, OREE observed prices + tenant Open-Meteo weather, 30 rolling-origin anchors, thesis-grade provenance.
- ✅ Prepared-ahead 90-anchor calibration preview — Dnipro selector/calibration evidence for the next demo path, not the Week 3 headline.
- ✅ Gold research layer — forecast diagnostics, value-aware ensemble, calibrated horizon-aware ensemble gate, risk-adjusted selector diagnostics, DFL training table, scalar and horizon-aware regret-weighted TFT/NBEATSx calibration.
- ✅ Strict LP/oracle re-evaluation — calibrated forecasts checked against same Level 1 simulator.
- ✅ Baseline LP documentation — current formula, data pipeline, ML boundaries, SOC handling, and literature support are captured in [BASELINE_LP_AND_DATA_PIPELINE.md](technical/BASELINE_LP_AND_DATA_PIPELINE.md).
- ✅ Operator weather signal boundary — dashboard `weather_bias` is documented as an explanatory weather-uplift read model, not the LP control input; see [BASELINE_LP_AND_DATA_PIPELINE.md#operator-weather-signal](technical/BASELINE_LP_AND_DATA_PIPELINE.md#operator-weather-signal).
- ✅ LP/data-pipeline bibliography archive — PDFs and 2-3 sentence source notes are indexed in [thesis/sources/README.md](thesis/sources/README.md#-source-map-baseline-lp-and-current-data-pipeline).
- ✅ Medallion cleanup — Dagster assets now carry explicit `medallion=bronze|silver|gold` tags, and the real-data benchmark has a Silver price/weather feature bridge before Gold evaluation.
- ✅ DFL/DT foundation slice — added SOTA-ready forecast training schema, differentiable relaxed-LP pilot rows, Silver NBEATSx/TFT forecast context for DT state, offline Decision Transformer trajectory rows, deterministic action projection, and simulated paper-trading replay rows.
- ✅ Architecture/data-flow infographic — visual overview of Data Sources → Dagster Medallion assets → AI/optimization agents → Pydantic-backed Bid Gatekeeper → FastAPI/Postgres/MLflow/Nuxt observability is indexed in [ARCHITECTURE_AND_DATA_FLOW.md](technical/ARCHITECTURE_AND_DATA_FLOW.md), with an evidence registry that separates Week 3 accepted results from preview material.
- ✅ Backend read models — `/dashboard/calibrated-ensemble-benchmark`, `/dashboard/risk-adjusted-value-gate`, and `/dashboard/forecast-dispatch-sensitivity` expose selector and diagnostic evidence for dashboard redesign without touching dashboard UI.
- ✅ Postgres read-model freshness/performance — `forecast_strategy_evaluations` now has a latest-batch read index, documented in [API_READ_MODEL_FRESHNESS_AND_PERFORMANCE.md](technical/API_READ_MODEL_FRESHNESS_AND_PERFORMANCE.md).
- ✅ Calibration QA manifest — research-layer exports now write `research_layer_manifest.json` with tenant/strategy freshness, row/anchor counts, source links, and conservative claim flags.
- ✅ Manifested calibration evidence registry — latest Dnipro 90-anchor calibration preview is indexed in [MANIFESTED_CALIBRATION_EVIDENCE_REGISTRY.md](technical/MANIFESTED_CALIBRATION_EVIDENCE_REGISTRY.md), with manifest/API/Postgres agreement and generated `data/` artifacts kept local.
- ✅ Dagster evidence checks and DFL readiness gate — thesis-grade, no-leakage, selector-cardinality, and claim-boundary checks are registered in Dagster and documented in [DFL_READINESS_GATE.md](technical/DFL_READINESS_GATE.md).
- ✅ Offline DFL experiment started — `offline_dfl_experiment_frame` materializes a bounded relaxed-LP training experiment on the gated Dnipro 90-anchor evidence; the first held-out result is negative and documented as research-only in [OFFLINE_DFL_EXPERIMENT.md](technical/OFFLINE_DFL_EXPERIMENT.md).
- ✅ All-tenant offline DFL panel — `offline_dfl_panel_experiment_frame` materializes five tenants × 90 anchors with a 90 tenant-anchor final holdout per model; v2 checkpointed relaxed-LP evidence is documented in [OFFLINE_DFL_PANEL_EXPERIMENT.md](technical/OFFLINE_DFL_PANEL_EXPERIMENT.md) and remains not full DFL / not market execution.
- ✅ Week 3 deep-research source map — the new report intake is indexed in [technical/deep-research-reports/week3 research/README.md](<technical/deep-research-reports/week3 research/README.md>) and mapped to implementation claims in [source-map.md](<technical/deep-research-reports/week3 research/source-map.md>).
- ✅ Baseline freeze — `strict_similar_day` is frozen as the Level 1 control comparator in [BASELINE_FREEZE.md](technical/BASELINE_FREEZE.md).
- ✅ DFL promotion gate — conservative candidate promotion rules are tracked in [DFL_PROMOTION_GATE.md](technical/DFL_PROMOTION_GATE.md); current offline DFL v0 remains diagnostic only.
- ✅ DFL vector evidence registry — `dfl_training_example_frame` now has a persisted 90-anchor Dnipro evidence run in [DFL_VECTOR_EVIDENCE_REGISTRY.md](technical/DFL_VECTOR_EVIDENCE_REGISTRY.md); all current candidates remain blocked against `strict_similar_day`.
- ✅ DFL action classifier baseline — `dfl_action_classifier_baseline_frame` materialized a transparent supervised action-label probe on the 1,040-row all-tenant panel; final-holdout accuracy is documented in [DFL_ACTION_CLASSIFIER_BASELINE.md](technical/DFL_ACTION_CLASSIFIER_BASELINE.md), and promotion remains blocked until strict LP/oracle value scoring.
- ✅ DFL classifier failure diagnostics — `dfl_action_classifier_failure_analysis_frame` explains why the plain and value-aware classifiers are blocked by strict LP/oracle value scoring; the recovery path is documented in [DFL_CLASSIFIER_FAILURE_ANALYSIS.md](technical/DFL_CLASSIFIER_FAILURE_ANALYSIS.md) and [DFL_DATA_RECOVERY_ROADMAP.md](technical/DFL_DATA_RECOVERY_ROADMAP.md).
- ✅ Forecast AFL hardening — rolling-origin neural forecasts now use forecast-available weather mode, current compact NBEATSx/TFT candidates are labeled in `forecast_candidate_forensics_frame`, and `afl_training_panel_frame` provides prior-only forecast features plus decision-value labels; see [DFL_FORECAST_AFL_HARDENING.md](technical/DFL_FORECAST_AFL_HARDENING.md).
- ✅ AFL forecast error audit — `afl_forecast_error_audit_frame` classifies compact NBEATSx/TFT failures by spread-shape, rank/extrema, LP-value, and prior-only weather/load context before official training or DFL loss work; see [DFL_AFL_FORECAST_ERROR_AUDIT.md](technical/DFL_AFL_FORECAST_ERROR_AUDIT.md).
- ✅ DFL forecast decision-loss v1 — `dfl_forecast_dfl_v1_panel_frame` and `dfl_forecast_dfl_v1_strict_lp_benchmark_frame` run a tiny prior-only correction through relaxed-loss training and strict LP/oracle scoring; current evidence is blocked by relaxed-solver fallback and documented in [DFL_FORECAST_DECISION_LOSS_V1.md](technical/DFL_FORECAST_DECISION_LOSS_V1.md).
- ✅ Residual DFL + offline DT research challenger — `dfl_real_data_trajectory_dataset_frame`, residual schedule/value assets, tiny offline DT assets, and the strict-default fallback wrapper are documented in [DFL_RESIDUAL_DT_RESEARCH_CHALLENGER.md](technical/DFL_RESIDUAL_DT_RESEARCH_CHALLENGER.md); the path remains research-only until strict LP/oracle promotion passes.
- ✅ Source-specific challenger gate — `dfl_source_specific_research_challenger_frame` separates TFT and NBEATSx evidence; TFT has a latest-holdout signal but is not robust across rolling strict-control windows, so production promotion remains blocked. See [DFL_SOURCE_SPECIFIC_RESEARCH_CHALLENGER.md](technical/DFL_SOURCE_SPECIFIC_RESEARCH_CHALLENGER.md).
- ✅ Production promotion gate — `dfl_production_promotion_gate_frame` can emit `production_promote=true` for a future robust source/regime, but the latest run blocks every row because the all-tenant panel has a 104-anchor ceiling and `coverage_gap` provenance against the 180-anchor promotion target. See [DFL_PRODUCTION_PROMOTION_GATE.md](technical/DFL_PRODUCTION_PROMOTION_GATE.md).
- ✅ Regime-gated TFT selector v2 — `dfl_regime_gated_tft_selector_v2_frame` verifies the stricter prior-only TFT switch rule, records the exact `2026-03-29 23:00` OREE/weather gap, and keeps production promotion blocked. See [DFL_REGIME_GATED_TFT_SELECTOR_V2.md](technical/DFL_REGIME_GATED_TFT_SELECTOR_V2.md).
- ✅ AFE semantic event context — `forecast_afe_feature_catalog_frame` catalogs leak-safe Ukrainian forecast features and blocks EU bridge rows from training, while `dfl_semantic_event_strict_failure_audit_frame` audits official Ukrenergo grid-event context against strict-control failure windows; see [AFE_TO_AFL_TO_DFL_ROADMAP.md](technical/AFE_TO_AFL_TO_DFL_ROADMAP.md) and [AFE_SEMANTIC_EVENT_CONTEXT.md](technical/AFE_SEMANTIC_EVENT_CONTEXT.md).
- ✅ MLflow/Postgres/Dagster persistence — latest run documented in [real-data-90-anchor-benchmark-report.md](technical/deep-research-reports/real-data-90-anchor-benchmark-report.md).
- ✅ Локальний PDF-архів — includes DFL, NBEATSx, TFT, storage DFL, TimeXer references.
- 🟡 Full differentiable DFL training — primitive exists, but forecast models are not yet fine-tuned end-to-end on regret.
- 🟡 Decision Transformer / M3DT-inspired strategy — policy scaffolding and trajectories exist; training/evaluation remains future work.

---

## 📚 LP / Data-Pipeline Source Index

The LP baseline is supported by Park et al. (2017), which shows that short-term ESS scheduling can be expressed as LP while retaining SOC, efficiency, and power-limit constraints. This is the direct academic basis for using a transparent deterministic LP as the Level 1 control group.

The degradation proxy is supported and bounded by Kumtepeli et al. (2024): the paper explains why rolling-horizon storage optimization often includes an aging/depreciation cost, while warning that this remains a proxy. This is why the project uses throughput/EFC degradation as an MVP economic penalty and does not claim a full electrochemical digital twin.

The ML forecast lane is supported by NBEATSx (Olivares et al., 2023) and TFT (Lim et al., 2021). They justify the research forecast candidates, while the current architecture still routes their predictions through the same strict LP instead of treating them as dispatch policies.

The operator weather signal is supported as an explainable read model, not as a dispatch input. Lago et al. (2021) justify strict benchmarking against simple baselines before promoting new EPF signals; NBEATSx, TFT, and TimeXer support weather and other exogenous variables upstream in forecast models. This is why the dashboard can show `weather_bias`, while the baseline LP remains price-forecast-driven until weather-aware forecasts pass rolling-origin realized-value/oracle-regret tests.

The future DFL lane is supported by OptNet (Amos & Kolter, 2017), cvxpylayers (Agrawal et al., 2019), the DFL survey (Mandi et al., 2024), and the energy-storage predict-then-bid paper (Yi et al., 2025). These sources justify the relaxed differentiable LP pilot and target architecture, but also reinforce the current claim boundary: full end-to-end DFL is planned, not yet the production controller.
