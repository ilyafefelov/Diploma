# docs/

Ця папка містить всю документацію проєкту: технічну, академічну та навчальну.

---

## 📁 Структура

```
docs/
├── technical/          # Технічна документація системи
│   ├── API_ENDPOINTS.md
│   ├── BASELINE_LP_AND_DATA_PIPELINE.md
│   ├── RESEARCH_INTEGRATION_PLAN.md
│   ├── OPERATOR_DEMO_READY.md
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
| **Технічна інтеграція** | [technical/API_ENDPOINTS.md](technical/API_ENDPOINTS.md) · [technical/BASELINE_LP_AND_DATA_PIPELINE.md](technical/BASELINE_LP_AND_DATA_PIPELINE.md) · [technical/OPERATOR_DEMO_READY.md](technical/OPERATOR_DEMO_READY.md) |
| **Research roadmap** | [technical/RESEARCH_INTEGRATION_PLAN.md](technical/RESEARCH_INTEGRATION_PLAN.md) · [technical/deep-research-reports/deep-research-report.md](technical/deep-research-reports/deep-research-report.md) |
| **Інженер / розробник** | [../src/](../src/) · [../api/](../api/) · [../dashboard/](../dashboard/) |
| **Дедлайни та вимоги** | [syllabus/Покрокова інструкція...](syllabus/) |

---

## 📌 Поточний стан (May 2026)

- ✅ Real-data 90-anchor DAM benchmark — OREE observed prices + tenant Open-Meteo weather.
- ✅ Gold research layer — forecast diagnostics, value-aware ensemble, calibrated horizon-aware ensemble gate, risk-adjusted selector diagnostics, DFL training table, scalar and horizon-aware regret-weighted TFT/NBEATSx calibration.
- ✅ Strict LP/oracle re-evaluation — calibrated forecasts checked against same Level 1 simulator.
- ✅ Baseline LP documentation — current formula, data pipeline, ML boundaries, SOC handling, and literature support are captured in [BASELINE_LP_AND_DATA_PIPELINE.md](technical/BASELINE_LP_AND_DATA_PIPELINE.md).
- ✅ Operator weather signal boundary — dashboard `weather_bias` is documented as an explanatory weather-uplift read model, not the LP control input; see [BASELINE_LP_AND_DATA_PIPELINE.md#operator-weather-signal](technical/BASELINE_LP_AND_DATA_PIPELINE.md#operator-weather-signal).
- ✅ LP/data-pipeline bibliography archive — PDFs and 2-3 sentence source notes are indexed in [thesis/sources/README.md](thesis/sources/README.md#-source-map-baseline-lp-and-current-data-pipeline).
- ✅ Medallion cleanup — Dagster assets now carry explicit `medallion=bronze|silver|gold` tags, and the real-data benchmark has a Silver price/weather feature bridge before Gold evaluation.
- ✅ DFL/DT foundation slice — added SOTA-ready forecast training schema, differentiable relaxed-LP pilot rows, offline Decision Transformer trajectory rows, deterministic action projection, and simulated paper-trading replay rows.
- ✅ Backend read models — `/dashboard/calibrated-ensemble-benchmark`, `/dashboard/risk-adjusted-value-gate`, and `/dashboard/forecast-dispatch-sensitivity` expose selector and diagnostic evidence for dashboard redesign without touching dashboard UI.
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
