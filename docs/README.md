# docs/

Ця папка містить всю документацію проєкту: технічну, академічну та навчальну.

---

## 📁 Структура

```
docs/
├── technical/          # Технічна документація системи
│   ├── API_ENDPOINTS.md
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
| **Технічна інтеграція** | [technical/API_ENDPOINTS.md](technical/API_ENDPOINTS.md) · [technical/OPERATOR_DEMO_READY.md](technical/OPERATOR_DEMO_READY.md) |
| **Research roadmap** | [technical/RESEARCH_INTEGRATION_PLAN.md](technical/RESEARCH_INTEGRATION_PLAN.md) · [technical/deep-research-reports/deep-research-report.md](technical/deep-research-reports/deep-research-report.md) |
| **Інженер / розробник** | [../src/](../src/) · [../api/](../api/) · [../dashboard/](../dashboard/) |
| **Дедлайни та вимоги** | [syllabus/Покрокова інструкція...](syllabus/) |

---

## 📌 Поточний стан (May 2026)

- ✅ Real-data 90-anchor DAM benchmark — OREE observed prices + tenant Open-Meteo weather.
- ✅ Gold research layer — forecast diagnostics, value-aware ensemble, calibrated horizon-aware ensemble gate, risk-adjusted selector diagnostics, DFL training table, scalar and horizon-aware regret-weighted TFT/NBEATSx calibration.
- ✅ Strict LP/oracle re-evaluation — calibrated forecasts checked against same Level 1 simulator.
- ✅ Medallion cleanup — Dagster assets now carry explicit `medallion=bronze|silver|gold` tags, and the real-data benchmark has a Silver price/weather feature bridge before Gold evaluation.
- ✅ DFL/DT foundation slice — added SOTA-ready forecast training schema, differentiable relaxed-LP pilot rows, offline Decision Transformer trajectory rows, deterministic action projection, and simulated paper-trading replay rows.
- ✅ Backend read models — `/dashboard/calibrated-ensemble-benchmark`, `/dashboard/risk-adjusted-value-gate`, and `/dashboard/forecast-dispatch-sensitivity` expose selector and diagnostic evidence for dashboard redesign without touching dashboard UI.
- ✅ MLflow/Postgres/Dagster persistence — latest run documented in [real-data-90-anchor-benchmark-report.md](technical/deep-research-reports/real-data-90-anchor-benchmark-report.md).
- ✅ Локальний PDF-архів — includes DFL, NBEATSx, TFT, storage DFL, TimeXer references.
- 🟡 Full differentiable DFL training — primitive exists, but forecast models are not yet fine-tuned end-to-end on regret.
- 🟡 Decision Transformer / M3DT-inspired strategy — policy scaffolding and trajectories exist; training/evaluation remains future work.
