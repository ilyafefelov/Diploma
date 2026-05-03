---
marp: true
theme: default
paginate: true
size: 16:9
style: |
  section {
    font-family: 'Aptos', 'Segoe UI', sans-serif;
    background: linear-gradient(180deg, #f9fdff 0%, #d7f0ff 100%);
    color: #17416a;
    padding: 42px 58px;
  }
  h1, h2 {
    color: #0a5e9a;
  }
  strong {
    color: #0d3d68;
  }
  a {
    color: #0079c1;
  }
  code {
    background: rgba(0, 121, 193, 0.08);
    color: #0d3d68;
    padding: 0.15em 0.35em;
    border-radius: 0.4em;
  }
  section::after {
    content: 'week1 / interactive_report1';
    position: absolute;
    right: 58px;
    bottom: 22px;
    font-size: 12px;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: rgba(13, 61, 104, 0.55);
  }
---

# Week 1 Interactive Report
## Автономний енергоарбітраж BESS, Україна 2026

- Supervisor-ready surface для сьогоднішнього walkthrough
- Nuxt 4 + Vite route: `/week1/interactive_report1`
- Marp deck для швидкого PDF/PPTX export

---

# 1. Що вже є на кінець Week 1

- Scope свідомо обмежено: **DAM-only**, **UAH**, **strict similar-day forecast**, **LP baseline**
- Поточний battery layer = **feasibility-and-economics preview model**
- Уже є **control-plane API**, **Dagster asset graph** і **operator-facing dashboard surface**
- Це **не** real bidding / clearing / dispatch і **не** full digital twin

---

# 2. Repo-grounded verified contour

- `10` assets у `MVP_DEMO_ASSETS`
- Розклад по шарах: `2 Bronze / 1 Silver / 7 Gold`
- `8` FastAPI endpoint-ів у current control-plane/read-model surface
- `3` named source classes: **OREE**, **Open-Meteo**, **tenant registry**

Це означає, що MVP уже можна показувати як **робочий контур**, а не як набір намірів.

---

# 3. Чому проста degradation penalty зараз коректна

Поточна модель не претендує на electrochemical ageing stack. Вона свідомо задає деградацію як economic penalty:

`MC_deg = C_cycle / (2 * Capacity_mwh)`

`Penalty_t = MC_deg * Throughput_t`

Для demo battery metrics:

- `Capacity = 10 MWh`
- `Max power = 2 MW`
- `Round-trip efficiency = 95%`
- `Capex anchor = 210 USD/kWh` (Grimaldi visible metadata)
- `Lifetime = 15 years`, `usage = ~1 cycle/day` (NREL ATB)
- `FX = 43.9129 UAH/USD` (НБУ, `04.05.2026`)
- `Degradation cost per cycle proxy = 16,843.3 UAH`
- `Throughput proxy = 842.2 UAH/MWh throughput`

---

# 4. Як реально працює current data flow

1. `Tenant registry` задає location + timezone
2. `Bronze` забирає Open-Meteo і OREE rows, з fallback на deterministic synthetic data
3. `Silver` формує `strict_similar_day_forecast`
4. `Gold` запускає LP baseline, projected SOC preview і gatekeeper validation
5. `API + dashboard` повертають preview semantics для operator walkthrough

Ключова теза: current output = **recommendation preview**, а не real execution.

---

# 5. API і demo surface

Поточний FastAPI surface:

- `GET /health`
- `GET /tenants`
- `POST /weather/run-config`
- `POST /weather/materialize`
- `GET /dashboard/signal-preview`
- `GET /dashboard/operator-status`
- `POST /dashboard/projected-battery-state`
- `GET /dashboard/baseline-lp-preview`

Supervisor-facing surfaces:

- Nuxt interactive report
- Operator dashboard preview

---

# 6. DOI-опори для архітектурних рішень

- Yi et al. — DFL / predict-then-bid: <https://doi.org/10.48550/arXiv.2505.01551>
- Olivares et al. — NBEATSx: <https://doi.org/10.1016/j.ijforecast.2022.03.001>
- Jiang et al. — TFT: <https://doi.org/10.1002/for.3084>
- Elmachtoub and Grigas — Smart Predict, then Optimize: <https://doi.org/10.1287/mnsc.2020.3922>
- Grimaldi et al. — degradation-aware arbitrage: <https://doi.org/10.1016/j.est.2024.112380>

---

# 7. Що важливо сказати без overclaim-ів

- Є **working baseline contour**, а не завершена фінальна AI-стратегія
- Є **operator-facing explanation surface**, а не production market execution engine
- Є **throughput-based economic degradation penalty**, а не full digital twin батареї
- Є research trajectory до **DFL**, але вона ще не завершена

---

# 8. Що робити сьогодні ввечері

- Deploy route `/week1/interactive_report1` на Vercel з root directory `dashboard`
- Тримати поруч:
  - `docs/thesis/weekly-reports/week1/report.md`
  - `docs/thesis/weekly-reports/week1/supervisor-summary.md`
  - `docs/thesis/weekly-reports/week1/presentation-script.md`
- Якщо потрібен PDF/PPTX: відкрити цей файл у Marp for VS Code і експортувати

Ключовий меседж для керівника: **проєкт уже має демонстраційно придатний baseline MVP і чесно окреслену траєкторію до final DFL version.**