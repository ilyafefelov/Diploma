# interactive_report1

Цей пакет містить supervisor-facing interactive report для Week 1 у двох формах:

- жива Nuxt-сторінка для Vercel deployment;
- Marp deck для швидкого export у PDF/PPTX з VS Code.

## Що саме входить у пакет

- `dashboard/app/pages/week1/interactive_report1.vue` — самодостатня interactive report surface;
- `docs/thesis/weekly-reports/week1/interactive_report1/slides.marp.md` — Marp deck для VS Code export;
- `docs/thesis/weekly-reports/week1/report.md` — повний канонічний звіт;
- `docs/thesis/weekly-reports/week1/supervisor-summary.md` — коротка supervisor-facing версія;
- `docs/thesis/weekly-reports/week1/presentation-script.md` — speaking script;
- `docs/thesis/weekly-reports/week1/supervisor-message.md` — готові тексти для відправки керівнику.

## Local preview

```powershell
Set-Location dashboard
npm install
npm run dev
```

Після цього interactive report доступний за route:

```text
/week1/interactive_report1
```

Локальна адреса у dev-режимі:

```text
http://localhost:64163/week1/interactive_report1
```

## Vercel

Для швидкого деплою сьогодні ввечері достатньо використовувати вже наявний Nuxt app у `dashboard/`.

- Root directory: `dashboard`
- Install command: `npm install`
- Build command: `npm run build`
- Framework preset: `Nuxt.js`
- Optional env for clean absolute social links: `NUXT_PUBLIC_SITE_URL=https://<your-vercel-domain>`

Окремий Vite scaffold не додавався, бо Nuxt 4 уже працює поверх Vite. Це найкоротший і найменш ризиковий шлях до deployable supervisor demo.

## Marp for VS Code

1. Відкрити `slides.marp.md` у VS Code.
2. Використати розширення Marp for VS Code для Preview.
3. За потреби експортувати HTML, PDF або PPTX.

## Ground truth

Усі числа в interactive report прив’язані до поточного repo state, а не вигаданих placeholder-метрик:

- `10` assets у `MVP_DEMO_ASSETS`;
- `8` FastAPI endpoint-ів для current control-plane/read-model surface;
- `3` named source classes: OREE, Open-Meteo, tenant registry;
- battery demo metrics: `10 MWh`, `2 MW`, `95%`, capex-throughput proxy `16,843.3 UAH/cycle`, що дає `842.2 UAH/MWh throughput`.
- proxy basis: `210 USD/kWh` (Grimaldi visible capex anchor), `15 years` і `~1 cycle/day` (NREL ATB), `43.9129 UAH/USD` (НБУ, `04.05.2026`).

DOI-посилання в deck і Nuxt page також вирівняні з уже використаними thesis artifacts.