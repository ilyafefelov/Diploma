# Chart Stack Decision

Status: accepted for the next dashboard slice

## Decision

Для нового operator dashboard у цьому repo приймаємо mixed chart stack:

- Primary runtime chart library: Apache ECharts через Vue wrapper
- Signature custom layer: D3 лише для окремих highly-designed views
- Prototype and research layer: Observable Plot для швидких exploratory charts і design spikes, не як основна production dependency

Visx не рекомендується як основний шлях для цього проекту, тому що він оптимізований під React, а не під Nuxt/Vue.

## Why This Decision

Поточний legacy dashboard у [_legacy_smart-energy-ai/dashboard/package.json](d:/School/GoIT/Courses/Diploma/_legacy_smart-energy-ai/dashboard/package.json) сидить на `chart.js` і `vue-chartjs`. Це достатньо для простих line/bar charts, але занадто слабко для того типу щільного energy-аналітичного UI, який потрібен далі:

- multi-series DAM overlays
- forecast vs actual bands
- dispatch schedule visuals
- regret / oracle comparison
- weather and solar context layers

Для нового dashboard потрібен стек, який дає одночасно:

- хорошу швидкість для time-series
- сильну declarative базу для 70-80% графіків
- можливість зробити кілька справді signature charts без боротьби з абстракціями бібліотеки
- нормальну підтримку в Nuxt/Vue кодовій базі

## Evaluated Options

### Apache ECharts

Що підтвердилось з офіційних матеріалів:

- 20+ chart types і composable components
- Canvas і SVG rendering
- progressive rendering і stream loading
- dataset/transforms support
- accessibility-friendly features

Чому підходить нам:

- добре перекриває більшість operational dashboard cases без custom low-level drawing
- природно підтримує multi-axis, overlays, brush/zoom, mark lines, data zoom, heatmaps, scatter, calendar/time-series combinations
- дає достатньо контролю для красивого branded analytical UI без повного переписування кожного chart вручну
- під Nuxt/Vue інтеграція пряміша, ніж у visx

Слабкі сторони:

- custom aesthetics вимагають дисципліни; дефолтний look у ECharts легко зробити "enterprise-generic"
- для деяких truly bespoke visuals все одно краще мати окремий D3 layer

### D3

Що підтвердилось з офіційних матеріалів:

- library for bespoke data visualization
- максимальна гнучкість і контроль над формою

Чому не беремо як primary stack:

- занадто дорогий у розробці для всіх routine dashboard charts
- занадто великий maintenance tax для AI-assisted codebase, де важлива передбачуваність і швидке перегенерування
- ризик, що більшість часу піде не на продуктову аналітику, а на низькорівневу побудову axes, interactions і layout glue

Де D3 потрібен:

- signature dispatch ribbon
- regret delta / waterfall with custom annotations
- solar path / daylight arc overlays
- views, де chart є частиною visual identity, а не просто display widget

### Observable Plot

Що підтвердилось з офіційних матеріалів:

- concise code for expressive exploratory charts
- дуже сильний для швидкого analysis/prototyping workflow

Чому не беремо як primary production stack:

- кращий для exploratory data work, ніж для повноцінного operator dashboard shell
- менше control surface для складної branded interaction-heavy app integration, ніж у ECharts + selective D3

Де він корисний:

- швидкі design spikes
- internal notebook-like experiments
- proof-of-concept chart directions до production implementation

### Visx

Що підтвердилось з офіційних матеріалів:

- expressive low-level visualization primitives for React
- intentionally unopinionated
- not a charting library

Чому відхиляємо:

- стек проекту зараз Nuxt/Vue, не React
- Visx має сенс, коли ми хочемо будувати власний chart system поверх React primitives
- у цьому repo він додає integration friction без достатньої вигоди над ECharts + D3

## Decision Matrix

Оцінка від 1 до 5.

| Option | Nuxt/Vue fit | Analytical breadth | Signature flexibility | Dev speed | Maintainability | Verdict |
| --- | --- | --- | --- | --- | --- | --- |
| Apache ECharts | 5 | 5 | 3 | 5 | 4 | Primary |
| D3 | 3 | 5 | 5 | 2 | 2 | Secondary custom layer |
| Observable Plot | 4 | 3 | 2 | 5 | 4 | Prototype layer |
| Visx | 1 | 4 | 4 | 2 | 2 | Reject for current stack |
| Chart.js | 4 | 2 | 2 | 4 | 4 | Legacy baseline only |

## Recommended Runtime Stack

### 1. Primary dashboard charts

Use Apache ECharts for:

- DAM history vs forecast overlay
- market price intraday profile
- weather context strips
- SOC / SOH trend panels
- stacked cost / revenue bars
- tenant comparison small multiples
- calendar heatmaps or time heatmaps

Recommended implementation shape:

- `echarts/core` imports only for the used chart types
- Vue wrapper such as `vue-echarts`
- client-only rendering in Nuxt where needed
- a shared theme module for colors, grid, tooltip, axis, line thickness, annotations

### 2. Signature charts

Use D3 only where the chart itself must become part of the product identity:

- dispatch ladder / schedule ribbon
- regret vs oracle delta narrative chart
- solar daylight arc behind operational timeline
- custom state-band visual showing charge / discharge / hold windows

Rule: якщо chart можна описати стандартним analytical grammar і він не втрачає сенс без custom geometry, його не треба писати на D3.

### 3. Prototype track

Use Observable Plot for:

- fast experiments before productization
- validating composition of marks and scales
- internal comparison of chart concepts

Після validation production version має переходити або в ECharts, або в D3, але не тягнути Plot в runtime без явної причини.

## Chart Patterns For The MVP Dashboard

### DAM history vs forecast overlay

Recommended stack: ECharts

Pattern:

- actual DAM line
- forecast line with lighter or dashed treatment
- forecast horizon band
- confidence or error envelope when available
- vertical markers for buy/sell commit points

### Dispatch plan ladder / schedule ribbon

Recommended stack: D3

Pattern:

- horizontal time ribbon
- vertically separated bands for `BUY`, `SELL`, `HOLD`
- power intensity encoded by thickness or opacity
- gatekeeper-blocked intervals explicitly marked

Fallback if speed matters more than uniqueness:

- ECharts custom series

### SOC / SOH operational state

Recommended stack: ECharts

Pattern:

- compact line/band chart, not a generic gauge
- operating thresholds at 5% and 95%
- SOH as slower companion trend or bullet-style reference

### Regret vs oracle comparison

Recommended stack: D3 or ECharts depending on depth

Pattern:

- waterfall or lollipop delta view
- cumulative regret line beneath or beside period deltas
- explicit annotation for best / worst intervals

Default choice:

- ECharts first
- D3 only if we need a more narrative visual explanation layer

### Weather and solar contribution context

Recommended stack: ECharts

Pattern:

- compact synchronized weather strip above or below DAM chart
- daylight background bands
- solar radiation area
- cloud / precipitation overlays kept restrained

## Nuxt Integration Guidance

- Avoid Axios in new chart data flows; use `fetch` or `$fetch`
- Keep a single chart adapter layer in the new dashboard codebase
- Centralize theme tokens so ECharts and D3 visuals share the same palette and spacing logic
- SSR should not be a blocker, but chart components should be mounted carefully on the client if library wrappers require DOM access

## Libraries To Add When Dashboard Work Starts

Primary candidate set:

- `echarts`
- `vue-echarts`
- `d3`

Do not add all candidate libraries at once. Add:

1. ECharts + Vue wrapper first
2. D3 only when the first signature chart is implemented
3. Observable Plot only if the team wants an explicit prototype lane inside the repo

## Final Recommendation

For this repo, the most practical and highest-upside stack is:

- ECharts as the production default
- D3 as the exception path for memorable, branded analytical visuals
- Observable Plot as a fast exploration tool, not the production foundation

This gives the project faster dashboard delivery than pure D3, much stronger analytical range than legacy Chart.js usage, and a cleaner path to the more creative chart language the next dashboard needs.