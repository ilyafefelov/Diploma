# Dashboard review

## Routes

| Route | Purpose | Review result |
|---|---|---|
| `/operator` | Human operator recommendation preview | Pass |
| `/defense` | Thesis evidence/explanation panels | Pass |
| `/ukraine-bess-arbitrage-index` | Public static portfolio surface | Pass |
| `/forecast-challenge` | Forecast publication view | Pass |
| `/model-scoreboard` | Realized scoring view | Pass |

## Strengths

- Claim-boundary language is visible near operational-looking content.
- Public JSON is parsed through a defensive content helper.
- Static generation provides an offline demo path.
- Explicit artifact interfaces replace loose `any` typing.
- Local icon collections remove deployment-time icon fetches.
- Architecture tests protect route content, public surface file budgets, and
  icon configuration.

## Remaining improvements

1. Split the approximately 722 kB client chunk by route or heavy visualization
   dependency and measure route-level performance before/after.
2. Reduce the 97 kB public page source by extracting presentation sections
   without changing the standalone evidence contract.
3. Add browser-level accessibility and visual regression checks to hosted CI.
4. Track upstream Nitro/H3 unused-import build warnings separately from product
   failures.
