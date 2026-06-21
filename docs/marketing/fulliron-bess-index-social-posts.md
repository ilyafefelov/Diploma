# Ukraine BESS Arbitrage Index Social Posts

Public URL: https://energy-index.full-iron.com/ukraine-bess-arbitrage-index

Image asset:
`docs/marketing/fulliron-bess-index-twitter-card.png`

## X / Twitter Single Post

Publishing a public path project:

Ukraine BESS Arbitrage Index.

Official OREE DAM prices -> source-backed BESS dispatch receipt for Ukrainian day-ahead power prices.

Public demo only. No bids. No execution.

https://energy-index.full-iron.com/ukraine-bess-arbitrage-index

## X / Twitter Thread

1/ I started publishing a small public path project from my energy-arbitrage work:

Ukraine BESS Arbitrage Index.

It asks one simple question:
How much value could a standard battery capture on Ukrainian day-ahead electricity prices?

https://energy-index.full-iron.com/ukraine-bess-arbitrage-index

2/ The page uses official OREE DAM rows and computes a deterministic BESS dispatch receipt for standard C&I battery presets.

It shows the source, schedule, SOC trace, value estimate, and freshness receipt.

No hidden "AI magic"; the boundary is visible.

3/ The point is not to claim market execution.

No bids.
No dispatch commands.
No utility integration claim.

It is a public analytics artifact and a lead-generation surface for BESS analytics, forecasting, optimization, and product design work.

4/ I am especially interested in feedback from people working around Ukraine, PV+BESS, C&I energy systems, energy trading, forecasting, climate/energy investing, and data/product roles.

If this is relevant to your work, I would be glad to talk.

## Facebook / LinkedIn-Style Version

I have started publishing a small public path project from my energy-arbitrage work: Ukraine BESS Arbitrage Index.

The question is simple: how much value could a standard battery capture on Ukrainian day-ahead electricity prices?

The public page uses official OREE DAM price data and turns it into a source-backed BESS dispatch receipt: value estimate, 24-hour schedule, SOC trace, battery assumptions, freshness status, and a clear claim boundary.

This is not market execution. It does not generate bids or dispatch commands. It is a public analytics and portfolio surface for the kind of work I want to keep developing: BESS analytics, forecasting, optimization, transparent reporting, and product design for energy decision-making.

I am looking for feedback and conversations with people working in Ukrainian energy, PV+BESS, C&I systems, energy analytics, climate/energy investing, recruiting, and data/product roles.

Public demo:
https://energy-index.full-iron.com/ukraine-bess-arbitrage-index

## Hashtags

#Ukraine #EnergyAnalytics #BESS #BatteryStorage #EnergyStorage #EnergyTransition #DataProducts #Optimization #Forecasting #ClimateTech #Fulliron

## Posting Cadence

- X/Twitter: post the single image post first, then a thread 1-2 days later if it gets engagement.
- LinkedIn: reuse the Facebook/LinkedIn version with a slightly more professional CTA.
- Facebook: use the medium version, warmer and less technical.
- Instagram stories: repost the visual with "Public demo now live" and a link sticker.

## Automation Channels

- Dedicated index X account: daily after the public JSON publisher succeeds.
- Personal X account: `x.com/fefelov`, weekly on Monday.
- Daily workflow: `.github/workflows/public-bess-social-daily.yml`.
- Weekly workflow: `.github/workflows/public-bess-social-weekly.yml`.
- Publisher script: `scripts/social_bess_index.py`.
- Idempotency log: `docs/marketing/social_log.json`.

Required X secrets for the dedicated daily account:

- `X_DAILY_API_KEY`
- `X_DAILY_API_SECRET`
- `X_DAILY_ACCESS_TOKEN`
- `X_DAILY_ACCESS_TOKEN_SECRET`

Required X secrets for the personal weekly account:

- `X_PERSONAL_API_KEY`
- `X_PERSONAL_API_SECRET`
- `X_PERSONAL_ACCESS_TOKEN`
- `X_PERSONAL_ACCESS_TOKEN_SECRET`
