# A Public Path Project: Ukraine BESS Arbitrage Index

Subtitle: Turning energy-arbitrage research into a public, source-backed analytics artifact.

I have started publishing a small public project from my energy-arbitrage work:

**Ukraine BESS Arbitrage Index**

The question behind it is intentionally simple:

**How much value could a standard battery capture on Ukrainian day-ahead electricity prices?**

The page uses official OREE DAM price data and computes a deterministic perfect-hindsight dispatch for standard commercial and industrial battery presets. It is not trying to pretend that a public demo is an operating energy system. It is a public analytics artifact: a way to show the data, assumptions, schedule, value calculation, and claim boundary in one place.

Public demo: https://energy-index.full-iron.com/ukraine-bess-arbitrage-index

Source: https://github.com/ilyafefelov/Diploma

## Why I built it

My thesis work started from a practical question: if a Ukrainian business installs PV and battery storage, how do we estimate the economic value of that battery under real market prices?

That question quickly becomes more than a model exercise. It touches data ingestion, market data quality, optimization, forecasting, ML boundaries, UI design, automation, and trust. A good-looking chart is not enough. A serious public artifact should also show what data was used, when it was generated, what is deterministic, what is experimental, and what the system is not claiming.

That is the main reason I wanted to publish this as a public path project rather than keep it as a private demo.

## What the index does

For each published delivery day, the system reads official day-ahead market price rows and computes a perfect-hindsight battery schedule for standard BESS presets.

The public page shows:

- realized arbitrage value;
- dispatch and state-of-charge traces;
- source and freshness receipts;
- battery assumptions;
- a public Forecast Challenge preview;
- a model scoreboard path;
- a strict no-execution claim boundary.

The public Forecast Challenge is important to me. Forecasts should not be mixed into realized index numbers. Forecast rows need their own timestamps, training cutoffs, and scoring after official data is published. This is how I want to present NBEATSx, TFT, schedule-selection, and later DT/HF DT work: visible, but not overclaimed.

## What this is not

This is post-defense public work. It is not private operator functionality.

It does not generate market bids.

It does not execute dispatch.

It does not claim integration with any utility or EMS platform.

That boundary matters. In energy systems, especially around batteries and dispatch, it is easy to make a prototype sound more operational than it is. I would rather be precise and earn trust gradually.

## Why this matters commercially

For Ukrainian commercial and industrial energy systems, the battery question is not only technical. It is also a business decision:

- What battery size makes sense?
- How much value could it capture under real price spreads?
- How many cycles does the strategy imply?
- How sensitive is the result to forecasts and assumptions?
- Can the economics be explained clearly to a decision-maker?

This kind of public index is not the final product. But it is a useful public surface for the kind of work I want to keep developing: BESS analytics, forecasting, optimization, transparent reporting, and product design for energy decision-making.

It also gives recruiters, partners, investors, and energy teams one concrete link instead of a vague description.

## The design angle

I also wanted the project to look like a real analytical product, not just a notebook exported to a dashboard.

The current page uses a light retro-blue academic style, receipt-like source sections, interactive dispatch visuals, and a public claim boundary that stays visible. The design goal is simple: make the page interesting enough for non-technical people to stay, but precise enough that technical people can inspect the assumptions.

That balance is important for Fulliron too. I want to build digital artifacts where product design, data, and technical credibility reinforce each other.

## What comes next

The next steps are likely:

- keep the daily public index running;
- improve the forecast challenge;
- add rolling model scoring;
- make the schedule-selection evidence stronger;
- keep DT/HF DT as gated research challengers, not default operator claims;
- improve the public page as a lead-generation and portfolio surface.

If you work in Ukrainian energy, PV+BESS, C&I energy systems, forecasting, optimization, climate/energy investing, or data/product roles, I would be glad to hear feedback.

This is still early, but it is a direction I want to keep developing.
