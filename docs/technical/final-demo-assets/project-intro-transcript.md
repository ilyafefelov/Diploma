# Project Intro Video Transcript

This is the English narration used for `project-intro.mp4`. It is a visual
documentation asset for the GitHub README and GitHub Pages intro page. It does
not change the product boundary: source-backed operator preview, read-model
evidence, no `ProposedBid`, and no market execution.

Energy storage can create value when prices change hour by hour. But in
electricity markets, a battery decision is not just a price signal. It has
source evidence, physical limits, safety gates, and operator responsibility.

Smart Energy Arbitrage 2026 turns that problem into a reviewable operator
preview for Ukrainian BESS assets. It reads DAM and IDM price context, forecast
rows, tenant state, and battery constraints, then presents hourly
recommendations as evidence, not commands.

On the operator dashboard, a reviewer can choose tenant, venue, and delivery
date, inspect source readiness, compare schedule candidates, and see when the
system abstains to HOLD. The default story is intentionally conservative:
source-backed data first, deterministic feasibility checks, and market
execution disabled.

The experimental layer explains why the recommendation is credible. A strict LP
and oracle contour provides the feasible decision reference. The V2 selector
establishes a baseline at 206.37 UAH mean regret, while the schedule/value
learner V2+ improves the headline evidence to 174.77 UAH.

Post-defense model-lineage audit corrected the 168.16 UAH artifact: historical
`dt_v2_plus` is random forest trained on exact mirrored rows, with four
profile-row switches on one date, not transformer/OOS evidence. The separate HF
transformer-backbone shadow shows a 158.71 UAH mirrored-packet diagnostic and a
distinct read-model audit without realized regret. These
are promising research signals, not production strategy switches.

For potential partners, the near-term value is practical: faster operator
review, clearer source traceability, safer abstention when evidence is weak,
and a pilot-ready path toward operational validation. For technical reviewers,
the value is reproducibility: tracked metrics, FastAPI read models, Dagster
lineage, tests, and a public repository that refuses unsafe claims.

Open the README, the operator preview, the defense dashboard, FastAPI docs, and
the thesis paper. The boundary remains simple: source-backed operator preview,
evidence first, human review, and no market execution.
