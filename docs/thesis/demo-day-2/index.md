# Demo Day 2 Evidence Packet

Date: 2026-05-19

This packet summarizes the work since the Week 2 demo using deterministic
figures from the current evidence stack. It is intended for a five-minute
supervisor/peer progress update.

Claim boundary: all results are **Offline Strategy Promotion** evidence only.
No live market execution, no dashboard/API default strategy switch, no deployed
Decision Transformer controller, and no EU/Poland feature training are claimed.

## Headline

The strongest result remains Ukrainian-only official global-panel NBEATSx
Schedule/Value Learner V2+:

- calibrated V2+ mean regret: `174.77` UAH;
- strict similar-day mean regret: `310.58` UAH;
- improvement vs strict: `43.73%`;
- rolling robustness: `4 / 4` windows;
- `market_execution_enabled=false`.

The new NBEATSx+TFT candidate-portfolio rolling gate materialized successfully
in Dagster run `35c6ddcd-ce54-4ae8-b527-670a875faf3f`, but it did **not**
replace V2+: rolling pass count was `0 / 4`.

## Visuals

| Figure | Purpose |
| --- | --- |
| [V2+ result card](assets/v2-plus-result-card.svg) | One-slide headline result and claim boundary. |
| [Regret ladder](assets/regret-ladder.svg) | Experiment ladder from strict baseline to V2+ and later selector attempts. |
| [TFT complementarity card](assets/tft-complementarity-card.svg) | Shows why TFT is useful evidence but not yet a robust replacement. |
| [Experiment timeline](assets/experiment-timeline.svg) | Last-10-days research path for the demo talk track. |
| [Offline architecture graph](assets/offline-architecture.svg) | Evidence pipeline from data sources to strict LP/oracle gate. |
| [Online/read-model graph](assets/online-read-model-architecture.svg) | Read-only demo architecture from Postgres evidence to Nuxt surfaces. |
| [Academic MVP gate passport](assets/academic-mvp-gate-passport.svg) | Credentialless MVP gates, blocked receipt readiness, and no-execution boundary. |
| [DT sequence contract](assets/dt-sequence-contract.svg) | Offline Decision Transformer sequence dataset contract. |
| [DT shadow regret/value comparison](assets/dt-shadow-regret-value.svg) | Current HF DT smoke metrics against strict, V2+, and behavior cloning. |

## Defense Deck

| Artifact | Purpose |
| --- | --- |
| [DT Shadow Academic MVP defense deck](dt-shadow-academic-mvp-defense-deck.pptx) | Editable 9-slide PowerPoint package for the credentialless Academic MVP and HF DT shadow defense story. |
| [Deck contact sheet](dt-shadow-academic-mvp-defense-contact-sheet.png) | Rendered thumbnail review sheet for the local PPTX export. |
| [Deck build manifest](dt-shadow-academic-mvp-defense-deck-manifest.json) | Source evidence, slide count, and no-execution claim boundary for the generated deck. |
| [Verified Google Slides import](https://docs.google.com/presentation/d/1uIJV-tj4SOTf7QKLD9p-Eo_igDVj1HshJPWCHfgeKOM) | Native Google Slides copy verified by connector readback. |

## Talk Track

1. Week 2 baseline was forecast-first: strict similar-day remained hard to beat.
2. The project moved to decision-value evidence: every candidate schedule is
   scored by the same strict LP/oracle regret metric.
3. Official global-panel NBEATSx plus Schedule/Value Learner V2+ is now the
   headline: `174.77` UAH mean regret and `4 / 4` rolling windows.
4. TFT was tested seriously as quantile/risk schedules. It has local
   opportunities (`24 / 90` latest tenant-anchors), but the prior selector
   cannot exploit them robustly; rolling gate result is `0 / 4`.
5. The next research branch should start DT/LAVA-style work only against frozen
   V2+, using candidate/value or schedule-neighbor supervision first.
6. The credentialless Academic MVP now has a defense packet: operator-preview
   gates pass, but market-submission receipts and DT/LAVA promotion remain
   blocked.
7. HF DecisionTransformerModel smoke is running as research-shadow evidence:
   it slightly beats V2+/behavior cloning on the current eval slice, loses to
   strict, and is not promoted.
