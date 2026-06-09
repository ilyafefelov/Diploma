# DT Shadow + Academic MVP Defense Packet

Date: 2026-05-25

This packet packages the credentialless Academic MVP and Hugging Face-backed
DT research-shadow smoke for defense use. It is intentionally scoped to
operator preview and offline evidence.

Claim boundary: **DAM/IDM hourly recommendation preview only**,
`market_execution_enabled=false`, no `ProposedBid`, no market order payload, no
promoted DT/LAVA controller, and no synthetic OREE/SCMO publication receipts.

## Current Evidence

- Academic MVP readiness: passed for credentialless diploma MVP.
- Market-submission receipt readiness: blocked by external credential/source
  access; this does not block the operator-preview MVP.
- DT research-shadow backbone:
  `huggingface_decision_transformer_model`.
- HF availability: `hf_decision_transformer_available=true`.
- Research-shadow training rows: `97,431`.
- V13-promotable training rows: `0`.
- Chronological split: `chronological_delivery_timestamp`.
- Action target: candidate index or schedule family, not raw hourly
  BUY/SELL/HOLD.
- Promotion: `dt_promotion_gate_passed=false`.

## Regret / Value Metrics

| Comparator | Mean regret UAH | Mean value UAH | Interpretation |
| --- | ---: | ---: | --- |
| DT shadow | `507.90` | `3403.59` | Working transformer smoke, not promoted. |
| V2+ fallback | `510.82` | `3400.67` | Teacher/comparator/fallback. |
| Strict reference | `431.70` | `3479.78` | Still the stronger reference on this slice. |
| Behavior cloning | `510.82` | `3400.67` | Control baseline matched V2+ on this eval slice. |

Secondary accuracy is `0.364`; the primary defense metric is regret/value, not
classification accuracy.

## Defense Deck Structure

1. Boundary: diploma MVP is credentialless operator preview.
2. Architecture: forecasts, candidate schedules, V2+ fallback, strict oracle,
   DT research-shadow lane.
3. Academic MVP gate passport: what passes, what stays blocked.
4. DT dataset contract: state, action, reward/RTG, chronological split.
5. HF transformer smoke: dependency extra, backbone, train/eval counts.
6. Regret/value comparison: DT vs V2+ vs strict vs behavior cloning.
7. Dashboard demo: defense page consumes `academic_mvp_readiness`; operator
   page defaults to V2+ and can manually switch to DT Shadow hourly schedules.
8. Next research steps: improve teacher rows and run a strict LP/oracle
   promotion benchmark without SCMO credentials or market execution.

Deck artifacts:

- Local editable deck:
  [dt-shadow-academic-mvp-defense-deck.pptx](dt-shadow-academic-mvp-defense-deck.pptx).
- Local render contact sheet:
  [dt-shadow-academic-mvp-defense-contact-sheet.png](dt-shadow-academic-mvp-defense-contact-sheet.png).
- Native Google Slides import:
  [DT Shadow Academic MVP Defense Deck - Verified](https://docs.google.com/presentation/d/1uIJV-tj4SOTf7QKLD9p-Eo_igDVj1HshJPWCHfgeKOM).
- Build manifest:
  [dt-shadow-academic-mvp-defense-deck-manifest.json](dt-shadow-academic-mvp-defense-deck-manifest.json).

## Next Strict LP/Oracle Promotion Path

The next research step is not market submission and not raw hourly DT control.
It is an offline candidate-index promotion experiment:

1. Keep V2+ frozen as teacher/comparator/fallback and default dashboard
   strategy.
2. Train challenger policies only on chronological delivery-time splits with
   candidate id / schedule-family actions.
3. Score every selected candidate through the unchanged strict LP/oracle
   evaluator against V2+, strict reference, and behavior-cloning controls.
4. Require lower mean regret than V2+, no median-regret degradation, 4 / 4
   rolling robustness windows, zero safety violations, and
   `market_execution_enabled=false`.
5. Keep V13 market-submission/source-readiness gates blocked until explicit
   source-backed DAM publication receipts exist; SCMO credentials are not
   required for this credentialless academic promotion experiment.

## Visuals

| Figure | Purpose |
| --- | --- |
| [Gate passport](assets/academic-mvp-gate-passport.svg) | Shows which gates pass for the credentialless MVP and which stay blocked. |
| [DT sequence contract](assets/dt-sequence-contract.svg) | Shows the offline DT state/action/reward contract. |
| [DT regret/value comparison](assets/dt-shadow-regret-value.svg) | Shows current HF DT smoke metrics against controls. |
