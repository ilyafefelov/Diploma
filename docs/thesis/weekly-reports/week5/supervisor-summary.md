# Week 5 Supervisor Summary

Цього тижня я фіналізував evidence package для академічного MVP і уточнив статус V13 readiness.

Головний результат: поточний MVP можна захищати як credentialless DAM delivery-day recommendation preview. Це не live trading system і не market-submittable bid generator. Система показує operator-facing recommendation/read-model evidence, зберігаючи `market_execution_enabled=false`.

V13 статус став чіткішим:

- Safe-switch lane валідовано: `77` incremental rows, `5` tenant/source pairs, `20 / 20` prior/train non-tail-risk material examples.
- V13 все ще заблокований explicit DAM publication receipts.
- SCMO/OREE receipt proof потребує source-backed export з `timestamp` і `source_publication_timestamp`.
- Safe-switch support alone does not unlock DT/LAVA.

Додатково я навчив direct DT candidate-index shadow без LAVA promotion. Це
працюючий HF DecisionTransformer research-shadow run: `3,741` training rows,
`1,735` train sequences, `90` eval sequences. Результат консервативний: Direct
DT повторює V2+ mean regret (`627.04` UAH) і гірший за strict/oracle
(`310.58` UAH), тому це manual dashboard preview, не thesis headline і не
deployment claim.

Що готове для перегляду:

- Week 5 report: [report.md](./report.md)
- Demo script: [demo-script.md](./demo-script.md)
- Evidence manifest: [../../appendices/evidence-manifest.md](../../appendices/evidence-manifest.md)
- Direct DT Shadow note: [../../../technical/DT_DIRECT_CANDIDATE_SHADOW.md](../../../technical/DT_DIRECT_CANDIDATE_SHADOW.md)
- V13 sprint note: [../../../technical/deep-research-reports/2026-05-25-full-project-review/v13-f3-acquisition-sprint-2026-05-26.md](../../../technical/deep-research-reports/2026-05-25-full-project-review/v13-f3-acquisition-sprint-2026-05-26.md)
- Methodology draft: [../../chapters/03-Methodology.md](../../chapters/03-Methodology.md)
- Results draft: [../../chapters/04-results-and-discussion.md](../../chapters/04-results-and-discussion.md)
- Google Docs thesis draft:
  [Draft.Thesis.2.goit.energy_arbitrage.Fefelov](https://docs.google.com/document/d/1jjja9ng99O-xCisijMUbPrEM-3UJi_hilwnFJY8nups/edit)

Thesis draft sync status: local Methodology/Results chapters now separate the
defendable MVP, offline evaluation, research-shadow evidence and V13
source-readiness blocker. The Google Docs results draft was updated to remove
hard V4/V5 run claims without packet paths and to include V13 F3 status.

Next week I will prepare the final demo around the credentialless MVP and, after review, choose either an all-tenant robustness pass or a narrow DFL pilot. SCMO receipt work remains a separate source-access lane, not a prerequisite for showing the diploma MVP.
