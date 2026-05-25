# Сценарій демо: Week 5

## Мета демо

Показати supervisor-ready handoff: credentialless academic MVP is defendable, V13 blocker is explicit and narrowed, and the project does not overclaim DT/LAVA or market execution.

## Передумови

1. Репозиторій відкрито на гілці `codex/28-dam-schedule-dock`.
2. Для live UI demo можна запустити dashboard/API, але цей handoff також працює як documentation-first demo.
3. Показувати тільки read-model/operator preview artifacts. Не показувати це як bid submission або dispatch automation.

## Крок 1. Показати full project review verdict

Відкрити:

- [../../../technical/deep-research-reports/2026-05-25-full-project-review/README.md](../../../technical/deep-research-reports/2026-05-25-full-project-review/README.md)
- [../../../technical/deep-research-reports/2026-05-25-full-project-review/fix-plan-closure-matrix.md](../../../technical/deep-research-reports/2026-05-25-full-project-review/fix-plan-closure-matrix.md)

Що сказати:

- Full review закрив локальні engineering gaps.
- Поточний MVP є credentialless offline/read-model demo.
- Verification lane був green для closure goal.
- Market execution не ввімкнено.

## Крок 2. Показати evidence manifest

Відкрити:

- [../../appendices/evidence-manifest.md](../../appendices/evidence-manifest.md)

Що сказати:

- Manifest є claim-to-artifact map для захисту.
- V2+ залишається headline offline research challenger.
- TFT, Poland, DT shadow і LAVA smoke описані як non-promoted/research evidence.
- V13 не є modeling slice; це acquisition/source-readiness gate.

## Крок 3. Показати V13 sprint

Відкрити:

- [../../../technical/deep-research-reports/2026-05-25-full-project-review/v13-f3-acquisition-sprint-2026-05-26.md](../../../technical/deep-research-reports/2026-05-25-full-project-review/v13-f3-acquisition-sprint-2026-05-26.md)

Короткий меседж:

- Safe-switch lane тепер має staged validated support: `77` rows, `5` tenant/source pairs, `20 / 20` prior examples floor.
- Це прибирає safe-switch deficit як активний локальний blocker.
- V13 все одно заблокований, бо немає explicit DAM publication receipt CSV.

Не казати:

- "V13 ready".
- "DT/LAVA can train".
- "The system can submit bids".

## Крок 4. Пояснити SCMO receipt handoff

Показати розділ `SCMO receipt handoff` у [report.md](./report.md).

Що сказати:

- Для receipt lane потрібен source-backed export з `timestamp` і `source_publication_timestamp`.
- Required env vars: `SCMO_USERNAME`, `SCMO_PASSWORD`, `SCMO_CLIENT_CERT_PEM`, `SCMO_CLIENT_KEY_PEM`, `SCMO_CLIENT_P12`.
- Observation time, first-seen polling time, HTTP `Date` і market-rule deadline не можна перетворювати на `source_publication_timestamp`.
- Після credentials виконується sanitized preflight, signed download/fetch, normalize, validate.

## Крок 5. Показати operator boundary

Відкрити:

- [../../../technical/OPERATOR_DAM_TIMING_AND_BID_BOUNDARY.md](../../../technical/OPERATOR_DAM_TIMING_AND_BID_BOUNDARY.md)

Що сказати:

- Operator surface є DAM delivery-day planning preview.
- `market_execution_enabled=false`.
- No `ProposedBid`, no market submission, no cleared trades, no dispatch commands.
- SCMO credentials are not required for the diploma MVP; they matter only for stronger market-submission-grade receipt proof.

## Крок 6. Показати thesis chapter readiness

Відкрити:

- [../../chapters/03-Methodology.md](../../chapters/03-Methodology.md)
- [../../chapters/04-results-and-discussion.md](../../chapters/04-results-and-discussion.md)
- [Google Docs thesis draft](https://docs.google.com/document/d/1jjja9ng99O-xCisijMUbPrEM-3UJi_hilwnFJY8nups/edit)

Що сказати:

- Week 5 syllabus очікує Methodology і first Results/Discussion draft.
- Ці chapter drafts мають бути прив'язані до evidence manifest, а не до unsupported claims.
- Negative/blocked evidence є частиною методологічної чесності: pipeline не підганяє висновок під DT/LAVA.
- Methodology тепер має reader map: MVP, offline evaluation, research shadows і source-readiness gate розділені явно.
- Results draft у Google Docs оновлено: V4/V5 без локального packet path позначені як pending evidence, а V13 F3 описано як safe-switch validated but DAM receipts blocked.

## Крок 7. Закрити демо коротким статусом

Фінальний меседж:

> Поточний дипломний результат defendable як credentialless DAM recommendation preview з сильним evidence manifest. V13 тепер має чіткий залишковий blocker: explicit DAM publication receipts. Safe-switch support validated, але без source-backed publication timestamps немає DT/LAVA readiness, training permission або market execution.
