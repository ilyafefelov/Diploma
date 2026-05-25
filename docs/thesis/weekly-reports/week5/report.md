# Щотижневий звіт 5

## 1. Фокус тижня

Week 5 сфокусовано на фіналізації доказового пакета перед фінальним демо: не розширювати scope, а закріпити те, що вже можна захищати. Поточний академічний MVP залишається credentialless DAM recommendation preview з відтворюваним offline/read-model evidence. V13 переведено з розмитого blocker status у конкретний acquisition blocker: safe-switch підтримка валідована, але explicit DAM publication receipts все ще відсутні.

Цей зріз не додає API contracts, Dagster assets, dashboard semantics або market execution. `market_execution_enabled=false` залишається незмінною межею.

## 2. Виконані завдання

- Закрито повний project-review fix plan як локально перевірений стан: Ruff, Mypy, Pytest, Dagster defs, Compose config, dashboard typecheck і Vitest були зеленими у closure matrix.
- Зафіксовано V13 acquisition sprint у [v13-f3-acquisition-sprint-2026-05-26.md](../../../technical/deep-research-reports/2026-05-25-full-project-review/v13-f3-acquisition-sprint-2026-05-26.md).
- Оновлено [evidence-manifest.md](../../appendices/evidence-manifest.md), щоб thesis claims посилалися на актуальний V13 packet і не повторювали старий safe-switch blocker.
- Підтверджено, що safe-switch support досягає `20 / 20` prior/train non-tail-risk material examples для всіх п'яти tenant/source pairs.
- Підтверджено, що V13 все ще блокується explicit DAM publication receipts, а не локальним safe-switch deficit.
- Підготовлено supervisor handoff для Week 5: цей report, [supervisor-summary.md](./supervisor-summary.md) і [demo-script.md](./demo-script.md).

## 3. Поточний MVP та V13 статус

Поточний defendable MVP:

- DAM delivery-day operator recommendation preview.
- Offline/read-model evidence поверх FastAPI, Dagster, dashboard і thesis artifacts.
- V2+ залишається headline offline research challenger.
- Dashboard/operator surface є preview/review interface, не market order system.

V13 acquisition status:

| Lane | Status | Evidence |
|---|---|---|
| Safe-switch examples | Validated staged support | `77` incremental rows, `5` tenant/source pairs, `20 / 20` prior examples floor |
| DAM publication receipts | Blocked | no source-backed CSV with both `timestamp` and `source_publication_timestamp` |
| SCMO credential path | Blocked external access | missing `SCMO_USERNAME`, `SCMO_PASSWORD`, cert/key or P12 material |
| DT/LAVA readiness | Not ready | `dt_lava_ready=false`, `permits_model_training=false` |
| Market execution | Disabled | `market_execution_enabled=false` |

Safe-switch support alone does not unlock V13. Explicit source-backed DAM publication receipts are still required before any candidate generation, DT/LAVA training permission, or stronger source-readiness claim.

## 4. Тестування та перевірки

Після V13 sprint було виконано:

| Перевірка | Результат |
|---|---|
| `git diff --check` | pass |
| Targeted V13/receipt/SCMO tests | `71 passed` |
| `tests/test_project_entrypoints.py` | `34 passed` |
| Targeted Ruff check | pass |
| Canonical V13 preflight | tracked config still conservative; both canonical CSV paths empty |

Для Week 5 package потрібно повторити:

```powershell
git diff --check
.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\test_project_entrypoints.py
rg -n "market_execution_enabled=true|dt_lava_ready=true|permits_model_training=true|ProposedBid" docs\thesis docs\technical
```

## 5. SCMO receipt handoff

Щоб розблокувати V13 receipt lane, потрібен source-backed export з двома обов'язковими колонками:

- `timestamp`
- `source_publication_timestamp`

Санітизований credential preflight очікує такі env vars:

- `SCMO_USERNAME`
- `SCMO_PASSWORD`
- `SCMO_CLIENT_CERT_PEM`
- `SCMO_CLIENT_KEY_PEM`
- `SCMO_CLIENT_P12`

Optional:

- `SCMO_CLIENT_KEY_PASSWORD`
- `SCMO_CLIENT_P12_PASSWORD`

Порядок після появи credentials:

1. Запустити `scripts/preflight_scmo_dam_ws_security_credentials.py`.
2. Виконати signed/authenticated SCMO download probe/fetch only for receipt evidence.
3. Normalize export.
4. Validate:

```powershell
.\.venv\Scripts\python.exe scripts\validate_oree_dam_publication_receipts.py --input <raw.csv> --output <normalized.csv>
```

Не можна виводити `source_publication_timestamp` з local observation time, first-seen polling time, HTTP response `Date`, або OREE market-rule deadline. Це має бути source-provided row-level publication timestamp.

## 6. Артефакти

- Full project review packet: [README.md](../../../technical/deep-research-reports/2026-05-25-full-project-review/README.md)
- Closure matrix: [fix-plan-closure-matrix.md](../../../technical/deep-research-reports/2026-05-25-full-project-review/fix-plan-closure-matrix.md)
- V13 sprint: [v13-f3-acquisition-sprint-2026-05-26.md](../../../technical/deep-research-reports/2026-05-25-full-project-review/v13-f3-acquisition-sprint-2026-05-26.md)
- Evidence manifest: [evidence-manifest.md](../../appendices/evidence-manifest.md)
- Operator boundary: [OPERATOR_DAM_TIMING_AND_BID_BOUNDARY.md](../../../technical/OPERATOR_DAM_TIMING_AND_BID_BOUNDARY.md)
- Methodology draft: [03-Methodology.md](../../chapters/03-Methodology.md)
- Results draft: [04-results-and-discussion.md](../../chapters/04-results-and-discussion.md)

## 7. Ризики та відповіді

| Ризик | Чому це важливо | Відповідь |
|---|---|---|
| V13 може виглядати готовим через safe-switch `20 / 20` | Це було б overclaim | У звіті явно вказано, що DAM receipts залишаються blocker |
| OREE PXS observation rows можуть бути сприйняті як publication receipts | Observation time не є source publication timestamp | Введено SCMO handoff rule: не деривити `source_publication_timestamp` |
| SCMO credentials можуть змішатися з diploma MVP | SCMO потрібні для stronger market-submission-grade receipt proof, не для credentialless MVP | MVP описано як defendable без SCMO |
| DT/LAVA smoke artifacts можуть виглядати як deployment | Це research-only evidence | `dt_lava_ready=false`, `permits_model_training=false`, no `ProposedBid` |

## 8. План на наступний тиждень

1. Підготувати фінальне демо як credentialless academic MVP: operator preview, evidence manifest, V2+ result, conservative V13 blocker.
2. Після supervisor review вирішити, чи потрібен all-tenant robustness pass або вузький DFL pilot.
3. Якщо з'являться SCMO credentials, виконати тільки sanitized preflight, receipt fetch/normalize/validate і повторний acquisition preflight.
4. Не змінювати `market_execution_enabled=false` і не вводити `ProposedBid` semantics до окремого gate.

## 9. Короткий висновок

Week 5 переводить проєкт у defendable handoff state. Академічний MVP можна показувати як стабільний DAM recommendation preview з доказовою базою, а V13 чесно залишається source-readiness blocker. Найсильніший меседж для керівника: система вже має захищуваний credentialless MVP, але не маскує відсутність market-submission-grade DAM receipt evidence.
