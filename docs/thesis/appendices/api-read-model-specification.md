# Додаток А. API read-model специфікація

Цей додаток описує FastAPI-шар як thesis-facing evidence surface для DAM/IDM hourly recommendation preview. API не є trading або dispatch контуром: він читає готові Dagster assets, Postgres/read stores, forecast store, telemetry store і локальні configuration sources, а потім подає оператору або викладачу відтворюваний read model. Усі endpoint-и нижче треба трактувати як контракти перегляду та перевірки evidence, а не як market-submission API.

Таблиця А.1. Основні endpoint-и та роль у thesis evidence

| Endpoint / група | Роль у роботі | Межа інтерпретації |
| --- | --- | --- |
| `GET /health`, `GET /tenants` | Перевірка доступності API та вибір tenant/location context. | Supporting layer; не strategy evidence. |
| `POST /weather/run-config`, `POST /weather/materialize` | Підготовка або запуск location-aware Bronze ingestion для weather/price context. | Materialization control; не доказ ML-переваги. |
| `GET /dashboard/baseline-lp-preview` | Deterministic LP будує feasible hourly DAM/IDM schedule, SOC projection і economics. | Preview only; no `ProposedBid`, no clearing, no live IDM bid, no dispatch. |
| `GET /dashboard/operator-recommendation` | Основний operator read model: official OREE row first, selected strategy evidence, V13 readiness, forecast/value-gap series і BUY/SELL/HOLD preview rows. | Default preview path; `market_execution_enabled=false`, no settlement, no market submission. |
| `GET /dashboard/shadow-recommendation-preview` | Manual diagnostic switch для DT/HF/TFT/DFL shadow sources поверх тих самих operator charts/tables. | Не змінює default strategy і не промотує research source. |
| Readiness/benchmark/gate endpoints | Показують rolling-origin benchmark rows, DFL/DT gates, academic MVP readiness і blocked source-readiness states. | Evidence and diagnostics only; promotion потребує окремого gate. |
| `GET /dashboard/future-stack-preview`, `GET /dashboard/decision-policy-preview` | Пояснюють forecast-stack і offline policy-preview surfaces для defense/demo. | Forecast/policy evidence, not live policy and not execution. |

Таблиця А.2. Ключові response fields та межі інтерпретації

| Field / група полів | Що означає | Як читати в тезисі |
| --- | --- | --- |
| `market_scope`, `market_venue`, `interval_minutes` | Явно задають DAM або IDM hourly planning scope. | IDM тут є hourly read-model lane, не 15-minute bid submission. |
| `target_delivery_date`, `anchor_timestamp`, `forecast_generated_at` | Фіксують delivery date, as-of момент і час forecast generation. | Потрібні для point-in-time audit і leakage-free interpretation. |
| `price_context_status` | Показує `official_published`, `pre_publication_forecast` або blocker state. | Published OREE row не переугадується ML-моделлю. |
| `recommendation_schedule`, `projected_state` / `soc_projection` | Feasible charge/discharge/hold schedule і SOC path. | Це schedule preview, а не dispatch command. |
| `bid_recommendation_preview` | Operator-facing BUY/SELL/HOLD rows із preview flags. | Назва історична; rows non-submittable і не є `ProposedBid`. |
| `decision_advisor.forecast_scenario_candidates` | Candidate schedules із forecast scenario, value/regret scoring і gatekeeper status. | Advisor може rank або abstain; він не замінює deterministic gates. |
| `v13_readiness` | Source-governance, receipt-gate і DT/LAVA readiness status. | V13 є source-readiness/acquisition gate, не modeling slice. |
| `comparison_metrics`, `boundary_labels`, `readiness_warnings` | Пояснюють shadow result, blockers, non-promotion labels і fallback reasons. | Негативний або abstained result є коректним diagnostic evidence. |
| `market_execution_enabled`, `market_order_payload_emitted`, `proposed_bid_status` | Фіксовані no-execution flags для read-model surfaces. | Очікуваний стан: `false`, `false`, `not_emitted_operator_preview`. |

Таблиця А.3. Shadow/research preview sources і promotion boundary

| `preview_source` | Роль | Boundary |
| --- | --- | --- |
| `best_valid` | Gate-passed default/fallback operator recommendation. | Promoted as read-model comparator/fallback only; no market execution. |
| `dt_shadow` | HF/local DT smoke over candidate-id або schedule-family targets. | Research shadow, not promoted. |
| `dt_direct_candidate_shadow` | Direct DT trained on candidate-index / schedule-family teacher targets. | Manual preview only. |
| `dt_v2_plus_apples_to_apples_shadow` | DT check against the real V2+ strict-row packet. | Comparator-aligned research evidence only. |
| `dt_v2_plus_distillation_shadow` | Rule-distillation DT smoke that mirrors V2+ selector targets. | Diagnostic, not default strategy. |
| `dt_decision_aware_shadow` | Decision-aware DT objective over regret/value with fallback and tail-risk guard. | Diagnostic, not autonomous controller. |
| `regret_aware_v2_plus_selector_shadow` | Value-gap selector with explicit abstention back to V2+. | Manual diagnostic preview only. |
| `dt_v2_plus_safe_switch_selector_shadow` | Corrected safe-switch shadow that searches rare non-V2+ opportunities. | Positive secondary evidence; V2+ fallback preserved. |
| `hf_live_safe_switch_shadow` | Live OREE/forecast candidate-ranking preview with LP-free deterministic templates. | Manual operator guidance only, not promoted. |
| `hf_live_safe_switch_value_aligned_shadow` | Value-aligned HF live shadow using read-only candidate-library audit. | Shadow/demo source; no production promotion and no market execution. |
| `poland_tft_shadow` | Poland/TFT challenger evidence. | Positive/near-miss evidence, not promoted for rolling robustness. |
| `dfl_diagnostics` | Candidate-value diagnostics for regret/value behavior. | Diagnostic only, not production strategy. |
| `v13_dt_lava_promoted_training` | Blocked V13/DT/LAVA roadmap state. | Blocked until source-readiness and receipt gates pass. |

Правила інтерпретації API evidence:

1. Official OREE DAM/IDM row має пріоритет для published targets; forecast-store rows використовуються тільки для unpublished або forecast-preview context.
2. Наявність endpoint-а не є доказом model quality. Якість кандидата визначається rolling-origin LP/oracle evaluation, source readiness, comparator discipline і deterministic gates.
3. Shadow preview sources не змінюють default `/dashboard/operator-recommendation`; вони потрібні для defense, diagnostics і supervised next-step design.
4. Усі read-model paths зберігають `market_execution_enabled=false`, не повертають market order payload, не створюють `ProposedBid`, settlement identifier або dispatch command.
