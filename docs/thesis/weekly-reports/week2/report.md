# Щотижневий звіт 2

## 1. Проєкт у цілому та його місце в дипломній роботі

Поточний дипломний проєкт присвячений побудові системи автономного енергоарбітражу для BESS на ринку України 2026. На практичному рівні це означає побудову керованого контуру, який поєднує збір даних, прогнозний шар, оптимізацію рішення, урахування фізичних обмежень батареї, operator-facing dashboard і відтворювану MLOps-інфраструктуру.

За форматом це інженерний диплом із вираженою дослідницькою траєкторією. Уже зараз у репозиторії є перевірюваний engineering result у вигляді Level 1 baseline, а цільовий research contribution полягає в переході до Decision-Focused Learning (DFL), де система оптимізує не лише forecast accuracy, а й quality of decision у термінах regret та економічного результату. Більш розгорнуте позиціювання проєкту наведено в [docs/thesis/chapters/01-project-overview.md](../../chapters/01-project-overview.md) і [docs/thesis/chapters/02-literature-review.md](../../chapters/02-literature-review.md).

## 2. Поточний статус: що вже реалізовано, що є demo-stage, а що лишається фінальною ціллю

На кінець другого тижня в проєкті потрібно чітко розрізняти три рівні.

Перший рівень — реалізований MVP baseline. Він включає Level 1 scope: погодинний DAM, strict similar-day forecast, LP-baseline strategy, throughput-based degradation penalty в UAH, Dagster asset graph, MLflow logging і strict Pydantic contracts.

У battery layer цей MVP коректніше описувати як feasibility-and-economics preview model, а не як повноцінну фізичну симуляцію батареї. На поточному етапі він виконує погодинний state-transition preview із SOC-обмеженнями, лімітами потужності, спрощеним ККД та throughput-based degradation penalty в UAH. Для current demo-profile цей penalty уже параметризується як public-source capex-throughput proxy: `210 USD/kWh` + `15 years` + `~1 cycle/day` + `43.9129 UAH/USD`, що дає `16,843.3 UAH/cycle` або `842.2 UAH/MWh throughput` для `10 MWh` battery. Цього достатньо для baseline optimization, regret-aware evaluation і operator-facing demo, але недостатньо для claims про full digital twin.

Другий рівень — demo-stage operator surface. На цьому тижні саме цей шар був основним фокусом. До baseline-контуру додано FastAPI control plane, backend-owned operator status, same-origin Nuxt proxy та dashboard surfaces для weather control і baseline recommendation preview.

Третій рівень — фінальна planned version. Вона включає DFL / predict-then-bid архітектуру, сильніший forecast layer на базі NBEATSx і TFT, differentiable або surrogate clearing, learned strategy layer, глибший digital twin і поступове розширення ринкового scope. Ця частина не є поточним deliverable і не повинна описуватися як уже реалізована.

## 3. Виконані завдання, досягнення та зміни за тиждень

- Підтверджено локальний tracker flow і зафіксовано його в [docs/technical/TRACKER_FLOW.md](../../../technical/TRACKER_FLOW.md).
- Реалізовано persisted operator status read model у [src/smart_arbitrage/resources/operator_status_store.py](../../../../src/smart_arbitrage/resources/operator_status_store.py) з in-memory fallback для dev-середовища.
- Розширено control-plane API в [api/main.py](../../../../api/main.py) через `GET /dashboard/operator-status`, `POST /dashboard/projected-battery-state` і `GET /dashboard/baseline-lp-preview`.
- Оновлено технічну документацію endpoint-ів у [docs/technical/API_ENDPOINTS.md](../../../technical/API_ENDPOINTS.md).
- Реалізовано dashboard Slice 1 через [dashboard/app/composables/useWeatherControls.ts](../../../../dashboard/app/composables/useWeatherControls.ts) та same-origin proxy routes у [dashboard/server/api/control-plane](../../../../dashboard/server/api/control-plane).
- Реалізовано dashboard Slice 2 через [dashboard/app/composables/useBaselinePreview.ts](../../../../dashboard/app/composables/useBaselinePreview.ts), [dashboard/app/components/dashboard/HudBaselinePreview.vue](../../../../dashboard/app/components/dashboard/HudBaselinePreview.vue) та відповідний read model proxy.
- Створено projected battery state simulator у [src/smart_arbitrage/optimization/projected_battery_state.py](../../../../src/smart_arbitrage/optimization/projected_battery_state.py).
- Підготовлено PRD та issue backlog для operator MVP slices у [docs/technical/PRD-operator-mvp-slices.md](../../../technical/PRD-operator-mvp-slices.md) і [docs/technical/issues](../../../technical/issues).
- Focused API tests для control-plane slices проходять у [tests/api/test_main.py](../../../../tests/api/test_main.py).
- Dashboard production build проходить успішно, а проект має окремий demo-boundary документ у [docs/technical/OPERATOR_DEMO_READY.md](../../../technical/OPERATOR_DEMO_READY.md).

## 4. Літературне та архітектурне обґрунтування вибраного напряму

Поточний вибір архітектури не є випадковим. Він спирається як на state of practice у побудові BESS-рішень, так і на research literature.

На рівні практичної інженерії логічно починати не з повного learned policy, а зі стабільного baseline-контуру, який можна тестувати, пояснювати і використовувати як контрольну групу. Саме тому поточний MVP побудовано навколо LP-baseline, а не навколо одразу повного DFL.

На рівні forecasting state of the art у літературі для цього проєкту важливі NBEATSx і TFT. Вони обґрунтовують, яким має бути сильніший prediction layer у наступних етапах, але не є обов’язковою стартовою умовою для першого demo-ready MVP.

На рівні decision-making центральною для диплома є логіка переходу від Predict-then-Optimize до Decision-Focused Learning. Роботи на кшталт Yi et al. та Elmachtoub & Grigas показують, що мінімізація forecast error не гарантує якість кінцевого рішення. Саме тому в проєкті важливе поняття regret і саме тому DFL зафіксовано як цільовий research layer.

На рівні battery economics важливо, що деградація в системі враховується не лише як KPI, а як складова objective. Це узгоджується з literature on degradation-aware arbitrage та пояснює, чому в current baseline economics окремо відображається degradation penalty в UAH.

Нова підбірка джерел значно підсилює цей блок. Vykhodtsev et al. допомагають обґрунтувати, чому надто грубе power-energy representation батареї є корисним для baseline-level studies, але недостатнім для повного фізичного опису активу. Hesse et al. і Maheshwari et al. показують наступний крок: ageing-aware dispatch, де деградація та ефективність вбудовуються в MILP або в наближені нелінійні постановки. Саме на цьому фоні поточний проєкт і позиціонує свою батарейну частину чесно: не як “already solved battery physics”, а як економічно осмислений preview layer для operator MVP.

Окремо ринковий блок посилюють Hu et al. та Li et al. Перші показують, що в європейському контексті чистий арбітраж часто не дає найкращої окупності без revenue stacking, а другі показують на прикладі energy plus contingency reserve bidding, чому зі зростанням складності multi-market control зростає інтерес до DRL-підходів. Для цього диплома це важливо як обґрунтування поетапності: DAM-only baseline є свідомим першим кроком, тоді як multi-venue intelligence, DRL/DFL і richer battery modeling належать до наступних етапів, а не до вже реалізованого demo-package.

Окремо потрібно зазначити роль MCP- та agent-based tooling. У поточному проєкті вони допомагають збирати research context, структурувати документацію та пришвидшувати інженерну ітерацію, але не є предметом основної наукової новизни. Центральною темою диплома лишається автономний енергоарбітраж BESS, а не сам MCP.

Поточна версія літературного розділу вже є завершеним first-pass chapter draft для подання керівнику: вона покриває forecasting, PTO/DFL, sequence modeling, differentiable optimization, degradation-aware economics і ринковий контекст, достатній для обґрунтування архітектури проєкту. На наступних етапах bibliographic base ще може розширюватися разом із новими експериментами, але цей розділ більше не є тимчасовою чернеткою.

## 5. Висновки щодо поточного demo-stage

На кінець другого тижня проєкт має не лише baseline optimization contour, а й operator-facing оболонку, через яку можна послідовно показати зв’язок між tenant selection, weather slice і baseline recommendation preview. Це важливо для пояснення дипломному керівнику, що система розвивається як керований MLOps/AI engineering продукт, а не як набір розрізнених моделей або слабо пов’язаних скриптів.

Ключовий висновок цього тижня полягає в тому, що backend-owned read models виявилися правильним кордоном між обчислювальною логікою та dashboard UX. Завдяки цьому вдалося отримати demo-ready surface без передчасного змішування operator preview з market execution semantics.

## 5.1. Статус першого демо

Станом на момент підготовки цього звіту технічний demo-package для першого supervisor walkthrough уже підготовлено. Для нього зібрано окремий сценарій у [docs/thesis/weekly-reports/week2/demo-script.md](./demo-script.md), а межі поточного demo-stage зафіксовано в [docs/technical/OPERATOR_DEMO_READY.md](../../../technical/OPERATOR_DEMO_READY.md).

Поточний звіт тому слід читати як текстовий пакет до першого демо: він фіксує, що саме вже реалізовано, які артефакти можна показати, які межі має поточний deliverable і які питання залишаються на наступний етап. Якщо live walkthrough із керівником відбудеться після відправки цього текстового звіту, матеріали нижче вже придатні як pre-demo briefing package.

## 6. Ризики та виклики

| Ризик / виклик | Чому це важливо | Запланована відповідь |
|---|---|---|
| `operator-status` для flow без запису в store може створювати зайвий control-plane noise | Це погіршує сприйняття demo-ready surface | Обробляти відсутній status record як нормальний idle-state, а не як UI-помилку |
| Slice 2 preview спирається на demo-ready data path, а не на повну market execution semantics | Для thesis narrative треба чітко відділити preview від bid / clearing / dispatch | Зберегти recommendation-preview framing у документації, UI і demo-сценарії |
| Є ризик переплутати current demo-stage з final planned version | Це методологічно небезпечно для weekly report і пояснювальної записки | У кожному матеріалі окремо позначати: що реалізовано зараз, що є demo-stage, а що лишається ціллю |
| DFL, differentiable clearing і deep battery physics ще не реалізовані | Це головний research layer, але він ще не має production-stable implementation | Тримати baseline як контрольний контур і переходити до DFL тільки після стабілізації current slices |
| У dashboard build присутні warnings про sourcemaps і chunk size | Це не блокує MVP, але залишає технічний борг | Зафіксувати як post-demo cleanup, не розширюючи scope прямо зараз |

## 7. План роботи на наступний тиждень

1. Підчистити UX-обробку відсутніх `operator-status` записів, щоб live demo не показувало зайвий warning там, де flow ще не ініційовано.
2. Завершити packaging матеріалів для supervisor demo: dashboard walkthrough, підтримуючі API artifacts, Dagster/MLflow supporting evidence.
3. Використати [docs/thesis/chapters/01-project-overview.md](../../chapters/01-project-overview.md) і [docs/thesis/chapters/02-literature-review.md](../../chapters/02-literature-review.md) як базу для пояснювальної записки та короткої презентації, окремо підсиливши narrative про feasibility-and-economics preview model, ageing-aware dispatch та європейський market context.
4. Не розширювати scope на DFL execution semantics, IDM або balancing до завершення стабілізації current demo-stage.
5. Після supervisor feedback визначити, що є пріоритетом Week 3: UX polish для operator MVP чи старт окремого DFL research slice.

## 8. Артефакти

- Загальна характеристика проєкту: [docs/thesis/chapters/01-project-overview.md](../../chapters/01-project-overview.md)
- Огляд літератури: [docs/thesis/chapters/02-literature-review.md](../../chapters/02-literature-review.md)
- Код backend: [api/main.py](../../../../api/main.py), [src/smart_arbitrage/resources/operator_status_store.py](../../../../src/smart_arbitrage/resources/operator_status_store.py), [src/smart_arbitrage/optimization/projected_battery_state.py](../../../../src/smart_arbitrage/optimization/projected_battery_state.py)
- Код dashboard: [dashboard/app/pages/index.vue](../../../../dashboard/app/pages/index.vue), [dashboard/app/components/dashboard/HudBaselinePreview.vue](../../../../dashboard/app/components/dashboard/HudBaselinePreview.vue), [dashboard/app/composables/useBaselinePreview.ts](../../../../dashboard/app/composables/useBaselinePreview.ts), [dashboard/app/composables/useWeatherControls.ts](../../../../dashboard/app/composables/useWeatherControls.ts)
- Тести: [tests/api/test_main.py](../../../../tests/api/test_main.py)
- Документація: [docs/technical/API_ENDPOINTS.md](../../../technical/API_ENDPOINTS.md), [docs/technical/TRACKER_FLOW.md](../../../technical/TRACKER_FLOW.md), [docs/technical/PRD-operator-mvp-slices.md](../../../technical/PRD-operator-mvp-slices.md), [docs/technical/OPERATOR_DEMO_READY.md](../../../technical/OPERATOR_DEMO_READY.md)
- Demo materials: [docs/thesis/weekly-reports/week2/demo-script.md](./demo-script.md)
- Літературна база: [docs/technical/papers/README.md](../../../technical/papers/README.md), [docs/technical/papers/2505.01551-predict-then-bid.pdf](../../../technical/papers/2505.01551-predict-then-bid.pdf), [docs/technical/papers/2104.05522-nbeatsx.pdf](../../../technical/papers/2104.05522-nbeatsx.pdf)
- За потреби для live demo можна використати локальні поверхні, описані в [docs/technical/OPERATOR_DEMO_READY.md](../../../technical/OPERATOR_DEMO_READY.md)

## 9. Короткий висновок

Другий тиждень завершено з готовим demo-level operator MVP для Slice 1 і Slice 2. Поточний результат уже придатний для supervisor review: він показує tenant-aware weather control flow, baseline LP recommendation preview, projected SOC та UAH economics, але водночас чітко відділяє поточний deliverable від фінальної planned version з DFL, richer forecasting та deeper battery modeling.