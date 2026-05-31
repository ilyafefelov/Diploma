import type {
  DefenseBilingualExplainerSection,
  DefenseDtLavaPlanStep
} from './defenseDatasetTypes'

export const CURRENT_DT_LAVA_NEXT_STEPS: DefenseDtLavaPlanStep[] = [
  {
    label: 'Teacher data',
    value: 'blocked by V10 gate',
    body: 'Train only after V10/V-next produces prior-supported, non-tail-risk material safe-switch labels. The latest V10 gate had 0 final safe switches and 126/126 final generated rows marked tail-risk, so final holdout stays scoring-only.',
    status: 'input'
  },
  {
    label: 'Prediction target',
    value: 'schedule block / candidate',
    body: 'Predict schedule family, schedule block, or candidate index first. Do not start by emitting raw hourly BUY/SELL/HOLD commands.',
    status: 'model'
  },
  {
    label: 'LAVA-style layer',
    value: 'feasible neighbors',
    body: 'Use precomputed LP-feasible schedule neighbors and value labels, so the model learns decision quality without live solver calls inside training.',
    status: 'model'
  },
  {
    label: 'Promotion gate',
    value: 'beat V2+',
    body: 'A challenger must beat 174.77 UAH mean regret, avoid median degradation, preserve rolling robustness, and keep market_execution_enabled=false.',
    status: 'gate'
  }
]

export const CURRENT_BILINGUAL_STRATEGY_EXPLAINER: DefenseBilingualExplainerSection[] = [
  {
    label: 'Offline vs online',
    englishTitle: 'Offline evidence is not live execution',
    englishBody: 'Offline strategy means the model is replayed on historical rolling windows using only information that was known before each window. Online strategy means using the latest forecast, SOC, telemetry, and safety checks to create a current operator preview. V2+ is currently promoted only as offline/read-model evidence, not automatic market bidding.',
    englishBullets: [
      'Offline: historical anchors, prior-known inputs, strict LP/oracle regret scoring.',
      'Online preview: latest forecast + battery state + tenant limits, still no exchange submission.',
      'Execution boundary: market_execution_enabled=false until a separate live/paper-trading gate exists.'
    ],
    ukrainianTitle: 'Офлайн-доказ не є live-виконанням',
    ukrainianBody: 'Офлайн-стратегія означає, що модель перевіряється на історичних rolling-вікнах і використовує тільки дані, які були відомі до початку кожного вікна. Онлайн-стратегія означає роботу з актуальним прогнозом, SOC, телеметрією та safety checks для operator preview. V2+ зараз є тільки offline/read-model evidence, не автоматичною біржовою заявкою.',
    ukrainianBullets: [
      'Офлайн: історичні anchors, prior-known inputs, strict LP/oracle regret scoring.',
      'Онлайн-preview: latest forecast + battery state + tenant limits, без відправки на біржу.',
      'Межа виконання: market_execution_enabled=false до окремого live/paper-trading gate.'
    ]
  },
  {
    label: 'V2+ pipeline',
    englishTitle: 'How the winning V2+ pipeline works',
    englishBody: 'The winning path starts with Ukrainian DAM prices, Open-Meteo/weather context, and tenant battery/load configuration. Dagster materializes Bronze/Silver feature assets, official global-panel NBEATSx forecasts prices across five tenants, prior-only horizon calibration corrects systematic forecast bias, and the schedule candidate library creates feasible battery schedules. V2+ selects schedules by expected decision value, then the same strict LP/oracle scorer measures regret.',
    englishBullets: [
      'Sources: OREE DAM, Open-Meteo/weather, tenant configuration and load context.',
      'Storage: Dagster assets, Postgres evidence rows, and local research packet exports.',
      'Winning metric: calibrated V2+ mean regret 174.77 UAH vs strict 310.58 UAH, 4/4 rolling windows.'
    ],
    ukrainianTitle: 'Як працює виграшний V2+ pipeline',
    ukrainianBody: 'Виграшний шлях починається з українських DAM-цін, Open-Meteo/weather context і tenant battery/load configuration. Dagster матеріалізує Bronze/Silver feature assets, official global-panel NBEATSx прогнозує ціни для пʼяти tenants, prior-only horizon calibration виправляє системний forecast bias, а schedule candidate library створює feasible battery schedules. V2+ вибирає schedule за очікуваною decision value, після чого той самий strict LP/oracle scorer рахує regret.',
    ukrainianBullets: [
      'Джерела: OREE DAM, Open-Meteo/weather, tenant configuration and load context.',
      'Зберігання: Dagster assets, Postgres evidence rows і локальні research packet exports.',
      'Виграшна метрика: calibrated V2+ mean regret 174.77 UAH проти strict 310.58 UAH, 4/4 rolling windows.'
    ]
  },
  {
    label: 'Governance to recommendation',
    englishTitle: 'How evidence becomes a safe recommendation',
    englishBody: 'Every feature route must pass source, timestamp, leakage, timezone, unit, licensing, and domain-shift checks before it can enter training. Approved Ukrainian-only evidence is exposed through FastAPI read models and rendered in Nuxt as defense/operator preview. The dashboard may show a V2+-style schedule recommendation, but it remains a preview candidate until a separate execution gate exists.',
    englishBullets: [
      'European market-coupling rows remain governance-only; they are not in the current V2+ training result.',
      'Recommendations are schedule previews: charge, discharge, hold, SOC feasibility, and value context.',
      'The Pydantic/feasibility gatekeeper stays between any model and physical/market action.'
    ],
    ukrainianTitle: 'Як evidence стає безпечною рекомендацією',
    ukrainianBody: 'Кожен feature route має пройти source, timestamp, leakage, timezone, unit, licensing і domain-shift checks перед тим, як потрапити в training. Approved Ukrainian-only evidence читається через FastAPI read models і показується в Nuxt як defense/operator preview. Dashboard може показувати V2+-style schedule recommendation, але це все ще preview candidate до окремого execution gate.',
    ukrainianBullets: [
      'European market-coupling rows залишаються governance-only; вони не входять у поточний V2+ training result.',
      'Recommendations є schedule previews: charge, discharge, hold, SOC feasibility і value context.',
      'Pydantic/feasibility gatekeeper залишається між будь-якою моделлю та фізичною/ринковою дією.'
    ]
  },
  {
    label: 'Path to DT/LAVA',
    englishTitle: 'How V2+ becomes the teacher for DT/LAVA',
    englishBody: 'The next DT/LAVA branch should not emit raw hourly BUY/SELL/HOLD first, and it should not start until the V10 learning-ceiling gate finds transferable non-tail-risk teacher labels. When that exists, it should learn from V2+, oracle, and high-value feasible schedules, condition on return-to-go, and predict schedule family, schedule block, or candidate index. LAVA-style work uses precomputed LP-feasible neighbors and value labels so the learner sees decision quality without live solver calls inside training.',
    englishBullets: [
      'Teacher data: V2+ schedules, oracle schedules, and high-value candidate schedules from prior/train anchors after the V10 transfer audit allows them.',
      'Model target: candidate index or schedule block first; raw actions only after the candidate layer works.',
      'Promotion rule: beat 174.77 UAH mean regret, no median degradation, rolling robustness preserved.'
    ],
    ukrainianTitle: 'Як V2+ стає teacher для DT/LAVA',
    ukrainianBody: 'Наступна DT/LAVA гілка не повинна одразу генерувати raw hourly BUY/SELL/HOLD і не повинна стартувати, поки V10 learning-ceiling gate не знайде transferable non-tail-risk teacher labels. Коли вони є, модель має вчитися на V2+, oracle і high-value feasible schedules, conditioning on return-to-go, і прогнозувати schedule family, schedule block або candidate index. LAVA-style робота використовує precomputed LP-feasible neighbors і value labels, щоб модель бачила decision quality без live solver calls усередині training.',
    ukrainianBullets: [
      'Teacher data: V2+ schedules, oracle schedules і high-value candidate schedules з prior/train anchors після V10 transfer audit.',
      'Model target: candidate index або schedule block спочатку; raw actions тільки після робочого candidate layer.',
      'Promotion rule: beat 174.77 UAH mean regret, без median degradation, rolling robustness preserved.'
    ]
  }
]
