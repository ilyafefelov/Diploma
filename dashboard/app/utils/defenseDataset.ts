import type {
  DecisionPolicyPreviewResponse,
  DecisionTransformerTrajectoryResponse,
  DflRelaxedPilotResponse,
  DflScheduleValueProductionGateResponse,
  RealDataBenchmarkResponse,
  SimulatedLiveTradingResponse
} from '../types/control-plane'

export interface DefenseBenchmarkSummary {
  tenantId: string
  marketVenue: string
  generatedAt: string
  dataQualityTier: string
  anchorCount: number
  modelCount: number
  bestModelName: string | null
  meanRegretUah: number
  medianRegretUah: number
  sourceMode: 'fastapi_live'
}

export interface DefenseModelRow {
  modelName: string
  role: 'control' | 'forecast_candidate' | 'ensemble_gate'
  anchorCount: number
  meanRegretUah: number
  medianRegretUah: number
  meanDecisionValueUah: number
  meanOracleValueUah: number
  winRate: number
  meanThroughputMwh: number
}

export interface ResearchReadinessRow {
  label: 'DFL' | 'Decision Transformer' | 'DT policy preview' | 'Paper trading'
  status: string
  metric: string
  boundary: string
}

export interface OfflineStrategyPromotionHeadline {
  modelName: string
  meanRegretUah: number
  strictMeanRegretUah: number
  improvementVsStrict: number
  rollingPassCount: number
  rollingWindowCount: number
  marketExecutionEnabled: boolean
  claimBoundary: string
}

export interface TftPortfolioClosureHeadline {
  label: string
  latestTenantAnchors: number
  tftBetterCandidateCount: number
  selectorFallbackCount: number
  rollingPassCount: number
  rollingWindowCount: number
  candidatePortfolioRows: number
  status: 'negative_evidence'
  interpretation: string
}

export interface DashboardExperimentCard {
  label: string
  value: string
  meta: string
  status: 'headline' | 'closed' | 'blocked' | 'next'
}

export interface DefenseRegretLadderPoint {
  label: string
  meanRegretUah: number
  note: string
  status: 'control' | 'improved' | 'headline' | 'plateau' | 'failed'
}

export interface DefensePortfolioDiagnosticPoint {
  label: string
  numerator: number
  denominator: number
  note: string
  status: 'opportunity' | 'fallback' | 'blocked'
}

export interface DefenseTftUseDecision {
  label: string
  value: string
  body: string
  status: 'useful' | 'blocked' | 'next'
}

export interface DefenseTftSafeSelectionExplanation {
  label: string
  englishTitle: string
  ukrainianTitle: string
  englishBody: string
  ukrainianBody: string
  status: 'opportunity' | 'leakage' | 'diagnosis' | 'next'
}

export interface DefenseV2PlusImprovementPoint {
  label: string
  value: string
  englishBody: string
  ukrainianBody: string
  status: 'candidate_space' | 'fallback' | 'scoring' | 'boundary'
}

export interface DefenseDtLavaPlanStep {
  label: string
  value: string
  body: string
  status: 'input' | 'model' | 'gate' | 'boundary'
}

export interface DefenseBilingualExplainerSection {
  label: string
  englishTitle: string
  englishBody: string
  englishBullets: string[]
  ukrainianTitle: string
  ukrainianBody: string
  ukrainianBullets: string[]
}

export const CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE: OfflineStrategyPromotionHeadline = {
  modelName: 'Schedule/Value Learner V2+',
  meanRegretUah: 174.77,
  strictMeanRegretUah: 310.58,
  improvementVsStrict: 0.4373,
  rollingPassCount: 4,
  rollingWindowCount: 4,
  marketExecutionEnabled: false,
  claimBoundary: 'Offline Strategy Promotion evidence only'
}

export const CURRENT_TFT_PORTFOLIO_CLOSURE: TftPortfolioClosureHeadline = {
  label: 'NBEATSx+TFT candidate portfolio',
  latestTenantAnchors: 90,
  tftBetterCandidateCount: 24,
  selectorFallbackCount: 90,
  rollingPassCount: 0,
  rollingWindowCount: 4,
  candidatePortfolioRows: 120380,
  status: 'negative_evidence',
  interpretation: 'TFT offered local candidate diversity, but the prior-only selector could not exploit it robustly.'
}

export const CURRENT_DASHBOARD_EXPERIMENTS: DashboardExperimentCard[] = [
  {
    label: 'Headline',
    value: 'V2+',
    meta: '174.77 UAH mean regret / 4 of 4 rolling windows',
    status: 'headline'
  },
  {
    label: 'TFT portfolio',
    value: '0/4 rolling',
    meta: '24/90 local TFT opportunities, selector fell back on latest holdout',
    status: 'closed'
  },
  {
    label: 'Market coupling',
    value: 'blocked',
    meta: 'ENTSO-E/Poland remains governance-only, not training input',
    status: 'blocked'
  },
  {
    label: 'Next branch',
    value: 'DT/LAVA',
    meta: 'candidate/value or schedule-neighbor supervision against V2+',
    status: 'next'
  }
]

export const CURRENT_REGRET_LADDER: DefenseRegretLadderPoint[] = [
  {
    label: 'strict_similar_day',
    meanRegretUah: 310.58,
    note: 'Frozen control comparator',
    status: 'control'
  },
  {
    label: 'Frozen V2',
    meanRegretUah: 206.37,
    note: 'First schedule/value promotion evidence',
    status: 'improved'
  },
  {
    label: 'Raw V2+',
    meanRegretUah: 193.36,
    note: 'Official global-panel NBEATSx, uncalibrated source',
    status: 'improved'
  },
  {
    label: 'Calibrated V2+',
    meanRegretUah: 174.77,
    note: 'Prior-only forecast correction by horizon; current thesis headline, 4/4 rolling windows',
    status: 'headline'
  },
  {
    label: 'V3/V4/V5',
    meanRegretUah: 174.77,
    note: 'Matched V2+ through fallback; no replacement claim',
    status: 'plateau'
  },
  {
    label: 'Official bridge DFL/DT',
    meanRegretUah: 367.70,
    note: 'Negative evidence versus V2+',
    status: 'failed'
  }
]

export const CURRENT_TFT_PORTFOLIO_DIAGNOSTICS: DefensePortfolioDiagnosticPoint[] = [
  {
    label: 'TFT local opportunities',
    numerator: 24,
    denominator: 90,
    note: 'TFT had a better candidate on some latest tenant-anchors.',
    status: 'opportunity'
  },
  {
    label: 'Selector fallback',
    numerator: 90,
    denominator: 90,
    note: 'Prior-only selector stayed with V2+ on the latest holdout.',
    status: 'fallback'
  },
  {
    label: 'Rolling robustness',
    numerator: 0,
    denominator: 4,
    note: 'Portfolio did not pass the rolling strict replay gate.',
    status: 'blocked'
  }
]

export const CURRENT_TFT_USE_DECISION: DefenseTftUseDecision[] = [
  {
    label: 'What worked',
    value: '24 / 90',
    body: 'TFT produced candidate schedules that beat V2+ on some latest tenant-anchor rows after strict LP/oracle scoring.',
    status: 'useful'
  },
  {
    label: 'Why not use now',
    value: '90 / 90 fallback',
    body: 'Before realized prices were known, the prior-only selector could not identify those wins safely, so it chose V2+ for every latest-holdout row.',
    status: 'blocked'
  },
  {
    label: 'Promotion blocker',
    value: '0 / 4 rolling',
    body: 'Using the 24 post-hoc winners directly would leak final-holdout information. TFT must pass rolling robustness before it becomes a selected strategy.',
    status: 'blocked'
  },
  {
    label: 'How to make TFT usable',
    value: 'future branch',
    body: 'Train a stronger prior-only portfolio selector or DT/LAVA schedule-neighbor model that predicts when TFT risk schedules beat V2+, then re-score through the same gate.',
    status: 'next'
  }
]

export const CURRENT_TFT_SAFE_SELECTION_EXPLAINER: DefenseTftSafeSelectionExplanation[] = [
  {
    label: '24 good schedules',
    englishTitle: 'They were post-hoc winners',
    ukrainianTitle: 'Це були post-hoc переможці',
    englishBody: 'The 24 TFT schedules were identified after realized prices were scored. That proves TFT has useful schedule diversity, but it does not prove we knew which TFT rows would win before the validation window started.',
    ukrainianBody: '24 TFT-розклади знайшлися після того, як уже були відомі realized prices і strict LP/oracle scoring. Це доводить, що TFT має корисну schedule diversity, але не доводить, що ми могли знати ці перемоги до початку validation window.',
    status: 'opportunity'
  },
  {
    label: 'Why not select them',
    englishTitle: 'Directly using them would leak the answer',
    ukrainianTitle: 'Прямо взяти їх означало б підглянути відповідь',
    englishBody: 'If we picked those 24 rows because they won after scoring, the selector would be using final-holdout information. That is leakage, so the result would not be thesis-safe or deployable.',
    ukrainianBody: 'Якщо вибрати ці 24 rows саме тому, що вони виграли після scoring, selector використає final-holdout information. Це leakage, тому такий результат не буде thesis-safe і не буде придатний для deployment.',
    status: 'leakage'
  },
  {
    label: 'Prior-only selector',
    englishTitle: 'The selector had to decide before the window',
    ukrainianTitle: 'Selector мав вирішити до початку вікна',
    englishBody: 'Prior-only means it may use only features available before the target hours: forecast disagreement, quantile spread, schedule distance, calendar/weather/load context, and prior rolling evidence. It cannot use realized prices or final regret labels from the same holdout window.',
    ukrainianBody: 'Prior-only означає, що можна використовувати тільки features, доступні до target hours: forecast disagreement, quantile spread, schedule distance, calendar/weather/load context і prior rolling evidence. Не можна використовувати realized prices або final regret labels з цього ж holdout window.',
    status: 'diagnosis'
  },
  {
    label: 'Why it failed',
    englishTitle: 'The prior signal was not reliable enough',
    ukrainianTitle: 'Prior-сигнал був недостатньо надійний',
    englishBody: 'The current prior features could not separate the few TFT-winning regimes from the many cases where V2+ stayed safer. The conservative fallback therefore chose V2+ on 90/90 latest rows and the portfolio failed 0/4 rolling windows.',
    ukrainianBody: 'Поточні prior features не змогли відрізнити рідкі TFT-winning regimes від багатьох випадків, де V2+ був безпечнішим. Тому conservative fallback вибрав V2+ у 90/90 latest rows, а portfolio отримав 0/4 rolling windows.',
    status: 'diagnosis'
  },
  {
    label: 'What next',
    englishTitle: 'Use TFT as a teacher signal, not a shortcut',
    ukrainianTitle: 'TFT треба використати як teacher signal, не shortcut',
    englishBody: 'Next work is to build stronger prior context and candidate/value or DT/LAVA schedule-neighbor supervision that learns when TFT-like risk schedules help. Promotion still requires beating V2+ before the fact under the same strict gate.',
    ukrainianBody: 'Наступний крок: сильніший prior context і candidate/value або DT/LAVA schedule-neighbor supervision, який навчиться передбачати, коли TFT-like risk schedules допомагають. Promotion можливий тільки якщо кандидат beat V2+ before the fact under the same strict gate.',
    status: 'next'
  }
]

export const CURRENT_V2_PLUS_IMPROVEMENT_STORY: DefenseV2PlusImprovementPoint[] = [
  {
    label: 'Frozen V2 to V2+',
    value: '206.37 -> 174.77 UAH',
    englishBody: 'The calibrated lane improved by 31.60 UAH mean regret, or 15.31% versus frozen V2, while keeping the same strict LP/oracle judge.',
    ukrainianBody: 'Calibrated lane покращив mean regret на 31.60 UAH, або 15.31% проти frozen V2, з тим самим strict LP/oracle evaluator.',
    status: 'scoring'
  },
  {
    label: 'Better schedule map',
    value: '5 new families',
    englishBody: 'V2+ adds rank-extrema, robust-spread, strict-neighborhood, temporal-block, and terminal-SOC candidates around the actual failure modes.',
    ukrainianBody: 'V2+ додає rank-extrema, robust-spread, strict-neighborhood, temporal-block і terminal-SOC candidates навколо реальних failure modes.',
    status: 'candidate_space'
  },
  {
    label: 'Safe fallback',
    value: 'V2 remains guard',
    englishBody: 'A new candidate can replace frozen V2 only when prior/train anchors predict non-degrading improvement. Otherwise V2+ falls back to V2.',
    ukrainianBody: 'Новий candidate може замінити frozen V2 тільки коли prior/train anchors прогнозують non-degrading improvement. Інакше V2+ повертається до V2.',
    status: 'fallback'
  },
  {
    label: 'What it proves',
    value: 'decision layer won',
    englishBody: 'The result is not a raw-forecast-superiority claim. It shows that forecast/context signals become valuable after calibration, candidate generation, and strict value scoring.',
    ukrainianBody: 'Це не claim, що raw forecast сам по собі кращий. Результат показує, що forecast/context signals стають корисними після calibration, candidate generation і strict value scoring.',
    status: 'boundary'
  }
]

export const CURRENT_DT_LAVA_NEXT_STEPS: DefenseDtLavaPlanStep[] = [
  {
    label: 'Teacher data',
    value: 'V2+ + oracle schedules',
    body: 'Train only on prior/train anchors with V2+, oracle/high-value schedules, and candidate value labels. Final holdout stays scoring-only.',
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
    englishBody: 'The next DT/LAVA branch should not emit raw hourly BUY/SELL/HOLD first. It should learn from V2+, oracle, and high-value feasible schedules, condition on return-to-go, and predict schedule family, schedule block, or candidate index. LAVA-style work uses precomputed LP-feasible neighbors and value labels so the learner sees decision quality without live solver calls inside training.',
    englishBullets: [
      'Teacher data: V2+ schedules, oracle schedules, and high-value candidate schedules from prior/train anchors.',
      'Model target: candidate index or schedule block first; raw actions only after the candidate layer works.',
      'Promotion rule: beat 174.77 UAH mean regret, no median degradation, rolling robustness preserved.'
    ],
    ukrainianTitle: 'Як V2+ стає teacher для DT/LAVA',
    ukrainianBody: 'Наступна DT/LAVA гілка не повинна одразу генерувати raw hourly BUY/SELL/HOLD. Вона має вчитися на V2+, oracle і high-value feasible schedules, conditioning on return-to-go, і прогнозувати schedule family, schedule block або candidate index. LAVA-style робота використовує precomputed LP-feasible neighbors і value labels, щоб модель бачила decision quality без live solver calls усередині training.',
    ukrainianBullets: [
      'Teacher data: V2+ schedules, oracle schedules і high-value candidate schedules з prior/train anchors.',
      'Model target: candidate index або schedule block спочатку; raw actions тільки після робочого candidate layer.',
      'Promotion rule: beat 174.77 UAH mean regret, без median degradation, rolling robustness preserved.'
    ]
  }
]

export const summarizeDefenseBenchmark = (
  response: RealDataBenchmarkResponse
): DefenseBenchmarkSummary => ({
  tenantId: response.tenant_id,
  marketVenue: response.market_venue,
  generatedAt: response.generated_at,
  dataQualityTier: response.data_quality_tier,
  anchorCount: response.anchor_count,
  modelCount: response.model_count,
  bestModelName: response.best_model_name,
  meanRegretUah: response.mean_regret_uah,
  medianRegretUah: response.median_regret_uah,
  sourceMode: 'fastapi_live'
})

export const buildDefenseModelRows = (
  benchmark: RealDataBenchmarkResponse,
  extraBenchmarks: RealDataBenchmarkResponse[] = []
): DefenseModelRow[] => {
  const allRows = [benchmark, ...extraBenchmarks].flatMap(response => response.rows)
  const modelNames = Array.from(new Set(allRows.map(row => row.forecast_model_name)))

  return modelNames
    .map((modelName) => {
      const rows = allRows.filter(row => row.forecast_model_name === modelName)
      const regrets = rows.map(row => row.regret_uah)

      return {
        modelName,
        role: modelRole(modelName),
        anchorCount: new Set(rows.map(row => row.anchor_timestamp)).size,
        meanRegretUah: mean(regrets),
        medianRegretUah: median(regrets),
        meanDecisionValueUah: mean(rows.map(row => row.decision_value_uah)),
        meanOracleValueUah: mean(rows.map(row => row.oracle_value_uah)),
        winRate: rows.length === 0
          ? 0
          : rows.filter(row => row.rank_by_regret === 1).length / rows.length,
        meanThroughputMwh: mean(rows.map(row => row.total_throughput_mwh))
      }
    })
    .sort((left, right) => modelSortRank(left.modelName) - modelSortRank(right.modelName))
}

export const buildResearchReadinessRows = (input: {
  dfl: DflRelaxedPilotResponse | null
  dt: DecisionTransformerTrajectoryResponse | null
  dtPolicy: DecisionPolicyPreviewResponse | null
  live: SimulatedLiveTradingResponse | null
}): ResearchReadinessRow[] => [
  {
    label: 'DFL',
    status: input.dfl && input.dfl.row_count > 0 ? 'pilot' : 'not materialized',
    metric: input.dfl ? `${formatCompactNumber(input.dfl.mean_relaxed_regret_uah)} UAH relaxed regret` : 'no rows',
    boundary: 'not full DFL'
  },
  {
    label: 'Decision Transformer',
    status: input.dt && input.dt.row_count > 0 ? 'trajectory data' : 'not materialized',
    metric: input.dt ? `${input.dt.episode_count} episodes / ${input.dt.row_count} rows` : 'no rows',
    boundary: 'not live policy'
  },
  {
    label: 'DT policy preview',
    status: input.dtPolicy && input.dtPolicy.row_count > 0 ? input.dtPolicy.policy_readiness : 'not materialized',
    metric: input.dtPolicy
      ? `${formatCompactNumber(input.dtPolicy.mean_value_gap_uah)} UAH mean value gap / ${formatPercent(input.dtPolicy.forecast_context_coverage_ratio)} forecast-conditioned`
      : 'no rows',
    boundary: input.dtPolicy?.market_execution_enabled ? 'market execution enabled' : 'preview only'
  },
  {
    label: 'Paper trading',
    status: input.live?.simulated_only ? 'simulated only' : 'not materialized',
    metric: input.live ? `${input.live.row_count} rows` : 'no rows',
    boundary: 'not market execution'
  }
]

export const summarizeScheduleValuePromotionReadModel = (
  response: DflScheduleValueProductionGateResponse | null
): string => {
  if (!response) {
    return 'backend read model pending'
  }

  return `${response.production_promote_count}/${response.row_count} offline rows passed, market execution ${response.market_execution_enabled ? 'enabled' : 'disabled'}`
}

export const formatCompactNumber = (value: number): string => {
  if (Math.abs(value) >= 1000) {
    return Math.round(value).toLocaleString('en-GB')
  }

  return Number.isInteger(value) ? `${value}` : value.toFixed(1)
}

export const formatUah = (value: number): string => `${Math.round(value).toLocaleString('en-GB')} UAH`

export const formatPercent = (value: number): string => `${Math.round(value * 100)}%`

const modelRole = (modelName: string): DefenseModelRow['role'] => {
  if (modelName === 'strict_similar_day') {
    return 'control'
  }

  if (modelName.includes('gate') || modelName.includes('ensemble')) {
    return 'ensemble_gate'
  }

  return 'forecast_candidate'
}

const modelSortRank = (modelName: string): number => {
  if (modelName === 'strict_similar_day') {
    return 0
  }

  if (modelName.includes('tft')) {
    return 1
  }

  if (modelName.includes('nbeatsx')) {
    return 2
  }

  if (modelName.includes('gate') || modelName.includes('ensemble')) {
    return 3
  }

  return 4
}

const mean = (values: number[]): number => {
  if (values.length === 0) {
    return 0
  }

  return values.reduce((total, value) => total + value, 0) / values.length
}

const median = (values: number[]): number => {
  if (values.length === 0) {
    return 0
  }

  const sortedValues = [...values].sort((left, right) => left - right)
  const midpoint = Math.floor(sortedValues.length / 2)

  if (sortedValues.length % 2 === 1) {
    return sortedValues[midpoint] ?? 0
  }

  return ((sortedValues[midpoint - 1] ?? 0) + (sortedValues[midpoint] ?? 0)) / 2
}
