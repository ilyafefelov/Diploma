import type {
  DefensePortfolioDiagnosticPoint,
  DefenseTftSafeSelectionExplanation,
  DefenseTftUseDecision,
  DefenseV2PlusImprovementPoint
} from './defenseDatasetTypes'

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
    value: 'teacher signal',
    body: 'Use TFT-like wins only as teacher/context signal. V10 showed that copied safe-looking templates can become tail-risk on final holdout, so any future selector or DT/LAVA branch must first pass the learning-ceiling audit.',
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
    englishBody: 'Next work is not to copy the winning TFT rows directly. The V10 transfer audit now checks whether safe-looking templates stay non-tail-risk before DT/LAVA or another selector is allowed to learn from them.',
    ukrainianBody: 'Наступний крок не в тому, щоб прямо копіювати TFT rows, які виграли post-hoc. V10 transfer audit тепер перевіряє, чи safe-looking templates залишаються non-tail-risk, перш ніж DT/LAVA або інший selector зможе на них навчатися.',
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
