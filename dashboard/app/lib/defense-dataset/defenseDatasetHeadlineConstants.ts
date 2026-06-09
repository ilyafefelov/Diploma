import type {
  DashboardExperimentCard,
  DefenseRegretLadderPoint,
  OfflineStrategyPromotionHeadline,
  PolandLag24ShadowChallengerHeadline,
  TftPortfolioClosureHeadline
} from './defenseDatasetTypes'

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

export const CURRENT_POLAND_LAG24_SHADOW_CHALLENGER: PolandLag24ShadowChallengerHeadline = {
  label: 'Poland lag-24 TFT shadow challenger',
  latestHoldoutImprovementVsV2Plus: 0.0316,
  latestHoldoutMeanRegretUah: 169.24,
  frozenV2PlusMeanRegretUah: 174.77,
  passingFeatureCount: 17,
  featureCount: 24,
  blockedFeatureCount: 7,
  rollingPassCount: 1,
  rollingWindowCount: 4,
  status: 'positive_not_promoted',
  interpretation: 'Poland/TFT improved the latest holdout by 3.16%, but rolling robustness was only 1/4 versus frozen V2+, so it remains a shadow challenger.'
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
    label: 'Poland shadow',
    value: '+3.16% latest',
    meta: '17/24 features consumed; 1/4 rolling vs frozen V2+; not promoted',
    status: 'shadow'
  },
  {
    label: 'Next branch',
    value: 'feature/value first',
    meta: 'repair Poland coverage, richer causal features, then DT/LAVA teacher labels',
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
