# HF Safe-Switch Robustness Summary

Source local artifact:

- `data/research_runs/week5_hf_safe_switch_scorer_robustness_2026_06_01/robustness_summary.json`

## Result

| Field | Value |
| --- | ---: |
| Claim scope | `hf_safe_switch_scorer_robustness_shadow_not_promotable_not_market_execution` |
| Seed count | `5` |
| Selected threshold | `100.0` UAH |
| Mean HF regret | `158.7121` UAH |
| Median HF regret | `158.4416` UAH |
| V2+ baseline mean regret | `174.77` UAH |
| Canonical DT/V2+ safe-switch mean regret | `168.1566` UAH |
| Mean HF minus V2+ | `-16.0579` UAH |
| Robustness gate passed | `true` |
| Promotion gate passed | `false` |
| Market execution enabled | `false` |

Interpretation: HF value-aligned shadow is a strong manual shadow/demo
challenger, not a production controller.

