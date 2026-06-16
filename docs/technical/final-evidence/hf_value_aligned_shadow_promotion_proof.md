# HF Value-Aligned Shadow Promotion Proof

Source local artifact:

- `data/research_runs/hf_live_safe_switch_value_aligned_shadow_promotion_proof_2026_05_01_2026_06_01/promotion_gate.md`

## Result

| Field | Value |
| --- | ---: |
| Promotion scope | `value_aligned_shadow_candidate_library` |
| Shadow promotion gate passed | `true` |
| Source-backed days | `32` |
| Non-fallback switch rate | `0.625` |
| Selected value improvement vs default | `645.39` UAH |
| Value gap ratio vs default | `0.3975` |
| Tail-failure delta vs default | `-16` |
| Safety failures | `0` |
| HF frozen mean regret | `158.71` UAH |
| V2+ baseline mean regret | `174.77` UAH |

Gate checks passed for source-backed multi-day coverage, non-fallback switch
rate, selected value improvement, value-gap ratio, tail-risk control, zero
safety failures, robustness, and frozen regret vs V2+.

Boundary: shadow/demo promotion only. No `ProposedBid`, no market order payload,
and no production market promotion.

