"""Canonical offline experiment metrics and pass/fail aggregation.

This module standardizes thesis-facing offline experiment metrics without
promoting any strategy to live market execution.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import json
import math
from pathlib import Path
from statistics import mean, pstdev
from typing import Any, Final

BASELINE_V2_PLUS_MEAN_REGRET_UAH: Final[float] = 174.77
PRIMARY_SUCCESS_MEAN_REGRET_UAH: Final[float] = 166.0
SECONDARY_SUCCESS_MEAN_REGRET_UAH: Final[float] = 178.26
PRIMARY_SUCCESS_ALPHA: Final[float] = 0.05
DEFAULT_REQUIRED_SEED_COUNT: Final[int] = 3
CANONICAL_EXPERIMENT_CLAIM_SCOPE: Final[str] = (
    "canonical_offline_experiment_metrics_not_market_execution"
)

_REQUIRED_STRING_FIELDS: Final[tuple[str, ...]] = (
    "run_id",
    "model",
    "git_commit",
    "cmd",
)
_REQUIRED_INT_FIELDS: Final[tuple[str, ...]] = (
    "seed",
    "epoch",
    "early_stop_epoch",
)
_REQUIRED_FLOAT_FIELDS: Final[tuple[str, ...]] = (
    "val_regret_mean",
    "val_regret_std",
    "test_regret_mean",
    "test_regret_std",
    "wall_time_s",
    "gpu_hours",
    "max_mem_gb",
)
_NON_NEGATIVE_FLOAT_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "val_regret_std",
        "test_regret_std",
        "wall_time_s",
        "gpu_hours",
        "max_mem_gb",
    }
)
_MODEL_LABEL_PREFIX_BY_ESTIMATOR: Final[dict[str, str]] = {
    "weighted_ridge": "weighted_ridge",
    "hist_gradient_boosting": "hist_gradient_boosting",
    "random_forest": "random_forest",
}


def validate_canonical_experiment_metrics_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate and normalize one canonical metrics payload."""

    required_fields = (
        set(_REQUIRED_STRING_FIELDS)
        | set(_REQUIRED_INT_FIELDS)
        | set(_REQUIRED_FLOAT_FIELDS)
    )
    missing = sorted(required_fields - set(payload))
    if missing:
        raise ValueError(f"canonical metrics payload missing required fields: {', '.join(missing)}")

    normalized: dict[str, Any] = {}
    for field_name in _REQUIRED_STRING_FIELDS:
        normalized[field_name] = _required_str(payload, field_name)
    for field_name in _REQUIRED_INT_FIELDS:
        int_value = _required_int(payload, field_name)
        if int_value < 0:
            raise ValueError(f"{field_name} must be non-negative.")
        normalized[field_name] = int_value
    for field_name in _REQUIRED_FLOAT_FIELDS:
        float_value = _required_float(payload, field_name)
        if field_name in _NON_NEGATIVE_FLOAT_FIELDS and float_value < 0.0:
            raise ValueError(f"{field_name} must be non-negative.")
        normalized[field_name] = float_value

    if "claim_scope" in payload:
        claim_scope = _required_str(payload, "claim_scope")
        if "not_market_execution" not in claim_scope:
            raise ValueError("claim_scope must include not_market_execution.")
        normalized["claim_scope"] = claim_scope
    else:
        normalized["claim_scope"] = CANONICAL_EXPERIMENT_CLAIM_SCOPE
    if "estimator_class" in payload:
        estimator_class = _required_str(payload, "estimator_class")
        _validate_model_estimator_lineage(
            model=str(normalized["model"]),
            estimator_class=estimator_class,
        )
        normalized["estimator_class"] = estimator_class
    if "artifact_identifier" in payload:
        normalized["artifact_identifier"] = _required_str(
            payload,
            "artifact_identifier",
        )
    if "independent_holdout" in payload:
        normalized["independent_holdout"] = _required_bool(
            payload,
            "independent_holdout",
        )
    if "evaluation_content_overlap_ratio" in payload:
        overlap_ratio = _required_float(payload, "evaluation_content_overlap_ratio")
        if not 0.0 <= overlap_ratio <= 1.0:
            raise ValueError("evaluation_content_overlap_ratio must be between 0 and 1.")
        normalized["evaluation_content_overlap_ratio"] = overlap_ratio
    normalized["market_execution_enabled"] = False
    normalized["promotion_gate_passed"] = False
    return normalized


def load_canonical_metrics_file(path: Path) -> list[dict[str, Any]]:
    """Load a metrics file containing one object, an array, or JSON lines."""

    if not path.exists():
        raise FileNotFoundError(path)
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        raise ValueError(f"canonical metrics file is empty: {path}")

    raw_payloads: list[Any]
    if text[0] in "[{":
        parsed = json.loads(text)
        raw_payloads = parsed if isinstance(parsed, list) else [parsed]
    else:
        raw_payloads = [json.loads(line) for line in text.splitlines() if line.strip()]

    normalized: list[dict[str, Any]] = []
    for payload in raw_payloads:
        if not isinstance(payload, Mapping):
            raise ValueError(f"canonical metrics record must be an object: {path}")
        normalized.append(validate_canonical_experiment_metrics_payload(payload))
    if not normalized:
        raise ValueError(f"canonical metrics file has no records: {path}")
    return normalized


def best_epoch_payload(payloads: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Select the best checkpoint payload by lowest validation regret."""

    if not payloads:
        raise ValueError("best epoch selection requires at least one metrics payload.")
    normalized = [
        validate_canonical_experiment_metrics_payload(payload) for payload in payloads
    ]
    return min(
        normalized,
        key=lambda payload: (
            float(payload["val_regret_mean"]),
            int(payload["epoch"]),
        ),
    )


def aggregate_canonical_model_dir(
    model_dir: Path,
    *,
    baseline_seed_means: Sequence[float] | None = None,
    baseline_mean: float = BASELINE_V2_PLUS_MEAN_REGRET_UAH,
    required_seed_count: int | None = DEFAULT_REQUIRED_SEED_COUNT,
) -> dict[str, Any]:
    """Aggregate canonical per-seed metrics into one thesis-facing summary."""

    if not model_dir.exists():
        raise ValueError(f"model metrics directory does not exist: {model_dir}")
    if not model_dir.is_dir():
        raise ValueError(f"model metrics path must be a directory: {model_dir}")
    if required_seed_count is not None and required_seed_count <= 0:
        raise ValueError("required_seed_count must be positive when provided.")
    seed_metrics_paths = _seed_metrics_paths(model_dir)
    if not seed_metrics_paths:
        raise ValueError(f"no seed metrics.json files found under {model_dir}")

    best_payloads = [
        best_epoch_payload(load_canonical_metrics_file(path))
        for path in seed_metrics_paths
    ]
    seed_values = [int(payload["seed"]) for payload in best_payloads]
    if len(seed_values) != len(set(seed_values)):
        raise ValueError("canonical metrics aggregate requires unique seed values.")
    if required_seed_count is not None and len(seed_values) != required_seed_count:
        raise ValueError(
            "canonical metrics aggregate requires exactly "
            f"{required_seed_count} seed files; found {len(seed_values)}."
        )

    seed_test_regrets = [float(payload["test_regret_mean"]) for payload in best_payloads]
    mean_test_regret = mean(seed_test_regrets)
    std_test_regret = pstdev(seed_test_regrets) if len(seed_test_regrets) > 1 else 0.0
    estimator_class = _consistent_optional_text(best_payloads, "estimator_class")
    artifact_identifier = _consistent_optional_text(
        best_payloads, "artifact_identifier"
    )
    payload_model = _consistent_optional_text(best_payloads, "model")
    independent_holdout = all(
        bool(payload.get("independent_holdout", True)) for payload in best_payloads
    )
    pvalue = (
        welch_t_pvalue(seed_test_regrets, baseline_seed_means)
        if independent_holdout
        else None
    )
    pass_level = (
        classify_experiment_pass_level(
            mean_test_regret=mean_test_regret,
            t_pvalue_vs_v2plus=pvalue,
        )
        if pvalue is not None
        else "diagnostic_only"
    )
    aggregate = {
        "claim_scope": CANONICAL_EXPERIMENT_CLAIM_SCOPE,
        "model": payload_model if estimator_class is not None else model_dir.name,
        "n_seeds": len(seed_test_regrets),
        "seeds": seed_values,
        "mean_test_regret": round(mean_test_regret, 4),
        "std_test_regret": round(std_test_regret, 4),
        "t_pvalue_vs_v2plus": round(pvalue, 6) if pvalue is not None else None,
        "pass_level": pass_level,
        "independent_holdout": independent_holdout,
        "inference_valid": independent_holdout,
        "maximum_evaluation_content_overlap_ratio": max(
            (
                float(payload.get("evaluation_content_overlap_ratio", 0.0))
                for payload in best_payloads
            ),
            default=0.0,
        ),
        "baseline_mean_regret": round(float(baseline_mean), 4),
        "baseline_seed_count": len(baseline_seed_means or []),
        "primary_success_threshold": PRIMARY_SUCCESS_MEAN_REGRET_UAH,
        "secondary_success_threshold": SECONDARY_SUCCESS_MEAN_REGRET_UAH,
        "alpha": PRIMARY_SUCCESS_ALPHA,
        "market_execution_enabled": False,
        "promotion_gate_passed": False,
    }
    if estimator_class is not None:
        aggregate["estimator_class"] = estimator_class
    if artifact_identifier is not None:
        aggregate["artifact_identifier"] = artifact_identifier
    return aggregate


def _consistent_optional_text(
    payloads: Sequence[Mapping[str, Any]], key: str
) -> str | None:
    values = [payload.get(key) for payload in payloads]
    populated = [value for value in values if value is not None]
    if not populated:
        return None
    if len(populated) != len(values):
        raise ValueError(f"canonical metrics must consistently declare {key}.")
    unique = {str(value) for value in populated}
    if len(unique) != 1:
        raise ValueError(f"canonical metrics must use one {key}; found {sorted(unique)}.")
    return unique.pop()


def write_canonical_aggregate(path: Path, aggregate: Mapping[str, Any]) -> None:
    """Write a canonical aggregate JSON file."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(dict(aggregate), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def classify_experiment_pass_level(
    *,
    mean_test_regret: float,
    t_pvalue_vs_v2plus: float,
) -> str:
    """Classify an offline experiment under the V2+ thesis rubric."""

    if (
        mean_test_regret <= PRIMARY_SUCCESS_MEAN_REGRET_UAH
        and t_pvalue_vs_v2plus <= PRIMARY_SUCCESS_ALPHA
    ):
        return "primary"
    if mean_test_regret <= SECONDARY_SUCCESS_MEAN_REGRET_UAH:
        return "secondary"
    return "fail"


def welch_t_pvalue(
    candidate_seed_means: Sequence[float],
    baseline_seed_means: Sequence[float] | None,
) -> float:
    """Return a two-sided Welch t-test p-value, or 1.0 when unavailable."""

    if baseline_seed_means is None:
        return 1.0
    candidate = [float(value) for value in candidate_seed_means]
    baseline = [float(value) for value in baseline_seed_means]
    if len(candidate) < 2 or len(baseline) < 2:
        return 1.0
    if not all(math.isfinite(value) for value in [*candidate, *baseline]):
        raise ValueError("Welch t-test inputs must be finite.")
    if pstdev(candidate) == 0.0 and pstdev(baseline) == 0.0:
        return (
            1.0
            if math.isclose(mean(candidate), mean(baseline), rel_tol=0.0, abs_tol=1e-12)
            else 0.0
        )

    from scipy import stats

    result = stats.ttest_ind(candidate, baseline, equal_var=False)
    pvalue = float(result.pvalue)
    if math.isnan(pvalue):
        return 1.0 if mean(candidate) == mean(baseline) else 0.0
    return max(0.0, min(1.0, pvalue))


def canonical_metrics_from_dt_v2_plus_promotion_summary(
    summary: Mapping[str, Any],
    *,
    seed: int,
    git_commit: str,
    cmd: str,
    run_id: str | None = None,
    epoch: int = 0,
) -> dict[str, Any]:
    """Adapt an existing DT/V2+ promotion-evidence summary into metrics JSON."""

    gate = _mapping(summary.get("gate", {}), "gate")
    evaluation = _mapping(summary.get("evaluation", {}), "evaluation")
    if bool(gate.get("market_execution_enabled", False)):
        raise ValueError("DT/V2+ summary adapter requires market_execution_enabled=false.")
    if bool(gate.get("promotion_gate_passed", False)):
        raise ValueError("DT/V2+ summary adapter requires promotion_gate_passed=false.")

    selected_mean_regret = _optional_float(
        evaluation.get("selected_mean_regret_uah"),
    )
    if selected_mean_regret is None:
        selected_mean_regret = _required_float(gate, "selected_mean_regret_uah")
    selected_median_regret = _optional_float(
        evaluation.get("selected_median_regret_uah"),
    )
    regret_std_proxy = 0.0 if selected_median_regret is None else abs(
        selected_mean_regret - selected_median_regret
    )
    payload = {
        "run_id": run_id or _required_str(summary, "run_slug"),
        "model": "dt_v2_plus_safe_switch_selector",
        "seed": seed,
        "epoch": epoch,
        "val_regret_mean": selected_mean_regret,
        "val_regret_std": regret_std_proxy,
        "test_regret_mean": selected_mean_regret,
        "test_regret_std": regret_std_proxy,
        "early_stop_epoch": epoch,
        "wall_time_s": 0.0,
        "gpu_hours": 0.0,
        "max_mem_gb": 0.0,
        "git_commit": git_commit,
        "cmd": cmd,
        "claim_scope": CANONICAL_EXPERIMENT_CLAIM_SCOPE,
    }
    return validate_canonical_experiment_metrics_payload(payload)


def _seed_metrics_paths(model_dir: Path) -> list[Path]:
    paths = {
        *model_dir.glob("seed_*/metrics.json"),
        *model_dir.glob("seed-*/metrics.json"),
    }
    return sorted(paths)


def _required_str(payload: Mapping[str, Any], field_name: str) -> str:
    value = payload[field_name]
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string.")
    return value.strip()


def _required_int(payload: Mapping[str, Any], field_name: str) -> int:
    value = payload[field_name]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer.")
    return value


def _required_float(payload: Mapping[str, Any], field_name: str) -> float:
    value = payload[field_name]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be numeric.")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError(f"{field_name} must be finite.")
    return normalized


def _required_bool(payload: Mapping[str, Any], field_name: str) -> bool:
    value = payload[field_name]
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be a boolean.")
    return value


def _validate_model_estimator_lineage(*, model: str, estimator_class: str) -> None:
    expected_prefix = _MODEL_LABEL_PREFIX_BY_ESTIMATOR.get(estimator_class)
    if expected_prefix is None:
        return
    if not model.startswith(expected_prefix):
        raise ValueError(
            f"model label must identify {estimator_class}; observed model={model!r}."
        )


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("optional numeric value must be numeric when provided.")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError("optional numeric value must be finite.")
    return normalized


def _mapping(value: Any, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be an object.")
    return value


__all__ = [
    "BASELINE_V2_PLUS_MEAN_REGRET_UAH",
    "CANONICAL_EXPERIMENT_CLAIM_SCOPE",
    "DEFAULT_REQUIRED_SEED_COUNT",
    "PRIMARY_SUCCESS_ALPHA",
    "PRIMARY_SUCCESS_MEAN_REGRET_UAH",
    "SECONDARY_SUCCESS_MEAN_REGRET_UAH",
    "aggregate_canonical_model_dir",
    "best_epoch_payload",
    "canonical_metrics_from_dt_v2_plus_promotion_summary",
    "classify_experiment_pass_level",
    "load_canonical_metrics_file",
    "validate_canonical_experiment_metrics_payload",
    "welch_t_pvalue",
    "write_canonical_aggregate",
]
