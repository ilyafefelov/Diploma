Here’s a compact, no‑layout‑change checklist to re‑run the exact lane you cited— **strict_similar_day LP control → official NBEATSx/TFT forecasts → schedule candidate library → Schedule/Value Learner V2+** —with `market_execution_enabled=false` and a **4/4 rolling‑window** eval. It’s copy‑pasteable; swap `<train_cli>` / `<eval_cli>` with your repo entrypoints.

---

# Run plan (deterministic + reproducible)

**Global run constants**

* `LANE=strict_similar_day→official_forecasts→candidate_library→SVL_V2_PLUS`
* `EXEC=false` (i.e., `market_execution_enabled=false`)
* `ROLLING_WINDOWS=4` (indexes `0..3`)
* **Seeds** : `SEEDS=(101 202 303 404)` (change if you prefer)
* **Artifacts root** : `runs/strict_sd_official_v2p_execFalse/`
* Save **one checkpoint per (seed, window)** + a `metrics.json` per (seed, window)
* Include **short git SHA** in file names for traceability: `$(git rev-parse --short HEAD)` → `${GIT}`

---

## 0) Environment hygiene

```bash
export PYTHONHASHSEED=0
export CUBLAS_WORKSPACE_CONFIG=:4096:8
export TOKENIZERS_PARALLELISM=false
GIT=$(git rev-parse --short HEAD)
```

---

## 1) Rolling‑window training (per seed × per window)

Train the Schedule/Value Learner V2+ on the **candidate library built from official NBEATSx/TFT** forecasts for each rolling window.

```bash
for SEED in 101 202 303 404; do
  for WINDOW_IDX in 0 1 2 3; do
    OUTDIR="runs/strict_sd_official_v2p_execFalse/seed_${SEED}/win_${WINDOW_IDX}"
    mkdir -p "${OUTDIR}"

    <train_cli> \
      --lane "${LANE}" \
      --window_idx "${WINDOW_IDX}" \
      --seed "${SEED}" \
      --market_execution_enabled "${EXEC}" \
      --forecaster_set "official:NBEATSx+TFT" \
      --schedule_learner "SVL_V2_PLUS" \
      --strict_lp_control "similar_day" \
      --save_dir "${OUTDIR}" \
      --save_top_k 1 \
      --checkpoint_name "svl_v2p_${GIT}_seed${SEED}_w${WINDOW_IDX}.ckpt" \
      --log_metrics_to "${OUTDIR}/metrics.json"
  done
done
```

**Notes**

* `--save_top_k 1` ensures exactly one checkpoint per (seed, window).
* `metrics.json` should include regret, calibration, and any window‑specific diagnostics.

---

## 2) Deterministic evaluation (per seed × per window)

Evaluate the saved checkpoint with **strict LP/oracle regret scoring** and offline promotion turned on, but  **execution off** .

```bash
for SEED in 101 202 303 404; do
  for WINDOW_IDX in 0 1 2 3; do
    OUTDIR="runs/strict_sd_official_v2p_execFalse/seed_${SEED}/win_${WINDOW_IDX}"
    CKPT=$(ls "${OUTDIR}"/svl_v2p_${GIT}_seed${SEED}_w${WINDOW_IDX}.ckpt)

    <eval_cli> \
      --lane "${LANE}" \
      --window_idx "${WINDOW_IDX}" \
      --seed "${SEED}" \
      --market_execution_enabled "${EXEC}" \
      --evaluation_mode "strict_lp_oracle_regret" \
      --checkpoint_path "${CKPT}" \
      --write_metrics_append "${OUTDIR}/metrics.json" \
      --write_predictions "${OUTDIR}/predictions.parquet"
  done
done
```

---

## 3) Per‑seed aggregation (4 windows → 1 summary per seed)

Creates `seed_summary.json` with window means/stdevs and per‑metric breakdown.

```bash
for SEED in 101 202 303 404; do
  SEED_DIR="runs/strict_sd_official_v2p_execFalse/seed_${SEED}"
  python - <<'PY'
import json, glob, os, statistics as st
import pathlib
seed_dir = os.environ['SEED_DIR'] if 'SEED_DIR' in os.environ else ''
def load_metrics(p):
    with open(p,'r',encoding='utf-8') as f: return json.load(f)
# Collect window metrics
metrics_paths = sorted(glob.glob(os.path.join(seed_dir, "win_*", "metrics.json")))
all_rows = []
for mp in metrics_paths:
    try:
        all_rows.append(load_metrics(mp))
    except Exception:
        pass
# Flatten and aggregate common keys (e.g., mean_regret)
def pick(k): return [r[k] for r in all_rows if k in r]
keys = sorted({k for r in all_rows for k in r.keys()})
agg = {}
for k in keys:
    vals = pick(k)
    if not vals: continue
    try:
        agg[k] = {
            "mean": float(st.mean(vals)),
            "stdev": float(st.pstdev(vals)),
            "count": len(vals)
        }
    except TypeError:
        # non‑numeric—skip
        pass
# Save
out = {
  "seed": int(pathlib.Path(seed_dir).name.split("_")[-1]),
  "git": os.popen("git rev-parse --short HEAD").read().strip(),
  "windows": [os.path.basename(os.path.dirname(p)) for p in metrics_paths],
  "aggregates": agg
}
with open(os.path.join(seed_dir,"seed_summary.json"),"w",encoding="utf-8") as f:
    json.dump(out,f,ensure_ascii=False,indent=2)
print("Wrote", os.path.join(seed_dir,"seed_summary.json"))
PY
done
```

Expected fields your metrics should already expose (rename if different):

* `mean_regret` (primary), `median_regret`, `win_rate_vs_control`, `calibration_rmse`, etc.

---

## 4) Across‑seeds aggregation (final reported numbers)

Aggregate `seed_summary.json` to produce the **reported mean regret** (and friends).

```bash
python - <<'PY'
import json, glob, os, statistics as st
root = "runs/strict_sd_official_v2p_execFalse"
seed_summaries = glob.glob(os.path.join(root,"seed_*","seed_summary.json"))
def load(p):
    with open(p,'r',encoding='utf-8') as f: return json.load(f)
rows = [load(p) for p in seed_summaries]
def mean_of(key):
    vals = []
    for r in rows:
        agg = r.get("aggregates",{})
        if key in agg and "mean" in agg[key]:
            vals.append(agg[key]["mean"])
    return (float(st.mean(vals)), float(st.pstdev(vals)), len(vals)) if vals else (None,None,0)

report = {}
for metric in ["mean_regret","median_regret","win_rate_vs_control","calibration_rmse"]:
    m, s, n = mean_of(metric)
    if m is not None:
        report[metric] = {"mean_across_seeds": m, "stdev_across_seeds": s, "num_seeds": n}

report["git"] = os.popen("git rev-parse --short HEAD").read().strip()
with open(os.path.join(root,"final_report.json"),"w",encoding="utf-8") as f:
    json.dump(report,f,ensure_ascii=False,indent=2)
print(json.dumps(report,indent=2,ensure_ascii=False))
PY
```

---

## 5) Sanity checks (quick)

* **Determinism** : rerun 1–2 windows for a seed and confirm byte‑identical `metrics.json` and checkpoint checksum.
* **Lane integrity** : log lane string at train & eval start; fail fast if mismatched.
* **Control** : store control (strict similar‑day LP) regret next to V2+ for the same windows to compute deltas.

---

## 6) File naming & structure (example)

```
runs/strict_sd_official_v2p_execFalse/
  seed_101/
    win_0/
      svl_v2p_<GIT>_seed101_w0.ckpt
      metrics.json
      predictions.parquet
    ...
    seed_summary.json
  ...
  final_report.json
```

---

## 7) One‑liners you may need

* Rebuild candidate library from official forecasts (if your repo splits this step):

```bash
<candidates_cli> --lane "${LANE}" --rolling_windows 4 --forecaster_set "official:NBEATSx+TFT" --out_root "artifacts/candidates_official_v2p"
```

* Validate strict LP/oracle scoring path:

```bash
<eval_cli> --dry_run --evaluation_mode "strict_lp_oracle_regret" --window_idx 0
```

---

## 8) What success looks like

* **All 4/4 windows pass** for each seed.
* `final_report.json` shows **lower mean regret** than the strict similar‑day control, stable across seeds, with reasonable calibration.

---

If you want, I can adapt this to your repo’s exact CLI flags (paste your `<train_cli>` / `<eval_cli>` signatures), or swap seeds/windows to match your current MLflow run grid.
