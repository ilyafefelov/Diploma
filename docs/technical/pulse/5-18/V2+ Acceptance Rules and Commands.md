Here’s a tiny, drop‑in **repro checklist + toolchain** so you (or a reviewer) can *deterministically* verify your BESS thesis headline:  **V2+ 174.77 UAH < frozen V2 206.37 UAH < strict 310.58 UAH** —with rolling‑window discipline and per‑seed stats.

---

# What “passes” (acceptance rules)

A run is **reproducible** only if **all** are true:

**A. Overall accuracy**
V2+ overall mean regret is within **±5%** of  **174.77 UAH** .

**B. Ordering**
**V2+ < frozen‑V2 < strict** on overall means.

**C. Rolling windows (4/4)**
In each of the 4 chronological windows, the **same ordering** holds and **no window’s mean** deviates by **>15%** from that model’s overall mean.

**D. Reporting**
You output:

* Per‑seed **mean±std** (all data).
* Per‑window means (for each model).
* A final **PASS/FAIL** flag; FAIL if any condition above breaks.

---

# Canonical run loop (seeds × 4 windows)

```bash
# repo root
MODEL_SET=("v2_plus" "frozen_v2" "strict")
SEEDS=(42 123 2024 777 999)
WINDOWS=(w1 w2 w3 w4)   # your 4 rolling windows (e.g., month blocks)

OUT=./.eval_out
mkdir -p "$OUT"

for m in "${MODEL_SET[@]}"; do
  for s in "${SEEDS[@]}"; do
    for w in "${WINDOWS[@]}"; do
      python tools/eval.py \
        --model "$m" \
        --seed "$s" \
        --window "$w" \
        --market_execution_enabled=false \
        --write_metrics "$OUT/${m}__seed${s}__${w}__metrics.json"
    done
  done
done
```

> Assumes each `metrics.json` contains at least:
> `{"model":"...", "seed":int, "window":"w1|w2|w3|w4", "mean_regret":float}`
> (If your keys differ, just tweak the parser below.)

---

# Merge metrics and compute stats (jq → python)

**1) Flatten to CSV‑ish rows with `jq`:**

```bash
jq -r '
  ["model","seed","window","mean_regret"],
  (.[]
   | [ .model, (.seed|tostring), .window, (.mean_regret|tostring) ])
  | @csv
' "$OUT"/*metrics.json > "$OUT/metrics_flat.csv"
```

**2) Aggregate in Python: mean±std per seed, per window, and overall + acceptance checks:**

```python
# save as tools/aggregate_and_check.py and run after jq step
import csv, math, statistics, sys, json
from collections import defaultdict, OrderedDict
import pathlib

IN = pathlib.Path("./.eval_out/metrics_flat.csv")
PASS_PATH = pathlib.Path("./.eval_out/FINAL_PASS_FAIL.txt")
REPORT_PATH = pathlib.Path("./.eval_out/AGGREGATE_REPORT.json")
CSV_OUT = pathlib.Path("./.eval_out/mean_std_per_seed.csv")
CSV_WIN = pathlib.Path("./.eval_out/per_window_means.csv")

TARGETS = {
    "v2_plus": 174.77,
    "strict": 310.58,
    "frozen_v2": 206.37
}
TOL_OVERALL = 0.05     # ±5%
TOL_WINDOW_DRIFT = 0.15 # 15%

rows = []
with IN.open() as f:
    r = csv.reader(f)
    header = next(r)
    for model, seed, window, mean_regret in r:
        rows.append({
            "model": model,
            "seed": int(seed),
            "window": window,
            "mean_regret": float(mean_regret)
        })

# group helpers
by_model_seed = defaultdict(list)
by_model_window = defaultdict(list)
by_model_all = defaultdict(list)

for x in rows:
    by_model_seed[(x["model"], x["seed"])].append(x["mean_regret"])
    by_model_window[(x["model"], x["window"])].append(x["mean_regret"])
    by_model_all[x["model"]].append(x["mean_regret"])

def mean(xs): return sum(xs)/len(xs) if xs else float("nan")
def std(xs):
    return statistics.pstdev(xs) if len(xs)>1 else 0.0

# per-seed mean±std (across all windows for that seed)
seed_table = []
for (m,s), vals in sorted(by_model_seed.items()):
    seed_table.append({
        "model": m,
        "seed": s,
        "mean": mean(vals),
        "std": std(vals)
    })

# per-window means (across seeds)
win_table = []
windows = sorted({x["window"] for x in rows})
for m in sorted(by_model_all.keys()):
    for w in windows:
        vals = by_model_window[(m,w)]
        win_table.append({
            "model": m, "window": w, "mean": mean(vals)
        })

# overall means
overall = { m: mean(v) for m,v in by_model_all.items() }

# --- Acceptance checks ---
ok = True
reasons = []

# A. Overall ±5% to target for V2+
if "v2_plus" in overall:
    v2p = overall["v2_plus"]; tgt = TARGETS["v2_plus"]
    if not (tgt*(1-TOL_OVERALL) <= v2p <= tgt*(1+TOL_OVERALL)):
        ok = False
        reasons.append(f"A: V2+ overall {v2p:.2f} not within ±5% of {tgt:.2f}")

# B. Ordering: V2+ < frozen_v2 < strict
need = ["v2_plus","frozen_v2","strict"]
if all(k in overall for k in need):
    if not (overall["v2_plus"] < overall["frozen_v2"] < overall["strict"]):
        ok = False
        reasons.append("B: Ordering breaks (expected v2_plus < frozen_v2 < strict)")

# C. 4/4 windows ordering + window drift ≤15%
for w in windows:
    have = []
    for m in need:
        if (m,w) in by_model_window and by_model_window[(m,w)]:
            have.append(m)
    if set(have)==set(need):
        m_v2p = mean(by_model_window[("v2_plus",w)])
        m_frz = mean(by_model_window[("frozen_v2",w)])
        m_str = mean(by_model_window[("strict",w)])
        if not (m_v2p < m_frz < m_str):
            ok = False
            reasons.append(f"C: Ordering breaks in window {w}")
        # drift check vs overall
        for m, mw in [("v2_plus", m_v2p), ("frozen_v2", m_frz), ("strict", m_str)]:
            ov = overall[m]
            if ov==0: continue
            if abs(mw-ov)/ov > TOL_WINDOW_DRIFT:
                ok = False
                reasons.append(f"C: {m} window {w} deviates >15% (win={mw:.2f}, overall={ov:.2f})")
    else:
        ok = False
        reasons.append(f"C: Missing models in window {w}: have {have}")

# D is “reporting”: we always emit files below.

# write per-seed mean±std CSV
with CSV_OUT.open("w", newline="") as f:
    wtr = csv.writer(f)
    wtr.writerow(["model","seed","mean","std"])
    for r in seed_table:
        wtr.writerow([r["model"], r["seed"], f'{r["mean"]:.6f}', f'{r["std"]:.6f}'])

# write per-window means CSV
with CSV_WIN.open("w", newline="") as f:
    wtr = csv.writer(f)
    wtr.writerow(["model","window","mean"])
    for r in win_table:
        wtr.writerow([r["model"], r["window"], f'{r["mean"]:.6f}'])

# write structured JSON report
REPORT_PATH.write_text(json.dumps({
    "overall_means": {k: round(v,6) for k,v in overall.items()},
    "targets": TARGETS,
    "per_seed": seed_table,
    "per_window": win_table,
    "acceptance": {
        "pass": ok,
        "fail_reasons": reasons
    }
}, indent=2))

# final pass/fail text
PASS_PATH.write_text("PASS\n" if ok else "FAIL\n" + "\n".join(reasons))

print("Done. See:")
print("-", CSV_OUT)
print("-", CSV_WIN)
print("-", REPORT_PATH)
print("-", PASS_PATH)
```

Run:

```bash
python tools/aggregate_and_check.py
```

---

# Quick “compare to headline” one‑liner (sanity)

```bash
python - <<'PY'
import csv,statistics
from collections import defaultdict
f='./.eval_out/metrics_flat.csv'
by=defaultdict(list)
with open(f) as x:
  r=csv.reader(x); next(r)
  for m,seed,w,reg in r: by[m].append(float(reg))
for m in sorted(by):
  mu=sum(by[m])/len(by[m]); sd=statistics.pstdev(by[m]) if len(by[m])>1 else 0.0
  print(f"{m:10s} mean={mu:.2f}  std={sd:.2f}")
PY
```

---

# Outputs you’ll hand in

* `AGGREGATE_REPORT.json` (ground truth)
* `mean_std_per_seed.csv` (seed stability)
* `per_window_means.csv` (4/4 ordering check)
* `FINAL_PASS_FAIL.txt` (**PASS** if A–D satisfied; else **FAIL** + reasons)

If you want, I can adapt this to your repo directory layout or wire it into your `make eval` / Dagster job so it runs after each experiment and pushes a PASS/FAIL badge.
