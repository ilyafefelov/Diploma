# NPZ Schema and One-Window Run

- Source URL: https://chatgpt.com/share/6a12d1c5-cf54-832d-b524-3412ef562018
- Share ID: `6a12d1c5-cf54-832d-b524-3412ef562018`
- Fetched at: `2026-05-24T11:26:07+00:00`
- Extraction: embedded ChatGPT share payload, text parts only; tool outputs remain redacted where the share redacts them.

## Full Text

### Message 1: assistant (model_editable_context)

{
  "content_type": "model_editable_context",
  "model_set_context": ""
}

### Message 2: assistant (text)

Here’s a compact, drop‑in plan to add “adjacent‑vertices” data and a smoke‑test CI to your thesis repo so you can trial Solver‑Free‑DFL–style surrogates without heavy solvers.

# What & why (super short)
- We’ll store candidate LP vertices and their local neighborhoods in a tiny **NPZ** so training/eval can read them fast and deterministically.
- A **mini sample** (N≈2–4, V≤64) lets CI run in seconds and validate your regret/ordering rules.

# File layout to add
```
data/
  mini/
    adj_vertices.npz        # tiny, checked-in sample
scripts/
  run_one_window.py         # runs one window across seeds, early-stops on plateau
tools/
  precompute_adjacent_vertices.py
ci/
  gate_metrics.py           # existing or stubbed “CI gate” that asserts thresholds/order
```

# NPZ format (single file, compact)
`data/mini/adj_vertices.npz` contains:
- `z_star`: shape `(N, V)` float32 — best-known objective values at each vertex (or use per‑instance scalars repeated to V).
- `adjacent_verts`: shape `(N, K, V)` float32 — for each instance and each of K neighbors, a probability or weight over the V vertices (one‑hot rows is fine).
- `adj_mask`: shape `(N, K)` uint8 — 1 for real neighbor rows, 0 for padding.
- `meta`: Python dict — e.g. `{"lane":"strict","windows":[0,1,2,3],"K":K,"V":V}`.

Pad neighbors to `K`; use `adj_mask` to ignore padding at train/eval.

# Generator (checked in)
`tools/precompute_adjacent_vertices.py` (outline):
```python
import numpy as np, json, pathlib, argparse
def main(out_path, N=3, V=32, K=6):
    rng = np.random.default_rng(42)
    z_star = rng.normal(size=(N, V)).astype(np.float32)
    # simple synthetic adjacency: K unique neighbors per instance
    adjacent_verts = np.zeros((N, K, V), np.float32)
    adj_mask = np.zeros((N, K), np.uint8)
    for i in range(N):
        idxs = rng.choice(V, size=K, replace=False)
        for k,j in enumerate(idxs):
            adjacent_verts[i,k,j] = 1.0
            adj_mask[i,k] = 1
    meta = {"lane":"strict","windows":[0], "K":K, "V":V}
    np.savez(out_path, z_star=z_star,
             adjacent_verts=adjacent_verts,
             adj_mask=adj_mask,
             meta=meta)
if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="data/mini/adj_vertices.npz")
    p.add_argument("--N", type=int, default=3)
    p.add_argument("--V", type=int, default=32)
    p.add_argument("--K", type=int, default=6)
    a = p.parse_args()
    pathlib.Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    main(a.out, a.N, a.V, a.K)
```

# Prototype runner (one window, multiple seeds)
`scripts/run_one_window.py` (outline—keep deps minimal):
```python
import json, time, argparse, pathlib
import numpy as np

def early_stop_plateau(history, patience=5, min_delta=1e-4):
    if len(history) < patience+1: return False
    recent = history[-(patience+1):]
    return (max(recent) - min(recent)) <= min_delta

def run_once(window:int, seed:int, adj_path:str, lane:str):
    rng = np.random.default_rng(seed)
    data = np.load(adj_path, allow_pickle=True)
    meta = data["meta"].item()
    # toy “regret” that decreases then plateaus (replace with your model loop)
    regrets = []
    cur = 1.0 + 0.1*rng.random()
    for step in range(200):
        cur = max(cur - 0.02*rng.random(), 0.05 + 0.005*rng.random())
        regrets.append(cur)
        if early_stop_plateau(regrets, patience=8, min_delta=5e-4):
            break
    return float(np.mean(regrets))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--window", type=int, required=True)
    ap.add_argument("--seeds", nargs="+", type=int, required=True)
    ap.add_argument("--adj", required=True)
    ap.add_argument("--out", required=True,
                    help="e.g. experiments/logs/run_w0_seed{seed}/metrics.json")
    ap.add_argument("--lane", default="strict")
    args = ap.parse_args()

    for seed in args.seeds:
        regret_mean = run_once(args.window, seed, args.adj, args.lane)
        out_path = args.out.format(seed=seed)
        pathlib.Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "regret_mean": regret_mean,
            "lane": args.lane,
            "window": args.window,
            "seed": seed,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
```

# How to run (exact)
1) Create the sample NPZ:
```
python -u tools/precompute_adjacent_vertices.py --out data/mini/adj_vertices.npz --N 3 --V 32 --K 6
```
2) Run the prototype for window 0 and three seeds; it writes **one `metrics.json` per seed**:
```
python -u scripts/run_one_window.py \
  --window 0 \
  --seeds 12 42 2026 \
  --adj data/mini/adj_vertices.npz \
  --out experiments/logs/run_w0_seed{seed}/metrics.json
```

# Metrics contract (CI relies on this)
Each file must be valid JSON with exactly these keys:
```json
{"regret_mean": 0.123,
 "lane": "strict",
 "window": 0,
 "seed": 42,
 "timestamp": "2026-05-24T00:00:00Z"}
```

# CI gate (ordering + thresholds)
Your existing CI should:
- **Parse all** `experiments/logs/run_w{W}_seed*/metrics.json`.
- **Check thresholds** (e.g., `regret_mean` ≤ target for “pass”).
- **Check ordering** if you produce multiple lanes later (e.g., `V2+` ≤ `strict`).
- **Fail the job** with a readable diff if any check breaks.

Minimal stub if you need one (`ci/gate_metrics.py`):
```python
import json, glob, sys
FAILS = []
TARGET = 0.9   # example loose bound for the toy runner
for p in sorted(glob.glob("experiments/logs/run_w*_seed*/metrics.json")):
    with open(p, "r", encoding="utf-8") as f:
        m = json.load(f)
    if m["regret_mean"] > TARGET:
        FAILS.append((p, m["regret_mean"]))
if FAILS:
    print("CI GATE FAILED:")
    for p,v in FAILS:
        print(f"  {p}: regret_mean={v:.4f} > {TARGET}")
    sys.exit(1)
print("CI GATE PASSED")
```

# Next steps (when you’re ready)
- Replace the toy loop with your **schedule‑neighbor surrogate loss** over `adjacent_verts` using `adj_mask` to ignore padding neighbors.
- Emit both **lane** (e.g., `strict`, `frozen`, `V2+`) and compare regrets in CI to lock regression safety.
- Scale NPZ size only for offline experiments; CI should always point at `data/mini/adj_vertices.npz` for speed.

Want me to drop these files into a zip or generate the tiny NPZ for you now so you can copy‑paste into `ilyafefelov/Diploma`?
