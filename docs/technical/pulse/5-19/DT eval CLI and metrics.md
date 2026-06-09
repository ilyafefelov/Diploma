Here’s a ready‑to‑drop evaluation CLI and a tiny aggregator so your CI can deterministically score Decision Transformer (DT) checkpoints against the repo’s **val_strict_similar_day** split and a V2+ baseline.

---

# experiments/eval_dt_cli.py

```python
#!/usr/bin/env python3
import argparse, json, os, random, time, hashlib
from pathlib import Path
import numpy as np
import torch

# ---------- Helpers ----------
def set_seed(seed: int):
    random.seed(seed); np.random.seed(seed); torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)
        torch.backends.cudnn.deterministic = True
        torch.backends.cudnn.benchmark = False

def load_commit_sha(checkpoint_path: str) -> str:
    # Fallback: derive short sha-like tag from file bytes if git not available in CI
    try:
        import subprocess
        return subprocess.check_output(["git","rev-parse","--short","HEAD"], text=True).strip()
    except Exception:
        h = hashlib.sha1(Path(checkpoint_path).read_bytes()).hexdigest()
        return h[:7]

def load_dt(checkpoint_path: str):
    # Replace with your repo’s DT model loader
    ckpt = torch.load(checkpoint_path, map_location="cpu")
    model = ckpt["model"] if "model" in ckpt else ckpt
    model.eval()
    return model

def load_val_env(split_name: str, window: int):
    """
    Return an iterable of validation episodes for the given split & window.
    Each episode should expose .reset() and .step(action) or a convenience
    rollout(fn_policy) that returns (reward, baseline_reward) if available.
    Replace with your repo’s dataloader/env wrapper.
    """
    from your_repo.val_loader import make_val_episodes  # TODO: implement in-repo
    return make_val_episodes(split=split_name, window=window)

def load_baseline_table(baseline_csv: str):
    """
    CSV must contain per-episode baseline value (V2+). Minimal columns:
    episode_id, baseline_value
    """
    import csv
    tbl = {}
    with open(baseline_csv, newline="") as f:
        for r in csv.DictReader(f):
            eid = r.get("episode_id") or r.get("id")
            tbl[eid] = float(r["baseline_value"])
    return tbl

def eval_one_episode(model, episode):
    """
    Run the policy for one episode using model; return (value, episode_id).
    Replace body with your repo’s rollout using DT decode.
    """
    value, episode_id = episode.rollout(lambda obs: model(obs)), episode.id
    return float(value), str(episode_id)

# ---------- CLI ----------
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--checkpoint", required=True)
    p.add_argument("--val-split", default="val_strict_similar_day")
    p.add_argument("--baseline", required=True, help="e.g., traces/baseline_v2plus_traces.csv or v2plus.csv")
    p.add_argument("--n-eval", type=int, default=50)
    p.add_argument("--window", type=int, required=True)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out", required=True, help="dir to write metrics.json")
    p.add_argument("--early-stop-std", type=float, default=0.0,
                   help="optional: stop early if running std <= this threshold (>0 enables)")
    args = p.parse_args()

    t0 = time.time()
    Path(args.out).mkdir(parents=True, exist_ok=True)

    set_seed(args.seed)
    model = load_dt(args.checkpoint)
    sha = load_commit_sha(args.checkpoint)

    baseline_tbl = load_baseline_table(args.baseline)
    episodes = load_val_env(args.val_split, args.window)

    vals, regrets, per_ep_regret = [], [], []
    baseline_seen = []
    n = 0

    for ep in episodes:
        if n >= args.n_eval: break
        v, eid = eval_one_episode(model, ep)
        b = baseline_tbl.get(eid)
        if b is None:
            # Skip episodes that baseline table doesn't cover
            continue
        reg = b - v  # regret: gap vs V2+ baseline
        vals.append(v); regrets.append(reg); per_ep_regret.append([eid, float(reg)])
        baseline_seen.append(b)
        n += 1

        if args.early_stop_std > 0 and n >= 10:
            if float(np.std(regrets, ddof=1)) <= args.early_stop_std:
                break

    if n == 0:
        raise RuntimeError("No evaluable episodes found (baseline mismatch or empty val set).")

    metrics = {
        "seed": int(args.seed),
        "commit": sha,
        "window": int(args.window),
        "regret_mean": float(np.mean(regrets)),
        "regret_std": float(np.std(regrets, ddof=1) if len(regrets) > 1 else 0.0),
        "regret_per_episode": per_ep_regret,  # list[[episode_id, regret], ...]
        "baseline_mean": float(np.mean(baseline_seen)),
        "n_eval": int(n),
        "elapsed": float(time.time() - t0),
        "early_stop_flag": bool(args.early_stop_std > 0),
    }

    out_file = Path(args.out) / "metrics.json"
    with open(out_file, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"Wrote {out_file}")

if __name__ == "__main__":
    main()
```

**Sample run (matches your naming):**

```bash
python experiments/eval_dt_cli.py \
  --checkpoint experiments/checkpoints/dt_chkpt_{window}_{seed}_{sha}.pt \
  --val-split val_strict_similar_day \
  --baseline traces/baseline_v2plus_traces.csv \
  --n-eval 50 \
  --window {w} \
  --seed {s} \
  --out experiments/logs/dt_runs/window_{w}/seed_{s}/
```

**Output schema (exact keys):**

```json
{"seed","commit","window","regret_mean","regret_std","regret_per_episode","baseline_mean","n_eval","elapsed","early_stop_flag"}
```

---

# scripts/aggregate_eval.py (one‑pager style)

```python
#!/usr/bin/env python3
import json, sys, glob, os
import numpy as np
from pathlib import Path

"""
Scans for **/metrics.json, merges across seeds/windows,
prints overall mean±std, verifies ordering vs V2+ per window,
and writes CI status files:
- artifacts/aggregate_summary.json
- artifacts/final_status.txt  (PASS or FAIL)
"""

def load_all(root="."):
    files = glob.glob(os.path.join(root, "**", "metrics.json"), recursive=True)
    recs = []
    for f in files:
        try:
            with open(f) as fh: recs.append(json.load(fh))
        except Exception: pass
    return recs

def main():
    root = sys.argv[1] if len(sys.argv) > 1 else "."
    Path("artifacts").mkdir(exist_ok=True)

    recs = load_all(root)
    if not recs:
        print("No metrics.json found.")
        Path("artifacts/final_status.txt").write_text("FAIL")
        sys.exit(1)

    # Overall
    regrets = [r["regret_mean"] for r in recs]
    overall = {
        "count": len(recs),
        "overall_regret_mean": float(np.mean(regrets)),
        "overall_regret_std": float(np.std(regrets, ddof=1) if len(regrets)>1 else 0.0),
        "by_window": {},
    }

    # By window check (DT should not be worse than V2+ on average -> regret_mean <= 0)
    windows = sorted(set(int(r["window"]) for r in recs))
    pass_flags = []
    for w in windows:
        wr = [r for r in recs if int(r["window"]) == w]
        wm = float(np.mean([r["regret_mean"] for r in wr]))
        overall["by_window"][str(w)] = {
            "n": len(wr),
            "regret_mean": wm,
            "regret_std": float(np.std([r["regret_mean"] for r in wr], ddof=1) if len(wr)>1 else 0.0),
        }
        pass_flags.append(wm <= 0.0)

    # CI rule: all four rolling windows must pass vs V2+ (regret_mean <= 0)
    all_pass = all(pass_flags) and len(windows) >= 4

    # Emit
    Path("artifacts/aggregate_summary.json").write_text(json.dumps(overall, indent=2))
    Path("artifacts/final_status.txt").write_text("PASS" if all_pass else "FAIL")

    # Console summary
    print(json.dumps(overall, indent=2))
    print("\nCI RESULT:", "PASS" if all_pass else "FAIL")

if __name__ == "__main__":
    main()
```

**Usage:**

```bash
python scripts/aggregate_eval.py experiments/logs/dt_runs
# writes artifacts/aggregate_summary.json and artifacts/final_status.txt
```

---

## Notes to wire into your repo quickly

* Replace `your_repo.val_loader.make_val_episodes(...)` and `episode.rollout(...)` with your existing eval hooks. Keep the return types the same.
* The **baseline CSV** needs `episode_id,baseline_value`. If your `baseline_v2plus_traces.csv` uses different column names, tweak `load_baseline_table`.
* The **regret** is defined `baseline − DT_value` so “better than V2+” ⇒ regret ≤ 0.
* Determinism: CUDA deterministic flags are set; ensure any data loader shuffling is disabled for eval.

If you want, I can also inline the small `val_loader` stub against your existing `val_strict_similar_day` objects.
