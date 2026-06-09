Here’s a compact, ready‑to‑drop‑in **evaluation runbook + minimal eval script** for your DT lane so you can measure **per‑seed downstream regret vs V2+ baseline** (and filtered BC), emit  **per‑episode traces** , and append a **single canonical CSV + metrics.json** that your dashboard/CI expects.

---

# What this is (quick context)

You’ve got forecasts (NBEATSx/TFT) → candidate library → V2+ schedule/value learner. This adds a tiny **Decision Transformer eval** that:

* fixes determinism per seed,
* evaluates **N_eval** episodes from the strict_similar_day validation split,
* computes  **regret = cost(policy) − cost(V2+ baseline)** ,
* writes per‑episode **CSV traces** and an appended **metrics.json** line so CI can gate on progress.

---

# Files & layout

```
experiments/
  eval_dt.py
  logs/
    dt_3seed/
      eval_traces_{seed}.csv
  dt/
    README.md
traces/
  baseline_v2plus.csv            # or baseline_v2plus_traces.csv with per-episode baseline_return
checkpoints/
  dt_chkpt_{seed}_{sha}.pt
```

---

# Minimal CLI

```
python experiments/eval_dt.py \
  --checkpoint experiments/checkpoints/dt_chkpt_{seed}_{sha}.pt \
  --val-split val_strict_similar_day \
  --baseline traces/baseline_v2plus.csv \
  --out experiments/logs/dt_3seed/ \
  --n-eval 50 \
  --context-len 20 \
  --seed {seed} \
  --commit {short_sha}
```

---

# CSV + metrics formats

**Traces CSV columns (one row per episode):**

```
episode_id,seed,policy_return,baseline_return,regret
```

**metrics.json** append one compact JSON line per seed (newline‑delimited JSON):

```json
{"seed": 13, "commit": "a1b2c3d", "regret_mean": 84.12, "regret_std": 17.55, "elapsed": 132.6, "early_stop_flag": false}
```

---

# Guardrails (CI)

* **Determinism:** fail if any seed produces mismatched metadata (e.g., different sampled episode IDs or different per‑episode returns on rerun).
* **Seed coverage:** if **regret_mean improves >5% vs BC** but uses  **<3 seeds** , fail (insufficient evidence).
* **Early stop:** set `early_stop_flag=true` when **regret_mean** hasn’t improved for `patience` eval steps.
* **N_eval:** start with `--n-eval 10` to verify plumbing; scale to `50` for the real run.

---

# Drop‑in eval script (minimal but complete)

```python
#!/usr/bin/env python3
# experiments/eval_dt.py
import argparse, json, os, random, time
from dataclasses import dataclass
from pathlib import Path
import numpy as np

# --- Stubs you should wire to your repo ---
# replace with your actual dataset loader, DT policy, and cost/return adapters.
def load_validation_split(name:str, n_eval:int, seed:int):
    rng = np.random.default_rng(seed)
    # Return list of (episode_id, episode_data) deterministically sampled
    all_ids = [f"ep_{i:05d}" for i in range(2000)]
    chosen = sorted(rng.choice(all_ids, size=n_eval, replace=False).tolist())
    return [(eid, {"mock": True}) for eid in chosen]

def load_checkpoint(path:str):
    # return initialized DT policy object
    return {"ckpt": path}

def run_dt_autoregressive(policy, episode_data, context_len:int, seed:int):
    # deterministic mock return using seed+episode id hash
    random.seed(hash(episode_data["mock"]) ^ seed)
    return float(1000.0 + (seed % 7))  # replace with true rollout return

def cost_from_return(ret: float) -> float:
    # if higher return is better, "cost" can be -return
    return -ret

def load_baseline_map(baseline_path: str):
    # supports either wide CSV with episode_id,baseline_return
    # or a traces file; here we synthesize a constant baseline
    return lambda episode_id: 1000.0

def load_bc_regret_mean() -> float:
    # optional: pull your filtered-BC baseline for the 5% rule
    return 120.0
# -------------------------------------------

@dataclass
class EvalArgs:
    checkpoint: str
    val_split: str
    baseline: str
    out_dir: str
    n_eval: int
    context_len: int
    seed: int
    commit: str
    patience: int

def set_determinism(seed:int):
    random.seed(seed)
    np.random.seed(seed)
    try:
        import torch
        torch.manual_seed(seed)
        torch.cuda.manual_seed_all(seed)
        torch.backends.cudnn.deterministic = True
        torch.backends.cudnn.benchmark = False
    except Exception:
        pass

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--checkpoint", required=True)
    p.add_argument("--val-split", required=True)
    p.add_argument("--baseline", required=True)
    p.add_argument("--out", dest="out_dir", required=True)
    p.add_argument("--n-eval", type=int, default=50)
    p.add_argument("--context-len", type=int, default=20)
    p.add_argument("--seed", type=int, default=13)
    p.add_argument("--commit", type=str, default="unknownsha")
    p.add_argument("--patience", type=int, default=4)
    args = EvalArgs(**vars(p.parse_args()))

    t0 = time.time()
    set_determinism(args.seed)

    Path(args.out_dir).mkdir(parents=True, exist_ok=True)
    traces_csv = Path(args.out_dir) / f"eval_traces_{args.seed}.csv"
    metrics_json = Path(args.out_dir) / "metrics.json"

    # 1) load checkpoint
    policy = load_checkpoint(args.checkpoint)

    # 2) sample episodes deterministically
    episodes = load_validation_split(args.val_split, args.n_eval, args.seed)
    sampled_ids = [e for e, _ in episodes]

    # determinism meta (for CI to recheck)
    determinism_meta = {
        "seed": args.seed,
        "commit": args.commit,
        "val_split": args.val_split,
        "episode_ids": sampled_ids
    }

    # 3) baseline map
    baseline_fn = load_baseline_map(args.baseline)

    rows = []
    for eid, edata in episodes:
        policy_ret = run_dt_autoregressive(policy, edata, args.context_len, args.seed)
        base_ret = float(baseline_fn(eid))
        regret = cost_from_return(policy_ret) - cost_from_return(base_ret)
        rows.append((eid, args.seed, policy_ret, base_ret, regret))

    # 4) aggregate
    regrets = np.array([r[-1] for r in rows], dtype=float)
    regret_mean, regret_std = float(np.mean(regrets)), float(np.std(regrets))

    # 5) write traces CSV (overwrite per seed run)
    with open(traces_csv, "w", encoding="utf-8") as f:
        f.write("episode_id,seed,policy_return,baseline_return,regret\n")
        for r in rows:
            f.write(f"{r[0]},{r[1]},{r[2]:.6f},{r[3]:.6f},{r[4]:.6f}\n")

    # 6) append metrics.json (one line per seed)
    elapsed = float(time.time() - t0)
    metrics_line = {
        "seed": args.seed,
        "commit": args.commit[:7],
        "regret_mean": regret_mean,
        "regret_std": regret_std,
        "elapsed": elapsed,
        "early_stop_flag": False,   # set by your trainer if no improvement for `patience`
        "determinism_meta": determinism_meta
    }
    with open(metrics_json, "a", encoding="utf-8") as f:
        f.write(json.dumps(metrics_line) + "\n")

    # 7) CI checks
    # (a) seed determinism check hook: in practice, re‑hydrate determinism_meta on rerun and compare
    # (b) 5% vs BC rule
    bc_mean = load_bc_regret_mean()
    if regret_mean < 0.95 * bc_mean and args.seed in (0,1,2):  # example: single-seed run
        # if fewer than 3 seeds used overall, you can make this fail hard in CI
        pass

    print(f"OK seed={args.seed} mean_regret={regret_mean:.4f} std={regret_std:.4f} elapsed={elapsed:.1f}s")
    print(f"traces: {traces_csv}")
    print(f"metrics: {metrics_json}")

if __name__ == "__main__":
    main()
```

---

# README stub (experiments/dt/README.md)

* **Lane:** `strict_similar_day → NBEATSx/TFT → candidate library → V2+`
* **Policy:** Decision Transformer (context_len=20), offline eval only.
* **Eval:** deterministic per‑seed sampling (`N_eval=50`), regret vs V2+ baseline.
* **Outputs:** `logs/dt_3seed/eval_traces_{seed}.csv`, append to `logs/dt_3seed/metrics.json`.
* **CI:** determinism check; >5% improvement vs **filtered BC** requires  **≥3 seeds** ; early stopping flag when no improvement for `patience`.
* **Tip:** Dry‑run with `--n-eval 10`, then switch to `50`.

---

# How to use it right now

1. Drop `eval_dt.py` + README into `experiments/`, confirm your dataset/baseline/DT hooks (replace the stubs).
2. Run 3 seeds (e.g., `13, 17, 23`) and the current git short SHA.
3. Point your dashboard/CI to read `metrics.json` lines and the per‑seed traces.

If you want, I can tailor the stub hooks to your repo structure (dataset loader, DT forward, and your real baseline traces).
