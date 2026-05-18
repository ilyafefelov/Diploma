Here’s a drop‑in **Decision Transformer (DT) config + run command** you can paste into your repo to get a quick, thesis‑safe baseline across 3 seeds—deterministic, low compute, and wired for your existing logging/artifacts.

> What this is (plain English): a compact DT experiment you can run to sanity‑check DT vs your current **V2+** and **filtered BC** learners on the *strict_similar_day_lane* dataset. It uses tiny model dims to keep GPU/RAM modest, enforces determinism, and writes a single checkpoint + `metrics.json` that your CI/metrics rules can pick up.

### 1) Save this YAML

Create: `codex/real-data-benchmark/configs/dt_3seed_compact.yaml`

```yaml
dataset: strict_similar_day_lane
model:
  d_model: 128
  n_layers: 3
  n_heads: 4
  context_len: 20
train:
  optimizer: AdamW
  lr: 5e-05
  batch_size: 64
  total_updates: 10000
  warmup_updates: 500
  grad_clip: 1.0
  early_stop_on: regret
  early_stop_patience: 3
  eval_interval: 1000
logging:
  log_dir: experiments/logs/dt_3seed
  metrics_file: experiments/logs/dt_3seed/metrics.json
artifacts:
  checkpoint_template: experiments/checkpoints/dt_chkpt_{seed}_{sha}.pt
seeds: [12,42,2026]
mapping:
  lane: strict_similar_day -> official_NBEATSx/TFT -> candidate_library -> V2+
  data_loader: codex/real-data-benchmark/dataloaders/similar_day_loader.py
run_example: "PYTHONHASHSEED=0 python -u experiments/train_dt.py --config configs/dt_3seed_compact.yaml --seed {seed} --deterministic"
```

### 2) Determinism switches (repo‑side)

* Set env seed (already in `run_example`) and, in your training entrypoint, enable:
  * `torch.use_deterministic_algorithms(True)`
  * Seed `random`, `numpy`, and torch (CPU+CUDA) with the provided seed.
* Keep cudnn deterministic or disabled for non‑det ops if needed.

### 3) Run it (3 seeds)

From repo root:

```bash
PYTHONHASHSEED=0 python -u experiments/train_dt.py --config configs/dt_3seed_compact.yaml --seed 12 --deterministic
PYTHONHASHSEED=0 python -u experiments/train_dt.py --config configs/dt_3seed_compact.yaml --seed 42 --deterministic
PYTHONHASHSEED=0 python -u experiments/train_dt.py --config configs/dt_3seed_compact.yaml --seed 2026 --deterministic
```

### 4) What gets produced

* Logs: `experiments/logs/dt_3seed/`
* Metrics: `experiments/logs/dt_3seed/metrics.json` (includes `regret_val` so `early_stop_on: regret` works)
* Checkpoint: `experiments/checkpoints/dt_chkpt_{seed}_{sha}.pt` (one per seed)

### 5) Quick notes

* **Update budget:** `total_updates: 10000` is intentionally small—fast to run but still meaningful for comparing against **V2+** and  **filtered BC** .
* **Eval key:** ensure your eval writer sets `regret_val` in `metrics.json`.
* **CI smoke:** for the tiniest runs, point `dataset:` to your mini smoke subset; keep the same log/artifact paths so dashboards don’t break.

If you want, I can also draft a tiny `experiments/train_dt.py` snippet with the seeding + deterministic toggles wired in and your metric writer stub (so `regret_val` lands exactly where early stopping expects).
