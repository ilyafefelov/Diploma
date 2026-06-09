
Here’s a practical, copy‑pasteable **reproducibility checklist** you can run through before kicking off big experiments (e.g., N‑BEATSx + TFT → DFL/DT). It’s designed so every run becomes a citable, re‑buildable artifact for your thesis.

---

# Reproducibility checklist (10 items)

1. **Data provenance**

* Write a `DATASET.md` with: source URLs, access date, license, original checksums, and any preprocessing scripts.
* Save raw → bronze → silver → gold hashes (e.g., SHA256) and keep scripts versioned.

2. **Seed control (full stack)**

* Fix seeds for Python, NumPy, PyTorch/TF, CUDA determinism flags; record library+driver versions.
* Example (PyTorch):
  ```python
  import os, random, numpy as np, torch
  seed=1337
  os.environ["PYTHONHASHSEED"]=str(seed)
  random.seed(seed); np.random.seed(seed)
  torch.manual_seed(seed); torch.cuda.manual_seed_all(seed)
  torch.backends.cudnn.deterministic=True
  torch.backends.cudnn.benchmark=False
  ```

3. **Environment capture**

* Freeze dependencies to a lockfile (`requirements.txt` + exact hashes or `poetry.lock`).
* Export the runtime: `pip freeze > requirements.txt`, `conda env export --from-history > environment.yml`.
* Consider a container: `Dockerfile` + pinned CUDA/cuDNN base image.

4. **Dataset splits (immutable)**

* Precompute and store split indices (train/val/test) as files with checksums.
* Document time‑based splits for forecasting (prevent leakage), e.g., “train ≤ 2023‑12‑31, val = 2024‑Q1, test = 2024‑Q2”.

5. **Baseline replication first**

* Implement and re‑run a known baseline (e.g., vanilla TFT or N‑BEATSx from paper settings).
* Save the exact config and metrics; verify you can reproduce within expected variance before adding your ideas.

6. **Metric stability checks**

* For each config, run ≥3 seeds; log mean, std, and 95% CI for key metrics (sMAPE, MASE, RMSE).
* Pre‑register acceptance criteria (e.g., “new model must beat baseline by ≥1.0 sMAPE with non‑overlapping CIs”).

7. **Hyperparameter & config logging**

* Store every run’s config as a single JSON/YAML snapshot (no implicit defaults).
* Include: model architecture (blocks, hidden sizes), learning rate schedule, optimizer, early‑stopping rules, loss functions, scaler choices, window lengths, covariates used, and feature engineering flags.

8. **Hardware & runtime notes**

* Record GPU model(s), CPU, RAM, CUDA/cuDNN, driver, batch size vs. max memory, epoch time, and wall‑clock total.
* Keep a tiny `HARDWARE.md` updated automatically (write once at run start).

9. **Exact run capture (commands + artifacts)**

* Use a single entrypoint command logged verbatim:
  ```
  python train.py --config configs/tft_elec.yaml --seed 1337 --run_id 2026-05-06T18-20Z
  ```
* Archive outputs in a structured folder:
  ```
  runs/
    2026-05-06T18-20Z/
      config.yaml
      git_commit.txt
      env/requirements.txt
      data_splits/
      metrics.csv
      checkpoints/
      plots/
      hardware.json
      README_run.md
  ```
* Save the exact Git commit SHA and “dirty” diff.

10. **Make it citable (DOIs + code tag)**

* Create a lightweight release per milestone (`v0.1-nbeatsx-tft-baseline`) and tag in Git.
* Upload the release archive (configs + env lock + scripts + README_run.md + immutable splits + metrics) to a DOI‑minting repository (e.g., Zenodo) to get a DOI.
* Put the DOI badge in your thesis and in the repo README. Each key figure/table in the thesis should reference: (a) Git tag, (b) Run ID, (c) DOI of the artifact bundle.

---

## Minimal starter kit (drop in your repo)

* `configs/` — all YAML configs (baseline + ablations).
* `scripts/`
  * `prepare_data.py` (writes split index files + checksums)
  * `train.py` (reads a single config file only)
  * `eval.py` (can re‑score from a saved checkpoint)
  * `summarize_runs.py` (aggregates seeds → mean/std/CI CSV)
* `tools/`
  * `env_capture.py` (spits `hardware.json`, `requirements.txt`)
  * `checksum.py` (hashes raw/processed datasets)
* `RUNBOOK.md` — step‑by‑step to reproduce one figure/table.
* `CITATION.md` — how to cite, with the current DOI(s).

---

## Quick ablation plan (fits the checklist)

* **Data windows** : lookback {96, 168, 336}; horizon {24, 48}.
* **Covariates** : weather on/off; calendar features on/off.
* **Model** : TFT vs. N‑BEATSx; N‑BEATSx block counts {2, 4}; basis {generic, trend+seasonal}.
* **Optimization** : Adam vs. AdamW; LR {1e‑3, 3e‑4}; weight decay {0, 1e‑4}.
* **Loss** : MSE vs. MAE vs. pinball (τ∈{0.1,0.5,0.9}) for quantiles.

Each line above = separate config file; run 3 seeds; aggregate with CIs.

---

## Ready‑to‑paste “run README” template

```
# Run: 2026-05-06T18-20Z
Git commit: abcd123 (dirty: no)
Config: configs/tft_elec.yaml
Seeds: [1337, 2029, 777]
Hardware: 1×A100 40GB, CUDA 12.1, driver 550.xx
Data: UCI_Electricity v1.0 (fetched 2026-05-03), SHA256=<...>
Splits: train ≤ 2024-12-31; val=2025-Q1; test=2025-Q2
Command:
python train.py --config configs/tft_elec.yaml --seed 1337

Results (sMAPE ↓):
seed 1337: 11.42
seed 2029: 11.37
seed 777 : 11.51
mean=11.43, std=0.06, 95% CI [11.39, 11.47]

Artifacts:
- checkpoints/model_best.pt
- metrics.csv
- plots/learning_curve.png
- env/requirements.txt
- hardware.json
- data_splits/*.json (with SHA256)

Reproduce:
1) conda env create -f environment.yml && conda activate thesis
2) python scripts/prepare_data.py --dataset uci_elec --out data_splits/
3) python train.py --config configs/tft_elec.yaml --seed 1337
```

---

If you want, I can generate:

* pinned `requirements.txt` / `environment.yml`,
* a cookie‑cutter repo structure with these files,
* skeleton configs for **N‑BEATSx** and  **TFT** , plus a CI job that checks deterministic replay and uploads a release bundle ready for DOI minting.
