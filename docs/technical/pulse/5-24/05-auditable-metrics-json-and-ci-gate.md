# Auditable metrics.json and CI Gate

- Source URL: https://chatgpt.com/share/6a12d1dc-5b0c-8390-92db-407d8a53b832
- Share ID: `6a12d1dc-5b0c-8390-92db-407d8a53b832`
- Fetched at: `2026-05-24T11:26:07+00:00`
- Extraction: embedded ChatGPT share payload, text parts only; tool outputs remain redacted where the share redacts them.

## Full Text

### Message 1: assistant (model_editable_context)

{
  "content_type": "model_editable_context",
  "model_set_context": ""
}

### Message 2: assistant (text)

Here’s a tiny, drop‑in way to make your training/eval jobs emit **auditable metrics per checkpoint** and let CI decide pass/fail deterministically.

![A simple CI gating flow: job runs → emits metrics.json → CI gate aggregates → pass/fail](https://dummyimage.com/800x180/eeeeee/333333&text=Job+→+metrics.json+→+CI+gate+→+Pass/Fail)

# What & why (super short)
- **Problem:** flaky, ad‑hoc “looks good” evals.
- **Fix:** every run writes a **single JSON** per *(lane × seed × window)* with the exact keys CI expects. Then one **CI gate** aggregates and compares against stable references → clean pass/fail with diagnostics.

# Emit this JSON (copy exact shape)
```json
{"lane":"v2_plus","seed":12,"window":0,"git_sha":"{short_sha}","timestamp":"2026-05-24T12:34:56Z","metrics":{"regret_mean_ua":174.77,"regret_std_ua":2.31,"eval_steps":120}}
```
**Keys (must exist):**
- `lane`, `seed`, `window`, `git_sha`, `timestamp`
- `metrics.regret_mean_ua` (primary), `metrics.regret_std_ua`, `metrics.eval_steps`

# Minimal Python helper (drop in your eval loop)
```python
import json, os, time, subprocess

def short_sha():
    try:
        return subprocess.check_output(["git","rev-parse","--short","HEAD"]).decode().strip()
    except Exception:
        return "unknown"

def write_metrics_json(path, lane, seed, window, regret_mean_ua, regret_std_ua, eval_steps):
    payload = {
        "lane": lane,
        "seed": int(seed),
        "window": int(window),
        "git_sha": short_sha(),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "metrics": {
            "regret_mean_ua": float(regret_mean_ua),
            "regret_std_ua": float(regret_std_ua),
            "eval_steps": int(eval_steps),
        },
    }
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    return path
```

**Usage example (per seed × window):**
```python
out = f"experiments/logs/v2_plus/seed12/window0/metrics.json"
write_metrics_json(out, lane="v2_plus", seed=12, window=0,
                   regret_mean_ua=174.77, regret_std_ua=2.31, eval_steps=120)
```

# CI gate (single CLI)
Run this **after artifacts are uploaded** (e.g., GH Actions step):
```bash
python ci_acceptance_gate.py \
  --metrics-glob 'experiments/logs/**/metrics*.json' \
  --v2plus-ref 174.77 \
  --frozen-ref 206.37 \
  --strict-ref 310.58 \
  --strict-ref
```
- `--metrics-glob` finds all emitted files.
- The `*-ref` flags are your **reference regrets** (update as your baselines move).
- The gate prints a summary table and exits **non‑zero on failure**.

# Folder convention (simple & grep‑friendly)
```
experiments/logs/
  └─ {lane}/seed{N}/window{K}/metrics.json
```

# What you’ll get in CI
- Deterministic **pass/fail**.
- A concise **diff vs references** per lane (v2_plus, frozen, strict).
- Easy trend graphs later (the JSON schema is stable).

If you want, I can generate a ready‑to‑paste GH Actions step and a `ci_acceptance_gate.py` skeleton that enforces thresholds (e.g., fail if `regret_mean_ua` > ref by 1.5%).
