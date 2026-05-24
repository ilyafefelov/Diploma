# Neighbor Surrogate Prototype Files

- Source URL: https://chatgpt.com/share/6a12d1b8-96a0-832c-ad4d-f6488851e582
- Share ID: `6a12d1b8-96a0-832c-ad4d-f6488851e582`
- Fetched at: `2026-05-24T11:26:07+00:00`
- Extraction: embedded ChatGPT share payload, text parts only; tool outputs remain redacted where the share redacts them.

## Full Text

### Message 1: assistant (model_editable_context)

{
  "content_type": "model_editable_context",
  "model_set_context": ""
}

### Message 2: assistant (text)

Here’s a tiny, CI‑fast drop‑in you can add to your repo to try a **schedule‑neighbor surrogate loss** (vectorized, masked, K‑padded) plus a micro tool + script + smoke test.

---

# src/losses/schedule_neighbor_surrogate.py
```python
import torch
import torch.nn as nn
import torch.nn.functional as F

class ScheduleNeighborSurrogate(nn.Module):
    """
    Inputs
      preds:        (B, V) float32 — model scores/utilities per vertex
      z_star:       (B, V) float32 — oracle/strict optimum one-hot or dense target over vertices
      adjacent_verts: (B, K, V) float32 — per-instance K neighbor vertices as one-hot/dense mixtures (padded)
      adj_mask:     (B, K) bool — True where a neighbor exists (mask padded slots)

    Computes
      obj_star = <z_star, preds>
      obj_adj  = einsum over K neighbors: for each neighbor z_k, <z_k, preds>
      hinge    = relu(margin + obj_adj - obj_star) on valid neighbors
    Returns
      mean per-instance hinge loss (scalar)
    """
    def __init__(self, margin: float = 0.0):
        super().__init__()
        self.margin = margin

    def forward(self,
                preds: torch.Tensor,
                z_star: torch.Tensor,
                adjacent_verts: torch.Tensor,
                adj_mask: torch.Tensor) -> torch.Tensor:
        # Shapes
        B, V = preds.shape
        _, K, V2 = adjacent_verts.shape
        assert V == V2, "V mismatch"

        preds = preds.float()
        z_star = z_star.float()
        adjacent_verts = adjacent_verts.float()
        adj_mask = adj_mask.bool()

        # obj_star: (B,)
        obj_star = torch.einsum("bv,bv->b", z_star, preds)

        # obj_adj_raw: (B, K)
        obj_adj_raw = torch.einsum("bkv,bv->bk", adjacent_verts, preds)

        # mask out padded neighbors
        neg_inf = torch.finfo(obj_adj_raw.dtype).min
        obj_adj = torch.where(adj_mask, obj_adj_raw, torch.full_like(obj_adj_raw, neg_inf))

        # hinge on valid neighbors only
        # broadcast obj_star to (B, K)
        margin = self.margin
        hinge = F.relu(margin + obj_adj - obj_star.unsqueeze(1))

        # zero out invalid slots (avoid -inf issues post-relu)
        hinge = torch.where(adj_mask, hinge, torch.zeros_like(hinge))

        # per-instance mean over valid K, then batch mean
        denom = adj_mask.float().sum(dim=1).clamp_min(1.0)  # avoid div0
        per_inst = hinge.sum(dim=1) / denom
        return per_inst.mean()
```

---

# tools/precompute_adjacent_vertices.py
```python
#!/usr/bin/env python3
"""
Tiny CLI that writes data/mini/adj_vertices.npz with:
  - adjacent_verts: (B,K,V) float32
  - adj_mask:       (B,K)   bool
  - z_star:         (B,V)   float32
  - preds:          (B,V)   float32  (toy)
Defaults: B=6, V=10, K=4. Uses a toy generator deterministic for CI.
"""
import argparse, os
import numpy as np

def make_toy(B=6, V=10, K=4, seed=0):
    rs = np.random.RandomState(seed)
    # Make z_star as one-hot argmax of a random “true utility”
    true_u = rs.randn(B, V).astype(np.float32)
    z_star_idx = true_u.argmax(axis=1)
    z_star = np.zeros((B, V), dtype=np.float32)
    z_star[np.arange(B), z_star_idx] = 1.0

    # Build neighbors: for each instance choose up to K indices near the argmax + random
    adjacent_verts = np.zeros((B, K, V), dtype=np.float32)
    adj_mask = np.zeros((B, K), dtype=bool)
    for b in range(B):
        cand = set()
        peak = z_star_idx[b]
        cand.update([peak])  # include optimum as a neighbor candidate
        while len(cand) < K and len(cand) < V:
            cand.add(rs.randint(0, V))
        idxs = list(cand)[:K]
        for k, v_idx in enumerate(idxs):
            adjacent_verts[b, k, v_idx] = 1.0
            adj_mask[b, k] = True

    # Toy model preds close to true_u but noisy
    preds = true_u + 0.05 * rs.randn(B, V).astype(np.float32)

    return adjacent_verts, adj_mask, z_star, preds

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/mini/adj_vertices.npz")
    ap.add_argument("--B", type=int, default=6)
    ap.add_argument("--V", type=int, default=10)
    ap.add_argument("--K", type=int, default=4)
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    adj, mask, z_star, preds = make_toy(B=args.B, V=args.V, K=args.K, seed=args.seed)
    np.savez_compressed(args.out,
                        adjacent_verts=adj.astype(np.float32),
                        adj_mask=mask,
                        z_star=z_star.astype(np.float32),
                        preds=preds.astype(np.float32))
    print(f"Wrote {args.out} with shapes adj {adj.shape}, mask {mask.shape}, z_star {z_star.shape}, preds {preds.shape}")

if __name__ == "__main__":
    main()
```

---

# scripts/run_one_window.py
```python
#!/usr/bin/env python3
"""
Minimal single-window runner:
 - loads toy data (or your real window tensors) for B,V,K
 - runs N seeds training a dummy linear head toward z_star using the surrogate loss
 - early-stops when no best-regret improvement for `patience` steps
 - writes metrics.json per seed in ./runs/<window_id>/seed_<s>/
This is deliberately compact and CPU-fast for CI.
"""
import argparse, json, os, numpy as np, torch
from pathlib import Path
from src.losses.schedule_neighbor_surrogate import ScheduleNeighborSurrogate

def load_npz(path):
    d = np.load(path)
    return {k: d[k] for k in d.files}

def regret_like(preds, z_star):
    # Higher is better objective; regret = best - chosen; we simulate by using dot with z_star
    obj_star = (z_star * preds).sum(-1)  # (B,)
    # pretend model chooses argmax vertex from preds distribution
    chosen_idx = preds.argmax(-1)
    chosen = torch.zeros_like(preds)
    chosen[torch.arange(preds.shape[0]), chosen_idx] = 1.0
    obj_chosen = (chosen * preds).sum(-1)
    regret = (obj_star - obj_chosen).mean().item()
    return float(regret)

def train_one(seed, data_path, lr=5e-2, steps=200, patience=20, margin=0.0, device="cpu"):
    torch.manual_seed(seed)
    d = load_npz(data_path)
    preds0 = torch.tensor(d["preds"]).to(device)           # (B,V)
    z_star = torch.tensor(d["z_star"]).to(device)          # (B,V)
    adj = torch.tensor(d["adjacent_verts"]).to(device)     # (B,K,V)
    mask = torch.tensor(d["adj_mask"]).to(device)          # (B,K)

    # Simple trainable residual head on top of initial preds
    B, V = preds0.shape
    head = torch.nn.Linear(V, V, bias=False).to(device)
    torch.nn.init.zeros_(head.weight)

    loss_fn = ScheduleNeighborSurrogate(margin=margin)
    opt = torch.optim.Adam(head.parameters(), lr=lr)

    best = {"regret": float("inf"), "step": -1}
    no_improve = 0

    for t in range(steps):
        opt.zero_grad(set_to_none=True)
        preds = preds0 + head(preds0)  # cheap, deterministic mapping
        loss = loss_fn(preds, z_star, adj, mask)
        loss.backward()
        opt.step()

        with torch.no_grad():
            r = regret_like(preds, z_star)
            if r + 1e-9 < best["regret"]:
                best = {"regret": r, "step": t}
                no_improve = 0
            else:
                no_improve += 1

        if no_improve >= patience:
            break

    return {"best_regret": best["regret"], "best_step": best["step"], "final_step": t, "seed": seed}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--window_id", default="demo")
    ap.add_argument("--data", default="data/mini/adj_vertices.npz")
    ap.add_argument("--seeds", type=int, default=3)
    ap.add_argument("--patience", type=int, default=15)
    ap.add_argument("--steps", type=int, default=200)
    ap.add_argument("--lr", type=float, default=5e-2)
    ap.add_argument("--margin", type=float, default=0.0)
    args = ap.parse_args()

    out_root = Path("runs") / args.window_id
    out_root.mkdir(parents=True, exist_ok=True)

    for s in range(args.seeds):
        metrics = train_one(seed=s,
                            data_path=args.data,
                            lr=args.lr,
                            steps=args.steps,
                            patience=args.patience,
                            margin=args.margin)
        run_dir = out_root / f"seed_{s}"
        run_dir.mkdir(parents=True, exist_ok=True)
        with open(run_dir / "metrics.json", "w") as f:
            json.dump(metrics, f, indent=2)
        print(f"[seed {s}] best_regret={metrics['best_regret']:.6f} @ step {metrics['best_step']}")

if __name__ == "__main__":
    main()
```

---

# tests/test_schedule_neighbor_surrogate.py
```python
import numpy as np
import torch
from src.losses.schedule_neighbor_surrogate import ScheduleNeighborSurrogate

def test_zero_loss_when_preds_match_optimum(tmp_path):
    # Deterministic tiny sample akin to data/mini
    B, V, K = 3, 8, 4
    # Make z_star one-hot, preds equal so the optimum equals chosen score
    z_star = torch.zeros(B, V, dtype=torch.float32)
    z_star[0, 2] = 1.0
    z_star[1, 5] = 1.0
    z_star[2, 1] = 1.0

    preds = torch.randn(B, V, dtype=torch.float32)
    # Force preds to match z_star's winner exactly by adding large bump
    bump = torch.zeros_like(preds)
    bump[torch.arange(B), z_star.argmax(dim=1)] = 10.0
    preds = preds + bump

    # Neighbors include the optimum + random padded
    adjacent_verts = torch.zeros(B, K, V, dtype=torch.float32)
    adj_mask = torch.zeros(B, K, dtype=torch.bool)
    for b in range(B):
        adjacent_verts[b, 0, z_star[b].argmax().item()] = 1.0
        adj_mask[b, 0] = True
        # leave K-1 as padded False

    loss_fn = ScheduleNeighborSurrogate(margin=0.0)
    loss = loss_fn(preds, z_star, adjacent_verts, adj_mask)
    assert torch.isfinite(loss)
    # Since obj_adj (optimum) == obj_star, hinge=relu(0)=0
    assert loss.item() == 0.0
```

---

## How to wire it up quickly

- Save the four files exactly as above (paths matter).
- Generate the toy data:
  ```bash
  python tools/precompute_adjacent_vertices.py --out data/mini/adj_vertices.npz --B 12 --V 16 --K 4 --seed 0
  ```
- Run the single window:
  ```bash
  python scripts/run_one_window.py --window_id w0 --data data/mini/adj_vertices.npz --seeds 3
  ```
  → writes `runs/w0/seed_*/metrics.json`.

- Run the smoke test (pytest):
  ```bash
  pytest -q tests/test_schedule_neighbor_surrogate.py
  ```

This keeps everything vectorized (einsum), uses masked K‑padding (default **K=4**, float32), and stays CI‑fast. If you want, I can also adapt `run_one_window.py` to load your real rolling‑window tensors from the repo and plug into your V2+ pipeline without changing your core training loop.
