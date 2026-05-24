# Rolling-Window Notebook With Tiny NPZ

- Source URL: https://chatgpt.com/share/6a12d1ad-3f74-832c-b5c0-0ec40e9017ec
- Share ID: `6a12d1ad-3f74-832c-b5c0-0ec40e9017ec`
- Fetched at: `2026-05-24T11:26:07+00:00`
- Extraction: embedded ChatGPT share payload, text parts only; tool outputs remain redacted where the share redacts them.

## Full Text

### Message 1: assistant (model_editable_context)

{
  "content_type": "model_editable_context",
  "model_set_context": ""
}

### Message 2: assistant (text)

Here’s a tiny, **solver‑free Decision‑Focused Learning (DFL)** spike you can drop into any repo to sanity‑check the **LAVA loss** end‑to‑end—no LP/Gurobi calls, just NumPy + PyTorch. It: (1) builds a mini NPZ, (2) defines a minimal MLP that predicts cost vectors, (3) implements batched LAVA exactly via `einsum` + `relu`, (4) runs a **3‑seed × 4‑window** rolling experiment, and (5) emits `metrics.json` per run.

---

### File: `scripts/spike_run.py`
```python
#!/usr/bin/env python3
import argparse, json, os, random, time
import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F

# ----------------------------
# 1) Tiny NPZ generator (CI sample)
# ----------------------------
def ensure_demo_npz(path="data/mini/solver_free_sample.npz", N=8, K=4, num_vars=32, d=6, seed=0):
    if os.path.exists(path): 
        return path
    os.makedirs(os.path.dirname(path), exist_ok=True)
    rng = np.random.RandomState(seed)
    costs = rng.randn(N, num_vars).astype(np.float32)
    # toy z_star: first N one‑hot rows (sum=1)
    z_star = np.eye(num_vars, dtype=np.float32)[:N]
    # K neighbors = cyclic shifts of z_star
    adj = np.stack([np.roll(z_star, shift=k+1, axis=1)[:N] for k in range(K)], axis=1).astype(np.float32)
    mask = np.ones((N, K), dtype=np.uint8)
    feats = rng.randn(N, d).astype(np.float32)
    np.savez_compressed(path, costs=costs, z_star=z_star, adjacent_verts=adj, adj_mask=mask, feats=feats)
    return path

# ----------------------------
# 2) Minimal MLP to predict cost vectors
# ----------------------------
class TinyMLP(nn.Module):
    def __init__(self, d_in, v_out, hidden=128):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(d_in, hidden),
            nn.ReLU(),
            nn.Linear(hidden, hidden),
            nn.ReLU(),
            nn.Linear(hidden, v_out),
        )
    def forward(self, x):  # (B, d) -> (B, V)
        return self.net(x)

# ----------------------------
# 3) Batched Solver‑Free LAVA loss (einsum/gemm)
#    Matches the ops you requested
# ----------------------------
class SolverFreeLAVA(nn.Module):
    def __init__(self, epsilon=0.0, reduction='mean'):
        super().__init__()
        self.epsilon = float(epsilon)
        self.reduction = reduction
    def forward(self, c_hat, batch):
        # c_hat: (B, V)
        z_star = batch['z_star'].to(c_hat.device)          # (B, V)
        Zadj   = batch['adjacent_verts'].to(c_hat.device)  # (B, K, V)
        mask   = batch.get('adj_mask', None)

        # obj_star: (B,1)
        obj_star = torch.einsum('bv,bv->b', c_hat, z_star).unsqueeze(1)   # (B,1)
        # obj_adj: (B,K)
        obj_adj  = torch.einsum('bv,bkv->bk', c_hat, Zadj)                # (B,K)

        margins = obj_star - obj_adj                                      # (B,K)
        per_neighbor = F.relu(margins + self.epsilon)                     # (B,K)

        if mask is not None:
            per_neighbor = per_neighbor * mask.to(per_neighbor.device)

        per_instance = per_neighbor.sum(dim=1)                            # sum over neighbors
        if self.reduction == 'mean':
            return per_instance.mean()
        elif self.reduction == 'sum':
            return per_instance.sum()
        else:
            return per_instance  # (B,)

# ----------------------------
# Data loader from NPZ
# ----------------------------
def load_npz(npz_path):
    D = np.load(npz_path)
    out = {k: torch.from_numpy(D[k]) for k in D.files}
    return out

def split_windows(N, num_windows=4):
    # Even-ish rolling windows (toy)
    idx = np.arange(N)
    return np.array_split(idx, num_windows)

def batchify(tensor, idx):
    return tensor[idx] if tensor is not None else None

def make_batch(D, idx):
    return {
        'feats':          batchify(D.get('feats'), idx).float(),
        'costs':          batchify(D.get('costs'), idx).float(),
        'z_star':         batchify(D.get('z_star'), idx).float(),
        'adjacent_verts': batchify(D.get('adjacent_verts'), idx).float(),
        'adj_mask':       batchify(D.get('adj_mask'), idx).float(),
    }

# ----------------------------
# 4) Train/eval over 3 seeds × 4 windows
# ----------------------------
def pick_pred_action_solver_free(c_true, Z_cands):
    """
    c_true:  (B,V)
    Z_cands: (B,C,V) where C = 1 + K (z_star + neighbors)
    returns one-hot-ish predicted z (B,V) chosen by argmin_z c_true^T z
    """
    # (B,C) = einsum over V
    obj = torch.einsum('bv,bcv->bc', c_true, Z_cands)
    best_idx = obj.argmin(dim=1)  # (B,)
    B, V = c_true.shape
    out = torch.zeros(B, V, device=c_true.device, dtype=Z_cands.dtype)
    out.scatter_(1, best_idx.view(-1,1).repeat(1,V), 0.0)  # just to allocate
    # gather chosen vertices
    chosen = Z_cands[torch.arange(B), best_idx]            # (B,V)
    return chosen

def regret_metric(c_true, z_pred, z_star):
    """
    regret = c_true^T z_pred - c_true^T z_star  >= 0
    """
    obj_pred = torch.einsum('bv,bv->b', c_true, z_pred)
    obj_star = torch.einsum('bv,bv->b', c_true, z_star)
    return (obj_pred - obj_star).clamp_min(0.0)  # (B,)

def run_once(npz_path, window_id, seed, device='cpu', lr=1e-3, steps=300, batch_size=32, epsilon=0.0):
    torch.manual_seed(seed); np.random.seed(seed); random.seed(seed)
    Dcpu = load_npz(npz_path)
    N, V = Dcpu['costs'].shape
    d    = Dcpu['feats'].shape[1]

    windows = split_windows(N, num_windows=4)
    idx = windows[window_id]
    if len(idx) == 0:
        raise RuntimeError("Empty window split in this tiny demo.")

    batch = make_batch(Dcpu, idx)
    feats, costs, z_star, Zadj, mask = batch['feats'], batch['costs'], batch['z_star'], batch['adjacent_verts'], batch['adj_mask']
    # Candidate set C = {z_star} ∪ adjacent_verts
    Zc = torch.cat([z_star.unsqueeze(1), Zadj], dim=1)  # (B, 1+K, V)

    model = TinyMLP(d_in=d, v_out=V).to(device)
    loss_fn = SolverFreeLAVA(epsilon=epsilon, reduction='mean')
    opt = torch.optim.Adam(model.parameters(), lr=lr)

    feats = feats.to(device); costs = costs.to(device)
    z_star = z_star.to(device); Zadj = Zadj.to(device); mask = mask.to(device); Zc = Zc.to(device)

    # simple full-batch or mini-batch loop (tiny data, so full-batch ok)
    for _ in range(steps):
        opt.zero_grad()
        c_hat = model(feats)                 # (B,V)
        batch_for_loss = {'z_star': z_star, 'adjacent_verts': Zadj, 'adj_mask': mask}
        loss = loss_fn(c_hat, batch_for_loss)
        loss.backward()
        opt.step()

    # Evaluation: solver-free argmin over precomputed candidate vertices using TRUE costs
    z_pred = pick_pred_action_solver_free(costs, Zc)      # (B,V)
    regret = regret_metric(costs, z_pred, z_star)         # (B,)
    return float(regret.mean().item())

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--data", type=str, default="data/mini/solver_free_sample.npz")
    p.add_argument("--seed", type=int, default=12)
    p.add_argument("--window", type=int, default=0)  # 0..3
    p.add_argument("--steps", type=int, default=300)
    p.add_argument("--lr", type=float, default=1e-3)
    p.add_argument("--epsilon", type=float, default=0.0)
    p.add_argument("--device", type=str, default="cpu")
    p.add_argument("--run_name", type=str, default="")
    args = p.parse_args()

    npz_path = ensure_demo_npz(args.data)
    t0 = time.time()
    regret_mean = run_once(npz_path, window_id=args.window, seed=args.seed,
                           device=args.device, lr=args.lr, steps=args.steps, epsilon=args.epsilon)
    dt = time.time() - t0
    run_name = args.run_name or f"lava_smoke_w{args.window}_s{args.seed}"
    out = {
        "run_name": run_name,
        "lane": "v2_plus",
        "window": args.window,
        "seed": args.seed,
        "regret_mean": round(regret_mean, 6),
        "train_seconds": round(dt, 3)
    }
    os.makedirs("runs", exist_ok=True)
    with open(os.path.join("runs", f"{run_name}.metrics.json"), "w") as f:
        json.dump(out, f)
    print(json.dumps(out, indent=2))

if __name__ == "__main__":
    main()
```

---

### Quick start (CI‑safe)
```bash
# from repo root
python -m venv .venv && . .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -q torch numpy

# single run
python -u scripts/spike_run.py --window 0 --seed 12

# 3 seeds × 4 windows
for w in 0 1 2 3; do
  for s in 12 34 56; do
    python -u scripts/spike_run.py --window $w --seed $s
  done
done

# outputs like: runs/lava_smoke_w0_s12.metrics.json
```

---

### How this maps to the paper (plain‑English)
- **What’s optimized?** A network predicts a **cost vector** \( \hat c \) for a linear program.  
- **LAVA loss (solver‑free):** Encourages \( \hat c^\top z^\* \le \hat c^\top z \) for each adjacent vertex \( z \) near the oracle \( z^\* \). In code: `einsum` to get objectives, **margin** = star − neighbor, then `relu(margin + epsilon)`, summed over neighbors.  
- **Evaluation (no solver):** Choose the action **only** from the candidate set \( \{z^\*\} \cup \text{adjacent\_verts} \) by argmin under **true** costs \( c \) and report **regret** \( c^\top z_{\text{pred}} - c^\top z^\* \).

---

If you want, I can also add a matching Jupyter notebook (`spike_notebook.ipynb`) that executes the same steps and can be run via:
```
jupyter nbconvert --to notebook --execute spike_notebook.ipynb --ExecutePreprocessor.timeout=600
```
