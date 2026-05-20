Here’s a plug‑and‑play way to make your DFL/“V2+” pipeline **solver‑free at training time** by precomputing LP adjacent vertices once offline, then loading them like regular tensors during loss evaluation.

---

# Why this helps (quickly)

Training with decision‑focused losses usually calls an LP/MIP solver *inside* every loss step—slow and flaky. If you precompute, per instance, the **optimal vertex** and a small set of **adjacent vertices** (local neighbors on the LP polytope), your loss can approximate the local geometry with  **zero runtime solver calls** . That slashes compute, keeps CI stable, and fits neatly into your existing Schedule/Value Learner V2+.

---

# What to add to your repo

## 1) Precompute script

`src/data/precompute_adjacent_vertices.py` (CLI below) runs offline on N instances:

* solve once per instance → get `z_star`
* run simplex pivots to enumerate up to `K` adjacent vertices
* save a compressed `.npz` bundle

**CLI (example):**

```bash
python -m src.data.precompute_adjacent_vertices \
  --out data/solver_free/adj_vertices.npz \
  --benchmark random_lp \
  --n 1000 \
  --k_max 6
```

**Minimal skeleton:**

```python
# src/data/precompute_adjacent_vertices.py
import argparse, time, json, numpy as np

def solve_offline(cost_vec, model_factory):
    # e.g., call Gurobi/CBC once; return dense z_star
    return model_factory(cost_vec).solve_to_vertex()

def enumerate_adjacent(z_star, model_factory, k_max):
    # pivot steps around z_star; return list of dense neighbor vectors
    return model_factory(None).adjacent_vertices_from(z_star, limit=k_max*2)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--benchmark", default="random_lp")
    ap.add_argument("--n", type=int, default=1000)
    ap.add_argument("--k_max", type=int, default=6)
    args = ap.parse_args()

    # 1) build/generate problem instances (feats_i, c_i)
    feats, costs = load_or_sample_instances(args.benchmark, args.n)   # shape: (N, F), (N, V)

    N, V = costs.shape
    K = args.k_max
    z_stars       = np.zeros((N, V), dtype=np.float32)
    adj_padded    = np.zeros((N, K, V), dtype=np.float32)
    adj_mask      = np.zeros((N, K), dtype=np.uint8)

    # optional compact/basis buffers (allocate lazily if you use them)

    for i, (feat_i, c_i) in enumerate(zip(feats, costs)):
        z_star = solve_offline(c_i, model_factory=make_model)
        z_stars[i] = z_star

        neighbors = enumerate_adjacent(z_star, make_model, K)
        keep = neighbors[:K]
        for j, v in enumerate(keep):
            adj_padded[i, j] = v.astype(np.float32)
            adj_mask[i, j] = 1

    meta = dict(
        k_max=K, num_vars=V, problem_type=args.benchmark,
        created_by="precompute_adjacent_vertices.py",
        timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )

    np.savez_compressed(
        args.out,
        feats=feats.astype(np.float32),
        costs=costs.astype(np.float32),
        z_star=z_stars,
        adjacent_verts=adj_padded,
        adj_mask=adj_mask,
        meta=json.dumps(meta).encode("utf-8"),
    )

if __name__ == "__main__":
    main()
```

**Suggested `.npz` schema** (`data/solver_free/adj_vertices.npz`):

* `feats`: `float32 (N, num_features)` – optional, for reproducibility
* `costs`: `float32 (N, num_vars)`
* `z_star`: `float32 (N, num_vars)`
* `adjacent_verts`: `float32 (N, K, num_vars)` (padded)
* `adj_mask`: `uint8 (N, K)` (1 if real neighbor, else 0)
* `meta`: `bytes` (JSON string with `{'k_max','num_vars','problem_type','created_by','timestamp'}`)

**Optional compact/basis if V is large/sparse:**

* `adj_basis_idx`: `int16 (N, K, nnz_per_neighbor)`
* `adj_basis_vals`: `float32 (N, K, nnz_per_neighbor)`
* `adj_nnz`: `int16 (N, K)`

> Rule of thumb: keep **dense (N,K,V)** for `V ≤ ~256` and `K ≤ 8`; switch to basis if `V >> 1k` or neighbor vectors are very sparse.

---

## 2) Dataloader hook

`src/dataloaders/solver_free_loader.py` — small NumPy → torch loader.

```python
# src/dataloaders/solver_free_loader.py
import numpy as np, torch
from torch.utils.data import Dataset, DataLoader

class AdjacentVertexDataset(Dataset):
    def __init__(self, npz_path):
        self._data = np.load(npz_path, allow_pickle=False)
        self.feats  = self._data["feats"]
        self.costs  = self._data["costs"]
        self.z_star = self._data["z_star"]
        self.adj    = self._data["adjacent_verts"]
        self.mask   = self._data["adj_mask"]

    def __len__(self): return self.costs.shape[0]

    def __getitem__(self, i):
        return dict(
            feats=torch.from_numpy(self.feats[i]),
            costs=torch.from_numpy(self.costs[i]),
            z_star=torch.from_numpy(self.z_star[i]),
            adjacent_verts=torch.from_numpy(self.adj[i]),
            adj_mask=torch.from_numpy(self.mask[i]),
        )

def collate(batch):
    # K already fixed by file; if mixing files, pad K here
    keys = batch[0].keys()
    out = {k: torch.stack([b[k] for b in batch], 0) for k in keys}
    return out
```

---

## 3) Loss wrapper

`src/losses/solver_free_lava.py` — consume `adjacent_verts` & `adj_mask`. The idea: compare your model’s predicted cost (or schedule/value) around `z_star` vs each adjacent vertex, weighting with `adj_mask`, and form a regret‑style loss without running a solver.

```python
# src/losses/solver_free_lava.py
import torch
def solver_free_loss(batch, model, epsilon=1e-6):
    # predict per-variable costs or utilities
    pred_c = model(batch["feats"])                 # (B, V)
    z_star = batch["z_star"].float()               # (B, V)
    adj    = batch["adjacent_verts"].float()       # (B, K, V)
    mask   = batch["adj_mask"].float()             # (B, K)

    # regret at z_star vs neighbors under predicted cost:
    # r_k = <pred_c, adj_k - z_star>
    delta = adj - z_star.unsqueeze(1)              # (B, K, V)
    r = (pred_c.unsqueeze(1) * delta).sum(-1)      # (B, K)
    r = r * mask                                   # ignore padded neighbors

    # hinge or softplus variant works well
    loss = torch.nn.functional.softplus(r).sum(dim=1).mean()
    return loss
```

---

## 4) Config + docs

* `configs/solver_free_dfl.yaml`
  ```yaml
  precomputed_adj_path: data/solver_free/adj_vertices.npz
  k_neighbors: 6
  epsilon: 1e-6
  python_version_hint: "3.10"
  ```
* `docs/technical/solver_free_onepage.md`
  One‑page explainer: LP vertices, “adjacent” via one‑pivot simplex moves, why local neighbor regret is a good training signal, and how it maps to the code above.

---

# File tree changes (minimal)

```
src/
  data/
    precompute_adjacent_vertices.py
  dataloaders/
    solver_free_loader.py
  losses/
    solver_free_lava.py
configs/
  solver_free_dfl.yaml
docs/
  technical/
    solver_free_onepage.md
data/
  solver_free/
    adj_vertices.npz        # produced by the precompute script
  mini/
    adj_vertices_tiny.npz   # tiny sample (e.g., N=8) for smoke tests
```

---

# Practical knobs

* **Memory** : float32 + `np.savez_compressed` is usually fine; include `adj_mask` to avoid padding artifacts.
* **K (neighbors)** : start with `K=6–8`. If training is noisy, increase `K` or switch to the compact basis.
* **Where to run** : do the precompute offline (CI job or a single beefy host), and check in a **tiny** `data/mini/*.npz` for unit tests.
* **Integration** : in your trainer, replace the old DFL loss with `solver_free_loss(...)` and point the dataloader to the `.npz`.

---

If you want, I can rewrite these snippets to your exact `ilyafefelov/Diploma` layout and names so you can copy‑paste without edits.
