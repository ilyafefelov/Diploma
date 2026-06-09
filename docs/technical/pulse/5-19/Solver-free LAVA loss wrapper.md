Here’s a drop‑in, solver‑free LAVA loss you can paste into `src/losses/solver_free_lava.py` and use in your DFL/BESS pipeline without calling an LP during training. It compares your predicted cost vector against the optimal vertex and its precomputed adjacent vertices, enforcing a margin so the model learns to rank the true solution above neighbors.

![Simple diagram showing z* and adjacent vertices with margin idea](https://chatgpt.com/attachment)

```python
# src/losses/solver_free_lava.py
import torch
import torch.nn as nn
import torch.nn.functional as F

class SolverFreeLAVA(nn.Module):
    """
    LAVA loss (Loss via Adjacent Vertex Alignment), solver-free.
    Expects precomputed optimal vertex z_star and its adjacent vertices per instance.

    batch_meta:
      - 'z_star': FloatTensor (B, n_vars)
      - Either:
          'adjacent_verts': FloatTensor (B, K, n_vars)
          'adj_mask' (optional): BoolTensor (B, K) where False marks padded neighbors
        OR sparse/basis format:
          'adj_basis_idx': LongTensor (B, K, S)   # var indices per neighbor (S nonzeros)
          'adj_basis_vals': FloatTensor (B, K, S) # values per neighbor
          'n_vars': int
          set use_basis_format=True
    """
    def __init__(self, epsilon: float = 0.0, reduction: str = "mean", use_basis_format: bool = False):
        super().__init__()
        self.epsilon = float(epsilon)
        self.reduction = reduction
        self.use_basis_format = use_basis_format

    def forward(self, preds: torch.Tensor, batch_meta: dict) -> torch.Tensor:
        # preds: (B, n_vars) -> predicted cost vector ĉ
        c_hat = preds
        z_star = batch_meta["z_star"].to(c_hat.device)

        # ĉ^T z*
        obj_star = (c_hat * z_star).sum(dim=1, keepdim=True)  # (B,1)

        if not self.use_basis_format:
            # dense adjacent vertices
            Zadj = batch_meta["adjacent_verts"].to(c_hat.device)  # (B,K,n_vars)
            obj_adj = torch.einsum("bv,bkv->bk", c_hat, Zadj)     # (B,K)

            # optional mask for padded neighbors
            adj_mask = batch_meta.get("adj_mask", None)
            if adj_mask is not None:
                # set masked positions to obj_star (zero margin) so they don't contribute
                obj_adj = torch.where(adj_mask.to(c_hat.device), obj_adj, obj_star.expand_as(obj_adj))
        else:
            # sparse/basis format: compute ĉ^T z_adj quickly
            obj_adj = _sparse_adj_obj_dot(c_hat, batch_meta)      # (B,K)

        # margins m = ĉ^T z* - ĉ^T z_adj  (want >= -epsilon)
        margins = obj_star - obj_adj                              # (B,K)
        # hinge with epsilon: loss = relu(m + epsilon)
        losses = F.relu(margins + self.epsilon)                   # (B,K)

        # reduce over neighbors then batch
        per_instance = losses.mean(dim=1)                         # (B,)
        if self.reduction == "mean":
            return per_instance.mean()
        if self.reduction == "sum":
            return per_instance.sum()
        return per_instance


def _sparse_adj_obj_dot(c_hat: torch.Tensor, meta: dict) -> torch.Tensor:
    """
    Compute ĉ^T z_adj for K sparse neighbors per instance.
    meta keys:
      adj_basis_idx: (B,K,S) long
      adj_basis_vals: (B,K,S) float
      n_vars: int (optional; used for safety asserts)
    """
    idx = meta["adj_basis_idx"].to(c_hat.device)   # (B,K,S)
    vals = meta["adj_basis_vals"].to(c_hat.device) # (B,K,S)

    # gather ĉ entries at neighbor nonzero positions
    # reshape to (B,1,n_vars) so we can batch-gather with indices (B,K,S)
    c_exp = c_hat.unsqueeze(1)                     # (B,1,n_vars)
    gathered = torch.gather(c_exp.expand(-1, idx.size(1), -1), 2, idx)  # (B,K,S)
    dot = (gathered * vals).sum(dim=2)             # (B,K)
    return dot
```

**Usage (example):**

```python
lava = SolverFreeLAVA(epsilon=0.0, reduction="mean", use_basis_format=False)

loss = lava(preds=c_hat, batch_meta={
    "z_star": z_star,                       # (B, n_vars)
    "adjacent_verts": Z_adj,                # (B, K, n_vars) padded to k_max
    "adj_mask": adj_mask_bool               # (B, K) True for real neighbors
})
```

**Notes (short):**

* Feed `float32` arrays; pad K to a small `k_max` (4–8) and pass a mask.
* Keep `epsilon ≥ 0` for robustness.
* Never call a solver in `forward()`—precompute `z_star` and adjacent vertices offline (once per instance or epoch).
* Returns a scalar if `reduction="mean"`, else per‑instance vector.

Want me to wire this into your current DFL V2+ training loop (loss registry + batch collation) and sketch the precompute step for adjacent vertices specific to your BESS LP?
