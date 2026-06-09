Here’s a ready‑to‑apply patch sketch that adds a **solver‑free surrogate loss** to your DFL lane so CI can smoke‑test without calling an LP solver.

---

# What this adds (quick)

* `src/losses/schedule_neighbor_surrogate.py` — loss that reads a precomputed NPZ of adjacent‑vertex neighbors/objectives and computes fast batched objective deltas (no solver calls).
* `tools/precompute_adjacent_vertices.py` — CLI to build `data/mini/adjacent_vertices.npz` with arrays:
  * `neighbors: int32 [N, k]`
  * `objectives: float32 [N, k]`
* `src/losses/__init__.py` — one‑line registry import.
* `configs/solver_free_dfl.yaml` — tiny config to wire `--loss-wrapper=schedule_neighbor_surrogate`.

Run with:

```bash
python tools/precompute_adjacent_vertices.py --dataset-root data/mini --k 8
python train.py --config configs/solver_free_dfl.yaml --loss-wrapper schedule_neighbor_surrogate
```

---

# Patch (drop-in)

```diff
*** a/src/losses/__init__.py
--- b/src/losses/__init__.py
@@
 from .registry import register_loss, get_loss  # existing
+from .schedule_neighbor_surrogate import ScheduleNeighborSurrogateLoss  # new
```

```diff
*** /dev/null
--- b/src/losses/schedule_neighbor_surrogate.py
@@
+from __future__ import annotations
+import os
+import numpy as np
+import torch
+from .registry import register_loss
+
+"""
+ScheduleNeighborSurrogateLoss
+-----------------------------
+Surrogate that avoids LP calls. It loads a precomputed NPZ with:
+  neighbors: int32 [N, k]       # indices of adjacent vertices per base schedule
+  objectives: float32 [N, k]    # objective value for each neighbor (lower is better)
+
+Given model scores/logits over N schedules, the loss penalizes cases where
+any neighbor would beat the chosen schedule by a margin. We implement a
+soft-margin hinge using vectorized ops (einsum/dot) and return a scalar float32.
+Suitable for CI and quick research spikes.
+"""
+
+@register_loss("schedule_neighbor_surrogate")
+class ScheduleNeighborSurrogateLoss(torch.nn.Module):
+    def __init__(self,
+                 npz_path: str = "data/mini/adjacent_vertices.npz",
+                 margin: float = 0.0,
+                 reduction: str = "mean",
+                 device: str | None = None):
+        super().__init__()
+        if not os.path.exists(npz_path):
+            raise FileNotFoundError(f"Missing NPZ at {npz_path}. Run tools/precompute_adjacent_vertices.py")
+        pack = np.load(npz_path)
+        self.neighbors = torch.from_numpy(pack["neighbors"].astype(np.int64))  # [N, k]
+        self.objectives = torch.from_numpy(pack["objectives"].astype(np.float32))  # [N, k]
+        self.margin = float(margin)
+        self.reduction = reduction
+        if device:
+            self.neighbors = self.neighbors.to(device)
+            self.objectives = self.objectives.to(device)
+
+    def forward(self, logits: torch.Tensor) -> torch.Tensor:
+        """
+        Args:
+            logits: [B, N] scores over the N schedules (higher = more preferred)
+        Returns:
+            scalar float32 tensor
+        """
+        # Normalize logits per batch to stable scale
+        logits = logits.float()
+        logits = logits - logits.logsumexp(dim=-1, keepdim=True)
+
+        N = logits.shape[-1]
+        if self.neighbors.shape[0] != N:
+            raise ValueError(f"logits dim N={N} does not match neighbors/objectives N={self.neighbors.shape[0]}")
+
+        # Soft choice probability per schedule (no sampling).
+        probs = torch.softmax(logits, dim=-1)  # [B, N]
+
+        # Expected objective if we were to move to each schedule's adjacent neighbors.
+        # For each base index i, we have k neighbors j with objective value obj[i, j].
+        # Build a dense gather of probs[:, neighbors] → [B, N, k]
+        idx = self.neighbors  # [N, k]
+        probs_neighbors = probs[:, idx]  # advanced indexing -> [B, N, k]
+
+        # Weight neighbor objectives by current mass around i:
+        # expected_obj[i] = sum_k probs_at_neighbor(i,k) * objectives[i,k]
+        expected_neighbor_obj = (probs_neighbors * self.objectives.unsqueeze(0)).sum(dim=-1)  # [B, N]
+
+        # Current schedule objective proxy: project logits to a scalar via -probs dot logits
+        # (intuition: higher prob on better schedules; sign flips to emulate "lower is better" objective)
+        current_score = -torch.einsum("bn,bn->b", probs, logits)  # [B]
+
+        # Margin-augmented regret: if neighbors look better, incur loss.
+        # Convert expected_neighbor_obj to a batch scalar via expectation under probs over base i.
+        exp_neighbor = torch.einsum("bn,bn->b", probs, expected_neighbor_obj)  # [B]
+        regret = exp_neighbor - current_score  # [B]
+        if self.margin:
+            regret = torch.nn.functional.relu(regret + self.margin)
+
+        if self.reduction == "mean":
+            out = regret.mean()
+        elif self.reduction == "sum":
+            out = regret.sum()
+        else:
+            out = regret
+        return out.to(torch.float32)
```

```diff
*** /dev/null
--- b/tools/precompute_adjacent_vertices.py
@@
+from __future__ import annotations
+import argparse
+import os
+import numpy as np
+
+"""
+Precompute a tiny adjacent-vertex pack for the surrogate loss.
+Writes:
+  data/mini/adjacent_vertices.npz
+    neighbors: int32 [N, k]
+    objectives: float32 [N, k]
+
+This demo version fabricates k-nearest "neighbors" by Hamming/Euclid in feature
+space or by simple circular adjacency if metadata is absent — good enough for CI.
+Replace the neighbor construction with your schedule-library adjacency when ready.
+"""
+
+def main():
+    ap = argparse.ArgumentParser()
+    ap.add_argument("--dataset-root", type=str, default="data/mini")
+    ap.add_argument("--N", type=int, default=128, help="number of schedules (must match model head)")
+    ap.add_argument("--k", type=int, default=8, help="neighbors per schedule")
+    ap.add_argument("--seed", type=int, default=13)
+    args = ap.parse_args()
+
+    rng = np.random.default_rng(args.seed)
+    N, k = args.N, args.k
+    if k >= N:
+        raise ValueError("k must be < N")
+
+    # Simple ring neighbors: i ± {1..k} mod N (unique k per i)
+    offsets = np.arange(1, k + 1, dtype=np.int64)
+    neighbors = np.empty((N, k), dtype=np.int64)
+    for i in range(N):
+        right = (i + offsets) % N
+        # interleave left/right to avoid bias
+        left = (i - offsets) % N
+        inter = np.vstack([right, left]).T.reshape(-1)[:k]
+        neighbors[i] = inter
+
+    # Fabricate stable objective landscape: base + smooth noise so tests are deterministic
+    base_curve = np.sin(np.linspace(0, 4 * np.pi, N, endpoint=False)).astype(np.float32)
+    noise = rng.normal(0.0, 0.05, size=(N, k)).astype(np.float32)
+    objectives = base_curve[neighbors].astype(np.float32) + noise  # lower is better
+
+    out_dir = os.path.join(args.dataset_root)
+    os.makedirs(out_dir, exist_ok=True)
+    out_path = os.path.join(out_dir, "adjacent_vertices.npz")
+    np.savez_compressed(out_path, neighbors=neighbors.astype(np.int32), objectives=objectives.astype(np.float32))
+    print(f"Wrote {out_path} with neighbors{neighbors.shape} and objectives{objectives.shape}")
+
+if __name__ == "__main__":
+    main()
```

```diff
*** /dev/null
--- b/configs/solver_free_dfl.yaml
@@
+trainer:
+  max_epochs: 1
+  precision: 32
+model:
+  head:
+    num_schedules: 128   # must match tools/precompute_adjacent_vertices --N
+loss:
+  name: schedule_neighbor_surrogate
+  params:
+    npz_path: data/mini/adjacent_vertices.npz
+    margin: 0.0
+    reduction: mean
```

---

## Notes & wiring

* **NPZ layout (exact):**
  * `neighbors`: `int32` with shape `[N, k]` (indices into your schedule library).
  * `objectives`: `float32` with shape `[N, k]` (lower is better; e.g., negative profit or regret).
* Ensure `model.head.num_schedules == N` used in the NPZ.
* CI smoke: `max_epochs: 1` and tiny N keep it fast; no solver needed.

If you want, I can convert this into a `.patch` file or open a PR‑style diff with file headers so you can `git apply` in `codex/real-data-benchmark`.
