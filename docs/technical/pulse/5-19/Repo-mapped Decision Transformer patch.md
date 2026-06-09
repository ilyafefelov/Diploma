Here’s a compact, copy‑paste patch + run guide to add a **deterministic Decision Transformer** to your `codex/real-data-benchmark` lane and run it per seed × 4 rolling windows.

![diagram](https://i.imgur.com/6bQ9P2Y.png)

# Minimal patch

## 1) `src/policy.py`

Add a small, self‑contained DT policy (causal transformer over (obs, action, reward) tokens; returns next‑action logits). Keep it framework‑agnostic to your repo, Torch‑only.

```python
# src/policy.py
import os
import torch
import torch.nn as nn
import torch.nn.functional as F

class DecisionTransformerPolicy(nn.Module):
    """
    Tiny DT: autoregressive over (obs, action, reward) triplets.
    forward(obs, actions, rewards, context_len=20) -> logits for next action.
    Shapes:
      obs:      [B, T, O]
      actions:  [B, T, A]  (one-hot or continuous; head can be adapted)
      rewards:  [B, T, 1]
    """
    def __init__(
        self,
        obs_dim: int,
        action_dim: int,
        d_model: int = 128,
        n_layers: int = 3,
        n_heads: int = 4,
        context_len: int = 20,
        dropout: float = 0.1,
    ):
        super().__init__()
        self.obs_dim = obs_dim
        self.action_dim = action_dim
        self.d_model = d_model
        self.context_len = context_len

        self.obs_emb = nn.Linear(obs_dim, d_model)
        self.act_emb = nn.Linear(action_dim, d_model)
        self.rew_emb = nn.Linear(1, d_model)

        self.pos_emb = nn.Embedding(context_len * 3, d_model)

        encoder_layer = nn.TransformerEncoderLayer(
            d_model=d_model, nhead=n_heads, dim_feedforward=4*d_model,
            dropout=dropout, batch_first=True, activation="gelu", norm_first=True
        )
        self.transformer = nn.TransformerEncoder(encoder_layer, num_layers=n_layers)

        self.head = nn.Linear(d_model, action_dim)

    def _pack_triplets(self, obs, actions, rewards):
        # Interleave [o0,a0,r0, o1,a1,r1, ...]
        B, T, _ = obs.shape
        z = torch.stack([
            self.obs_emb(obs), self.act_emb(actions), self.rew_emb(rewards)
        ], dim=2)                      # [B, T, 3, d]
        z = z.reshape(B, T*3, -1)      # [B, 3T, d]
        return z

    def forward(self, obs, actions, rewards, context_len=20):
        """
        Returns logits for the *next* action at the last timestep.
        If sequence > context_len, we take the last window.
        """
        x = self._pack_triplets(obs, actions, rewards)  # [B, 3T, d]
        B, L, D = x.shape
        if L > context_len * 3:
            x = x[:, -context_len*3:, :]
            L = x.shape[1]

        pos_ids = torch.arange(L, device=x.device).unsqueeze(0).expand(B, L)
        x = x + self.pos_emb(pos_ids)

        attn_mask = torch.triu(torch.ones(L, L, device=x.device), diagonal=1).bool()
        x = self.transformer(x, mask=attn_mask)

        # Take the last token (… r_{t}) as query to predict a_{t+1}
        last = x[:, -1, :]                 # [B, d]
        logits = self.head(last)           # [B, action_dim]
        return logits
```

## 2) `experiments/train_dt.py`

Add a DT training entrypoint (reads your dataset windows; trains ~10k updates; early‑stops on regret). Include determinism flags.

```python
# experiments/train_dt.py
import os, argparse, json, hashlib, time, random
import numpy as np
import torch
from torch import optim
from torch.utils.data import DataLoader
from src.policy import DecisionTransformerPolicy
from src.data import load_window_dataset  # assumes you already have this
from src.eval import evaluate_regret      # assumes you already have this

def set_deterministic(seed: int):
    os.environ["PYTHONHASHSEED"] = str(seed)
    torch.use_deterministic_algorithms(True)
    random.seed(seed); np.random.seed(seed); torch.manual_seed(seed); torch.cuda.manual_seed_all(seed)

def make_sha_stub():
    # Use short Git SHA if available
    try:
        import subprocess
        sha = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"]).decode().strip()
    except Exception:
        sha = "nogit"
    return sha

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", type=str, required=True)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--window", type=int, required=True)
    ap.add_argument("--deterministic", action="store_true")
    args = ap.parse_args()

    if args.deterministic:
        set_deterministic(args.seed)

    with open(args.config) as f:
        cfg = json.load(f)

    device = "cuda" if torch.cuda.is_available() else "cpu"

    # Hyperparams exposed in config
    d_model     = cfg.get("d_model", 128)
    n_layers    = cfg.get("n_layers", 3)
    n_heads     = cfg.get("n_heads", 4)
    context_len = cfg.get("context_len", 20)
    lr          = cfg.get("lr", 1e-3)
    total_updates = cfg.get("total_updates", 10_000)  # thesis‑safe
    batch_size  = cfg.get("batch_size", 64)
    early_on    = cfg.get("early_stop_on", "regret")

    # Load rolling window dataset
    ds_train, ds_val, meta = load_window_dataset(window_id=args.window, context_len=context_len)
    train_loader = DataLoader(ds_train, batch_size=batch_size, shuffle=True, drop_last=True)
    val_loader   = DataLoader(ds_val,   batch_size=batch_size, shuffle=False)

    obs_dim    = meta["obs_dim"]
    action_dim = meta["action_dim"]

    model = DecisionTransformerPolicy(
        obs_dim=obs_dim, action_dim=action_dim,
        d_model=d_model, n_layers=n_layers, n_heads=n_heads, context_len=context_len
    ).to(device)

    opt = optim.AdamW(model.parameters(), lr=lr)

    best_score = float("inf")  # lower regret is better
    best_path  = None
    step = 0
    sha = make_sha_stub()

    model.train()
    while step < total_updates:
        for batch in train_loader:
            obs, actions, rewards, target_next_actions = [x.to(device) for x in batch]
            logits = model(obs, actions, rewards, context_len=context_len)
            loss = F.cross_entropy(logits, target_next_actions.argmax(dim=-1)) \
                   if target_next_actions.dim()==2 else F.mse_loss(logits, target_next_actions)
            opt.zero_grad(set_to_none=True); loss.backward(); opt.step()
            step += 1
            if step % 500 == 0 or step >= total_updates:
                # quick validation via regret
                model.eval()
                with torch.no_grad():
                    regret = evaluate_regret(model, val_loader, context_len=context_len, device=device)
                model.train()

                if early_on == "regret" and regret < best_score:
                    best_score = regret
                    best_path = f"dt_chkpt_{args.window}_{args.seed}_{sha}.pt"
                    torch.save({"model": model.state_dict(),
                                "meta": meta,
                                "cfg": cfg,
                                "score": best_score}, best_path)
            if step >= total_updates:
                break

    if best_path is None:
        # Save last if no improvement occurred (should be rare)
        best_path = f"dt_chkpt_{args.window}_{args.seed}_{sha}.pt"
        torch.save({"model": model.state_dict(), "meta": meta, "cfg": cfg, "score": None}, best_path)

    print(json.dumps({"best_ckpt": best_path, "best_score": best_score}))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
```

## 3) `configs/dt_repo_mapped.yaml` (JSON or YAML; loader expects JSON here)

Expose the knobs the brief asked for. Keep values small/cheap.

```json
{
  "d_model": 128,
  "n_layers": 3,
  "n_heads": 4,
  "context_len": 20,
  "lr": 0.001,
  "batch_size": 64,
  "total_updates": 10000,
  "early_stop_on": "regret"
}
```

> If you prefer YAML, switch the reader accordingly.

## 4) Wire up imports

If your repo uses a package layout, ensure `src/__init__.py` exists and `experiments/` is a module or on `PYTHONPATH`. If `src.data` / `src.eval` names differ, adjust the two import lines in `train_dt.py`.

# Determinism snippet (already in the train entrypoint)

* `PYTHONHASHSEED` and `torch.use_deterministic_algorithms(True)` are applied when `--deterministic` is passed.

# Run matrix (seeds × 4 windows)

Use the three seeds `[12, 42, 2026]` and windows `[0,1,2,3]`:

```bash
# from repo root
set -a; export PYTHONHASHSEED=0; set +a

for s in 12 42 2026; do
  for w in 0 1 2 3; do
    python -u experiments/train_dt.py \
      --config configs/dt_repo_mapped.json \
      --seed $s \
      --window $w \
      --deterministic
  done
done
```

Each run produces a checkpoint:

```
dt_chkpt_{window}_{seed}_{sha}.pt
```

# How this slots into your thesis lane

* **Where it fits:** after your strict LP/oracle regret scoring, this DT gives a cheap, offline policy learner baseline trained on logged trajectories (your candidate schedules & outcomes).
* **Why this design:** tiny, deterministic transformer (d_model=128, n_layers=3, context_len=20) is fast enough to run per window and seed on a single GPU/CPU, keeping the runs “thesis‑safe”.
* **Evaluation you already have:** reuse your `evaluate_regret` against strict/oracle to report pass/fail per rolling window.

If you want, I can adapt the `train_dt.py` loader to your exact dataset calls (names/shapes) and drop in a ready‑to‑commit patch bundle.
