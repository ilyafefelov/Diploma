# Solver-Free DFL Spike, Thesis-Safe

- Source URL: https://chatgpt.com/share/6a12d191-b000-832e-929e-1b7bb01e518b
- Share ID: `6a12d191-b000-832e-929e-1b7bb01e518b`
- Fetched at: `2026-05-24T11:26:07+00:00`
- Extraction: embedded ChatGPT share payload, text parts only; tool outputs remain redacted where the share redacts them.

## Full Text

### Message 1: assistant (model_editable_context)

{
  "content_type": "model_editable_context",
  "model_set_context": ""
}

### Message 2: assistant (text)

Solver-Free Decision-Focused Learning for Linear LAVA loss equation (4) implementation site:arxiv.org OR site:github.com

### Message 3: tool (text)

The output of this plugin was redacted.

### Message 4: tool (text)

The output of this plugin was redacted.

### Message 5: assistant (text)

I’m sharing this because there’s a newly published method that dramatically cuts the cost of **decision‑focused learning** for linear optimization by removing the need to invoke a solver during training, instead leveraging geometry to define a *solver‑free loss* you can implement in PyTorch and validate quickly. citeturn0search0turn0search1

image_group{"layout":"carousel","aspect_ratio":"16:9","query":["Solver-Free Decision-Focused Learning for Linear Optimization Problems linear optimization polytope adjacent vertices diagram","LP feasible region adjacent vertices geometry optimization illustration","PyTorch model training for predict-then-optimize machine learning optimization","regret loss geometry in linear programming optimization"],"num_per_query":1}

The core idea is that for a **linear program**, a basic feasible solution *z* is optimal **if and only if** its cost is no worse than every **adjacent vertex** on the feasible polytope; this geometric statement replaces expensive solver runs. citeturn0search7

A recently posted paper — *Solver‑Free Decision‑Focused Learning for Linear Optimization Problems* (arXiv 2505.22224, NeurIPS 2025) — formalizes this and introduces a loss called **LAVA (Loss via Adjacent Vertex Alignment)**. citeturn0search0

- It revisits **decision‑focused learning (DFL)**, where a model learns to predict cost vectors that lead to good decisions on downstream optimization tasks, using regret as the objective. citeturn0search7  
- Traditional DFL computes the optimization solution at each loss evaluation, which is the expensive part. citeturn0search1  
- The solver‑free loss works by comparing the predicted cost’s evaluation of the *true* optimal solution against **precomputed adjacent vertices** of that optimum — no resolver in the training loop. citeturn0search7

Concretely:  
- For each training instance with true parameters **c** and true optimal solution **z\***, the LAVA loss sums how much worse **z\*** under the predicted cost **ĉ** is relative to each adjacent vertex **z_adj** if that margin is positive. citeturn0search7  
- Mathematically:  
  \[
  \text{LAVA}(\hat{c}, z^{*}) = \sum_{z_{\text{adj}} \in Z_{\text{adj}}(z^{*})} \max(\hat{c}^\top z^{*} - \hat{c}^\top z_{\text{adj}}, 0)
  \]
  where \(Z_{\text{adj}}(z^{*})\) are the vertices adjacent to the true optimum. citeturn0search7  
- This yields a **batched, differentiable loss** in PyTorch that doesn’t require a solver during training, meeting the minimal‑dependency constraint.

The adjacent vertices must be **precomputed offline** — that step *can* call a solver but is outside training — and stored in a small NPZ for use as the reference set; this aligns perfectly with your plan to check in a tiny NPZ (e.g., N=8, K=4). citeturn0search9

There’s also a companion code repo linked to the original arXiv paper that implements this approach, which could serve as a practical reference. citeturn0search6

This strategy directly supports quick rolling‑window regret readouts and low‑cost CI smoke tests by obviating solver calls during training — exactly the pattern you’re preparing for.
