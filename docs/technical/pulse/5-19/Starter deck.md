# 30‑min research sprint (pick one)

* **DFL/DPF neighbor loss variant:** skim Solver‑Free‑DFL and sketch how a “schedule‑neighbor surrogate loss” would plug into your existing V2+ candidate library. Deliverable: a 5‑line diagram (inputs → neighbor selection → loss → backprop → metrics).
* **N‑BEATSx variant:** outline a single global‑panel config to compare vs TFT on your current windows. Deliverable: one YAML/JSON config stub + the exact metrics you’ll log (MAE, pinball, regret).

# One‑pass “outstanding” doc (finishable today)

* **2‑page spec:** “How V2+ feeds DFL safely.”

  Include: problem statement, data flow (Bronze→NB→GFT), candidate library interface, regret scoring, promotion rules, and risks. Aim for ~600–800 words, single pass.

# One conversation to start

* **Pick one:**

1. *Team/mentor:* ask for a 20‑min sanity check on the neighbor‑loss plan.
2. *PR about diploma:* short note offering a visual of “oracle‑regret before/after” for a blog blurb.
3. *Collab ask:* a quick ping for a labeled day of market data to validate the regret metric.

---
