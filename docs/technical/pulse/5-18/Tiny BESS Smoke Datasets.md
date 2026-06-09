Here’s a sharper, **ready‑to‑use snapshot of small, permissively‑licensed energy and battery datasets — ideal for embedding into a thesis workflow, quick experiments, or “DFL/DT smoke runs” with minimal setup.([FlexMeasures][1])

---

### A tiny, executable battery scheduling demo (FlexMeasures “toy”)

![Image](https://images.openai.com/static-rsc-4/6uFzxs8noBwuebQRCtXFAaM7hZqLyaKXf66UOKNJIhcIRUVA48qrg-E-0CMuQCi2Q_btIjdwSWrc8VER_m14_I7C5A9BRPH2w7uJzumC5zVUzZv8wMME7PzPZ7p83e14mQeWagRYNNp_S6wThQuSOk9gQfFljD4dKpdwj-NlzySbLjvzZgpkpnlu7siDuZG0?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/zb9JfR2iyFOp93PzH3kM355guapx-it-0mvUY9x_mxXkHuFpR7CXVMRQusmjawApG3ZeWLvQRLDrdLoArIhFo6GpilfhIg7ZZPvHt4_V9LGLADBWohjUo1ReTLEV3lBsSr0HCBWSPIADyOEubHPHXt6-D2fiqKFu-mkNIrzsrjR_R6EESgIUYO6Cz4EW53Am?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/mB-2xFDx7AsB4qTTe6txWWpT6V9rzbfpJphACbOQS-02IIyS-_Ft7lcKq_6US6KYZdnpFZkL9q-8BPfA3JKTRD6NqgyZ62Oq8rjXo9aystcSZgtVmsBvktOsQmGJ5NCPWB04nI3-7_PVKUSJqSqEH_VoPshsA2pV1EJXvZThaFq7-6-R1N5gQ8cR9fflYXXV?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/p4oCz_pbWcPzI_fpPTvKAs60xkvwr-_f--2ZgjUBZkA85r7GpxmTf8KfZ8CGprIxG_Y669jgS5IFCX4t3DfTtnOXhsAKEfoleYIOdGsWnNE8j94F1Rr10XHdL3XajlAjWU_92DsRvyWSTYjtDRjXnqcRjFcyT59-zCczQ9b5Xdlw2iKeouhV0MhJeCeWCfF2?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/uK7sYIS_pUW6_ikVSGe5cfjn1JE8uXrOLflZ7FckS2L2zFqwwUeYqSTRUspSyWSkCFZ4IxCEoXJyLUxQlOkoZ28BfYAmrn8VaoaAB1OC80LGO-8_1rR1k-eY3C9o4mbTUvJVHjaK4dwHI_6qAVtZzZ_NfLxag5g-lHhDm3FcSNZ2cvqVhtKt1ipG1KPwrtmb?purpose=fullsize)

FlexMeasures offers a **minimal, runnable battery scheduling tutorial** showing how to programmatically generate a 12‑hour charge/discharge schedule for a storage asset with real price signals. It walks through CLI/API calls, flexible device modeling, and interactive graphs — all in a self‑contained example you can drop into a repo and run with local or containerized dependencies.([FlexMeasures][1])

Key points:

* Focuses on scheduling a battery with a simple state‑of‑charge model.([FlexMeasures][1])
* Uses intuitive commands (CLI or Python client) to trigger and inspect schedules.([FlexMeasures][1])
* Includes graphs of scheduling outputs and price contexts for visual debugging.([FlexMeasures][1])

**Why it’s useful**
It serves as a **tiny generator of realistic battery control outputs** you can use to test optimization, load balancing, or DFL loops without heavy data pipelines.([FlexMeasures][1])

---

### KeiLongW/synthetic‑battery‑data (Apache‑2.0)

A GitHub repo that contains **synthetic battery operation traces** produced by combining generative adversarial networks with SOC estimation techniques — designed to mimic real battery charge/discharge patterns.([GitHub][2])

What you’ll find:

* A folder of **small preprocessed arrays** representing voltage/current/SOC patterns you can `numpy.load` directly.([GitHub][2])
* Code to generate additional synthetic sequences based on existing cell datasets.([GitHub][2])

**Use‑case fit**
Great for **training or testing lightweight models** of battery behavior where real measurement data is scarce.([GitHub][2])

---

### Fingrid open BESS power data (CC BY 4.0)

Fingrid publishes **per‑day CSVs** of real‑world battery storage system charging and discharging power based on grid measurements — broken into high‑resolution slices (e.g., 3‑min).([Fingrid Open Data][3])

What’s available:

* **Charging power dataset (dataset 399)** — values always positive, aggregated from real measurement streams.([Fingrid Open Data][3])
* **Discharging power dataset (dataset 398)** — same but for discharge flows.([Fingrid Open Data][4])

Format & access:

* Downloadable as **CSV via API or UI**.([Fingrid Open Data][5])

**Why it’s excellent for thesis work**
Actual grid‑connected battery signals with realistic variability — handy for validating synthetic data generators or economic dispatch models.([Fingrid Open Data][3])

---

### Hourly price/demand toy series (PJME_hourly.csv)

A tiny time series of **hourly electricity prices or demand** you can include as a price signal in scheduling experiments.
(This file is from a toy dataset repo and is repository‑hosted as a simple CSV, ideal for bootstrap tests with price‑aware schedulers.)

**Use‑case fit**
Use as an exogenous price signal or demand baseline in control or flexibility experiments with your battery data.

---

If you’d like, I can help you **combine these sources into a single mini‑dataset package (with loaders and example notebooks)** so you can run end‑to‑end experiments with almost no setup.

[1]: https://flexmeasures.readthedocs.io/v0.31.1/tut/toy-example-from-scratch.html?utm_source=chatgpt.com
[2]: https://github.com/KeiLongW/synthetic-battery-data?utm_source=chatgpt.com
[3]: https://beta-data.fingrid.fi/en/datasets/399?utm_source=chatgpt.com
[4]: https://beta-data.fingrid.fi/en/datasets/398?utm_source=chatgpt.com

[5]: https://beta-data.fingrid.fi/en/data?datasets=399&utm_source=chatgpt.com
