I’m sharing this because having **a focused set of DOI‑backed, high‑impact papers — original architectures, a 2024–2025 survey, and applied work with reproducible code — will fast‑track your ability to map datasets, metrics, and evaluation choices before you write a line of code.**

![Image](https://images.openai.com/static-rsc-4/3wm-JNjO9x7lUpSYzSPBwWDMlJiE6MpNm8v5dumRb8GWSnH9CSsny2Vn4xeLBalsCo8ltEpGeX68WkLWC9q4wEN_GfZssxZtYCA6Yt7XicBSgI0w2ADc0ZYzWzapLC4SPIFwU18UHpVIotSzB6MujTrLMbgl4lT3q3kQjxRfP3vtb5OqzVxPQh2IYolsRQh0?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/_t09ik-FcxiUqOzF1QdlqdlSdz9zkKiEIhfHPQ0fc0GUmXjDrXxDjC8uH0EcyGqdiUSS2DUX8zWcqfFV8yzJ95DcUx5WAkrretdJtFm3DIbXFmo2Q5bgw_6pPQBowVhkzvqfh7Ss0Wj6wqoU7OLiQM3tb0JxqYwXA0zdG7wR4D16PqiKNtxQvBnRrg8VlPED?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/OUjSjY5YxVLK3AaTCinbJRnz6eZoEywZz5oSdfAwb-25CSmp1nA2B4MJn78wzJdFeU5vIoUEf4xR9ZhT5okuj1z_UgPCUKhku6NpGATPKodOOri9ZlzIA_ursQobSySJdh5388XQb8sDVVm3LbPym8FPKytbKbC2lPosNRRuEiwvDUb_rNsWw69Qxj0nB96P?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/BaR15fq-80pLklnB-B864nDgsNdEFmGrKR4TrX6AAD3w6CybZWHHK5QRRcpNkEmwPkxl3k0pkuSLEWuZst4YfWNKZ26DCwwTfM80wM5PStghSDVcau26pSz-WoBB_wMoOiKw8PFn0iPmborlZdvIRFH1XPCrVOyMpUvgR_olxLa3toWUMP9UGAWgWQvraF86?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/CYi2kgAA4IKM5495vsABC5Elfy64ozrMIBkiYmSdsI4yJMFPLIbI1ooi10FUayVb-ikcE2NeXZTI3rj9nVJBIKmb2FkChIMMEMKMYYbgl8R_Tj_1sNiN7qfDtz1o13La3U4rocm-mTtACX6mli7PCY0NBICjmu7nOoGMJ3uF2lmWyyBJjkzl2uKBaFBlVwvC?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/8xcc9E7GarAdqsklzP0z88lxNx2GaVaPd4RHIk1_oADgnQs5jOlVGK8qXXKOS7CoGNRaprcZczHEmtgDaxPBkhbuM_r1KX0LkcFE-RhBJnpuKbOLmuFuLeEaswKUMac8tSD_ShgsmtZGG846Wreb7y-TSPOYDyj3QpYywu2w21YZjlOpi2rUUeG-hyGOzPN8?purpose=fullsize)

**Fundamental architectures you *must* read first (original sources):**

1. **N‑BEATS: Neural Basis Expansion Analysis for Interpretable Time Series Forecasting** — the foundational paper introducing N‑BEATS’ modular, residual MLP‑based architecture with interpretable components that set new benchmarks on M‑series competitions. ([arXiv](https://arxiv.org/abs/1905.10437?utm_source=chatgpt.com "N-BEATS: Neural basis expansion analysis for interpretable time series forecasting"))
   *ICLR 2020 (arXiv/DOI).*
2. **Temporal Fusion Transformers for Interpretable Multi‑horizon Time Series Forecasting** — the definitive paper that frames TFT as an attention‑augmented sequence‑to‑sequence model combining static and dynamic inputs for multi‑step forecasting with built‑in interpretability. ([ScienceDirect](https://www.sciencedirect.com/science/article/pii/S0169207021000637?utm_source=chatgpt.com "Temporal Fusion Transformers for interpretable multi ..."))
   *International Journal of Forecasting 37(4), DOI:10.1016/j.ijforecast.2021.03.012.*

**DOI‑backed extensions & applied models you’ll want on hand:**

3. **Neural Basis Expansion with Exogenous Variables (N‑BEATSx)** — extends N‑BEATS to handle external covariates, with electricity price forecasting results and reproducible code in journal repositories. ([ScienceDirect](https://www.sciencedirect.com/science/article/pii/S0169207022000413?utm_source=chatgpt.com "Neural basis expansion analysis with exogenous variables"))
   *Int. J. Forecast. (2023), DOI:10.1016/j.ijforecast.2022.03.001.*

**Recent comprehensive surveys (2024–2025) that contextualize both models:**

4. **Transformers for Time‑Series Forecasting: A Comprehensive Survey through 2024** — organizes the evolution of transformer‑based forecasting, challenges, and design patterns across tasks and datasets. ([ResearchGate](https://www.researchgate.net/publication/399324903_Transformers_for_time-series_forecasting_A_comprehensive_survey_through_2024?utm_source=chatgpt.com "(PDF) Transformers for time-series forecasting"))
5. **Deep Learning for Time Series Forecasting (2025)** — broad survey that situates N‑BEATS, TFT, and other paradigms in the broader DL forecasting landscape, highlighting strengths, weaknesses, and dataset/metric trends. ([Springer Link](https://link.springer.com/article/10.1007/s10462-025-11223-9?utm_source=chatgpt.com "A comprehensive survey of deep learning for time series ..."))

**Recent applied studies (2024–2025) with reproducible or accessible code links:**

6. **TFT for Weekly Retail Demand Forecasting** — multi‑horizon probabilistic forecasts demonstrating TFT’s practical utility and interpretability in retail sales; code usually accompanies arXiv submissions. ([arXiv](https://arxiv.org/abs/2511.00552?utm_source=chatgpt.com "Temporal Fusion Transformer for Multi-Horizon Probabilistic Forecasting of Weekly Retail Sales"))
7. **TFT & N‑BEATS comparisons in cryptocurrency forecasting** — shows an empirical head‑to‑head in volatile financial series with error metrics and code repositories tied to published DOI. ([Ranah Research](https://jurnal.ranahresearch.com/index.php/R2J/article/view/1949?utm_source=chatgpt.com "Prediksi Harga Cryptocurrency Menggunakan Algoritma ..."))
8. Domain‑specific forecasting (hydroelectric power, weather) where TFT enhancements or hybrid methods with N‑BEATS variants are evaluated — useful for cross‑domain metric alignment. ([ResearchGate](https://www.researchgate.net/publication/395890751_Time_series_forecasting_based_on_multi-criteria_optimization_for_model_and_filter_selection_applied_to_hydroelectric_power_plants?utm_source=chatgpt.com "Time series forecasting based on multi-criteria optimization ..."))

---

**Practical tip before coding:**
For each paper above, spend your first 15–30 minutes read­ing  **the abstract + methods + results tables** , and create a small matrix tracking:

* **Task & horizon** (univariate vs multivariate; point vs probabilistic)
* **Datasets used** (M3/M4, commodity prices, electricity, retail)
* **Metrics reported** (MAE, RMSE, MAPE, R², quantile losses)
* **External inputs/covariates** (static, known future, exogenous series)
  Mapping these will make downstream implementation and comparison both rigorous and defensible.
