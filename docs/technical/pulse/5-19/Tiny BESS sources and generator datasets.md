I’m sharing this because there’s a cluster of modern, permissively licensed grid/storage sources that make great lightweight test/training traces and tools you can literally *plug into CI or a thesis* without fuss and stay under budget or size limits.

![Image](https://images.openai.com/static-rsc-4/iUh8_7kmMBrB3yUT2zqw8kOPe0LpoDmsYe-wm2q67uwA9babIgOX5eyVncGmKZQPoSZaFzZXsNVkwX___UQCD2wy6-VfhSY9MI_zaftQIwnk5Xq7wKHI515UPdKvxFwjVCBfuq3M05mkSYbj5Lj6bRbEm4zKu14xACjlYfqhidUtJu4FOyz9F0oyAHt0_26b?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/KvoNOq5oPQvhOk6M1Mnt4BwzD5MjwQpOsmi13N04xQzT2zlS1WppuMv-lpHqDlTf1apAeIoJQN6rNzO70Cfzg_wNKqMwQ-EUjKeIs2zZQBiyyFe_x90YFE26UyzCyZ1_cCH5gM2do1u2ZbfAToqY2-xCpeAC1yvJIlkt4xwy9RoMQ_s9kplgzHTAUmxkfcAb?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/ddJjeP5u1g5i5FTqvxzNj5_ztRoj5N7On1TvxSBQpUdlBe4ZBkPqPwYfEfvq6DY1KkafncUlHar8Uph2TdTvDaYfqeB6W7nALoU8oR0peBSCk3pBsvtPvbtBBcHm6dRuScUQ7Ht_fVNveMfBi_-QCeX3NfqQ6sPg4M2qLDehP-3VfDEc7pPi3b4gPH23bdXP?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/vvK6WD_qs7X5sLizBIHb6uc4XrVgEzzlfHaCbBVwCZ4zcweDA07Iv1VpJY-aWA8VXiQyaNooRwc17OQ35PRzJnaUn7R5V2lQn30OqWUkOvVaQ2QV8ee5Hgv2G8eBnD-W4bRnqKC0K1bMCmOc9CjDlrIOYHdAM0eAM1nltKgOtHYiKIbUoknXQ-lD3t_D1Mvs?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/kCHrBUjfLhp5gMHGEL-rVUkqEcIZw2fqT-Rm8G4bYsuELQBmQtmK-F5UzUyoR5KHqKVt-tKkikUrwqHo5nFHUAOFWSeYSmNQVjDEcQ3XbDxDhqBb8TlK9AWLb_Wkc5XhvTG4oeCAWcKLPrcxEZOjdYXbw2JZBVEvU6sA-16ln9HlYqBcaAkFNv0_SaM4PzrS?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/ZFkbqsFOk0c3eSyWtRhnIf5v4PnPnrDO6GkZuLun27knMvtEwuAab0fOZcPco9fn6z8JCeuRwc4E8_UMvRnc71191ARyx1uDRpfdfpwYQDVSM0i7QfMlt9TMJFPY5_0dsYqvpHsBBKZmyRzST9qDQBpyF61AqJsh7hy5s3QNjH6o2HBMq3zKpwvZduy1ksVb?purpose=fullsize)

First, there’s a real open‑source *probabilistic grid + storage simulator* called **[sandialabs/snl-progress on GitHub](https://github.com/sandialabs/snl-progress?utm_source=chatgpt.com)** that generates example ESS system data (charge/discharge, SOC dynamics, and generation mixes) using Python — great for synthesizing small CSV fixtures from real scenario structures. It’s BSD‑family friendly and meant for research use, and includes example system files you can adapt without writing your own models. ([GitHub](https://github.com/sandialabs/snl-progress?utm_source=chatgpt.com "sandialabs/snl-progress: The Probabilistic Grid Reliability ..."))

For simple market/storage dispatch traces, **[gschivley/battery_model on GitHub](https://github.com/gschivley/battery_model?utm_source=chatgpt.com)** is a MIT‑licensed dispatch demo built around NYISO hourly LMP data and battery profitability/dispatch logic. It comes with a `full_year_optimization_results.csv` you can subset to a weekly or 3‑minute slice for <1 MB CI fixtures. ([GitHub](https://github.com/gschivley/battery_model?utm_source=chatgpt.com "Sample project modeling battery storage and dispatch · ..."))

On the data‑prep side, **[IBM/beep-data-utils on GitHub](https://github.com/IBM/beep-data-utils/blob/main/README.md?utm_source=chatgpt.com)** gives you MIT‑licensed Python utilities to convert raw BEEP pickle battery datasets into unified CSVs — exactly what you’d need to extract tiny per‑cell time series for simulation/loaders with minimal code. ([GitHub](https://github.com/IBM/beep-data-utils/blob/main/README.md?utm_source=chatgpt.com "BEEP dataset preparation and training scripts"))

And for cell/aging time series that approximate real physical behavior, the **[Ruifeng‑Tan/BatteryLife on GitHub](https://github.com/Ruifeng-Tan/BatteryLife?utm_source=chatgpt.com)** project is a canonical battery life dataset integrating many sources with standardized structures; you can grab individual battery curves from Hugging Face/Zenodo to build MB‑scale slices. ([GitHub](https://github.com/Ruifeng-Tan/BatteryLife?utm_source=chatgpt.com "Ruifeng-Tan/BatteryLife"))

These all sit well below 50 MB when you trim to a week or month at high resolution and are under MIT/BSD‑style licenses, so they’re safe to drop into CI and academic repos without legal or size headaches. ([GitHub](https://github.com/sandialabs/snl-progress?utm_source=chatgpt.com "sandialabs/snl-progress: The Probabilistic Grid Reliability ..."))
