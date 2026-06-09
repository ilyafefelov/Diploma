# Evaluating Battery Degradation Models in Rolling-Horizon BESS Arbitrage Optimization

Source: https://www.mdpi.com/1996-1073/19/4/1056

DOI: https://doi.org/10.3390/en19041056

Authors: Chase Humiston, Mehmet Cetin, Anderson Rodrigo de Queiroz.

Published: 2026-02-18 in *Energies*, 19(4), article 1056.

Why archived as Markdown: the MDPI PDF endpoint returned HTTP 403 during local archiving, so this file records a durable citation and research note for the literature corpus. The article page and ResearchGate metadata identify the paper as open access under CC BY 4.0.

Research note:

The paper evaluates rolling-horizon BESS arbitrage under multiple degradation model classes: Linear-Calendar, Energy-Throughput, and Cycle-Based/rainflow. The reported framing is directly relevant to this project because our current Level 1 simulator uses a throughput/EFC degradation-cost proxy. The article supports describing that choice as an academically acceptable MVP proxy, while also warning that degradation model choice can materially change BESS valuation and that cycle/rainflow models need careful calibration before making strong digital-twin claims.

Use in thesis:

- Justify the current model label: Level 1 feasibility and degradation-cost proxy, not full electrochemical digital twin.
- Explain why throughput/EFC is suitable for the 8-week benchmark slice.
- List rainflow/cycle-based degradation and capacity-fade calibration as future work before stronger asset-health claims.
