# External benchmark

## Official sources reviewed

- [OREE official site](https://www.oree.com.ua/) and [market results](https://www.oree.com.ua/index.php/control/results_mo?lang=ukr) are the canonical Ukrainian price/publication context for this preview.
- [OREE market rules](https://www.oree.com.ua/index.php/web/11465) define publication timing and remain the authority for Ukrainian DAM workflow assumptions.
- [ENTSO-E Transparency Platform](https://www.entsoe.eu/data/transparency-platform/) is a useful European transparency benchmark, not a substitute for Ukrainian target rows.
- [Nord Pool](https://www.nordpoolgroup.com/) and its [data API](https://data-api.nordpoolgroup.com/index.html) demonstrate mature public market-data access patterns.
- [AEMO Quarterly Energy Dynamics](https://www.aemo.com.au/energy-systems/major-publications/quarterly-energy-dynamics-qed) demonstrates how mature market reports separate energy arbitrage from ancillary-service revenue.
- [`diffcp` 1.1.8 on PyPI](https://pypi.org/project/diffcp/1.1.8/) documents the C++ build prerequisite relevant to Windows reproducibility.

## Comparison

The project is strongest where it mirrors mature transparency practice:
provenance, point-in-time timestamps, explicit status, and separate observed vs
forecast artifacts. It is correctly weaker than production market platforms in
submission, settlement, ancillary services, and operational telemetry. Those
gaps must remain visible; external market examples inform product design but do
not validate Ukrainian economics or authorize execution.
