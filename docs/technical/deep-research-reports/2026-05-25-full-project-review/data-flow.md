# Data Flow and Evidence Infographics

## Main System Flow

```mermaid
flowchart LR
    A["Source data: DAM rows, weather, tenant context, rules"] --> B["Bronze Dagster assets"]
    B --> C["Silver normalization and feature engineering"]
    C --> D["Forecast adapters: NBEATSx, TFT, official smoke lanes"]
    C --> E["Strict LP/oracle comparator"]
    D --> F["Schedule/value candidate library"]
    E --> F
    F --> G["V2+ schedule/value learner"]
    G --> H["Research packet: regret, value, rolling robustness"]
    H --> I["FastAPI read-model"]
    I --> J["Nuxt operator dashboard"]
    J --> K["Operator recommendation preview"]
    K --> L["No market payload: market_execution_enabled=false"]
```

## Safety and Governance Lanes

```mermaid
flowchart TB
    A["Candidate schedule or preview"] --> B["Pydantic strict contracts"]
    B --> C["Physical envelope: SOC, power, duration"]
    C --> D["Market-rule caps by regime"]
    D --> E{"Passes deterministic gates?"}
    E -- "yes" --> F["Read-model preview evidence"]
    E -- "no" --> G["Validation failure evidence"]
    F --> H["Dashboard display only"]
    G --> H
    H --> I["No execution API or order payload"]
```

## V13 Acquisition Gate

```mermaid
flowchart LR
    A["Required source families"] --> B["Explicit DAM publication receipts"]
    A --> C["Tenant/source safe-switch examples"]
    B --> D{"Row-level receipts ready?"}
    C --> E{"20 prior/train safe examples per tenant/source?"}
    D -- "no" --> F["data_acquisition_needed"]
    E -- "no" --> F
    F --> G["dt_lava_ready=false"]
    G --> H["permits_model_training=false"]
    H --> I["market_execution_enabled=false"]
```

## Experiment Evidence Ladder

```mermaid
flowchart BT
    A["CI/prototype smoke: LAVA NPZ"] --> B["Shadow research: DT sequence contract"]
    B --> C["Non-promoted challengers: TFT and Poland"]
    C --> D["Promoted offline evidence: V2+"]
    D --> E["Academic MVP: DAM read-model preview"]
    E --> F["Future only: market-submittable bidding"]
    F:::blocked
    classDef blocked fill:#f8d7da,stroke:#842029,color:#842029;
```

Interpretation:

- The project currently reaches the academic MVP/read-model level.
- It does not reach market-submittable bidding.
- The red future node is intentionally blocked by governance and source-readiness gates.

