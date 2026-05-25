# Infographics Index

Use these visual blocks in the thesis, defense notes, or demo-day deck.

These are code-native Mermaid infographics. I kept them deterministic because the local `imagegen` skill explicitly says simple diagrams, wireframes, and repo-native visuals are better produced directly in SVG/HTML/CSS/Mermaid than as generated bitmap assets.

## 1. What the System Is

```mermaid
flowchart LR
    A["Market and tenant evidence"] --> B["Dagster assets"]
    B --> C["Strict validation and LP/oracle comparator"]
    C --> D["V2+ offline schedule/value learner"]
    D --> E["FastAPI read-model"]
    E --> F["Nuxt operator preview"]
    F --> G["Human review only"]
```

Caption:

> The MVP is a source-governed DAM recommendation preview. It produces read-model evidence for a human operator, not market-submittable bids.

## 2. What Is Blocked

```mermaid
flowchart TB
    A["DT/LAVA promotion request"] --> B{"Explicit DAM publication receipts?"}
    B -- "missing" --> E["Blocked: data acquisition needed"]
    B -- "ready" --> C{"20 safe-switch examples per tenant/source?"}
    C -- "missing" --> E
    C -- "ready" --> D["Future promotion review"]
    E --> F["dt_lava_ready=false"]
    F --> G["market_execution_enabled=false"]
```

Caption:

> V13 is an acquisition/source-readiness gate. It does not become a modeling result until receipt and safe-switch evidence is ready.

## 3. How to Explain Experiments

```mermaid
flowchart LR
    A["Strict LP/oracle"] --> B["V2"]
    B --> C["V2+ headline"]
    C --> D["TFT: negative/complementary"]
    C --> E["Poland: positive signal, not promoted"]
    C --> F["DT shadow: research only"]
    C --> G["LAVA smoke: contract only"]
```

Caption:

> V2+ is the current defensible result. The other branches are useful because they show disciplined research, not because they replace the headline.
