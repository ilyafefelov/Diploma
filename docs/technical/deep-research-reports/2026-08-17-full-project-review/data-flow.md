# Data flow

```mermaid
flowchart LR
    O["Official OREE DAM rows"] --> B["Bronze source and receipt evidence"]
    W["Weather / tenant / optional telemetry"] --> B
    B --> S["Silver normalized hourly frames"]
    S --> F["Forecast adapters: naive / NBEATSx / TFT"]
    S --> L["Strict LP and physical constraints"]
    F --> G["Gold recommendation and research evidence"]
    L --> G
    G --> K["Pydantic deterministic gatekeeper"]
    K --> P["Postgres / static JSON read models"]
    P --> A["FastAPI operator endpoints"]
    P --> D["Nuxt operator / defense / public routes"]
    K -. "blocked candidates" .-> V["validation_failures evidence"]
    D --> H["Human review only"]
```

The control boundary ends at human-readable recommendations. The diagram does
not contain a market submission or hardware dispatch edge because the current
system does not implement one.

## Lineage expectations

- Every observed price series identifies source, delivery date, and freshness.
- Forecasts identify model, cutoff, generation timestamp, and quality boundary.
- Comparator metrics use the same frozen LP/oracle contour.
- Research artifacts identify configuration, input window, and promotion gate.
- Missing evidence stays visible as a blocker rather than falling back to a
  stronger claim.
