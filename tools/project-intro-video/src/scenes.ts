export type Metric = {
  label: string;
  value: string;
  note: string;
  color: string;
};

export type SceneKind = "intro" | "product" | "results" | "business" | "close";

export type SceneConfig = {
  start: number;
  end: number;
  eyebrow: string;
  title: string;
  body: string;
  kind: SceneKind;
};

export const metrics: Metric[] = [
  {
    label: "Strict LP/oracle",
    value: "310.58 UAH",
    note: "mean regret reference",
    color: "#f59e0b",
  },
  {
    label: "V2 selector",
    value: "206.37 UAH",
    note: "historical baseline",
    color: "#38bdf8",
  },
  {
    label: "V2+ learner",
    value: "174.77 UAH",
    note: "headline/default evidence",
    color: "#a3ff12",
  },
  {
    label: "DT/V2+ shadow",
    value: "168.16 UAH",
    note: "4 switches / 86 abstentions",
    color: "#c084fc",
  },
  {
    label: "HF value-aligned",
    value: "158.71 UAH",
    note: "manual frozen signal",
    color: "#22d3ee",
  },
];

export const scenes: SceneConfig[] = [
  {
    start: 0,
    end: 36,
    eyebrow: "Smart Energy Arbitrage 2026",
    title: "Battery decisions need evidence before action.",
    body:
      "Hourly DAM/IDM prices can create value, but a BESS operator needs source-backed context, physical limits, and safety gates before any decision is reviewed.",
    kind: "intro",
  },
  {
    start: 36,
    end: 66,
    eyebrow: "Operator product",
    title: "A reviewable DAM/IDM recommendation preview.",
    body:
      "Tenant, venue, delivery date, source readiness, candidate schedules, HOLD cases, and value evidence stay visible in one dashboard for human review.",
    kind: "product",
  },
  {
    start: 66,
    end: 92,
    eyebrow: "Algorithms and evidence",
    title: "LP reference, V2+ evidence, transformer shadows.",
    body:
      "LP gives the feasible reference. V2+ improves schedule/value evidence. Transformer-based shadows remain gated research signals, not production switches.",
    kind: "results",
  },
  {
    start: 92,
    end: 110,
    eyebrow: "Partner value",
    title: "A safer path toward controlled BESS pilots.",
    body:
      "The near-term value is faster review, clearer source traceability, safer abstention, reproducible evidence, and a pilot-ready validation path.",
    kind: "business",
  },
  {
    start: 110,
    end: 126,
    eyebrow: "Review path",
    title: "Open the product, then inspect the proof.",
    body:
      "README, /operator, /defense, FastAPI /docs, and the thesis paper show the same boundary: source-backed operator preview, evidence first, no market execution.",
    kind: "close",
  },
];
