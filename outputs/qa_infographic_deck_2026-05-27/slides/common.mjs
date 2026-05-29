import path from "node:path";

const C = {
  ink: "#14213D",
  muted: "#40566B",
  teal: "#087E79",
  amber: "#D99000",
  red: "#C7372F",
  green: "#16845B",
  panel: "#FFFFFFEA",
  panelSoft: "#F7FAFBEF",
  line: "#D6E0E7",
};

export const SLIDES = [
  {
    image: "q01.png",
    number: "01",
    title: "Как работает LP",
    question: "Механизм покупки, продажи и физических ограничений",
    answer:
      "LP смотрит на 24 часа цен. Если цена низкая - выгодно зарядить батарею. Если цена высокая - выгодно разрядить и продать энергию. Он выбирает не одно действие, а весь безопасный профиль charge/discharge/SOC.",
    formula:
      "max sum_t [ price_t * (discharge_t - charge_t) * dt - k_deg * (charge_t + discharge_t) * dt ]",
    details: [
      "SOC: 5-95% емкости",
      "charge/discharge <= max power",
      "SOC_t+1 = SOC_t + charge*sqrt(RTE)*dt - discharge/sqrt(RTE)*dt",
      "Коммитится только первый час rolling horizon",
    ],
    footer: "Результат: BUY если net_power < 0, SELL если > 0, HOLD если около 0.",
  },
  {
    image: "q02.png",
    number: "02",
    title: "Strict Similar Benchmark",
    question: "Как считается regret и связь с oracle",
    answer:
      "Strict benchmark не обучается. Он берет похожий прошлый день как прогноз: для Tue-Fri чаще yesterday, иначе same weekday last week. Потом LP строит schedule по этому прогнозу.",
    formula: "regret = oracle_value - achieved_value",
    details: [
      "achieved_value: schedule из strict forecast, оцененный на actual prices",
      "oracle_value: тот же LP, но с actual future prices",
      "oracle - это верхняя планка, не реальный доступный режим",
      "Меньше regret = лучше decision quality",
    ],
    footer: "Benchmark нужен как честный frozen comparator для V2+, DT и forecast models.",
  },
  {
    image: "q03.png",
    number: "03",
    title: "Raw Data",
    question: "Какие данные входят сырыми и какой формы",
    answer:
      "Основной raw input - hourly rows: timestamp + price/weather/source metadata. Украинская цена DAM одна для market zone IPS; tenants отличаются погодой, координатами, BESS параметрами, SOC и economics.",
    formula: "hourly row = timestamp + tenant_id/source + value columns + provenance",
    details: [
      "OREE DAM: hourly price, UAH/MWh, market zone IPS",
      "Open-Meteo: hourly weather by tenant coordinates/timezone",
      "Tenant config: capacity, power, SOC limits, degradation cost",
      "Synthetic fallback rows are demo stability only, not measured-performance claims",
    ],
    footer: "Pipeline shape: Bronze observed rows -> Silver features -> Gold schedule/evidence rows.",
  },
  {
    image: "q04.png",
    number: "04",
    title: "Features",
    question: "Что создается, как выбирается и где используется",
    answer:
      "Features создаются после source/governance checks. Они описывают время, историю цен, погоду, батарею и candidate schedule. Важное правило: нельзя использовать future actual/oracle/regret как input.",
    formula: "features_t = calendar + lags + rolling stats + weather + BESS + candidate context",
    details: [
      "Forecast stage: calendar, lags, rolling price stats, forecast-available weather",
      "LP stage: forecast vector + BESS limits + degradation cost",
      "V2+/DT stage: prior context, candidate family, dispatch/SOC/throughput summaries",
      "Leakage guard blocks actual/oracle/regret/labels from selector inputs",
    ],
    footer: "Features are chosen by availability at decision time, not by after-the-fact usefulness.",
  },
  {
    image: "q05.png",
    number: "05",
    title: "TFT и NBEATSx",
    question: "Их роль в V2+ и DT strategy",
    answer:
      "TFT/NBEATSx - это forecast adapters. Они дают hourly price forecast and explanatory signals. Дальше LP/candidate scorer превращает прогноз в feasible schedules; V2+/DT выбирают между candidate schedules.",
    formula: "history + exogenous features -> forecast vector -> LP/candidates -> evidence",
    details: [
      "Они не являются controller и не отправляют market bids",
      "TFT полезен еще и VSN/explainability evidence",
      "NBEATSx/TFT сравниваются через same frozen LP/oracle scorer",
      "Текущие claims: adapter readiness/research evidence, not SOTA execution",
    ],
    footer: "Short version: forecast layer, not execution layer.",
  },
  {
    image: "q06.png",
    number: "06",
    title: "Exogenous Features",
    question: "Что это, какие используются, что с Poland",
    answer:
      "Exogenous features - внешние сигналы, которые известны до решения и помогают объяснить price regime: weather, calendar, neighboring-market context, tenant context.",
    formula: "target = UA DAM; exogenous = context, not replacement target",
    details: [
      "Used/available: weather, calendar, tenant/BESS context, lagged market features",
      "Poland lane: ENTSO-E PL day-ahead price aligned as lag-24 context",
      "Poland features: lag-24 level, deltas, daily spread/rank, peak/trough hour",
      "Governance blocks EU/PL rows from becoming Ukrainian training targets",
    ],
    footer: "Current Poland result is not promoted: mechanically usable, but weaker than frozen V2+.",
  },
  {
    image: "q07.png",
    number: "07",
    title: "Schedule Selection",
    question: "Как V2+ создает много schedules и как DT выбирает safe schedules",
    answer:
      "V2+ builds a candidate library: several schedule families perturb or guard the baseline schedules, then each candidate is LP/oracle-scored offline. The selector chooses the lowest prior-regret/value candidate, with fallback to frozen V2.",
    formula: "candidate library -> safety/feasibility -> prior-only selector -> selected family",
    details: [
      "V2+ families include extrema perturbation, spread penalty, neighborhood shift, block reconciliation, terminal SOC target",
      "DT-shadow does not output raw hourly actions; it predicts when switching away from V2+ is safe",
      "It switches only if predicted improvement clears threshold and tail-risk guard passes",
      "Otherwise it abstains and keeps V2+",
    ],
    footer: "All of this is offline recommendation evidence, not market execution.",
  },
  {
    image: "q08.png",
    number: "08",
    title: "From Prediction to Recommendation",
    question: "How forecast/candidate becomes feasible LP schedule",
    answer:
      "If the selected input is a forecast, LP solves charge/discharge/SOC under battery limits and creates a feasible 24h schedule. If the selected input is an existing candidate, the system reuses its stored dispatch vector.",
    formula: "stored dispatch vector = [dispatch_mw at hour 1...24] plus SOC/action metadata",
    details: [
      "forecast vector -> LP -> dispatch_mw_vector + soc_fraction_vector",
      "candidate row already stores dispatch_mw_vector from earlier LP/candidate scoring",
      "operator preview reads committed_action and committed_power_mw",
      "Gatekeeper blocks candidates outside physical/market envelope",
    ],
    footer: "The output is BUY/SELL/HOLD recommendation schedule, not ProposedBid.",
  },
  {
    image: "q09.png",
    number: "09",
    title: "Current DT Shadow Candidate",
    question: "Why 3 wins and 1 tie; how it got about 3.8% better than V2+",
    answer:
      "The selector was conservative. Out of 90 final anchors, it kept V2+ for 86 and switched only 4 times to strict_reference. Those 4 switches produced 3 lower-regret wins and 1 equal-regret tie, with 0 losses.",
    formula: "mean regret: V2+ 174.77 UAH -> selected 168.16 UAH = 3.78% lower",
    details: [
      "Wins: Kyiv mall, Lviv office, Odesa hotel at 2026-04-15T23:00",
      "Tie: Kharkiv hospital at the same anchor",
      "It noticed a prior-context/candidate-family pattern where strict_reference looked safer than V2+",
      "It did not see future oracle; oracle/regret scored the choice afterward",
    ],
    footer: "Boundary: V2+ remains default; DT/LAVA not promoted; market_execution_enabled=false.",
  },
];

function bulletText(items) {
  return items.map((item) => `- ${item}`).join("\n");
}

export async function renderQaSlide(presentation, ctx, data) {
  const slide = presentation.slides.add();
  const assetPath = path.join(ctx.workspaceDir, "assets_v2", data.image);

  await ctx.addImage(slide, {
    path: assetPath,
    x: 0,
    y: 0,
    width: ctx.W,
    height: ctx.H,
    fit: "cover",
    alt: data.title,
  });

  ctx.addShape(slide, {
    x: 492,
    y: 38,
    width: 744,
    height: 644,
    fill: "#FFFFFFF8",
    line: { fill: "#8ABCB8", width: 1.2, style: "solid" },
  });

  ctx.addText(slide, {
    text: data.number,
    x: 534,
    y: 64,
    width: 58,
    height: 34,
    fontSize: 18,
    bold: true,
    color: C.teal,
    align: "center",
    valign: "middle",
    fill: "#E7F6F4",
    line: ctx.line("#9ED3CD", 1),
    insets: { left: 0, right: 0, top: 0, bottom: 0 },
  });

  ctx.addText(slide, {
    text: data.title,
    x: 610,
    y: 60,
    width: 560,
    height: 48,
    fontSize: 30,
    bold: true,
    color: C.ink,
    typeface: ctx.fonts.title,
  });

  ctx.addText(slide, {
    text: data.question,
    x: 536,
    y: 120,
    width: 646,
    height: 46,
    fontSize: 17,
    color: C.muted,
  });

  ctx.addText(slide, {
    text: data.formula,
    x: 536,
    y: 184,
    width: 646,
    height: 78,
    fontSize: 16,
    bold: true,
    color: C.ink,
    typeface: ctx.fonts.mono,
    fill: "#F4F9FA",
    line: ctx.line(C.line, 1),
    insets: { left: 18, right: 18, top: 12, bottom: 10 },
  });

  ctx.addText(slide, {
    text: data.answer,
    x: 536,
    y: 286,
    width: 646,
    height: 142,
    fontSize: 23,
    bold: true,
    color: C.ink,
    typeface: ctx.fonts.title,
  });

  ctx.addText(slide, {
    text: bulletText(data.details),
    x: 550,
    y: 452,
    width: 620,
    height: 142,
    fontSize: 17,
    color: C.ink,
    insets: { left: 0, right: 0, top: 0, bottom: 0 },
  });

  ctx.addText(slide, {
    text: data.footer,
    x: 536,
    y: 608,
    width: 454,
    height: 58,
    fontSize: 15,
    color: C.teal,
    bold: true,
  });

  ctx.addText(slide, {
    text: "market_execution_enabled=false",
    x: 972,
    y: 616,
    width: 210,
    height: 36,
    fontSize: 10,
    bold: true,
    color: C.red,
    align: "center",
    valign: "middle",
    fill: "#FFF1F0E8",
    line: ctx.line("#F0B3AD", 1),
    insets: { left: 8, right: 8, top: 4, bottom: 4 },
  });

  return slide;
}
