const C = {
  bg: "#F7F4EC", paper: "#FFFFFF", ink: "#152238", muted: "#637083", faint: "#EEF1F0", line: "#D9DED8",
  dark: "#16202D", dark2: "#243247", cyan: "#0E7490", cyanSoft: "#DDF4F7", green: "#2F855A", greenSoft: "#DCFCE7",
  amber: "#D99100", amberSoft: "#FFF0C2", red: "#C2410C", redSoft: "#FFE4DA", violet: "#6D5BD0", violetSoft: "#E8E5FF",
  transparent: "#00000000",
};

function rect(slide, ctx, x, y, w, h, fill, opts = {}) {
  return ctx.addShape(slide, { left: x, top: y, width: w, height: h, geometry: opts.geometry || "rect", fill, line: opts.line || ctx.line(C.transparent, 0), name: opts.name });
}
function text(slide, ctx, value, x, y, w, h, opts = {}) {
  return ctx.addText(slide, {
    text: String(value || ""), left: x, top: y, width: w, height: h, fontSize: opts.size || 20, color: opts.color || C.ink,
    bold: Boolean(opts.bold), typeface: opts.face || (opts.mono ? ctx.fonts.mono : ctx.fonts.body), align: opts.align || "left", valign: opts.valign || "top",
    fill: opts.fill || C.transparent, line: opts.line || ctx.line(C.transparent, 0), insets: opts.insets || { left: 0, right: 0, top: 0, bottom: 0 }, name: opts.name,
  });
}
function rule(slide, ctx, x, y, w, color = C.line, weight = 1) { rect(slide, ctx, x, y, w, weight, color); }
function vRule(slide, ctx, x, y, h, color = C.line, weight = 1) { rect(slide, ctx, x, y, weight, h, color); }
function base(slide, ctx, page, kicker = "Newcomer overview") {
  rect(slide, ctx, 0, 0, ctx.W, ctx.H, C.bg); rect(slide, ctx, 0, 0, 18, ctx.H, C.dark); rect(slide, ctx, 18, 0, 8, ctx.H, C.cyan);
  rule(slide, ctx, 58, 675, 1088, C.line, 1); text(slide, ctx, kicker.toUpperCase(), 58, 688, 640, 16, { size: 8.5, color: C.muted, bold: true });
  text(slide, ctx, String(page).padStart(2, "0"), 1160, 682, 64, 24, { size: 13, color: C.muted, bold: true, align: "right" });
}
function kicker(slide, ctx, value, x = 58, y = 42) {
  rect(slide, ctx, x, y + 5, 10, 10, C.cyan); text(slide, ctx, value.toUpperCase().split("").join(" "), x + 22, y, 520, 22, { size: 9.5, color: C.muted, bold: true, name: `kicker-${value}` });
}
function title(slide, ctx, value, x = 58, y = 78, w = 900, h = 86, size = 34) { text(slide, ctx, value, x, y, w, h, { size, color: C.ink, bold: true, face: ctx.fonts.title }); }
function subtitle(slide, ctx, value, x = 58, y = 158, w = 860, h = 54) { text(slide, ctx, value, x, y, w, h, { size: 17, color: C.muted }); }
function tag(slide, ctx, value, x, y, w, fill = C.faint, color = C.ink) {
  rect(slide, ctx, x, y, w, 26, fill, { line: ctx.line(C.line, 1) }); text(slide, ctx, value, x + 10, y + 6, w - 20, 14, { size: 9.5, color, bold: true, align: "center" });
}
function softBox(slide, ctx, x, y, w, h, fill = C.paper, accent = C.cyan, opts = {}) {
  rect(slide, ctx, x, y, w, h, fill, { line: ctx.line(opts.line || C.line, opts.lineWidth || 1), name: opts.name }); if (opts.accent !== false) rect(slide, ctx, x, y, 6, h, accent);
}
function callout(slide, ctx, value, x, y, w, h, fill, accent, opts = {}) {
  softBox(slide, ctx, x, y, w, h, fill, accent, opts); text(slide, ctx, value, x + 20, y + 18, w - 40, h - 34, { size: opts.size || 17, color: opts.color || C.ink, bold: opts.bold, valign: "mid" });
}
function bullet(slide, ctx, value, x, y, w, color = C.ink, size = 15) { rect(slide, ctx, x, y + 7, 7, 7, C.cyan); text(slide, ctx, value, x + 18, y, w - 18, 34, { size, color }); }
function smallLabel(slide, ctx, value, x, y, w, color = C.muted) { text(slide, ctx, value, x, y, w, 18, { size: 9.5, color, bold: true }); }
function code(slide, ctx, value, x, y, w, h, opts = {}) { text(slide, ctx, value, x, y, w, h, { size: opts.size || 12.5, color: opts.color || C.ink, mono: true, fill: opts.fill || C.transparent, insets: opts.insets || { left: 8, right: 8, top: 5, bottom: 5 } }); }
function node(slide, ctx, label, note, x, y, w, h, color) {
  softBox(slide, ctx, x, y, w, h, C.paper, color, { accent: false }); rect(slide, ctx, x, y, w, 6, color);
  text(slide, ctx, label, x + 14, y + 16, w - 28, 22, { size: 13.5, color: C.ink, bold: true, align: "center" });
  text(slide, ctx, note, x + 14, y + 44, w - 28, h - 54, { size: 10.5, color: C.muted, align: "center" });
}
function row(slide, ctx, left, mid, right, x, y, w, color) {
  rect(slide, ctx, x, y, w, 44, C.paper, { line: ctx.line(C.line, 1) }); rect(slide, ctx, x, y, 5, 44, color);
  text(slide, ctx, left, x + 14, y + 11, 230, 20, { size: 11.5, color: C.ink, bold: true, mono: true }); text(slide, ctx, mid, x + 260, y + 11, 315, 20, { size: 12, color: C.ink }); text(slide, ctx, right, x + 600, y + 11, w - 615, 20, { size: 11.5, color: C.muted });
}

function drawSlide01(presentation, ctx) {
  const slide = presentation.slides.add(); base(slide, ctx, 1, "Newcomer overview / week2"); rect(slide, ctx, 838, 0, 442, 720, C.dark); rect(slide, ctx, 838, 0, 11, 720, C.amber);
  text(slide, ctx, "Newcomer\noverview", 58, 94, 550, 132, { size: 52, color: C.ink, bold: true, face: ctx.fonts.title });
  text(slide, ctx, "Smart Energy Arbitrage for BESS", 58, 247, 600, 30, { size: 22, color: C.cyan, bold: true });
  text(slide, ctx, "A practical map of what the repository does, where the important files live, and what a newcomer should learn next.", 58, 300, 620, 78, { size: 20, color: C.muted });
  callout(slide, ctx, "Current system: recommendation preview, not live market execution.", 58, 430, 610, 78, C.paper, C.red, { size: 21, bold: true });
  bullet(slide, ctx, "Ukraine-focused BESS / hourly DAM baseline MVP", 58, 548, 620, C.ink, 15); bullet(slide, ctx, "Backend, Dagster assets, FastAPI, Nuxt dashboard, thesis artifacts", 58, 586, 620, C.ink, 15);
  [["01", "Data", "tenant, weather, market, telemetry"], ["02", "Forecast", "strict similar-day plus research candidates"], ["03", "Optimize", "LP baseline with SOC and degradation"], ["04", "Preview", "API read models and operator dashboard"]].forEach((item, i) => {
    const y = 92 + i * 120; const color = i === 0 ? C.cyan : i === 1 ? C.green : i === 2 ? C.amber : C.violet; rect(slide, ctx, 908, y, 58, 58, color); text(slide, ctx, item[0], 908, y + 16, 58, 22, { size: 18, color: "#FFFFFF", bold: true, align: "center" });
    text(slide, ctx, item[1], 992, y + 4, 220, 28, { size: 23, color: "#FFFFFF", bold: true, face: ctx.fonts.title }); text(slide, ctx, item[2], 992, y + 40, 210, 40, { size: 13, color: "#C9D4E4" }); if (i < 3) vRule(slide, ctx, 936, y + 70, 48, "#6B778A", 2);
  });
  return slide;
}

function drawSlide02(presentation, ctx) {
  const slide = presentation.slides.add(); base(slide, ctx, 2); kicker(slide, ctx, "Mental model"); title(slide, ctx, "The repo is an operator-preview pipeline, not a live trading bot."); subtitle(slide, ctx, "A newcomer should first understand the path from tenant context to a safe operator-facing recommendation.");
  const xs = [62, 254, 446, 638, 830, 1022]; const colors = [C.cyan, C.cyan, C.green, C.amber, C.violet, C.red]; const labels = ["Resolve tenant", "Ingest data", "Build forecast", "Optimize plan", "Check battery", "Show preview"]; const notes = ["location, timezone, battery defaults", "market, weather, telemetry", "Level 1 strict similar-day", "deterministic LP baseline", "SOC, power, wear cost", "FastAPI + Nuxt read models"];
  xs.forEach((x, i) => { node(slide, ctx, labels[i], notes[i], x, 275, 156, 126, colors[i]); if (i < xs.length - 1) rule(slide, ctx, x + 156, 335, 36, C.line, 3); });
  callout(slide, ctx, "Keep three ideas separate: market intent, market clearing, and physical dispatch.", 130, 464, 1020, 78, C.paper, C.cyan, { size: 22, bold: true });
  tag(slide, ctx, "Implemented now: recommendation preview", 190, 580, 270, C.greenSoft, C.green); tag(slide, ctx, "Future: Proposed Bid / Cleared Trade / Dispatch", 490, 580, 380, C.violetSoft, C.violet); tag(slide, ctx, "Do not overclaim current scope", 900, 580, 250, C.redSoft, C.red);
  return slide;
}

function drawSlide03(presentation, ctx) {
  const slide = presentation.slides.add(); base(slide, ctx, 3); kicker(slide, ctx, "Architecture"); title(slide, ctx, "Dagster assets make the system readable as lineage, not scripts."); subtitle(slide, ctx, "The medallion layers show how raw signals become a baseline strategy and dashboard read models.");
  softBox(slide, ctx, 58, 252, 150, 216, C.paper, C.cyan); text(slide, ctx, "Tenant\nregistry", 84, 288, 92, 58, { size: 22, color: C.ink, bold: true, align: "center", face: ctx.fonts.title }); text(slide, ctx, "simulations/\ntenants.yml", 82, 372, 96, 42, { size: 12, color: C.muted, mono: true, align: "center" });
  [["Bronze", "weather / market\ningestion", "source metadata\nsynthetic fallback", C.cyan], ["Silver", "features and\nforecast candidates", "NBEATSx-style\nTFT-style", C.green], ["Gold", "baseline LP and\nsimulated trades", "regret-aware\nevaluation path", C.amber]].forEach((col, i) => {
    const x = 260 + i * 260; softBox(slide, ctx, x, 232, 210, 250, C.paper, col[3]); text(slide, ctx, col[0], x + 24, 260, 162, 34, { size: 27, color: col[3], bold: true, face: ctx.fonts.title, align: "center" });
    text(slide, ctx, col[1], x + 25, 324, 160, 52, { size: 17, color: C.ink, bold: true, align: "center" }); text(slide, ctx, col[2], x + 30, 394, 150, 56, { size: 13, color: C.muted, align: "center" }); if (i < 2) rule(slide, ctx, x + 210, 357, 50, C.line, 3);
  });
  rule(slide, ctx, 208, 357, 52, C.line, 3); softBox(slide, ctx, 1040, 232, 168, 250, C.dark, C.violet, { line: C.dark }); text(slide, ctx, "Resource\nstores", 1060, 260, 128, 52, { size: 24, color: "#FFFFFF", bold: true, align: "center", face: ctx.fonts.title }); text(slide, ctx, "Postgres / in-memory / null adapters", 1064, 338, 118, 58, { size: 13, color: "#CFD7E6", align: "center" }); text(slide, ctx, "FastAPI + Nuxt read models", 1064, 414, 118, 36, { size: 12.5, color: C.amberSoft, align: "center", bold: true });
  callout(slide, ctx, "Key engineering habit: change assets by contract, not by side effect.", 216, 540, 800, 62, C.paper, C.green, { size: 20, bold: true }); return slide;
}

function drawSlide04(presentation, ctx) {
  const slide = presentation.slides.add(); base(slide, ctx, 4); kicker(slide, ctx, "Repository map"); title(slide, ctx, "Read the repository by responsibility before reading it by filename."); subtitle(slide, ctx, "Most newcomer confusion disappears when each folder is tied to the role it plays in the MVP loop.");
  smallLabel(slide, ctx, "PATH", 82, 232, 200); smallLabel(slide, ctx, "RESPONSIBILITY", 342, 232, 260); smallLabel(slide, ctx, "WHY A NEWCOMER CARES", 686, 232, 320);
  [["src/smart_arbitrage/", "Python package", "assets, solver, schemas, stores, telemetry, training", C.cyan], ["api/main.py", "FastAPI control plane", "dashboard-owned contracts and demo endpoints", C.green], ["dashboard/", "Nuxt operator UI", "same-origin proxy and operator-facing surfaces", C.amber], ["simulations/tenants.yml", "Tenant registry", "canonical battery and location defaults", C.violet], ["docs/technical/", "Architecture docs", "API, PRD, issues, and demo readiness", C.red], ["docs/thesis/", "Academic trail", "chapters, sources, weekly reports, decks", C.cyan], ["docker-compose.yaml", "Local stack", "Postgres, MQTT, MLflow, API, Dagster", C.green]].forEach((item, i) => row(slide, ctx, item[0], item[1], item[2], 58, 256 + i * 51, 1124, item[3]));
  callout(slide, ctx, "First stop: CONTEXT.md. It defines the words the code is allowed to use.", 232, 626, 790, 46, C.dark, C.amber, { size: 17, color: "#FFFFFF", bold: true, line: C.dark }); return slide;
}

function drawSlide05(presentation, ctx) {
  const slide = presentation.slides.add(); base(slide, ctx, 5); kicker(slide, ctx, "Domain boundary"); title(slide, ctx, "The safest codebase changes respect the market-to-physics boundary."); subtitle(slide, ctx, "CONTEXT.md separates intent, clearing, and dispatch so the MVP does not accidentally claim execution semantics.");
  [["Market intent", "Proposed Bid", "Bid Curve", "Bid Gatekeeper", C.violet], ["Market result", "Cleared Trade", "Uniform Settlement", "Allocation", C.amber], ["Physical execution", "Dispatch Command", "Battery Telemetry", "HOLD", C.red]].forEach((lane, i) => {
    const y = 250 + i * 100; softBox(slide, ctx, 80, y, 1070, 70, i === 0 ? C.violetSoft : i === 1 ? C.amberSoft : C.redSoft, lane[4], { accent: false, line: C.line });
    text(slide, ctx, lane[0], 104, y + 18, 180, 28, { size: 19, color: lane[4], bold: true, face: ctx.fonts.title }); text(slide, ctx, lane[1], 342, y + 17, 210, 26, { size: 17, color: C.ink, bold: true }); text(slide, ctx, lane[2], 610, y + 17, 230, 26, { size: 17, color: C.ink, bold: true }); text(slide, ctx, lane[3], 900, y + 17, 210, 26, { size: 17, color: C.ink, bold: true });
  });
  softBox(slide, ctx, 236, 555, 810, 66, C.paper, C.green, { line: C.green }); text(slide, ctx, "Current MVP lives before those execution claims: baseline recommendation preview + projected battery state preview.", 268, 575, 746, 28, { size: 18, color: C.ink, bold: true, align: "center" }); return slide;
}

function drawSlide06(presentation, ctx) {
  const slide = presentation.slides.add(); base(slide, ctx, 6); kicker(slide, ctx, "MVP data flow"); title(slide, ctx, "The current demo path is narrow enough to test and explain end to end."); subtitle(slide, ctx, "Level 1 scope is hourly DAM baseline, not full multi-market bidding.");
  const steps = [["Tenant registry", "location, timezone, battery defaults", "simulations/tenants.yml", C.cyan], ["Bronze inputs", "weather, market, telemetry with provenance", "assets/bronze + telemetry", C.cyan], ["Forecast input", "strict similar-day Level 1 baseline", "forecasting / silver assets", C.green], ["LP baseline", "charge, discharge, SOC variables", "assets/gold/baseline_solver.py", C.amber], ["Battery preview", "SOC, limits, throughput, degradation", "optimization/projected_battery_state.py", C.violet], ["Read models", "persisted or in-memory stores", "resources/*_store.py", C.green], ["Operator surface", "FastAPI contracts and Nuxt dashboard", "api/main.py + dashboard/", C.red]];
  vRule(slide, ctx, 204, 236, 370, C.line, 3); steps.forEach((s, i) => { const y = 216 + i * 55; rect(slide, ctx, 178, y + 8, 52, 36, s[3]); text(slide, ctx, String(i + 1), 178, y + 17, 52, 16, { size: 14, color: "#FFFFFF", bold: true, align: "center" }); text(slide, ctx, s[0], 265, y + 2, 255, 24, { size: 16.5, color: C.ink, bold: true }); text(slide, ctx, s[1], 530, y + 4, 350, 22, { size: 13.5, color: C.muted }); code(slide, ctx, s[2], 895, y, 280, 30, { size: 11.2, fill: "#FFFFFFB8" }); }); return slide;
}

function drawSlide07(presentation, ctx) {
  const slide = presentation.slides.add(); base(slide, ctx, 7); kicker(slide, ctx, "Baseline solver"); title(slide, ctx, "The LP baseline is the reliable anchor future DFL must beat."); subtitle(slide, ctx, "It is deliberately deterministic: easy to inspect, test, and compare against learned strategies later.");
  softBox(slide, ctx, 72, 242, 520, 292, C.dark, C.cyan, { line: C.dark }); text(slide, ctx, "Objective", 104, 276, 220, 30, { size: 26, color: "#FFFFFF", bold: true, face: ctx.fonts.title }); text(slide, ctx, "maximize", 108, 338, 128, 24, { size: 18, color: C.amberSoft, bold: true, mono: true }); text(slide, ctx, "market value - degradation penalty", 108, 384, 396, 32, { size: 25, color: "#FFFFFF", bold: true, face: ctx.fonts.title }); text(slide, ctx, "rolling horizon commits only the first step", 108, 454, 360, 26, { size: 15.5, color: "#C9D4E4" });
  [["Inputs", "hourly DAM prices\nLevel 1 forecast\nbattery defaults", C.cyan], ["Constraints", "SOC min/max\nmax power\ncharge/discharge balance", C.red], ["Output", "signed MW preview\nprojected SOC\nUAH economics", C.green]].forEach((b, i) => { const x = 650 + i * 180; softBox(slide, ctx, x, 260, 150, 220, C.paper, b[2]); text(slide, ctx, b[0], x + 16, 284, 118, 26, { size: 20, color: b[2], bold: true, align: "center", face: ctx.fonts.title }); text(slide, ctx, b[1], x + 18, 338, 114, 92, { size: 13, color: C.ink, align: "center" }); });
  callout(slide, ctx, "Why it matters: it is the control group for regret-aware research, not a throwaway fallback.", 218, 580, 844, 54, C.paper, C.amber, { size: 19, bold: true }); return slide;
}

function drawSlide08(presentation, ctx) {
  const slide = presentation.slides.add(); base(slide, ctx, 8); kicker(slide, ctx, "Battery economics"); title(slide, ctx, "Battery wear is priced into the preview instead of hidden after the fact."); subtitle(slide, ctx, "The current battery layer is a feasibility-and-economics preview model, not a full electrochemical digital twin.");
  softBox(slide, ctx, 78, 255, 330, 270, C.paper, C.green); text(slide, ctx, "SOC envelope", 110, 284, 250, 28, { size: 25, color: C.green, bold: true, face: ctx.fonts.title }); rect(slide, ctx, 132, 360, 220, 34, C.faint, { line: ctx.line(C.line, 1) }); rect(slide, ctx, 143, 360, 198, 34, C.green); text(slide, ctx, "5%", 102, 366, 44, 18, { size: 12, color: C.muted, bold: true, align: "right" }); text(slide, ctx, "95%", 342, 366, 44, 18, { size: 12, color: C.muted, bold: true }); text(slide, ctx, "floor and ceiling enforced before a recommendation is trusted", 110, 438, 240, 42, { size: 14, color: C.muted, align: "center" });
  [["842.2", "UAH/MWh throughput", C.amber], ["16,843", "UAH per full cycle proxy", C.red], ["10 MWh", "demo battery capacity", C.cyan]].forEach((m, i) => { const x = 478 + i * 224; softBox(slide, ctx, x, 276, 184, 180, C.paper, m[2], { accent: false }); text(slide, ctx, m[0], x + 18, 314, 148, 44, { size: 33, color: m[2], bold: true, face: ctx.fonts.title, align: "center" }); text(slide, ctx, m[1], x + 22, 374, 140, 44, { size: 13, color: C.ink, align: "center", bold: true }); });
  callout(slide, ctx, "This is enough for baseline optimization and operator demo claims, but not enough for full digital-twin claims.", 468, 514, 678, 72, C.dark, C.red, { size: 18, color: "#FFFFFF", bold: true, line: C.dark }); return slide;
}
