import React from "react";
import {
  AbsoluteFill,
  Easing,
  Img,
  interpolate,
  spring,
  staticFile,
  useCurrentFrame,
  useVideoConfig,
} from "remotion";
import { HEIGHT, WIDTH } from "./root";
import { metrics, SceneConfig, scenes } from "./scenes";

const colors = {
  bg: "#061521",
  panel: "rgba(8, 35, 57, 0.86)",
  panelSoft: "rgba(18, 72, 103, 0.58)",
  cyan: "#7dd3fc",
  lime: "#a3ff12",
  amber: "#fbbf24",
  pink: "#fb7185",
  text: "#f4fbff",
  muted: "#aac4d7",
  line: "rgba(125, 211, 252, 0.18)",
};

export const ProjectIntro: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps, durationInFrames } = useVideoConfig();
  const time = frame / fps;
  const currentScene = activeScene(time);
  const local = sceneProgress(currentScene, time);
  const fade = edgeFade(currentScene, time);

  return (
    <AbsoluteFill style={rootStyle}>
      <Background frame={frame} />
      <div style={chromeStyle}>
        <span>SMART ENERGY ARBITRAGE 2026</span>
        <span>operator preview / read model / no market execution</span>
      </div>
      <main
        style={{
          ...mainStyle,
          opacity: fade,
          transform: `translateY(${interpolate(local, [0, 1], [18, 0], {
            extrapolateRight: "clamp",
            extrapolateLeft: "clamp",
            easing: Easing.bezier(0.16, 1, 0.3, 1),
          })}px)`,
        }}
      >
        <SceneText scene={currentScene} progress={local} />
        <SceneVisual scene={currentScene} progress={local} frame={frame} fps={fps} />
      </main>
      <Footer frame={frame} duration={durationInFrames} />
    </AbsoluteFill>
  );
};

export const ProjectIntroPoster: React.FC = () => {
  return (
    <>
      <ProjectIntro />
      <PosterPlayOverlay />
    </>
  );
};

const PosterPlayOverlay: React.FC = () => {
  return (
    <AbsoluteFill style={posterOverlayStyle}>
      <div style={posterPlayButtonStyle}>
        <div style={posterPlayTriangleStyle} />
      </div>
    </AbsoluteFill>
  );
};

const activeScene = (time: number): SceneConfig => {
  return scenes.find((scene) => time >= scene.start && time < scene.end) ?? scenes[scenes.length - 1];
};

const sceneProgress = (scene: SceneConfig, time: number): number => {
  return clamp((time - scene.start) / (scene.end - scene.start));
};

const edgeFade = (scene: SceneConfig, time: number): number => {
  const inFade = clamp((time - scene.start) / 0.7);
  const outFade = clamp((scene.end - time) / 0.7);
  return Math.min(1, inFade, outFade);
};

const Background: React.FC<{ frame: number }> = ({ frame }) => {
  const drift = frame * 0.22;
  return (
    <AbsoluteFill
      style={{
        background:
          "radial-gradient(circle at 12% 18%, rgba(34, 211, 238, 0.22), transparent 26%), radial-gradient(circle at 86% 78%, rgba(163, 255, 18, 0.16), transparent 24%), linear-gradient(135deg, #061521 0%, #0b2f4a 52%, #061521 100%)",
      }}
    >
      <div
        style={{
          position: "absolute",
          inset: 0,
          backgroundImage:
            "linear-gradient(rgba(125,211,252,0.10) 1px, transparent 1px), linear-gradient(90deg, rgba(125,211,252,0.10) 1px, transparent 1px)",
          backgroundSize: "72px 72px",
          backgroundPosition: `${-drift}px ${drift * 0.5}px`,
          opacity: 0.8,
        }}
      />
      <div style={largeGlow(-180, -120, 620, "rgba(14, 165, 233, 0.20)")} />
      <div style={largeGlow(1360, 720, 560, "rgba(132, 204, 22, 0.15)")} />
    </AbsoluteFill>
  );
};

const SceneText: React.FC<{ scene: SceneConfig; progress: number }> = ({ scene, progress }) => {
  const titleRise = interpolate(progress, [0, 0.22], [26, 0], {
    extrapolateRight: "clamp",
    extrapolateLeft: "clamp",
    easing: Easing.bezier(0.16, 1, 0.3, 1),
  });

  return (
    <section style={copyStyle}>
      <p style={eyebrowStyle}>{scene.eyebrow}</p>
      <h1 style={{ ...titleStyle, transform: `translateY(${titleRise}px)` }}>{scene.title}</h1>
      <p style={bodyStyle}>{scene.body}</p>
      <BoundaryStrip />
    </section>
  );
};

const SceneVisual: React.FC<{
  scene: SceneConfig;
  progress: number;
  frame: number;
  fps: number;
}> = ({ scene, progress, frame, fps }) => {
  switch (scene.kind) {
    case "intro":
      return <IntroVisual progress={progress} frame={frame} fps={fps} />;
    case "product":
      return <ProductVisual progress={progress} />;
    case "results":
      return <ResultsVisual progress={progress} frame={frame} fps={fps} />;
    case "business":
      return <BusinessVisual progress={progress} />;
    case "close":
      return <CloseVisual progress={progress} />;
  }
};

const IntroVisual: React.FC<{ progress: number; frame: number; fps: number }> = ({
  progress,
  frame,
  fps,
}) => {
  const pulse = spring({ frame, fps, config: { damping: 80, stiffness: 90 } });
  const chartProgress = interpolate(progress, [0.08, 0.56], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });
  const path = pricePath(chartProgress);

  return (
    <section style={visualStyle}>
      <div style={marketPanelStyle}>
        <div style={panelHeaderStyle}>
          <span>DAM/IDM price context</span>
          <span style={{ color: colors.lime }}>source-backed</span>
        </div>
        <svg width="620" height="320" viewBox="0 0 780 360" style={{ overflow: "visible" }}>
          <g opacity="0.32">
            {[0, 1, 2, 3, 4].map((item) => (
              <line
                key={item}
                x1="42"
                x2="740"
                y1={58 + item * 62}
                y2={58 + item * 62}
                stroke={colors.line}
                strokeWidth="2"
              />
            ))}
          </g>
          <path d={path} fill="none" stroke={colors.cyan} strokeLinecap="round" strokeWidth="8" />
          <circle cx={80 + chartProgress * 630} cy={180 - Math.sin(chartProgress * 8) * 94} r="10" fill={colors.lime} />
        </svg>
        <div style={decisionRowStyle}>
          <DecisionChip label="Charge" value="low-price hours" color={colors.amber} />
          <DecisionChip label="Hold" value="weak evidence" color={colors.cyan} />
          <DecisionChip label="Discharge" value="high-value hours" color={colors.lime} />
        </div>
      </div>
      <div style={batteryPanelStyle}>
        <p style={cardEyebrowStyle}>Battery envelope</p>
        <div style={batteryShellStyle}>
          <div style={{ ...batteryFillStyle, height: `${54 + pulse * 14}%` }} />
        </div>
        <p style={bigNumberStyle}>SOC 57%</p>
        <p style={smallTextStyle}>Recommendation preview only. The operator reviews before action.</p>
      </div>
    </section>
  );
};

const ProductVisual: React.FC<{ progress: number }> = ({ progress }) => {
  const scale = interpolate(progress, [0, 0.35], [0.95, 1], {
    extrapolateRight: "clamp",
    easing: Easing.bezier(0.16, 1, 0.3, 1),
  });

  return (
    <section style={visualStyle}>
      <div style={{ ...screenshotFrameStyle, transform: `scale(${scale})` }}>
        <Img src={staticFile("assets/operator-preview-desktop.png")} style={screenshotStyle} />
      </div>
      <div style={floatingCardsStyle}>
        <InfoCard title="Choose context" body="tenant, DAM/IDM venue, target date" />
        <InfoCard title="Inspect readiness" body="official rows first, forecast rows for unpublished horizons" />
        <InfoCard title="Review safely" body="HOLD and abstention are valid outcomes" />
      </div>
    </section>
  );
};

const ResultsVisual: React.FC<{ progress: number; frame: number; fps: number }> = ({
  progress,
  frame,
  fps,
}) => {
  const barSpring = spring({ frame, fps, config: { damping: 150, stiffness: 80 } });

  return (
    <section style={resultsStyle}>
      <div style={metricPanelStyle}>
        {metrics.map((metric, index) => {
          const numeric = Number.parseFloat(metric.value);
          const height = (numeric / 330) * 330 * clamp(barSpring - index * 0.035 + progress * 0.2);
          return (
            <div key={metric.label} style={metricItemStyle}>
              <div style={barTrackStyle}>
                <div
                  style={{
                    ...barStyle,
                    height,
                    background: `linear-gradient(180deg, ${metric.color}, rgba(125,211,252,0.45))`,
                  }}
                />
              </div>
              <p style={metricValueStyle}>{metric.value}</p>
              <p style={metricLabelStyle}>{metric.label}</p>
              <p style={metricNoteStyle}>{metric.note}</p>
            </div>
          );
        })}
      </div>
      <div style={evidenceImageGridStyle}>
        <Img src={staticFile("assets/regret-ladder.png")} style={evidenceImageStyle} />
        <Img src={staticFile("assets/hf-value-aligned-shadow-flow.png")} style={evidenceImageStyle} />
      </div>
    </section>
  );
};

const BusinessVisual: React.FC<{ progress: number }> = ({ progress }) => {
  const items = [
    ["BESS operators", "faster hourly review"],
    ["Pilot owners", "clear validation path"],
    ["Energy partners", "source traceability"],
    ["Technical reviewers", "reproducible evidence"],
  ];

  return (
    <section style={businessGridStyle}>
      <div style={businessLeftStyle}>
        <Img src={staticFile("assets/pipeline.png")} style={businessImageStyle} />
      </div>
      <div style={businessCardsStyle}>
        {items.map(([title, body], index) => (
          <div
            key={title}
            style={{
              ...businessCardStyle,
              transform: `translateY(${interpolate(progress, [0, 0.4], [40, 0], {
                extrapolateRight: "clamp",
              }) + index * 0}px)`,
            }}
          >
            <span style={businessIndexStyle}>{index + 1}</span>
            <p style={businessTitleStyle}>{title}</p>
            <p style={businessBodyStyle}>{body}</p>
          </div>
        ))}
      </div>
    </section>
  );
};

const CloseVisual: React.FC<{ progress: number }> = ({ progress }) => {
  const steps = ["README", "/operator", "/defense", "FastAPI /docs", "thesis paper"];
  return (
    <section style={closeStyle}>
      <div style={closeCardsStyle}>
        {steps.map((step, index) => (
          <div
            key={step}
            style={{
              ...closeCardStyle,
              opacity: interpolate(progress, [0.02 + index * 0.035, 0.12 + index * 0.035], [0, 1], {
                extrapolateLeft: "clamp",
                extrapolateRight: "clamp",
              }),
            }}
          >
            <span style={closeIndexStyle}>{index + 1}</span>
            <span>{step}</span>
          </div>
        ))}
      </div>
      <div style={finalBoundaryStyle}>
        source-backed operator preview / evidence first / human review / no market execution
      </div>
    </section>
  );
};

const BoundaryStrip: React.FC = () => {
  return (
    <div style={boundaryStripStyle}>
      <span style={boundaryChipStyle}>read-model evidence</span>
      <span style={boundaryChipStyle}>market_execution_enabled=false</span>
      <span style={boundaryChipStyle}>no ProposedBid</span>
    </div>
  );
};

const DecisionChip: React.FC<{ label: string; value: string; color: string }> = ({
  label,
  value,
  color,
}) => (
  <div style={decisionChipStyle}>
    <strong style={{ color }}>{label}</strong>
    <span>{value}</span>
  </div>
);

const InfoCard: React.FC<{ title: string; body: string }> = ({ title, body }) => (
  <div style={infoCardStyle}>
    <p style={infoTitleStyle}>{title}</p>
    <p style={infoBodyStyle}>{body}</p>
  </div>
);

const Footer: React.FC<{ frame: number; duration: number }> = ({ frame, duration }) => {
  const width = interpolate(frame, [0, duration], [0, 100], {
    extrapolateRight: "clamp",
  });

  return (
    <footer style={footerStyle}>
      <div style={progressTrackStyle}>
        <div style={{ ...progressFillStyle, width: `${width}%` }} />
      </div>
      <span>Source-backed DAM/IDM recommendation preview</span>
    </footer>
  );
};

const pricePath = (progress: number): string => {
  const points = [
    [60, 240],
    [150, 192],
    [255, 210],
    [340, 118],
    [430, 250],
    [520, 278],
    [610, 98],
    [705, 138],
  ];
  const count = Math.max(2, Math.ceil(points.length * progress));
  return points
    .slice(0, count)
    .map(([x, y], index) => `${index === 0 ? "M" : "L"} ${x} ${y}`)
    .join(" ");
};

const clamp = (value: number): number => Math.max(0, Math.min(1, value));

const rootStyle: React.CSSProperties = {
  background: colors.bg,
  color: colors.text,
  fontFamily: "Inter, Segoe UI, Arial, sans-serif",
  overflow: "hidden",
};

const posterOverlayStyle: React.CSSProperties = {
  alignItems: "center",
  display: "flex",
  justifyContent: "center",
  pointerEvents: "none",
  zIndex: 30,
};

const posterPlayButtonStyle: React.CSSProperties = {
  alignItems: "center",
  backdropFilter: "blur(10px)",
  background:
    "linear-gradient(135deg, rgba(163,255,18,0.92), rgba(125,211,252,0.92))",
  border: "5px solid rgba(244,251,255,0.88)",
  borderRadius: "50%",
  boxShadow: "0 28px 90px rgba(0,0,0,0.46), 0 0 0 20px rgba(244,251,255,0.08)",
  display: "flex",
  height: 210,
  justifyContent: "center",
  width: 210,
};

const posterPlayTriangleStyle: React.CSSProperties = {
  borderBottom: "48px solid transparent",
  borderLeft: "76px solid #061521",
  borderTop: "48px solid transparent",
  height: 0,
  marginLeft: 16,
  width: 0,
};

const chromeStyle: React.CSSProperties = {
  position: "absolute",
  top: 42,
  left: 64,
  right: 64,
  display: "flex",
  justifyContent: "space-between",
  color: colors.muted,
  fontSize: 22,
  fontWeight: 800,
  letterSpacing: 0,
  textTransform: "uppercase",
  zIndex: 10,
};

const mainStyle: React.CSSProperties = {
  position: "absolute",
  inset: "112px 72px 104px",
  display: "grid",
  gridTemplateColumns: "0.92fr 1.16fr",
  gap: 44,
  alignItems: "center",
  zIndex: 5,
};

const copyStyle: React.CSSProperties = {
  maxWidth: 730,
};

const eyebrowStyle: React.CSSProperties = {
  margin: "0 0 22px",
  color: colors.lime,
  fontFamily: "Cascadia Mono, Consolas, monospace",
  fontSize: 28,
  fontWeight: 900,
  letterSpacing: 0,
  textTransform: "uppercase",
};

const titleStyle: React.CSSProperties = {
  margin: 0,
  fontSize: 78,
  lineHeight: 0.98,
  letterSpacing: 0,
};

const bodyStyle: React.CSSProperties = {
  margin: "28px 0 0",
  color: "#cde6f2",
  fontSize: 31,
  lineHeight: 1.42,
};

const boundaryStripStyle: React.CSSProperties = {
  display: "flex",
  flexWrap: "wrap",
  gap: 12,
  marginTop: 34,
};

const boundaryChipStyle: React.CSSProperties = {
  padding: "12px 16px",
  border: "1px solid rgba(163,255,18,0.34)",
  borderRadius: 999,
  background: "rgba(163,255,18,0.10)",
  color: "#eaffd1",
  fontSize: 19,
  fontWeight: 900,
};

const visualStyle: React.CSSProperties = {
  position: "relative",
  minHeight: 720,
};

const marketPanelStyle: React.CSSProperties = {
  position: "absolute",
  top: 0,
  left: 0,
  width: 700,
  height: 518,
  padding: 30,
  border: "1px solid rgba(125,211,252,0.34)",
  borderRadius: 24,
  background: colors.panel,
  boxShadow: "0 26px 80px rgba(0,0,0,0.32)",
};

const panelHeaderStyle: React.CSSProperties = {
  display: "flex",
  justifyContent: "space-between",
  color: colors.text,
  fontSize: 24,
  fontWeight: 850,
  marginBottom: 20,
};

const decisionRowStyle: React.CSSProperties = {
  display: "grid",
  gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
  gap: 12,
};

const decisionChipStyle: React.CSSProperties = {
  display: "flex",
  flexDirection: "column",
  gap: 6,
  minWidth: 0,
  padding: "16px 14px",
  borderRadius: 18,
  background: "rgba(3, 15, 25, 0.72)",
  color: colors.muted,
  fontSize: 19,
  overflow: "hidden",
};

const batteryPanelStyle: React.CSSProperties = {
  position: "absolute",
  right: -20,
  bottom: 48,
  width: 286,
  height: 408,
  padding: 22,
  border: "1px solid rgba(163,255,18,0.38)",
  borderRadius: 28,
  background: "rgba(10, 45, 34, 0.76)",
};

const batteryShellStyle: React.CSSProperties = {
  position: "relative",
  width: 98,
  height: 190,
  margin: "20px auto",
  border: "8px solid rgba(232,248,255,0.72)",
  borderRadius: 22,
  overflow: "hidden",
};

const batteryFillStyle: React.CSSProperties = {
  position: "absolute",
  left: 0,
  right: 0,
  bottom: 0,
  background: `linear-gradient(180deg, ${colors.lime}, #0ea5e9)`,
};

const bigNumberStyle: React.CSSProperties = {
  margin: "0 0 8px",
  color: colors.lime,
  fontSize: 50,
  fontWeight: 900,
};

const smallTextStyle: React.CSSProperties = {
  margin: 0,
  color: colors.muted,
  fontSize: 20,
  lineHeight: 1.3,
};

const cardEyebrowStyle: React.CSSProperties = {
  margin: 0,
  color: colors.lime,
  fontSize: 22,
  fontWeight: 900,
  textTransform: "uppercase",
};

const screenshotFrameStyle: React.CSSProperties = {
  position: "absolute",
  top: 0,
  left: 0,
  width: 1010,
  height: 640,
  border: "1px solid rgba(125,211,252,0.38)",
  borderRadius: 26,
  overflow: "hidden",
  background: colors.panel,
  boxShadow: "0 30px 90px rgba(0,0,0,0.34)",
};

const screenshotStyle: React.CSSProperties = {
  width: "100%",
  height: "100%",
  objectFit: "cover",
  objectPosition: "left top",
};

const floatingCardsStyle: React.CSSProperties = {
  position: "absolute",
  right: 0,
  bottom: 0,
  width: 410,
  display: "grid",
  gap: 18,
};

const infoCardStyle: React.CSSProperties = {
  padding: 24,
  border: "1px solid rgba(163,255,18,0.22)",
  borderRadius: 20,
  background: "rgba(6, 25, 40, 0.9)",
};

const infoTitleStyle: React.CSSProperties = {
  margin: "0 0 8px",
  color: colors.lime,
  fontSize: 26,
  fontWeight: 900,
};

const infoBodyStyle: React.CSSProperties = {
  margin: 0,
  color: "#d9edf8",
  fontSize: 23,
  lineHeight: 1.28,
};

const resultsStyle: React.CSSProperties = {
  display: "grid",
  gap: 22,
};

const metricPanelStyle: React.CSSProperties = {
  height: 420,
  display: "grid",
  gridTemplateColumns: "repeat(5, 1fr)",
  gap: 16,
  padding: 24,
  border: "1px solid rgba(125,211,252,0.30)",
  borderRadius: 24,
  background: colors.panel,
};

const metricItemStyle: React.CSSProperties = {
  display: "flex",
  flexDirection: "column",
  justifyContent: "flex-end",
  minWidth: 0,
};

const barTrackStyle: React.CSSProperties = {
  height: 230,
  display: "flex",
  alignItems: "flex-end",
  borderRadius: 18,
  background: "rgba(2, 12, 21, 0.54)",
  overflow: "hidden",
};

const barStyle: React.CSSProperties = {
  width: "100%",
  minHeight: 12,
  borderTopLeftRadius: 18,
  borderTopRightRadius: 18,
};

const metricValueStyle: React.CSSProperties = {
  margin: "18px 0 4px",
  color: colors.text,
  fontSize: 28,
  fontWeight: 950,
};

const metricLabelStyle: React.CSSProperties = {
  margin: 0,
  color: colors.lime,
  fontSize: 18,
  fontWeight: 900,
};

const metricNoteStyle: React.CSSProperties = {
  margin: "4px 0 0",
  color: colors.muted,
  fontSize: 16,
};

const evidenceImageGridStyle: React.CSSProperties = {
  display: "grid",
  gridTemplateColumns: "1fr 1fr",
  gap: 22,
};

const evidenceImageStyle: React.CSSProperties = {
  width: "100%",
  height: 250,
  objectFit: "cover",
  border: "1px solid rgba(125,211,252,0.28)",
  borderRadius: 20,
};

const businessGridStyle: React.CSSProperties = {
  display: "grid",
  gridTemplateColumns: "1.1fr 0.9fr",
  gap: 26,
};

const businessLeftStyle: React.CSSProperties = {
  border: "1px solid rgba(125,211,252,0.32)",
  borderRadius: 24,
  overflow: "hidden",
  background: colors.panel,
  minHeight: 580,
};

const businessImageStyle: React.CSSProperties = {
  width: "100%",
  height: "100%",
  objectFit: "cover",
};

const businessCardsStyle: React.CSSProperties = {
  display: "grid",
  gap: 18,
};

const businessCardStyle: React.CSSProperties = {
  position: "relative",
  padding: "26px 26px 26px 82px",
  border: "1px solid rgba(163,255,18,0.26)",
  borderRadius: 20,
  background: "rgba(6, 25, 40, 0.88)",
};

const businessIndexStyle: React.CSSProperties = {
  position: "absolute",
  left: 24,
  top: 28,
  color: colors.lime,
  fontSize: 28,
  fontWeight: 950,
};

const businessTitleStyle: React.CSSProperties = {
  margin: "0 0 4px",
  color: colors.text,
  fontSize: 28,
  fontWeight: 900,
};

const businessBodyStyle: React.CSSProperties = {
  margin: 0,
  color: colors.muted,
  fontSize: 22,
};

const closeStyle: React.CSSProperties = {
  minHeight: 620,
  display: "flex",
  flexDirection: "column",
  justifyContent: "center",
  gap: 34,
};

const closeCardsStyle: React.CSSProperties = {
  display: "grid",
  gridTemplateColumns: "repeat(5, 1fr)",
  gap: 14,
};

const closeCardStyle: React.CSSProperties = {
  minHeight: 150,
  padding: 22,
  border: "1px solid rgba(125,211,252,0.28)",
  borderRadius: 20,
  background: colors.panel,
  color: colors.text,
  fontSize: 27,
  fontWeight: 900,
};

const closeIndexStyle: React.CSSProperties = {
  display: "block",
  marginBottom: 20,
  color: colors.lime,
  fontFamily: "Cascadia Mono, Consolas, monospace",
};

const finalBoundaryStyle: React.CSSProperties = {
  padding: 28,
  border: "1px solid rgba(163,255,18,0.42)",
  borderRadius: 22,
  background: "rgba(13, 66, 48, 0.82)",
  color: colors.text,
  fontSize: 31,
  fontWeight: 900,
  textAlign: "center",
};

const footerStyle: React.CSSProperties = {
  position: "absolute",
  left: 64,
  right: 64,
  bottom: 38,
  zIndex: 10,
  color: colors.muted,
  fontSize: 20,
  fontWeight: 800,
};

const progressTrackStyle: React.CSSProperties = {
  height: 7,
  marginBottom: 12,
  borderRadius: 99,
  background: "rgba(232,248,255,0.16)",
  overflow: "hidden",
};

const progressFillStyle: React.CSSProperties = {
  height: "100%",
  borderRadius: 99,
  background: `linear-gradient(90deg, ${colors.lime}, ${colors.cyan})`,
};

const largeGlow = (left: number, top: number, size: number, color: string): React.CSSProperties => ({
  position: "absolute",
  left,
  top,
  width: size,
  height: size,
  borderRadius: size,
  background: color,
  filter: "blur(12px)",
});
