import { copyFileSync, mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, "../../..");
const outDir = resolve(here, "../public/assets");

const assets = [
  [
    "docs/technical/final-demo-assets/operator-preview-desktop.png",
    "operator-preview-desktop.png",
  ],
  [
    "docs/technical/final-demo-assets/defense-dashboard-desktop.png",
    "defense-dashboard-desktop.png",
  ],
  [
    "docs/thesis/chapters/assets/compact-fig-4-1-regret-ladder.png",
    "regret-ladder.png",
  ],
  [
    "docs/thesis/chapters/assets/compact-fig-3-1-pipeline.png",
    "pipeline.png",
  ],
  [
    "docs/thesis/chapters/assets/hf-value-aligned-shadow-flow.png",
    "hf-value-aligned-shadow-flow.png",
  ],
];

mkdirSync(outDir, { recursive: true });

for (const [source, target] of assets) {
  copyFileSync(resolve(repoRoot, source), resolve(outDir, target));
}

console.log(`Prepared ${assets.length} Remotion visual assets.`);
