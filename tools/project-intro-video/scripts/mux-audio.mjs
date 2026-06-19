import { existsSync, renameSync, rmSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync as run } from "node:child_process";

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, "../../..");
const silentVideo = resolve(
  repoRoot,
  "docs/technical/final-demo-assets/project-intro.silent.mp4",
);
const narration = resolve(
  repoRoot,
  "docs/technical/final-demo-assets/project-intro-remotion/narration.wav",
);
const output = resolve(
  repoRoot,
  "docs/technical/final-demo-assets/project-intro.mp4",
);
const tmpOutput = resolve(
  repoRoot,
  "docs/technical/final-demo-assets/project-intro.muxing.mp4",
);

if (!existsSync(silentVideo)) {
  throw new Error(`Missing silent video: ${silentVideo}`);
}

if (!existsSync(narration)) {
  throw new Error(`Missing narration: ${narration}`);
}

function resolveFfmpeg() {
  const fromUv = run(
    "uv",
    [
      "run",
      "python",
      "-c",
      "import imageio_ffmpeg; print(imageio_ffmpeg.get_ffmpeg_exe())",
    ],
    { cwd: repoRoot, encoding: "utf8" },
  );

  if (fromUv.status === 0) {
    return fromUv.stdout.trim();
  }

  return "ffmpeg";
}

const ffmpeg = resolveFfmpeg();

if (existsSync(tmpOutput)) {
  rmSync(tmpOutput, { force: true });
}

const result = run(ffmpeg, [
  "-y",
  "-i",
  silentVideo,
  "-i",
  narration,
  "-map",
  "0:v:0",
  "-map",
  "1:a:0",
  "-c:v",
  "libx264",
  "-b:v",
  "2500k",
  "-maxrate",
  "3000k",
  "-bufsize",
  "6000k",
  "-pix_fmt",
  "yuv420p",
  "-preset",
  "veryfast",
  "-c:a",
  "aac",
  "-movflags",
  "+faststart",
  tmpOutput,
], { stdio: "inherit" });

if (result.status !== 0) {
  throw new Error(`ffmpeg mux failed with exit code ${result.status}`);
}

rmSync(output, { force: true });
renameSync(tmpOutput, output);

try {
  rmSync(silentVideo, { force: true });
} catch (error) {
  console.warn(`Could not remove temporary silent video: ${error.message}`);
}

console.log(`Wrote ${output}`);
