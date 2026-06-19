# Final Demo Assets

Date: 2026-06-16

This folder stores small, tracked screenshots and a short intro video used by
the root README and final defense runbook. These assets are visual
documentation only; they do not change the evidence status, model promotion
status, or market-execution boundary.

README-facing assets:

| File | Use |
| --- | --- |
| `operator-preview-desktop.png` | Root README hero and `/operator` proof surface |
| `project-intro-poster.png` | Root README thumbnail for the English intro video |
| `project-intro.mp4` | 42-second English concept-to-product intro with local voiceover |

The root README links the poster to the GitHub Pages player at
`docs/project-intro/index.html`; the MP4 remains a small tracked visual
documentation artifact, not execution evidence.
| `defense-dashboard-desktop.png` | Root README evidence-surface screenshot for `/defense` |
| `operator-preview-mobile.png` | Mobile layout check and optional reviewer context |

The intro video source lives in `project-intro-hyperframes/`. The voiceover was
generated locally with HyperFrames/Kokoro because no `OPENAI_API_KEY` was
available in the local shell; the narration is English and stays within the
same no-execution claim boundary as the README.

Additional README visuals are reused from tracked thesis assets under
`docs/thesis/chapters/assets/`; do not duplicate them here unless a future
GitHub rendering issue requires a curated copy.

Regenerate these only after the local stack is running and the dashboard renders
without console errors. Keep screenshots small enough for GitHub review.

## Capture Checklist

1. Start:

   Windows:

   ```powershell
   .\scripts\start-local-project.ps1 -ApiPort 8000 -DashboardPort 64163
   ```

   macOS/Linux:

   ```bash
   bash ./scripts/start-local-project.sh --api-port 8000 --dashboard-port 64163
   ```

2. Open `http://127.0.0.1:64163/operator`.
3. Confirm the first viewport shows `Operator Preview` and the boundary strip.
4. Capture `operator-preview-desktop.png`.
5. Open `http://127.0.0.1:64163/defense`.
6. Capture `defense-dashboard-desktop.png`.

Keep all captions and surrounding copy aligned with the defended boundary:
operator preview, read-model evidence, `market_execution_enabled=false`, no
`ProposedBid`, and no market order payload.

