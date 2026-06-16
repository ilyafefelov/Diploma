# Final Demo Assets

Date: 2026-06-16

This folder stores small, tracked screenshots used by the root README and final
defense runbook.

Expected assets:

| File | Use |
| --- | --- |
| `operator-preview-desktop.png` | Root README hero screenshot and GitHub first impression |
| `defense-dashboard-desktop.png` | Optional defense evidence screenshot |
| `operator-preview-mobile.png` | Optional mobile layout check |

Regenerate these only after the local stack is running and the dashboard renders
without console errors. Keep screenshots small enough for GitHub review.

## Capture Checklist

1. Start:

   ```powershell
   .\scripts\start-local-project.ps1 -ApiPort 8000 -DashboardPort 64163
   ```

2. Open `http://127.0.0.1:64163/operator`.
3. Confirm the first viewport shows `Operator Preview` and the boundary strip.
4. Capture `operator-preview-desktop.png`.
5. Open `http://127.0.0.1:64163/defense`.
6. Capture `defense-dashboard-desktop.png`.

These assets are visual documentation only. They do not change the evidence
status or market-execution boundary.

