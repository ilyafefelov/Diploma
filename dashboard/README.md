# Smart Arbitrage Dashboard

Tracked Nuxt dashboard for the diploma repo. This app is separate from the legacy dashboard runtime and starts from a clean root-level workspace in `./dashboard`.

## Current slice

- Root operator shell for the Level 1 DAM baseline
- Same-origin Nuxt server route for the FastAPI control plane
- Tenant registry selection and inspection rail
- ECharts-backed tenant location view

## Install

```bash
npm install
```

## Development

The dashboard expects the FastAPI control-plane API to be available. By default it looks for `http://127.0.0.1:8000`.

```bash
npm run dev
```

To point the dashboard at a different API base:

```bash
NUXT_API_BASE=http://127.0.0.1:8010 npm run dev
```

## Production build

```bash
npm run build
node .output/server/index.mjs
```

To override the upstream API when running the built server:

```bash
NUXT_API_BASE=http://127.0.0.1:8010 node .output/server/index.mjs
```
