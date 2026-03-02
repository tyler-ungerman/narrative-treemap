# Narrative Treemap
Local-first narrative intelligence dashboard that clusters public news into topics, scores momentum/novelty, and turns signals into actionable decision workflows.

## Why this is useful
Narrative Treemap helps you quickly see which stories are actually accelerating across many sources, instead of manually scanning dozens of feeds. It also enforces trust gates before surfacing actions, so you can focus on signals with clearer evidence and lifecycle tracking.

## What you get
- Treemap heatmap by volume + momentum
- Action Center, Decision Briefing, Backtest, Paper Portfolio, Alerts
- 100+ public no-key sources (global + local + domain-specific)
- Local-first stack: FastAPI + SQLite + Next.js

## Features array
```ts
const features = [
  "Treemap + 3D narrative landscape",
  "Action Center (trust-gated recommendations)",
  "Decision Briefing (explicit next actions)",
  "Backtest (7d/30d with warmup transparency)",
  "Paper Portfolio (open risk + realized/unrealized P/L)",
  "Alert Rules (threshold-based notifications)",
  "Narrative-to-asset evidence + connection reasoning"
];
```

## Quick start (easy)
### 1) Install deps
```bash
make backend-install
make frontend-install
```

### 2) Run both services
```bash
make dev
```

### 3) Open app
- Frontend: `http://localhost:3001`
- Backend API: `http://localhost:8001`

## Common commands
```bash
make backend-dev
make frontend-dev
```

## Project layout
- `backend/` FastAPI ingestion, clustering, scoring, APIs
- `frontend/` Next.js UI (treemap + decision surfaces)
- `docs/` launch notes/assets

## Notes
- No API keys required.
- Local data/cache lives in `backend/data/` and is git-ignored.
