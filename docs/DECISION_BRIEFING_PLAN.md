# Decision Briefing Tab Plan

## Goal
Turn narrative clusters into actionable guidance with clear priority, rationale, and next steps for a user role.

## User Outcome
- Know what changed since the last run.
- Know which narratives require action now vs monitoring.
- Export/share a concise briefing.

## Tab Name
`Decision Briefing`

## Scope (v1)
1. Change Summary
- New narratives: topics absent in previous snapshot.
- Accelerating narratives: momentum up and volume up.
- Fading narratives: momentum negative with declining weighted volume.

2. Priority Queue
- Rank by `decision_score`.
- Display: label, momentum, novelty, weighted volume delta, source diversity, confidence.
- Action bucket:
  - `Act now`
  - `Monitor`
  - `Ignore`

3. Role Presets
- Presets alter weighting:
  - `Investor`
  - `Research`
  - `Operations`
  - `Security`

4. Decision Cards
- For top N topics show:
  - Why this matters (deterministic template)
  - What to do next (deterministic template)
  - Supporting links (top representative items)

5. Export
- Copy markdown briefing.
- Download CSV for top decisions.

## Data/Backend Changes
1. Persist brief snapshots
- New table: `decision_snapshots`
  - `window`, `generated_at`, `topics_json`

2. New endpoint
- `GET /api/briefing?window=24h&profile=investor&top_n=15`
- Response:
  - `summary`: high-level stats
  - `changes`: new/accelerating/fading arrays
  - `decisions`: prioritized decision cards
  - `generated_at`

3. Scoring
- `decision_score = w1*momentum + w2*novelty + w3*log(weighted_volume_now+1) + w4*diversity + w5*label_confidence`
- Profile presets provide different `(w1..w5)`.

## Frontend Changes
1. New tab entry in tab switcher:
- `Treemap`, `Action Center`, `Decision Briefing`

2. New component
- `frontend/components/DecisionBriefing.tsx`
- Sections:
  - header + profile selector
  - change summary chips
  - prioritized decision table/cards
  - export actions

3. UX requirements
- Skeleton loading state.
- Empty state when no decisions.
- “Open in Detail Drawer” action from each decision card.

## Acceptance Criteria
- A user can select a profile and instantly see top actionable narratives.
- Every decision has explicit rationale and suggested next action.
- Exported markdown is ready to paste into notes/slack/email.
- No API keys, works with local cached data.

## Implementation Order
1. Backend snapshot + `/api/briefing`.
2. Frontend `DecisionBriefing` tab scaffold.
3. Decision scoring + profile presets.
4. Rationale/action templates.
5. Export + polish + tests.

## Tests
- Backend:
  - score ordering deterministic
  - new/accelerating/fading classification correctness
  - endpoint response shape
- Frontend:
  - tab renders with sample payload
  - profile changes reorder decisions
  - export action outputs non-empty markdown
