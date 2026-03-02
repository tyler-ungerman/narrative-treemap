from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query, Request

from app.core.time_windows import supported_windows
from app.pipeline.service import NarrativeService
from app.schemas.api import (
    AlertEvaluateResponse,
    AlertRuleModel,
    AlertRuleUpsertRequest,
    AlertRulesResponse,
    BacktestResponse,
    DecisionBriefingResponse,
    PortfolioResponse,
    TopicsResponse,
)

router = APIRouter(prefix="/api")


def parse_csv_values(value: str | None) -> list[str] | None:
    if not value:
        return None
    values = [segment.strip() for segment in value.split(",") if segment.strip()]
    return values or None


def get_service(request: Request) -> NarrativeService:
    service = getattr(request.app.state, "narrative_service", None)
    if service is None:
        raise HTTPException(status_code=503, detail="Service not ready")
    return service


@router.get("/topics", response_model=TopicsResponse)
async def get_topics(
    request: Request,
    window: str = Query("24h"),
    verticals: str | None = Query(default=None),
    sources: str | None = Query(default=None),
    only_rising: bool = Query(default=False),
    search: str | None = Query(default=None),
) -> dict:
    if window not in supported_windows():
        raise HTTPException(status_code=400, detail=f"Unsupported window '{window}'")

    service = get_service(request)
    payload = service.get_topics(
        window=window,
        verticals=parse_csv_values(verticals),
        sources=parse_csv_values(sources),
        only_rising=only_rising,
        search=search,
    )
    return payload


@router.get("/briefing", response_model=DecisionBriefingResponse)
async def get_briefing(
    request: Request,
    window: str = Query("24h"),
    profile: str = Query("investor"),
    top_n: int = Query(default=15, ge=5, le=50),
    verticals: str | None = Query(default=None),
    sources: str | None = Query(default=None),
    only_rising: bool = Query(default=False),
    search: str | None = Query(default=None),
) -> dict:
    if window not in supported_windows():
        raise HTTPException(status_code=400, detail=f"Unsupported window '{window}'")

    service = get_service(request)
    valid_profiles = set(service.briefing_profiles())
    if profile.strip().lower() not in valid_profiles:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported briefing profile '{profile}'. Valid: {', '.join(sorted(valid_profiles))}",
        )

    payload = service.get_decision_briefing(
        window=window,
        profile=profile,
        top_n=top_n,
        verticals=parse_csv_values(verticals),
        sources=parse_csv_values(sources),
        only_rising=only_rising,
        search=search,
    )
    return payload


@router.get("/backtest", response_model=BacktestResponse)
async def get_backtest(
    request: Request,
    window: str = Query("24h"),
    profile: str = Query("investor"),
    verticals: str | None = Query(default=None),
    sources: str | None = Query(default=None),
    only_rising: bool = Query(default=False),
    search: str | None = Query(default=None),
) -> dict:
    if window not in supported_windows():
        raise HTTPException(status_code=400, detail=f"Unsupported window '{window}'")
    service = get_service(request)
    valid_profiles = set(service.briefing_profiles())
    if profile.strip().lower() not in valid_profiles:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported briefing profile '{profile}'. Valid: {', '.join(sorted(valid_profiles))}",
        )
    return service.get_backtest(
        window=window,
        profile=profile,
        verticals=parse_csv_values(verticals),
        sources=parse_csv_values(sources),
        only_rising=only_rising,
        search=search,
    )


@router.get("/portfolio", response_model=PortfolioResponse)
async def get_portfolio(
    request: Request,
    window: str = Query("24h"),
    profile: str = Query("investor"),
    verticals: str | None = Query(default=None),
    sources: str | None = Query(default=None),
    only_rising: bool = Query(default=False),
    search: str | None = Query(default=None),
) -> dict:
    if window not in supported_windows():
        raise HTTPException(status_code=400, detail=f"Unsupported window '{window}'")
    service = get_service(request)
    valid_profiles = set(service.briefing_profiles())
    if profile.strip().lower() not in valid_profiles:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported briefing profile '{profile}'. Valid: {', '.join(sorted(valid_profiles))}",
        )
    return service.get_paper_portfolio(
        window=window,
        profile=profile,
        verticals=parse_csv_values(verticals),
        sources=parse_csv_values(sources),
        only_rising=only_rising,
        search=search,
    )


@router.post("/refresh")
async def refresh_topics(request: Request, window: str = Query("24h"), force: bool = Query(default=False)) -> dict:
    if window not in supported_windows():
        raise HTTPException(status_code=400, detail=f"Unsupported window '{window}'")

    service = get_service(request)
    if force:
        await service.refresh_all_windows(force=True)
    else:
        service.schedule_refresh(window=window, force=False)

    return {
        "status": "ok",
        "window": window,
        "force": force,
        "requested_at": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/alerts", response_model=AlertRulesResponse)
def list_alerts(
    request: Request,
    events_limit: int = Query(default=50, ge=1, le=500),
) -> dict:
    service = get_service(request)
    return {
        "rules": service.list_alert_rules(),
        "events": service.list_alert_events(limit=events_limit),
    }


@router.post("/alerts/rules", response_model=AlertRuleModel)
def create_alert_rule(request: Request, payload: AlertRuleUpsertRequest) -> dict:
    service = get_service(request)
    return service.upsert_alert_rule(payload=payload.model_dump())


@router.put("/alerts/rules/{rule_id}", response_model=AlertRuleModel)
def update_alert_rule(rule_id: str, request: Request, payload: AlertRuleUpsertRequest) -> dict:
    service = get_service(request)
    return service.upsert_alert_rule(payload=payload.model_dump(), rule_id=rule_id)


@router.delete("/alerts/rules/{rule_id}", response_model=dict)
def delete_alert_rule(rule_id: str, request: Request) -> dict:
    service = get_service(request)
    service.delete_alert_rule(rule_id)
    return {"status": "ok", "rule_id": rule_id}


@router.post("/alerts/evaluate", response_model=AlertEvaluateResponse)
def evaluate_alerts(
    request: Request,
    rule_id: str | None = Query(default=None),
) -> dict:
    service = get_service(request)
    return service.evaluate_alert_rules(rule_id=rule_id)


@router.get("/sources")
def list_sources(request: Request) -> dict:
    service = get_service(request)
    quality_scores = service.get_source_quality_scores()
    source_rows = [
        {
            "name": source.name,
            "vertical": source.vertical,
            "category": source.category,
            "parser": source.parser,
            "cadence_minutes": source.cadence_minutes,
            "max_items": source.max_items,
            "failover_behavior": source.failover_behavior,
            "quality_score": quality_scores.get(source.name, 1.0),
        }
        for source in service.sources
    ]
    return {
        "sources": source_rows,
        "health": service.source_health_snapshot(),
    }


@router.get("/windows")
def windows() -> dict:
    return {"windows": supported_windows()}
