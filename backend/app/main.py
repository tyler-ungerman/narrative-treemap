from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import router as api_router
from app.core.config import settings
from app.core.database import Database
from app.core.logging import configure_logging
from app.pipeline.service import NarrativeService

logger = logging.getLogger(__name__)


async def periodic_refresh(service: NarrativeService, interval_seconds: int) -> None:
    while True:
        try:
            await service.refresh_all_windows(force=False)
        except Exception as exc:
            logger.exception("periodic_refresh_failed", extra={"extra": {"error": str(exc)}})
        await asyncio.sleep(interval_seconds)


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    database = Database(settings.database_file)
    narrative_service = NarrativeService(database=database)
    narrative_service.seed_cache_if_needed()

    app.state.database = database
    app.state.narrative_service = narrative_service
    app.state.refresh_task = asyncio.create_task(periodic_refresh(narrative_service, settings.refresh_interval_seconds))
    app.state.bootstrap_task = asyncio.create_task(narrative_service.refresh_all_windows(force=False))

    yield

    for task_name in ("refresh_task", "bootstrap_task"):
        task = getattr(app.state, task_name, None)
        if task:
            task.cancel()
    database.close()


app = FastAPI(title=settings.app_name, lifespan=lifespan)
configured_origins = [origin.strip() for origin in settings.cors_origins.split(",") if origin.strip()]
allow_all_origins = "*" in configured_origins or not configured_origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if allow_all_origins else configured_origins,
    allow_credentials=not allow_all_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
