from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from app.core.database import Database

logger = logging.getLogger(__name__)


def _normalize_stooq_symbol(ticker: str) -> str | None:
    normalized = ticker.strip().upper()
    if not normalized:
        return None
    if normalized.endswith("-USD"):
        return None
    if not normalized.replace(".", "").replace("-", "").isalnum():
        return None
    if "." in normalized:
        return normalized.lower()
    if normalized.isalpha() and len(normalized) <= 5:
        return f"{normalized.lower()}.us"
    return None


class MarketDataService:
    def __init__(self, database: Database):
        self.database = database
        self._timeout = httpx.Timeout(10.0, connect=5.0)
        self._user_agent = "NarrativeTreemap/1.0 (market-data; local-first)"

    def _fetch_history_from_stooq(
        self,
        *,
        ticker: str,
        start_at: datetime,
        end_at: datetime,
    ) -> list[dict[str, Any]]:
        symbol = _normalize_stooq_symbol(ticker)
        if not symbol:
            return []

        start_text = start_at.date().strftime("%Y%m%d")
        end_text = end_at.date().strftime("%Y%m%d")
        url = f"https://stooq.com/q/d/l/?s={symbol}&i=d&d1={start_text}&d2={end_text}"

        try:
            with httpx.Client(timeout=self._timeout, headers={"User-Agent": self._user_agent}) as client:
                response = client.get(url)
                response.raise_for_status()
            content = response.text.strip()
        except Exception as exc:
            logger.warning(
                "market_data_fetch_failed",
                extra={"extra": {"ticker": ticker, "url": url, "error": str(exc)}},
            )
            return []

        if not content:
            return []

        reader = csv.DictReader(io.StringIO(content))
        rows: list[dict[str, Any]] = []
        for row in reader:
            date_value = (row.get("Date") or "").strip()
            close_value = (row.get("Close") or "").strip()
            if not date_value or not close_value:
                continue
            try:
                close = float(close_value)
            except ValueError:
                continue
            rows.append(
                {
                    "price_date": date_value,
                    "close": close,
                }
            )
        if rows:
            self.database.save_asset_prices(ticker=ticker, prices=rows)
        return rows

    def _ensure_price_range(
        self,
        *,
        ticker: str,
        start_at: datetime,
        end_at: datetime,
    ) -> None:
        cached = self.database.get_asset_prices(
            ticker=ticker,
            start_date=start_at,
            end_date=end_at,
        )
        if cached:
            latest_cached = cached[-1]
            fetched_at = latest_cached.get("fetched_at")
            if isinstance(fetched_at, datetime):
                age_seconds = (datetime.now(timezone.utc) - fetched_at).total_seconds()
                if age_seconds <= 6 * 3600:
                    return
        fetch_start = start_at - timedelta(days=5)
        fetch_end = end_at + timedelta(days=1)
        self._fetch_history_from_stooq(ticker=ticker, start_at=fetch_start, end_at=fetch_end)

    def close_on_or_before(self, *, ticker: str, on_or_before: datetime) -> float | None:
        check_start = on_or_before - timedelta(days=45)
        self._ensure_price_range(ticker=ticker, start_at=check_start, end_at=on_or_before)
        row = self.database.get_latest_asset_price_before(ticker=ticker, on_or_before=on_or_before)
        if not row:
            return None
        return float(row["close"])

    def basket_return(
        self,
        *,
        tickers: list[str],
        entry_at: datetime,
        exit_at: datetime,
        direction: str,
    ) -> dict[str, Any]:
        leg_returns: list[dict[str, Any]] = []
        for ticker in tickers:
            entry_price = self.close_on_or_before(ticker=ticker, on_or_before=entry_at)
            exit_price = self.close_on_or_before(ticker=ticker, on_or_before=exit_at)
            if entry_price is None or exit_price is None or entry_price <= 0:
                continue
            raw_return = (exit_price - entry_price) / entry_price
            direction_upper = direction.strip().lower()
            if direction_upper == "short":
                directional_return = -raw_return
            elif direction_upper == "hedge":
                directional_return = abs(raw_return)
            else:
                directional_return = raw_return
            leg_returns.append(
                {
                    "ticker": ticker,
                    "entry_price": round(entry_price, 4),
                    "exit_price": round(exit_price, 4),
                    "raw_return": round(raw_return, 6),
                    "directional_return": round(directional_return, 6),
                }
            )

        if not leg_returns:
            return {
                "available": False,
                "basket_return": None,
                "leg_returns": [],
            }

        basket = sum(row["directional_return"] for row in leg_returns) / len(leg_returns)
        return {
            "available": True,
            "basket_return": round(float(basket), 6),
            "leg_returns": leg_returns,
        }
