"""Observe-only event calendar context helpers for FXPM."""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Union


@dataclass
class EventRecord:
    event_time: datetime
    currency: str = ""
    symbol: str = ""
    severity: str = ""
    title: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def minutes_to_event(self, at: datetime) -> float:
        return (self.event_time - at).total_seconds() / 60.0

    def to_dict(self, at: Optional[datetime] = None) -> Dict[str, Any]:
        payload = {
            "event_time": self.event_time.isoformat(),
            "currency": self.currency,
            "symbol": self.symbol,
            "severity": self.severity,
            "title": self.title,
            "metadata": dict(self.metadata or {}),
        }
        if at is not None:
            payload["minutes_to_event"] = round(self.minutes_to_event(at), 2)
        return payload


@dataclass
class EventObservation:
    symbol: str
    observed_at: datetime
    nearby_events: List[Dict[str, Any]] = field(default_factory=list)
    action: str = "annotate"


class EventCalendar:
    """Small CSV-backed event calendar for observe-only live annotations."""

    TIME_COLUMNS: Sequence[str] = ("event_time", "time", "datetime", "date")

    def __init__(self, events: Optional[Iterable[EventRecord]] = None):
        self.events = sorted(list(events or []), key=lambda item: item.event_time)

    @classmethod
    def from_csv(cls, path: Union[str, Path]) -> "EventCalendar":
        path = Path(path)
        if not path.exists():
            return cls()
        events: List[EventRecord] = []
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                event = cls._event_from_row(row)
                if event is not None:
                    events.append(event)
        return cls(events)

    @classmethod
    def _event_from_row(cls, row: Dict[str, Any]) -> Optional[EventRecord]:
        raw_time = ""
        for column in cls.TIME_COLUMNS:
            if row.get(column):
                raw_time = str(row[column])
                break
        if not raw_time:
            return None
        try:
            event_time = datetime.fromisoformat(raw_time.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            try:
                event_time = datetime.strptime(raw_time, "%Y-%m-%d %H:%M:%S")
            except Exception:
                return None
        return EventRecord(
            event_time=event_time,
            currency=str(row.get("currency", "") or "").upper(),
            symbol=str(row.get("symbol", "") or "").upper(),
            severity=str(row.get("severity", row.get("impact", "")) or "").lower(),
            title=str(row.get("title", row.get("event", "")) or ""),
            metadata={k: v for k, v in row.items() if k not in {"event_time", "time", "datetime", "date"}},
        )

    @staticmethod
    def _symbol_currencies(symbol: str) -> Set[str]:
        clean = "".join(ch for ch in str(symbol or "").upper() if ch.isalpha())
        if len(clean) >= 6:
            return {clean[:3], clean[3:6]}
        return {clean} if clean else set()

    def nearby_events(self,
                      symbol: str,
                      at: Optional[datetime] = None,
                      window_minutes: int = 120,
                      severities: Optional[Iterable[str]] = None) -> List[EventRecord]:
        at = at or datetime.now()
        window = timedelta(minutes=max(1, int(window_minutes)))
        start = at - window
        end = at + window
        wanted_currencies = self._symbol_currencies(symbol)
        wanted_symbol = str(symbol or "").upper()
        severity_filter = {str(item).lower() for item in (severities or []) if str(item).strip()}
        result: List[EventRecord] = []
        for event in self.events:
            if event.event_time < start or event.event_time > end:
                continue
            if severity_filter and event.severity.lower() not in severity_filter:
                continue
            event_symbol = str(event.symbol or "").upper()
            event_currency = str(event.currency or "").upper()
            if event_symbol and event_symbol != wanted_symbol:
                continue
            if not event_symbol and event_currency and event_currency not in wanted_currencies:
                continue
            result.append(event)
        return result


def observe_event_context(symbol: str,
                          config: Any,
                          nearby_events: Optional[List[Dict[str, Any]]] = None,
                          observed_at: Optional[datetime] = None,
                          calendar: Optional[EventCalendar] = None) -> EventObservation:
    enabled = bool(getattr(config, "event_calendar_enabled", False))
    mode = str(getattr(config, "event_calendar_mode", "observe") or "").lower()
    at = observed_at or datetime.now()
    events = list(nearby_events or [])
    if enabled and mode == "observe" and calendar is None:
        path = str(getattr(config, "event_calendar_path", "") or "")
        calendar = EventCalendar.from_csv(path) if path else EventCalendar()
    if enabled and mode == "observe" and calendar is not None and not events:
        events = [event.to_dict(at) for event in calendar.nearby_events(symbol, at=at)]
    action = "annotate" if enabled and mode == "observe" else "none"
    return EventObservation(symbol=str(symbol), observed_at=at, nearby_events=events, action=action)
