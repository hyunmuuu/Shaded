from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))
WED = 2  # Monday=0 ... Wednesday=2


def _to_z(dt_utc: datetime) -> str:
    return dt_utc.replace(microsecond=0, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_z(z: str) -> datetime:
    # "2026-02-04T00:00:00Z" -> aware UTC datetime
    return datetime.fromisoformat(z.replace("Z", "+00:00"))


@dataclass(frozen=True)
class WeekWindow:
    start_utc_z: str
    end_utc_z: str

    @property
    def start_kst(self) -> datetime:
        return _parse_z(self.start_utc_z).astimezone(KST)

    @property
    def end_kst(self) -> datetime:
        return _parse_z(self.end_utc_z).astimezone(KST)


def week_window_utc(now_utc: datetime | None = None) -> WeekWindow:
    """
    주간: 수요일 09:00(KST) ~ 다음 수요일 09:00(KST)
    반환: UTC Z 문자열 2개
    """
    now_utc = now_utc or datetime.now(timezone.utc)
    now_kst = now_utc.astimezone(KST)

    today_9 = now_kst.replace(hour=9, minute=0, second=0, microsecond=0)
    delta_days = (now_kst.weekday() - WED) % 7
    start_kst = today_9 - timedelta(days=delta_days)

    # 수요일인데 09:00 이전이면 지난주로
    if now_kst < start_kst:
        start_kst -= timedelta(days=7)

    end_kst = start_kst + timedelta(days=7)

    return WeekWindow(
        start_utc_z=_to_z(start_kst.astimezone(timezone.utc)),
        end_utc_z=_to_z(end_kst.astimezone(timezone.utc)),
    )


def last_week_window_utc(now_utc: datetime | None = None) -> WeekWindow:
    w = week_window_utc(now_utc)
    start = _parse_z(w.start_utc_z) - timedelta(days=7)
    end = _parse_z(w.end_utc_z) - timedelta(days=7)
    return WeekWindow(_to_z(start), _to_z(end))
