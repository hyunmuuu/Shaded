from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))  # Korea has no DST
WED = 2  # Monday=0 ... Wednesday=2

def to_z(dt_utc: datetime) -> str:
    return dt_utc.replace(microsecond=0, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

def from_z(z: str) -> datetime:
    return datetime.fromisoformat(z.replace("Z", "+00:00"))

def week_window_utc(now_utc: datetime | None = None) -> tuple[str, str]:
    now_utc = now_utc or datetime.now(timezone.utc)
    now_kst = now_utc.astimezone(KST)

    today_9 = now_kst.replace(hour=9, minute=0, second=0, microsecond=0)
    delta_days = (now_kst.weekday() - WED) % 7
    start_kst = today_9 - timedelta(days=delta_days)

    if now_kst < start_kst:
        start_kst -= timedelta(days=7)

    end_kst = start_kst + timedelta(days=7)

    start_utc = start_kst.astimezone(timezone.utc)
    end_utc = end_kst.astimezone(timezone.utc)

    return to_z(start_utc), to_z(end_utc)

def last_week_window_utc(now_utc: datetime | None = None) -> tuple[str, str]:
    this_start_z, _ = week_window_utc(now_utc)
    this_start = from_z(this_start_z)
    last_start = this_start - timedelta(days=7)
    last_end = this_start
    return to_z(last_start), to_z(last_end)

if __name__ == "__main__":
    s, e = week_window_utc()
    ls, le = last_week_window_utc()
    print("this_week_start_utc =", s)
    print("this_week_end_utc   =", e)
    print("last_week_start_utc =", ls)
    print("last_week_end_utc   =", le)
