import sqlite3
from pathlib import Path
from print_week_window import week_window_utc, last_week_window_utc

DB_PATH = Path("db/shaded.db")
SQL_PATH = Path("db/query_weekly_leaderboard.sql")

CLAN_ID = "shaded_steam"
LIMIT = 10  # Top10

def fetch(week_start_utc: str, week_end_utc: str):
    sql = SQL_PATH.read_text(encoding="utf-8")

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    rows = cur.execute(
        sql,
        {
            "clan_id": CLAN_ID,
            "week_start_utc": week_start_utc,
            "week_end_utc": week_end_utc,
            "limit": LIMIT,
        },
    ).fetchall()

    con.close()
    return rows

def print_rank(title: str, start_utc: str, end_utc: str, rows):
    print("=" * 60)
    print(title)
    print(f"[{CLAN_ID}] {start_utc} ~ {end_utc} (UTC)")
    if not rows:
        print("(no data)")
        return
    for i, r in enumerate(rows, 1):
        print(f"{i:>2}. {r['player_name']:<20} {int(r['kills'])}")

def main():
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")
    if not SQL_PATH.exists():
        raise SystemExit(f"SQL not found: {SQL_PATH}")

    this_s, this_e = week_window_utc()
    last_s, last_e = last_week_window_utc()

    this_rows = fetch(this_s, this_e)
    last_rows = fetch(last_s, last_e)

    print_rank("[/주간랭킹] 이번 주 TOP10", this_s, this_e, this_rows)
    print_rank("[/지난랭킹] 지난 주 TOP10", last_s, last_e, last_rows)

if __name__ == "__main__":
    main()
