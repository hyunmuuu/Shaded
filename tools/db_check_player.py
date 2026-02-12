from __future__ import annotations

import sys
import sqlite3
from pathlib import Path

from print_week_window import last_week_window_utc, week_window_utc

DB_PATH = Path("db/shaded.db")
CLAN_ID = "shaded_steam"
PLATFORM = "steam"

def main():
    if len(sys.argv) < 2:
        raise SystemExit('사용법: python tools/db_check_player.py "PUBG닉네임"')

    name = sys.argv[1]
    last_s, last_e = last_week_window_utc()
    this_s, this_e = week_window_utc()

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # 1) account_id 확인
    row = cur.execute(
        "SELECT account_id FROM players WHERE platform=? AND lower(player_name)=lower(?)",
        (PLATFORM, name),
    ).fetchone()
    if not row:
        print("player not found in players table:", name)
        con.close()
        return

    account_id = row[0]
    print("== player ==")
    print("name      =", name)
    print("account_id=", account_id)

    # 2) clan_members에 있는지/active인지
    cm = cur.execute(
        """
        SELECT clan_id, platform, is_active
        FROM clan_members
        WHERE clan_id=? AND platform=? AND account_id=?
        """,
        (CLAN_ID, PLATFORM, account_id),
    ).fetchone()
    print("\n== clan_members ==")
    print(cm)

    # 3) 지난주/이번주 윈도우
    print("\n== windows (UTC Z) ==")
    print("LAST :", last_s, "~", last_e)
    print("THIS :", this_s, "~", this_e)

    # 4) 지난주: 필터 없이(매치/킬)
    print("\n== LAST: NO FILTER ==")
    r = cur.execute(
        """
        SELECT
          COUNT(DISTINCT m.match_id) AS match_cnt,
          COALESCE(SUM(pm.kills),0) AS kills
        FROM player_matches pm
        JOIN matches m ON m.platform=pm.platform AND m.match_id=pm.match_id
        WHERE pm.platform=? AND pm.account_id=?
          AND m.created_at_utc>=? AND m.created_at_utc<?
        """,
        (PLATFORM, account_id, last_s, last_e),
    ).fetchone()
    print("match_cnt, kills =", r)

    # 5) 지난주: 필터 적용(캐주얼/커스텀 제외 + scope별)
    def filt(scope: str):
        ranked_clause = "1=1"
        if scope == "normal":
            ranked_clause = "COALESCE(m.is_ranked,0)=0"
        elif scope == "ranked":
            ranked_clause = "COALESCE(m.is_ranked,0)=1"

        return cur.execute(
            f"""
            SELECT
              COUNT(DISTINCT m.match_id) AS match_cnt,
              COALESCE(SUM(pm.kills),0) AS kills
            FROM player_matches pm
            JOIN matches m ON m.platform=pm.platform AND m.match_id=pm.match_id
            WHERE pm.platform=? AND pm.account_id=?
              AND m.created_at_utc>=? AND m.created_at_utc<?
              AND COALESCE(m.is_casual,0)=0
              AND COALESCE(m.is_custom_match,0)=0
              AND {ranked_clause}
            """,
            (PLATFORM, account_id, last_s, last_e),
        ).fetchone()

    print("\n== LAST: FILTERED ==")
    print("normal =", filt("normal"))
    print("ranked =", filt("ranked"))
    print("total  =", filt("total"))

    # 6) 지난주: 플래그 분포(왜 다 걸리는지 바로 보임)
    print("\n== LAST: flag distribution ==")
    rows = cur.execute(
        """
        SELECT
          COALESCE(is_ranked,0) AS is_ranked,
          COALESCE(is_casual,0) AS is_casual,
          COALESCE(is_custom_match,0) AS is_custom_match,
          COUNT(*) AS cnt,
          COALESCE(SUM(pm.kills),0) AS kills
        FROM player_matches pm
        JOIN matches m ON m.platform=pm.platform AND m.match_id=pm.match_id
        WHERE pm.platform=? AND pm.account_id=?
          AND m.created_at_utc>=? AND m.created_at_utc<?
        GROUP BY 1,2,3
        ORDER BY cnt DESC
        """,
        (PLATFORM, account_id, last_s, last_e),
    ).fetchall()
    for x in rows:
        print(x)

    con.close()

if __name__ == "__main__":
    main()
