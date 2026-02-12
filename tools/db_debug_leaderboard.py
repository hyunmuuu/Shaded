from __future__ import annotations

import sqlite3
from pathlib import Path

# ✅ tools 폴더 안에 있으니, 같은 폴더의 print_week_window를 그대로 사용
from print_week_window import week_window_utc, last_week_window_utc

DB_PATH = Path("db/shaded.db")
CLAN_ID = "shaded_steam"
PLATFORM = "steam"


def has_col(con: sqlite3.Connection, table: str, col: str) -> bool:
    cols = [r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()]
    return col in cols


def main():
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    w_this = week_window_utc()
    w_last = last_week_window_utc()

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    print("== DB PATH ==")
    print(DB_PATH.resolve())

    print("\n== WINDOWS (UTC Z) ==")
    print("THIS :", w_this[0], "~", w_this[1])
    print("LAST :", w_last[0], "~", w_last[1])

    # clan_id 실제 값 확인
    print("\n== clan_members clan_id/platform DISTINCT ==")
    rows = cur.execute(
        "SELECT clan_id, platform, COUNT(*) FROM clan_members GROUP BY clan_id, platform ORDER BY clan_id, platform"
    ).fetchall()
    for r in rows:
        print(r)

    # 멤버 확인
    print("\n== MEMBERS (alias clan_id) ==")
    rows = cur.execute(
        """
        SELECT p.player_name, cm.account_id, cm.platform, cm.is_active
        FROM clan_members cm
        JOIN players p ON p.platform=cm.platform AND p.account_id=cm.account_id
        WHERE cm.clan_id=? AND cm.platform=?
        ORDER BY p.player_name
        """,
        (CLAN_ID, PLATFORM),
    ).fetchall()
    print("members_cnt =", len(rows))
    for r in rows:
        print(r)

    # 테이블 카운트
    def cnt(t: str) -> int:
        return cur.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]

    print("\n== TABLE COUNTS ==")
    for t in ["players", "clan_members", "matches", "player_matches"]:
        try:
            print(t, "=", cnt(t))
        except Exception as e:
            print(t, "ERROR:", e)

    # matches 스키마 + 최소/최대 created_at_utc
    print("\n== matches schema ==")
    print(cur.execute("PRAGMA table_info(matches)").fetchall())

    print("\n== matches created_at_utc range ==")
    mm = cur.execute("SELECT MIN(created_at_utc), MAX(created_at_utc) FROM matches").fetchone()
    print("min,max =", mm)

    last_s, last_e = w_last

    print("\n== matches count in LAST window ==")
    c_last = cur.execute(
        "SELECT COUNT(*) FROM matches WHERE platform=? AND created_at_utc>=? AND created_at_utc<?",
        (PLATFORM, last_s, last_e),
    ).fetchone()[0]
    print("matches_in_last =", c_last)

    print("\n== player_matches sanity ==")
    pm_total = cur.execute("SELECT COUNT(*) FROM player_matches WHERE platform=?", (PLATFORM,)).fetchone()[0]
    print("player_matches_total =", pm_total)

    # player_matches 중 matches와 매칭 안 되는 것(조인 실패)
    pm_orphan = cur.execute(
        """
        SELECT COUNT(*)
        FROM player_matches pm
        LEFT JOIN matches m ON m.platform=pm.platform AND m.match_id=pm.match_id
        WHERE pm.platform=? AND m.match_id IS NULL
        """,
        (PLATFORM,),
    ).fetchone()[0]
    print("player_matches_orphan(no match row) =", pm_orphan)

    # 지난주 구간: 필터 없이 멤버별 매치수/킬
    print("\n== LAST window: per member (NO FILTER) ==")
    rows = cur.execute(
        """
        SELECT
          p.player_name,
          COUNT(DISTINCT m.match_id) AS match_cnt,
          COALESCE(SUM(pm.kills),0) AS kills
        FROM clan_members cm
        JOIN players p ON p.platform=cm.platform AND p.account_id=cm.account_id
        LEFT JOIN player_matches pm ON pm.platform=cm.platform AND pm.account_id=cm.account_id
        LEFT JOIN matches m
          ON m.platform=pm.platform AND m.match_id=pm.match_id
         AND m.created_at_utc>=? AND m.created_at_utc<?
        WHERE cm.clan_id=? AND cm.platform=? AND COALESCE(cm.is_active,1)=1
        GROUP BY p.player_name
        ORDER BY kills DESC, p.player_name
        """,
        (last_s, last_e, CLAN_ID, PLATFORM),
    ).fetchall()
    for r in rows:
        print(r)

    # 필터 컬럼 존재 여부
    has_ranked = has_col(con, "matches", "is_ranked")
    has_casual = has_col(con, "matches", "is_casual")
    has_custom = has_col(con, "matches", "is_custom_match")

    print("\n== matches flag columns ==")
    print("has is_ranked =", has_ranked, "/ has is_casual =", has_casual, "/ has is_custom_match =", has_custom)

    # 필터 적용(있을 때만)
    if has_ranked or has_casual or has_custom:
        def extra(where_ranked: str):
            casual_clause = "1=1" if not has_casual else "COALESCE(m.is_casual,0)=0"
            custom_clause = "1=1" if not has_custom else "COALESCE(m.is_custom_match,0)=0"
            ranked_clause = "1=1" if not has_ranked else where_ranked
            return casual_clause, custom_clause, ranked_clause

        def run(label: str, where_ranked: str):
            casual_clause, custom_clause, ranked_clause = extra(where_ranked)
            print(f"\n== LAST window: {label} ==")
            rows2 = cur.execute(
                f"""
                SELECT
                  p.player_name,
                  COUNT(DISTINCT m.match_id) AS match_cnt,
                  COALESCE(SUM(pm.kills),0) AS kills
                FROM clan_members cm
                JOIN players p ON p.platform=cm.platform AND p.account_id=cm.account_id
                JOIN player_matches pm ON pm.platform=cm.platform AND pm.account_id=cm.account_id
                JOIN matches m ON m.platform=pm.platform AND m.match_id=pm.match_id
                WHERE cm.clan_id=? AND cm.platform=? AND COALESCE(cm.is_active,1)=1
                  AND m.created_at_utc>=? AND m.created_at_utc<?
                  AND {casual_clause}
                  AND {custom_clause}
                  AND {ranked_clause}
                GROUP BY p.player_name
                ORDER BY kills DESC, p.player_name
                """,
                (CLAN_ID, PLATFORM, last_s, last_e),
            ).fetchall()
            for r in rows2:
                print(r)

        run("NORMAL (ranked=0)", "COALESCE(m.is_ranked,0)=0")
        run("RANKED (ranked=1)", "COALESCE(m.is_ranked,0)=1")
        run("TOTAL (no ranked filter)", "1=1")

    # 샘플 매치 보기
    print("\n== SAMPLE matches in LAST window (up to 5) ==")
    rows = cur.execute(
        """
        SELECT match_id, created_at_utc, game_mode
        FROM matches
        WHERE platform=? AND created_at_utc>=? AND created_at_utc<?
        ORDER BY created_at_utc DESC
        LIMIT 5
        """,
        (PLATFORM, last_s, last_e),
    ).fetchall()
    for r in rows:
        print(r)

    con.close()


if __name__ == "__main__":
    main()
