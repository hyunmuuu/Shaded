from __future__ import annotations

import aiosqlite

from shaded.services.sqlite_conn import open_db

# scope: "normal"(일반) | "ranked"(경쟁) | "total"(전체)
SQL_WEEKLY = """
SELECT
  p.player_name AS player_name,
  COALESCE(SUM(pm.kills), 0) AS kills
FROM clan_members cm
JOIN players p
  ON p.platform = cm.platform AND p.account_id = cm.account_id
JOIN player_matches pm
  ON pm.platform = cm.platform AND pm.account_id = cm.account_id
JOIN matches m
  ON m.platform = pm.platform AND m.match_id = pm.match_id
WHERE
  cm.clan_id = :clan_id
  AND cm.platform = :platform
  AND COALESCE(cm.is_active, 1) = 1
  AND m.created_at_utc >= :start_utc
  AND m.created_at_utc <  :end_utc
  -- 캐주얼/커스텀 제외(고정)
  AND COALESCE(m.is_casual, 0) = 0
  AND COALESCE(m.is_custom_match, 0) = 0
  -- scope 필터(아래에서 추가)
  {scope_clause}
GROUP BY p.player_name
ORDER BY kills DESC, p.player_name ASC
LIMIT :limit;
"""


async def _fetchone(con: aiosqlite.Connection, sql: str, params: dict) -> aiosqlite.Row | None:
    """
    aiosqlite 버전/래퍼 차이로 execute_fetchone이 없을 수 있어서 호환 처리
    """
    if hasattr(con, "execute_fetchone"):
        return await con.execute_fetchone(sql, params)  # type: ignore

    cur = await con.execute(sql, params)
    try:
        row = await cur.fetchone()
        return row
    finally:
        await cur.close()


async def _fetchall(con: aiosqlite.Connection, sql: str, params: dict) -> list[aiosqlite.Row]:
    if hasattr(con, "execute_fetchall"):
        return await con.execute_fetchall(sql, params)  # type: ignore

    cur = await con.execute(sql, params)
    try:
        rows = await cur.fetchall()
        return list(rows)
    finally:
        await cur.close()


async def fetch_weekly_leaderboard(
    db_path: str,
    clan_id: str,
    platform: str,
    start_utc_z: str,
    end_utc_z: str,
    scope: str,   # "normal" | "ranked" | "total"
    limit: int = 10,
) -> list[tuple[str, int]]:
    scope = (scope or "total").lower()
    if scope == "normal":
        scope_clause = "AND COALESCE(m.is_ranked, 0) = 0"
    elif scope == "ranked":
        scope_clause = "AND COALESCE(m.is_ranked, 0) = 1"
    else:
        scope_clause = ""  # total

    sql = SQL_WEEKLY.format(scope_clause=scope_clause)

    async with open_db(db_path) as con:
        con.row_factory = aiosqlite.Row
        rows = await _fetchall(
            con,
            sql,
            {
                "clan_id": clan_id,
                "platform": platform,
                "start_utc": start_utc_z,
                "end_utc": end_utc_z,
                "limit": limit,
            },
        )
    return [(r["player_name"], int(r["kills"])) for r in rows]


# (호환용) 기존 함수명이 다른 곳에서 호출될 수 있어서 래퍼 유지
async def fetch_weekly_leaderboard_normal(
    db_path: str,
    clan_id: str,
    platform: str,
    start_utc_z: str,
    end_utc_z: str,
    limit: int = 10,
) -> list[tuple[str, int]]:
    return await fetch_weekly_leaderboard(db_path, clan_id, platform, start_utc_z, end_utc_z, "normal", limit)


# =========================
# weekly snapshot (지난랭킹 스냅샷)
# =========================

SNAPSHOT_META_SQL = """
CREATE TABLE IF NOT EXISTS weekly_snapshot_meta (
  clan_id        TEXT NOT NULL,
  platform       TEXT NOT NULL,
  week_start_utc TEXT NOT NULL,
  week_end_utc   TEXT NOT NULL,
  scope          TEXT NOT NULL,  -- normal|ranked|total
  created_at_utc TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (clan_id, platform, week_start_utc, scope)
);
"""

SNAPSHOT_ROWS_SQL = """
CREATE TABLE IF NOT EXISTS weekly_snapshot_rows (
  clan_id        TEXT NOT NULL,
  platform       TEXT NOT NULL,
  week_start_utc TEXT NOT NULL,
  scope          TEXT NOT NULL,
  rank           INTEGER NOT NULL,
  player_name    TEXT NOT NULL,
  kills          INTEGER NOT NULL,
  PRIMARY KEY (clan_id, platform, week_start_utc, scope, rank)
);
"""

SNAPSHOT_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_weekly_snapshot_rows_lookup
ON weekly_snapshot_rows (clan_id, platform, week_start_utc, scope);
"""

SQL_SNAPSHOT_META = """
SELECT week_end_utc, created_at_utc
  FROM weekly_snapshot_meta
 WHERE clan_id = :clan_id
   AND platform = :platform
   AND week_start_utc = :week_start_utc
   AND scope = :scope
 LIMIT 1;
"""

SQL_SNAPSHOT_ROWS = """
SELECT player_name, kills
  FROM weekly_snapshot_rows
 WHERE clan_id = :clan_id
   AND platform = :platform
   AND week_start_utc = :week_start_utc
   AND scope = :scope
 ORDER BY rank ASC
 LIMIT :limit;
"""


async def init_weekly_snapshot_tables(db_path: str) -> None:
    async with open_db(db_path) as db:
        await db.execute(SNAPSHOT_META_SQL)
        await db.execute(SNAPSHOT_ROWS_SQL)
        await db.execute(SNAPSHOT_INDEX_SQL)
        await db.commit()


async def fetch_weekly_snapshot(
    db_path: str,
    clan_id: str,
    platform: str,
    week_start_utc_z: str,
    scope: str,
    limit: int = 10,
) -> tuple[list[tuple[str, int]], str | None]:
    """
    return: (rows, snapshot_created_at_utc or None)

    - 스냅샷이 없으면 ([], None)
    - 스냅샷은 있어도 Top10이 비어있을 수 있음(클랜 활동 0) → ([], created_at_utc)
    """
    scope = (scope or "total").lower()

    async with open_db(db_path) as con:
        con.row_factory = aiosqlite.Row

        meta = await _fetchone(
            con,
            SQL_SNAPSHOT_META,
            {
                "clan_id": clan_id,
                "platform": platform,
                "week_start_utc": week_start_utc_z,
                "scope": scope,
            },
        )
        if not meta:
            return [], None

        rows = await _fetchall(
            con,
            SQL_SNAPSHOT_ROWS,
            {
                "clan_id": clan_id,
                "platform": platform,
                "week_start_utc": week_start_utc_z,
                "scope": scope,
                "limit": limit,
            },
        )

        out = [(r["player_name"], int(r["kills"])) for r in rows]
        return out, str(meta["created_at_utc"])
