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
        rows = await con.execute_fetchall(
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
