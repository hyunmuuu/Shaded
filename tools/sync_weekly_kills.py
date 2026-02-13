import os
import asyncio
import socket
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple
from datetime import datetime, timezone

from dotenv import load_dotenv
import aiohttp

load_dotenv(override=True)

from shaded.services.pubg_api import PubgApiClient, PubgApiError
from shaded.utils.time_window import last_week_window_utc
from shaded.services.clan_store import CLAN_ID_ALIAS
from shaded.services.sqlite_conn import open_db_sync

# sync_state는 프로젝트 버전에 따라 함수가 다를 수 있어서 안전하게 처리
try:
    from shaded.services.sync_state import set_weekly_sync_last_utc_z, set_weekly_sync_last_error
except Exception:
    from shaded.services.sync_state import set_weekly_sync_last_utc_z  # type: ignore

    async def set_weekly_sync_last_error(db_path: str, msg: str) -> None:  # type: ignore
        return


DB_PATH = Path(os.getenv("DB_PATH", "db/shaded.db"))
SHARD = os.getenv("PUBG_SHARD", "steam").strip() or "steam"
API_KEY = (os.getenv("PUBG_API_KEY", "") or "").strip().removeprefix("Bearer ").strip()

# 집계 대상 6모드(솔/듀/스쿼드 + FPP 3개)
ALLOWED_MODES = {"solo", "duo", "squad", "solo-fpp", "duo-fpp", "squad-fpp"}

# DB 안정화(환경변수로 조절 가능)
BUSY_TIMEOUT_SEC = float(os.getenv("SQLITE_BUSY_TIMEOUT_SEC", "8"))
JOB_LOCK_TTL_SEC = int(os.getenv("SYNC_JOB_LOCK_TTL_SEC", "1800"))
JOB_NAME = "sync_weekly_kills"
WRITE_BATCH_SIZE = int(os.getenv("SYNC_WRITE_BATCH_SIZE", "25"))

# 스냅샷: 지난주 Top10을 고정 저장
SNAPSHOT_SCOPES = ("normal", "ranked", "total")


def _to_z(dt_utc: datetime) -> str:
    return dt_utc.replace(microsecond=0, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def _chunked(xs: List[str], n: int) -> List[List[str]]:
    return [xs[i:i + n] for i in range(0, len(xs), n)]


def _classify_match_flags(attrs: dict, game_mode: str | None) -> Tuple[int, int, int]:
    gm = (game_mode or "").lower()

    v = attrs.get("isRanked")
    if isinstance(v, bool):
        is_ranked = 1 if v else 0
    else:
        is_ranked = 1 if "ranked" in gm else 0

    is_custom_match = 1 if attrs.get("isCustomMatch") else 0
    is_casual = 1 if "casual" in gm else 0
    return is_ranked, is_custom_match, is_casual


def _extract_participant_kills(match_json: Dict[str, Any], clan_ids: Set[str]) -> List[Tuple[str, str, int]]:
    """
    return [(account_id, player_name, kills), ...] for clan members only
    """
    out: List[Tuple[str, str, int]] = []
    for obj in match_json.get("included") or []:
        if obj.get("type") != "participant":
            continue
        stats = ((obj.get("attributes") or {}).get("stats") or {})
        pid = stats.get("playerId")
        if not pid or pid not in clan_ids:
            continue
        nm = (stats.get("name") or "").strip() or pid
        k = stats.get("kills")
        try:
            kills = int(k) if k is not None else 0
        except Exception:
            kills = 0
        out.append((pid, nm, kills))
    return out


def _ensure_tables(con) -> None:
    con.execute("PRAGMA foreign_keys=ON;")

    con.execute("""
    CREATE TABLE IF NOT EXISTS matches (
      match_id        TEXT PRIMARY KEY,
      platform        TEXT NOT NULL,
      created_at_utc  TEXT NOT NULL,
      game_mode       TEXT,
      is_ranked       INTEGER NOT NULL DEFAULT 0,
      inserted_at_utc TEXT NOT NULL DEFAULT (datetime('now')),
      is_custom_match INTEGER NOT NULL DEFAULT 0,
      is_casual       INTEGER NOT NULL DEFAULT 0
    )
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS player_matches (
      match_id        TEXT NOT NULL,
      platform        TEXT NOT NULL,
      account_id      TEXT NOT NULL,
      kills           INTEGER NOT NULL DEFAULT 0,
      inserted_at_utc TEXT NOT NULL DEFAULT (datetime('now')),
      PRIMARY KEY (match_id, platform, account_id),
      FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
    )
    """)

    con.execute("""
    CREATE INDEX IF NOT EXISTS idx_matches_time_flags
    ON matches(created_at_utc, is_ranked, is_casual, is_custom_match)
    """)

    con.execute("""
    CREATE INDEX IF NOT EXISTS idx_player_matches_player
    ON player_matches(platform, account_id)
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS sync_state (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      updated_at INTEGER NOT NULL
    )
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS job_lock (
      job_name     TEXT PRIMARY KEY,
      locked_until INTEGER NOT NULL,
      locked_by    TEXT,
      updated_at   INTEGER NOT NULL
    )
    """)

    # 지난랭킹 스냅샷 테이블
    con.execute("""
    CREATE TABLE IF NOT EXISTS weekly_snapshot_meta (
      clan_id        TEXT NOT NULL,
      platform       TEXT NOT NULL,
      week_start_utc TEXT NOT NULL,
      week_end_utc   TEXT NOT NULL,
      scope          TEXT NOT NULL,  -- normal|ranked|total
      created_at_utc TEXT NOT NULL DEFAULT (datetime('now')),
      PRIMARY KEY (clan_id, platform, week_start_utc, scope)
    )
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS weekly_snapshot_rows (
      clan_id        TEXT NOT NULL,
      platform       TEXT NOT NULL,
      week_start_utc TEXT NOT NULL,
      scope          TEXT NOT NULL,
      rank           INTEGER NOT NULL,
      player_name    TEXT NOT NULL,
      kills          INTEGER NOT NULL,
      PRIMARY KEY (clan_id, platform, week_start_utc, scope, rank)
    )
    """)

    con.execute("""
    CREATE INDEX IF NOT EXISTS idx_weekly_snapshot_rows_lookup
    ON weekly_snapshot_rows (clan_id, platform, week_start_utc, scope)
    """)


def _get_active_clan_members(con) -> List[Tuple[str, str]]:
    rows = con.execute(
        """
        SELECT cm.account_id, p.player_name
          FROM clan_members cm
          JOIN players p
            ON p.platform = cm.platform AND p.account_id = cm.account_id
         WHERE cm.clan_id = ?
           AND cm.platform = ?
           AND COALESCE(cm.is_active, 1) = 1
         ORDER BY COALESCE(cm.joined_at, 0) ASC, p.player_name ASC
        """,
        (CLAN_ID_ALIAS, SHARD),
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def _existing_match_ids(con, match_ids: List[str]) -> Set[str]:
    if not match_ids:
        return set()
    exist: Set[str] = set()
    for batch in _chunked(match_ids, 900):
        ph = ",".join(["?"] * len(batch))
        rows = con.execute(f"SELECT match_id FROM matches WHERE match_id IN ({ph})", batch).fetchall()
        for r in rows:
            exist.add(r[0])
    return exist


def _insert_match_and_kills(
    con,
    match_id: str,
    created_at_utc: str,
    game_mode: str,
    is_ranked: int,
    is_custom_match: int,
    is_casual: int,
    rows: List[Tuple[str, str, int]],
) -> None:
    con.execute(
        """
        INSERT OR IGNORE INTO matches (
          match_id, platform, created_at_utc, game_mode, is_ranked, is_custom_match, is_casual
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (match_id, SHARD, created_at_utc, game_mode, is_ranked, is_custom_match, is_casual),
    )

    # 이름 최신화
    for account_id, player_name, _kills in rows:
        con.execute(
            """
            UPDATE players
               SET player_name = ?, updated_at = strftime('%s','now')
             WHERE platform = ? AND account_id = ?
            """,
            (player_name, SHARD, account_id),
        )

    con.executemany(
        """
        INSERT OR REPLACE INTO player_matches (match_id, platform, account_id, kills)
        VALUES (?, ?, ?, ?)
        """,
        [(match_id, SHARD, account_id, kills) for (account_id, _nm, kills) in rows],
    )


def _try_acquire_job_lock(con, job_name: str, locked_by: str, ttl_sec: int) -> Tuple[bool, int]:
    now = int(time.time())
    until = now + int(ttl_sec)

    con.execute("BEGIN IMMEDIATE;")
    try:
        cur = con.execute(
            """
            UPDATE job_lock
               SET locked_until=?, locked_by=?, updated_at=?
             WHERE job_name=? AND locked_until < ?
            """,
            (until, locked_by, now, job_name, now),
        )
        if cur.rowcount and cur.rowcount > 0:
            con.commit()
            return True, until

        cur2 = con.execute(
            """
            INSERT OR IGNORE INTO job_lock (job_name, locked_until, locked_by, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (job_name, until, locked_by, now),
        )
        if cur2.rowcount and cur2.rowcount > 0:
            con.commit()
            return True, until

        row = con.execute("SELECT locked_until FROM job_lock WHERE job_name=?", (job_name,)).fetchone()
        locked_until = int(row[0]) if row else 0
        con.commit()
        return False, locked_until
    except Exception:
        con.rollback()
        raise


def _release_job_lock(con, job_name: str, locked_by: str) -> None:
    now = int(time.time())
    try:
        con.execute("BEGIN IMMEDIATE;")
        con.execute(
            """
            UPDATE job_lock
               SET locked_until=0, locked_by=NULL, updated_at=?
             WHERE job_name=? AND locked_by=?
            """,
            (now, job_name, locked_by),
        )
        con.commit()
    except Exception:
        con.rollback()


def _flush_pending(con, pending: List[Tuple[str, str, str, int, int, int, List[Tuple[str, str, int]]]]) -> None:
    if not pending:
        return
    con.execute("BEGIN IMMEDIATE;")
    try:
        for (mid, created_at_utc, game_mode, is_ranked, is_custom, is_casual, rows) in pending:
            _insert_match_and_kills(
                con,
                match_id=mid,
                created_at_utc=created_at_utc,
                game_mode=game_mode,
                is_ranked=is_ranked,
                is_custom_match=is_custom,
                is_casual=is_casual,
                rows=rows,
            )
        con.commit()
    except Exception:
        con.rollback()
        raise


def _snapshot_exists(con, week_start_utc_z: str, scope: str) -> bool:
    row = con.execute(
        """
        SELECT 1
          FROM weekly_snapshot_meta
         WHERE clan_id = ?
           AND platform = ?
           AND week_start_utc = ?
           AND scope = ?
         LIMIT 1
        """,
        (CLAN_ID_ALIAS, SHARD, week_start_utc_z, scope),
    ).fetchone()
    return row is not None


def _query_weekly_top10(con, week_start_utc_z: str, week_end_utc_z: str, scope: str) -> List[Tuple[str, int]]:
    scope = (scope or "total").lower()
    if scope == "normal":
        scope_clause = "AND COALESCE(m.is_ranked, 0) = 0"
    elif scope == "ranked":
        scope_clause = "AND COALESCE(m.is_ranked, 0) = 1"
    else:
        scope_clause = ""

    sql = f"""
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
      cm.clan_id = ?
      AND cm.platform = ?
      AND COALESCE(cm.is_active, 1) = 1
      AND m.created_at_utc >= ?
      AND m.created_at_utc <  ?
      AND m.game_mode IN ('solo','duo','squad','solo-fpp','duo-fpp','squad-fpp')
      AND COALESCE(m.is_casual, 0) = 0
      AND COALESCE(m.is_custom_match, 0) = 0
      {scope_clause}
    GROUP BY p.player_name
    ORDER BY kills DESC, p.player_name ASC
    LIMIT 10;
    """
    rows = con.execute(sql, (CLAN_ID_ALIAS, SHARD, week_start_utc_z, week_end_utc_z)).fetchall()
    return [(r[0], int(r[1])) for r in rows]


def _create_last_week_snapshots_if_missing(con) -> None:
    w = last_week_window_utc()
    week_start = w.start_utc_z
    week_end = w.end_utc_z

    for scope in SNAPSHOT_SCOPES:
        if _snapshot_exists(con, week_start, scope):
            continue

        top10 = _query_weekly_top10(con, week_start, week_end, scope)

        con.execute(
            """
            INSERT OR IGNORE INTO weekly_snapshot_meta
              (clan_id, platform, week_start_utc, week_end_utc, scope)
            VALUES (?, ?, ?, ?, ?)
            """,
            (CLAN_ID_ALIAS, SHARD, week_start, week_end, scope),
        )

        if top10:
            con.executemany(
                """
                INSERT OR REPLACE INTO weekly_snapshot_rows
                  (clan_id, platform, week_start_utc, scope, rank, player_name, kills)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (CLAN_ID_ALIAS, SHARD, week_start, scope, i, name, kills)
                    for i, (name, kills) in enumerate(top10, 1)
                ],
            )


async def _get_players_by_ids_safe(client: PubgApiClient, ids: List[str]) -> List[Dict[str, Any]]:
    """
    filter[playerIds] 배치 조회가 404로 터지는 케이스 방어:
    - 배치(<=10)가 404면 1개씩 쪼개서 가능한 것만 수집
    """
    ids = [i.strip() for i in (ids or []) if str(i).strip()]
    if not ids:
        return []

    # PubgApiClient에 get_players_by_ids가 없을 수도 있어 fallback
    async def _call(batch: List[str]) -> List[Dict[str, Any]]:
        if hasattr(client, "get_players_by_ids"):
            return await client.get_players_by_ids(batch)  # type: ignore
        data, _ = await client._get("/players", params={"filter[playerIds]": ",".join(batch)})
        return data.get("data") or []

    try:
        return await _call(ids)
    except PubgApiError as e:
        msg = str(e)
        if ("404" in msg or "Not Found" in msg) and len(ids) > 1:
            out: List[Dict[str, Any]] = []
            for one in ids:
                try:
                    got = await _call([one])
                    out.extend(got)
                except PubgApiError as e2:
                    print(f"[WARN] get_players_by_ids failed: {one} {e2}", flush=True)
            return out
        raise


async def main() -> None:
    if not API_KEY:
        raise SystemExit("PUBG_API_KEY is empty (.env에 PUBG_API_KEY 설정 필요)")
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    con = open_db_sync(str(DB_PATH), timeout_sec=BUSY_TIMEOUT_SEC)
    locked_by = f"{socket.gethostname()}:{os.getpid()}"

    try:
        _ensure_tables(con)

        acquired, locked_until = _try_acquire_job_lock(con, JOB_NAME, locked_by, JOB_LOCK_TTL_SEC)
        if not acquired:
            print(f"[SKIP] already running: job={JOB_NAME} locked_until={locked_until}", flush=True)
            return

        members = _get_active_clan_members(con)
        if not members:
            raise SystemExit("clan_members에 활성 멤버가 없음. 먼저 멤버 등록/동기화 필요.")

        clan_ids = {aid for (aid, _nm) in members}

        # 보관 정책: 지난주 시작(UTC)보다 오래된 매치는 삭제
        last_w = last_week_window_utc()
        keep_from_utc = last_w.start_utc_z

        # 1) playerIds로 10명씩 배치 조회해서 최근 match id 수집 (✅ 여기 수정)
        all_recent_match_ids: Set[str] = set()
        async with aiohttp.ClientSession() as session:
            client = PubgApiClient(API_KEY, SHARD, session, rpm=10, max_retries=3)

            ids = [aid for (aid, _nm) in members]
            for batch in _chunked(ids, 10):
                players = await _get_players_by_ids_safe(client, batch)
                for p in players:
                    rel = (p.get("relationships") or {}).get("matches") or {}
                    refs = rel.get("data") or []
                    for m in refs:
                        mid = m.get("id")
                        if mid:
                            all_recent_match_ids.add(mid)

            candidates = list(all_recent_match_ids)
            exist = _existing_match_ids(con, candidates)
            new_match_ids = [mid for mid in candidates if mid not in exist]

            inserted = 0
            skipped_old = 0
            pending: List[Tuple[str, str, str, int, int, int, List[Tuple[str, str, int]]]] = []

            # 2) 새 매치만 상세 조회(/matches) 후 DB 저장
            for mid in new_match_ids:
                try:
                    mj, _ = await client._get(f"/matches/{mid}")
                except PubgApiError as e:
                    print(f"[WARN] match fetch failed: {mid} {e}", flush=True)
                    continue

                data = mj.get("data") or {}
                attrs = data.get("attributes") or {}
                created_at_utc = (attrs.get("createdAt") or "").strip()
                game_mode = (attrs.get("gameMode") or "").strip().lower()

                if not created_at_utc:
                    continue

                if created_at_utc < keep_from_utc:
                    skipped_old += 1
                    continue

                if game_mode and game_mode not in ALLOWED_MODES:
                    continue

                is_ranked, is_custom_match, is_casual = _classify_match_flags(attrs, game_mode)
                rows = _extract_participant_kills(mj, clan_ids)
                if not rows:
                    continue

                pending.append((mid, created_at_utc, game_mode, is_ranked, is_custom_match, is_casual, rows))

                if len(pending) >= WRITE_BATCH_SIZE:
                    _flush_pending(con, pending)
                    inserted += len(pending)
                    pending.clear()

            if pending:
                _flush_pending(con, pending)
                inserted += len(pending)
                pending.clear()

        # 3) 오래된 매치 삭제(지난주 시작 이전)
        con.execute("BEGIN IMMEDIATE;")
        try:
            con.execute("DELETE FROM matches WHERE created_at_utc < ?", (keep_from_utc,))
            # 4) 지난주 스냅샷(없으면 생성)
            _create_last_week_snapshots_if_missing(con)
            con.commit()
        except Exception:
            con.rollback()
            raise

        await set_weekly_sync_last_utc_z(str(DB_PATH), _to_z(datetime.now(timezone.utc)))
        await set_weekly_sync_last_error(str(DB_PATH), "")

        if not all_recent_match_ids:
            print(f"[OK] no matches from players. keep_from={keep_from_utc}", flush=True)
        else:
            print(
                f"[OK] members={len(members)} recent_matches={len(all_recent_match_ids)} "
                f"new_matches={len(new_match_ids)} inserted={inserted} skipped_old={skipped_old} keep_from={keep_from_utc}",
                flush=True,
            )

    finally:
        try:
            _release_job_lock(con, JOB_NAME, locked_by)
        finally:
            con.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[STOP] cancelled by user", flush=True)
    except asyncio.CancelledError:
        print("[STOP] cancelled", flush=True)
    except SystemExit as e:
        msg = str(e).strip()
        if msg and DB_PATH.exists():
            try:
                asyncio.run(set_weekly_sync_last_error(str(DB_PATH), f"SystemExit: {msg}"))
            except Exception:
                pass
        raise
    except Exception as e:
        try:
            asyncio.run(set_weekly_sync_last_error(str(DB_PATH), f"{type(e).__name__}: {e}"))
        except Exception:
            pass
        raise
