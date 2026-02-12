# tools/sync_weekly_kills.py
import os
import asyncio
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple
from datetime import datetime, timezone

import aiohttp

from shaded.services.pubg_api import PubgApiClient, PubgApiError
from shaded.utils.time_window import last_week_window_utc
from shaded.services.sync_state import set_weekly_sync_last_utc_z
from shaded.services.clan_store import CLAN_ID_ALIAS


def _load_env_fallback(env_path: str = ".env", override: bool = True) -> None:
    """
    python-dotenv가 없을 때를 대비한 초간단 .env 로더.
    - KEY=VALUE 형태만 지원
    - # 주석/빈줄 무시
    - "..." / '...' 따옴표 제거
    """
    p = Path(env_path)
    if not p.exists():
        return

    for raw in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.lower().startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue

        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip()

        if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
            v = v[1:-1]

        if not k:
            continue

        if override or (k not in os.environ):
            os.environ[k] = v


def load_env() -> None:
    """
    python-dotenv가 설치되어 있으면 그걸 사용,
    없으면 fallback 로더로 .env를 읽음.
    """
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(override=True)
        return
    except Exception:
        _load_env_fallback(".env", override=True)


# 집계 대상 6모드(솔/듀/스쿼드 + FPP 3개)
ALLOWED_MODES = {"solo", "duo", "squad", "solo-fpp", "duo-fpp", "squad-fpp"}


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


def _ensure_tables(con: sqlite3.Connection) -> None:
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


def _get_active_clan_members(con: sqlite3.Connection, shard: str) -> List[Tuple[str, str]]:
    rows = con.execute(
        """
        SELECT cm.account_id, p.player_name
          FROM clan_members cm
          JOIN players p
            ON p.platform = cm.platform AND p.account_id = cm.account_id
         WHERE cm.clan_id = ?
           AND cm.platform = ?
           AND COALESCE(cm.is_active, 1) = 1
        """,
        (CLAN_ID_ALIAS, shard),
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def _existing_match_ids(con: sqlite3.Connection, match_ids: List[str]) -> Set[str]:
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
    con: sqlite3.Connection,
    shard: str,
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
        (match_id, shard, created_at_utc, game_mode, is_ranked, is_custom_match, is_casual),
    )

    for account_id, player_name, _kills in rows:
        con.execute(
            """
            UPDATE players
               SET player_name = ?
             WHERE platform = ? AND account_id = ?
            """,
            (player_name, shard, account_id),
        )

    con.executemany(
        """
        INSERT OR REPLACE INTO player_matches (match_id, platform, account_id, kills)
        VALUES (?, ?, ?, ?)
        """,
        [(match_id, shard, account_id, kills) for (account_id, _nm, kills) in rows],
    )


async def main():
    load_env()

    db_path = Path(os.getenv("DB_PATH", "db/shaded.db"))
    shard = (os.getenv("PUBG_SHARD", "steam").strip() or "steam")
    api_key = (os.getenv("PUBG_API_KEY", "") or "").strip().removeprefix("Bearer ").strip()

    if not api_key:
        raise SystemExit("PUBG_API_KEY 환경변수가 비어있음 (.env 또는 환경변수 설정 필요)")
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys=ON;")
    con.execute("PRAGMA journal_mode=WAL;")
    try:
        _ensure_tables(con)

        members = _get_active_clan_members(con, shard)
        if not members:
            raise SystemExit("clan_members에 활성 멤버가 없음. 먼저 멤버 등록/동기화 필요.")

        clan_ids = {aid for (aid, _nm) in members}

        # 보관 정책: 지난주 시작(UTC)보다 오래된 매치는 삭제
        last_w = last_week_window_utc()
        keep_from_utc = last_w.start_utc_z

        # 1) 플레이어 10명씩 배치로 최근 match 목록 수집 (C안)
        all_recent_match_ids: Set[str] = set()

        async with aiohttp.ClientSession() as session:
            client = PubgApiClient(api_key, shard, session, rpm=10, max_retries=3)

            ids = [aid for (aid, _nm) in members]
            for batch in _chunked(ids, 10):
                players = await client.get_players_by_ids(batch)
                for p in players:
                    rel = (p.get("relationships") or {}).get("matches") or {}
                    refs = rel.get("data") or []
                    for m in refs:
                        mid = m.get("id")
                        if mid:
                            all_recent_match_ids.add(mid)

        # 2) 이미 DB에 있는 매치는 제외
        candidates = list(all_recent_match_ids)
        exist = _existing_match_ids(con, candidates)
        new_match_ids = [mid for mid in candidates if mid not in exist]

        # 3) 새 매치만 상세(/matches) 조회 후 저장
        inserted = 0
        skipped_old = 0

        async with aiohttp.ClientSession() as session:
            match_client = PubgApiClient(api_key, shard, session, rpm=600, max_retries=2)

            con.execute("BEGIN;")
            for mid in new_match_ids:
                try:
                    mj, _ = await match_client._get(f"/matches/{mid}")
                except PubgApiError as e:
                    print(f"[WARN] match fetch failed: {mid} {e}")
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

                _insert_match_and_kills(
                    con, shard,
                    match_id=mid,
                    created_at_utc=created_at_utc,
                    game_mode=game_mode,
                    is_ranked=is_ranked,
                    is_custom_match=is_custom_match,
                    is_casual=is_casual,
                    rows=rows,
                )
                inserted += 1

            con.execute("DELETE FROM matches WHERE created_at_utc < ?", (keep_from_utc,))
            con.commit()

        # 4) 마지막 갱신 시각 저장
        await set_weekly_sync_last_utc_z(str(db_path), _to_z(datetime.now(timezone.utc)))

        print(f"[OK] members={len(members)} new_matches={len(new_match_ids)} inserted={inserted} skipped_old={skipped_old} keep_from={keep_from_utc}")

    finally:
        con.close()


if __name__ == "__main__":
    asyncio.run(main())
