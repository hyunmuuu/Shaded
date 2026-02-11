import os
import sys
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
import requests

from print_week_window import week_window_utc, last_week_window_utc

DB_PATH = Path("db/shaded.db")
CLAN_ID = "shaded_steam"
SHARD = "steam"  # PUBG API shard
BASE = f"https://api.pubg.com/shards/{SHARD}"

ALLOWED_MODES = {"solo", "duo", "squad", "solo-fpp", "duo-fpp", "squad-fpp"}

def api_get(path: str) -> Dict[str, Any]:
    api_key = os.getenv("PUBG_API_KEY")
    if not api_key:
        raise SystemExit("PUBG_API_KEY 환경변수가 비어있음 (PowerShell에서 $env:PUBG_API_KEY=... 먼저)")

    url = path if path.startswith("http") else f"{BASE}{path}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/vnd.api+json",
    }
    r = requests.get(url, headers=headers, timeout=20)
    if r.status_code != 200:
        raise SystemExit(f"HTTP {r.status_code} {url}\n{r.text[:800]}")
    return r.json()

def upsert_member(con: sqlite3.Connection, account_id: str, player_name: str):
    cur = con.cursor()
    # players
    cur.execute(
        """
        INSERT INTO players (platform, account_id, player_name)
        VALUES (:platform, :account_id, :player_name)
        ON CONFLICT(platform, account_id) DO UPDATE SET
          player_name = excluded.player_name
        """,
        {"platform": SHARD, "account_id": account_id, "player_name": player_name},
    )
    # clan_members (지금은 테스트 목적이라 자동으로 넣음)
    cur.execute(
        """
        INSERT INTO clan_members (clan_id, platform, account_id)
        VALUES (:clan_id, :platform, :account_id)
        ON CONFLICT(clan_id, platform, account_id) DO NOTHING
        """,
        {"clan_id": CLAN_ID, "platform": SHARD, "account_id": account_id},
    )

def find_player(player_name: str) -> Tuple[str, str, list[str]]:
    data = api_get(f"/players?filter[playerNames]={player_name}")
    items = data.get("data") or []
    if not items:
        raise SystemExit(f"플레이어를 찾지 못함: {player_name}")

    p = items[0]
    account_id = p.get("id")
    name = (p.get("attributes") or {}).get("name") or player_name

    rel = (p.get("relationships") or {}).get("matches") or {}
    match_refs = (rel.get("data") or [])
    match_ids = [m.get("id") for m in match_refs if m.get("id")]

    return account_id, name, match_ids

def extract_kills_from_match(match_json: Dict[str, Any], account_id: str, player_name: str) -> Optional[int]:
    included = match_json.get("included") or []
    for obj in included:
        if obj.get("type") != "participant":
            continue
        stats = ((obj.get("attributes") or {}).get("stats") or {})
        pid = stats.get("playerId")
        nm = stats.get("name")
        if pid == account_id or nm == player_name:
            k = stats.get("kills")
            return int(k) if k is not None else 0
    return None

def upsert_match_rows(con: sqlite3.Connection, match_id: str, created_at_utc: str, game_mode: str, account_id: str, kills: int):
    cur = con.cursor()

    # matches: 네 스키마에 맞춰 is_custom_match 컬럼 없이 저장
    cur.execute(
        """
        INSERT INTO matches (match_id, platform, created_at_utc, game_mode)
        VALUES (:match_id, :platform, :created_at_utc, :game_mode)
        ON CONFLICT(match_id) DO UPDATE SET
          platform       = excluded.platform,
          created_at_utc = excluded.created_at_utc,
          game_mode      = excluded.game_mode
        """,
        {
            "match_id": match_id,
            "platform": SHARD,
            "created_at_utc": created_at_utc,
            "game_mode": game_mode,
        },
    )

    # player_matches: PRIMARY KEY (match_id, platform, account_id) 기준으로 UPSERT
    cur.execute(
        """
        INSERT INTO player_matches (match_id, platform, account_id, kills)
        VALUES (:match_id, :platform, :account_id, :kills)
        ON CONFLICT(match_id, platform, account_id) DO UPDATE SET
          kills = excluded.kills
        """,
        {
            "match_id": match_id,
            "platform": SHARD,
            "account_id": account_id,
            "kills": kills,
        },
    )


def purge_before(con: sqlite3.Connection, cutoff_utc: str):
    cur = con.cursor()
    cur.execute(
        """
        DELETE FROM player_matches
        WHERE match_id IN (SELECT match_id FROM matches WHERE created_at_utc < :cutoff)
        """,
        {"cutoff": cutoff_utc},
    )
    cur.execute(
        "DELETE FROM matches WHERE created_at_utc < :cutoff",
        {"cutoff": cutoff_utc},
    )


def main():
    if len(sys.argv) < 2:
        raise SystemExit('사용법: python tools/ingest_one_player.py "PUBG닉네임"')

    target_name = sys.argv[1]

    # 지난주 시작~이번주 끝까지만 DB에 남기기(= 지난주 랭킹 + 이번주 랭킹 가능)
    last_s, _last_e = last_week_window_utc()
    this_s, this_e = week_window_utc()

    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA foreign_keys=ON;")

    # 1) 플레이어 계정ID + 최근 매치ID 리스트
    account_id, player_name, match_ids = find_player(target_name)

    # 2) 멤버 upsert
    upsert_member(con, account_id, player_name)

    # 3) 매치 적재 (matches 엔드포인트는 rate limit에 포함되지 않음) :contentReference[oaicite:1]{index=1}
    inserted = 0
    skipped = 0

    for mid in match_ids:
        mj = api_get(f"/matches/{mid}")
        attrs = (mj.get("data") or {}).get("attributes") or {}

        created_at = attrs.get("createdAt")
        game_mode = attrs.get("gameMode")

        # 커스텀 매치는 isCustomMatch=true 로 구분됨 :contentReference[oaicite:2]{index=2}
        is_custom = 1 if attrs.get("isCustomMatch") is True else 0
        if is_custom:
            skipped += 1
            continue

        if not created_at or not game_mode:
            skipped += 1
            continue

        if game_mode not in ALLOWED_MODES:
            skipped += 1
            continue

        # 기간 필터(문자열 비교 가능: ...Z ISO8601)
        if not (last_s <= created_at < this_e):
            skipped += 1
            continue

        kills = extract_kills_from_match(mj, account_id, player_name)
        if kills is None:
            skipped += 1
            continue

        upsert_match_rows(con, mid, created_at, game_mode, account_id, kills)
        inserted += 1

    # 4) 오래된 매치 정리(지난주 시작 이전 삭제)
    purge_before(con, last_s)

    con.commit()
    con.close()

    print(f"player = {player_name} ({account_id})")
    print(f"window = {last_s} ~ {this_e} (UTC)")
    print(f"inserted = {inserted}, skipped = {skipped}")

if __name__ == "__main__":
    main()
