import os
import sqlite3
from pathlib import Path
import requests

DB_PATH = Path("db/shaded.db")

SHARD = "steam"
BASE_URL = f"https://api.pubg.com/shards/{SHARD}"
CLAN_ID_ALIAS = "shaded_steam"

API_KEY = os.getenv("PUBG_API_KEY")
if not API_KEY:
    raise SystemExit("PUBG_API_KEY 환경변수 먼저 설정 필요")

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/vnd.api+json",
}

def chunked(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]

def api_get(path: str, params=None):
    url = path if path.startswith("http") else f"{BASE_URL}{path}"
    r = requests.get(url, headers=HEADERS, params=params, timeout=20)
    if r.status_code != 200:
        raise SystemExit(f"HTTP {r.status_code} {url}\n{r.text[:800]}")
    return r.json()

def get_player_by_name(player_name: str) -> dict:
    data = api_get("/players", params={"filter[playerNames]": player_name})
    items = data.get("data") or []
    if not items:
        raise SystemExit(f"플레이어 검색 실패: {player_name}")
    return items[0]

def get_players_by_ids(player_ids: list[str]) -> dict[str, str]:
    # return {account_id: name}
    name_map = {}
    for batch in chunked(player_ids, 10):
        data = api_get("/players", params={"filter[playerIds]": ",".join(batch)})
        for p in data.get("data") or []:
            pid = p.get("id")
            nm = (p.get("attributes") or {}).get("name")
            if pid and nm:
                name_map[pid] = nm
    return name_map

def get_clan(clan_id: str) -> dict:
    # include=members로 included에 player 리소스가 들어오도록 유도
    return api_get(f"/clans/{clan_id}", params={"include": "members"})

def extract_member_ids(clan_json: dict) -> list[str]:
    data = clan_json.get("data") or {}
    rel = data.get("relationships") or {}
    members_rel = (rel.get("members") or {}).get("data") or []
    ids = [m.get("id") for m in members_rel if m.get("id")]
    return ids

def extract_included_name_map(clan_json: dict) -> dict[str, str]:
    name_map = {}
    for obj in clan_json.get("included") or []:
        if obj.get("type") == "player":
            pid = obj.get("id")
            nm = (obj.get("attributes") or {}).get("name")
            if pid and nm:
                name_map[pid] = nm
    return name_map

def upsert_players(con: sqlite3.Connection, items: list[tuple[str, str]]):
    # items: [(account_id, player_name), ...]
    cur = con.cursor()
    cur.executemany(
        """
        INSERT INTO players (platform, account_id, player_name)
        VALUES (?, ?, ?)
        ON CONFLICT(platform, account_id) DO UPDATE SET
          player_name = excluded.player_name
        """,
        [(SHARD, aid, nm) for (aid, nm) in items],
    )

def upsert_clan_members(con: sqlite3.Connection, member_ids: list[str]):
    cur = con.cursor()
    # joined_at는 기본값 유지, left_at은 사용 안 함(너 정책: 탈퇴자는 삭제)
    cur.executemany(
        """
        INSERT INTO clan_members (clan_id, platform, account_id, clan_role, is_active)
        VALUES (?, ?, ?, ?, 1)
        ON CONFLICT(clan_id, platform, account_id) DO UPDATE SET
          clan_role = excluded.clan_role,
          is_active = 1,
          left_at = NULL
        """,
        [(CLAN_ID_ALIAS, SHARD, aid, "member") for aid in member_ids],
    )

def delete_left_members(con: sqlite3.Connection, member_ids: list[str]):
    cur = con.cursor()
    if not member_ids:
        # 안전: 멤버 0명일 때 전부 삭제하지 않음
        return
    ph = ",".join(["?"] * len(member_ids))
    cur.execute(
        f"""
        DELETE FROM clan_members
         WHERE clan_id = ?
           AND platform = ?
           AND account_id NOT IN ({ph})
        """,
        [CLAN_ID_ALIAS, SHARD, *member_ids],
    )

def main(seed_name: str):
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    # 1) seed -> clanId
    p = get_player_by_name(seed_name)
    clan_id = (p.get("attributes") or {}).get("clanId")
    if not clan_id:
        raise SystemExit(f"seed '{seed_name}'에 clanId가 없음 (클랜 미소속/샤드 불일치 가능)")

    # 2) clan -> member ids
    clan_json = get_clan(clan_id)
    member_ids = extract_member_ids(clan_json)
    if not member_ids:
        raise SystemExit("clan members 목록이 비어있음(응답 구조 확인 필요)")

    # 3) id -> name
    name_map = extract_included_name_map(clan_json)
    missing = [aid for aid in member_ids if aid not in name_map]
    if missing:
        name_map.update(get_players_by_ids(missing))

    players_items = [(aid, name_map.get(aid) or aid) for aid in member_ids]

    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA foreign_keys=ON;")
    con.execute("PRAGMA journal_mode=WAL;")
    try:
        con.execute("BEGIN;")
        upsert_players(con, players_items)
        upsert_clan_members(con, member_ids)
        delete_left_members(con, member_ids)
        con.commit()
    finally:
        con.close()

    print(f"[OK] clanId={clan_id} synced members={len(member_ids)}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        raise SystemExit('Usage: python tools/sync_clan_members.py "seed_player_name"')
    main(sys.argv[1])
