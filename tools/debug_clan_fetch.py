import os
import requests

SHARD = "steam"
BASE_URL = f"https://api.pubg.com/shards/{SHARD}"

API_KEY = os.getenv("PUBG_API_KEY")
if not API_KEY:
    raise SystemExit("PUBG_API_KEY 환경변수 먼저 설정 필요")

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/vnd.api+json",
}

def api_get(path: str, params=None):
    url = f"{BASE_URL}{path}"
    r = requests.get(url, headers=HEADERS, params=params, timeout=20)
    if r.status_code != 200:
        raise SystemExit(f"HTTP {r.status_code} {url}\n{r.text[:800]}")
    return r.json()

def main(seed_name: str):
    # 1) seed -> clanId
    p = api_get("/players", params={"filter[playerNames]": seed_name}).get("data", [])
    if not p:
        raise SystemExit(f"플레이어 검색 실패: {seed_name}")
    p0 = p[0]
    clan_id = (p0.get("attributes") or {}).get("clanId")
    print("seed_player =", (p0.get("attributes") or {}).get("name"))
    print("clanId      =", clan_id)
    if not clan_id:
        raise SystemExit("seed 플레이어에 clanId가 없음(클랜 미소속 또는 shard 불일치)")

    # 2) clan -> members
    cj = api_get(f"/clans/{clan_id}", params={"include": "members"})
    cdata = cj.get("data") or {}
    cattr = cdata.get("attributes") or {}
    print("clan_name   =", cattr.get("name"))
    print("clan_tag    =", cattr.get("tag"))

    rel = (cdata.get("relationships") or {}).get("members") or {}
    members_data = rel.get("data") or []
    print("members(rel).count =", len(members_data))

    # included에 player가 얼마나 들어왔는지
    included = cj.get("included") or []
    included_players = [x for x in included if x.get("type") == "player"]
    print("included player count =", len(included_players))

    # 멤버 이름 일부 출력(최대 10)
    names = []
    for obj in included_players[:10]:
        names.append((obj.get("attributes") or {}).get("name"))
    print("sample member names =", names)

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        raise SystemExit('Usage: python tools/debug_clan_fetch.py "seed_player_name"')
    main(sys.argv[1])
