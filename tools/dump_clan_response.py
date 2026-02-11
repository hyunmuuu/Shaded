import os, json, sys
import requests
from pathlib import Path

SHARD = "steam"
BASE_URL = f"https://api.pubg.com/shards/{SHARD}"
OUT = Path("tools/_debug")
OUT.mkdir(parents=True, exist_ok=True)

API_KEY = os.getenv("PUBG_API_KEY")
if not API_KEY:
    raise SystemExit("PUBG_API_KEY 환경변수 먼저 설정 필요")

HEADERS = {"Authorization": f"Bearer {API_KEY}", "Accept": "application/vnd.api+json"}

def api_get(path: str, params=None):
    r = requests.get(f"{BASE_URL}{path}", headers=HEADERS, params=params, timeout=20)
    print("HTTP", r.status_code, r.url)
    if r.status_code != 200:
        print(r.text[:1200])
        raise SystemExit("request failed")
    return r.json()

def main(seed_name: str):
    # player -> clanId
    pj = api_get("/players", params={"filter[playerNames]": seed_name})
    p0 = (pj.get("data") or [None])[0]
    if not p0:
        raise SystemExit("player not found")
    clan_id = (p0.get("attributes") or {}).get("clanId")
    print("seed_player =", (p0.get("attributes") or {}).get("name"))
    print("clanId      =", clan_id)
    if not clan_id:
        raise SystemExit("seed player has no clanId")

    # clan raw
    cj = api_get(f"/clans/{clan_id}", params={"include": "members"})
    (OUT / "clan.json").write_text(json.dumps(cj, ensure_ascii=False, indent=2), encoding="utf-8")
    print("saved -> tools/_debug/clan.json")

    data = cj.get("data") or {}
    attrs = data.get("attributes") or {}
    rels = data.get("relationships") or {}

    print("data.type =", data.get("type"))
    print("attr.keys =", sorted(list(attrs.keys()))[:50])
    print("rel.keys  =", sorted(list(rels.keys()))[:50])

    # relationships 안에 player id가 어디에 붙는지 샘플 출력
    for k, v in rels.items():
        d = (v or {}).get("data")
        if isinstance(d, list) and d:
            sample = [(x.get("type"), x.get("id")) for x in d[:5]]
            print(f"rel[{k}].sample =", sample)

    inc = cj.get("included") or []
    types = {}
    for o in inc:
        types[o.get("type")] = types.get(o.get("type"), 0) + 1
    print("included types =", types)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit('Usage: python tools/dump_clan_response.py "seed_player_name"')
    main(sys.argv[1])
