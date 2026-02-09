import time
from typing import Any, Dict, Optional, Tuple
import aiohttp

PUBG_BASE = "https://api.pubg.com"

class PubgApiError(Exception):
    pass

class PubgApiClient:
    def __init__(self, api_key: str, shard: str, session: aiohttp.ClientSession):
        self.api_key = api_key
        self.shard = shard
        self.session = session
        self._season_cache: Tuple[Optional[str], float] = (None, 0.0)  # (season_id, ts)

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/vnd.api+json",
        }

    async def _get(self, path: str, params: Optional[Dict[str, str]] = None) -> Tuple[Dict[str, Any], aiohttp.typedefs.LooseHeaders]:
        url = f"{PUBG_BASE}/shards/{self.shard}{path}"
        async with self.session.get(url, headers=self._headers(), params=params) as resp:
            data = await resp.json(content_type=None)

            if resp.status == 429:
                reset = resp.headers.get("X-RateLimit-Reset", "")
                remaining = resp.headers.get("X-RateLimit-Remaining", "")
                raise PubgApiError(f"PUBG API rate limited (remaining={remaining}, reset={reset})")

            if resp.status >= 400:
                raise PubgApiError(f"PUBG API error {resp.status}: {data}")

            return data, resp.headers

    async def get_player_id(self, player_name: str) -> str:
        data, _ = await self._get("/players", params={"filter[playerNames]": player_name})
        items = data.get("data") or []
        if not items:
            raise PubgApiError(f"플레이어를 찾지 못함: {player_name}")
        return items[0]["id"]

    async def get_current_season_id(self) -> str:
        cached, ts = self._season_cache
        if cached and (time.time() - ts) < 60 * 60 * 24 * 7:  # 7일 캐시
            return cached

        data, _ = await self._get("/seasons")
        for s in data.get("data", []):
            attrs = s.get("attributes") or {}
            if attrs.get("isCurrentSeason") is True:
                season_id = s["id"]
                self._season_cache = (season_id, time.time())
                return season_id

        raise PubgApiError("현재 시즌을 찾지 못함")

    async def get_season_stats(self, player_id: str, season_id: str) -> Dict[str, Any]:
        data, _ = await self._get(f"/players/{player_id}/seasons/{season_id}")
        return data

    async def get_ranked_stats(self, player_id: str, season_id: str) -> Dict[str, Any]:
        # /players/{playerId}/seasons/{seasonId}/ranked
        data, _ = await self._get(f"/players/{player_id}/seasons/{season_id}/ranked")
        return data
