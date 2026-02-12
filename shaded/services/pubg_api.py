import asyncio
import random
import time
from typing import Any, Dict, Optional, Tuple, List
import aiohttp

PUBG_BASE = "https://api.pubg.com"


class PubgApiError(Exception):
    pass


class _AsyncRateLimiter:
    """
    키 단위 Rate Limit(예: 10 RPM)을 '요청 시작 간격'으로 보장하는 간단한 리미터.
    rpm=10이면 평균 6초에 1회만 통과.
    """
    def __init__(self, rpm: int = 10):
        rpm = int(rpm) if rpm and int(rpm) > 0 else 10
        self._interval = 60.0 / float(rpm)
        self._lock = asyncio.Lock()
        self._next_ts = 0.0  # loop.time()

    async def wait(self) -> None:
        loop = asyncio.get_running_loop()
        async with self._lock:
            now = loop.time()
            if now < self._next_ts:
                delay = self._next_ts - now
                self._next_ts += self._interval
            else:
                delay = 0.0
                self._next_ts = now + self._interval
        if delay > 0:
            await asyncio.sleep(delay)


def _retry_delay(headers: aiohttp.typedefs.LooseHeaders) -> float:
    """
    429 대응:
    - Retry-After(초) 우선
    - X-RateLimit-Reset(epoch seconds) 있으면 reset까지 대기
    - 없으면 6초
    """
    try:
        ra = headers.get("Retry-After")
        if ra is not None:
            v = float(str(ra).strip())
            if v > 0:
                return min(v, 120.0)
    except Exception:
        pass

    try:
        reset = headers.get("X-RateLimit-Reset")
        if reset is not None:
            reset_ts = float(str(reset).strip())
            wait = reset_ts - time.time()
            if wait > 0:
                return min(wait, 120.0)
    except Exception:
        pass

    return 6.0


def _chunked(xs: List[str], n: int) -> List[List[str]]:
    return [xs[i:i + n] for i in range(0, len(xs), n)]


class PubgApiClient:
    """
    - 기본: rpm=10, max_retries=3
    - PUBG /players 필터는 playerIds/playerNames 모두 최대 10개 콤마-구분 지원
    """
    def __init__(
        self,
        api_key: str,
        shard: str,
        session: aiohttp.ClientSession,
        *,
        rpm: int = 10,
        max_retries: int = 3,
        limiter: Optional[_AsyncRateLimiter] = None,
    ):
        self.api_key = (api_key or "").strip()
        self.shard = shard
        self.session = session
        self.max_retries = int(max_retries) if max_retries is not None else 3
        self._limiter = limiter or _AsyncRateLimiter(rpm)
        self._season_cache: Tuple[Optional[str], float] = (None, 0.0)  # (season_id, ts)

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/vnd.api+json",
        }

    async def _get(
        self,
        path: str,
        params: Optional[Dict[str, str]] = None
    ) -> Tuple[Dict[str, Any], aiohttp.typedefs.LooseHeaders]:
        url = f"{PUBG_BASE}/shards/{self.shard}{path}"

        last_err: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            await self._limiter.wait()

            try:
                async with self.session.get(url, headers=self._headers(), params=params, timeout=aiohttp.ClientTimeout(total=25)) as resp:
                    data = await resp.json(content_type=None)

                    if resp.status == 429:
                        delay = _retry_delay(resp.headers) + random.uniform(0.1, 0.7)
                        if attempt < self.max_retries:
                            await asyncio.sleep(delay)
                            continue
                        remaining = resp.headers.get("X-RateLimit-Remaining", "")
                        reset = resp.headers.get("X-RateLimit-Reset", "")
                        raise PubgApiError(f"PUBG API rate limited (remaining={remaining}, reset={reset}, delay={delay:.1f}s)")

                    if resp.status in (500, 502, 503, 504):
                        if attempt < self.max_retries:
                            backoff = min(2.0 ** attempt, 20.0) + random.uniform(0.1, 0.9)
                            await asyncio.sleep(backoff)
                            continue
                        raise PubgApiError(f"PUBG API server error {resp.status}: {data}")

                    if resp.status >= 400:
                        raise PubgApiError(f"PUBG API error {resp.status}: {data}")

                    return data, resp.headers

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_err = e
                if attempt < self.max_retries:
                    backoff = min(2.0 ** attempt, 20.0) + random.uniform(0.1, 0.9)
                    await asyncio.sleep(backoff)
                    continue
                raise PubgApiError(f"PUBG API network error: {e}") from e

        raise PubgApiError(f"PUBG API failed: {last_err}")

    # -----------------------------
    # Players (C안: 10명 배치 조회)
    # -----------------------------
    async def get_players_by_names(self, player_names: List[str]) -> List[Dict[str, Any]]:
        names = [n.strip() for n in (player_names or []) if str(n).strip()]
        if not names:
            return []
        if len(names) > 10:
            raise ValueError("player_names must be <= 10 per request")
        data, _ = await self._get("/players", params={"filter[playerNames]": ",".join(names)})
        return data.get("data") or []

    async def get_players_by_ids(self, player_ids: List[str]) -> List[Dict[str, Any]]:
        ids = [i.strip() for i in (player_ids or []) if str(i).strip()]
        if not ids:
            return []
        if len(ids) > 10:
            raise ValueError("player_ids must be <= 10 per request")
        data, _ = await self._get("/players", params={"filter[playerIds]": ",".join(ids)})
        return data.get("data") or []

    async def get_player(self, player_name: str) -> Dict[str, Any]:
        items = await self.get_players_by_names([player_name])
        if not items:
            raise PubgApiError(f"플레이어를 찾지 못함: {player_name}")
        return items[0]

    async def get_player_id(self, player_name: str) -> str:
        p = await self.get_player(player_name)
        return p["id"]

    # -----------------------------
    # Seasons / Stats
    # -----------------------------
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
        data, _ = await self._get(f"/players/{player_id}/seasons/{season_id}/ranked")
        return data
