from dataclasses import dataclass
from typing import Any, Dict, Optional
import aiohttp

from shaded.services.pubg_api import PubgApiClient, PubgApiError

def _safe_div(a: float, b: float) -> float:
    return a / b if b else 0.0

def _tier_str(t) -> str:
    if isinstance(t, dict):
        tier = str(t.get("tier") or "").strip()
        sub = str(t.get("subTier") or "").strip()
        if tier and sub:
            return f"{tier} {sub}"
        return tier or sub or "-"
    if isinstance(t, str):
        return t.strip() or "-"
    return "-"

def season_label(season_id: str) -> str:
    last = season_id.split("-")[-1]
    if last.isdigit():
        return f"PC 시즌 {last}"
    return season_id

def mode_key(base_mode: str, view: str) -> str:
    return base_mode if view == "tpp" else f"{base_mode}-fpp"

@dataclass(frozen=True)
class NormalStats:
    nickname: str
    season_label: str
    title: str

    kd: float
    win_rate: float
    top10_rate: float
    adr: float
    rounds: int
    yopo: int
    hs_rate: float
    longest_kill: float
    survival_txt: str

@dataclass(frozen=True)
class RankedStats:
    nickname: str
    season_label: str
    title: str

    tier: str
    rp: int
    best_rp: int
    best_tier: str

    kd: float
    win_rate: float
    top10_rate: float
    adr: float
    rounds: int

class PubgStatsService:
    def __init__(self, api_key: str, shard: str):
        self.api_key = api_key
        self.shard = shard

    async def fetch_normal(self, nickname: str, base_mode: str, view: str) -> NormalStats:
        async with aiohttp.ClientSession() as session:
            client = PubgApiClient(self.api_key, self.shard, session)

            player_id = await client.get_player_id(nickname)
            season_id = await client.get_current_season_id()
            payload = await client.get_season_stats(player_id, season_id)

            attrs = payload.get("data", {}).get("attributes", {})
            gm = (attrs.get("gameModeStats") or {}).get(mode_key(base_mode, view))
            if not gm:
                raise PubgApiError("전적 데이터가 없음(현재 시즌/모드)")

            rounds = int(gm.get("roundsPlayed", 0))
            if rounds == 0:
                raise PubgApiError("플레이 기록이 없음(현재 시즌/모드)")

            wins = int(gm.get("wins", 0))
            top10 = int(gm.get("top10s", 0))
            kills = int(gm.get("kills", 0))
            dmg = float(gm.get("damageDealt", 0.0))
            losses = int(gm.get("losses", 0))

            headshot_kills = int(gm.get("headshotKills", 0))
            longest_kill = float(gm.get("longestKill", 0.0))
            time_survived = float(gm.get("timeSurvived", 0.0))
            yopo = int(float(gm.get("roundMostKills") or 0))

            win_rate = _safe_div(wins * 100.0, rounds)
            top10_rate = _safe_div(top10 * 100.0, rounds)
            kd = _safe_div(kills, max(losses, 1))
            adr = _safe_div(dmg, rounds)

            hs_rate = _safe_div(headshot_kills * 100.0, kills)
            avg_survival_sec = _safe_div(time_survived, rounds)
            mm = int(avg_survival_sec // 60)
            ss = int(avg_survival_sec % 60)
            survival_txt = f"{mm}m {ss:02d}s" if avg_survival_sec > 0 else "-"

            title = f"일반 {base_mode.upper()} {view.upper()}".replace("SOLO", "솔로").replace("DUO", "듀오").replace("SQUAD", "스쿼드")

            return NormalStats(
                nickname=nickname,
                season_label=season_label(season_id),
                title=title,
                kd=kd,
                win_rate=win_rate,
                top10_rate=top10_rate,
                adr=adr,
                rounds=rounds,
                yopo=yopo,
                hs_rate=hs_rate,
                longest_kill=longest_kill,
                survival_txt=survival_txt,
            )

    async def fetch_ranked(self, nickname: str, base_mode: str, view: str) -> RankedStats:
        async with aiohttp.ClientSession() as session:
            client = PubgApiClient(self.api_key, self.shard, session)

            player_id = await client.get_player_id(nickname)
            season_id = await client.get_current_season_id()
            payload = await client.get_ranked_stats(player_id, season_id)

            attrs = payload.get("data", {}).get("attributes", {})
            ranked_map = attrs.get("rankedGameModeStats") or {}
            gm = ranked_map.get(mode_key(base_mode, view))

            # FPP 없고 TPP만 있을 때 메시지용 에러
            if not gm and view == "fpp" and ranked_map.get(mode_key(base_mode, "tpp")):
                raise PubgApiError("FPP 데이터가 없음 → TPP로 선택해서 조회")

            if not gm:
                raise PubgApiError("전적 데이터가 없음(현재 시즌/모드)")

            tier = _tier_str(gm.get("currentTier"))
            best_tier = _tier_str(gm.get("bestTier"))
            rp = int(gm.get("currentRankPoint") or 0)
            best_rp = int(gm.get("bestRankPoint") or 0)

            rounds = int(gm.get("roundsPlayed", 0))
            wins = int(gm.get("wins", 0))
            top10 = int(gm.get("top10s", 0))

            kills = int(gm.get("kills", 0))
            deaths = int(gm.get("deaths", 0) or gm.get("losses", 0))
            dmg = float(gm.get("damageDealt", 0.0))

            win_rate = _safe_div(wins * 100.0, rounds)
            top10_rate = _safe_div(top10 * 100.0, rounds)
            kd = _safe_div(kills, max(deaths, 1))
            adr = _safe_div(dmg, rounds)

            title = f"경쟁 {base_mode.upper()} {view.upper()}".replace("SOLO", "솔로").replace("DUO", "듀오").replace("SQUAD", "스쿼드")

            return RankedStats(
                nickname=nickname,
                season_label=season_label(season_id),
                title=title,
                tier=tier,
                rp=rp,
                best_rp=best_rp,
                best_tier=best_tier,
                kd=kd,
                win_rate=win_rate,
                top10_rate=top10_rate,
                adr=adr,
                rounds=rounds,
            )
