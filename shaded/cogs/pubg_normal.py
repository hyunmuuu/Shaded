import discord
from discord import app_commands
from discord.ext import commands
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

# 드롭다운 선택지
KIND_CHOICES = [
    app_commands.Choice(name="일반", value="normal"),
    app_commands.Choice(name="경쟁", value="ranked"),
]

MODE_CHOICES = [
    app_commands.Choice(name="솔로", value="solo"),
    app_commands.Choice(name="듀오", value="duo"),
    app_commands.Choice(name="스쿼드", value="squad"),
]

VIEW_CHOICES = [
    app_commands.Choice(name="TPP", value="tpp"),
    app_commands.Choice(name="FPP", value="fpp"),
]

MODE_KO = {"solo": "솔로", "duo": "듀오", "squad": "스쿼드"}

class PubgStatsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    def _season_label(self, season_id: str) -> str:
        last = season_id.split("-")[-1]
        if last.isdigit():
            return f"PC 시즌 {last}"
        return season_id

    def _mode_key(self, base_mode: str, view: str) -> str:
        return base_mode if view == "tpp" else f"{base_mode}-fpp"

    async def _send_normal(self, interaction: discord.Interaction, nickname: str, base_mode: str, view: str):
        settings = getattr(self.bot, "settings", None)
        if not settings or not settings.pubg_api_key:
            await interaction.response.send_message("PUBG_API_KEY가 .env에 없음", ephemeral=True)
            return

        await interaction.response.defer(thinking=True)

        async with aiohttp.ClientSession() as session:
            client = PubgApiClient(settings.pubg_api_key, settings.pubg_shard, session)

            try:
                player_id = await client.get_player_id(nickname)
                season_id = await client.get_current_season_id()
                payload = await client.get_season_stats(player_id, season_id)

                season_label = self._season_label(season_id)
                mode_key = self._mode_key(base_mode, view)
                mode_title = f"일반 {MODE_KO.get(base_mode, base_mode)} {view.upper()}"

                attrs = payload.get("data", {}).get("attributes", {})
                gm = (attrs.get("gameModeStats") or {}).get(mode_key)

                if not gm:
                    await interaction.followup.send(f"`{nickname}`: {mode_title} 전적 데이터가 없음(현재 시즌)")
                    return

                rounds = int(gm.get("roundsPlayed", 0))
                if rounds == 0:
                    await interaction.followup.send(f"`{nickname}`: 현재 시즌 {mode_title} 플레이 기록이 없음")
                    return

                wins = int(gm.get("wins", 0))
                top10 = int(gm.get("top10s", 0))
                kills = int(gm.get("kills", 0))
                dmg = float(gm.get("damageDealt", 0.0))
                losses = int(gm.get("losses", 0))

                headshot_kills = int(gm.get("headshotKills", 0))
                longest_kill = float(gm.get("longestKill", 0.0))
                time_survived = float(gm.get("timeSurvived", 0.0))
                yopo = int(float(gm.get("roundMostKills") or 0))  # 여포(한 판 최다킬)

                win_rate = _safe_div(wins * 100.0, rounds)
                top10_rate = _safe_div(top10 * 100.0, rounds)
                kd = _safe_div(kills, max(losses, 1))
                adr = _safe_div(dmg, rounds)

                hs_rate = _safe_div(headshot_kills * 100.0, kills)
                avg_survival_sec = _safe_div(time_survived, rounds)
                mm = int(avg_survival_sec // 60)
                ss = int(avg_survival_sec % 60)
                survival_txt = f"{mm}m {ss:02d}s" if avg_survival_sec > 0 else "-"

                em = discord.Embed(title=f"Shaded 전적 | {mode_title} (Steam)")
                em.add_field(name="닉네임", value=nickname, inline=False)
                em.add_field(name="시즌", value=season_label, inline=False)

                em.add_field(name="K/D", value=f"{kd:.2f}", inline=True)
                em.add_field(name="승률", value=f"{win_rate:.1f}%", inline=True)
                em.add_field(name="Top10", value=f"{top10_rate:.1f}%", inline=True)

                em.add_field(name="평균 딜량", value=f"{adr:.1f}", inline=True)
                em.add_field(name="게임수", value=str(rounds), inline=True)
                em.add_field(name="여포", value=str(yopo), inline=True)

                em.add_field(name="헤드샷", value=f"{hs_rate:.1f}%", inline=True)
                em.add_field(name="저격", value=f"{longest_kill:.1f}m" if longest_kill > 0 else "-", inline=True)
                em.add_field(name="생존", value=survival_txt, inline=True)

                await interaction.followup.send(embed=em)

            except PubgApiError as e:
                await interaction.followup.send(f"전적 조회 실패: {e}")

    async def _send_ranked(self, interaction: discord.Interaction, nickname: str, base_mode: str, view: str):
        settings = getattr(self.bot, "settings", None)
        if not settings or not settings.pubg_api_key:
            await interaction.response.send_message("PUBG_API_KEY가 .env에 없음", ephemeral=True)
            return

        await interaction.response.defer(thinking=True)

        async with aiohttp.ClientSession() as session:
            client = PubgApiClient(settings.pubg_api_key, settings.pubg_shard, session)

            try:
                player_id = await client.get_player_id(nickname)
                season_id = await client.get_current_season_id()
                payload = await client.get_ranked_stats(player_id, season_id)

                season_label = self._season_label(season_id)
                attrs = payload.get("data", {}).get("attributes", {})
                ranked_map = attrs.get("rankedGameModeStats") or {}

                mode_key = self._mode_key(base_mode, view)
                gm = ranked_map.get(mode_key)

                # FPP가 없고 TPP만 있는 경우 안내
                if not gm and view == "fpp":
                    alt = ranked_map.get(self._mode_key(base_mode, "tpp"))
                    if alt:
                        await interaction.followup.send(
                            f"`{nickname}`: 경쟁 {MODE_KO.get(base_mode, base_mode)} FPP 데이터가 없음 → TPP로 선택해서 조회해줘"
                        )
                        return

                if not gm:
                    keys = list(ranked_map.keys())
                    show = ", ".join(keys[:10]) if keys else "(없음)"
                    await interaction.followup.send(
                        f"`{nickname}`: 경쟁 {MODE_KO.get(base_mode, base_mode)} {view.upper()} 전적 데이터가 없음(현재 시즌)\n"
                        f"가능 모드 키: {show}"
                    )
                    return

                current_tier = _tier_str(gm.get("currentTier"))
                best_tier = _tier_str(gm.get("bestTier"))
                current_rp = int(gm.get("currentRankPoint") or 0)
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

                mode_title = f"경쟁 {MODE_KO.get(base_mode, base_mode)} {view.upper()}"

                em = discord.Embed(title=f"Shaded 전적 | {mode_title} (Steam)")
                em.add_field(name="닉네임", value=nickname, inline=False)
                em.add_field(name="시즌", value=season_label, inline=False)

                em.add_field(name="티어", value=current_tier, inline=True)
                em.add_field(name="RP", value=str(current_rp), inline=True)
                em.add_field(name="최고RP", value=str(best_rp), inline=True)

                em.add_field(name="K/D", value=f"{kd:.2f}", inline=True)
                em.add_field(name="승률", value=f"{win_rate:.1f}%", inline=True)
                em.add_field(name="Top10", value=f"{top10_rate:.1f}%", inline=True)

                em.add_field(name="평균 딜량", value=f"{adr:.1f}", inline=True)
                em.add_field(name="게임수", value=str(rounds), inline=True)
                em.add_field(name="최고티어", value=best_tier, inline=True)

                await interaction.followup.send(embed=em)

            except PubgApiError as e:
                await interaction.followup.send(f"전적 조회 실패: {e}")

    @app_commands.command(name="전적검색", description="배그 전적검색(일반/경쟁)")
    @app_commands.describe(
        kind="일반/경쟁",
        nickname="PUBG 닉네임(스팀)",
        mode="모드(솔로/듀오/스쿼드)",
        view="시점(TPP/FPP)",
    )
    @app_commands.choices(kind=KIND_CHOICES, mode=MODE_CHOICES, view=VIEW_CHOICES)
    async def stats(
        self,
        interaction: discord.Interaction,
        kind: app_commands.Choice[str],
        nickname: str,
        mode: app_commands.Choice[str],
        view: app_commands.Choice[str],
    ):
        if kind.value == "normal":
            await self._send_normal(interaction, nickname, base_mode=mode.value, view=view.value)
            return

        if kind.value == "ranked":
            await self._send_ranked(interaction, nickname, base_mode=mode.value, view=view.value)
            return

        await interaction.response.send_message("지원하지 않는 종류", ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(PubgStatsCog(bot))
