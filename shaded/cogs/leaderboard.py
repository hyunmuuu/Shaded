from __future__ import annotations

import discord
from discord.ext import commands
from discord import app_commands

from shaded.config import Settings
from shaded.utils.time_window import week_window_utc, last_week_window_utc
from shaded.services.leaderboard_store import fetch_weekly_leaderboard
from shaded.services.clan_store import CLAN_ID_ALIAS


SCOPE_CHOICES = [
    app_commands.Choice(name="일반(캐주얼/커스텀 제외)", value="normal"),
    app_commands.Choice(name="경쟁(캐주얼/커스텀 제외)", value="ranked"),
    app_commands.Choice(name="전체(캐주얼/커스텀 제외)", value="total"),
]


def _scope_label(scope: str) -> str:
    return {"normal": "일반", "ranked": "경쟁", "total": "전체"}.get(scope, "전체")


def _scope_desc(scope: str) -> str:
    if scope == "normal":
        return "일반(캐주얼/커스텀 제외) / 6모드 합산"
    if scope == "ranked":
        return "경쟁(캐주얼/커스텀 제외) / 6모드 합산"
    return "전체(캐주얼/커스텀 제외, 일반+경쟁) / 6모드 합산"


async def _send_board(
    interaction: discord.Interaction,
    settings: Settings,
    title_prefix: str,
    scope: str,
    start_utc_z: str,
    end_utc_z: str,
    start_kst_str: str,
    end_kst_str: str,
):
    rows = await fetch_weekly_leaderboard(
        db_path=settings.db_path,
        clan_id=CLAN_ID_ALIAS,         # ✅ DB는 alias로 고정
        platform=settings.pubg_shard,
        start_utc_z=start_utc_z,
        end_utc_z=end_utc_z,
        scope=scope,
        limit=10,
    )

    # ✅ 디버그가 필요하면 여기 한 줄만 켜서 콘솔에서 rows 확인 가능
    # print("[LEADERBOARD]", title_prefix, scope, start_utc_z, end_utc_z, rows)

    label = _scope_label(scope)
    embed = discord.Embed(
        title=f"{title_prefix} · {label}",
        description=f"기간: **{start_kst_str} ~ {end_kst_str} (KST)**\n집계: {_scope_desc(scope)}",
    )

    if not rows:
        embed.add_field(name="결과", value="데이터 없음", inline=False)
        await interaction.response.send_message(embed=embed)
        return

    lines = [f"**{i}.** `{name}` — **{kills}**" for i, (name, kills) in enumerate(rows, 1)]
    embed.add_field(name="TOP 10", value="\n".join(lines), inline=False)
    await interaction.response.send_message(embed=embed)


class LeaderboardCog(commands.Cog):
    def __init__(self, bot: commands.Bot, settings: Settings):
        self.bot = bot
        self.settings = settings

    @app_commands.command(name="주간랭킹", description="이번 주 주간 킬 랭킹")
    @app_commands.choices(scope=SCOPE_CHOICES)
    async def weekly(
        self,
        interaction: discord.Interaction,
        scope: app_commands.Choice[str],
    ):
        w = week_window_utc()
        await _send_board(
            interaction=interaction,
            settings=self.settings,
            title_prefix="주간 킬 랭킹",
            scope=scope.value,
            start_utc_z=w.start_utc_z,
            end_utc_z=w.end_utc_z,
            start_kst_str=w.start_kst.strftime("%Y-%m-%d %H:%M"),
            end_kst_str=w.end_kst.strftime("%Y-%m-%d %H:%M"),
        )

    @app_commands.command(name="지난랭킹", description="지난 주 주간 킬 랭킹")
    @app_commands.choices(scope=SCOPE_CHOICES)
    async def last_week(
        self,
        interaction: discord.Interaction,
        scope: app_commands.Choice[str],
    ):
        w = last_week_window_utc()
        await _send_board(
            interaction=interaction,
            settings=self.settings,
            title_prefix="지난 주 킬 랭킹",
            scope=scope.value,
            start_utc_z=w.start_utc_z,
            end_utc_z=w.end_utc_z,
            start_kst_str=w.start_kst.strftime("%Y-%m-%d %H:%M"),
            end_kst_str=w.end_kst.strftime("%Y-%m-%d %H:%M"),
        )


async def setup(bot: commands.Bot):
    settings = getattr(bot, "settings", None) or Settings()
    await bot.add_cog(LeaderboardCog(bot, settings))
