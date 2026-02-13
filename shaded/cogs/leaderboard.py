from __future__ import annotations

import discord
from discord.ext import commands
from discord import app_commands

from shaded.config import Settings
from shaded.utils.time_window import week_window_utc, last_week_window_utc
from shaded.services.leaderboard_store import fetch_weekly_leaderboard, fetch_weekly_snapshot
from shaded.services.clan_store import CLAN_ID_ALIAS
from shaded.services.sync_state import get_weekly_sync_last_utc_z
from datetime import datetime, timezone, timedelta


KST = timezone(timedelta(hours=9))


def _fmt_last_sync_kst(utc_z: str | None) -> str:
    if not utc_z:
        return "-"
    try:
        dt = datetime.fromisoformat(utc_z.replace('Z', '+00:00')).astimezone(KST)
        return dt.strftime('%Y-%m-%d %H:%M')
    except Exception:
        return "-"


def _fmt_snapshot_created_kst(created_at_utc: str | None) -> str | None:
    if not created_at_utc:
        return None
    # sqlite datetime('now') -> 'YYYY-MM-DD HH:MM:SS' (UTC)
    try:
        dt = datetime.fromisoformat(created_at_utc.replace("Z", "").replace("T", " "))
        dt = dt.replace(tzinfo=timezone.utc).astimezone(KST)
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return None


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
    rows_override: list[tuple[str, int]] | None = None,
    snapshot: bool = False,
    snapshot_created_at_utc: str | None = None,
):
    rows = rows_override if rows_override is not None else await fetch_weekly_leaderboard(
        db_path=settings.db_path,
        clan_id=CLAN_ID_ALIAS,         # ✅ DB는 alias로 고정
        platform=settings.pubg_shard,
        start_utc_z=start_utc_z,
        end_utc_z=end_utc_z,
        scope=scope,
        limit=10,
    )

    label = _scope_label(scope)
    embed = discord.Embed(
        title=f"{title_prefix}{' (스냅샷)' if snapshot else ''} · {label}",
        description=f"기간: **{start_kst_str} ~ {end_kst_str} (KST)**\n집계: {_scope_desc(scope)}"
        + (f"\n스냅샷 생성: {_fmt_snapshot_created_kst(snapshot_created_at_utc)} (KST)" if snapshot and _fmt_snapshot_created_kst(snapshot_created_at_utc) else "")
        + ("\n스냅샷: ✅ (지난 주 주간 종료 시점 기준)" if snapshot else ""),
    )

    last_sync_utc_z = await get_weekly_sync_last_utc_z(settings.db_path)
    embed.set_footer(text=f"마지막 갱신: {_fmt_last_sync_kst(last_sync_utc_z)} (KST)")

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

        # ✅ 스냅샷 우선 (없으면 기존처럼 실시간 집계)
        snap_rows, snap_created = await fetch_weekly_snapshot(
            db_path=self.settings.db_path,
            clan_id=CLAN_ID_ALIAS,
            platform=self.settings.pubg_shard,
            week_start_utc_z=w.start_utc_z,
            scope=scope.value,
            limit=10,
        )

        await _send_board(
            interaction=interaction,
            settings=self.settings,
            title_prefix="지난 주 킬 랭킹",
            scope=scope.value,
            start_utc_z=w.start_utc_z,
            end_utc_z=w.end_utc_z,
            start_kst_str=w.start_kst.strftime("%Y-%m-%d %H:%M"),
            end_kst_str=w.end_kst.strftime("%Y-%m-%d %H:%M"),
            rows_override=snap_rows if snap_created is not None else None,  # empty list여도 스냅샷이면 그대로 표시
            snapshot=True if snap_created is not None else False,
            snapshot_created_at_utc=snap_created,
        )


async def setup(bot: commands.Bot):
    settings = getattr(bot, "settings", None) or Settings()
    await bot.add_cog(LeaderboardCog(bot, settings))
