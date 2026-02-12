from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta

import discord
from discord import app_commands
from discord.ext import commands

from shaded.config import Settings
from shaded.services.clan_store import CLAN_ID_ALIAS
from shaded.services.leaderboard_store import fetch_weekly_leaderboard
from shaded.services.sqlite_conn import open_db
from shaded.services.sync_state import get_weekly_sync_last_utc_z, get_weekly_sync_last_error
from shaded.utils.time_window import week_window_utc

KST = timezone(timedelta(hours=9))
JOB_NAME = "sync_weekly_kills"


def _has_any_role(member: discord.Member, role_ids: set[int]) -> bool:
    if not role_ids:
        return True
    return any(getattr(r, "id", 0) in role_ids for r in getattr(member, "roles", []))


def _fmt_dt_kst(dt: datetime) -> str:
    return dt.astimezone(KST).strftime("%Y-%m-%d %H:%M")


def _fmt_last_sync_kst(utc_z: str | None) -> str:
    if not utc_z:
        return "-"
    try:
        dt = datetime.fromisoformat(utc_z.replace("Z", "+00:00")).astimezone(KST)
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return "-"


async def _get_job_lock(db_path: str) -> tuple[bool, int, str | None]:
    """return (running, locked_until_epoch, locked_by)"""
    now = int(time.time())
    try:
        async with open_db(db_path) as db:
            cur = await db.execute(
                "SELECT locked_until, locked_by FROM job_lock WHERE job_name=?",
                (JOB_NAME,),
            )
            row = await cur.fetchone()
            await cur.close()
    except Exception:
        return False, 0, None

    if not row:
        return False, 0, None

    locked_until = int(row[0] or 0)
    locked_by = row[1]
    return (locked_until > now), locked_until, locked_by


async def _count_active_members(db_path: str, clan_id: str, platform: str) -> int:
    async with open_db(db_path) as db:
        cur = await db.execute(
            """
            SELECT COUNT(*)
              FROM clan_members
             WHERE clan_id=? AND platform=? AND COALESCE(is_active, 1)=1
            """,
            (clan_id, platform),
        )
        row = await cur.fetchone()
        await cur.close()
        return int(row[0] or 0)


async def _count_week_matches(db_path: str, platform: str, start_utc_z: str, end_utc_z: str) -> int:
    async with open_db(db_path) as db:
        cur = await db.execute(
            """
            SELECT COUNT(*)
              FROM matches
             WHERE platform=?
               AND created_at_utc >= ?
               AND created_at_utc < ?
               AND COALESCE(is_casual, 0) = 0
               AND COALESCE(is_custom_match, 0) = 0
            """,
            (platform, start_utc_z, end_utc_z),
        )
        row = await cur.fetchone()
        await cur.close()
        return int(row[0] or 0)


class StatusCog(commands.Cog):
    def __init__(self, bot: commands.Bot, settings: Settings):
        self.bot = bot
        self.settings = settings

    @app_commands.command(name="status", description="봇 상태 확인(운영자 전용)")
    async def status(self, interaction: discord.Interaction):
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not member or not _has_any_role(member, self.settings.register_role_ids):
            await interaction.response.send_message("권한이 없음", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        w = week_window_utc()
        start_kst_str = _fmt_dt_kst(w.start_kst)
        end_kst_str = _fmt_dt_kst(w.end_kst)

        last_sync_utc_z = await get_weekly_sync_last_utc_z(self.settings.db_path)
        last_sync_kst = _fmt_last_sync_kst(last_sync_utc_z)

        running, locked_until, locked_by = await _get_job_lock(self.settings.db_path)
        if running:
            until_kst = _fmt_dt_kst(datetime.fromtimestamp(locked_until, tz=timezone.utc))
            lock_str = f"RUNNING (until {until_kst} KST)"
            if locked_by:
                lock_str += f"\nby `{locked_by}`"
        else:
            lock_str = "IDLE"

        active_members = await _count_active_members(self.settings.db_path, CLAN_ID_ALIAS, self.settings.pubg_shard)
        week_matches = await _count_week_matches(self.settings.db_path, self.settings.pubg_shard, w.start_utc_z, w.end_utc_z)

        top1_rows = await fetch_weekly_leaderboard(
            db_path=self.settings.db_path,
            clan_id=CLAN_ID_ALIAS,
            platform=self.settings.pubg_shard,
            start_utc_z=w.start_utc_z,
            end_utc_z=w.end_utc_z,
            scope="total",
            limit=1,
        )
        top1_str = "-" if not top1_rows else f"{top1_rows[0][0]} ({top1_rows[0][1]})"

        last_err = await get_weekly_sync_last_error(self.settings.db_path)
        if last_err and (last_err[0] or "").strip():
            err_msg, err_at = last_err
            err_time = _fmt_dt_kst(datetime.fromtimestamp(int(err_at), tz=timezone.utc))
            err_str = f"{err_time} KST\n{err_msg}"[:900]
        else:
            err_str = "none"

        embed = discord.Embed(
            title="Shaded Status",
            description=(
                f"**Week**: {start_kst_str} ~ {end_kst_str} (KST)\n"
                f"**Last Sync**: {last_sync_kst} (KST)\n"
                f"**Sync Lock**: {lock_str}\n"
                f"**Active Members**: {active_members}\n"
                f"**Week Matches**: {week_matches}\n"
                f"**Top1 (kills)**: {top1_str}\n"
                f"**Last Error**: {err_str}"
            ),
        )

        await interaction.followup.send(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot):
    settings = getattr(bot, "settings", None) or Settings()
    await bot.add_cog(StatusCog(bot, settings))
