from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timezone, timedelta
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from shaded.config import Settings, ROOT_DIR
from shaded.services.sync_state import get_weekly_sync_last_utc_z


KST = timezone(timedelta(hours=9))


def _has_any_role(member: discord.Member, role_ids: set[int]) -> bool:
    if not role_ids:
        return True
    return any(getattr(r, "id", 0) in role_ids for r in getattr(member, "roles", []))


def _fmt_last_sync_kst(utc_z: Optional[str]) -> str:
    if not utc_z:
        return "-"
    try:
        dt = datetime.fromisoformat(utc_z.replace("Z", "+00:00")).astimezone(KST)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "-"


def _tail(text: str, max_lines: int = 12, max_chars: int = 900) -> str:
    lines = (text or "").splitlines()
    t = "\n".join(lines[-max_lines:])
    if len(t) > max_chars:
        t = t[-max_chars:]
    return t


class SyncNowCog(commands.Cog):
    def __init__(self, bot: commands.Bot, settings: Settings):
        self.bot = bot
        self.settings = settings

    @app_commands.command(name="sync_now", description="주간 킬 동기화를 즉시 1회 실행(운영자 전용)")
    async def sync_now(self, interaction: discord.Interaction):
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not member or not _has_any_role(member, self.settings.register_role_ids):
            await interaction.response.send_message("권한이 없음", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        # bot와 동일한 venv python으로, 프로젝트 루트에서 실행(.env 로드 안정화)
        cmd = [sys.executable, "-m", "tools.sync_weekly_kills"]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=str(ROOT_DIR),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except Exception as e:
            await interaction.followup.send(f"실행 실패: {type(e).__name__}: {e}", ephemeral=True)
            return

        stdout_b, stderr_b = await proc.communicate()

        rc = int(proc.returncode or 0)
        stdout = (stdout_b or b"").decode("utf-8", errors="ignore")
        stderr = (stderr_b or b"").decode("utf-8", errors="ignore")

        last_sync_utc_z = await get_weekly_sync_last_utc_z(self.settings.db_path)
        last_sync_kst = _fmt_last_sync_kst(last_sync_utc_z)

        if rc == 0:
            title = "SYNC OK"
        else:
            title = f"SYNC FAIL (rc={rc})"

        desc = f"**Last Sync**: {last_sync_kst} (KST)\n"
        if "[SKIP]" in stdout:
            desc += "**Result**: 이미 실행 중이어서 SKIP\n"

        out_tail = _tail(stdout)
        err_tail = _tail(stderr)

        embed = discord.Embed(title=title, description=desc)

        if out_tail.strip():
            embed.add_field(name="stdout (tail)", value=f"```\n{out_tail}\n```", inline=False)
        if err_tail.strip():
            embed.add_field(name="stderr (tail)", value=f"```\n{err_tail}\n```", inline=False)

        await interaction.followup.send(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot):
    settings = getattr(bot, "settings", None) or Settings()
    await bot.add_cog(SyncNowCog(bot, settings))
