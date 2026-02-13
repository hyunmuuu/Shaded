from __future__ import annotations

import os
import time
import traceback
from datetime import datetime, timezone, timedelta

import discord
from discord import app_commands
from discord.ext import commands

from shaded.config import Settings
from shaded.services.command_error_store import (
    init_command_error_log,
    record_command_error,
    fetch_last_errors_by_command,
    fetch_recent_errors,
    clear_errors,
)
from shaded.services.sqlite_conn import open_db
from shaded.utils.time_window import week_window_utc, last_week_window_utc
from shaded.services.leaderboard_store import fetch_weekly_leaderboard, fetch_weekly_snapshot
from shaded.services.user_store import get_pubg_nickname
from shaded.services.clan_store import CLAN_ID_ALIAS

KST = timezone(timedelta(hours=9))


def _kst(ts: int) -> str:
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).astimezone(KST).strftime("%m-%d %H:%M:%S")
    except Exception:
        return "-"


def _has_any_role(member: discord.Member, role_ids: set[int]) -> bool:
    if not role_ids:
        return True
    return any(getattr(r, "id", 0) in role_ids for r in getattr(member, "roles", []))


async def _table_exists(db_path: str, name: str) -> bool:
    async with open_db(db_path) as db:
        cur = await db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (name,),
        )
        try:
            row = await cur.fetchone()
            return row is not None
        finally:
            await cur.close()


class DiagnosticsCog(commands.Cog):
    def __init__(self, bot: commands.Bot, settings: Settings):
        self.bot = bot
        self.settings = settings

    @app_commands.command(name="진단", description="명령어 오류/DB 상태를 한 번에 점검(운영진 전용)")
    @app_commands.describe(hours="최근 몇 시간의 오류를 집계할지", reset="오류 기록을 비울지")
    async def diagnose(self, interaction: discord.Interaction, hours: int = 24, reset: bool = False):
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not member or not _has_any_role(member, self.settings.register_role_ids):
            await interaction.response.send_message("권한이 없음", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        await init_command_error_log(self.settings.db_path)
        if reset:
            await clear_errors(self.settings.db_path)

        # 1) 기본 점검(환경/DB/테이블)
        checks: list[str] = []

        token_ok = bool((self.settings.discord_token or "").strip())
        checks.append(f"DISCORD_TOKEN: {'OK' if token_ok else 'MISSING'}")

        pubg_ok = bool((self.settings.pubg_api_key or "").strip())
        checks.append(f"PUBG_API_KEY: {'OK' if pubg_ok else 'MISSING'}")

        checks.append(f"DB_PATH: {self.settings.db_path}")

        required_tables = [
            "players",
            "clan_members",
            "matches",
            "player_matches",
            "sync_state",
            "weekly_snapshot_meta",
            "weekly_snapshot_rows",
            "command_error_log",
        ]
        missing = []
        for t in required_tables:
            try:
                if not await _table_exists(self.settings.db_path, t):
                    missing.append(t)
            except Exception:
                missing.append(t)

        checks.append("TABLES: OK" if not missing else f"TABLES: MISSING {', '.join(missing)}")

        # 2) 스모크 테스트(“실제 명령어 실행” 대신 핵심 내부 호출만)
        smoke: list[str] = []
        try:
            # 닉네임 조회(내전적/아이디등록 계열)
            _ = await get_pubg_nickname(self.settings.db_path, int(interaction.user.id))
            smoke.append("user_store.get_pubg_nickname: OK")
        except Exception as e:
            smoke.append(f"user_store.get_pubg_nickname: FAIL ({type(e).__name__})")

        try:
            w = week_window_utc()
            _ = await fetch_weekly_leaderboard(
                db_path=self.settings.db_path,
                clan_id=CLAN_ID_ALIAS,
                platform=self.settings.pubg_shard,
                start_utc_z=w.start_utc_z,
                end_utc_z=w.end_utc_z,
                scope="total",
                limit=1,
            )
            smoke.append("leaderboard.fetch_weekly_leaderboard: OK")
        except Exception as e:
            smoke.append(f"leaderboard.fetch_weekly_leaderboard: FAIL ({type(e).__name__})")

        try:
            lw = last_week_window_utc()
            _ = await fetch_weekly_snapshot(
                db_path=self.settings.db_path,
                clan_id=CLAN_ID_ALIAS,
                platform=self.settings.pubg_shard,
                week_start_utc_z=lw.start_utc_z,
                scope="total",
                limit=1,
            )
            smoke.append("leaderboard.fetch_weekly_snapshot: OK")
        except Exception as e:
            smoke.append(f"leaderboard.fetch_weekly_snapshot: FAIL ({type(e).__name__})")

        # 3) “어떤 명령어가 터졌는지” 한 번에 보기
        now = int(time.time())
        since = now - max(1, int(hours)) * 3600
        last_err = await fetch_last_errors_by_command(self.settings.db_path, since_epoch=since)

        all_cmds = [c.name for c in self.bot.tree.get_commands()]
        all_cmds.sort()

        bad = []
        for name in all_cmds:
            if name in last_err:
                ts, msg = last_err[name]
                msg1 = msg.replace("\n", " ")[:160]
                bad.append(f"- `/{name}`  {_kst(ts)}  {msg1}")

        recent = await fetch_recent_errors(self.settings.db_path, limit=8)
        recent_lines = []
        for cmd, ts, msg in recent:
            msg1 = msg.replace("\n", " ")[:160]
            recent_lines.append(f"- `/{cmd}` {_kst(ts)}  {msg1}")

        embed = discord.Embed(
            title="Shaded 진단 결과",
            description=f"기간: 최근 {int(hours)}시간 (기준: {_kst(since)} ~ {_kst(now)})",
        )

        embed.add_field(name="Core Checks", value="```" + "\n".join(checks)[:950] + "```", inline=False)
        embed.add_field(name="Smoke Tests", value="```" + "\n".join(smoke)[:950] + "```", inline=False)

        if bad:
            embed.add_field(
                name=f"Commands with errors ({len(bad)})",
                value="\n".join(bad)[:1000],
                inline=False,
            )
        else:
            embed.add_field(name="Commands with errors", value="없음", inline=False)

        if recent_lines:
            embed.add_field(name="Recent error log (tail)", value="\n".join(recent_lines)[:1000], inline=False)

        await interaction.followup.send(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot):
    settings = getattr(bot, "settings", None) or Settings()
    await bot.add_cog(DiagnosticsCog(bot, settings))

    # ✅ 전역 슬래시 에러 훅: 자동 기록 + “응답하지 않음” 방지
    if getattr(bot, "_shaded_tree_error_hooked", False):
        return

    @bot.tree.error
    async def _on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
        await init_command_error_log(settings.db_path)

        # 원인 unwrap
        base = getattr(error, "original", None) or error

        tb = "".join(traceback.format_exception(type(base), base, base.__traceback__))
        tb_tail = tb[-1800:] if len(tb) > 1800 else tb

        cmd_name = "unknown"
        try:
            if interaction.command:
                cmd_name = interaction.command.name
        except Exception:
            pass

        try:
            await record_command_error(settings.db_path, cmd_name, tb_tail)
        except Exception:
            # 기록 실패는 그냥 통과
            pass

        msg = f"실행 중 오류 발생: `/{cmd_name}`\n`/진단`으로 어느 명령어가 터지는지 한 번에 확인 가능"

        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except Exception:
            pass

    bot._shaded_tree_error_hooked = True
