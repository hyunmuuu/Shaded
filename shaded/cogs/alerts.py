from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional

import discord
from discord.ext import commands

from shaded.config import Settings
from shaded.services.sync_state import (
    get_weekly_sync_last_error,
    get_weekly_sync_last_error_notified_at,
    set_weekly_sync_last_error_notified_at,
)

KST = timezone(timedelta(hours=9))


def _fmt_kst(epoch: int) -> str:
    try:
        return (
            datetime.fromtimestamp(int(epoch), tz=timezone.utc)
            .astimezone(KST)
            .strftime("%Y-%m-%d %H:%M:%S")
        )
    except Exception:
        return "-"


def _build_role_mentions(role_ids: set[int]) -> str:
    if not role_ids:
        return ""
    return " ".join(f"<@&{rid}>" for rid in sorted(role_ids))


class AlertsCog(commands.Cog):
    """
    sync_state.weekly_sync_last_error 가 갱신되면 ALERT_CHANNEL_ID로 임베드 알림을 보냄.
    - 중복 발송 방지: weekly_sync_last_error_notified_at(값=epoch) 저장
    """

    def __init__(self, bot: commands.Bot, settings: Settings):
        self.bot = bot
        self.settings = settings
        self._task: Optional[asyncio.Task] = None

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._loop(), name="alerts-loop")

    def stop(self) -> None:
        if self._task:
            self._task.cancel()

    async def cog_unload(self) -> None:
        self.stop()

    async def _get_channel(self) -> Optional[discord.abc.Messageable]:
        cid = int(getattr(self.settings, "alert_channel_id", 0) or 0)
        if cid <= 0:
            return None

        ch = self.bot.get_channel(cid)
        if ch:
            return ch

        try:
            return await self.bot.fetch_channel(cid)
        except Exception as e:
            print(f"[ALERTS] fetch_channel failed: {type(e).__name__}: {e}", flush=True)
            return None

    async def _loop(self) -> None:
        await self.bot.wait_until_ready()

        cid = int(getattr(self.settings, "alert_channel_id", 0) or 0)
        if cid <= 0:
            print("[ALERTS] disabled: ALERT_CHANNEL_ID not set", flush=True)
            return

        print(f"[ALERTS] enabled: channel_id={cid}", flush=True)

        allowed = discord.AllowedMentions(everyone=False, users=False, roles=True, replied_user=False)

        while not self.bot.is_closed():
            try:
                err = await get_weekly_sync_last_error(self.settings.db_path)
                if err:
                    msg, updated_at = err
                    msg = (msg or "").strip()

                    notified_at = await get_weekly_sync_last_error_notified_at(self.settings.db_path)

                    if msg and int(updated_at) > int(notified_at):
                        ch = await self._get_channel()
                        if ch is None:
                            await asyncio.sleep(30)
                            continue

                        embed = discord.Embed(
                            title="SYNC ERROR",
                            description=msg[:1800],
                        )
                        embed.add_field(name="time (KST)", value=_fmt_kst(updated_at), inline=False)

                        mention = _build_role_mentions(self.settings.alert_mention_role_ids)

                        if mention:
                            await ch.send(content=mention, embed=embed, allowed_mentions=allowed)
                        else:
                            await ch.send(embed=embed)

                        await set_weekly_sync_last_error_notified_at(self.settings.db_path, int(updated_at))
                        print(f"[ALERTS] sent: updated_at={updated_at}", flush=True)

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[ALERTS] loop error: {type(e).__name__}: {e}", flush=True)

            await asyncio.sleep(30)


async def setup(bot: commands.Bot):
    settings = getattr(bot, "settings", None) or Settings()
    cog = AlertsCog(bot, settings)
    await bot.add_cog(cog)
    cog.start()
