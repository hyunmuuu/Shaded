import traceback
import pkgutil

import discord
from discord.ext import commands

from .config import Settings
from shaded.services.user_store import init_db
from shaded.services.clan_store import init_clan_tables


def discover_extensions() -> list[str]:
    import shaded.cogs as cogs_pkg

    exts: list[str] = []
    for m in pkgutil.iter_modules(cogs_pkg.__path__):
        if m.ispkg:
            continue
        if m.name.startswith("_"):
            continue
        exts.append(f"shaded.cogs.{m.name}")
    exts.sort()
    return exts


class ShadedBot(commands.Bot):
    def __init__(self, settings: Settings):
        # ✅ privileged intents 없이도 슬래시 커맨드/역할체크(인터랙션 payload의 roles)는 동작
        intents = discord.Intents.default()
        intents.guilds = True

        # ❌ 이거 켜면 개발자포털에서 Server Members Intent를 반드시 켜야 함
        intents.members = False

        # prefix 커맨드 안 쓸거면 message_content는 불필요(경고만 나고 크래시는 아님)
        intents.message_content = False

        super().__init__(
            command_prefix="!",
            intents=intents,
            activity=discord.Game(name="Shaded | /ping"),
        )
        self.settings = settings

    async def setup_hook(self):
        await init_db(self.settings.db_path)
        await init_clan_tables(self.settings.db_path)

        exts = discover_extensions()
        print(f"[BOOT] discover_extensions={len(exts)}", flush=True)

        for ext in exts:
            try:
                await self.load_extension(ext)
                print(f"[LOAD] OK  {ext}", flush=True)
            except Exception:
                print(f"[LOAD] FAIL {ext}", flush=True)
                traceback.print_exc()

        # 현재 트리에 올라간 커맨드 목록 출력
        try:
            cmds = [c.name for c in self.tree.get_commands()]
            print(f"[TREE] global_commands={len(cmds)} {cmds}", flush=True)
        except Exception as e:
            print(f"[TREE] inspect failed: {type(e).__name__}: {e}", flush=True)

        # ✅ 개발 중이면 길드 sync로 즉시 반영
        if self.settings.guild_id:
            guild = discord.Object(id=self.settings.guild_id)
            self.tree.copy_global_to(guild=guild)
            synced = await self.tree.sync(guild=guild)
            print(f"[SYNC] guild={guild.id} commands={len(synced)} {[c.name for c in synced]}", flush=True)
        else:
            synced = await self.tree.sync()
            print(f"[SYNC] global commands={len(synced)} {[c.name for c in synced]}", flush=True)

    async def on_ready(self):
        print(f"[READY] user={self.user} id={getattr(self.user, 'id', None)}", flush=True)
        print(f"[READY] guilds={len(self.guilds)}", flush=True)
