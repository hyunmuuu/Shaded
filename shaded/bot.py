import discord
from discord.ext import commands
from .config import Settings
from shaded.services.user_store import init_db
from shaded.services.clan_store import init_clan_tables 


EXTENSIONS = [
    "shaded.cogs.ping",
    "shaded.cogs.pubg",
    "shaded.cogs.moderation",
    "shaded.cogs.leaderboard",
    "shaded.cogs.status",
]


class ShadedBot(commands.Bot):
    def __init__(self, settings: Settings):
        intents = discord.Intents.default()
        super().__init__(
            command_prefix="!",
            intents=intents,
            activity=discord.Game(name="Shaded | /ping"),
        )
        self.settings = settings

    async def setup_hook(self):
        for ext in EXTENSIONS:
            await self.load_extension(ext)

        await init_db(self.settings.db_path)
        await init_clan_tables(self.settings.db_path)

        if self.settings.guild_id:
            guild = discord.Object(id=self.settings.guild_id)

            # ✅ 글로벌 커맨드(/ping)를 테스트 서버(길드)로 복사
            self.tree.copy_global_to(guild=guild)

            synced = await self.tree.sync(guild=guild)
            print(f"[SYNC] guild={guild.id} commands={len(synced)}", flush=True)

        else:
            synced = await self.tree.sync()
            print(f"[SYNC] global commands={len(synced)}")
            
    async def on_ready(self):
        print(f"[READY] user={self.user} id={getattr(self.user, 'id', None)}", flush=True)
        print(f"[READY] guilds={len(self.guilds)}", flush=True)
