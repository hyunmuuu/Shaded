import discord
from discord import app_commands
from discord.ext import commands

from shaded.services.pubg_stats import PubgStatsService
from shaded.services.pubg_api import PubgApiError
from shaded.ui.embeds import normal_embed, ranked_embed
from shaded.services.user_store import get_pubg_nickname, set_pubg_nickname

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

class PubgCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    def _svc(self) -> PubgStatsService:
        settings = getattr(self.bot, "settings", None)
        return PubgStatsService(settings.pubg_api_key, settings.pubg_shard)

    def _db_path(self) -> str:
        settings = getattr(self.bot, "settings", None)
        return settings.db_path

    # ✅ 닉네임 등록
    @app_commands.command(name="아이디등록", description="내 PUBG(스팀) 닉네임을 등록/변경")
    @app_commands.describe(nickname="PUBG 닉네임(스팀)")
    async def register_pubg(self, interaction: discord.Interaction, nickname: str):
        settings = getattr(self.bot, "settings", None)
        if not settings or not settings.pubg_api_key:
            await interaction.response.send_message("PUBG_API_KEY가 .env에 없음", ephemeral=True)
            return

        discord_id = interaction.user.id
        try:
            await set_pubg_nickname(self._db_path(), discord_id, nickname)
        except ValueError:
            await interaction.response.send_message("닉네임이 비어있음", ephemeral=True)
            return

        await interaction.response.send_message(
            f"등록 완료: `{nickname.strip()}`\n이제 `/내전적`에서 닉네임 없이 조회 가능",
            ephemeral=True,
        )

    # ✅ 내전적: 닉네임 입력 없이(등록된 닉네임 사용)
    @app_commands.command(name="내전적", description="등록된 닉네임으로 내 전적 조회(일반/경쟁)")
    @app_commands.describe(
        kind="일반/경쟁",
        mode="모드(솔로/듀오/스쿼드)",
        view="시점(TPP/FPP)",
    )
    @app_commands.choices(kind=KIND_CHOICES, mode=MODE_CHOICES, view=VIEW_CHOICES)
    async def my_stats(
        self,
        interaction: discord.Interaction,
        kind: app_commands.Choice[str],
        mode: app_commands.Choice[str],
        view: app_commands.Choice[str],
    ):
        settings = getattr(self.bot, "settings", None)
        if not settings or not settings.pubg_api_key:
            await interaction.response.send_message("PUBG_API_KEY가 .env에 없음", ephemeral=True)
            return

        nickname = await get_pubg_nickname(self._db_path(), interaction.user.id)
        if not nickname:
            await interaction.response.send_message(
                "먼저 `/배그등록 닉네임`으로 본인 닉네임을 등록해줘.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(thinking=True)
        svc = self._svc()

        try:
            if kind.value == "normal":
                s = await svc.fetch_normal(nickname, mode.value, view.value)
                await interaction.followup.send(embed=normal_embed(s))
                return

            if kind.value == "ranked":
                s = await svc.fetch_ranked(nickname, mode.value, view.value)
                await interaction.followup.send(embed=ranked_embed(s))
                return

            await interaction.followup.send("지원하지 않는 종류")

        except PubgApiError as e:
            await interaction.followup.send(f"전적 조회 실패: {e}")

    # (기존) 전적검색: 닉네임 직접 입력 버전도 유지하고 싶으면 그대로 둠
    @app_commands.command(name="전적검색", description="배그 전적검색(일반/경쟁) - 닉네임 직접 입력")
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
        settings = getattr(self.bot, "settings", None)
        if not settings or not settings.pubg_api_key:
            await interaction.response.send_message("PUBG_API_KEY가 .env에 없음", ephemeral=True)
            return

        await interaction.response.defer(thinking=True)
        svc = self._svc()

        try:
            if kind.value == "normal":
                s = await svc.fetch_normal(nickname, mode.value, view.value)
                await interaction.followup.send(embed=normal_embed(s))
                return

            if kind.value == "ranked":
                s = await svc.fetch_ranked(nickname, mode.value, view.value)
                await interaction.followup.send(embed=ranked_embed(s))
                return

            await interaction.followup.send("지원하지 않는 종류")

        except PubgApiError as e:
            await interaction.followup.send(f"전적 조회 실패: {e}")

async def setup(bot: commands.Bot):
    await bot.add_cog(PubgCog(bot))
