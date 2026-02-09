import discord
from discord import app_commands
from discord.ext import commands

# 운영진/관리자 역할 ID로 바꿔 넣기
ALLOWED_ROLE_IDS = (
    1468993548773884055,
    1468993250399359322,
)

class ModerationCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="삭제", description="최근 메시지를 지정한 개수만큼 삭제")
    @app_commands.describe(count="삭제할 메시지 개수(1~100)")

    # ✅ 여기 두 줄이 핵심
    @app_commands.checks.has_any_role(*ALLOWED_ROLE_IDS)
    @app_commands.checks.bot_has_permissions(manage_messages=True, read_message_history=True)

    async def delete_messages(
        self,
        interaction: discord.Interaction,
        count: app_commands.Range[int, 1, 100],
    ):
        if interaction.guild is None or interaction.channel is None:
            await interaction.response.send_message("서버 채널에서만 사용할 수 있어.", ephemeral=True)
            return

        channel = interaction.channel
        if not isinstance(channel, (discord.TextChannel, discord.Thread)):
            await interaction.response.send_message("이 채널에서는 지원하지 않아.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        deleted = await channel.purge(
            limit=count,
            check=lambda m: not m.pinned,  # 고정 메시지 보호(원하면 제거)
            reason=f"/삭제 by {interaction.user} ({interaction.user.id})",
        )

        await interaction.followup.send(
            f"요청: {count}개 / 실제 삭제: {len(deleted)}개 (고정 메시지 제외)",
            ephemeral=True,
        )

    @delete_messages.error
    async def delete_messages_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        # 체크 실패(역할 없음/DM에서 호출 등)는 커맨드 본문 실행 전에 여기로 옴
        if isinstance(error, app_commands.MissingAnyRole):
            msg = "이 명령어는 지정된 운영 역할만 사용할 수 있어."
        elif isinstance(error, app_commands.BotMissingPermissions):
            msg = "봇 권한이 부족해. (메시지 관리 + 메시지 기록 보기 필요)"
        elif isinstance(error, app_commands.NoPrivateMessage):
            msg = "DM에서는 사용할 수 없어."
        else:
            msg = f"오류: {error}"

        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(ModerationCog(bot))
