import discord
from shaded.services.pubg_stats import NormalStats, RankedStats

def normal_embed(s: NormalStats) -> discord.Embed:
    em = discord.Embed(title=f"Shaded 전적 | {s.title} (Steam)")
    em.add_field(name="닉네임", value=s.nickname, inline=False)
    em.add_field(name="시즌", value=s.season_label, inline=False)

    em.add_field(name="K/D", value=f"{s.kd:.2f}", inline=True)
    em.add_field(name="승률", value=f"{s.win_rate:.1f}%", inline=True)
    em.add_field(name="Top10", value=f"{s.top10_rate:.1f}%", inline=True)

    em.add_field(name="평균 딜량", value=f"{s.adr:.1f}", inline=True)
    em.add_field(name="게임수", value=str(s.rounds), inline=True)
    em.add_field(name="최다 킬", value=str(s.yopo), inline=True)

    em.add_field(name="헤드샷", value=f"{s.hs_rate:.1f}%", inline=True)
    em.add_field(name="저격", value=f"{s.longest_kill:.1f}m" if s.longest_kill > 0 else "-", inline=True)
    em.add_field(name="생존", value=s.survival_txt, inline=True)
    return em

def ranked_embed(s: RankedStats) -> discord.Embed:
    em = discord.Embed(title=f"Shaded 전적 | {s.title} (Steam)")
    em.add_field(name="닉네임", value=s.nickname, inline=False)
    em.add_field(name="시즌", value=s.season_label, inline=False)

    em.add_field(name="티어", value=s.tier, inline=True)
    em.add_field(name="RP", value=str(s.rp), inline=True)
    em.add_field(name="최고RP", value=str(s.best_rp), inline=True)

    em.add_field(name="K/D", value=f"{s.kd:.2f}", inline=True)
    em.add_field(name="승률", value=f"{s.win_rate:.1f}%", inline=True)
    em.add_field(name="Top10", value=f"{s.top10_rate:.1f}%", inline=True)

    em.add_field(name="평균 딜량", value=f"{s.adr:.1f}", inline=True)
    em.add_field(name="게임수", value=str(s.rounds), inline=True)
    em.add_field(name="최고티어", value=s.best_tier, inline=True)
    return em
