from .config import Settings
from .bot import ShadedBot

def main():
    settings = Settings()
    if not settings.discord_token:
        raise SystemExit("DISCORD_TOKEN is missing in .env")

    bot = ShadedBot(settings)
    bot.run(settings.discord_token)

if __name__ == "__main__":
    main()
