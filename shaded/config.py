from dataclasses import dataclass
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(override=True)

ROOT_DIR = Path(__file__).resolve().parents[1]  # 프로젝트 루트(Shaded)
DEFAULT_DB_PATH = ROOT_DIR / "data" / "shaded.db"

@dataclass(frozen=True)
class Settings:
    discord_token: str = os.getenv("DISCORD_TOKEN", "").strip()
    guild_id: int = int((os.getenv("GUILD_ID", "0") or "0").strip())

    pubg_api_key: str = (
        os.getenv("PUBG_API_KEY", "")
        .strip()
        .removeprefix("Bearer ")
        .strip()
        .strip('"')
        .strip("'")
    )
    pubg_shard: str = os.getenv("PUBG_SHARD", "steam").strip()

    db_path: str = os.getenv("DB_PATH", str(DEFAULT_DB_PATH)).strip()
    