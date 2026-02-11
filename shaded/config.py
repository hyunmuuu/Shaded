from dataclasses import dataclass, field
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(override=True)

ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = ROOT_DIR / "db" / "shaded.db"  # 너는 db/shaded.db 쓰기로 했으니 이걸 기본값으로

def _parse_id_list(v: str) -> set[int]:
    v = (v or "").strip()
    if not v:
        return set()
    out = set()
    for part in v.split(","):
        part = part.strip()
        if part.isdigit():
            out.add(int(part))
    return out

@dataclass(frozen=True)
class Settings:
    # Discord
    discord_token: str = os.getenv("DISCORD_TOKEN", "").strip()
    guild_id: int = int((os.getenv("GUILD_ID", "0") or "0").strip())
    register_role_ids: set[int] = field(
        default_factory=lambda: _parse_id_list(os.getenv("REGISTER_ROLE_IDS", ""))
    )

    # PUBG
    pubg_api_key: str = (
        os.getenv("PUBG_API_KEY", "")
        .strip()
        .removeprefix("Bearer ")
        .strip()
        .strip('"')
        .strip("'")
    )
    pubg_shard: str = os.getenv("PUBG_SHARD", "steam").strip()
    pubg_clan_id: str = os.getenv("PUBG_CLAN_ID", "").strip()

    # DB
    db_path: str = os.getenv("DB_PATH", str(DEFAULT_DB_PATH)).strip()
