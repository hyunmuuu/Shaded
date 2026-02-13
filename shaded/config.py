from dataclasses import dataclass, field
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(override=True)

ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = ROOT_DIR / "db" / "shaded.db"


def _parse_id_list(v: str) -> set[int]:
    v = (v or "").strip()
    if not v:
        return set()
    out: set[int] = set()
    for part in v.split(","):
        part = part.strip()
        if part.isdigit():
            out.add(int(part))
    return out


def _resolve_db_path(v: str) -> str:
    v = (v or "").strip()
    if not v:
        return str(DEFAULT_DB_PATH)
    p = Path(v)
    if p.is_absolute():
        return str(p)
    return str((ROOT_DIR / p).resolve())


def _clean_pubg_key(v: str) -> str:
    v = (v or "").strip()
    v = v.removeprefix("Bearer ").strip()
    return v.strip('"').strip("'")


@dataclass(frozen=True)
class Settings:
    # Discord
    discord_token: str = os.getenv("DISCORD_TOKEN", "").strip()
    guild_id: int = int((os.getenv("GUILD_ID", "0") or "0").strip())
    register_role_ids: set[int] = field(
        default_factory=lambda: _parse_id_list(os.getenv("REGISTER_ROLE_IDS", ""))
    )

    # Alerts
    alert_channel_id: int = int((os.getenv("ALERT_CHANNEL_ID", "0") or "0").strip())
    alert_mention_role_ids: set[int] = field(
        default_factory=lambda: _parse_id_list(os.getenv("ALERT_MENTION_ROLE_IDS", ""))
    )

    # PUBG
    pubg_api_key: str = _clean_pubg_key(os.getenv("PUBG_API_KEY", ""))
    pubg_shard: str = os.getenv("PUBG_SHARD", "steam").strip()
    pubg_clan_id: str = os.getenv("PUBG_CLAN_ID", "").strip()

    # DB
    db_path: str = _resolve_db_path(os.getenv("DB_PATH", ""))
