import aiosqlite
import time

CLAN_ID_ALIAS = "shaded_steam"  # 너 프로젝트에서 쓰는 내부 클랜 키(고정)

async def init_clan_tables(db_path: str) -> None:
    async with aiosqlite.connect(db_path) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS players (
            platform TEXT NOT NULL,
            account_id TEXT NOT NULL,
            player_name TEXT NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (platform, account_id)
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS clan_members (
            clan_id TEXT NOT NULL,
            platform TEXT NOT NULL,
            account_id TEXT NOT NULL,
            clan_role TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            joined_at TEXT DEFAULT (datetime('now')),
            left_at TEXT,
            PRIMARY KEY (clan_id, platform, account_id)
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS discord_clan_link (
            discord_id INTEGER PRIMARY KEY,
            platform TEXT NOT NULL,
            account_id TEXT NOT NULL,
            updated_at INTEGER NOT NULL,
            UNIQUE(platform, account_id)
        )
        """)
        await db.commit()

async def register_member(db_path: str, discord_id: int, platform: str, account_id: str, player_name: str) -> None:
    now = int(time.time())
    async with aiosqlite.connect(db_path) as db:
        await db.execute("BEGIN")
        await db.execute(
            """
            INSERT INTO players (platform, account_id, player_name, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(platform, account_id) DO UPDATE SET
              player_name=excluded.player_name,
              updated_at=excluded.updated_at
            """,
            (platform, account_id, player_name, now),
        )
        await db.execute(
            """
            INSERT INTO clan_members (clan_id, platform, account_id, clan_role, is_active, left_at)
            VALUES (?, ?, ?, ?, 1, NULL)
            ON CONFLICT(clan_id, platform, account_id) DO UPDATE SET
              clan_role=excluded.clan_role,
              is_active=1,
              left_at=NULL
            """,
            (CLAN_ID_ALIAS, platform, account_id, "member"),
        )
        await db.execute(
            """
            INSERT INTO discord_clan_link (discord_id, platform, account_id, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(discord_id) DO UPDATE SET
              platform=excluded.platform,
              account_id=excluded.account_id,
              updated_at=excluded.updated_at
            """,
            (discord_id, platform, account_id, now),
        )
        await db.commit()
