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


async def upsert_clan_member(
    db_path: str,
    platform: str,
    account_id: str,
    player_name: str,
    *,
    clan_role: str = "member",
) -> None:
    """주간랭킹 집계 대상(= clan_members)에만 추가/갱신.

    - Discord 계정과의 링크(discord_clan_link)는 만들지 않음
    - 이미 있으면 이름/활성 상태만 갱신
    """
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
            (CLAN_ID_ALIAS, platform, account_id, clan_role),
        )
        await db.commit()


async def deactivate_clan_member(db_path: str, platform: str, account_id: str) -> int:
    """집계 대상에서 제거(비활성화). 반환값: 영향받은 row 수."""
    async with aiosqlite.connect(db_path) as db:
        cur = await db.execute(
            """
            UPDATE clan_members
               SET is_active=0,
                   left_at=datetime('now')
             WHERE clan_id=? AND platform=? AND account_id=?
               AND COALESCE(is_active, 1)=1
            """,
            (CLAN_ID_ALIAS, platform, account_id),
        )
        await db.commit()
        return int(cur.rowcount or 0)


async def find_active_member_account_id(db_path: str, platform: str, player_name: str) -> str | None:
    """DB에 이미 등록된(활성) 멤버면 account_id를 반환."""
    name = (player_name or "").strip()
    if not name:
        return None
    async with aiosqlite.connect(db_path) as db:
        cur = await db.execute(
            """
            SELECT p.account_id
              FROM clan_members cm
              JOIN players p
                ON p.platform = cm.platform AND p.account_id = cm.account_id
             WHERE cm.clan_id=?
               AND cm.platform=?
               AND COALESCE(cm.is_active, 1)=1
               AND p.player_name=?
             LIMIT 1
            """,
            (CLAN_ID_ALIAS, platform, name),
        )
        row = await cur.fetchone()
        await cur.close()
        return row[0] if row else None
