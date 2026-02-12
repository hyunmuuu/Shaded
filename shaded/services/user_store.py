from __future__ import annotations
from pathlib import Path
from typing import Optional
import time
from shaded.services.sqlite_conn import open_db

from shaded.services.sync_state import init_sync_state

async def init_db(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    async with open_db(db_path) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS pubg_user (
                discord_id INTEGER PRIMARY KEY,
                pubg_nickname TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """
        )
        await db.commit()

    # sync_state(마지막 갱신 시각 저장용)
    await init_sync_state(db_path)


async def set_pubg_nickname(db_path: str, discord_id: int, nickname: str) -> None:
    nick = nickname.strip()
    if not nick:
        raise ValueError("nickname is empty")

    now = int(time.time())
    async with open_db(db_path) as db:
        await db.execute(
            """
            INSERT INTO pubg_user (discord_id, pubg_nickname, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(discord_id) DO UPDATE SET
                pubg_nickname=excluded.pubg_nickname,
                updated_at=excluded.updated_at
            """,
            (discord_id, nick, now),
        )
        await db.commit()

async def get_pubg_nickname(db_path: str, discord_id: int) -> Optional[str]:
    async with open_db(db_path) as db:
        cur = await db.execute(
            "SELECT pubg_nickname FROM pubg_user WHERE discord_id=?",
            (discord_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        return row[0] if row else None
