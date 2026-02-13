from __future__ import annotations

from pathlib import Path
from typing import Optional
import time

import aiosqlite
from shaded.services.sqlite_conn import open_db

from shaded.services.sync_state import init_sync_state
from shaded.services.leaderboard_store import init_weekly_snapshot_tables


async def _fetchone(con: aiosqlite.Connection, sql: str, params: tuple) -> Optional[aiosqlite.Row]:
    """
    aiosqlite/래퍼 버전에 따라 execute_fetchone이 없을 수 있어 호환 처리.
    """
    if hasattr(con, "execute_fetchone"):
        return await con.execute_fetchone(sql, params)  # type: ignore

    cur = await con.execute(sql, params)
    try:
        row = await cur.fetchone()
        return row
    finally:
        await cur.close()


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

    # snapshot 테이블(지난랭킹 고정 저장)
    await init_weekly_snapshot_tables(db_path)


async def set_pubg_nickname(db_path: str, discord_id: int, nickname: str) -> None:
    nick = (nickname or "").strip()
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
            (int(discord_id), nick, int(now)),
        )
        await db.commit()


async def get_pubg_nickname(db_path: str, discord_id: int) -> Optional[str]:
    async with open_db(db_path) as db:
        row = await _fetchone(
            db,
            "SELECT pubg_nickname FROM pubg_user WHERE discord_id=?",
            (int(discord_id),),
        )
    if not row:
        return None
    return str(row[0])
