from __future__ import annotations

import time
from typing import Optional
import aiosqlite


STATE_KEY_WEEKLY_SYNC_UTC_Z = "weekly_sync_last_utc_z"


async def init_sync_state(db_path: str) -> None:
    async with aiosqlite.connect(db_path) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS sync_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """
        )
        await db.commit()


async def set_weekly_sync_last_utc_z(db_path: str, utc_z: str) -> None:
    now = int(time.time())
    async with aiosqlite.connect(db_path) as db:
        await db.execute(
            """
            INSERT INTO sync_state (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
              value=excluded.value,
              updated_at=excluded.updated_at
            """,
            (STATE_KEY_WEEKLY_SYNC_UTC_Z, utc_z, now),
        )
        await db.commit()


async def get_weekly_sync_last_utc_z(db_path: str) -> Optional[str]:
    async with aiosqlite.connect(db_path) as db:
        cur = await db.execute(
            "SELECT value FROM sync_state WHERE key=?",
            (STATE_KEY_WEEKLY_SYNC_UTC_Z,),
        )
        row = await cur.fetchone()
        await cur.close()
        return row[0] if row else None
