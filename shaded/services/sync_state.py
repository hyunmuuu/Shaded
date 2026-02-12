from __future__ import annotations

import time
from typing import Optional, Tuple

from shaded.services.sqlite_conn import open_db


STATE_KEY_WEEKLY_SYNC_UTC_Z = "weekly_sync_last_utc_z"
STATE_KEY_WEEKLY_SYNC_LAST_ERROR = "weekly_sync_last_error"


async def init_sync_state(db_path: str) -> None:
    async with open_db(db_path) as db:
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


async def _upsert_state(db_path: str, key: str, value: str) -> None:
    now = int(time.time())
    async with open_db(db_path) as db:
        await db.execute(
            """
            INSERT INTO sync_state (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
              value=excluded.value,
              updated_at=excluded.updated_at
            """,
            (key, value, now),
        )
        await db.commit()


async def _get_state(db_path: str, key: str) -> Optional[Tuple[str, int]]:
    async with open_db(db_path) as db:
        cur = await db.execute(
            "SELECT value, updated_at FROM sync_state WHERE key=?",
            (key,),
        )
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return None
        return str(row[0]), int(row[1])


async def set_weekly_sync_last_utc_z(db_path: str, utc_z: str) -> None:
    await _upsert_state(db_path, STATE_KEY_WEEKLY_SYNC_UTC_Z, utc_z)


async def get_weekly_sync_last_utc_z(db_path: str) -> Optional[str]:
    v = await _get_state(db_path, STATE_KEY_WEEKLY_SYNC_UTC_Z)
    return v[0] if v else None


async def set_weekly_sync_last_error(db_path: str, message: str) -> None:
    # message가 비어있으면 "none"으로 저장해서 /status에서 깔끔하게 표시
    msg = (message or "").strip()
    await _upsert_state(db_path, STATE_KEY_WEEKLY_SYNC_LAST_ERROR, msg)


async def get_weekly_sync_last_error(db_path: str) -> Optional[Tuple[str, int]]:
    return await _get_state(db_path, STATE_KEY_WEEKLY_SYNC_LAST_ERROR)
