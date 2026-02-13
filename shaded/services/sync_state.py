from __future__ import annotations

import time
from typing import Optional, Tuple

from shaded.services.sqlite_conn import open_db


STATE_KEY_WEEKLY_SYNC_UTC_Z = "weekly_sync_last_utc_z"
STATE_KEY_WEEKLY_SYNC_LAST_ERROR = "weekly_sync_last_error"
STATE_KEY_WEEKLY_SYNC_LAST_ERROR_NOTIFIED_AT = "weekly_sync_last_error_notified_at"


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
    msg = (message or "").strip()
    await _upsert_state(db_path, STATE_KEY_WEEKLY_SYNC_LAST_ERROR, msg)


async def get_weekly_sync_last_error(db_path: str) -> Optional[Tuple[str, int]]:
    return await _get_state(db_path, STATE_KEY_WEEKLY_SYNC_LAST_ERROR)


async def set_weekly_sync_last_error_notified_at(db_path: str, updated_at: int) -> None:
    # updated_at(epoch)를 value(TEXT)에 저장
    await _upsert_state(db_path, STATE_KEY_WEEKLY_SYNC_LAST_ERROR_NOTIFIED_AT, str(int(updated_at)))


async def get_weekly_sync_last_error_notified_at(db_path: str) -> int:
    v = await _get_state(db_path, STATE_KEY_WEEKLY_SYNC_LAST_ERROR_NOTIFIED_AT)
    if not v:
        return 0
    try:
        return int(v[0])
    except Exception:
        return 0
