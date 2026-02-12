from __future__ import annotations

import sqlite3
from contextlib import asynccontextmanager
from typing import AsyncIterator

import aiosqlite


DEFAULT_TIMEOUT_SEC = 5.0


async def _apply_pragmas_async(db: aiosqlite.Connection, timeout_sec: float) -> None:
    # WAL: reader/writer 공존에 유리 (특히 봇 + 배치 동시 접근)
    await db.execute("PRAGMA journal_mode=WAL;")
    await db.execute("PRAGMA foreign_keys=ON;")
    await db.execute(f"PRAGMA busy_timeout={int(timeout_sec * 1000)};")


def _apply_pragmas_sync(con: sqlite3.Connection, timeout_sec: float) -> None:
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA foreign_keys=ON;")
    con.execute(f"PRAGMA busy_timeout={int(timeout_sec * 1000)};")


@asynccontextmanager
async def open_db(db_path: str, timeout_sec: float = DEFAULT_TIMEOUT_SEC) -> AsyncIterator[aiosqlite.Connection]:
    """aiosqlite 연결 + 공통 PRAGMA 적용.

    - timeout_sec: sqlite3 connect timeout(=잠김 대기)
    - busy_timeout: PRAGMA로도 동일 값 적용(드라이버/환경 차이를 줄임)
    """
    db = await aiosqlite.connect(db_path, timeout=timeout_sec)
    try:
        await _apply_pragmas_async(db, timeout_sec)
        yield db
    finally:
        await db.close()


def open_db_sync(db_path: str, timeout_sec: float = DEFAULT_TIMEOUT_SEC) -> sqlite3.Connection:
    """sqlite3 동기 연결 + 공통 PRAGMA 적용."""
    con = sqlite3.connect(db_path, timeout=timeout_sec)
    _apply_pragmas_sync(con, timeout_sec)
    return con
